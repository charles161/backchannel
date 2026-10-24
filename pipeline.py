"""
pipeline.py — Backchannel orchestrator (Phase 1): a resumable, durable async
state machine over the 7 steps, streaming every transition to the UI via SSE.

Phase 1 runs steps 1-4 + 7 for real; steps 5 (call_vc) and 6 (call_founder)
are clean Phase-2 stubs (marked `phase2`, never executed).

Resume / idempotency (durable via db.py):
  * Cheap, side-effect-free steps (resolve, find_references) are recomputed from
    the Crustdata disk CACHE on resume — free, fast, deterministic. They are only
    *emitted* (active -> done) when not already done.
  * The reference call is the ONLY human/paid side effect. Its Bolna execution_id
    is persisted the instant the call is placed, so a crash mid-call resumes by
    POLLING that execution_id. A resume NEVER dials: a live run interrupted before
    its execution_id was saved is marked as an error instead.
  * Every run carries its own call_mode (stored at creation); a resume uses that,
    never the server's current CALL_MODE.
  * synthesize_signal (a gateway call, no human side effect) may re-run at most
    once on an unlucky restart; done steps are skipped.

Reuses the proven spike modules: crust.py, overlap.py, report.py, gateway.py.
Crustdata is served CACHE-ONLY (the monthly cap is exhausted / 402), so the app
makes ZERO live Crustdata calls.

Referee privacy: the referee is shown by role + company, never by name, and the
mock-mode call is labelled "Simulated" (its transcript is a fictional sample).
"""

from __future__ import annotations

import asyncio
import difflib
import json
import os
import sys
from pathlib import Path
from typing import Any

import db
import bolna
import report as report_mod
from crust import Crust, CrustError
from overlap import find_offlist_references, _merge_companies, _name, _linkedin

HERE = Path(__file__).resolve().parent
SAMPLE_TRANSCRIPT = HERE / "samples" / "transcript_sample.json"
FALLBACK_REPORT = HERE / "samples" / "fallback_report.json"

# ---------------------------------------------------------------------------
# config (env)
# ---------------------------------------------------------------------------
CALL_MODE = os.environ.get("CALL_MODE", "mock").strip().lower()  # mode for NEW runs
BOLNA_AGENT_ID = os.environ.get("BACKCHANNEL_AGENT_ID", bolna.DEFAULT_AGENT_ID)
REF_PHONE = os.environ.get("BACKCHANNEL_REF_PHONE", "").strip()
ALLOWED_PHONES = {
    p.strip()
    for p in os.environ.get("BACKCHANNEL_ALLOWED_PHONES", "").split(",")
    if p.strip()
}
POLL_INTERVAL_S = float(os.environ.get("BACKCHANNEL_POLL_S", "4"))
CALL_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_CALL_TIMEOUT_S", "240"))   # until connected
CALL_HARD_CAP_S = float(os.environ.get("BACKCHANNEL_CALL_HARD_CAP_S", "480"))  # once connected
DISCONNECT_GRACE_S = 10.0   # after call-disconnected, wait this long for "completed"
SYNTH_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_SYNTH_TIMEOUT_S", "25"))
RESUME_WINDOW_S = 15 * 60   # only resume runs touched in the last 15 minutes
# The one warm-cached founder. Real value lives in .env.local (never committed).
DEMO_FOUNDER = " ".join(os.environ.get("BACKCHANNEL_DEMO_FOUNDER", "").split())

# Bolna statuses after which the call will not progress any further.
FINISHED_STATUSES = {
    "completed", "no-answer", "busy", "failed", "canceled", "cancelled",
    "stopped", "error", "balance-low",
}
_CALL_END_MSG = {
    "no-answer": "The reference didn't pick up. Start over to try again.",
    "busy": "The reference's line was busy. Start over to try again.",
    "failed": "The call couldn't be connected. Start over to try again.",
    "error": "The call couldn't be connected. Start over to try again.",
    "canceled": "The call was cancelled before anyone spoke.",
    "cancelled": "The call was cancelled before anyone spoke.",
    "stopped": "The call was stopped before anyone spoke.",
    "balance-low": "The call couldn't go through (calling credit is low).",
    "completed": "The call ended before any conversation was recorded.",
    "call-disconnected": "The call ended before any conversation was recorded.",
}
_STEP_FAIL_MSG = {
    "resolve_founder": "Couldn't load this founder's work history. Start over and try again.",
    "find_references": "Couldn't map this founder's ex-colleagues. Start over and try again.",
    "call_reference": "The reference call couldn't be completed. Start over and try again.",
    "synthesize_signal": "Couldn't put the verdict together. Start over and try again.",
}
_GENERIC_FAIL_MSG = "Something went wrong finishing this check. Start over and try again."


class FriendlyError(RuntimeError):
    """An error whose message is safe to put on the stage screen as-is."""


def _log(msg: str) -> None:
    print(f"[backchannel] {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# the 7-step registry
# ---------------------------------------------------------------------------
STEPS: list[dict] = [
    {"step": "resolve_founder",   "label": "Finding {founder}'s work history",    "phase2": False},
    {"step": "find_references",   "label": "Mapping ex-colleagues by overlap",    "phase2": False},
    {"step": "call_reference",    "label": "Calling the top reference to verify", "phase2": False},
    {"step": "synthesize_signal", "label": "Synthesizing the signal",             "phase2": False},
    {"step": "call_vc",           "label": "Calling you with the verdict",        "phase2": True},
    {"step": "call_founder",      "label": "Booking the meeting with {founder}",  "phase2": True},
    {"step": "done",              "label": "Verdict ready",                       "phase2": False},
]
STEP_INFO = {s["step"]: s for s in STEPS}


def initial_steps(founder: str) -> list[dict]:
    out = []
    for s in STEPS:
        out.append({
            "step": s["step"],
            "label": s["label"].replace("{founder}", founder or "the founder"),
            "phase2": s["phase2"],
        })
    return out


# ---------------------------------------------------------------------------
# founder-name matching (a typo or case slip on stage must not hard-error)
# ---------------------------------------------------------------------------
def _norm(s: str | None) -> str:
    return " ".join((s or "").lower().split())


def canonical_founder(name: str | None) -> str:
    """Map a typed founder name onto DEMO_FOUNDER when it's clearly meant to be it.

    Case-insensitive; tolerates small typos ("Abhilsh Chowdury") and a lone first
    or last name. Anything else is returned as typed (and will miss the cache)."""
    typed = " ".join((name or "").split())
    if not DEMO_FOUNDER:
        return typed
    if not typed:
        return DEMO_FOUNDER
    a, b = _norm(typed), _norm(DEMO_FOUNDER)
    if a == b or difflib.SequenceMatcher(None, a, b).ratio() >= 0.8:
        return DEMO_FOUNDER
    demo_tokens = b.split()
    typed_tokens = a.split()
    if len(a) >= 3 and all(
        any(d.startswith(t) or difflib.SequenceMatcher(None, t, d).ratio() >= 0.8
            for d in demo_tokens)
        for t in typed_tokens
    ):
        return DEMO_FOUNDER
    return typed


# ---------------------------------------------------------------------------
# cache-only Crustdata (guarantees ZERO live calls — the cap is 402'd)
# ---------------------------------------------------------------------------
class CacheOnlyCrust(Crust):
    """A Crust that NEVER hits the network: a cache miss raises instead of calling
    the live API. find_offlist_references already degrades gracefully on a per-company
    error, so uncached companies are skipped while cached ones still yield referees."""

    def _post(self, endpoint, body, *, cache=True):  # type: ignore[override]
        path = self._cache_path(endpoint, body)
        if path.exists():
            self.cache_hits += 1
            self.call_log.append({"endpoint": endpoint, "source": "cache", "file": path.name})
            return json.loads(path.read_text())
        self.call_log.append({"endpoint": endpoint, "source": "cache-miss-blocked"})
        raise CrustError(
            f"{endpoint} -> cache miss (live Crustdata disabled for the demo; cap 402)"
        )


def _crust() -> CacheOnlyCrust:
    # Crust.__init__ requires CRUSTDATA_API_KEY present; run.sh sources it. We never
    # use it live, but satisfy the constructor (fall back to a placeholder if absent).
    if not os.environ.get("CRUSTDATA_API_KEY"):
        os.environ["CRUSTDATA_API_KEY"] = "cache-only-demo"
    return CacheOnlyCrust()


# ---------------------------------------------------------------------------
# sync (blocking) workers — run via asyncio.to_thread; they NEVER touch the DB
# ---------------------------------------------------------------------------
def _current_title(profile: dict) -> str | None:
    bp = profile.get("basic_profile") or {}
    if bp.get("current_title"):
        return bp["current_title"]
    emp = (profile.get("experience") or {}).get("employment_details") or {}
    cur = emp.get("current") or []
    return cur[0].get("title") if cur else None


def _not_preloaded(name: str) -> FriendlyError:
    hint = f" Try {DEMO_FOUNDER}." if DEMO_FOUNDER and name != DEMO_FOUNDER else ""
    return FriendlyError(f"No pre-loaded work history for “{name}” in this demo.{hint}")


def resolve_founder_sync(name: str) -> dict:
    """Crustdata person/search by name (cache HIT for the demo founder). 0 cost."""
    name = canonical_founder(name)
    c = _crust()
    try:
        res = c.search_people(
            [{"field": "basic_profile.name", "type": "(.)", "value": name}], limit=1
        )
    except CrustError:
        raise _not_preloaded(name) from None
    profiles = res.get("profiles") or []
    if not profiles:
        raise _not_preloaded(name)
    return profiles[0]


def find_refs_sync(profile: dict) -> tuple[list[dict], dict]:
    c = _crust()
    return find_offlist_references(c, profile, max_companies=3, pool_per_company=10)


def synthesize_sync(candidate: dict, referee: dict, transcript: dict) -> dict:
    return report_mod.synthesize(candidate, referee, transcript)


# ---------------------------------------------------------------------------
# result shaping
# ---------------------------------------------------------------------------
_VERDICT_MAP = {
    "STRONG HIRE": ("FUND", "strong"),
    "HIRE": ("FUND", ""),
    "LEAN HIRE": ("LEAN", ""),
    "NO HIRE": ("PASS", ""),
}
_SCORE_KEYS = ("credibility", "enthusiasm", "claim_corroboration")


def _verdict_from_report(rep: dict) -> dict:
    rec = rep.get("recommendation") or {}
    tier = (rec.get("tier") or rec.get("verdict") or "LEAN HIRE").upper()
    verdict, strength = _VERDICT_MAP.get(tier, ("LEAN", ""))
    return {
        "verdict": verdict,          # FUND | LEAN | PASS
        "strength": strength,        # "strong" for STRONG HIRE
        "tier": tier,
        "confidence": rec.get("confidence", "medium"),
        "rationale": rec.get("rationale", ""),
    }


def _companies_summary(profile: dict) -> dict:
    tenures = sorted(_merge_companies(profile), key=lambda t: t.end, reverse=True)
    names = [t.company for t in tenures]
    rows = [{"company": t.company, "window": t.window(), "current": t.is_current}
            for t in tenures]
    if len(names) <= 3:
        summary = ", ".join(names)
    else:
        summary = ", ".join(names[:3]) + f" +{len(names) - 3} more"
    return {
        "name": _name(profile),
        "headline": (profile.get("basic_profile") or {}).get("headline"),
        "current_title": _current_title(profile),
        "linkedin_url": _linkedin(profile),
        "companies": rows,
        "summary": summary,
    }


def _ref_display(r: dict) -> str:
    """How a referee is shown anywhere outside the server: role + company, no name."""
    title = (r.get("title") or "").strip() or "Former colleague"
    company = (r.get("company") or "").strip()
    if company and company.lower() not in title.lower():
        return f"{title} at {company}"
    return title


def _referee_brief(r: dict) -> dict:
    return {
        "display": _ref_display(r),
        "title": r.get("title"),
        "company": r.get("company"),
        "overlap_window": r.get("overlap_window"),
        "overlap_months": r.get("overlap_months"),
        "seniority": r.get("seniority"),
        "score": r.get("score"),
    }


def _referee_for_synthesis(r: dict) -> dict:
    """The referee as the LLM sees it — anonymized so no name lands in the report."""
    return {**r, "name": _ref_display(r), "linkedin_url": None}


def _intro_email(vc_name: str, founder: dict, referee: dict, verdict: dict) -> str:
    me = (vc_name or "").strip()
    full = founder.get("name") or "there"
    first = full.split(" ")[0]
    refco = referee.get("company") or "a company you both worked at"
    intro = f"I'm {me}. " if me else ""
    sign = f"Best,\n{me}" if me else "Best,"
    v = verdict.get("verdict")
    if v == "FUND":
        body = (f"{intro}I've been doing some background work ahead of a potential "
                f"investment and spoke with one of your former colleagues at {refco}, "
                f"who spoke very highly of your work.\n\n"
                f"I'd love to grab 30 minutes to hear what you're building and where I "
                f"might be able to help. Are you free sometime this week?")
        subject = f"Quick intro — {me or 'an investor'} <> {full}"
    elif v == "LEAN":
        body = (f"{intro}I've been doing some background work ahead of a potential "
                f"investment, including a conversation with one of your former "
                f"colleagues at {refco}.\n\n"
                f"I'd like to understand more about what you're building before going "
                f"further. Could we find 30 minutes in the next couple of weeks?")
        subject = f"Intro — {me or 'an investor'} <> {full}"
    else:
        body = (f"{intro}Thanks for the time so far. After some background work I don't "
                f"think this is the right fit for me at this stage, but I'd be glad to "
                f"stay in touch as things progress.")
        subject = "Staying in touch"
    return f"Subject: {subject}\n\nHi {first},\n\n{body}\n\n{sign}"


def _build_result(run: dict, rep: dict, verdict: dict,
                  founder: dict, referee: dict) -> dict:
    mode = run.get("call_mode") or "mock"
    strengths = rep.get("strengths") or []
    claims = rep.get("claims_confirmed") or []
    highlights = (strengths + claims)[:4]
    return {
        "verdict": verdict["verdict"],
        "strength": verdict["strength"],
        "tier": verdict["tier"],
        "confidence": verdict["confidence"],
        "rationale": verdict["rationale"],
        "call_mode": mode,
        "simulated": mode != "live",
        "founder": founder,
        "referee": {**referee, "relationship": rep.get("relationship")},
        "highlights": highlights,
        "claims_confirmed": claims,
        "strengths": strengths,
        "concerns": rep.get("concerns") or [],
        "sub_scores": rep.get("sub_scores") or {},
        "synthesis_mode": rep.get("synthesis_mode"),
        "intro_email": _intro_email(run["inputs"]["vc_name"], founder, referee, verdict),
        "meeting": {
            "status": "phase2",
            "note": "The call-back to you + founder call + meeting booking land in "
                    "Phase 2 (steps 5-6). Phase 1 ends at a verified verdict.",
        },
    }


# ---------------------------------------------------------------------------
# synthesis with a hard deadline + safe fallbacks (step 4)
# ---------------------------------------------------------------------------
def _scores_missing(rep: dict) -> bool:
    ss = rep.get("sub_scores") or {}
    vals = [ss.get(k) for k in _SCORE_KEYS]
    if any(not isinstance(v, (int, float)) for v in vals):
        return True
    return all(v == 50 for v in vals)  # report.py's "both scorers failed" default


def _presaved_report(candidate: dict, referee: dict) -> dict:
    """A pre-saved good report (mock mode only) dressed with this run's identities."""
    data = json.loads(FALLBACK_REPORT.read_text())
    window = referee.get("overlap_window", "")
    months = referee.get("overlap_months", 0)
    relationship = (f"Worked alongside the founder at {referee.get('company')} "
                    f"({window}, {months} months overlap).")
    rec = dict(data["recommendation"])
    return {
        "referee": {
            "name": referee.get("name"),
            "title": referee.get("title") or "former colleague",
            "company": referee.get("company"),
            "linkedin_url": None,
            "relationship": relationship,
        },
        "candidate": dict(candidate),
        "relationship": relationship,
        "claims_confirmed": list(data["claims_confirmed"]),
        "strengths": list(data["strengths"]),
        "concerns": list(data["concerns"]),
        "recommendation": rec,
        "sub_scores": dict(data["sub_scores"]),
        "synthesis_mode": "presaved",
        "provenance": {
            "evidence": "employment-overlap graph (cached Crustdata)",
            "overlap_window": window,
            "overlap_months": months,
            "transcript_channel": "simulated",
            "synthesis": "pre-saved report (live synthesis slow or unavailable)",
            "scorer": "pre-saved",
        },
    }


def _unavailable_report(candidate: dict, referee: dict) -> dict:
    """Live mode only: an honest placeholder — never canned content for a real call."""
    return {
        "referee": {"name": referee.get("name"), "title": referee.get("title"),
                    "company": referee.get("company"), "linkedin_url": None,
                    "relationship": None},
        "candidate": dict(candidate),
        "relationship": None,
        "claims_confirmed": [],
        "strengths": [],
        "concerns": [],
        "recommendation": {
            "tier": "LEAN HIRE", "verdict": "LEAN HIRE", "confidence": "low",
            "rationale": "Automatic analysis didn't finish in time — read the call "
                         "transcript before deciding.",
        },
        "sub_scores": {},
        "synthesis_mode": "unavailable",
        "provenance": {"synthesis": "unavailable (gateway slow or down)"},
    }


async def _synthesize(mode: str, candidate: dict, referee: dict, transcript: dict) -> dict:
    """Step 4 with a hard deadline. Mock mode can never hang or show 50/50/50 bars."""
    rep: dict | None
    try:
        rep = await asyncio.wait_for(
            asyncio.to_thread(synthesize_sync, candidate, referee, transcript),
            timeout=SYNTH_TIMEOUT_S,
        )
    except Exception as e:  # noqa: BLE001 — timeout, gateway transport, anything
        _log(f"synthesis failed ({type(e).__name__}: {e}); using fallback")
        rep = None

    if mode != "live":
        if rep is None or rep.get("synthesis_mode") == "fallback" or _scores_missing(rep):
            return _presaved_report(candidate, referee)
        return rep

    if rep is None:
        return _unavailable_report(candidate, referee)
    if _scores_missing(rep):
        rep["sub_scores"] = {}  # the UI hides the bars rather than show fake 50s
    return rep


# ---------------------------------------------------------------------------
# event bus (in-memory subscribers; DB is the durable source of truth)
# ---------------------------------------------------------------------------
SUBSCRIBERS: dict[str, set[asyncio.Queue]] = {}
RUNNING_TASKS: dict[str, asyncio.Task] = {}
RUNNING_MODES: dict[str, str] = {}


def subscribe(run_id: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    SUBSCRIBERS.setdefault(run_id, set()).add(q)
    return q


def unsubscribe(run_id: str, q: asyncio.Queue) -> None:
    subs = SUBSCRIBERS.get(run_id)
    if subs:
        subs.discard(q)
        if not subs:
            SUBSCRIBERS.pop(run_id, None)


def _emit(run_id: str, event: dict) -> None:
    for q in list(SUBSCRIBERS.get(run_id, ())):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:  # pragma: no cover
            pass


def _set_step(run_id: str, step: str, status: str, *, label: str | None = None,
              result: Any = None, execution_id: str | None = None) -> None:
    """Persist a step transition, then push the {type:'step'} event to subscribers."""
    db.upsert_step(run_id, step, status=status, label=label,
                   result=result, execution_id=execution_id)
    info = STEP_INFO.get(step, {})
    # label for the event: the one just written, else the stored/registry label
    ev_label = label
    if ev_label is None:
        row = next((s for s in (db.get_run(run_id) or {}).get("steps", [])
                    if s["step"] == step), None)
        ev_label = row["label"] if row else info.get("label")
    _emit(run_id, {
        "type": "step",
        "step": step,
        "label": ev_label,
        "status": status,
        "result": result,
        "phase2": bool(info.get("phase2")),
    })


def _emit_banner(run_id: str, text: str, active: bool) -> None:
    _emit(run_id, {"type": "banner", "text": text, "active": active})


# ---------------------------------------------------------------------------
# the reference call (step 3) — mock streams a canned transcript; live polls Bolna
# ---------------------------------------------------------------------------
async def _reference_call(run_id: str, referee: dict, prior_step: dict, mode: str,
                          founder_name: str, fresh: bool) -> dict:
    """Return a transcript dict {"qa":[...], "signals":{...}}.

    Streams transcript turns to the UI as they arrive. A resumed live run only
    ever POLLS its stored execution_id; it never places a new call."""
    display = _ref_display(referee)
    simulated = mode != "live"
    prior_exec = prior_step.get("execution_id")
    if mode == "live" and not prior_exec and not fresh:
        raise FriendlyError(
            "This check was interrupted before the call was confirmed, so it wasn't "
            "redialed automatically. Start over to place a new call."
        )

    base_result = {
        "referee": display,
        "referee_title": referee.get("title"),
        "company": referee.get("company"),
        "mode": mode,
        "simulated": simulated,
        "ringing": True,
        "turns": [],
    }
    label = f"Simulated call with {display}" if simulated else f"Calling {display} to verify"
    _set_step(run_id, "call_reference", "active", label=label, result=base_result)
    _emit_banner(run_id, f"\U0001F4DE Ringing {display}…"
                 + (" (simulated)" if simulated else ""), True)

    if mode == "live":
        transcript = await _live_call(run_id, prior_exec, base_result)
    else:
        transcript = await _mock_call(run_id, base_result, founder_name)

    _emit_banner(run_id, "", False)
    return transcript


def _connected_banner(base_result: dict) -> str:
    tail = " · simulated call" if base_result.get("simulated") else ""
    return f"Connected to {base_result.get('referee') or 'the reference'}{tail}"


def _load_mock_transcript(founder_name: str) -> tuple[list[dict], dict]:
    data = json.loads(SAMPLE_TRANSCRIPT.read_text())
    first = (founder_name or "").split(" ")[0] or "them"
    qa = [{"q": (t.get("q") or "").replace("{first_name}", first),
           "a": (t.get("a") or "").replace("{first_name}", first)}
          for t in data.get("qa") or []]
    return qa, data.get("signals") or {}


async def _mock_call(run_id: str, base_result: dict, founder_name: str) -> dict:
    qa, signals = _load_mock_transcript(founder_name)
    await asyncio.sleep(1.4)  # "ringing"
    base_result = {**base_result, "ringing": False, "connected": True}
    db.upsert_step(run_id, "call_reference", result=base_result)
    _emit_banner(run_id, _connected_banner(base_result), True)
    turns: list[dict] = []
    for turn in qa:
        turns.append(turn)
        res = {**base_result, "turns": list(turns)}
        db.upsert_step(run_id, "call_reference", result=res)
        _emit(run_id, {"type": "transcript", "step": "call_reference",
                       "turns": list(turns), "partial": True})
        await asyncio.sleep(0.9)
    return {"qa": turns, "signals": signals}


async def _live_call(run_id: str, prior_exec: str | None, base_result: dict) -> dict:
    # Idempotency: reuse a stored execution_id (resume) instead of re-dialing.
    exec_id = prior_exec
    if not exec_id:
        if not REF_PHONE:
            raise FriendlyError("Live calling isn't set up on this server (no reference "
                                "number configured).")
        if REF_PHONE not in ALLOWED_PHONES:
            raise FriendlyError("Live calling is limited to verified numbers, and the "
                                "configured number isn't on the allowlist.")
        try:
            resp = await asyncio.to_thread(bolna.place_call, BOLNA_AGENT_ID, REF_PHONE)
        except bolna.BolnaError as e:
            _log(f"place_call failed: {e}")
            raise FriendlyError("The call couldn't be placed right now. Start over and "
                                "try again in a minute.") from None
        exec_id = resp.get("execution_id")
        if not exec_id:
            _log(f"place_call returned no execution_id: {str(resp)[:300]}")
            raise FriendlyError("The phone service didn't confirm the call. Start over "
                                "and try again.")
        # PERSIST execution_id IMMEDIATELY so a crash now resumes by polling, not re-calling.
        db.upsert_step(run_id, "call_reference", execution_id=exec_id,
                       result={**base_result, "execution_id": exec_id})

    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + CALL_TIMEOUT_S        # ring/connect budget
    hard_cap = started + CALL_HARD_CAP_S       # a connected call may run up to this
    disconnected_at: float | None = None
    connected = False
    timed_out = False
    status = ""
    last_n = 0
    transcript: dict = {"qa": []}
    while True:
        ex = None
        try:
            ex = await asyncio.to_thread(bolna.get_execution, exec_id)
        except bolna.BolnaError as e:
            _log(f"get_execution failed (will retry): {e}")
        if ex is not None:
            status = (ex.get("status") or "").lower()
            parsed = bolna.parse_transcript(ex.get("transcript"))
            if parsed["qa"]:
                transcript = parsed  # never wipe turns with a transiently empty field
            turns = transcript["qa"]
            if not connected and (status == "in-progress" or turns):
                connected = True
                deadline = max(deadline, hard_cap)  # don't cut a live conversation off
                res = {**base_result, "ringing": False, "connected": True,
                       "execution_id": exec_id, "turns": list(turns)}
                db.upsert_step(run_id, "call_reference", result=res, execution_id=exec_id)
                _emit_banner(run_id, _connected_banner(base_result), True)
            if len(turns) != last_n:
                last_n = len(turns)
                res = {**base_result, "ringing": False, "connected": True,
                       "execution_id": exec_id, "turns": list(turns),
                       "duration": ex.get("conversation_duration"),
                       "cost": ex.get("total_cost")}
                db.upsert_step(run_id, "call_reference", result=res, execution_id=exec_id)
                _emit(run_id, {"type": "transcript", "step": "call_reference",
                               "turns": list(turns), "partial": True})
            if status in FINISHED_STATUSES:
                break
            if status == "call-disconnected":
                # transcript/duration are finalized a little after the hang-up
                if disconnected_at is None:
                    disconnected_at = loop.time()
                elif loop.time() - disconnected_at >= DISCONNECT_GRACE_S:
                    break
        if loop.time() > deadline:
            timed_out = True
            break
        await asyncio.sleep(min(2.0, POLL_INTERVAL_S) if disconnected_at is not None
                            else POLL_INTERVAL_S)

    if not transcript.get("qa"):
        _log(f"live call {exec_id} ended without transcript: status={status!r} "
             f"timed_out={timed_out}")
        if timed_out and not connected:
            mins = max(1, round(CALL_TIMEOUT_S / 60))
            raise FriendlyError(f"No answer after about {mins} minute{'s' if mins != 1 else ''}. "
                                f"Start over to try again.")
        raise FriendlyError(_CALL_END_MSG.get(
            status, "The call ended before any conversation was recorded."))
    transcript.setdefault("signals", {})
    return transcript


# ---------------------------------------------------------------------------
# the pipeline (resumable)
# ---------------------------------------------------------------------------
async def run_pipeline(run_id: str, fresh: bool = False) -> None:
    run = db.get_run(run_id)
    if not run:
        return
    mode = (run.get("call_mode") or "mock").strip().lower()   # the RUN's mode, not ours
    prior = {s["step"]: s for s in run["steps"]}
    founder_name = canonical_founder(run["inputs"]["founder_name"] or DEMO_FOUNDER)
    current = "resolve_founder"
    # tiny grace so a just-redirected browser connects before step 1 goes active
    # (pure visual polish; the SSE snapshot already guarantees no state is lost)
    if prior["resolve_founder"]["status"] == "pending":
        await asyncio.sleep(0.5)
    try:
        # --- Step 1: resolve_founder (cheap, cache-only) -------------------
        current = "resolve_founder"
        if prior["resolve_founder"]["status"] != "done":
            _set_step(run_id, "resolve_founder", "active")
            await asyncio.sleep(0.4)
            profile = await asyncio.to_thread(resolve_founder_sync, founder_name)
            founder = _companies_summary(profile)
            _set_step(run_id, "resolve_founder", "done",
                      result={"founder": founder, "summary": founder["summary"]})
        else:
            profile = await asyncio.to_thread(resolve_founder_sync, founder_name)
            founder = _companies_summary(profile)

        # --- Step 2: find_references (cheap, cache-only) -------------------
        current = "find_references"
        if prior["find_references"]["status"] != "done":
            _set_step(run_id, "find_references", "active")
            await asyncio.sleep(0.4)
        ranked, meta = await asyncio.to_thread(find_refs_sync, profile)
        if not ranked:
            raise FriendlyError(
                f"No ex-colleagues with overlapping tenure were found for {founder['name']}.")
        top = ranked[0]
        if prior["find_references"]["status"] != "done":
            _set_step(run_id, "find_references", "done", result={
                "n_found": len(ranked),
                "top": _referee_brief(top),
                "others": [_referee_brief(r) for r in ranked[1:4]],
                "summary": f"{len(ranked)} found · top: {_ref_display(top)} "
                           f"({top.get('overlap_months')} mo overlap)",
            })

        # --- Step 3: call_reference (THE side effect — idempotent on resume) -
        current = "call_reference"
        if prior["call_reference"]["status"] == "done":
            stored = prior["call_reference"]["result"] or {}
            transcript = {"qa": stored.get("turns") or [],
                          "signals": stored.get("signals") or {}}
        else:
            transcript = await _reference_call(run_id, top, prior["call_reference"],
                                               mode, founder["name"] or founder_name, fresh)
            _set_step(run_id, "call_reference", "done", result={
                "referee": _ref_display(top),
                "referee_title": top.get("title"),
                "company": top.get("company"),
                "mode": mode,
                "simulated": mode != "live",
                "ringing": False,
                "connected": True,
                "turns": transcript["qa"],
                "n_turns": len(transcript["qa"]),
                "signals": transcript.get("signals") or {},
                "summary": (f"{len(transcript['qa'])} exchanges"
                            + (" · simulated call" if mode != "live" else " recorded")),
            })

        # --- Step 4: synthesize_signal (gateway, hard deadline + fallback) --
        current = "synthesize_signal"
        if prior["synthesize_signal"]["status"] == "done":
            rep = (prior["synthesize_signal"]["result"] or {}).get("report") or {}
            verdict = _verdict_from_report(rep)
        else:
            _set_step(run_id, "synthesize_signal", "active")
            cand = {"name": founder["name"], "current_title": founder["current_title"],
                    "linkedin_url": founder["linkedin_url"]}
            rep = await _synthesize(mode, cand, _referee_for_synthesis(top), transcript)
            verdict = _verdict_from_report(rep)
            _set_step(run_id, "synthesize_signal", "done", result={
                "verdict": verdict["verdict"],
                "tier": verdict["tier"],
                "confidence": verdict["confidence"],
                "sub_scores": rep.get("sub_scores") or {},
                "summary": f"{verdict['verdict']} · {verdict['confidence']} confidence",
                "report": rep,
            })

        # --- Steps 5 & 6: Phase-2 stubs (never executed) -------------------
        for stub in ("call_vc", "call_founder"):
            current = stub
            if prior[stub]["status"] != "phase2":
                _set_step(run_id, stub, "phase2", result={
                    "note": "Phase 2 — not built for this demo."})

        # --- Step 7: done --------------------------------------------------
        current = "done"
        result = _build_result(run, rep, verdict, founder, _referee_brief(top))
        _set_step(run_id, "done", "done",
                  result={"verdict": verdict["verdict"], "summary": "Verdict ready"})
        db.update_run(run_id, status="done", result=result)
        _emit(run_id, {"type": "done", "result": result})

    except Exception as e:  # noqa: BLE001
        _log(f"run {run_id} failed at {current}: {type(e).__name__}: {e}")
        if isinstance(e, FriendlyError):
            msg = str(e)
        else:
            msg = _STEP_FAIL_MSG.get(current, _GENERIC_FAIL_MSG)
        db.update_run(run_id, status="error", error=msg)
        _set_step(run_id, current, "error",
                  result={"error": msg, "detail": f"{type(e).__name__}: {e}"[:300]})
        _emit(run_id, {"type": "error", "step": current, "error": msg})
    finally:
        _emit(run_id, {"type": "end"})
        RUNNING_TASKS.pop(run_id, None)
        RUNNING_MODES.pop(run_id, None)


def ensure_running(run_id: str, *, fresh: bool = False, mode: str | None = None) -> None:
    """Launch the pipeline task for a run if one isn't already alive in this process.

    fresh=True only for the launch right after POST /run created the run; every
    other launch is a resume and may not place a new live call."""
    t = RUNNING_TASKS.get(run_id)
    if t and not t.done():
        return
    if mode is None:
        mode = ((db.get_run(run_id) or {}).get("call_mode") or "mock")
    RUNNING_MODES[run_id] = mode
    RUNNING_TASKS[run_id] = asyncio.create_task(run_pipeline(run_id, fresh=fresh))


def live_call_in_progress() -> bool:
    """True if a live-mode run is currently executing in this process."""
    return any(RUNNING_MODES.get(rid) == "live" and not t.done()
               for rid, t in RUNNING_TASKS.items())


def _expire(run_id: str, run: dict) -> None:
    msg = "This check was interrupted and has expired. Start over to run a new one."
    db.update_run(run_id, status="error", error=msg)
    for s in run.get("steps") or []:
        if s["status"] == "active":
            db.upsert_step(run_id, s["step"], status="error",
                           result={**(s.get("result") or {}), "error": msg})
            break


async def resume_unfinished() -> None:
    """On startup, relaunch runs left 'running' — only recent ones (<15 min)."""
    cutoff = db.now() - RESUME_WINDOW_S
    for run_id in db.running_run_ids():
        run = db.get_run(run_id)
        if not run:
            continue
        if (run.get("updated_at") or 0) < cutoff:
            _log(f"not resuming stale run {run_id}; marking it expired")
            _expire(run_id, run)
            continue
        ensure_running(run_id, fresh=False, mode=run.get("call_mode") or "mock")

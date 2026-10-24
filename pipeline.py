"""
pipeline.py — Backchannel orchestrator: a resumable, durable async state machine
over the 7 steps, streaming every transition to the UI via SSE.

The 3-call chain: 3 call_reference (verify the founder) -> 4 synthesize_signal ->
5 call_vc (deliver the verdict, get the VC's free slots) -> 6 call_founder (pitch
the interest, match the founder to a slot, confirm a time) -> 7 done (verdict +
booked meeting + intro email + the founder's summary email with an .ics invite).
Runs created before Phase 2 shipped keep their steps 5-6 as `phase2` stubs and
finish exactly as they started.

An optional `role` (what the VC is hiring/funding for) rides along: it colours the
reference call (mock script / live user_data), and synthesis turns the verdict into
a fit-for-that-role verdict (result.role_fit). The 7-step SSE contract is unchanged;
new data only lands in existing step results and the final result.

Resume / idempotency (durable via db.py):
  * Cheap, side-effect-free steps (resolve, find_references) are recomputed from
    the Crustdata disk CACHE on resume — free, fast, deterministic. They are only
    *emitted* (active -> done) when not already done.
  * The three calls are the only human/paid side effects. Each call's Bolna
    execution_id is persisted on its own step the instant the call is placed, so
    a crash mid-call resumes by POLLING that execution_id. A resume NEVER dials: a
    live call that hadn't been placed (or whose execution_id wasn't saved) before
    the interruption is reported instead of placed.
  * A failed VC/founder call doesn't throw away the verdict: the step is marked
    as an error and the run still finishes, with the meeting marked not booked.
  * Every run carries its own call_mode (stored at creation); a resume uses that,
    never the server's current CALL_MODE.
  * synthesize_signal and the slot/time extraction (gateway calls, no human side
    effect) may re-run at most once on an unlucky restart; done steps are skipped.
  * The founder summary email is the fourth side effect. Its state is persisted on
    the run (db email_json) before and after the send: a resume reuses a finished
    send (never re-sends), and an unconfirmed one is retried with the SAME Resend
    Idempotency-Key and a byte-identical payload, which Resend de-duplicates.

Reuses the proven spike modules: crust.py, overlap.py, report.py, gateway.py.
Crustdata: CRUSTDATA_LIVE=1 (default) = live person/search behind the disk cache
(a repeat is free); a 402 / network error falls back to the cache and never crashes
a run (a 402 also pauses live calls for 10 minutes). CRUSTDATA_LIVE=0 = cache-only,
ZERO live calls.

Referee privacy: the referee is shown by role + company, never by name, and the
mock-mode calls are labelled "Simulated" (their transcripts are fictional samples).

Contacts: each displayed reference brief (top + 3 others) carries a DISPLAY-ONLY
masked `email` ("a•••.k•••@stripe.com") and `phone` ("+1 31XXX X1004") from Crustdata
contact-enrich (contacts.py, disk-cached per person), or a masked placeholder from
placeholder_contacts.py when there's no real contact. They are never dialed: the three
calls dial REF_PHONE, the VC's own typed number and FOUNDER_PHONE only, and _live_call
refuses a masked number before anything reaches bolna.place_call.
"""

from __future__ import annotations

import asyncio
import difflib
import json
import os
import re
import secrets
import sys
import time
from pathlib import Path
from typing import Any

import db
import bolna
import contacts
import invite
import placeholder_contacts
import report as report_mod
import scheduling
from crust import Crust, CrustError
from overlap import find_offlist_references, _merge_companies, _name, _linkedin

HERE = Path(__file__).resolve().parent
SAMPLE_TRANSCRIPT = HERE / "samples" / "transcript_sample.json"
FALLBACK_REPORT = HERE / "samples" / "fallback_report.json"
VC_SAMPLE = HERE / "samples" / "vc_call_sample.json"
FOUNDER_SAMPLE = HERE / "samples" / "founder_call_sample.json"

# ---------------------------------------------------------------------------
# config (env)
# ---------------------------------------------------------------------------
CALL_MODE = os.environ.get("CALL_MODE", "mock").strip().lower()  # mode for NEW runs
BOLNA_AGENT_ID = os.environ.get("BACKCHANNEL_AGENT_ID", bolna.DEFAULT_AGENT_ID)
# Phase 2: one Bolna agent per role, created by setup_agents.py (ids in .env.local).
VC_AGENT_ID = os.environ.get("BACKCHANNEL_VC_AGENT_ID", "").strip()
FOUNDER_AGENT_ID = os.environ.get("BACKCHANNEL_FOUNDER_AGENT_ID", "").strip()
REF_PHONE = os.environ.get("BACKCHANNEL_REF_PHONE", "").strip()
FOUNDER_PHONE = os.environ.get("BACKCHANNEL_FOUNDER_PHONE", "").strip()
ALLOWED_PHONES = {
    p.strip()
    for p in os.environ.get("BACKCHANNEL_ALLOWED_PHONES", "").split(",")
    if p.strip()
}
# the same allowlist, normalized: a VC typing "+91 98xxx-xxxxx" still matches
_ALLOWED_NORM = {bolna.normalize_phone(p) for p in ALLOWED_PHONES} - {""}
POLL_INTERVAL_S = float(os.environ.get("BACKCHANNEL_POLL_S", "4"))
CALL_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_CALL_TIMEOUT_S", "240"))   # until connected
CALL_HARD_CAP_S = float(os.environ.get("BACKCHANNEL_CALL_HARD_CAP_S", "480"))  # once connected
DISCONNECT_GRACE_S = 10.0   # after call-disconnected, wait this long for "completed"
SYNTH_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_SYNTH_TIMEOUT_S", "25"))
EXTRACT_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_EXTRACT_TIMEOUT_S", "15"))
EMAIL_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_EMAIL_TIMEOUT_S", "20"))
RESUME_WINDOW_S = 15 * 60   # only resume runs touched in the last 15 minutes
# The one warm-cached founder. Real value lives in .env.local (never committed).
DEMO_FOUNDER = " ".join(os.environ.get("BACKCHANNEL_DEMO_FOUNDER", "").split())

# Bolna statuses after which the call will not progress any further.
FINISHED_STATUSES = {
    "completed", "no-answer", "busy", "failed", "canceled", "cancelled",
    "stopped", "error", "balance-low",
}
_CALL_END_MSG = {
    "no-answer": "{who} didn't pick up. Start over to try again.",
    "busy": "{whose} line was busy. Start over to try again.",
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
    "call_vc": "The call to you couldn't be completed.",
    "call_founder": "The call to the founder couldn't be completed.",
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
    {"step": "call_vc",           "label": "Calling you with the verdict",        "phase2": False},
    {"step": "call_founder",      "label": "Booking the meeting with {founder}",  "phase2": False},
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
# Crustdata: live behind the disk cache (default), or cache-only (CRUSTDATA_LIVE=0)
# ---------------------------------------------------------------------------
_LIVE_PAUSE_S = 600.0        # after a 402, stay cache-only this long (don't hammer a cap)
_NET_PAUSE_S = 60.0          # after a timeout / dropped connection, a short cache-only spell
_LIVE_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_CRUST_TIMEOUT_S", "20"))
_live_paused_until = 0.0


def crust_live() -> bool:
    """CRUSTDATA_LIVE: "1" (default) = live lookups behind the cache; "0" = cache-only."""
    v = os.environ.get("CRUSTDATA_LIVE", "1").strip().lower() or "1"
    return v not in ("0", "false", "no", "off")


def _credit_error(msg: str) -> bool:
    return "402" in msg or "insufficient_credits" in msg or "credit_limit" in msg


def _credit_error_recent() -> bool:
    """The current pause is a long (402) one, not a short network one."""
    return _live_paused_until - time.time() > _NET_PAUSE_S


def _cache_read(c: Crust, endpoint: str, body: dict, why: str) -> Any:
    """Serve from the disk cache, or raise CrustError on a miss (never calls the API)."""
    path = c._cache_path(endpoint, body)
    if path.exists():
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError):
            data = None
        if data is not None:
            c.cache_hits += 1
            c.call_log.append({"endpoint": endpoint, "source": "cache", "file": path.name})
            return data
    c.call_log.append({"endpoint": endpoint, "source": "cache-miss-blocked"})
    raise CrustError(f"{endpoint} -> cache miss ({why})")


class CacheOnlyCrust(Crust):
    """A Crust that NEVER hits the network: a cache miss raises instead of calling
    the live API. find_offlist_references already degrades gracefully on a per-company
    error, so uncached companies are skipped while cached ones still yield referees."""

    live = False

    def _post(self, endpoint, body, *, cache=True):  # type: ignore[override]
        return _cache_read(self, endpoint, body, "live Crustdata is off: CRUSTDATA_LIVE=0")


class LiveCrust(Crust):
    """Live Crustdata behind the disk cache: a hit is free, a miss calls the API and
    caches the answer. Any live failure (402 / 429 / network / 5xx) falls back to
    cache-only behaviour — the caller gets a CrustError it already degrades on, never
    a crash. A 402 also pauses live calls process-wide for _LIVE_PAUSE_S."""

    live = True

    def __init__(self, *a, **k):
        k.setdefault("timeout", _LIVE_TIMEOUT_S)   # a hung API mustn't stall a step for minutes
        super().__init__(*a, **k)

    def _post(self, endpoint, body, *, cache=True):  # type: ignore[override]
        global _live_paused_until
        path = self._cache_path(endpoint, body)
        if path.exists():
            try:
                return _cache_read(self, endpoint, body, "unreadable cache file")
            except CrustError:
                path.unlink(missing_ok=True)   # half-written / corrupt: refetch it live
        if time.time() < _live_paused_until:
            return _cache_read(self, endpoint, body,
                               "live Crustdata paused after a credit error (HTTP 402)"
                               if _credit_error_recent() else
                               "live Crustdata paused after a network error")
        try:
            return super()._post(endpoint, body, cache=cache)
        except CrustError as e:
            msg = str(e)
            if _credit_error(msg):
                _live_paused_until = time.time() + _LIVE_PAUSE_S
            elif "network error" in msg:   # down / hanging: let the next lookups use the cache
                _live_paused_until = max(_live_paused_until, time.time() + _NET_PAUSE_S)
            _log(f"live Crustdata {endpoint} failed ({msg[:160]}); falling back to cache")
            self.call_log.append({"endpoint": endpoint, "source": "live-failed"})
            raise CrustError(f"{endpoint} -> live lookup failed, no cached copy: {msg[:300]}") \
                from None


def make_crust() -> Crust:
    """The Crustdata client for this call: LiveCrust, or CacheOnlyCrust when
    CRUSTDATA_LIVE=0 or no API key is configured (the cache still works keyless)."""
    if not os.environ.get("CRUSTDATA_API_KEY"):
        os.environ["CRUSTDATA_API_KEY"] = "cache-only-demo"   # satisfies Crust.__init__
    if crust_live() and os.environ.get("CRUSTDATA_API_KEY") != "cache-only-demo":
        return LiveCrust()
    return CacheOnlyCrust()


_crust = make_crust


def _crust_stats(c: Crust) -> dict:
    return {"live": bool(getattr(c, "live", False)), **c.stats()}


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


def _lookup_failed(name: str, c: Crust, err: str) -> FriendlyError:
    """A resolve that missed the cache, explained for the mode we're in."""
    if not getattr(c, "live", False):
        return _not_preloaded(name)
    hint = f" Try {DEMO_FOUNDER}." if DEMO_FOUNDER and name != DEMO_FOUNDER else ""
    if _credit_error(err) or "after a credit error" in err:
        return FriendlyError(f"Live lookups are paused (the data provider's credit limit was "
                             f"hit) and there's no saved work history for “{name}”.{hint}")
    return FriendlyError(f"Couldn't reach the data provider to look up “{name}”, and there's "
                         f"no saved work history for them. Try again in a minute.{hint}")


def _person_id(v: Any) -> int | None:
    try:
        n = int(str(v).strip())
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _company_match(want: str, have: str) -> bool:
    """Whole-word company match: 'Stripe' ~ 'Stripe, Inc.'; 'AI' never ~ 'Airtable'.
    Names under 3 characters must match exactly."""
    want, have = _norm(want), _norm(have)
    if not want or not have:
        return False
    if want == have:
        return True
    short, long_ = sorted((want, have), key=len)
    return len(short) >= 3 and re.search(rf"(?<!\w){re.escape(short)}(?!\w)", long_) is not None


def _worked_at(profile: dict, company: str) -> bool:
    """True if `company` appears in the profile's employment history."""
    return any(_company_match(company, t.company) for t in _merge_companies(profile))


def _same_person_name(typed: str, found: str | None) -> bool:
    """Loose check that a pinned profile is the person whose name was typed."""
    a, b = _norm(typed), _norm(found)
    if not a or not b:
        return True
    if a == b or difflib.SequenceMatcher(None, a, b).ratio() >= 0.6:
        return True
    return bool(set(a.split()) & set(b.split()))


def resolve_founder_sync(name: str, person_id: Any = None, company: str = "",
                         prefer_id: Any = None) -> tuple[dict, dict]:
    """Crustdata person/search -> (profile, crustdata stats + how it matched).

    By name (fuzzy), or pinned to an exact crustdata_person_id — e.g. a candidate
    picked from POST /search, so a common name can't resolve to the wrong person (a
    pin whose profile clearly isn't the typed name is ignored: a stale UI pick).
    With a company and a first hit that never worked there, the top 5 name matches
    are checked for one that did (a common name -> the right person). The first,
    limit=1 request is unchanged, so cached founders still resolve cache-only.
    `prefer_id` (a resume) returns the person resolved the first time, never a
    namesake that a now-working lookup would pick instead.
    Cache hit = free; live mode looks up anyone else (CRUSTDATA_LIVE=1)."""
    name = canonical_founder(name)
    pid = _person_id(person_id)
    want = _person_id(prefer_id)
    c = _crust()
    by_name = {"field": "basic_profile.name", "type": "(.)", "value": name}
    match: dict = {"by": "person_id" if pid else "name"}
    profiles: list = []
    if pid:
        try:
            profiles = c.search_people([{"field": "crustdata_person_id", "type": "=",
                                         "value": pid}], limit=1).get("profiles") or []
        except CrustError as e:   # pin unavailable (cache-only miss / outage): try the name
            _log(f"founder_person_id {pid} lookup failed ({str(e)[:120]}); trying the name")
        if profiles and not _same_person_name(name, _name(profiles[0])):
            _log(f"founder_person_id {pid} is {_name(profiles[0])!r}, not {name!r}; "
                 f"resolving by name instead")
            profiles = []
        if not profiles:
            pid, match = None, {"by": "name", "pin_ignored": True}
    if not profiles:
        try:
            profiles = c.search_people([by_name], limit=1).get("profiles") or []
        except CrustError as e:
            raise _lookup_failed(name, c, str(e)) from None
    if not profiles:
        if not getattr(c, "live", False):
            raise _not_preloaded(name)
        raise FriendlyError(f"Couldn't find a work history for “{name}”. Check the spelling "
                            f"or use their full name.")
    profile = profiles[0]
    if want and profile.get("crustdata_person_id") == want:
        return profile, {**_crust_stats(c), "match": {**match, "resumed": True}}
    company = (company or "").strip()
    if (company and not pid) or want:
        if company and not pid:
            match["company"] = company
            match["company_matched"] = _worked_at(profile, company)
        if want or not match.get("company_matched"):
            try:   # a cache miss in cache-only mode just keeps the first hit
                more = c.search_people([by_name], limit=5).get("profiles") or []
            except CrustError:
                more = []
            if want:
                hit = next((p for p in more if p.get("crustdata_person_id") == want), None)
                if hit is None:   # last resort: the exact person by id
                    try:
                        hit = next(iter(c.search_people(
                            [{"field": "crustdata_person_id", "type": "=", "value": want}],
                            limit=1).get("profiles") or []), None)
                    except CrustError:
                        hit = None
                if hit is not None:
                    return hit, {**_crust_stats(c), "match": {**match, "resumed": True}}
            hit = next((p for p in more if _worked_at(p, company)), None) if company else None
            if hit is not None and not pid:
                profile = hit
                match.update(by="name+company", company_matched=True)
    return profile, {**_crust_stats(c), "match": match}


_REFS_SHOWN = 4   # the step-2 brief shows the top referee + 3 others


def ref_real_contacts() -> bool:
    """BACKCHANNEL_REF_CONTACTS: "real" (default) = contact-enrich the shown referees;
    "placeholder" = salted placeholders only. A real contact is the same masked string
    as that person's own candidate card, so it can link an anonymous referee to a name."""
    return os.environ.get("BACKCHANNEL_REF_CONTACTS", "real").strip().lower() != "placeholder"


def find_refs_sync(profile: dict) -> tuple[list[dict], dict]:
    """Ranked referees; the ones the UI shows also get their masked contact (in place)."""
    c = _crust()
    ranked, meta = find_offlist_references(c, profile, max_companies=3, pool_per_company=10)
    if ref_real_contacts():
        shown = ranked[:_REFS_SHOWN]
        got, meta["contacts"] = contacts.display(c, shown, _ref_placeholder,
                                                 id_key="crustdata_person_id",
                                                 url_key="linkedin_url")
        for r, ct in zip(shown, got):
            r.update(ct)
    meta["crustdata"] = _crust_stats(c)
    return ranked, meta


def synthesize_sync(candidate: dict, referee: dict, transcript: dict,
                    role: str | None = None) -> dict:
    return report_mod.synthesize(candidate, referee, transcript, role=role)


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
        "person_id": profile.get("crustdata_person_id"),
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


# Referee placeholder phones are seeded with a secret, per-process salt: a candidate's
# placeholder is derived from the same Crustdata id, so an unsalted seed would print the
# same digits on the referee card and on that person's NAMED candidate card. (A REAL
# contact can't be salted: BACKCHANNEL_REF_CONTACTS=placeholder keeps referees on these.)
_REF_SALT = secrets.token_hex(8)


def _ref_placeholder(r: dict) -> dict:
    """A referee's masked placeholder email/phone (salted, see _REF_SALT)."""
    who = r.get("crustdata_person_id") or f"{r.get('name') or r.get('title')}|" \
                                          f"{r.get('company')}|{r.get('overlap_window')}"
    return placeholder_contacts.for_person(r.get("name") or "ref", r.get("company"),
                                           f"ref|{_REF_SALT}|{who}", masked_email=True)


def _ref_contact(r: dict) -> dict:
    """DISPLAY-ONLY masked email/phone for a referee (never dialed — the reference call
    always dials REF_PHONE): the contact find_refs_sync attached (real contact-enrich,
    else a placeholder), else a placeholder. A resumed run's referee already carries
    it (persisted in the step-2 brief, so a restart's new salt doesn't change it)."""
    if r.get("email") and r.get("phone"):
        out = {"email": r["email"], "phone": r["phone"]}
        if r.get("contact_source"):
            out["contact_source"] = r["contact_source"]
        return out
    return {**_ref_placeholder(r), "contact_source": {"email": "placeholder",
                                                      "phone": "placeholder"}}


def _referee_brief(r: dict) -> dict:
    return {
        "display": _ref_display(r),
        "name": r.get("name"),
        "title": r.get("title"),
        "company": r.get("company"),
        "overlap_window": r.get("overlap_window"),
        "overlap_months": r.get("overlap_months"),
        "seniority": r.get("seniority"),
        "seniority_weight": r.get("seniority_weight"),
        "score": r.get("score"),
        **_ref_contact(r),
    }


def _referee_from_brief(b: dict) -> dict:
    """A resumed run's referee, rebuilt from its persisted step-2 brief (no name —
    nothing downstream needs it: synthesis and every call use role + company)."""
    return {"name": None, "linkedin_url": None, "title": b.get("title"),
            "company": b.get("company"), "overlap_window": b.get("overlap_window"),
            "overlap_months": b.get("overlap_months"), "seniority": b.get("seniority"),
            "seniority_weight": b.get("seniority_weight") or 1.0, "score": b.get("score"),
            "email": b.get("email"), "phone": b.get("phone"),   # display-only, never dialed
            "contact_source": b.get("contact_source")}


def _referee_for_synthesis(r: dict) -> dict:
    """The referee as the LLM sees it — anonymized so no name lands in the report
    (and no placeholder contact either)."""
    keep = {k: v for k, v in r.items() if k not in ("email", "phone", "contact_source")}
    return {**keep, "name": _ref_display(r), "linkedin_url": None}


def _intro_email(vc_name: str, founder: dict, referee: dict, verdict: dict,
                 meeting_time: str | None = None) -> str:
    """The VC's note to the founder. With a booked meeting it confirms the time
    instead of asking for one; without, the wording is exactly Phase 1's."""
    me = (vc_name or "").strip()
    if meeting_time:
        meeting_time = meeting_time.replace(" ", " at ", 1)  # "Thursday at 3:00 PM"
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
                + (f"Looking forward to our call on {meeting_time} — I'd love to hear "
                   f"what you're building and where I might be able to help."
                   if meeting_time else
                   f"I'd love to grab 30 minutes to hear what you're building and where I "
                   f"might be able to help. Are you free sometime this week?"))
        subject = f"Quick intro — {me or 'an investor'} <> {full}"
    elif v == "LEAN":
        body = (f"{intro}I've been doing some background work ahead of a potential "
                f"investment, including a conversation with one of your former "
                f"colleagues at {refco}.\n\n"
                + (f"I'd like to understand more about what you're building before going "
                   f"further — looking forward to our call on {meeting_time}."
                   if meeting_time else
                   f"I'd like to understand more about what you're building before going "
                   f"further. Could we find 30 minutes in the next couple of weeks?"))
        subject = f"Intro — {me or 'an investor'} <> {full}"
    elif meeting_time:
        body = (f"{intro}I've been doing some background work ahead of a potential "
                f"investment and still have a few open questions — looking forward to "
                f"our call on {meeting_time}.")
        subject = f"Intro — {me or 'an investor'} <> {full}"
    else:
        body = (f"{intro}Thanks for the time so far. After some background work I don't "
                f"think this is the right fit for me at this stage, but I'd be glad to "
                f"stay in touch as things progress.")
        subject = "Staying in touch"
    return f"Subject: {subject}\n\nHi {first},\n\n{body}\n\n{sign}"


_LEGACY_MEETING = {
    "status": "phase2",
    "note": "The call-back to you + founder call + meeting booking land in "
            "Phase 2 (steps 5-6). Phase 1 ends at a verified verdict.",
}


_NO_EMAIL = {"sent": False, "to": None, "status": "skipped",
             "reason": "No meeting was booked, so no invite was sent."}


def _build_result(run: dict, rep: dict, verdict: dict,
                  founder: dict, referee: dict, meeting: dict | None = None,
                  email: dict | None = None) -> dict:
    """meeting=None -> a run that started before Phase 2 (legacy phase2 stubs)."""
    mode = run.get("call_mode") or "mock"
    booked_time = (meeting or {}).get("time") if (meeting or {}).get("status") == "booked" else None
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
        "intro_email": _intro_email(run["inputs"]["vc_name"], founder, referee, verdict,
                                    booked_time),
        "meeting": meeting if meeting is not None else dict(_LEGACY_MEETING),
        "role": (run["inputs"].get("role") or "").strip() or None,
        "role_fit": rep.get("role_fit"),
        "email": email if email is not None else dict(_NO_EMAIL),
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


def _presaved_report(candidate: dict, referee: dict, role: str | None = None) -> dict:
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
        "role": role or None,
        "role_fit": ({"fit": "unclear",
                      "rationale": "Pre-saved report — fit for this role wasn't assessed "
                                   "automatically. Read the call before deciding."}
                     if role else None),
        "provenance": {
            "evidence": "employment-overlap graph (cached Crustdata)",
            "overlap_window": window,
            "overlap_months": months,
            "transcript_channel": "simulated",
            "synthesis": "pre-saved report (live synthesis slow or unavailable)",
            "scorer": "pre-saved",
        },
    }


def _unavailable_report(candidate: dict, referee: dict, role: str | None = None) -> dict:
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
        "role": role or None,
        "role_fit": ({"fit": "unclear", "rationale": "Automatic analysis didn't finish — "
                      "judge fit for this role from the transcript."} if role else None),
        "provenance": {"synthesis": "unavailable (gateway slow or down)"},
    }


async def _synthesize(mode: str, candidate: dict, referee: dict, transcript: dict,
                      role: str | None = None) -> dict:
    """Step 4 with a hard deadline. Mock mode can never hang or show 50/50/50 bars."""
    rep: dict | None
    try:
        rep = await asyncio.wait_for(
            asyncio.to_thread(synthesize_sync, candidate, referee, transcript, role),
            timeout=SYNTH_TIMEOUT_S,
        )
    except Exception as e:  # noqa: BLE001 — timeout, gateway transport, anything
        _log(f"synthesis failed ({type(e).__name__}: {e}); using fallback")
        rep = None

    if mode != "live":
        if rep is None or rep.get("synthesis_mode") == "fallback" or _scores_missing(rep):
            return _presaved_report(candidate, referee, role)
        return rep

    if rep is None:
        return _unavailable_report(candidate, referee, role)
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
              result: Any = None, execution_id: str | None = None,
              phase2: bool | None = None) -> None:
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
        "phase2": bool(info.get("phase2")) if phase2 is None else phase2,
    })


def _emit_banner(run_id: str, text: str, active: bool) -> None:
    _emit(run_id, {"type": "banner", "text": text, "active": active})


def _first(name: str | None, default: str) -> str:
    return ((name or "").strip().split(" ") or [""])[0] or default


def _party(step: str, founder_name: str | None = None) -> dict:
    """How a call's callee is named in failure messages."""
    if step == "call_vc":
        return {"who": "You", "whose": "Your"}
    if step == "call_founder":
        f = _first(founder_name, "The founder")
        return {"who": f, "whose": f"{f}'s"}
    return {"who": "The reference", "whose": "The reference's"}


def _not_placed_error(step: str, founder_name: str | None = None,
                      attempted: bool = True) -> FriendlyError:
    """A resumed live run reached a call it may not place (resume never dials)."""
    if step == "call_reference":
        return FriendlyError(
            "This check was interrupted before the call was confirmed, so it wasn't "
            "redialed automatically. Start over to place a new call.")
    callee = "you" if step == "call_vc" else _first(founder_name, "the founder")
    if not attempted:
        return FriendlyError(
            f"The server restarted before the call to {callee} was placed, and a resumed "
            f"check never dials on its own. Start over to run the calls again.")
    return FriendlyError(
        f"This check was interrupted before the call to {callee} was confirmed, so it "
        f"wasn't dialed automatically. Start over to run the calls again.")


# ---------------------------------------------------------------------------
# the reference call (step 3) — mock streams a canned transcript; live polls Bolna
# ---------------------------------------------------------------------------
async def _reference_call(run_id: str, referee: dict, prior_step: dict, mode: str,
                          founder_name: str, fresh: bool, role: str = "") -> dict:
    """Return a transcript dict {"qa":[...], "signals":{...}}.

    Streams transcript turns to the UI as they arrive. A resumed live run only
    ever POLLS its stored execution_id; it never places a new call. `role` steers
    the questions: live -> Bolna user_data ({{role}} etc. in a role-aware agent's
    prompt); mock -> the simulated script asks about fit for the role."""
    display = _ref_display(referee)
    simulated = mode != "live"
    prior_exec = prior_step.get("execution_id")
    if mode == "live" and not prior_exec and not fresh:
        raise _not_placed_error("call_reference")

    base_result = {
        "referee": display,
        "referee_title": referee.get("title"),
        "company": referee.get("company"),
        "mode": mode,
        "simulated": simulated,
        "ringing": True,
        "turns": [],
    }
    if role:
        base_result["role"] = role
    label = f"Simulated call with {display}" if simulated else f"Calling {display} to verify"
    _set_step(run_id, "call_reference", "active", label=label, result=base_result)
    _emit_banner(run_id, f"\U0001F4DE Ringing {display}…"
                 + (" (simulated)" if simulated else ""), True)

    if mode == "live":
        # Dials REF_PHONE (the controlled number) — NEVER referee["phone"], which is
        # masked and display-only (a real contact-enrich number or a placeholder).
        transcript = await _live_call(run_id, "call_reference", prior_exec, base_result,
                                      agent_id=BOLNA_AGENT_ID, phone=REF_PHONE,
                                      party=_party("call_reference"),
                                      user_data=(_reference_user_data(founder_name, referee,
                                                                      role)
                                                 if BOLNA_AGENT_ID != bolna.DEFAULT_AGENT_ID
                                                 else None))
    else:
        qa, signals = _load_mock_transcript(founder_name, role)
        transcript = await _mock_call(run_id, "call_reference", base_result, qa, signals)

    _emit_banner(run_id, "", False)
    return transcript


def _connected_banner(base_result: dict) -> str:
    if base_result.get("banner_connected"):
        return base_result["banner_connected"]
    tail = " · simulated call" if base_result.get("simulated") else ""
    return f"Connected to {base_result.get('referee') or 'the reference'}{tail}"


def _reference_user_data(founder_name: str, referee: dict, role: str) -> dict:
    """Bolna user_data for the reference call: fills {{placeholders}} in a role-aware
    reference agent (setup_agents.py). An agent without placeholders ignores it."""
    full = (founder_name or "").strip() or "the candidate"
    return {
        "candidate_name": full,
        "candidate_first": _first(full, "the candidate"),
        "referee_name": _first(referee.get("name") or "", "there"),
        "role": role or "a senior role",
        "company": referee.get("company") or "a previous company",
    }


def _load_mock_transcript(founder_name: str, role: str = "") -> tuple[list[dict], dict]:
    """The fictional reference-call script. With a role, the agent opens with it and
    asks one fit-for-the-role question before the closing would-you-work-again."""
    data = json.loads(SAMPLE_TRANSCRIPT.read_text())
    first = (founder_name or "").split(" ")[0] or "them"
    values = {"first_name": first, "role": role}
    qa = [{"q": _fill(t.get("q") or "", values), "a": _fill(t.get("a") or "", values)}
          for t in data.get("qa") or []]
    turn = data.get("role_turn") or {}
    if role and qa and turn.get("q"):
        lead = _fill(turn.get("opening") or "", values)
        if lead:
            qa[0] = {**qa[0], "q": f"{lead} {qa[0]['q']}"}
        qa.insert(max(0, len(qa) - 1), {"q": _fill(turn["q"], values),
                                        "a": _fill(turn.get("a") or "", values)})
    return qa, data.get("signals") or {}


def _fill(text: str, values: dict[str, str]) -> str:
    for k, v in values.items():
        text = text.replace("{" + k + "}", v)
    return text


def _load_sample(path: Path, values: dict[str, str]) -> tuple[list[dict], dict]:
    """A Phase-2 mock call script with its {placeholders} filled from this run."""
    data = json.loads(path.read_text())
    qa = [{"q": _fill(t.get("q") or "", values), "a": _fill(t.get("a") or "", values)}
          for t in data.get("qa") or []]
    return qa, data.get("expected") or {}


async def _mock_call(run_id: str, step: str, base_result: dict, qa: list[dict],
                     signals: dict) -> dict:
    await asyncio.sleep(1.4)  # "ringing"
    base_result = {**base_result, "ringing": False, "connected": True}
    db.upsert_step(run_id, step, result=base_result)
    _emit_banner(run_id, _connected_banner(base_result), True)
    turns: list[dict] = []
    for turn in qa:
        turns.append(turn)
        res = {**base_result, "turns": list(turns)}
        db.upsert_step(run_id, step, result=res)
        _emit(run_id, {"type": "transcript", "step": step,
                       "turns": list(turns), "partial": True})
        await asyncio.sleep(0.9)
    return {"qa": turns, "signals": signals}


async def _live_call(run_id: str, step: str, prior_exec: str | None, base_result: dict, *,
                     agent_id: str, phone: str, party: dict,
                     user_data: dict | None = None) -> dict:
    # Idempotency: reuse a stored execution_id (resume) instead of re-dialing.
    exec_id = prior_exec
    if not exec_id:
        if not agent_id:
            raise FriendlyError("Live calling isn't set up on this server (no voice agent "
                                "configured for this call).")
        if not phone:
            raise FriendlyError("Live calling isn't set up on this server (no "
                                + ("reference " if step == "call_reference" else "")
                                + "number configured).")
        # `phone` is always a configured/typed number (REF_PHONE, the VC's own number,
        # FOUNDER_PHONE). A display-only contact (real or placeholder) is masked; refuse
        # it outright so it can never reach bolna.place_call, whatever the allowlist says.
        if placeholder_contacts.is_placeholder_phone(phone):
            raise FriendlyError("Live calling only dials verified numbers, and this one "
                                "is a masked display number.")
        if bolna.normalize_phone(phone) not in _ALLOWED_NORM:
            raise FriendlyError("Live calling is limited to verified numbers, and the "
                                "configured number isn't on the allowlist.")
        try:
            resp = await asyncio.to_thread(bolna.place_call, agent_id,
                                           bolna.normalize_phone(phone), user_data=user_data)
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
        db.upsert_step(run_id, step, execution_id=exec_id,
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
                db.upsert_step(run_id, step, result=res, execution_id=exec_id)
                _emit_banner(run_id, _connected_banner(base_result), True)
            if len(turns) != last_n:
                last_n = len(turns)
                res = {**base_result, "ringing": False, "connected": True,
                       "execution_id": exec_id, "turns": list(turns),
                       "duration": ex.get("conversation_duration"),
                       "cost": ex.get("total_cost")}
                db.upsert_step(run_id, step, result=res, execution_id=exec_id)
                _emit(run_id, {"type": "transcript", "step": step,
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
        _log(f"live call {exec_id} ({step}) ended without transcript: status={status!r} "
             f"timed_out={timed_out}")
        if timed_out and not connected:
            mins = max(1, round(CALL_TIMEOUT_S / 60))
            raise FriendlyError(f"No answer after about {mins} minute{'s' if mins != 1 else ''}. "
                                f"Start over to try again.")
        raise FriendlyError(_CALL_END_MSG.get(
            status, "The call ended before any conversation was recorded.").format(**party))
    transcript.setdefault("signals", {})
    return transcript


# ---------------------------------------------------------------------------
# Phase 2 — call 2 (the VC) and call 3 (the founder)
# ---------------------------------------------------------------------------
_VERDICT_PHRASE = {
    "FUND": "a clear yes, this is someone you can back",
    "LEAN": "a cautious yes, worth a meeting with a few questions",
    "PASS": "a pass, real concerns came up",
}
# mock-only fallback if a sample has no `expected` slots. Live mode never invents
# slots: an unreadable VC call skips the founder call instead.
DEFAULT_SLOTS = ["Thursday afternoon", "Friday afternoon"]
# mock already knows the answer (its sample's `expected`), so don't wait long for Haiku
MOCK_EXTRACT_TIMEOUT_S = min(EXTRACT_TIMEOUT_S, 6.0)
_FLEXIBLE_PHRASE = "flexible this week"


def _clause(s: str | None) -> str:
    """A report bullet as a mid-sentence clause: no trailing period, lower-case
    first letter unless it starts an acronym/proper noun ('SaaS', 'APAC')."""
    t = (s or "").strip().rstrip(".").replace(": ", " — ")  # spoken after "stood out:"
    if len(t) > 1 and t[0].isupper() and not t[1].isupper():
        t = t[0].lower() + t[1:]
    return t


def _slots_phrase(slots: list[str]) -> str:
    if not slots:
        return _FLEXIBLE_PHRASE
    if len(slots) == 1:
        return slots[0]
    return ", ".join(slots[:-1]) + " or " + slots[-1]


def _vc_context(run: dict, founder: dict, referee: dict, verdict: dict, rep: dict) -> dict:
    """Everything the VC call needs: mock-script placeholders + Bolna user_data."""
    highlights = [h for h in ((rep.get("strengths") or []) + (rep.get("claims_confirmed") or []))
                  if str(h).strip()]
    concerns = [c for c in (rep.get("concerns") or []) if str(c).strip()]
    v = verdict.get("verdict") or "LEAN"
    founder_name = founder.get("name") or "the founder"
    founder_first = _first(founder_name, "the founder")
    months = referee.get("overlap_months")
    h1 = _clause(highlights[0]) if highlights else "real ownership of their work"
    h2 = _clause(highlights[1]) if len(highlights) > 1 else "they'd work with them again"
    concern_line = (f"One watch-out: {_clause(concerns[0])}. Nothing they called disqualifying."
                    if concerns else "They didn't raise any real red flags.")
    mock = {
        "vc_first": _first(run["inputs"].get("vc_name"), "there"),
        "founder_first": founder_first,
        "overlap_months": str(months) if months else "several",
        "verdict_phrase": _VERDICT_PHRASE.get(v, _VERDICT_PHRASE["LEAN"]),
        "highlight_1": h1,
        "highlight_2": h2,
        "vc_reaction": "Hm, okay." if v == "PASS" else "That's encouraging.",
        "concern_line": concern_line,
    }
    # Bolna fills {{key}} in the VC agent's prompt + welcome message (setup_agents.py)
    user_data = {
        "vc_name": mock["vc_first"],
        "founder_name": founder_name,
        "founder_first": founder_first,
        "verdict": f"{v} — {mock['verdict_phrase']}",
        "confidence": str(verdict.get("confidence") or "medium"),
        "highlights": "; ".join(_clause(h) for h in highlights[:3]) or h1,
        "concern": _clause(concerns[0]) if concerns else "none raised",
        "referee_role": _ref_display(referee),
        "overlap": f"{months} months" if months else "a long stretch",
        "role": (run["inputs"].get("role") or "").strip() or "a potential investment",
    }
    return {"mock": mock, "user_data": user_data, "founder_first": founder_first}


def _pitch(verdict: dict) -> tuple[str, str]:
    """How the founder call frames the investor's interest: (mock sentence, Bolna clause).
    A PASS that the VC still wants to discuss is a meeting, not a funding offer."""
    if verdict.get("verdict") == "PASS":
        return ("They'd like to set up a meeting to learn more about what you're building.",
                "would like to set up a meeting to learn more about what you're building")
    return ("They'd like to fund what you're building and set up a meeting.",
            "is interested in funding you and would like to set up a meeting")


async def _extract_slots(mode: str, turns: list[dict], fallback: dict) -> dict:
    """VC transcript -> {"wants_meeting","slots","source","assumed","flexible","unreadable"}.

    Hard deadline. Live mode never makes slots up:
      * Haiku read it: declined -> no meeting; slots -> firm; "yes, but no day" ->
        flexible (the founder picks; the booking stays tentative).
      * Haiku unavailable: the regex read is a best guess (tentative); if it finds
        no weekday the call is unreadable and the founder is NOT called (the VC may
        have declined)."""
    deadline = EXTRACT_TIMEOUT_S if mode == "live" else MOCK_EXTRACT_TIMEOUT_S
    res: dict | None
    try:
        res = await asyncio.wait_for(
            asyncio.to_thread(scheduling.extract_vc_slots, turns,
                              timeout=max(3.0, deadline - 2)),
            timeout=deadline)
    except Exception as e:  # noqa: BLE001 — timeout, gateway transport, anything
        _log(f"slot extraction failed ({type(e).__name__}: {e}); using fallback")
        res = None
    out = {"wants_meeting": True, "slots": [], "source": None, "assumed": False,
           "flexible": False, "unreadable": False}
    if mode != "live":
        if res is None or not res.get("slots"):
            return {**out, "slots": list(fallback.get("slots") or DEFAULT_SLOTS),
                    "source": "sample"}
        return {**out, "slots": res["slots"], "source": res["source"]}
    if res is None:
        res = scheduling.heuristic_slots(turns)
    if res.get("source") == "haiku":
        if not res.get("wants_meeting"):
            return {**out, "wants_meeting": False, "source": "haiku"}
        if res.get("slots"):
            return {**out, "slots": res["slots"], "source": "haiku"}
        return {**out, "source": "haiku", "assumed": True, "flexible": True}
    if res.get("slots"):
        return {**out, "slots": res["slots"], "source": "heuristic", "assumed": True}
    return {**out, "wants_meeting": False, "source": "heuristic", "assumed": True,
            "unreadable": True}


async def _extract_time(mode: str, turns: list[dict], slots: list[str],
                        fallback: dict) -> dict:
    """Founder transcript -> {"confirmed","time","source","assumed"}, hard deadline.
    Live: only a Haiku-read agreement counts as firm; a regex read is a best guess."""
    deadline = EXTRACT_TIMEOUT_S if mode == "live" else MOCK_EXTRACT_TIMEOUT_S
    res: dict | None
    try:
        res = await asyncio.wait_for(
            asyncio.to_thread(scheduling.extract_confirmed_time, turns, slots,
                              timeout=max(3.0, deadline - 2)),
            timeout=deadline)
    except Exception as e:  # noqa: BLE001
        _log(f"time extraction failed ({type(e).__name__}: {e}); using fallback")
        res = None
    if mode != "live":
        if res is None or not res.get("confirmed"):
            t = fallback.get("time") or scheduling.default_time_for(slots[0] if slots else None)
            return {"confirmed": True, "time": t, "source": "sample", "assumed": False}
        return {**res, "assumed": False}
    if res is None:
        res = scheduling.heuristic_time(turns)
    if not res.get("confirmed"):
        # nothing firm on the call: propose the first VC slot (if any), tentatively
        return {"confirmed": False, "source": res.get("source") or "heuristic", "assumed": True,
                "time": scheduling.default_time_for(slots[0] if slots else None)}
    return {**res, "assumed": res.get("source") != "haiku"}


async def _vc_leg(run_id: str, run: dict, prior_step: dict, mode: str, fresh: bool,
                  founder: dict, referee: dict, verdict: dict, rep: dict) -> dict:
    """Step 5: call the VC with the verdict, get their free slots.

    Returns {"ok", "wants_meeting", "slots", "slots_source", "assumed", "flexible",
    "unreadable", "error"}."""
    step = "call_vc"
    status = prior_step.get("status")
    if status == "done":
        r = prior_step.get("result") or {}
        return {"ok": True, "wants_meeting": bool(r.get("wants_meeting", True)),
                "slots": list(r.get("slots") or []), "slots_source": r.get("slots_source"),
                "assumed": bool(r.get("assumed")), "flexible": bool(r.get("flexible")),
                "unreadable": bool(r.get("unreadable"))}
    if status == "error":  # failed before a restart: never re-dial, just carry on
        return {"ok": False, "error": (prior_step.get("result") or {}).get("error")
                or _STEP_FAIL_MSG[step]}

    simulated = mode != "live"
    ctx = _vc_context(run, founder, referee, verdict, rep)
    founder_first = ctx["founder_first"]
    sim = " (simulated)" if simulated else ""
    base_result = {
        "callee": "You",
        "mode": mode,
        "simulated": simulated,
        "ringing": True,
        "turns": [],
        "banner_ringing": "\U0001F4DE Your phone is ringing — the agent has the verdict" + sim,
        "banner_connected": "On the line with you" + (" · simulated call" if simulated else ""),
    }
    try:
        prior_exec = prior_step.get("execution_id")
        if mode == "live" and not prior_exec and not fresh:
            raise _not_placed_error(step, attempted=status == "active")
        label = ("Simulated call to you with the verdict" if simulated
                 else "Calling you with the verdict")
        _set_step(run_id, step, "active", label=label, result=base_result)
        _emit_banner(run_id, base_result["banner_ringing"], True)
        if mode == "live":
            transcript = await _live_call(
                run_id, step, prior_exec, base_result, agent_id=VC_AGENT_ID,
                phone=run["inputs"].get("vc_phone") or "", party=_party(step),
                user_data=ctx["user_data"])
            expected: dict = {}
        else:
            qa, expected = _load_sample(VC_SAMPLE, ctx["mock"])
            transcript = await _mock_call(run_id, step, base_result, qa, {})
        _emit_banner(run_id, "", False)
        turns = transcript["qa"]
        done_call = {**base_result, "ringing": False, "connected": True, "turns": turns,
                     "n_turns": len(turns)}
        exec_id = prior_exec or db_exec(run_id, step)
        if exec_id:
            done_call["execution_id"] = exec_id
        _set_step(run_id, step, "active",
                  result={**done_call, "summary": "Reading your availability off the call…"})
        slots = await _extract_slots(mode, turns, expected)
    except Exception as e:  # noqa: BLE001 — a failed call keeps the verdict
        _emit_banner(run_id, "", False)
        msg = str(e) if isinstance(e, FriendlyError) else _STEP_FAIL_MSG[step]
        _log(f"run {run_id} {step} failed: {type(e).__name__}: {e}")
        _set_step(run_id, step, "error",
                  result={"error": msg, "detail": f"{type(e).__name__}: {e}"[:300]})
        return {"ok": False, "error": msg}

    if slots["unreadable"]:
        summary = "Couldn't read your availability off the call"
    elif not slots["wants_meeting"]:
        summary = "You didn't want a meeting"
    elif slots["flexible"]:
        summary = f"No firm slot from you · {founder_first} will propose a time"
    else:
        summary = "You're free " + _slots_phrase(slots["slots"])
        if slots["assumed"]:
            summary += " (best guess from the call)"
    _set_step(run_id, step, "done", result={
        **done_call,
        "wants_meeting": slots["wants_meeting"],
        "slots": slots["slots"],
        "slots_source": slots["source"],
        "assumed": slots["assumed"],
        "flexible": slots["flexible"],
        "unreadable": slots["unreadable"],
        "summary": summary + (" · simulated call" if simulated else ""),
    })
    return {"ok": True, "wants_meeting": slots["wants_meeting"], "slots": slots["slots"],
            "slots_source": slots["source"], "assumed": slots["assumed"],
            "flexible": slots["flexible"], "unreadable": slots["unreadable"]}


def db_exec(run_id: str, step: str) -> str | None:
    """The execution_id stored on a step (set the instant a live call is placed)."""
    row = next((s for s in (db.get_run(run_id) or {}).get("steps", []) if s["step"] == step),
               None)
    return row["execution_id"] if row else None


def _not_booked(founder: dict, vc: dict, mode: str, status: str, note: str) -> dict:
    return {"status": status, "time": None, "with": founder.get("name"),
            "vc_slots": vc.get("slots") or [], "matched_slot": None,
            "simulated": mode != "live", "note": note}


def _skip_founder(run_id: str, meeting: dict, summary: str) -> dict:
    _set_step(run_id, "call_founder", "skipped",
              result={"skipped": True, "meeting": meeting, "summary": summary})
    return meeting


async def _founder_leg(run_id: str, run: dict, prior_step: dict, mode: str, fresh: bool,
                       founder: dict, vc: dict, verdict: dict) -> dict:
    """Step 6: call the founder, match them to one of the VC's slots, confirm a time.

    Returns the result's `meeting` dict (status booked | tentative | declined |
    not_booked)."""
    step = "call_founder"
    status = prior_step.get("status")
    if status in ("done", "skipped") and (prior_step.get("result") or {}).get("meeting"):
        return prior_step["result"]["meeting"]
    founder_name = founder.get("name") or "the founder"
    founder_first = _first(founder_name, "the founder")
    if status == "error":
        return _not_booked(founder, vc, mode, "not_booked",
                           (prior_step.get("result") or {}).get("error") or _STEP_FAIL_MSG[step])

    if not vc.get("ok"):
        return _skip_founder(run_id, _not_booked(
            founder, vc, mode, "not_booked",
            f"The call to you didn't go through, so {founder_first} wasn't called."),
            "Skipped — the call to you didn't go through")
    if vc.get("unreadable"):
        return _skip_founder(run_id, _not_booked(
            founder, vc, mode, "not_booked",
            f"Your availability couldn't be read off the call, so {founder_first} wasn't "
            f"called. Check the transcript and follow up by email."),
            "Skipped — your availability couldn't be read")
    if not vc.get("wants_meeting"):
        return _skip_founder(run_id, _not_booked(
            founder, vc, mode, "declined",
            f"You didn't ask for a meeting on the call, so {founder_first} wasn't called."),
            "Skipped — no meeting requested")

    simulated = mode != "live"
    slots = list(vc.get("slots") or [])
    vc_name = (run["inputs"].get("vc_name") or "").strip()
    company = ((run["inputs"].get("founder_company") or "").strip()
               or next((c["company"] for c in founder.get("companies") or [] if c.get("current")), ""))
    pitch_sentence, pitch_clause = _pitch(verdict)
    sim = " (simulated)" if simulated else ""
    base_result = {
        "callee": founder_first,
        "mode": mode,
        "simulated": simulated,
        "ringing": True,
        "turns": [],
        "vc_slots": slots,
        "banner_ringing": f"\U0001F4DE Ringing {founder_first}…{sim}",
        "banner_connected": f"Connected to {founder_first}"
                            + (" · simulated call" if simulated else ""),
    }
    try:
        prior_exec = prior_step.get("execution_id")
        if mode == "live" and not prior_exec and not fresh:
            raise _not_placed_error(step, founder_name, attempted=status == "active")
        label = (f"Simulated call with {founder_first} to book the meeting" if simulated
                 else f"Calling {founder_first} to book the meeting")
        _set_step(run_id, step, "active", label=label, result=base_result)
        _emit_banner(run_id, base_result["banner_ringing"], True)
        if mode == "live":
            # Dials FOUNDER_PHONE (the controlled number) — never a candidate's or
            # referee's display-only placeholder `phone`.
            transcript = await _live_call(
                run_id, step, prior_exec, base_result, agent_id=FOUNDER_AGENT_ID,
                phone=FOUNDER_PHONE, party=_party(step, founder_name),
                user_data={
                    "founder_name": founder_first,
                    "vc_name": vc_name or "an investor",
                    "vc_slots": _slots_phrase(slots),
                    "company": company or "your company",
                    "pitch": pitch_clause,
                })
            expected: dict = {}
        else:
            qa, expected = _load_sample(FOUNDER_SAMPLE, {
                "founder_first": founder_first,
                "vc_display": f"{vc_name}, an investor" if vc_name else "an investor",
                "vc_short": _first(vc_name, "them"),
                "vc_slots_phrase": _slots_phrase(slots),
                "pitch_sentence": pitch_sentence,
            })
            transcript = await _mock_call(run_id, step, base_result, qa, {})
        _emit_banner(run_id, "", False)
        turns = transcript["qa"]
        done_call = {**base_result, "ringing": False, "connected": True, "turns": turns,
                     "n_turns": len(turns)}
        exec_id = prior_exec or db_exec(run_id, step)
        if exec_id:
            done_call["execution_id"] = exec_id
        _set_step(run_id, step, "active",
                  result={**done_call, "summary": f"Matching {founder_first}'s time to your slots…"})
        got = await _extract_time(mode, turns, slots, expected)
    except Exception as e:  # noqa: BLE001 — a failed call keeps the verdict
        _emit_banner(run_id, "", False)
        msg = str(e) if isinstance(e, FriendlyError) else _STEP_FAIL_MSG[step]
        _log(f"run {run_id} {step} failed: {type(e).__name__}: {e}")
        _set_step(run_id, step, "error",
                  result={"error": msg, "detail": f"{type(e).__name__}: {e}"[:300]})
        return _not_booked(founder, vc, mode, "not_booked", msg)

    t = got.get("time")
    matched = scheduling.match_slot(t, slots)
    firm = bool(got.get("confirmed") and t and not got.get("assumed") and not vc.get("assumed"))
    if firm:
        status_m = "booked"
        note = (f"Matches your {matched} slot." if matched
                else f"{founder_first} picked a time outside the slots you gave — double-check it.")
        summary = f"Booked {t}" + (f" · in your {matched} slot" if matched else "")
    else:
        status_m = "tentative"
        if got.get("confirmed") and t and vc.get("flexible"):
            note = (f"You didn't give a firm slot on the call, so {founder_first} proposed "
                    f"{t}. Confirm it works for you.")
        elif got.get("confirmed") and t:
            note = (f"{founder_first} agreed to {t}, but the automatic read-out of the calls "
                    f"was unavailable. Check the transcripts and confirm.")
        else:
            note = (f"{founder_first} didn't lock in a time on the call; "
                    + (f"{t} is proposed — confirm by email." if t else "follow up by email."))
        summary = ("Tentative" + (f" · {t}" if t else " · no firm time yet"))
    meeting = {
        "status": status_m,
        "time": t,
        "with": founder_name,
        "vc_slots": slots,
        "matched_slot": matched,
        "simulated": simulated,
        "source": got.get("source"),
        "slots_assumed": bool(vc.get("assumed")),
        "note": note,
    }
    _set_step(run_id, step, "done", result={
        **done_call, "meeting": meeting,
        "summary": summary + (" · simulated call" if simulated else ""),
    })
    return meeting


# ---------------------------------------------------------------------------
# step 7 side effect — the founder's summary email + .ics invite (exactly once)
# ---------------------------------------------------------------------------
_EMAIL_FINAL = ("sent", "skipped", "failed")


def _mask_email(addr: str | None) -> str | None:
    """'jane.doe@acme.com' -> 'ja•••@acme.com' — run results are public."""
    if not addr or "@" not in addr:
        return addr
    local, _, domain = addr.partition("@")
    return f"{local[:2]}•••@{domain}"


def _email_public(rec: dict) -> dict:
    """The result's `email` field: {sent, to (masked), id?, status, ...}."""
    out = {"sent": rec.get("state") == "sent", "to": _mask_email(rec.get("to")),
           "status": rec.get("state")}
    for k in ("id", "subject", "simulated", "invite", "reason"):
        if rec.get(k) is not None:
            out[k] = rec[k]
    return out


async def _founder_email(run_id: str, run: dict, founder: dict, meeting: dict | None,
                         mode: str) -> dict:
    """Send the founder a summary email with an .ics invite for the booked meeting.

    Idempotent across resumes: the record is persisted on the run before and after
    the send. A finished record (sent / skipped / failed) is returned as-is — never
    re-sent. A record stuck in "sending" (crash mid-send) is retried with the same
    Idempotency-Key and the byte-identical payload, which Resend de-duplicates.
    Never raises: a failed email doesn't fail the run."""
    stored = db.get_email(run_id) or {}
    if stored.get("state") in _EMAIL_FINAL:
        if stored.get("state") == "sent":
            _log(f"run {run_id}: founder email already sent (id={stored.get('id')}); "
                 f"not re-sending")
        return _email_public(stored)

    def finish(rec: dict) -> dict:
        db.set_email(run_id, rec)
        return _email_public(rec)

    if not meeting or meeting.get("status") != "booked" or not meeting.get("time"):
        return finish({"state": "skipped", "to": None,
                       "reason": "No meeting was booked, so no invite was sent."})
    cfg = invite.config()
    if cfg is None:
        return finish({"state": "skipped", "to": None,
                       "reason": invite.unavailable_reason()})
    company = ((run["inputs"].get("founder_company") or "").strip()
               or next((c["company"] for c in founder.get("companies") or []
                        if c.get("current")), ""))
    try:
        composed = invite.compose(
            cfg=cfg, run_id=run_id, created_at=run.get("created_at"),
            meeting_time=meeting["time"], vc_name=run["inputs"].get("vc_name") or "",
            founder_name=founder.get("name") or meeting.get("with") or "",
            company=company, simulated=mode != "live")
    except Exception as e:  # noqa: BLE001 — a bad slot string must not fail the run
        _log(f"run {run_id}: couldn't build the founder email: {type(e).__name__}: {e}")
        return finish({"state": "failed", "to": cfg["to"],
                       "reason": "The invite email couldn't be prepared."})
    key = f"backchannel-{run_id}-founder-invite"
    attempts = int(stored.get("attempts") or 0) + 1
    db.set_email(run_id, {"state": "sending", **composed["summary"],
                          "idempotency_key": key, "attempts": attempts})
    res, err = None, None
    for i in range(2):   # an unknown outcome (timeout / dropped) is retried once, same key
        try:
            res = await asyncio.wait_for(
                asyncio.to_thread(invite.send, cfg, composed["payload"], key),
                timeout=EMAIL_TIMEOUT_S)
            break
        except Exception as e:  # noqa: BLE001 — timeout, Resend error, anything
            err = e
            _log(f"run {run_id}: founder email attempt {i + 1} failed "
                 f"({type(e).__name__}: {e})")
            unknown = (isinstance(e, asyncio.TimeoutError)
                       or (isinstance(e, invite.InviteError) and e.outcome_unknown))
            if not unknown:
                break
    if res is None:
        unknown = (isinstance(err, asyncio.TimeoutError)
                   or (isinstance(err, invite.InviteError) and err.outcome_unknown))
        return finish({"state": "failed", **composed["summary"], "attempts": attempts,
                       "reason": ("The invite email couldn't be confirmed — it may still "
                                  "arrive." if unknown else "The invite email couldn't be sent."),
                       "detail": f"{type(err).__name__}: {err}"[:300]})
    _log(f"run {run_id}: founder email sent to {cfg['to']} (id={res['id']}"
         + (f", retry {attempts}" if attempts > 1 else "") + ")")
    return finish({"state": "sent", **composed["summary"], "id": res["id"],
                   "attempts": attempts})


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
    person_id = run["inputs"].get("founder_person_id")
    founder_company = (run["inputs"].get("founder_company") or "").strip()
    role = (run["inputs"].get("role") or "").strip()
    current = "resolve_founder"
    # tiny grace so a just-redirected browser connects before step 1 goes active
    # (pure visual polish; the SSE snapshot already guarantees no state is lost)
    if prior["resolve_founder"]["status"] == "pending":
        await asyncio.sleep(0.5)
    try:
        # A resume reuses the founder + referee persisted by steps 1-2. Live lookups
        # can answer differently on a retry (a lookup that failed the first time may
        # now succeed), and the stored call transcript belongs to THAT referee.
        s1, s2 = prior["resolve_founder"], prior["find_references"]
        kept_founder = (s1.get("result") or {}).get("founder") if s1["status"] == "done" else None
        kept_top = (s2.get("result") or {}).get("top") if s2["status"] == "done" else None

        # --- Step 1: resolve_founder (live behind the cache) ---------------
        current = "resolve_founder"
        profile = None
        if kept_founder and kept_top:
            founder = kept_founder
        elif s1["status"] != "done":
            _set_step(run_id, "resolve_founder", "active")
            await asyncio.sleep(0.4)
            profile, cstats = await asyncio.to_thread(resolve_founder_sync, founder_name,
                                                      person_id, founder_company)
            founder = _companies_summary(profile)
            _set_step(run_id, "resolve_founder", "done",
                      result={"founder": founder, "summary": founder["summary"],
                              "crustdata": cstats})
        else:
            profile, _ = await asyncio.to_thread(
                resolve_founder_sync, founder_name, person_id, founder_company,
                (kept_founder or {}).get("person_id"))
            founder = kept_founder or _companies_summary(profile)

        # --- Step 2: find_references (live behind the cache) ---------------
        current = "find_references"
        if kept_top and kept_founder:
            top = _referee_from_brief(kept_top)
        else:
            _set_step(run_id, "find_references", "active")
            await asyncio.sleep(0.4)
            ranked, meta = await asyncio.to_thread(find_refs_sync, profile)
            if not ranked:
                raise FriendlyError(
                    f"No ex-colleagues with overlapping tenure were found for {founder['name']}.")
            top = ranked[0]
            _set_step(run_id, "find_references", "done", result={
                "n_found": len(ranked),
                "top": _referee_brief(top),
                "others": [_referee_brief(r) for r in ranked[1:_REFS_SHOWN]],
                "summary": f"{len(ranked)} found · top: {_ref_display(top)} "
                           f"({top.get('overlap_months')} mo overlap)",
                "crustdata": {**(meta.get("crustdata") or {}),
                              "companies": [{"company": c.get("company"),
                                             "status": c.get("status")}
                                            for c in meta.get("companies_searched") or []],
                              "contacts": meta.get("contacts")},
            })

        # --- Step 3: call_reference (THE side effect — idempotent on resume) -
        current = "call_reference"
        if prior["call_reference"]["status"] == "done":
            stored = prior["call_reference"]["result"] or {}
            transcript = {"qa": stored.get("turns") or [],
                          "signals": stored.get("signals") or {}}
        else:
            transcript = await _reference_call(run_id, top, prior["call_reference"],
                                               mode, founder["name"] or founder_name, fresh,
                                               role)
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
            rep = await _synthesize(mode, cand, _referee_for_synthesis(top), transcript, role)
            verdict = _verdict_from_report(rep)
            _set_step(run_id, "synthesize_signal", "done", result={
                "verdict": verdict["verdict"],
                "tier": verdict["tier"],
                "confidence": verdict["confidence"],
                "sub_scores": rep.get("sub_scores") or {},
                "summary": f"{verdict['verdict']} · {verdict['confidence']} confidence",
                "report": rep,
                "role_fit": rep.get("role_fit"),
            })

        meeting: dict | None = None
        if prior["call_vc"]["phase2"] or prior["call_founder"]["phase2"]:
            # --- a run created before Phase 2 shipped: keep its stubs ---------
            for stub in ("call_vc", "call_founder"):
                current = stub
                if prior[stub]["status"] != "phase2":
                    _set_step(run_id, stub, "phase2", phase2=True, result={
                        "note": "Phase 2 — not built for this demo."})
        else:
            # --- Step 5: call_vc (deliver the verdict, get the VC's slots) -----
            current = "call_vc"
            vc = await _vc_leg(run_id, run, prior["call_vc"], mode, fresh,
                               founder, top, verdict, rep)
            # --- Step 6: call_founder (match a slot, confirm the time) ---------
            current = "call_founder"
            meeting = await _founder_leg(run_id, run, prior["call_founder"], mode, fresh,
                                         founder, vc, verdict)

        # --- Step 7: done (+ the founder's summary email & .ics, once) -------
        current = "done"
        email = await _founder_email(run_id, run, founder, meeting, mode)
        result = _build_result(run, rep, verdict, founder, _referee_brief(top), meeting, email)
        if meeting and meeting.get("status") == "booked":
            done_label = "Meeting booked"
            done_summary = f"{verdict['verdict']} · meeting {meeting['time']} with " \
                           f"{_first(meeting.get('with'), 'the founder')}"
        else:
            done_label, done_summary = None, "Verdict ready"
        done_result = {"verdict": verdict["verdict"], "summary": done_summary,
                       "email_sent": bool(email.get("sent"))}
        if meeting is not None:
            done_result["meeting_status"] = meeting.get("status")
        _set_step(run_id, "done", "done", label=done_label, result=done_result)
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


def live_preflight(inputs: dict) -> str | None:
    """Live mode: refuse a run up front (before any call) if the 3-call chain can't
    complete — every number must be on the verified allowlist. None = good to go."""
    if not REF_PHONE:
        return "Live calling isn't set up on this server (no reference number configured)."
    if bolna.normalize_phone(REF_PHONE) not in _ALLOWED_NORM:
        return ("Live calling isn't fully set up on this server (the reference number "
                "isn't on the verified allowlist).")
    if not VC_AGENT_ID or not FOUNDER_AGENT_ID:
        return ("Live calling isn't fully set up on this server (the call-back and "
                "founder voice agents aren't configured).")
    vc = bolna.normalize_phone(inputs.get("vc_phone"))
    if not vc:
        return "Enter your phone number so the agent can call you back."
    if placeholder_contacts.is_placeholder_phone(inputs.get("vc_phone")):
        return "Enter your full phone number (digits only) so the agent can call you back."
    if vc not in _ALLOWED_NORM:
        if not vc.startswith("+"):
            return "Include your country code (for example +91…) so the agent can call you back."
        return ("Live calls only go to verified numbers, and your number isn't on this "
                "demo's allowlist.")
    if not FOUNDER_PHONE or bolna.normalize_phone(FOUNDER_PHONE) not in _ALLOWED_NORM:
        return ("Live calling isn't fully set up on this server (the founder's number "
                "isn't configured or isn't verified).")
    return None


def live_call_in_progress() -> bool:
    """True if a live-mode run is currently executing in this process."""
    return any(RUNNING_MODES.get(rid) == "live" and not t.done()
               for rid, t in RUNNING_TASKS.items())


def _finish_expired(run_id: str, run: dict) -> bool:
    """A stale run that already has its verdict (step 4 done) finishes as done, with
    the meeting marked not booked — the verdict isn't thrown away. Never dials.
    Returns False (old behaviour) for runs without a verdict and pre-Phase-2 runs."""
    steps = {s["step"]: s for s in run.get("steps") or []}
    synth = steps.get("synthesize_signal") or {}
    rep = (synth.get("result") or {}).get("report")
    founder = ((steps.get("resolve_founder") or {}).get("result") or {}).get("founder")
    top = ((steps.get("find_references") or {}).get("result") or {}).get("top")
    if (synth.get("status") != "done" or not rep or not founder or not top
            or steps["call_vc"]["phase2"] or steps["call_founder"]["phase2"]):
        return False
    mode = run.get("call_mode") or "mock"
    vc_res = steps["call_vc"].get("result") or {}
    fo = steps["call_founder"]
    meeting = (fo.get("result") or {}).get("meeting") if fo["status"] in ("done", "skipped") else None
    msg = ("This check was interrupted and expired before the meeting was booked. "
           "The verdict below still stands — follow up by email.")
    for name in ("call_vc", "call_founder"):
        s = steps[name]
        if s["status"] == "active":
            db.upsert_step(run_id, name, status="error",
                           result={**(s.get("result") or {}), "error": msg})
        elif s["status"] == "pending":
            db.upsert_step(run_id, name, status="skipped",
                           result={"skipped": True, "summary": "Skipped — the check expired"})
    if meeting is None:
        meeting = {"status": "not_booked", "time": None, "with": founder.get("name"),
                   "vc_slots": vc_res.get("slots") or [], "matched_slot": None,
                   "simulated": mode != "live", "note": msg}
    verdict = _verdict_from_report(rep)
    stored = db.get_email(run_id) or {}
    if stored.get("state") in _EMAIL_FINAL:
        email = _email_public(stored)
    elif stored.get("state") == "sending":   # never sends from here (startup, stale run)
        email = {**_email_public(stored), "sent": False, "status": "unconfirmed",
                 "reason": "The check expired while the invite email was being sent — "
                           "it may or may not have arrived."}
    else:
        email = {"sent": False, "to": None, "status": "skipped",
                 "reason": "The check expired before the invite was sent."}
    result = _build_result(run, rep, verdict, founder, top, meeting, email)
    db.upsert_step(run_id, "done", status="done",
                   result={"verdict": verdict["verdict"], "summary": "Verdict ready",
                           "meeting_status": meeting.get("status"),
                           "email_sent": bool(email.get("sent"))})
    db.update_run(run_id, status="done", result=result)
    return True


def _expire(run_id: str, run: dict) -> None:
    if _finish_expired(run_id, run):
        return
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

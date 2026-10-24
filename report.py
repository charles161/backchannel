"""
report.py — reference-report synthesizer (REAL LLM).

synthesize(candidate, referee, transcript, role=None) turns a reference-call transcript
into a structured, sourced report using our Vercel AI Gateway. When `role` (what the
evaluator is hiring/funding the candidate for) is given, the verdict is a FIT verdict
for that role and the report carries a `role_fit` assessment:

  - Haiku (anthropic/claude-haiku-5.5) GENERATES the report body as strict JSON:
    relationship, claims_confirmed, strengths, concerns, recommendation.
  - Jev (typesafe-ai/jev-1.14.0), an EVAL model on /v1/evaluate, SCORES three
    0-100 sub-scores: credibility, enthusiasm, claim_corroboration. Jev is
    best-effort and never blocks the Haiku pass; if the eval endpoint errors, the
    sub-scores are produced by a Haiku call instead.

Robustness: Haiku's JSON is parsed after stripping code fences; a parse failure
retries once with a "return ONLY valid JSON" nudge; a second failure falls back to
the old template mapping and marks synthesis_mode="fallback". The key is read from
the environment only (never hardcoded, never logged).
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import gateway

_VALID_TIERS = {"STRONG HIRE", "HIRE", "LEAN HIRE", "NO HIRE"}
_VALID_CONF = {"high", "medium", "low"}
_VALID_FIT = {"strong", "moderate", "weak", "unclear"}


# ---------------------------------------------------------------------------
# JSON extraction helpers
# ---------------------------------------------------------------------------

def _strip_fences(text: str) -> str:
    """Pull JSON out of a model reply, tolerating ```json fences / prose wrapping."""
    t = text.strip()
    m = re.search(r"```(?:json)?\s*(.*?)```", t, re.DOTALL)
    if m:
        t = m.group(1).strip()
    # If there's leading/trailing prose, grab the outermost {...}.
    if not t.startswith("{"):
        start = t.find("{")
        end = t.rfind("}")
        if start != -1 and end != -1 and end > start:
            t = t[start : end + 1]
    return t


def _parse_json(text: str) -> dict[str, Any] | None:
    try:
        obj = json.loads(_strip_fences(text))
        return obj if isinstance(obj, dict) else None
    except (json.JSONDecodeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# fallback (template) — used only if Haiku generation can't produce valid JSON
# ---------------------------------------------------------------------------

def _confidence(referee: dict) -> str:
    months = referee.get("overlap_months", 0) or 0
    weight = referee.get("seniority_weight", 1.0) or 1.0
    if months >= 12 and weight >= 1.2:
        return "high"
    if months >= 4:
        return "medium"
    return "low"


def _fallback_recommendation(signals: dict, referee: dict) -> dict:
    sentiment = (signals.get("overall_sentiment") or "").lower()
    rehire = signals.get("would_rehire")
    conf = _confidence(referee)

    if sentiment == "strong_positive" and rehire:
        tier = "STRONG HIRE"
        rationale = (
            "Enthusiastic, specific reference from a senior ex-colleague with "
            "substantial tenure overlap; core claims corroborated."
        )
    elif sentiment in ("positive", "lean_positive") and rehire:
        tier = "HIRE"
        rationale = "Solid reference with real overlap; a few caveats worth probing."
    elif rehire is False or sentiment in ("negative", "strong_negative"):
        tier = "NO HIRE"
        rationale = (
            "Reference raised material concerns and/or would not re-hire; "
            "corroborate before advancing."
        )
    else:
        tier = "LEAN HIRE"
        rationale = "Mixed but net-positive signal; gather a second off-list reference."

    return {"tier": tier, "verdict": tier, "confidence": conf, "rationale": rationale}


def _fallback_body(signals: dict, referee: dict, relationship: str) -> dict:
    rec = _fallback_recommendation(signals, referee)
    return {
        "relationship": relationship,
        "claims_confirmed": list(signals.get("confirmed_claims") or []),
        "strengths": list(signals.get("strengths") or []),
        "concerns": list(signals.get("concerns") or []),
        "recommendation": rec,
    }


# ---------------------------------------------------------------------------
# Haiku generation
# ---------------------------------------------------------------------------

_GEN_SYSTEM = (
    "You are a reference-check analyst. You are given a candidate, the referee who "
    "vouched for them (an OFF-LIST ex-colleague, i.e. not a reference the candidate "
    "submitted), and the transcript of a reference call. From the transcript ONLY, "
    "extract a structured, evidence-grounded report. Do not invent facts that the "
    "transcript does not support. Be concise and specific.\n\n"
    "Return STRICT JSON (no prose, no code fences) with EXACTLY these keys:\n"
    '  "relationship": one sentence stating how the referee knows the candidate '
    "(role, org, how they overlapped).\n"
    '  "claims_confirmed": array of short strings — concrete claims the referee '
    "explicitly confirmed about the candidate's scope/ownership/track record.\n"
    '  "strengths": array of short strings — specific strengths the referee cited.\n'
    '  "concerns": array of short strings — weaknesses/caveats the referee raised '
    "(empty array if none).\n"
    '  "recommendation": object {"tier": one of "STRONG HIRE"|"HIRE"|"LEAN HIRE"|'
    '"NO HIRE", "confidence": one of "high"|"medium"|"low", "rationale": one '
    "sentence}. Weigh the referee's enthusiasm, the specificity of what they "
    "confirmed, the depth of overlap, and whether they would work with the "
    "candidate again."
)


_ROLE_SYSTEM = (
    "\n\nThe evaluator is considering the candidate for a SPECIFIC ROLE (given below). "
    "Judge FIT for that role, not general quality: the recommendation tier and its "
    "rationale must reflect how well the evidence supports the candidate in that role "
    "(name the role in the rationale). Add one more key:\n"
    '  "role_fit": object {"fit": one of "strong"|"moderate"|"weak"|"unclear", '
    '"rationale": one sentence tying specific transcript evidence to the role}. Use '
    '"unclear" when the transcript says little that bears on the role.'
)


def _role_text(role: str | None) -> str:
    return " ".join(str(role or "").split())[:300]


def _gen_user(candidate: dict, referee: dict, transcript: dict,
              role: str | None = None) -> str:
    qa_lines = []
    for i, turn in enumerate(transcript.get("qa") or [], 1):
        qa_lines.append(f"Q{i}: {turn.get('q', '')}\nA{i}: {turn.get('a', '')}")
    qa_text = "\n\n".join(qa_lines) if qa_lines else "(no transcript turns)"

    ref_block = (
        f"name: {referee.get('name')}\n"
        f"title: {referee.get('title') or 'n/a'}\n"
        f"company (shared with candidate): {referee.get('company')}\n"
        f"overlap: {referee.get('overlap_window', 'n/a')} "
        f"({referee.get('overlap_months', 0)} months)\n"
        f"seniority: {referee.get('seniority') or 'n/a'}"
    )
    cand_block = (
        f"name: {candidate.get('name')}\n"
        f"current title: {candidate.get('current_title') or 'n/a'}\n"
        f"linkedin: {candidate.get('linkedin_url') or 'n/a'}"
    )
    role_t = _role_text(role)
    role_block = f"ROLE BEING EVALUATED FOR: {role_t}\n\n" if role_t else ""
    return (
        f"CANDIDATE (what we are verifying):\n{cand_block}\n\n"
        f"{role_block}"
        f"REFEREE (off-list, vouched):\n{ref_block}\n\n"
        f"REFERENCE-CALL TRANSCRIPT:\n{qa_text}\n\n"
        "Produce the strict JSON report now."
    )


def _normalize_recommendation(rec: Any, referee: dict, signals: dict) -> dict:
    """Coerce whatever Haiku returned for `recommendation` into the fixed shape."""
    if not isinstance(rec, dict):
        return _fallback_recommendation(signals, referee)
    tier = str(rec.get("tier") or rec.get("verdict") or "").strip().upper()
    if tier not in _VALID_TIERS:
        tier = _fallback_recommendation(signals, referee)["tier"]
    conf = str(rec.get("confidence") or "").strip().lower()
    if conf not in _VALID_CONF:
        conf = _confidence(referee)
    rationale = str(rec.get("rationale") or "").strip() or "See strengths and concerns."
    return {"tier": tier, "verdict": tier, "confidence": conf, "rationale": rationale}


def _normalize_role_fit(v: Any) -> dict:
    """Coerce Haiku's `role_fit` into {"fit", "rationale"} (fit defaults to unclear)."""
    if not isinstance(v, dict):
        return {"fit": "unclear", "rationale": "The call didn't say enough about this role."}
    fit = str(v.get("fit") or "").strip().lower()
    rationale = str(v.get("rationale") or "").strip()
    return {"fit": fit if fit in _VALID_FIT else "unclear",
            "rationale": rationale or "The call didn't say enough about this role."}


def _as_str_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str) and v.strip():
        return [v.strip()]
    return []


def _generate_body(candidate: dict, referee: dict, transcript: dict,
                   signals: dict, relationship: str,
                   role: str | None = None) -> tuple[dict, bool]:
    """Return (body, used_haiku). On failure, returns (template_body, False)."""
    model = os.environ.get("HAIKU_MODEL", "anthropic/claude-haiku-5.5")
    user = _gen_user(candidate, referee, transcript, role)
    system = _GEN_SYSTEM + (_ROLE_SYSTEM if _role_text(role) else "")

    attempts = [
        [{"role": "system", "content": system},
         {"role": "user", "content": user}],
        [{"role": "system", "content": system},
         {"role": "user", "content": user},
         {"role": "user", "content": "Your previous reply was not valid JSON. "
          "Return ONLY the valid JSON object, no prose, no code fences."}],
    ]

    for messages in attempts:
        try:
            raw = gateway.chat(model, messages, max_tokens=900,
                               temperature=0.4, reasoning_effort="none")
        except gateway.GatewayError:
            continue
        parsed = _parse_json(raw)
        if parsed is None:
            continue
        body = {
            "relationship": str(parsed.get("relationship") or "").strip() or relationship,
            "claims_confirmed": _as_str_list(parsed.get("claims_confirmed")),
            "strengths": _as_str_list(parsed.get("strengths")),
            "concerns": _as_str_list(parsed.get("concerns")),
            "recommendation": _normalize_recommendation(
                parsed.get("recommendation"), referee, signals),
        }
        if _role_text(role):
            body["role_fit"] = _normalize_role_fit(parsed.get("role_fit"))
        return body, True

    body = _fallback_body(signals, referee, relationship)
    if _role_text(role):
        body["role_fit"] = _normalize_role_fit(None)
    return body, False


# ---------------------------------------------------------------------------
# Sub-scores: Jev (preferred) with a Haiku fallback
# ---------------------------------------------------------------------------

# Ordered 5-rung ladders (worst -> best); Jev returns the expected rung 0..4.
_SCORE_LADDERS: dict[str, list[str]] = {
    "credibility": [
        "The referee barely knew the candidate and cannot speak to their work",
        "The referee knew the candidate only distantly or very briefly",
        "The referee worked near the candidate and has some first-hand view",
        "The referee worked closely with the candidate over a meaningful period",
        "The referee co-owned work with the candidate and observed them directly "
        "and at length",
    ],
    "enthusiasm": [
        "The referee is clearly negative about the candidate",
        "The referee is lukewarm or hesitant about the candidate",
        "The referee is mildly positive but reserved",
        "The referee is clearly positive and would work with the candidate again",
        "The referee is emphatically positive and would actively seek to hire the "
        "candidate again",
    ],
    "claim_corroboration": [
        "The referee contradicts the candidate's claims",
        "The referee cannot corroborate the candidate's claims",
        "The referee vaguely supports the claims without specifics",
        "The referee confirms the main claims with concrete detail",
        "The referee independently confirms the claims with specific, verifiable "
        "detail beyond what was asked",
    ],
}


def _jev_state(candidate: dict, referee: dict, transcript: dict, signals: dict) -> dict:
    qa = []
    for turn in transcript.get("qa") or []:
        qa.append({"q": turn.get("q", ""), "a": turn.get("a", "")})
    return {
        "candidate_name": candidate.get("name"),
        "candidate_current_title": candidate.get("current_title"),
        "referee_title": referee.get("title"),
        "referee_overlap_months": referee.get("overlap_months", 0),
        "referee_overlap_window": referee.get("overlap_window", ""),
        "referee_seniority": referee.get("seniority"),
        "reference_call_qa": qa,
        "candidate_claims_under_test": list(signals.get("confirmed_claims") or []),
        "scoring_guidance": (
            "Judge ONLY from the reference-call Q&A. Match the transcript to the "
            "highest rung whose description it actually satisfies; do not cluster "
            "toward the middle."
        ),
    }


def _jev_subscores(candidate: dict, referee: dict, transcript: dict,
                   signals: dict) -> dict[str, int] | None:
    """Try Jev; return {metric: 0..100} or None if the eval endpoint is unusable."""
    model = os.environ.get("JEV_MODEL", "typesafe-ai/jev-1.14.0")
    questions = {
        k: {"type": "score", "criteria": ladder}
        for k, ladder in _SCORE_LADDERS.items()
    }
    try:
        answers = gateway.evaluate(
            model, _jev_state(candidate, referee, transcript, signals), questions)
    except gateway.GatewayError:
        return None

    out: dict[str, int] = {}
    for k, ladder in _SCORE_LADDERS.items():
        a = answers.get(k)
        if not isinstance(a, dict) or a.get("type") != "score":
            return None  # partial/garbled -> treat as unusable, let Haiku fill in
        raw = a.get("score")
        if not isinstance(raw, (int, float)):
            return None
        top = max(1, len(ladder) - 1)
        out[k] = max(0, min(100, round((raw / top) * 100)))
    return out


_SUBSCORE_SYSTEM = (
    "You score a reference call. From the transcript ONLY, rate three dimensions "
    "0-100 and return STRICT JSON with integer keys exactly: "
    '"credibility" (how well-positioned the referee is to judge the candidate — '
    "depth and directness of their working relationship), "
    '"enthusiasm" (how positive/emphatic the referee is, incl. would-rehire), '
    '"claim_corroboration" (how concretely the referee confirms the candidate\'s '
    'specific claims). Return ONLY the JSON object, e.g. '
    '{"credibility": 80, "enthusiasm": 90, "claim_corroboration": 75}.'
)


def _haiku_subscores(candidate: dict, referee: dict, transcript: dict) -> dict[str, int]:
    """Fallback sub-scores via Haiku when Jev is unavailable."""
    model = os.environ.get("HAIKU_MODEL", "anthropic/claude-haiku-5.5")
    user = _gen_user(candidate, referee, transcript)
    default = {"credibility": 50, "enthusiasm": 50, "claim_corroboration": 50}
    try:
        raw = gateway.chat(
            model,
            [{"role": "system", "content": _SUBSCORE_SYSTEM},
             {"role": "user", "content": user}],
            max_tokens=120, temperature=0.2, reasoning_effort="none")
    except gateway.GatewayError:
        return default
    parsed = _parse_json(raw) or {}
    out = {}
    for k in default:
        v = parsed.get(k)
        out[k] = int(max(0, min(100, round(v)))) if isinstance(v, (int, float)) else default[k]
    return out


# ---------------------------------------------------------------------------
# public entrypoint
# ---------------------------------------------------------------------------

def synthesize(candidate: dict, referee: dict, transcript: dict,
               role: str | None = None) -> dict:
    """Return a structured reference report, generated by Haiku + scored by Jev.

    candidate : {"name", "linkedin_url", "current_title"} (what we claim to verify)
    referee   : a record from overlap.find_offlist_references (who vouched)
    transcript: a reference-call transcript dict (see samples/)
    role      : optional — what the candidate is being hired/funded for; makes the
                verdict a fit-for-role verdict and adds report["role_fit"]
    """
    signals = transcript.get("signals") or {}

    window = referee.get("overlap_window", "")
    months = referee.get("overlap_months", 0)
    ref_title = referee.get("title") or "former colleague"
    relationship = (
        f"Ex-colleague at {referee.get('company')} "
        f"({window}, {months} mo overlap). Referee role: {ref_title}. "
        f"Not on the candidate's submitted reference list (off-list)."
    )

    # 1) Haiku generates the report body (the primary pass).
    body, used_haiku = _generate_body(
        candidate, referee, transcript, signals, relationship, role)

    # 2) Jev scores the three sub-scores (best-effort; never blocks step 1).
    sub_scores = _jev_subscores(candidate, referee, transcript, signals)
    jev_ran = sub_scores is not None
    jev_note = ""
    if not jev_ran:
        sub_scores = _haiku_subscores(candidate, referee, transcript)
        jev_note = ("Jev /v1/evaluate returned no usable score answers; "
                    "sub-scores computed via Haiku fallback.")

    # 3) synthesis mode
    if not used_haiku:
        synthesis_mode = "fallback"
    elif jev_ran:
        synthesis_mode = "haiku+jev"
    else:
        synthesis_mode = "haiku"

    report = {
        "referee": {
            "name": referee.get("name"),
            "title": ref_title,
            "company": referee.get("company"),
            "linkedin_url": referee.get("linkedin_url"),
            "relationship": body["relationship"],
        },
        "candidate": {
            "name": candidate.get("name"),
            "current_title": candidate.get("current_title"),
            "linkedin_url": candidate.get("linkedin_url"),
        },
        "relationship": body["relationship"],
        "claims_confirmed": body["claims_confirmed"],
        "strengths": body["strengths"],
        "concerns": body["concerns"],
        "recommendation": body["recommendation"],
        "sub_scores": sub_scores,
        "synthesis_mode": synthesis_mode,
        "role": _role_text(role) or None,
        "role_fit": body.get("role_fit"),
        "provenance": {
            "evidence": "employment-overlap graph (live Crustdata)",
            "overlap_window": window,
            "overlap_months": months,
            "transcript_channel": transcript.get("channel", "stubbed"),
            "synthesis": (
                f"Haiku generation + {'Jev' if jev_ran else 'Haiku'} sub-scores "
                f"(mode: {synthesis_mode})"
            ),
            "scorer": "jev (typesafe-ai/jev-1.14.0, /v1/evaluate)" if jev_ran
                      else "haiku fallback",
        },
    }
    if jev_note:
        report["provenance"]["jev_note"] = jev_note
    return report

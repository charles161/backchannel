"""
scheduling.py — the glue between calls 2 and 3 (Phase 2).

After the VC call, Haiku pulls the VC's available slots out of the transcript;
after the founder call, it pulls the confirmed meeting time. Both have a
deterministic regex fallback so a slow/failed gateway never stalls the run, and
match_slot() checks the confirmed time against the VC's slots.

Shapes:
  extract_vc_slots(turns)              -> {"wants_meeting": bool, "slots": [str], "source"}
  extract_confirmed_time(turns, slots) -> {"confirmed": bool, "time": str|None, "source"}
  heuristic_slots(turns) / heuristic_time(turns)  same shapes, no network
  match_slot(time, slots)              -> the VC slot the time falls in, or None

A slot is a weekday + a part of day or a clock time ("Thursday afternoon",
"Friday 10:00 AM"); a confirmed time is a weekday + clock time ("Thursday 3:00 PM").
"today" / "tonight" / "tomorrow" are resolved to weekdays in BACKCHANNEL_TZ
(default Asia/Kolkata), and Haiku is told today's date for the same reason.
The regex fallback is deliberately conservative: it reads one clause at a time,
skips negated clauses ("Friday doesn't work"), and ignores durations ("2 hours").
The pipeline treats anything it produces as tentative, never as booked.
Uses the gateway's Haiku (HAIKU_MODEL), never Jev; the key stays in the environment.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from typing import Any
from zoneinfo import ZoneInfo

import gateway
from report import _parse_json

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
# full weekday names (+ "tues"/"weds"/"thurs") and relative days; bare "sat"/"sun"/"mon"
# are ordinary words
_DAY_RE = re.compile(r"\b(monday|tuesday|tues|wednesday|weds|thursday|thurs|friday|"
                     r"saturday|sunday|today|tonight|tomorrow)\b", re.I)
_PART_RE = re.compile(r"\b(morning|afternoon|evening|tonight|lunch(?:time)?|noon)\b", re.I)
_PART_HOURS = {"morning": (8, 12), "afternoon": (12, 17), "evening": (17, 21),
               "lunchtime": (12, 14)}
_WORD_HOURS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
               "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12}
# "3", "3pm", "3:30", "3.30 p.m.", "at three", "three o'clock" — but not a duration or a
# count ("2 hours", "one of our offices", "10 people")
_NOT_A_TIME = (r"(?!\s*(?:of|or\s+so|hours?|hrs?|minutes?|mins?|days?|weeks?|months?|"
               r"years?|people|percent|times)\b)")
_TIME_RE = re.compile(
    r"\b(?:(at|around|by|after|before)\s+)?(\d{1,2}|" + "|".join(_WORD_HOURS) + r")"
    r"(?:[:.](\d{2}))?\s*(o'?clock|a\.?m\.?|p\.?m\.?)?(?=\W|$)" + _NOT_A_TIME, re.I)
# "2 to 4 pm", "10-12", "3:30 till 5"
_RANGE_RE = re.compile(
    r"\b(\d{1,2})(?::\d{2})?\s*(a\.?m\.?|p\.?m\.?)?\s*(?:-|–|to|till|until)\s*"
    r"(\d{1,2})(?::\d{2})?\s*(a\.?m\.?|p\.?m\.?)?(?=\W|$)" + _NOT_A_TIME, re.I)
_CLAUSE_SPLIT = re.compile(r"[.;!?]+|\bbut\b|\bthough\b|\bhowever\b", re.I)
_LEAD_INTERJECTION = re.compile(r"^(?:no|nope|yes|yeah|yep|ok|okay|sure|well|hmm+|um+|uh+)\b"
                                r"[\s,]*", re.I)
_NEGATION = re.compile(r"\b(?:not|no|never|neither|nor|can't|cannot|won't|doesn't|don't|"
                       r"isn't|wouldn't|couldn't|busy|unavailable|except)\b", re.I)


def _model() -> str:
    return os.environ.get("HAIKU_MODEL", "anthropic/claude-haiku-5.5")


def _today() -> dt.date:
    try:
        tz = ZoneInfo(os.environ.get("BACKCHANNEL_TZ", "Asia/Kolkata"))
    except Exception:  # noqa: BLE001 — a bad TZ name must not break a run
        tz = ZoneInfo("Asia/Kolkata")
    return dt.datetime.now(tz).date()


def _day(token: str, today: dt.date | None = None) -> str | None:
    t = token.strip().lower()
    if t in ("today", "tonight"):
        return DAYS[(today or _today()).weekday()]
    if t == "tomorrow":
        return DAYS[((today or _today()).weekday() + 1) % 7]
    t = t[:3]
    return next((d for d in DAYS if d.lower().startswith(t)), None) if t else None


def _fmt(hour: int, minute: int = 0) -> str:
    h12 = hour % 12 or 12
    return f"{h12}:{minute:02d} {'AM' if hour < 12 else 'PM'}"


def _part_name(token: str) -> str:
    t = token.lower()
    if t.startswith("lunch") or t == "noon":
        return "lunchtime"
    return "evening" if t == "tonight" else t


def _part_of(hour: int) -> str:
    return "morning" if hour < 12 else ("afternoon" if hour < 17 else "evening")


def _turn_text(turns: list[dict], side: str) -> list[str]:
    return [str(t.get(side) or "") for t in turns or [] if str(t.get(side) or "").strip()]


def _qa_block(turns: list[dict], callee: str) -> str:
    lines = []
    for t in turns or []:
        if t.get("q"):
            lines.append(f"AGENT: {t['q']}")
        if t.get("a"):
            lines.append(f"{callee}: {t['a']}")
    return "\n".join(lines) or "(empty transcript)"


def _date_line(today: dt.date) -> str:
    return (f"Today is {DAYS[today.weekday()]}, {today.isoformat()}. Convert relative "
            "days (today, tonight, tomorrow, next Tuesday) to weekday names.")


# ---------------------------------------------------------------------------
# parsing helpers (shared by the heuristics and the Haiku-output normalizers)
# ---------------------------------------------------------------------------

def _hour24(hour: int, suffix: str) -> int:
    if suffix.startswith("p") and hour < 12:
        return hour + 12
    if suffix.startswith("a") and hour == 12:
        return 0
    if not suffix.startswith(("a", "p")) and 1 <= hour <= 7:
        return hour + 12  # "at 3" in a meeting context means 3 PM
    return hour


def _clock(text: str) -> tuple[int, int] | None:
    """First plausible clock time in text -> (hour24, minute); bare 1-7 -> PM."""
    for m in _TIME_RE.finditer(text):
        lead, num, mins, suffix = m.group(1), m.group(2), m.group(3), (m.group(4) or "").lower()
        if not (lead or mins or suffix):
            continue  # a bare number ("2 minutes", "10 people") isn't a time
        hour = _WORD_HOURS.get(num.lower()) if not num.isdigit() else int(num)
        if hour is None or not 0 <= hour <= 23:
            continue
        minute = int(mins) if mins else 0
        if minute > 59:
            continue
        return _hour24(hour, suffix), minute
    return None


def _range_part(text: str) -> str | None:
    """'2 to 4 pm' -> 'afternoon' (a window is a part of day, never its end time)."""
    m = _RANGE_RE.search(text)
    if not m:
        return None
    start, end = int(m.group(1)), int(m.group(3))
    if not (0 <= start <= 23 and 0 <= end <= 23):
        return None
    suffix = (m.group(2) or m.group(4) or "").lower()
    if not m.group(2) and suffix.startswith("p") and start > end:
        suffix = "a"  # "11 to 1 pm" starts in the morning
    return _part_of(_hour24(start, suffix))


def _qualifier(text: str) -> str | None:
    """The time-of-day said in a stretch of text: a window, a clock time or a part."""
    rng = _range_part(text)
    if rng:
        return rng
    clock = _clock(text)
    if clock:
        return _fmt(*clock)
    pm = _PART_RE.search(text)
    return _part_name(pm.group(1)) if pm else None


def _norm_slot(raw: Any, today: dt.date | None = None) -> str | None:
    """'thurs afternoon' -> 'Thursday afternoon'; 'friday 10am' -> 'Friday 10:00 AM';
    'tomorrow 2-4pm' -> '<weekday> afternoon'."""
    s = " ".join(str(raw or "").split())
    if not s:
        return None
    dm = _DAY_RE.search(s)
    day = _day(dm.group(1), today) if dm else None
    if not day:
        return s[:60]  # unparseable but non-empty: keep the VC's words
    q = _qualifier(s) or ("evening" if dm.group(1).lower() == "tonight" else None)
    return f"{day} {q}" if q else day


def _norm_time(raw: Any, today: dt.date | None = None) -> str | None:
    """A confirmed time must have a weekday and a clock time: 'Thursday 3:00 PM'."""
    s = " ".join(str(raw or "").split())
    dm = _DAY_RE.search(s)
    clock = _clock(s)
    if dm and clock:
        return f"{_day(dm.group(1), today)} {_fmt(*clock)}"
    return None


def _dedupe(items: list[str]) -> list[str]:
    out: list[str] = []
    for x in items:
        if x and x.lower() not in {o.lower() for o in out}:
            out.append(x)
    return out


def _clauses(text: str) -> list[str]:
    """Affirmative clauses only: 'Friday doesn't work, but Thursday at 4' -> ['Thursday at 4']."""
    out = []
    for part in _CLAUSE_SPLIT.split(text):
        part = _LEAD_INTERJECTION.sub("", part.strip())
        if part and not _NEGATION.search(part):
            out.append(part)
    return out


# ---------------------------------------------------------------------------
# deterministic fallbacks (no network) — the pipeline treats these as tentative
# ---------------------------------------------------------------------------

def heuristic_slots(turns: list[dict], today: dt.date | None = None) -> dict:
    """Slots from the VC's own words, one affirmative clause at a time: each weekday
    + the time-of-day said after it. A weekday with no qualifier of its own shares
    the next one ("Thursday or Friday afternoon" -> both afternoons), else one said
    before the first weekday ("at 4 on Thursday")."""
    today = today or _today()
    slots: list[str] = []
    for ans in _turn_text(turns, "a"):
        for clause in _clauses(ans):
            hits = list(_DAY_RE.finditer(clause))
            if not hits:
                continue
            segs = [clause[m.end():hits[i + 1].start() if i + 1 < len(hits) else None]
                    for i, m in enumerate(hits)]
            quals = [_qualifier(seg) or ("evening" if hits[i].group(1).lower() == "tonight"
                                         else None) for i, seg in enumerate(segs)]
            before = _qualifier(clause[:hits[0].start()])
            for i, m in enumerate(hits):
                q = quals[i] or next((x for x in quals[i + 1:] if x), None) or before
                d = _day(m.group(1), today)
                slots.append(f"{d} {q}" if q else d)
    slots = _dedupe(slots)[:4]
    return {"wants_meeting": bool(slots), "slots": slots, "source": "heuristic"}


def _clause_time(clause: str, today: dt.date) -> str | None:
    hits = list(_DAY_RE.finditer(clause))
    if not hits:
        return None
    for i, m in enumerate(hits):  # a clock time in the weekday's own stretch of text
        clock = _clock(clause[m.end():hits[i + 1].start() if i + 1 < len(hits) else None])
        if clock:
            return f"{_day(m.group(1), today)} {_fmt(*clock)}"
    clock = _clock(clause[:hits[0].start()])  # "at 3 on Thursday"
    return f"{_day(hits[0].group(1), today)} {_fmt(*clock)}" if clock else None


def heuristic_time(turns: list[dict], today: dt.date | None = None) -> dict:
    """The founder's last affirmative weekday + clock time (else the agent's read-back)."""
    today = today or _today()
    for side in ("a", "q"):
        for text in reversed(_turn_text(turns, side)):
            for clause in reversed(_clauses(text)):
                t = _clause_time(clause, today)
                if t:
                    return {"confirmed": True, "time": t, "source": "heuristic"}
    return {"confirmed": False, "time": None, "source": "heuristic"}


def match_slot(time_str: str | None, slots: list[str]) -> str | None:
    """Which of the VC's slots a confirmed time falls in (same weekday, hour in range)."""
    if not time_str:
        return None
    dm = _DAY_RE.search(time_str)
    clock = _clock(time_str)
    if not dm or not clock:
        return None
    day, hour = _day(dm.group(1)), clock[0]
    for slot in slots or []:
        sm = _DAY_RE.search(slot)
        if not sm or _day(sm.group(1)) != day:
            continue
        sc = _clock(slot)
        if sc:
            if sc[0] == hour:
                return slot
            continue
        pm = _PART_RE.search(slot)
        if not pm:
            return slot  # a whole-day slot
        lo, hi = _PART_HOURS[_part_name(pm.group(1))]
        if lo <= hour < hi:
            return slot
    return None


def default_time_for(slot: str | None) -> str | None:
    """A sensible concrete time inside a slot ('Thursday afternoon' -> 'Thursday 3:00 PM')."""
    if not slot:
        return None
    t = _norm_time(slot)
    if t:
        return t
    dm = _DAY_RE.search(slot)
    if not dm:
        return None
    pm = _PART_RE.search(slot)
    part = _part_name(pm.group(1)) if pm else "afternoon"
    hour = {"morning": 10, "evening": 18, "lunchtime": 12}.get(part, 15)
    return f"{_day(dm.group(1))} {_fmt(hour)}"


# ---------------------------------------------------------------------------
# Haiku extraction (blocking; the pipeline runs these in a thread with a deadline)
# ---------------------------------------------------------------------------

_SLOTS_SYSTEM = (
    "You read the transcript of a phone call in which an AI agent gave an investor "
    "(the VC) the result of a reference check on a founder and asked when the VC is "
    "free to meet the founder. Extract the VC's availability from what the VC said. "
    "Return STRICT JSON (no prose, no code fences): "
    '{"wants_meeting": true|false, "slots": ["<Weekday> <morning|afternoon|evening '
    'or a clock time like 3:00 PM>", ...]}. One entry per distinct window the VC '
    'offered, e.g. "Thursday or Friday afternoon" -> ["Thursday afternoon", '
    '"Friday afternoon"]. Leave out days the VC ruled out. Use only what the VC '
    'actually said. "wants_meeting" is false only if the VC declined to meet; if they '
    'want to meet but gave no concrete day, return {"wants_meeting": true, "slots": []}.'
)

_TIME_SYSTEM = (
    "You read the transcript of a phone call in which an AI agent told a founder that "
    "an investor wants to meet, offered the investor's free slots, and tried to "
    "confirm a meeting time. Extract the meeting time the founder agreed to. Return "
    'STRICT JSON (no prose, no code fences): {"confirmed": true|false, "day": '
    '"<Weekday>", "time": "<h:mm AM|PM>"}. "confirmed" is true if the founder agreed '
    "to a specific day and time, even one outside the investor's slots. If they only "
    "gave a day and part of day, use the time the agent read back; if nothing was "
    "agreed, set confirmed false."
)


def _chat_json(system: str, user: str, timeout: float) -> dict | None:
    try:
        raw = gateway.chat(_model(), [{"role": "system", "content": system},
                                      {"role": "user", "content": user}],
                           max_tokens=200, temperature=0.0, reasoning_effort="none",
                           timeout=timeout)
    except gateway.GatewayError:
        return None
    return _parse_json(raw)


def extract_vc_slots(turns: list[dict], *, timeout: float = 12.0,
                     today: dt.date | None = None) -> dict:
    """Haiku -> the VC's slots. Falls back to heuristic_slots on any gateway problem.

    wants_meeting=True with no slots means "yes, but no concrete day given"."""
    today = today or _today()
    parsed = _chat_json(_SLOTS_SYSTEM, f"{_date_line(today)}\n\nTRANSCRIPT:\n"
                        + _qa_block(turns, "VC"), timeout)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("slots"), list):
        return heuristic_slots(turns, today)
    slots = _dedupe([s for s in (_norm_slot(x, today) for x in parsed["slots"]) if s])[:4]
    wants = parsed.get("wants_meeting")
    wants = bool(slots) if not isinstance(wants, bool) else wants
    return {"wants_meeting": wants, "slots": slots if wants else [], "source": "haiku"}


def extract_confirmed_time(turns: list[dict], vc_slots: list[str], *,
                           timeout: float = 12.0, today: dt.date | None = None) -> dict:
    """Haiku -> the agreed meeting time. Falls back to heuristic_time."""
    today = today or _today()
    user = (f"{_date_line(today)}\n\nINVESTOR'S FREE SLOTS: {json.dumps(vc_slots or [])}\n\n"
            "TRANSCRIPT:\n" + _qa_block(turns, "FOUNDER"))
    parsed = _chat_json(_TIME_SYSTEM, user, timeout)
    if not isinstance(parsed, dict) or "confirmed" not in parsed:
        return heuristic_time(turns, today)
    t = _norm_time(f"{parsed.get('day') or ''} {parsed.get('time') or ''}", today)
    if parsed.get("confirmed") is True and t:
        return {"confirmed": True, "time": t, "source": "haiku"}
    return {"confirmed": False, "time": None, "source": "haiku"}

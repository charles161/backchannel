"""
invite.py — the founder's summary email + .ics calendar invite, sent via Resend
after the founder call books the meeting (inside the pipeline's `done` step).

  meeting_window(time_str, created_at) -> (start, end) aware datetimes, 30 minutes
  build_ics(...)                        -> an RFC 5545 VCALENDAR (METHOD:REQUEST)
  compose(...)                          -> the full Resend payload (deterministic)
  send(payload, idempotency_key)        -> {"id": ...} or raises InviteError

Determinism is what makes the send idempotent: every field — the resolved meeting
date, DTSTAMP, UID — derives from the run (its id, created_at, meeting time), never
from "now". A resume that has to retry an unconfirmed send rebuilds the byte-identical
payload and reuses the same Idempotency-Key, so Resend returns the original email
instead of sending a second one (keys live 24h; the pipeline's stored email state
covers everything after that).

Config comes from the environment ONLY: RESEND_API_KEY (send-only key),
RESEND_FROM, BACKCHANNEL_FOUNDER_EMAIL (Resend test mode can only deliver to the
account owner's address). Optional: BACKCHANNEL_VC_EMAIL (the invite's organizer;
defaults to the From address), BACKCHANNEL_TZ (default Asia/Kolkata).
"""

from __future__ import annotations

import base64
import datetime as dt
import html
import os
import re
from email.utils import parseaddr
from zoneinfo import ZoneInfo

import requests

import scheduling

RESEND_URL = "https://api.resend.com/emails"
MEETING_MINUTES = 30
ICS_FILENAME = "invite.ics"
_MIN_LEAD = dt.timedelta(hours=1)   # a slot less than an hour away rolls to next week


class InviteError(RuntimeError):
    """A failed send (the API key is never attached)."""


def _tz() -> ZoneInfo:
    try:
        return ZoneInfo(os.environ.get("BACKCHANNEL_TZ", "Asia/Kolkata"))
    except Exception:  # noqa: BLE001 — a bad TZ name must not break a run
        return ZoneInfo("Asia/Kolkata")


def config() -> dict | None:
    """{key, sender, to, organizer} or None when Resend isn't configured."""
    key = os.environ.get("RESEND_API_KEY", "").strip()
    sender = os.environ.get("RESEND_FROM", "").strip()
    to = os.environ.get("BACKCHANNEL_FOUNDER_EMAIL", "").strip()
    if not (key and sender and to):
        return None
    organizer = (os.environ.get("BACKCHANNEL_VC_EMAIL", "").strip()
                 or parseaddr(sender)[1] or sender)
    return {"key": key, "sender": sender, "to": to, "organizer": organizer}


def missing_config() -> list[str]:
    return [k for k in ("RESEND_API_KEY", "RESEND_FROM", "BACKCHANNEL_FOUNDER_EMAIL")
            if not os.environ.get(k, "").strip()]


# ---------------------------------------------------------------------------
# "Thursday 3:00 PM" -> a concrete datetime
# ---------------------------------------------------------------------------
_DAY_RE = re.compile(r"\b(monday|tuesday|tues|wednesday|weds|thursday|thurs|friday|"
                     r"saturday|sunday)\b", re.I)


def meeting_window(time_str: str | None, created_at: float | None) -> tuple[dt.datetime, dt.datetime]:
    """The soonest future occurrence of the booked slot, measured from when the run
    was created (so a resume a day later resolves to the same date). A slot with no
    clock time gets the slot's default (afternoon -> 3 PM); no weekday -> tomorrow."""
    tz = _tz()
    ref = dt.datetime.fromtimestamp(created_at or 0, tz) if created_at else dt.datetime.now(tz)
    concrete = scheduling.default_time_for(time_str) or time_str or ""
    clock = scheduling._clock(concrete) or (15, 0)
    dm = _DAY_RE.search(concrete)
    if dm:
        target = scheduling.DAYS.index(scheduling._day(dm.group(1)))
        days = (target - ref.weekday()) % 7
    else:
        days = 1
    start = (ref + dt.timedelta(days=days)).replace(hour=clock[0], minute=clock[1],
                                                    second=0, microsecond=0)
    if start - ref < _MIN_LEAD:
        start += dt.timedelta(days=7 if dm else 1)
    return start, start + dt.timedelta(minutes=MEETING_MINUTES)


def human_when(start: dt.datetime, end: dt.datetime) -> str:
    """'Thursday, 29 October 2026 · 3:00–3:30 PM IST'"""
    def clock(d: dt.datetime) -> str:
        return scheduling._fmt(d.hour, d.minute)
    s, e = clock(start), clock(end)
    if s[-2:] == e[-2:]:
        s = s[:-3]
    return f"{start.strftime('%A')}, {start.day} {start.strftime('%B %Y')} · {s}–{e} {start.tzname()}"


# ---------------------------------------------------------------------------
# .ics (RFC 5545)
# ---------------------------------------------------------------------------
def _esc(text: str) -> str:
    return (str(text).replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,")
            .replace("\r\n", "\\n").replace("\n", "\\n"))


def _cn(text: str) -> str:
    return '"' + str(text).replace('"', "'") + '"'


def _fold(line: str) -> str:
    """Fold a content line at 75 octets (continuation lines start with a space)."""
    raw = line.encode("utf-8")
    if len(raw) <= 75:
        return line
    parts, cur = [], b""
    for ch in line:
        b = ch.encode("utf-8")
        if len(cur) + len(b) > (75 if not parts else 74):
            parts.append(cur.decode("utf-8"))
            cur = b""
        cur += b
    parts.append(cur.decode("utf-8"))
    return "\r\n ".join(parts)


def _utc(d: dt.datetime) -> str:
    return d.astimezone(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_ics(*, uid: str, start: dt.datetime, end: dt.datetime, stamp: dt.datetime,
              summary: str, description: str, organizer_name: str, organizer_email: str,
              attendee_name: str, attendee_email: str) -> str:
    lines = [
        "BEGIN:VCALENDAR",
        "PRODID:-//Backchannel//Meeting Invite//EN",
        "VERSION:2.0",
        "CALSCALE:GREGORIAN",
        "METHOD:REQUEST",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{_utc(stamp)}",
        f"DTSTART:{_utc(start)}",
        f"DTEND:{_utc(end)}",
        f"SUMMARY:{_esc(summary)}",
        f"DESCRIPTION:{_esc(description)}",
        f"ORGANIZER;CN={_cn(organizer_name)}:mailto:{organizer_email}",
        f"ATTENDEE;CN={_cn(attendee_name)};ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;"
        f"RSVP=TRUE:mailto:{attendee_email}",
        "STATUS:CONFIRMED",
        "SEQUENCE:0",
        "TRANSP:OPAQUE",
        "BEGIN:VALARM",
        "TRIGGER:-PT10M",
        "ACTION:DISPLAY",
        "DESCRIPTION:Reminder",
        "END:VALARM",
        "END:VEVENT",
        "END:VCALENDAR",
    ]
    return "\r\n".join(_fold(x) for x in lines) + "\r\n"


# ---------------------------------------------------------------------------
# the email
# ---------------------------------------------------------------------------
def compose(*, cfg: dict, run_id: str, created_at: float | None, meeting_time: str,
            vc_name: str, founder_name: str, company: str, simulated: bool) -> dict:
    """The complete Resend payload + a public summary. Pure: same inputs, same bytes."""
    start, end = meeting_window(meeting_time, created_at)
    stamp = dt.datetime.fromtimestamp(created_at or 0, dt.timezone.utc)
    vc = (vc_name or "").strip() or "An investor"
    vc_label = f"{vc}, an investor," if (vc_name or "").strip() else "An investor"
    founder = (founder_name or "").strip() or "there"
    first = founder.split(" ")[0]
    when = human_when(start, end)
    about = f"to talk about {company}" if company else "with you"
    tag = "[Simulated demo] " if simulated else ""
    subject = f"{tag}{vc} would like to meet you — {start.strftime('%a %d %b')}, " \
              f"{scheduling._fmt(start.hour, start.minute)} {start.tzname()}"
    sim_line = ("Simulated demo — this email was sent by the Backchannel hackathon demo. "
                "No real investor is involved and no real meeting was booked.")
    context = f"A VC wants to meet you. {vc_label} would like a 30-minute intro call {about}."

    text_lines = []
    if simulated:
        text_lines += [sim_line, ""]
    text_lines += [
        f"Hi {first},",
        "",
        context,
        "",
        f"When: {when} (30 minutes)",
        f"With: {vc}",
        "",
        f"The calendar invite is attached ({ICS_FILENAME}) — add it to your calendar.",
        "",
        "— Backchannel, on behalf of " + vc,
    ]
    text = "\n".join(text_lines)

    e = html.escape
    sim_html = (f'<p style="margin:0 0 20px;padding:10px 12px;border:1px solid #c9a227;'
                f'background:#fdf6dc;color:#5a4a00;font-size:13px">'
                f'<strong>Simulated demo</strong> — {e(sim_line.split(" — ", 1)[1])}</p>'
                if simulated else "")
    html_body = (
        '<div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;'
        'color:#141412;max-width:520px;line-height:1.5">'
        f"{sim_html}"
        f"<p>Hi {e(first)},</p>"
        f"<p><strong>{e(context)}</strong></p>"
        '<table style="border-collapse:collapse;margin:8px 0 16px">'
        f'<tr><td style="padding:2px 12px 2px 0;color:#6b6b66">When</td>'
        f"<td>{e(when)} (30 minutes)</td></tr>"
        f'<tr><td style="padding:2px 12px 2px 0;color:#6b6b66">With</td><td>{e(vc)}</td></tr>'
        "</table>"
        f"<p>The calendar invite is attached (<code>{ICS_FILENAME}</code>) — add it to "
        "your calendar.</p>"
        f'<p style="color:#6b6b66">— Backchannel, on behalf of {e(vc)}</p>'
        "</div>"
    )

    ics = build_ics(
        uid=f"{run_id}@backchannel.lightslips.ai",
        start=start, end=end, stamp=stamp,
        summary=f"{tag}{vc} <> {founder} — intro call",
        description=(f"{sim_line}\n\n" if simulated else "") + context,
        organizer_name=vc, organizer_email=cfg["organizer"],
        attendee_name=founder, attendee_email=cfg["to"],
    )
    payload = {
        "from": cfg["sender"],
        "to": [cfg["to"]],
        "subject": subject,
        "text": text,
        "html": html_body,
        "attachments": [{
            "filename": ICS_FILENAME,
            "content": base64.b64encode(ics.encode("utf-8")).decode("ascii"),
            "content_type": "text/calendar; charset=utf-8; method=REQUEST",
        }],
        "tags": [{"name": "app", "value": "backchannel"},
                 {"name": "mode", "value": "simulated" if simulated else "live"}],
    }
    summary = {
        "to": cfg["to"],
        "subject": subject,
        "simulated": simulated,
        "invite": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "timezone": str(start.tzinfo),
            "when": when,
            "filename": ICS_FILENAME,
            "organizer": vc,
            "attendee": founder,
        },
    }
    return {"payload": payload, "summary": summary, "ics": ics}


def send(cfg: dict, payload: dict, idempotency_key: str, *, timeout: float = 15.0) -> dict:
    """POST the email to Resend. Returns {"id": ...}; raises InviteError on failure."""
    try:
        resp = requests.post(
            RESEND_URL, json=payload, timeout=timeout,
            headers={"Authorization": f"Bearer {cfg['key']}",
                     "Content-Type": "application/json",
                     "Idempotency-Key": idempotency_key[:256]})
    except requests.RequestException as e:
        raise InviteError(f"Resend unreachable: {type(e).__name__}") from None
    if resp.status_code not in (200, 201):
        raise InviteError(f"Resend -> HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        data = resp.json()
    except ValueError:
        raise InviteError("Resend returned a non-JSON response") from None
    if not data.get("id"):
        raise InviteError(f"Resend returned no email id: {str(data)[:200]}")
    return {"id": data["id"]}

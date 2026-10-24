"""
bolna.py — thin Bolna voice-call client for Backchannel.

Only what the orchestrator needs:
  * place_call(agent_id, phone, user_data=...) -> {"status","execution_id", ...}
  * get_execution(execution_id)      -> {"status","transcript", ...}
  * parse_transcript(flat_string)    -> {"qa":[{"q","a"}, ...]}
  * normalize_phone(raw)             -> "+919800000000" (for allowlist checks)

The API key is read from the environment ONLY (BOLNA_API_KEY) — never hardcoded,
never logged. Source your Bolna env file (default ~/.bolna.env) before running.

Proven on a real +91 call:
  POST https://api.bolna.ai/call        body {agent_id, recipient_phone_number}
  GET  https://api.bolna.ai/executions/{execution_id}
Both take header  Authorization: Bearer $BOLNA_API_KEY

Per-call context (Phase 2): POST /call accepts `user_data`, an object whose keys
fill {{placeholders}} in the agent's system prompt AND welcome message. The VC and
founder agents (setup_agents.py) are written against those placeholders.

BOLNA_BASE_URL overrides the API origin (tests point it at a local fake).
"""

from __future__ import annotations

import time as _time
import os
import re
from typing import Any

import requests

BASE_URL = os.environ.get("BOLNA_BASE_URL", "https://api.bolna.ai").rstrip("/")
# The pre-created "reference-checker" agent (gpt-4o-mini + alloy TTS + nova-2).
DEFAULT_AGENT_ID = "8570f965-a837-472b-9f6d-215ef5200162"


class BolnaError(RuntimeError):
    """Raised on a bad Bolna response (the key is never attached)."""


def _headers() -> dict[str, str]:
    key = os.environ.get("BOLNA_API_KEY")
    if not key:
        raise BolnaError(
            "BOLNA_API_KEY not set. Run:  source ~/.bolna.env"
        )
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}


def normalize_phone(raw: str | None) -> str:
    """'+91 98000-00000' -> '+919800000000'. Keeps a leading + and the digits only."""
    s = (raw or "").strip()
    digits = re.sub(r"\D", "", s)
    if not digits:
        return ""
    return ("+" if s.startswith("+") else "") + digits


def place_call(agent_id: str, phone: str, *, user_data: dict[str, Any] | None = None,
               timeout: int = 30) -> dict[str, Any]:
    """POST /call — trigger a real outbound call. Returns {status, execution_id, ...}.

    user_data fills {{placeholders}} in the agent's prompt + welcome message."""
    body: dict[str, Any] = {"agent_id": agent_id, "recipient_phone_number": phone}
    if user_data:
        body["user_data"] = user_data
    resp = None
    last_err: Exception | None = None
    for attempt in range(4):
        try:
            resp = requests.post(
                f"{BASE_URL}/call", headers=_headers(), json=body, timeout=timeout
            )
            break
        except requests.RequestException as e:
            last_err = e
            if attempt < 3:
                _time.sleep(1.5 * (attempt + 1))
    if resp is None:
        raise BolnaError(f"Bolna /call unreachable after retries: {last_err}") from None
    if resp.status_code not in (200, 201):
        raise BolnaError(f"Bolna /call -> HTTP {resp.status_code}: {resp.text[:300]}")
    return resp.json()


def get_execution(execution_id: str, *, timeout: int = 30) -> dict[str, Any]:
    """GET /executions/{id} — poll call status + transcript."""
    resp = None
    last_err: Exception | None = None
    for attempt in range(4):
        try:
            resp = requests.get(
                f"{BASE_URL}/executions/{execution_id}", headers=_headers(), timeout=timeout
            )
            break
        except requests.RequestException as e:
            last_err = e
            if attempt < 3:
                _time.sleep(1.5 * (attempt + 1))
    if resp is None:
        raise BolnaError(f"Bolna /executions unreachable after retries: {last_err}") from None
    if resp.status_code != 200:
        raise BolnaError(
            f"Bolna /executions/{execution_id} -> HTTP {resp.status_code}: {resp.text[:300]}"
        )
    return resp.json()


_ROLE_RE = re.compile(
    r"^(assistant|agent|ai|bot|user|human|caller|customer)\s*[:\-]\s*(.*)$", re.I
)
_ASSISTANT_ROLES = {"assistant", "agent", "ai", "bot"}


def parse_transcript(flat: str | None) -> dict[str, list[dict[str, str]]]:
    """Parse Bolna's flat transcript string into {"qa":[{"q","a"}, ...]}.

    Bolna returns turns like:  "assistant: Hi ...\nuser: Yeah ...\nassistant: ...".
    An assistant line is a question (q); the following user line is its answer (a).
    Multi-line turns and consecutive same-role lines are merged. Robust to a
    leading user greeting or a trailing assistant line.
    """
    if not flat or not str(flat).strip():
        return {"qa": []}

    # 1) split into (role, text) segments, folding continuation lines.
    segs: list[list[str]] = []
    for line in str(flat).splitlines():
        line = line.strip()
        if not line:
            continue
        m = _ROLE_RE.match(line)
        if m:
            role = "assistant" if m.group(1).lower() in _ASSISTANT_ROLES else "user"
            segs.append([role, m.group(2).strip()])
        elif segs:
            segs[-1][1] = (segs[-1][1] + " " + line).strip()

    # 2) merge consecutive same-role segments into one turn.
    merged: list[list[str]] = []
    for role, text in segs:
        if merged and merged[-1][0] == role:
            merged[-1][1] = (merged[-1][1] + " " + text).strip()
        else:
            merged.append([role, text])

    # 3) pair assistant(q) -> user(a).
    qa: list[dict[str, str]] = []
    i = 0
    while i < len(merged):
        role, text = merged[i]
        if role == "assistant":
            answer = ""
            if i + 1 < len(merged) and merged[i + 1][0] == "user":
                answer = merged[i + 1][1]
                i += 2
            else:
                i += 1
            qa.append({"q": text, "a": answer})
        else:
            # user turn with no preceding assistant (e.g. the pickup greeting)
            qa.append({"q": "", "a": text})
            i += 1

    return {"qa": [t for t in qa if t["q"] or t["a"]]}

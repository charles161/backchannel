"""
gateway.py — thin client for our Vercel AI Gateway (OpenAI-compatible).

Two surfaces, because Jev is an *evaluation* model, not a chat model:
  - chat()     -> POST {GATEWAY_URL}         (OpenAI-compatible chat/completions)
  - evaluate() -> POST {origin}/v1/evaluate  (typed decision questions, Jev)

Config comes from the environment ONLY (GATEWAY_URL / GATEWAY_KEY). The key is
never hardcoded and never logged. Source .env.local (in this directory) before running.
"""

from __future__ import annotations

import http.client
import json
import os
import urllib.error
import urllib.request
from typing import Any


class GatewayError(RuntimeError):
    """Raised when the gateway is misconfigured or returns an error."""


def _cfg() -> tuple[str, str]:
    """Return (chat_url, key), normalizing a bare base URL to chat/completions."""
    raw = os.environ.get("GATEWAY_URL")
    key = os.environ.get("GATEWAY_KEY") or os.environ.get("AI_GATEWAY_API_KEY")
    if not raw:
        raise GatewayError("GATEWAY_URL is not set. Run: source .env.local")
    if not key:
        raise GatewayError(
            "GATEWAY_KEY (or AI_GATEWAY_API_KEY) is not set. Run: source .env.local"
        )
    trimmed = raw.rstrip("/")
    url = trimmed if trimmed.endswith("/chat/completions") else f"{trimmed}/chat/completions"
    return url, key


def _evaluate_url(chat_url: str) -> str:
    """Derive the Jev evaluate endpoint ({origin}/v1/evaluate) from the chat URL."""
    from urllib.parse import urlsplit

    p = urlsplit(chat_url)
    return f"{p.scheme}://{p.netloc}/v1/evaluate"


def _post(url: str, body: dict[str, Any], timeout: float) -> dict[str, Any]:
    _, key = _cfg()
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "authorization": f"Bearer {key}",
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            payload = json.loads(e.read().decode("utf-8"))
            detail = (payload.get("error") or {}).get("message") or ""
        except Exception:
            pass
        raise GatewayError(f"gateway {e.code}{': ' + detail if detail else ''}") from None
    except urllib.error.URLError as e:
        raise GatewayError(f"gateway unreachable: {e.reason}") from None
    except (TimeoutError, OSError, http.client.HTTPException) as e:
        # slow response (bare socket timeout), dropped connection, truncated body
        raise GatewayError(f"gateway transport error: {type(e).__name__}") from None

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise GatewayError("gateway returned non-JSON response") from None


def chat(
    model: str,
    messages: list[dict[str, str]],
    max_tokens: int,
    *,
    temperature: float = 0.7,
    reasoning_effort: str | None = "none",
    timeout: float = 30.0,
) -> str:
    """POST an OpenAI-style chat completion; return the assistant message content.

    reasoning_effort defaults to "none": Haiku 5.5 otherwise burns the whole token
    budget on extended thinking and returns empty content (a known gateway gotcha).
    Pass reasoning_effort=None to omit the field.
    """
    url, _ = _cfg()
    body: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if reasoning_effort is not None:
        body["reasoning_effort"] = reasoning_effort

    data = _post(url, body, timeout)
    choices = data.get("choices") or []
    if not choices:
        raise GatewayError("gateway returned no choices")
    choice = choices[0]
    content = (choice.get("message") or {}).get("content")
    if not isinstance(content, str) or not content.strip():
        why = (
            "LLM hit the token ceiling"
            if choice.get("finish_reason") == "length"
            else "LLM returned an empty response"
        )
        raise GatewayError(why)
    return content


def evaluate(
    model: str,
    state: dict[str, Any],
    questions: dict[str, Any],
    *,
    timeout: float = 20.0,
) -> dict[str, Any]:
    """POST a Jev evaluation (typed questions over a state); return the answers map.

    Mirrors roast's gateway.evaluate(): body is {model, state, questions}; the
    response carries {answers: {<key>: {type, ...}}}. A `score` question takes an
    ORDERED `criteria` list (worst->best); its answer `score` lives in 0..n-1.
    """
    url, _ = _cfg()
    data = _post(
        _evaluate_url(url),
        {"model": model, "state": state, "questions": questions},
        timeout,
    )
    answers = data.get("answers")
    if not isinstance(answers, dict):
        raise GatewayError("evaluation model returned no answers")
    return answers

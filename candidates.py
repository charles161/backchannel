"""
candidates.py — POST /search: a role (what the VC is hiring/funding for) -> a ranked
list of people to pick from, for the no-name "search -> select" flow.

  search(client, role, limit) -> {"candidates": [...], "message", "source", ...}

Crustdata person/search with the beta natural-language `search` object
({"query": role, "mode": "hybrid"}) plus any obvious title filter read off the role
("founder", "CTO", ...). Crustdata ranks by relevance and tags each head profile
with a `fit` tier. Haiku then writes a one-line `why_fit` per candidate and flags
whether they actually match the role (an investor surfacing for "founder" doesn't);
matches are listed first, otherwise Crustdata's order is kept (hard deadline; a
deterministic line from the profile, and Crustdata's order, if Haiku is slow).

Each SHOWN candidate also carries a DISPLAY-ONLY masked `email` + `phone` from
Crustdata contact-enrich (contacts.py: disk-cached per person, so a repeat costs
nothing; a masked placeholder when there's no real contact). They are never dialed:
picking a candidate sends only their name, company and person_id.

Read-only and credit-conscious: the Crustdata response is disk-cached by the
client (same role = same request body = free), and the assembled list is cached
under .cache/candidates-*.json (without the contact fields) so a repeat skips Haiku
too. Never raises: every failure is an empty list + a human message.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from pathlib import Path
from typing import Any

import contacts
import gateway
import placeholder_contacts
from crust import CrustError
from report import _parse_json

CACHE_DIR = Path(__file__).resolve().parent / ".cache"
MAX_RESULTS = 8          # what we ask Crustdata for (and the most we return)
DEFAULT_LIMIT = 6
WHY_FIT_TIMEOUT_S = float(os.environ.get("BACKCHANNEL_WHYFIT_TIMEOUT_S", "8"))
FIELDS = ["crustdata_person_id", "fit", "basic_profile",
          "experience.employment_details.current",
          "social_handles.professional_network_identifier.profile_url"]
_FIT_RANK = {"strong": 0, "possible": 1, "weak": 2}
_CONTACT_KEYS = ("email", "phone", "contact_source")

# role words that map cleanly onto a current-title filter (a recall/ranking signal
# in Crustdata's default "managed" mode, not a hard constraint)
_TITLE_HINTS = [
    (re.compile(r"\bco-?founders?\b|\bfounders?\b|\bfounding ceo\b", re.I), "founder"),
    (re.compile(r"\bcto\b|\bchief technology officer\b", re.I), "CTO"),
    (re.compile(r"\bcfo\b|\bchief financial officer\b", re.I), "CFO"),
    (re.compile(r"\bcoo\b|\bchief operating officer\b", re.I), "COO"),
    (re.compile(r"\bcmo\b|\bchief marketing officer\b", re.I), "CMO"),
    (re.compile(r"\bcpo\b|\bchief product officer\b", re.I), "Chief Product Officer"),
    (re.compile(r"\bceo\b|\bchief executive\b", re.I), "CEO"),
]


def _log(msg: str) -> None:
    print(f"[backchannel] {msg}", file=sys.stderr, flush=True)


def normalize_role(role: Any) -> str:
    t = "".join(ch if ch.isprintable() else " " for ch in str(role or ""))
    return " ".join(t.split())[:300]


def title_filters(role: str) -> list[dict]:
    """At most one structured title filter derived from the role ([] if none is obvious)."""
    for rx, title in _TITLE_HINTS:
        if rx.search(role):
            return [{"field": "experience.employment_details.current.title",
                     "type": "(.)", "value": title}]
    return []


def _cache_path(role: str) -> Path:
    key = json.dumps({"role": role.lower(), "v": 2}, sort_keys=True)
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return CACHE_DIR / f"candidates-{digest}.json"


def _candidate(p: dict) -> dict | None:
    bp = p.get("basic_profile") or {}
    name = (bp.get("name") or "").strip()
    if not name:
        return None
    cur = ((p.get("experience") or {}).get("employment_details") or {}).get("current") or []
    first = cur[0] if cur else {}
    title = (bp.get("current_title") or first.get("title") or "").strip() or None
    company = (first.get("name") or "").strip() or None
    linkedin = (((p.get("social_handles") or {}).get("professional_network_identifier") or {})
                .get("profile_url"))
    loc = (bp.get("location") or {}).get("full_location") or None
    return {
        "name": name,
        "title": title,
        "company": company,
        "linkedin": linkedin,
        "headline": (bp.get("headline") or "").strip()[:220] or None,
        "location": loc,
        "fit": p.get("fit"),                       # Crustdata relevance tier
        "person_id": p.get("crustdata_person_id"),  # pass back as founder_person_id
    }


def _placeholder(c: dict) -> dict:
    return placeholder_contacts.for_person(c.get("name"), c.get("company"),
                                           c.get("person_id"), masked_email=True)


def _without_contacts(c: dict) -> dict:
    return {k: v for k, v in c.items() if k not in _CONTACT_KEYS}


def _add_contacts(client, shown: list[dict]) -> dict:
    """Masked email/phone on each SHOWN candidate, in place: real contact-enrich (one
    lookup per person, ever — it's disk-cached), else a masked placeholder. Returns
    the contact stats."""
    got, stats = contacts.display(client, shown, _placeholder,
                                  id_key="person_id", url_key="linkedin")
    for c, ct in zip(shown, got):
        c.update(ct)
    return stats


def _fallback_why(c: dict) -> str:
    bits = [b for b in (c.get("title"), c.get("company")) if b]
    lead = " at ".join(bits) if bits else (c.get("headline") or "Profile match")
    tier = {"strong": "Strong match", "possible": "Possible match",
            "weak": "Weak match"}.get(c.get("fit") or "", "Match")
    return f"{tier}: {lead}"[:140]


_WHY_SYSTEM = (
    "You help an investor pick whom to run diligence on. Given a role/thesis and a "
    "numbered list of people (title, company, headline), write ONE short line per "
    "person (max 18 words) saying concretely why they fit the role — or, if they "
    "clearly don't (e.g. an investor when the role asks for a founder), say so "
    "plainly. Use only the facts given. Return STRICT JSON: "
    '{"items": [{"i": <number>, "fits": <true if they plausibly match the role, else '
    'false>, "why_fit": "<line>"}]}.'
)


def _why_fit_haiku(role: str, cands: list[dict]) -> dict[int, tuple[str, bool | None]]:
    lines = []
    for i, c in enumerate(cands):
        lines.append(f"{i}. {c.get('name')} | title: {c.get('title') or 'n/a'} | "
                     f"company: {c.get('company') or 'n/a'} | "
                     f"headline: {c.get('headline') or 'n/a'}")
    user = f"ROLE / THESIS: {role}\n\nPEOPLE:\n" + "\n".join(lines)
    model = os.environ.get("HAIKU_MODEL", "anthropic/claude-haiku-5.5")
    raw = gateway.chat(model, [{"role": "system", "content": _WHY_SYSTEM},
                               {"role": "user", "content": user}],
                       max_tokens=700, temperature=0.3, reasoning_effort="none",
                       timeout=WHY_FIT_TIMEOUT_S)
    parsed = _parse_json(raw) or {}
    out: dict[int, tuple[str, bool | None]] = {}
    for it in parsed.get("items") or []:
        if not isinstance(it, dict):
            continue
        try:
            i = int(it.get("i"))
        except (TypeError, ValueError):
            continue
        why = " ".join(str(it.get("why_fit") or "").split())
        fits = it.get("fits") if isinstance(it.get("fits"), bool) else None
        if why and 0 <= i < len(cands):
            out[i] = (why[:200], fits)
    return out


def _add_why_fit(role: str, cands: list[dict]) -> str:
    """Fill c["why_fit"] (+ c["matches_role"]) in place; returns "haiku" or "profile"."""
    got: dict[int, tuple[str, bool | None]] = {}
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        got = pool.submit(_why_fit_haiku, role, cands).result(timeout=WHY_FIT_TIMEOUT_S + 1)
    except FutureTimeout:
        _log("why_fit: Haiku timed out; using profile lines")
    except Exception as e:  # noqa: BLE001 — gateway down, bad JSON, anything
        _log(f"why_fit: Haiku failed ({type(e).__name__}: {e}); using profile lines")
    finally:
        pool.shutdown(wait=False)
    for i, c in enumerate(cands):
        why, fits = got.get(i) or (_fallback_why(c), None)
        c["why_fit"] = why
        c["matches_role"] = fits
    return "haiku" if len(got) == len(cands) and cands else "profile"


def _empty(role: str, message: str, source: str, **extra) -> dict:
    return {"role": role, "candidates": [], "count": 0, "message": message,
            "source": source, **extra}


def search(client, role: str, limit: int = DEFAULT_LIMIT) -> dict:
    """role -> {"role", "candidates": [{name,title,company,linkedin,headline,why_fit,
    fit,location,person_id,email,phone,contact_source}], "count", "message", "source",
    "cached", "contacts"}. email/phone are masked and display-only (see _add_contacts);
    only the `limit` candidates returned are looked up."""
    role = normalize_role(role)
    limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_RESULTS))
    live = bool(getattr(client, "live", False))
    path = _cache_path(role)
    if path.exists():
        try:
            data = json.loads(path.read_text())
            # a list cached before contact-enrich carries placeholders: never reuse them
            shown = [_without_contacts(c) for c in data.get("candidates") or []][:limit]
        except (OSError, ValueError, AttributeError):
            shown = None
        if shown is not None:
            stats = _add_contacts(client, shown)
            return {**data, "candidates": shown, "count": len(shown), "source": "cache",
                    "cached": True, "contacts": stats}

    filters = title_filters(role)
    try:
        res = client.search_people_nl(role, limit=MAX_RESULTS, conditions=filters or None,
                                      fields=FIELDS)
        if filters and not (res.get("profiles") or []):
            filters = []   # the title hint was too narrow: retry on the query alone
            res = client.search_people_nl(role, limit=MAX_RESULTS, fields=FIELDS)
    except CrustError as e:
        msg = str(e)
        _log(f"/search failed for role={role!r}: {msg[:200]}")
        if not live:
            return _empty(role, "Live search is off on this server and this role hasn't "
                                "been searched before. Type a name instead.", "cache")
        if "402" in msg or "insufficient_credits" in msg or "paused" in msg:
            return _empty(role, "Search is unavailable right now (the data provider's "
                                "credit limit was hit). Type a name instead.", "live")
        return _empty(role, "Search is unavailable right now. Try again in a minute, or "
                            "type a name instead.", "live")

    cands = [c for c in (_candidate(p) for p in res.get("profiles") or []) if c]
    # keep Crustdata's relevance order, but never rank an unrated tail above the head
    cands.sort(key=lambda c: _FIT_RANK.get(c.get("fit") or "", 3))
    cands = cands[:MAX_RESULTS]
    if not cands:
        return _empty(role, "No matching people found. Try describing the role "
                            "differently, or type a name.", "live" if live else "cache")
    why_source = _add_why_fit(role, cands)
    # people Haiku judged a real match first; stable, so Crustdata's order holds within
    cands.sort(key=lambda c: 1 if c.get("matches_role") is False else 0)
    for i, c in enumerate(cands, 1):
        c["rank"] = i
    data = {
        "role": role,
        "candidates": cands,
        "count": len(cands),
        "message": None,
        "source": "live" if live else "cache",
        "why_fit_source": why_source,
        "filters": filters,
        "total_count": res.get("total_count"),
    }
    if why_source == "haiku":   # don't pin a degraded list; the raw response is cached anyway
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(data, indent=2))
            os.replace(tmp, path)   # atomic
        except OSError:
            pass
    shown = [dict(c) for c in cands[:limit]]
    stats = _add_contacts(client, shown)
    return {**data, "candidates": shown, "count": len(shown), "cached": False,
            "contacts": stats}

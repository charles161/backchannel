"""
contacts.py — the real, MASKED, display-only email + phone on candidate and reference
cards, from Crustdata contact-enrich.

  display(client, people, fallback, id_key=..., url_key=...) -> [{email, phone,
      contact_source}]   one per person, in order; masked; never raises

Source: POST /person/contact/enrich, by crustdata_person_id (else the LinkedIn URL).
It costs 5 credits a person (a not_found answer is free; both measured 2026-10-24).
  * The Crust client disk-caches every response under .cache/ (same person id = same
    request body = same file), and a not_found answer is cached too, so a person is
    enriched at most once. Lookups for one person running at the same time share one
    request.
  * Best email: a deliverable business email, then any business, then a deliverable
    personal, then any personal (one marked invalid/undeliverable only as a last
    resort). Phone: the first number with a country code, else the first number.
  * Only masked values leave this module (placeholder_contacts.mask_email/mask_phone):
    "p•••.c•••@stripe.com", "+91 63XXX X5634". The raw address/number stays in the
    server-side cache file.
  * Fallback, per field: cache-only miss, an error, a timeout, no match, or no
    email/phone -> the caller's placeholder (masked too), so a card never shows a
    blank. contact_source says which you got: {"email": "crustdata"|"placeholder",
    "phone": ...}.
  * A lookup still running at the deadline keeps going in the background and caches
    its answer: the card shows the placeholder now and the real contact next time.
  * BACKCHANNEL_CONTACT_ENRICH=0: cached answers only, no new lookups (no credits).
    A 401/402/403 pauses new lookups for 10 minutes, a network error for 60s. The
    pause is this module's own: the request goes through the base Crust._post, so a
    failed or slow contact lookup never pauses founder/search lookups (LiveCrust's).

NEVER DIALED: a masked number always contains "X", which pipeline._live_call and
pipeline.live_preflight refuse (placeholder_contacts.is_placeholder_phone). Every call
dials BACKCHANNEL_REF_PHONE / the VC's own typed number / BACKCHANNEL_FOUNDER_PHONE.
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Any, Callable

from crust import Crust, CrustError
from placeholder_contacts import mask_email, mask_phone

ENDPOINT = "/person/contact/enrich"
TIMEOUT_S = float(os.environ.get("BACKCHANNEL_CONTACT_TIMEOUT_S", "10"))
_PAUSE_S = 600.0             # after a 401/402/403, don't retry for this long
_NET_PAUSE_S = 60.0          # after a timeout / dropped connection
_paused_until = 0.0
_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="contact-enrich")
_inflight: dict[str, Future] = {}
_lock = threading.RLock()    # re-entrant: a done-callback can fire inside submit


def _log(msg: str) -> None:
    print(f"[backchannel] {msg}", file=sys.stderr, flush=True)


def enrich_enabled() -> bool:
    """BACKCHANNEL_CONTACT_ENRICH: "1" (default) = live lookups behind the cache."""
    v = os.environ.get("BACKCHANNEL_CONTACT_ENRICH", "1").strip().lower() or "1"
    return v not in ("0", "false", "no", "off")


def _body(person_id: Any, linkedin_url: Any) -> dict | None:
    """The request for one person: by id when there is one, else by profile URL."""
    try:
        pid = int(str(person_id).strip())
    except (TypeError, ValueError):
        pid = 0
    if pid > 0:
        return {"crustdata_person_ids": [pid]}
    url = str(linkedin_url or "").strip()
    if url.startswith("http") and "linkedin.com/" in url:
        return {"professional_network_profile_urls": [url]}
    return None


def _read_cache(client: Crust, body: dict) -> Any:
    path = client._cache_path(ENDPOINT, body)
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    client.cache_hits += 1
    client.call_log.append({"endpoint": ENDPOINT, "source": "cache", "file": path.name})
    return data


def _contact_block(data: Any) -> dict:
    """The best match's raw `contact` object ({} for not_found / an odd shape)."""
    if not isinstance(data, list) or not data or not isinstance(data[0], dict):
        return {}
    matches = [m for m in data[0].get("matches") or [] if isinstance(m, dict)]
    if not matches:
        return {}
    best = max(matches, key=lambda m: _score(m.get("confidence_score")))
    contact = (best.get("person_data") or {}).get("contact")
    return contact if isinstance(contact, dict) else {}


def _score(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


_BAD_EMAIL = {"invalid", "undeliverable"}


def best_email(contact: dict) -> str | None:
    """A deliverable business email, then any business, then personal (deliverable
    first); an invalid/undeliverable one only when there's nothing else."""
    def emails(key: str) -> list[dict]:
        out = []
        for e in contact.get(key) or []:
            e = {"email": e} if isinstance(e, str) else e
            if isinstance(e, dict) and "@" in str(e.get("email") or ""):
                out.append(e)
        return out

    def rank(e: dict, business: bool) -> int:
        status = str(e.get("status") or "").lower()
        return (4 if status in _BAD_EMAIL else 0) + (0 if business else 2) + \
               (0 if status == "deliverable" else 1)

    ranked = sorted([(rank(e, True), e) for e in emails("business_emails")] +
                    [(rank(e, False), e) for e in emails("personal_emails")],
                    key=lambda t: t[0])   # stable: the API's order holds within a rank
    return str(ranked[0][1]["email"]).strip() if ranked else None


def best_phone(contact: dict) -> tuple[str, str] | None:
    """(country code, national number) of the first phone number that has a country
    code, else of the first one; None when there's no number."""
    infos = [i for i in contact.get("phone_numbers_info") or [] if isinstance(i, dict)
             and re.sub(r"\D", "", str(i.get("phone_number") or ""))]
    info = next((i for i in infos if re.sub(r"\D", "", str(i.get("country_code") or ""))),
                infos[0] if infos else None)
    if info:
        cc = re.sub(r"\D", "", str(info.get("country_code") or ""))
        num = re.sub(r"\D", "", str(info.get("phone_number")))
        if cc and num.startswith(cc) and len(num) - len(cc) >= 10:
            num = num[len(cc):]   # the country code was repeated inside the number
        return cc, num
    raw = next((str(p) for p in contact.get("phone_numbers") or [] if p), "").strip()
    d = re.sub(r"\D", "", raw)
    if not d:
        return None
    if raw.startswith("+"):
        m = re.match(r"\+(\d{1,3})[\s\-.(]", raw)   # "+61 412 345 678": the first group
        if m:
            return m.group(1), d[len(m.group(1)):]
        if len(d) > 10:   # "+<cc><10-digit national number>", no separators
            return d[:-10], d[-10:]
    return "", d


def _lookup(client: Crust, body: dict) -> dict:
    """One person's raw (UNMASKED — server-side only) {"email", "phone", "source"}.
    Runs on the pool; never raises."""
    global _paused_until
    data = _read_cache(client, body)
    source = "cache"
    if data is None:
        why = ("off (BACKCHANNEL_CONTACT_ENRICH=0)" if not enrich_enabled() else
               "cache-only" if not getattr(client, "live", True) else
               "paused after an error" if time.time() < _paused_until else None)
        if why:
            return {"source": why}
        try:
            # the base _post (not LiveCrust's): caches the answer, not_found included,
            # without touching the process-wide pause the founder/search lookups use
            data = Crust._post(client, ENDPOINT, body)
            source = "api"
        except CrustError as e:
            msg = str(e)
            if re.search(r"HTTP 40[123]\b", msg) or "insufficient_credits" in msg:
                _paused_until = time.time() + _PAUSE_S
            elif "network error" in msg:
                _paused_until = max(_paused_until, time.time() + _NET_PAUSE_S)
            _log(f"contact-enrich failed ({msg[:160]}); using the placeholder")
            return {"source": "error"}
        except Exception as e:  # noqa: BLE001 — never crash a run or /search
            _log(f"contact-enrich failed ({type(e).__name__}); using the placeholder")
            return {"source": "error"}
    contact = _contact_block(data)
    return {"email": best_email(contact), "phone": best_phone(contact), "source": source}


def _submit(client: Crust, body: dict) -> Future:
    key = json.dumps(body, sort_keys=True)
    with _lock:
        fut = _inflight.get(key)
        if fut is None:
            fut = _pool.submit(_lookup, client, body)
            _inflight[key] = fut
            fut.add_done_callback(lambda _f, k=key: _forget(k, _f))
    return fut


def _forget(key: str, fut: Future) -> None:
    with _lock:
        if _inflight.get(key) is fut:
            del _inflight[key]


def display(client: Crust, people: list[dict], fallback: Callable[[dict], dict], *,
            id_key: str, url_key: str, timeout: float = TIMEOUT_S) -> tuple[list[dict], dict]:
    """Masked {"email", "phone", "contact_source"} for each person, in order, plus stats
    {"crustdata", "placeholder", "api", "cache", "credits_used"}. `fallback(person)`
    gives the placeholder {"email", "phone"} (already masked) for a missing field.
    Waits at most `timeout` seconds for the whole batch."""
    log_start = len(client.call_log)
    futs: list[Future | None] = []
    for p in people:
        body = _body(p.get(id_key), p.get(url_key))
        futs.append(_submit(client, body) if body else None)
    deadline = time.monotonic() + timeout
    out, stats = [], {"crustdata": 0, "placeholder": 0, "api": 0, "cache": 0}
    for p, fut in zip(people, futs):
        got: dict = {}
        if fut is not None:
            try:
                got = fut.result(timeout=max(0.0, deadline - time.monotonic()))
            except FutureTimeout:
                got = {"source": "timeout"}
            except Exception:  # noqa: BLE001
                got = {"source": "error"}
        if got.get("source") in ("api", "cache"):
            stats[got["source"]] += 1
        email = mask_email(got.get("email"))
        phone = mask_phone(*got["phone"]) if got.get("phone") else None
        fb = fallback(p) if not (email and phone) else {}
        src = {"email": "crustdata" if email else "placeholder",
               "phone": "crustdata" if phone else "placeholder"}
        stats["crustdata" if email or phone else "placeholder"] += 1
        out.append({"email": email or fb.get("email"), "phone": phone or fb.get("phone"),
                    "contact_source": src})
    credits = 0.0
    for e in client.call_log[log_start:]:
        if e.get("endpoint") == ENDPOINT and e.get("source") == "api":
            try:
                credits += float(e.get("credits_used") or 0)
            except (TypeError, ValueError):
                pass
    stats["credits_used"] = credits
    if people:
        _log(f"contacts: {len(people)} people · {stats['crustdata']} from Crustdata · "
             f"{stats['placeholder']} placeholder · api={stats['api']} cache={stats['cache']} "
             f"credits={credits:g}")
    return out, stats

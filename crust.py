"""
crust.py — thin, credit-conscious Crustdata API client for Backchannel.

Design goals (this is a hackathon spike, not prod):
  * Reads the API key from the environment ONLY (never hardcoded, never logged).
  * Sends the 3 required headers on every request.
  * Disk-caches every response under .cache/ keyed by a hash of the request, so
    re-runs during development hit the cache instead of burning credits.
  * Tracks how many real API calls vs cache hits we made, so the CLI can print it.

Only two endpoints are used by the pipeline:
  * POST /person/search   (ref doc §1) — discovery of colleagues, plus the beta
                          natural-language `search` object (role -> candidates)
  * POST /person/enrich   (ref doc §2) — full profile for a known LinkedIn URL

Every failure — a non-200, a dropped connection, a timeout, a non-JSON body —
surfaces as CrustError, so callers can degrade gracefully instead of crashing.

Autocomplete (POST /person/search/autocomplete, FREE) is exposed for filter-value
validation without spending search credits.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests

BASE_URL = "https://api.crustdata.com"
API_VERSION = "2025-11-01"
CACHE_DIR = Path(__file__).resolve().parent / ".cache"


class CrustError(RuntimeError):
    """Raised when the API returns a non-200 (body is attached, key never is)."""


class Crust:
    def __init__(self, cache_dir: Path | str = CACHE_DIR, timeout: int = 60):
        key = os.environ.get("CRUSTDATA_API_KEY")
        if not key:
            raise CrustError(
                "CRUSTDATA_API_KEY not set. Run:  source ~/.crustdata.env"
            )
        self._key = key
        self.timeout = timeout
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        # counters so the CLI can prove how few live calls it made
        self.api_calls = 0
        self.cache_hits = 0
        self.call_log: list[dict[str, Any]] = []

    # ---- internals -------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "authorization": f"Bearer {self._key}",
            "x-api-version": API_VERSION,
            "content-type": "application/json",
        }

    @staticmethod
    def _slug(endpoint: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", endpoint.lower()).strip("-")

    def _cache_path(self, endpoint: str, body: dict) -> Path:
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256((endpoint + "|" + canonical).encode()).hexdigest()[:16]
        return self.cache_dir / f"{self._slug(endpoint)}-{digest}.json"

    def _post(self, endpoint: str, body: dict, *, cache: bool = True) -> Any:
        path = self._cache_path(endpoint, body)
        if cache and path.exists():
            self.cache_hits += 1
            self.call_log.append({"endpoint": endpoint, "source": "cache", "file": path.name})
            return json.loads(path.read_text())

        url = f"{BASE_URL}{endpoint}"
        try:
            resp = self.session.post(url, headers=self._headers(), json=body,
                                     timeout=self.timeout)
        except requests.RequestException as e:
            self.call_log.append({"endpoint": endpoint, "source": "api", "status": "network"})
            raise CrustError(f"{endpoint} -> network error: {type(e).__name__}") from None
        self.api_calls += 1
        remaining = resp.headers.get("x-ratelimit-remaining")
        credits = resp.headers.get("x-credits-used")
        self.call_log.append(
            {
                "endpoint": endpoint,
                "source": "api",
                "status": resp.status_code,
                "credits_used": credits,
                "ratelimit_remaining": remaining,
            }
        )

        if resp.status_code != 200:
            # Never include self._key in the error. The body may hold {error|reason|description}.
            raise CrustError(
                f"{endpoint} -> HTTP {resp.status_code}: {resp.text[:500]}"
            )

        try:
            data = resp.json()
        except ValueError:
            raise CrustError(f"{endpoint} -> HTTP 200 with a non-JSON body") from None
        if cache:   # atomic: a crash or a concurrent writer never leaves half a file
            tmp = path.with_suffix(f".{os.getpid()}.{time.monotonic_ns()}.tmp")
            tmp.write_text(json.dumps(data, indent=2))
            os.replace(tmp, path)
        return data

    # ---- public API ------------------------------------------------------

    def search_people(self, conditions: list[dict], limit: int = 10, *, op: str = "and") -> dict:
        """POST /person/search. `conditions` is a list of {field,type,value}.

        Returns the raw object: {"profiles":[...], "next_cursor":..., "total_count":N}.
        ~0.03 credits per returned result. Keep `limit` small.
        """
        if limit > 10:
            raise CrustError("Refusing limit > 10 during the spike (credit guard).")
        body = {"filters": {"op": op, "conditions": conditions}, "limit": limit}
        return self._post("/person/search", body)

    def search_people_nl(self, query: str, limit: int = 8, *,
                         conditions: list[dict] | None = None,
                         fields: list[str] | None = None, mode: str = "hybrid") -> dict:
        """POST /person/search with the beta natural-language `search` object.

        Crustdata ranks people against `query` (hybrid = keyword + semantic) and tags
        each head profile with a `fit` tier (strong | possible | weak). Optional
        `conditions` are extra structured filters; in the default `managed` recall
        mode they steer recall/ranking rather than act as hard constraints.
        Same price as a filter search: ~0.03 credits per returned result.
        """
        if limit > 10:
            raise CrustError("Refusing limit > 10 during the spike (credit guard).")
        body: dict[str, Any] = {"search": {"query": query, "mode": mode}, "limit": limit}
        if conditions:
            body["filters"] = (conditions[0] if len(conditions) == 1
                               else {"op": "and", "conditions": conditions})
        if fields:
            body["fields"] = fields
        return self._post("/person/search", body)

    def enrich_person(
        self,
        linkedin_url: str | None = None,
        person_id: int | str | None = None,
        fields: list[str] | None = None,
    ) -> dict | None:
        """Return ONE normalized person_data dict (or None if not matched).

        * linkedin_url -> POST /person/enrich (the documented full-profile path, 1 credit).
        * person_id    -> fallback via /person/search on crustdata_person_id (enrich's
                          documented request only accepts profile URLs; search returns the
                          same profile shape for an id lookup).
        """
        if fields is None:
            fields = ["basic_profile", "experience", "education"]

        if linkedin_url:
            body = {"professional_network_profile_urls": [linkedin_url], "fields": fields}
            data = self._post("/person/enrich", body)
            return _first_match_person_data(data)

        if person_id is not None:
            res = self.search_people(
                [{"field": "crustdata_person_id", "type": "=", "value": person_id}], limit=1
            )
            profiles = res.get("profiles") or []
            return profiles[0] if profiles else None

        raise CrustError("enrich_person needs either linkedin_url or person_id")

    def autocomplete(self, field: str, query: str = "", limit: int = 10) -> list[str]:
        """POST /person/search/autocomplete — FREE. Validate filter values cheaply."""
        body = {"field": field, "query": query, "limit": limit}
        data = self._post("/person/search/autocomplete", body)
        return [s.get("value") for s in data.get("suggestions", [])]

    # ---- reporting -------------------------------------------------------

    def stats(self) -> dict:
        return {"api_calls": self.api_calls, "cache_hits": self.cache_hits}


def _first_match_person_data(enrich_response: Any) -> dict | None:
    """Enrich returns a top-level array; each entry has matches[].person_data."""
    if not isinstance(enrich_response, list) or not enrich_response:
        return None
    entry = enrich_response[0]
    if entry.get("match_status") not in (None, "matched") and not entry.get("matches"):
        return None
    matches = entry.get("matches") or []
    if not matches:
        return None
    # highest confidence first
    matches = sorted(matches, key=lambda m: m.get("confidence_score", 0), reverse=True)
    return matches[0].get("person_data")

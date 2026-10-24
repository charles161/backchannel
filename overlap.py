"""
overlap.py — the core IP of Backchannel.

Given a candidate's enriched profile, discover OFF-LIST references: ex-colleagues
the candidate did not list, ranked by a dated employment-overlap graph.

The signal is simple and defensible: two people who worked at the same company at
the same time knew each other. The longer the overlap and the more senior the
colleague, the stronger the reference. We compute that overlap from real tenure
dates returned by Crustdata.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _parse_dt(value: str | None) -> datetime | None:
    """Parse Crustdata ISO dates: '2016-01-01T00:00:00', '...+00:00', '...Z'."""
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")
    for candidate in (s, s[:19], s[:10]):
        try:
            return datetime.fromisoformat(candidate).replace(tzinfo=None)
        except ValueError:
            continue
    return None


def _fmt_month(dt: datetime) -> str:
    return dt.strftime("%b %Y")


# ---------------------------------------------------------------------------
# Tenure parsing
# ---------------------------------------------------------------------------

def parse_tenure(employment_entry: dict) -> tuple[str | None, datetime | None, datetime, str | None]:
    """(company, start_date, end_date-or-now, seniority) for one employment entry.

    A null end_date means the person is still there, so we clamp it to 'now'.
    """
    company = employment_entry.get("name")
    start = _parse_dt(employment_entry.get("start_date"))
    end = _parse_dt(employment_entry.get("end_date")) or datetime.now()
    seniority = employment_entry.get("seniority_level")
    return company, start, end, seniority


def overlap_months(
    a_start: datetime | None,
    a_end: datetime | None,
    b_start: datetime | None,
    b_end: datetime | None,
) -> int:
    """Whole months two tenures intersect (inclusive of both endpoint months)."""
    if not a_start or not b_start:
        return 0
    a_end = a_end or datetime.now()
    b_end = b_end or datetime.now()
    start = max(a_start, b_start)
    end = min(a_end, b_end)
    if end < start:
        return 0
    months = (end.year - start.year) * 12 + (end.month - start.month) + 1
    return max(0, months)


# ---------------------------------------------------------------------------
# Seniority -> weight (senior/peer referees weigh more than junior ones)
# ---------------------------------------------------------------------------

_SENIORITY_WEIGHTS = [
    # (substring to look for in lowercased seniority_level, weight)
    ("cxo", 1.6),
    ("c-suite", 1.6),
    ("chief", 1.6),
    ("owner", 1.5),
    ("partner", 1.5),
    ("founder", 1.5),
    ("vice president", 1.5),
    ("vp", 1.5),
    ("director", 1.4),
    ("principal", 1.3),
    ("staff", 1.3),
    ("senior", 1.25),
    ("lead", 1.2),
    ("manager", 1.15),
    ("entry level manager", 1.15),
    ("entry", 0.9),
    ("junior", 0.9),
    ("training", 0.8),
    ("intern", 0.8),
]


def seniority_weight(seniority_level: str | None) -> float:
    if not seniority_level:
        return 1.0
    s = seniority_level.lower()
    # "entry level manager" should score as manager, so check manager before entry
    if "manager" in s:
        return 1.15
    for needle, weight in _SENIORITY_WEIGHTS:
        if needle in s:
            return weight
    return 1.0


# ---------------------------------------------------------------------------
# Profile helpers
# ---------------------------------------------------------------------------

def _employment_entries(profile: dict) -> list[dict]:
    emp = (profile.get("experience") or {}).get("employment_details") or {}
    out = []
    for bucket in ("current", "past"):
        for e in emp.get(bucket) or []:
            e = dict(e)
            e["_is_current"] = bucket == "current"
            out.append(e)
    return out


def _linkedin(profile: dict) -> str | None:
    return (
        (profile.get("social_handles") or {})
        .get("professional_network_identifier", {})
        .get("profile_url")
    )


def _name(profile: dict) -> str | None:
    return (profile.get("basic_profile") or {}).get("name")


class CompanyTenure:
    """A candidate's full, merged tenure at one company (across role changes)."""

    def __init__(self, company: str, company_id: int, start: datetime, end: datetime,
                 end_is_now: bool, is_current: bool, seniority: str | None, headcount: int):
        self.company = company
        self.company_id = company_id
        self.start = start
        self.end = end
        self.end_is_now = end_is_now
        self.is_current = is_current
        self.seniority = seniority
        self.headcount = headcount

    def window(self) -> str:
        end = "present" if self.end_is_now else _fmt_month(self.end)
        return f"{_fmt_month(self.start)}–{end}"


def _merge_companies(profile: dict) -> list[CompanyTenure]:
    """Collapse multiple role-entries at the same company into one tenure.

    Join key is crustdata_company_id when present (!=0), else the company name.
    """
    groups: dict[Any, dict] = {}
    for e in _employment_entries(profile):
        company, start, end, seniority = parse_tenure(e)
        if not company or not start:
            continue
        cid = e.get("crustdata_company_id") or 0
        key = cid if cid else company.lower()
        end_is_now = not e.get("end_date")
        g = groups.get(key)
        if g is None:
            groups[key] = {
                "company": company,
                "company_id": cid,
                "start": start,
                "end": end,
                "end_is_now": end_is_now,
                "is_current": bool(e.get("_is_current")),
                "seniority": seniority,
                "best_weight": seniority_weight(seniority),
                "headcount": e.get("company_headcount_latest") or 0,
            }
        else:
            g["start"] = min(g["start"], start)
            if end > g["end"]:
                g["end"] = end
            g["end_is_now"] = g["end_is_now"] or end_is_now
            g["is_current"] = g["is_current"] or bool(e.get("_is_current"))
            w = seniority_weight(seniority)
            if w > g["best_weight"]:
                g["best_weight"] = w
                g["seniority"] = seniority
            g["headcount"] = max(g["headcount"], e.get("company_headcount_latest") or 0)
    return [
        CompanyTenure(g["company"], g["company_id"], g["start"], g["end"],
                      g["end_is_now"], g["is_current"], g["seniority"], g["headcount"])
        for g in groups.values()
    ]


def _colleague_tenure_at(colleague: dict, company_id: int, company_name: str):
    """Return (start, end, seniority, end_is_now) for the colleague's time at a company.

    Matches on company_id when available (precise), else on company name.
    Merges multiple role-entries.
    """
    start = end = None
    end_is_now = False
    best_sen = None
    best_w = -1.0
    cname_l = (company_name or "").lower()
    for e in _employment_entries(colleague):
        cid = e.get("crustdata_company_id") or 0
        name = (e.get("name") or "").lower()
        match = (company_id and cid == company_id) or (not company_id and name == cname_l) \
            or (name and cname_l and name == cname_l)
        if not match:
            continue
        _, s, en, sen = parse_tenure(e)
        if not s:
            continue
        start = s if start is None else min(start, s)
        end = en if end is None else max(end, en)
        if not e.get("end_date"):
            end_is_now = True
        w = seniority_weight(sen)
        if w > best_w:
            best_w = w
            best_sen = sen
    return start, end, best_sen, end_is_now


# ---------------------------------------------------------------------------
# The pipeline
# ---------------------------------------------------------------------------

def find_offlist_references(
    client,
    candidate_profile: dict,
    max_companies: int = 3,
    pool_per_company: int = 10,
    skip_large_headcount: int = 50_000,
    listed_names: Iterable[str] | None = None,
    include_current: bool = True,
):
    """Discover and rank off-list references for a candidate.

    Steps:
      1. Merge the candidate's employment into per-company tenures.
      2. Take their companies, most recent first. We ALWAYS search with
         `past.company_name == company`, which returns people who have since LEFT
         that company — i.e. ex-colleagues. That holds whether the company is a
         past employer OR the candidate's current one (alumni of the current
         employer are prime off-list references), so include_current=True keeps
         the current employer in the set.
      3. For each company, person/search `past.company_name == company`.
      4. Compute overlap_months against the candidate's tenure at that company.
      5. Keep overlap >= 1 month, score = overlap_months * seniority_weight(referee).
      6. Exclude the candidate and any already-listed references. Dedupe, rank.

    `client` is a crust.Crust instance. Returns (ranked_list, meta).
    """
    listed = {n.strip().lower() for n in (listed_names or []) if n.strip()}
    cand_id = candidate_profile.get("crustdata_person_id")
    cand_name = (_name(candidate_profile) or "").lower()
    cand_linkedin = (_linkedin(candidate_profile) or "").lower().rstrip("/")

    tenures = _merge_companies(candidate_profile)
    pool = tenures if include_current else [t for t in tenures if not t.is_current]
    # most recent first (current employers have end=now, so they rank first)
    pool.sort(key=lambda t: t.end, reverse=True)

    selected = [t for t in pool if not t.headcount or t.headcount <= skip_large_headcount]
    if not selected:  # don't end up with nothing just because every company is huge
        selected = pool
    selected = selected[:max_companies]
    past = [t for t in tenures if not t.is_current]  # for meta/reporting

    results: dict[Any, dict] = {}
    searched = []

    for t in selected:
        entry = {"company": t.company, "candidate_window": t.window(),
                 "headcount": t.headcount, "is_current_employer": t.is_current,
                 "status": "ok"}
        searched.append(entry)
        try:
            res = client.search_people(
                [{"field": "experience.employment_details.past.company_name",
                  "type": "[.]", "value": t.company}],
                limit=pool_per_company,
            )
        except Exception as e:  # noqa: BLE001 — degrade gracefully (402 cap / 429 / 5xx)
            # A capped/rate-limited company must not abort the whole run; other
            # companies (often already cached) can still produce references.
            msg = str(e)
            if "402" in msg or "insufficient_credits" in msg or "credit_limit" in msg:
                reason = "credit cap hit (HTTP 402) — run from cache or wait for reset"
            elif "429" in msg:
                reason = "rate limited (HTTP 429)"
            else:
                reason = msg.split("\n")[0][:70]
            entry["status"] = f"skipped: {reason}"
            continue
        for colleague in res.get("profiles") or []:
            c_id = colleague.get("crustdata_person_id")
            c_name = _name(colleague)
            c_link = (_linkedin(colleague) or "").lower().rstrip("/")
            if c_id and cand_id and c_id == cand_id:
                continue
            if c_name and c_name.lower() == cand_name:
                continue
            if c_link and cand_linkedin and c_link == cand_linkedin:
                continue
            if c_name and c_name.lower() in listed:
                continue

            cs, ce, c_sen, c_now = _colleague_tenure_at(colleague, t.company_id, t.company)
            if not cs:
                continue
            months = overlap_months(t.start, t.end, cs, ce)
            if months < 1:
                continue

            ov_start = max(t.start, cs)
            ov_end = min(t.end, ce)
            weight = seniority_weight(c_sen)
            score = round(months * weight, 1)

            # colleague's headline title (current role) for display
            title = None
            for e in _employment_entries(colleague):
                if (t.company_id and (e.get("crustdata_company_id") or 0) == t.company_id) or \
                   ((e.get("name") or "").lower() == (t.company or "").lower()):
                    title = e.get("title")
                    break

            record = {
                "name": c_name,
                "title": title,
                "company": t.company,
                "linkedin_url": _linkedin(colleague),
                "overlap_window": f"{_fmt_month(ov_start)}–{_fmt_month(ov_end)}",
                "overlap_months": months,
                "seniority": c_sen,
                "seniority_weight": weight,
                "score": score,
                "crustdata_person_id": c_id,
            }
            key = c_id or (c_name, t.company)
            if key not in results or score > results[key]["score"]:
                results[key] = record

    ranked = sorted(results.values(), key=lambda r: r["score"], reverse=True)
    meta = {
        "candidate": _name(candidate_profile),
        "candidate_linkedin": _linkedin(candidate_profile),
        "companies_searched": searched,
        "n_past_companies": len(past),
        "n_referees": len(ranked),
    }
    return ranked, meta

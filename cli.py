#!/usr/bin/env python3
"""
cli.py — Backchannel end-to-end spike.

    python3 cli.py "Jane Doe"
    python3 cli.py "https://www.linkedin.com/in/<profile-slug>"
    python3 cli.py "Jane Doe" --max-companies 2 --pool 10 --listed "Some Listed Ref"

Proves the REAL legs against live Crustdata:
  discovery  ->  dated employment-overlap graph  ->  ranking
then, for the top referee, a SIMULATED reference call (the fictional sample
transcript in samples/) followed by REAL report synthesis (Haiku + Jev).

Credit guard: at most max_companies person/search calls per run (+1 to resolve the
candidate). Everything is disk-cached, so re-runs cost 0 credits.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from crust import Crust, CrustError  # noqa: E402
from overlap import find_offlist_references, _merge_companies, _name, _linkedin  # noqa: E402
import report as report_mod  # noqa: E402

SAMPLE_TRANSCRIPT = HERE / "samples" / "transcript_sample.json"


# ---------------------------------------------------------------------------
# pretty printing
# ---------------------------------------------------------------------------

def rule(char="─", n=78):
    print(char * n)


def header(title):
    rule()
    print(title)
    rule()


def _current_title(profile):
    bp = profile.get("basic_profile") or {}
    if bp.get("current_title"):
        return bp["current_title"]
    emp = (profile.get("experience") or {}).get("employment_details") or {}
    cur = emp.get("current") or []
    return cur[0].get("title") if cur else None


def print_candidate(profile):
    header("CANDIDATE")
    print(f"  Name       : {_name(profile)}")
    hl = (profile.get('basic_profile') or {}).get('headline')
    if hl:
        print(f"  Headline   : {hl}")
    print(f"  LinkedIn   : {_linkedin(profile)}")
    print(f"  Person ID  : {profile.get('crustdata_person_id')}")
    print()
    print("  Employment graph input (merged tenures):")
    cos = sorted(_merge_companies(profile), key=lambda t: t.start, reverse=True)
    for t in cos:
        tag = "CURRENT" if t.is_current else "past   "
        hc = f"{t.headcount:,}" if t.headcount else "?"
        print(f"    [{tag}] {t.company:<28} {t.window():<22} "
              f"seniority={t.seniority or '?':<20} headcount={hc}")
    print()


def print_ranked(ranked, meta):
    header("OFF-LIST REFERENCES  (ranked by dated overlap x seniority)")
    print("  Companies searched (we look for ALUMNI of each = ex-colleagues):")
    for s in meta["companies_searched"]:
        hc = f"{s['headcount']:,}" if s["headcount"] else "?"
        tag = " [current employer]" if s.get("is_current_employer") else ""
        status = "" if s.get("status") == "ok" else f"  <<{s.get('status')}>>"
        print(f"    - {s['company']}{tag}  (candidate there {s['candidate_window']}, headcount {hc}){status}")
    print()
    if not ranked:
        print("  No overlapping off-list references found.")
        print()
        return
    cols = f"  {'#':<3}{'NAME':<24}{'COMPANY':<14}{'OVERLAP WINDOW':<20}{'MOS':>4} {'W':>4} {'SCORE':>6}"
    print(cols)
    print("  " + "-" * (len(cols) - 2))
    for i, r in enumerate(ranked, 1):
        name = (r["name"] or "?")[:23]
        comp = (r["company"] or "?")[:13]
        win = r["overlap_window"][:19]
        print(f"  {i:<3}{name:<24}{comp:<14}{win:<20}{r['overlap_months']:>4}"
              f"{r['seniority_weight']:>5.2f}{r['score']:>7.1f}")
    print()
    print("  Evidence detail:")
    for i, r in enumerate(ranked, 1):
        print(f"    {i}. {r['name']} — {r['title'] or 'role n/a'}")
        print(f"       {r['company']}: overlapped {r['overlap_window']} "
              f"({r['overlap_months']} mo), seniority={r['seniority'] or '?'} "
              f"(weight {r['seniority_weight']:.2f}) -> score {r['score']}")
        print(f"       {r['linkedin_url']}")
    print()


def print_report(rep):
    header("SYNTHESIZED REFERENCE REPORT  (top referee)")
    ref = rep["referee"]
    rec = rep["recommendation"]
    print(f"  Referee     : {ref['name']}  ({ref['title']})")
    print(f"  LinkedIn    : {ref['linkedin_url']}")
    print(f"  Relationship: {textwrap.fill(rep['relationship'], 72, subsequent_indent='                ')}")
    print()
    tier = rec.get('tier') or rec.get('verdict')
    print(f"  RECOMMENDATION: {tier}  (confidence: {rec['confidence']})")
    print(f"    {textwrap.fill(rec['rationale'], 72, subsequent_indent='    ')}")
    print()
    subs = rep.get("sub_scores") or {}
    if subs:
        print("  Sub-scores (0-100):")
        for k in ("credibility", "enthusiasm", "claim_corroboration"):
            if k in subs:
                print(f"    {k.replace('_', ' '):<22}: {subs[k]}")
        print()
    print("  Claims confirmed:")
    for c in rep["claims_confirmed"]:
        print(f"    + {c}")
    print("  Strengths:")
    for s in rep["strengths"]:
        print(f"    + {s}")
    print("  Concerns:")
    for c in rep["concerns"]:
        print(f"    - {c}")
    print()
    print(f"  Synthesis   : mode={rep.get('synthesis_mode', '?')} | "
          f"scorer={rep['provenance'].get('scorer', '?')}")
    jn = rep['provenance'].get('jev_note')
    if jn:
        print(f"                {textwrap.fill(jn, 60, subsequent_indent='                ')}")
    print(f"  Provenance  : {rep['provenance']['evidence']} | "
          f"overlap {rep['provenance']['overlap_window']} | "
          f"synthesis: {rep['provenance']['synthesis']}")
    print()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def resolve_candidate(client, who, name_search_type):
    if "linkedin.com" in who.lower():
        print(f"[resolve] enriching LinkedIn URL: {who}")
        profile = client.enrich_person(linkedin_url=who)
        if not profile:
            raise SystemExit("No match for that LinkedIn URL.")
        return profile
    print(f"[resolve] person/search by name: {who!r}")
    res = client.search_people(
        [{"field": "basic_profile.name", "type": name_search_type, "value": who}], limit=1
    )
    profiles = res.get("profiles") or []
    if not profiles:
        raise SystemExit(f"No person found for name {who!r}.")
    return profiles[0]


def main():
    ap = argparse.ArgumentParser(description="Backchannel off-list reference check (spike)")
    ap.add_argument("candidate", help="candidate name OR LinkedIn profile URL")
    ap.add_argument("--max-companies", type=int, default=3)
    ap.add_argument("--pool", type=int, default=10, help="colleagues per company (<=10)")
    ap.add_argument("--listed", default="",
                    help="comma-separated names the candidate DID list (excluded as off-list)")
    ap.add_argument("--name-match", default="(.)",
                    help="match type for the name lookup ('(.)' typo-tolerant, '[.]' exact)")
    args = ap.parse_args()

    if not os.environ.get("CRUSTDATA_API_KEY"):
        raise SystemExit("CRUSTDATA_API_KEY not set. Run: source ~/.crustdata.env")

    client = Crust()
    listed = [x for x in args.listed.split(",") if x.strip()]

    print()
    header("BACKCHANNEL  —  off-list reference check (spike)")
    print("  REAL (live Crustdata): discovery, overlap graph, ranking")
    print("  REAL (AI gateway): report synthesis (Haiku) + sub-scores (Jev)")
    print("  SIMULATED: the reference call (fictional sample transcript)")
    print()

    candidate = resolve_candidate(client, args.candidate, args.name_match)
    print_candidate(candidate)

    ranked, meta = find_offlist_references(
        client, candidate,
        max_companies=args.max_companies,
        pool_per_company=min(args.pool, 10),
        listed_names=listed,
    )
    print_ranked(ranked, meta)

    if ranked:
        top = ranked[0]
        print(f"[simulated] reference call with top referee {top['name']} "
              f"(fictional sample transcript — no real call in the CLI)")
        transcript = json.loads(SAMPLE_TRANSCRIPT.read_text())
        first = (_name(candidate) or "").split(" ")[0] or "them"
        for turn in transcript.get("qa") or []:
            for k in ("q", "a"):
                turn[k] = (turn.get(k) or "").replace("{first_name}", first)
        cand_summary = {
            "name": _name(candidate),
            "current_title": _current_title(candidate),
            "linkedin_url": _linkedin(candidate),
        }
        rep = report_mod.synthesize(cand_summary, top, transcript)
        print()
        print_report(rep)

    stats = client.stats()
    header("API USAGE")
    print(f"  live API calls this run : {stats['api_calls']}")
    print(f"  cache hits this run     : {stats['cache_hits']}")
    print(f"  (every response cached under .cache/ — re-runs cost 0 credits)")
    print()


if __name__ == "__main__":
    try:
        main()
    except CrustError as e:
        raise SystemExit(f"Crustdata error: {e}")

# Backchannel

An AI agent that runs the **off-list reference check** on a founder, the part of
due diligence nobody wants to do by hand. You give it a founder. It:

1. pulls the founder's real employment history (Crustdata),
2. finds the ex-colleagues the founder did **not** list as references, ranked by a
   dated employment-overlap graph,
3. **phones the top reference** with a voice agent (Bolna) and streams the
   transcript live,
4. turns the call into a verdict (**FUND / LEAN / PASS**) with sub-scores and a draft
   intro email (Haiku for the report, Jev for scoring, via an AI gateway).

Phase 1 is a single-page web app (FastAPI + SSE) with a durable, resumable run
pipeline. A CLI version of the discovery and synthesis steps is also included.

## What's real vs simulated

| Leg | Status | How |
|-----|--------|-----|
| Founder resolution | **Real data, cache-only** | `/person/search` by name. The app serves Crustdata from the on-disk cache (`.cache/`) and never makes live calls; the CLI can call live. |
| Off-list discovery | **Real** | `/person/search` on `past.company_name` for each of the founder's employers returns alumni (ex-colleagues). |
| Dated overlap graph and ranking | **Real** | `score = overlap_months × seniority_weight(referee)` |
| Reference call | **Mock (default)** or **live** | Mock streams a fictional sample transcript and is labelled "Simulated" in the UI. Live places a real Bolna call to one allowlisted, verified number. |
| Report synthesis | **Real** | Haiku writes the report as strict JSON and Jev scores credibility, enthusiasm and claim corroboration. There's a 25-second deadline. In mock mode, a slow or failed synthesis falls back to a pre-saved report (`samples/fallback_report.json`). |
| Call-back to the VC, founder call, meeting booking | **Phase 2** | Shown as Phase 2 steps. Not built. |

The referee is always shown by **role and company**, never by name.

## Run the web app

```bash
cat > .env.local <<'EOF'          # git-ignored; example values only
GATEWAY_URL=https://<your-gateway>/v1
GATEWAY_KEY=<your-key>
BACKCHANNEL_DEMO_FOUNDER="Jane Doe"
BACKCHANNEL_REF_PHONE=+919800000000
BACKCHANNEL_ALLOWED_PHONES=+919800000000
EOF
./run.sh                                  # mock mode on http://0.0.0.0:8900
CALL_MODE=live HOST=127.0.0.1 ./run.sh    # live calls, bound locally behind a tunnel
```

`run.sh` sources `$CRUSTDATA_ENV_FILE` (default `~/.crustdata.env`),
`$BOLNA_ENV_FILE` (default `~/.bolna.env`) and `./.env.local`. All three are
git-ignored and none of their values are ever printed.

### Environment

| Variable | Purpose |
|----------|---------|
| `CRUSTDATA_API_KEY` | Crustdata key. The app runs cache-only, so it's only needed for the CLI. |
| `BOLNA_API_KEY` | Bolna key. Live mode only. |
| `GATEWAY_URL`, `GATEWAY_KEY` | AI gateway (OpenAI-compatible) used for synthesis. |
| `HAIKU_MODEL`, `JEV_MODEL` | Optional model overrides. |
| `CALL_MODE` | `mock` (default) or `live`. Applies to new runs; each run keeps the mode it started with. |
| `BACKCHANNEL_REF_PHONE` | Number the live call dials. Empty by default. |
| `BACKCHANNEL_ALLOWED_PHONES` | Comma-separated allowlist. The live call refuses any other number. |
| `BACKCHANNEL_DEMO_FOUNDER` | The founder whose data is pre-cached. Names typed in the form are matched to it case-insensitively and typo-tolerantly. |
| `BACKCHANNEL_DEMO_COMPANY`, `BACKCHANNEL_DEMO_VC_NAME` | Optional form defaults, served by `/healthz`. |
| `BACKCHANNEL_DEMO_TOKEN` | If set, `POST /run` requires it. Open the UI once as `/?token=…`. `run.sh` refuses live mode on a non-local host without it. |
| `BACKCHANNEL_DB` | SQLite path (default `.state/runs.db`). |
| `BACKCHANNEL_SYNTH_TIMEOUT_S`, `BACKCHANNEL_CALL_TIMEOUT_S`, `BACKCHANNEL_CALL_HARD_CAP_S`, `BACKCHANNEL_POLL_S` | Timing knobs. Defaults: 25s, 240s to connect, 480s once connected, 4s. |

### Safety properties

- **No double calls.** Each submit carries its own idempotency key, so a retry of
  the same submit gets the same run back. Identical inputs join a run that's still
  in flight. An errored or finished run is never restarted.
- **Resume never dials.** After a restart, only runs touched in the last
  15 minutes resume, using their own call mode. A live run resumes by polling its
  stored Bolna `execution_id`. If it was interrupted before that id was saved, it's
  marked as an error rather than re-dialed.
- **One live call at a time.** POSTs are rate-limited (5 new runs a minute).
- **Missed calls end fast.** `no-answer`, `busy`, `failed`, `canceled`, `stopped`,
  `error` and `balance-low` end the call step at once with a readable message.

## Run the CLI

```bash
source ~/.crustdata.env && source .env.local
python3 cli.py "Jane Doe"                                   # by name
python3 cli.py "https://www.linkedin.com/in/<profile-slug>"  # by LinkedIn URL
python3 cli.py "Jane Doe" --max-companies 3 --pool 10 --listed "Someone They Did List"
```

The CLI prints the founder's employment graph, then the ranked off-list referees
with their real overlap windows, then a synthesized report for the top referee
(built from the fictional sample transcript), then an API-usage summary.

## Credit conservation (limited hackathon key)

- **At most `max_companies` search calls per CLI run** (default 3), plus one to
  resolve the candidate. Each uses `limit ≤ 10`, and `crust.py` refuses a higher
  limit.
- **Every response is cached to `.cache/`**, keyed by a hash of the request. Re-runs
  cost 0 credits. `.cache/` is git-ignored because it holds real profile data.
- The web app is **cache-only**: a cache miss is an error, not a live call.
- If the key hits its monthly cap (`HTTP 402`) or a rate limit (`429`), that one
  company search is skipped and the run continues.

## Files

- `app.py`: FastAPI routes, including dedupe, rate limit, the optional token, and SSE.
- `pipeline.py`: the resumable 7-step state machine, the mock and live call, and
  synthesis with a deadline and fallbacks.
- `db.py`: durable run state (SQLite, under `.state/`, git-ignored).
- `crust.py`: thin Crustdata client with disk cache and call counters.
- `overlap.py`: the core logic. Tenure parsing, `overlap_months`, seniority weighting
  and `find_offlist_references`.
- `bolna.py`: Bolna client (`place_call`, `get_execution`, transcript parsing).
- `gateway.py`: AI gateway client (`chat` for Haiku, `evaluate` for Jev).
- `report.py`: reference-report synthesizer (Haiku and Jev, with fallbacks).
- `cli.py`: end-to-end CLI runner.
- `web/index.html`: the single-page UI.
- `samples/transcript_sample.json`: a **fictional** reference-call transcript.
  It's the mock call's script.
- `samples/fallback_report.json`: the pre-saved report used if mock-mode synthesis
  fails.
- `spec/`: product, tech and UI specs.

## Known limitations

- **Short or generic company names** over-match on the `[.]` phrase filter and
  dilute the top-10 pool, so they often yield no clean overlap. Distinctive names
  work well. Fixes for production: search by `crustdata_company_id`, raise the pool
  with pagination, or use live search.
- Overlap is a **proxy** for "knew each other". A co-founder and a junior employee
  can overlap for years without close contact, and seniority weighting is only a
  rough correction. Production would add title-adjacency and team signals.
- Live mode dials one pre-verified number, which stands in for the referee. Finding
  contact details (`/person/contact/enrich`) is out of scope.
- The demo serves one pre-cached founder (`BACKCHANNEL_DEMO_FOUNDER`).

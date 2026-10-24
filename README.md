# Backchannel

An AI agent that runs the **off-list reference check** on a founder, the part of
due diligence nobody wants to do by hand. You give it a founder. It:

1. pulls the founder's real employment history (Crustdata),
2. finds the ex-colleagues the founder did **not** list as references, ranked by a
   dated employment-overlap graph,
3. **phones the top reference** with a voice agent (Bolna) and streams the
   transcript live,
4. turns the call into a verdict (**FUND / LEAN / PASS**) with sub-scores and a draft
   intro email (Haiku for the report, Jev for scoring, via an AI gateway),
5. **calls the VC back** with the verdict and asks when they're free to meet,
6. **calls the founder**, matches their availability to the VC's slots and confirms
   a time, then **emails the founder a summary with a calendar invite** (`.ics`).
   The run ends with the verdict and a booked meeting.

Give it a **role** (what you're hiring or funding for) and the reference call asks
about fit for that role, and the verdict becomes a fit-for-the-role verdict. Don't
have a name? `POST /search` turns the role into a ranked list of people to pick from.

It's a single-page web app (FastAPI + SSE) with a durable, resumable run pipeline:
three phone calls, nobody in the loop. A CLI version of the discovery and synthesis
steps is also included.

## What's real vs simulated

| Leg | Status | How |
|-----|--------|-----|
| Founder resolution | **Real, live + cached** | `/person/search` by name, or by `crustdata_person_id` when the founder was picked from `/search`. With a company, a common name resolves to the person who worked there. Live by default (`CRUSTDATA_LIVE=1`), every response disk-cached under `.cache/` so a repeat is free; a 402 or network error falls back to the cache. `CRUSTDATA_LIVE=0` = cache-only. |
| Candidate search (no name) | **Real** | `POST /search {role}`: Crustdata's natural-language people search (`search.query`, `hybrid`), plus a title filter when the role names one ("founder", "CTO"). Haiku writes a one-line `why_fit` per person and flags non-matches (e.g. an investor for a founder role), which are listed last. |
| Off-list discovery | **Real** | `/person/search` on `past.company_name` for each of the founder's employers returns alumni (ex-colleagues). |
| Dated overlap graph and ranking | **Real** | `score = overlap_months × seniority_weight(referee)` |
| Reference call | **Mock (default)** or **live** | Mock streams a fictional sample transcript and is labelled "Simulated" in the UI; with a role, the script asks about fit for it. Live places a real Bolna call to one allowlisted, verified number and passes the role and candidate in `user_data` (only the role-aware agent, `BACKCHANNEL_AGENT_ID`, uses them). |
| Report synthesis | **Real** | Haiku writes the report as strict JSON and Jev scores credibility, enthusiasm and claim corroboration. With a role, the tier and rationale judge fit for that role and the report adds `role_fit` (strong / moderate / weak / unclear). There's a 25-second deadline. In mock mode, a slow or failed synthesis falls back to a pre-saved report (`samples/fallback_report.json`). |
| Call-back to the VC (call 2) | **Mock (default)** or **live** | Mock streams `samples/vc_call_sample.json`, with the agent's lines filled from this run's real verdict and highlights. Live calls the VC's typed number, which must be allowlisted. Haiku extracts the VC's free slots, with a 15-second deadline and a cautious regex fallback. Live mode never invents slots. If the call can't be read, the founder isn't called. If the VC gave no firm day, or only the regex could read the call, the booking is marked tentative. |
| Founder call and booking (call 3) | **Mock (default)** or **live** | Mock streams `samples/founder_call_sample.json`. Live calls `BACKCHANNEL_FOUNDER_PHONE`. Haiku extracts the confirmed time, which is checked against the VC's slots. "Today" and "tomorrow" are resolved in `BACKCHANNEL_TZ`. After a PASS, the founder is offered a meeting, not funding. |
| Contact lines on the cards | **Real, masked + cached** | Each shown candidate and each shown referee (top + 3) gets a masked email (`p•••.c•••@stripe.com`) and phone (`+1 31XXX X1004`) from `/person/contact/enrich`, by `crustdata_person_id` (else the LinkedIn URL). The raw answer stays in `.cache/`; only the masked forms reach the UI, the DB and the logs. No match, an error or a missing field falls back to a masked placeholder (`contact_source` says which). Display-only, never dialed. |
| Founder summary email | **Real (Resend)** | After a firm booking, the `done` step emails `BACKCHANNEL_FOUNDER_EMAIL` the meeting time, a one-line "a VC wants to meet you" and an `.ics` invite (30 minutes, organizer = the VC, attendee = the founder). The slot ("Thursday 3:00 PM") becomes the next such date in `BACKCHANNEL_TZ`. Mock runs send it too, marked "Simulated demo". No Resend config = skipped, with the reason in `result.email`. |

The referee is always shown by **role and company**, never by name. If the VC or
founder call fails, the run still finishes with the verdict, and the meeting is
marked as not booked.

## Run the web app

```bash
cat > .env.local <<'EOF'          # git-ignored; example values only
GATEWAY_URL=https://<your-gateway>/v1
GATEWAY_KEY=<your-key>
BACKCHANNEL_DEMO_FOUNDER="Jane Doe"
BACKCHANNEL_REF_PHONE=+919800000000
BACKCHANNEL_FOUNDER_PHONE=+919800000001
BACKCHANNEL_ALLOWED_PHONES=+919800000000,+919800000001,+919800000002
EOF
./run.sh                                  # mock mode on http://0.0.0.0:8900
CALL_MODE=live HOST=127.0.0.1 ./run.sh    # live calls, bound locally behind a tunnel
```

Live mode needs two more Bolna agents, one for the VC call-back and one for the
founder call. Create them once (or reuse them by name), then add the two printed
lines to `.env.local`:

```bash
source ~/.bolna.env && python3 setup_agents.py     # --update pushes edited prompts
```

Each agent's prompt and welcome message use `{{placeholders}}`. Every `POST /call`
fills them from its `user_data`: the verdict, highlights and watch-out for the VC,
and the VC's slots and name for the founder.

`run.sh` sources `$CRUSTDATA_ENV_FILE` (default `~/.crustdata.env`),
`$BOLNA_ENV_FILE` (default `~/.bolna.env`), `$RESEND_ENV_FILE` (default
`~/.resend.env`) and `./.env.local`. All of them are git-ignored and none of their
values are ever printed.

### API

- `POST /run` takes `vc_name`, `vc_phone`, `founder_name`, `founder_company`,
  `idempotency_key`, plus two optional fields: `role` (what you're hiring or funding
  for) and `founder_person_id` (the `person_id` of a candidate picked from
  `/search`, which pins the exact person). Both are stored on the run, and returned
  under `inputs` by `GET /run/{id}`.
- `POST /search {role, limit?}` returns
  `{role, candidates: [{rank, name, title, company, linkedin, headline, location, why_fit, fit, matches_role, person_id}], count, message, source, cached}`.
  It returns at most 8 candidates (6 by default) and is read-only. Results are
  disk-cached, and a cached role is answered without using the rate limit (5 new
  searches a minute per IP). A failure is never a 500: you get an empty list and a
  readable `message`.
- The SSE contract is unchanged: the same 7 steps and event shapes. New data rides in
  existing payloads: `result.role`, `result.role_fit`,
  `result.email = {sent, to, id?, status, subject, simulated, invite: {start, end, timezone, when}, reason?}`
  (`to` is masked, as in `fo•••@gmail.com`, because run results are public),
  the `done` step's `email_sent`, the synthesize step's `role_fit`, and a
  `crustdata` block (`live`, `api_calls`, `cache_hits`) on steps 1-2.

### Environment

| Variable | Purpose |
|----------|---------|
| `CRUSTDATA_API_KEY` | Crustdata key. Without it the app runs cache-only. |
| `CRUSTDATA_LIVE` | `1` (default): live lookups for anything not in `.cache/`. `0`: cache-only, zero live calls. A 402 pauses live calls for 10 minutes. |
| `BACKCHANNEL_CONTACT_ENRICH` | `1` (default): contact-enrich anyone shown who isn't in `.cache/` yet. `0`: cached contacts only, placeholders for the rest, no credits. A 401/402/403 pauses new lookups for 10 minutes. |
| `BACKCHANNEL_REF_CONTACTS` | `real` (default): referee cards show the real masked contact. `placeholder`: referee cards get salted placeholders and no lookup. Use it when referee anonymity matters: a real contact is the same masked string as that person's own candidate card, and the email shows their initials and domain. |
| `BACKCHANNEL_CONTACT_TIMEOUT_S` | How long a `/search` or the find-references step waits for contacts (10s). A slower lookup shows the placeholder, finishes in the background and is cached for next time. |
| `RESEND_API_KEY`, `RESEND_FROM`, `BACKCHANNEL_FOUNDER_EMAIL` | The founder summary email. All three are needed, or the email is skipped. Resend's test mode only delivers to the account owner's address. |
| `BACKCHANNEL_VC_EMAIL` | Optional `.ics` organizer address. Defaults to the `RESEND_FROM` address. |
| `BACKCHANNEL_EMAIL` | `0` turns the founder email off, for example on a public mock demo where every booked run would otherwise send one (Resend's free tier allows 100 a day). |
| `BACKCHANNEL_AGENT_ID` | Optional role-aware reference agent (`setup_agents.py`). Unset = the proven Phase-1 reference agent, which ignores the role. |
| `BOLNA_API_KEY` | Bolna key. Live mode only. |
| `GATEWAY_URL`, `GATEWAY_KEY` | AI gateway (OpenAI-compatible) used for synthesis. |
| `HAIKU_MODEL`, `JEV_MODEL` | Optional model overrides. |
| `CALL_MODE` | `mock` (default) or `live`. Applies to new runs; each run keeps the mode it started with. |
| `BACKCHANNEL_REF_PHONE` | Number the live reference call dials. Empty by default. |
| `BACKCHANNEL_FOUNDER_PHONE` | Number the live founder call dials. Empty by default. |
| `BACKCHANNEL_ALLOWED_PHONES` | Comma-separated allowlist of verified numbers. A live run is refused up front unless the reference number, the VC's typed number (normalized) and the founder number are all on it. |
| `BACKCHANNEL_VC_AGENT_ID`, `BACKCHANNEL_FOUNDER_AGENT_ID` | The Phase-2 Bolna agents created by `setup_agents.py`. Live mode only. |
| `BACKCHANNEL_DEMO_FOUNDER` | The founder whose data is pre-cached. Names typed in the form are matched to it case-insensitively and typo-tolerantly. |
| `BACKCHANNEL_DEMO_COMPANY`, `BACKCHANNEL_DEMO_VC_NAME` | Optional form defaults, served by `/healthz`. |
| `BACKCHANNEL_DEMO_TOKEN` | If set, `POST /run` requires it. Open the UI once as `/?token=…`. `run.sh` refuses live mode on a non-local host without it. |
| `BACKCHANNEL_DB` | SQLite path (default `.state/runs.db`). |
| `BACKCHANNEL_TZ` | Time zone used to resolve "today" and "tomorrow" on the calls (default `Asia/Kolkata`). |
| `BACKCHANNEL_SYNTH_TIMEOUT_S`, `BACKCHANNEL_EXTRACT_TIMEOUT_S`, `BACKCHANNEL_CALL_TIMEOUT_S`, `BACKCHANNEL_CALL_HARD_CAP_S`, `BACKCHANNEL_POLL_S` | Timing knobs. Defaults: 25s, 15s per slot or time extraction (6s in mock mode), 240s to connect, 480s once connected, 4s. |
| `BACKCHANNEL_EMAIL_TIMEOUT_S`, `BACKCHANNEL_WHYFIT_TIMEOUT_S`, `BACKCHANNEL_CRUST_TIMEOUT_S` | Resend send deadline (20s), the `/search` why-fit deadline (8s) and the per-request live Crustdata timeout (20s). A timeout also pauses live lookups for 60s. |

### Safety properties

- **No double calls.** Each submit carries its own idempotency key, so a retry of
  the same submit gets the same run back. Identical inputs join a run that's still
  in flight. An errored or finished run is never restarted.
- **Resume never dials.** After a restart, only runs touched in the last
  15 minutes resume, using their own call mode. Each of the three calls stores its
  own Bolna `execution_id` as soon as it's placed, and a resumed live run polls it.
  A resumed run never places a call. If the restart came before a call's id was
  saved, or before the call was placed at all, that call is reported, not dialed.
  For the VC or founder call, the run still finishes with the verdict. A run that
  expires after its verdict is ready also finishes with the verdict, marked not booked.
- **Displayed contacts are never dialed.** The three calls dial
  `BACKCHANNEL_REF_PHONE`, the VC's typed number and `BACKCHANNEL_FOUNDER_PHONE`
  only. A masked number always contains `X`, and `_live_call` refuses it before
  `bolna.place_call` even if it's on the allowlist. `live_preflight` refuses one
  typed as the VC number.
- **Old runs finish the old way.** A run started before Phase 2 keeps steps 5-6 as
  Phase 2 placeholders when it resumes.
- **One live run at a time.** POSTs are rate-limited (5 new runs a minute).
- **One email, ever.** The founder email's state lives on the run and is written
  before and after the send. A resume reuses a finished send and never re-sends. A
  send cut off mid-flight is retried with the same Resend `Idempotency-Key` and a
  byte-identical payload (the dates, `DTSTAMP` and `UID` come from the run, not the
  clock), so Resend de-duplicates it.
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
- The web app looks things up **live behind the cache** (`CRUSTDATA_LIVE=1`). A new
  founder costs one resolve plus at most three company searches, and a `/search`
  costs one search of 8 results. Set `CRUSTDATA_LIVE=0` for a strictly cache-only
  demo.
- **Contact-enrich costs 5 credits a person** (a not-found answer is free). Only the
  people shown are looked up (at most 6 per `/search`, 4 referees per run), each one
  once ever: the answer, not-found included, is cached per person id. Two lookups
  of one person at the same time share a request.
- If the key hits its monthly cap (`HTTP 402`), a rate limit (`429`) or a network
  error, that one lookup falls back to the cache. A missed company search is
  skipped and the run continues. A 402 also pauses live calls for 10 minutes.

## Files

- `app.py`: FastAPI routes, including dedupe, rate limit, the optional token, and SSE.
- `pipeline.py`: the resumable 7-step state machine, the mock and live calls (the
  reference, the VC and the founder), and synthesis with a deadline and fallbacks.
- `scheduling.py`: the glue between calls 2 and 3. Haiku extracts the VC's slots and
  the confirmed time, with regex fallbacks and slot matching.
- `setup_agents.py`: creates or reuses the VC and founder Bolna agents and the
  role-aware reference agent.
- `candidates.py`: `POST /search`. Natural-language Crustdata search, why-fit lines,
  ranking and caching.
- `invite.py`: the founder summary email. Turns the slot into a date, builds the
  RFC 5545 `.ics` and sends through Resend with an idempotency key.
- `db.py`: durable run state (SQLite, under `.state/`, git-ignored).
- `crust.py`: thin Crustdata client with disk cache and call counters.
- `contacts.py`: contact-enrich for the cards (per-person cache, deadline, fallback).
- `placeholder_contacts.py`: email/phone masking, the masked placeholder fallback,
  and `is_placeholder_phone` (the never-dial guard).
- `overlap.py`: the core logic. Tenure parsing, `overlap_months`, seniority weighting
  and `find_offlist_references`.
- `bolna.py`: Bolna client (`place_call` with per-call `user_data`, `get_execution`,
  transcript parsing, phone normalization).
- `gateway.py`: AI gateway client (`chat` for Haiku, `evaluate` for Jev).
- `report.py`: reference-report synthesizer (Haiku and Jev, with fallbacks).
- `cli.py`: end-to-end CLI runner.
- `web/index.html`: the single-page UI.
- `samples/transcript_sample.json`: a **fictional** reference-call transcript.
  It's the mock call's script.
- `samples/fallback_report.json`: the pre-saved report used if mock-mode synthesis
  fails.
- `samples/vc_call_sample.json`, `samples/founder_call_sample.json`: **fictional**
  scripts for the mock VC and founder calls. `{placeholders}` are filled from the
  run. Each has an `expected` block that mock mode falls back to if extraction is
  slow.
- `spec/`: product, tech and UI specs.

## Known limitations

- **Short or generic company names** over-match on the `[.]` phrase filter and
  dilute the top-10 pool, so they often yield no clean overlap. Distinctive names
  work well. Fixes for production: search by `crustdata_company_id`, raise the pool
  with pagination, or use live search.
- Overlap is a **proxy** for "knew each other". A co-founder and a junior employee
  can overlap for years without close contact, and seniority weighting is only a
  rough correction. Production would add title-adjacency and team signals.
- Live mode dials pre-verified numbers that stand in for the referee and the founder.
  The real contacts from `/person/contact/enrich` are only displayed (masked), never
  dialed. A card can mix a real email with a placeholder phone when Crustdata has
  only one of them.
- Slots are weekday-level ("Thursday afternoon"). The invite turns the booked slot
  into the next such date in `BACKCHANNEL_TZ`, and is sent only to
  `BACKCHANNEL_FOUNDER_EMAIL` (the VC isn't emailed).
- Name-only resolution takes Crustdata's top fuzzy match, so a common name can
  resolve to the wrong person. Passing `founder_company` or a `founder_person_id`
  from `/search` avoids that.
- `BACKCHANNEL_DEMO_FOUNDER` is pre-cached. Anyone else is looked up live (and then
  cached).

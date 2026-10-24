# Backchannel — Tech Spec

**What it is:** one backend **orchestrator** that runs the job as a step-by-step state machine, drives 3 phone calls, and streams every step to the UI live.

```
Browser (UI)  ⇄  Orchestrator API (FastAPI)  →  Crustdata · Bolna · AI Gateway
   │  POST /run  (start)                            (data)   (calls)  (Haiku+Jev)
   └─ GET /run/{id}/events  (SSE live stream)
```

---

## The pipeline (a state machine — one step = one timeline row)

| # | Step | What it does | Tech |
|---|------|--------------|------|
| 1 | **resolve_founder** | name (+company) → profile + dated work history | Crustdata `person/search` → `person/enrich` |
| 2 | **find_references** | ex-colleagues per past employer → rank by overlap → top 1 | Crustdata `person/search` + `overlap.py` |
| 3 | **call_reference** | call the ref, verify the founder, ask diligence Qs | Bolna `POST /call` → poll → transcript |
| 4 | **synthesize_signal** | transcript → verdict + scores | Gateway: Haiku (write) + Jev (score) |
| 5 | **call_vc** | call the VC: deliver signal, ask "when are you free?" | Bolna call → transcript → Haiku extracts **VC slots** |
| 6 | **call_founder** | call founder: "a VC wants to fund you", get availability, confirm a slot | Bolna call → transcript → Haiku extracts **confirmed time** |
| 7 | **done** | result: verdict + booked time + draft intro email | — |

Each step emits an event **on start** (`active`) and **on finish** (`done` + result payload).

---

## The 3 key mechanisms

**1. Dynamic call context.** One Bolna agent per role (reference / VC / founder), or one agent + per-call variables. Each `POST /call` passes the specifics — founder name, the questions, the VC's slots — as call-level variables that fill `{{placeholders}}` in the agent prompt. *(Confirm Bolna's variable field name when wiring; fallback = per-call prompt override.)*

**2. Transcript → structured data (the glue between calls).** After each call, Haiku parses the transcript to pull exactly what the next step needs:
- after call 3 (ref) → the **signal** (strengths, concerns, would-back)
- after call 5 (VC) → the **available slots** ("Thursday or Friday afternoon")
- after call 6 (founder) → the **confirmed time**

**3. Live updates.** Orchestrator keeps a run record `{run_id: {steps[], status}}`. Every step change is pushed to an **SSE** stream the UI subscribes to (1s polling fallback). Record persists (SQLite) so if the VC leaves and returns, the page resumes where it is. Calls are **fire-and-poll** — trigger, then `GET /executions/{id}` every ~3–5s until finalized.

---

## Stack
- **Backend:** Python + FastAPI (async), SSE for live events.
- **Data:** Crustdata REST (served from cache while the cap is capped).
- **Voice:** Bolna REST — the 3 calls.
- **AI:** our Vercel AI Gateway — **Haiku** (generate/extract) + **Jev** (score).
- **State:** SQLite (or in-memory for the demo).
- **Frontend:** minimal — Next.js or plain HTML+JS hitting `POST /run` + the SSE stream.

## Reuse (already built + proven)
`crust.py` · `overlap.py` · `report.py` (synthesis) · `gateway.py` — from the spike. The orchestrator wraps these and adds the Bolna calls + the VC/founder legs + SSE. **The Crustdata→call→transcript→verdict chain is already proven on a real +91 call.**

---

## Demo scope / stubs
- Crustdata cap not lifted → run from the **warm cache** (one pre-cached founder).
- All 3 numbers = **controlled + verified** in Bolna.
- Calendar invite = optional; the verbal confirm on the calls is the core.
- Out: ref-doesn't-know-founder retries, multi-ref, multi-founder, auth.

## Build order
1. **Orchestrator skeleton + SSE + the Start→timeline UI** (fake events first) → proves the live view.
2. Plug in **steps 1–2** (Crustdata discovery — already built).
3. Wire **call 1** (reference → transcript → synthesis — already proven).
4. Wire **call 2** (VC) + slot extraction.
5. Wire **call 3** (founder) + confirm.
6. **Result screen** + polish.

Ship step 1–3 first = a working, demo-able core. 4–6 = the full wow.

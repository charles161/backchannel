"""
app.py — Backchannel FastAPI orchestrator + UI (Phase 1).

Routes:
  GET  /                     -> the single-page UI (web/index.html)
  POST /run                  -> start (or dedupe to) a run; returns {run_id, existing}
  GET  /run/{run_id}         -> full persisted state (resume / polling fallback)
  GET  /run/{run_id}/events  -> SSE: snapshot FIRST, then live step/transcript events
  GET  /healthz              -> liveness + call mode + demo form defaults

Durable + resumable: state lives in SQLite (db.py). A refresh reconnects by run_id
(no re-POST); a server restart relaunches recent unfinished runs on startup.

Dedupe (POST /run):
  * the UI sends a fresh idempotency_key per submit; a retry of that SAME key gets
    the same run back (never a second run or a second phone call);
  * identical inputs join a run that is still in flight; key-less clients also get
    a run that finished < 2 min ago. Anything older starts a fresh run;
  * an existing run is only (re)launched while its status is "running" — an errored
    or finished run is never restarted (no silent re-dial);
  * the rate limit is checked AFTER dedupe, so a retry never burns the budget.

Optional access control: if BACKCHANNEL_DEMO_TOKEN is set, POST /run requires it
(open the UI as /?token=...). Only one live call may run at a time.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid
from collections import defaultdict, deque
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import db
import pipeline

HERE = Path(__file__).resolve().parent
INDEX_HTML = HERE / "web" / "index.html"

app = FastAPI(title="Backchannel", docs_url=None, redoc_url=None)

# ---- simple per-IP rate limit on POST /run (credit guard) ------------------
_RATE_MAX = 5
_RATE_WINDOW_S = 60.0
_rate_hits: dict[str, deque] = defaultdict(deque)
_DEDUPE_RECENT_S = 120.0
DEMO_TOKEN = os.environ.get("BACKCHANNEL_DEMO_TOKEN", "").strip()


def _rate_limited(ip: str) -> bool:
    now = time.time()
    dq = _rate_hits[ip]
    while dq and now - dq[0] > _RATE_WINDOW_S:
        dq.popleft()
    if len(dq) >= _RATE_MAX:
        return True
    dq.append(now)
    return False


def _inputs_hash(inputs: dict, mode: str) -> str:
    norm = "|".join([
        (inputs.get("vc_name") or "").strip().lower(),
        (inputs.get("vc_phone") or "").strip(),
        (inputs.get("founder_name") or "").strip().lower(),
        (inputs.get("founder_company") or "").strip().lower(),
        mode,
    ])
    return hashlib.sha256(norm.encode()).hexdigest()[:24]


def _err(status: int, code: str, detail: str) -> JSONResponse:
    return JSONResponse({"error": code, "detail": detail}, status_code=status)


def _existing(run_id: str) -> dict:
    run = db.get_run(run_id)
    if run and run["status"] == "running":
        pipeline.ensure_running(run_id)  # resume in this process; never re-dials
    return {"run_id": run_id, "existing": True}


@app.on_event("startup")
async def _startup() -> None:
    db.init_db()
    await pipeline.resume_unfinished()


@app.get("/healthz")
async def healthz():
    return {
        "ok": True,
        "call_mode": pipeline.CALL_MODE,
        "token_required": bool(DEMO_TOKEN),
        # stage form defaults come from env (.env.local), never from the repo
        "defaults": {
            "vc_name": os.environ.get("BACKCHANNEL_DEMO_VC_NAME", "").strip(),
            "founder_name": pipeline.DEMO_FOUNDER,
            "founder_company": os.environ.get("BACKCHANNEL_DEMO_COMPANY", "").strip(),
        },
    }


@app.get("/")
async def index():
    return FileResponse(str(INDEX_HTML))


@app.post("/run")
async def start_run(request: Request):
    ip = request.client.host if request.client else "?"
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    if DEMO_TOKEN:
        given = str(body.get("demo_token") or request.headers.get("x-demo-token") or "")
        if not hmac.compare_digest(given.encode(), DEMO_TOKEN.encode()):
            return _err(403, "forbidden", "This demo needs its access link (…/?token=…).")

    inputs = {
        "vc_name": str(body.get("vc_name") or "").strip()[:120],
        "vc_phone": str(body.get("vc_phone") or "").strip()[:40],
        "founder_name": pipeline.canonical_founder(str(body.get("founder_name") or "")[:120]),
        "founder_company": str(body.get("founder_company") or "").strip()[:120],
    }
    if not inputs["founder_name"]:
        return _err(400, "missing_founder", "Enter the founder's name to check.")
    mode = pipeline.CALL_MODE
    ihash = _inputs_hash(inputs, mode)
    explicit = str(body.get("idempotency_key") or "").strip()[:100] or None

    # 1) a retry of the very same submit -> the same run, as-is.
    if explicit:
        rid = db.find_run_id_by_idem(f"key:{explicit}")
        if rid:
            return _existing(rid)

    # 2) identical inputs -> join a run that is still in flight (key-less clients
    #    also get one that finished moments ago). Older/errored runs start fresh.
    recent = db.latest_run_by_hash(ihash)
    if recent:
        in_flight = recent["status"] == "running"
        just_done = (not explicit and recent["status"] == "done"
                     and time.time() - (recent["updated_at"] or 0) < _DEDUPE_RECENT_S)
        if in_flight or just_done:
            return _existing(recent["run_id"])

    # 3) only genuinely new runs count against the rate limit.
    if _rate_limited(ip):
        return _err(429, "rate_limited",
                    f"Too many checks in a minute (max {_RATE_MAX}). Wait a moment and retry.")
    if mode == "live" and pipeline.live_call_in_progress():
        return _err(409, "call_in_progress",
                    "A live call is already in progress. Wait for it to finish.")

    run_id = uuid.uuid4().hex[:12]
    idem = f"key:{explicit}" if explicit else f"hash:{ihash}:{run_id}"
    db.create_run(run_id, idem, inputs, mode,
                  pipeline.initial_steps(inputs["founder_name"]), inputs_hash=ihash)
    pipeline.ensure_running(run_id, fresh=True, mode=mode)
    return {"run_id": run_id, "existing": False}


@app.get("/run/{run_id}")
async def get_run(run_id: str):
    run = db.get_run(run_id)
    if not run:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return run


def _sse(obj: dict) -> str:
    return f"data: {json.dumps(obj)}\n\n"


@app.get("/run/{run_id}/events")
async def run_events(run_id: str, request: Request):
    import asyncio

    run = db.get_run(run_id)
    if not run:
        return JSONResponse({"error": "not_found"}, status_code=404)

    async def gen():
        q = pipeline.subscribe(run_id)
        try:
            # 1) snapshot FIRST — repaints the whole timeline instantly on (re)connect.
            snap = db.get_run(run_id)
            yield _sse({"type": "snapshot", "run": snap})

            # make sure the pipeline is actually progressing in this process
            if snap and snap["status"] == "running":
                pipeline.ensure_running(run_id)
            else:
                # already finished: hand over the final result, then close.
                if snap and snap.get("result"):
                    yield _sse({"type": "done", "result": snap["result"]})
                yield _sse({"type": "end"})
                return

            # 2) then stream live events.
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
                    continue
                yield _sse(ev)
                if ev.get("type") == "end":
                    break
        finally:
            pipeline.unsubscribe(run_id, q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # disable proxy buffering (cloudflared/nginx)
        },
    )

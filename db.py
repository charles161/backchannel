"""
db.py — durable run state for Backchannel (SQLite).

Source of truth for every run. Survives a screen refresh AND a server restart:
GET /run/{id} and the SSE snapshot are both built from here, so a client that
reconnects — or a process that was killed mid-run — resumes with nothing lost.

Two tables:
  runs(run_id, idem_key UNIQUE, inputs_hash, inputs..., status, result_json, error, timestamps)
  steps(run_id, step, idx, label, status, result_json, execution_id, phase2)

All writes happen on the asyncio loop thread (the pipeline's blocking work runs in
threads but only READS env/cache/network and returns data; it never touches the DB).
A lock is held anyway so a stray concurrent write can't corrupt a row.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve().parent
_DEFAULT_DB = _HERE / ".state" / "runs.db"
DB_PATH = Path(os.environ.get("BACKCHANNEL_DB", str(_DEFAULT_DB)))

_LOCK = threading.Lock()
_CONN: sqlite3.Connection | None = None


def _conn() -> sqlite3.Connection:
    global _CONN
    if _CONN is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONN = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _CONN.row_factory = sqlite3.Row
        _CONN.execute("PRAGMA journal_mode=WAL")
        _CONN.execute("PRAGMA synchronous=NORMAL")
    return _CONN


def init_db() -> None:
    with _LOCK:
        c = _conn()
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id      TEXT PRIMARY KEY,
                idem_key    TEXT UNIQUE,
                vc_name     TEXT,
                vc_phone    TEXT,
                founder_name TEXT,
                founder_company TEXT,
                call_mode   TEXT,
                status      TEXT,
                result_json TEXT,
                error       TEXT,
                created_at  REAL,
                updated_at  REAL
            );
            CREATE TABLE IF NOT EXISTS steps (
                run_id       TEXT,
                step         TEXT,
                idx          INTEGER,
                label        TEXT,
                status       TEXT,
                result_json  TEXT,
                execution_id TEXT,
                phase2       INTEGER DEFAULT 0,
                PRIMARY KEY (run_id, step)
            );
            """
        )
        # migration: inputs_hash (dedupe of identical in-flight submits) on old DBs
        cols = {r["name"] for r in c.execute("PRAGMA table_info(runs)").fetchall()}
        if "inputs_hash" not in cols:
            c.execute("ALTER TABLE runs ADD COLUMN inputs_hash TEXT")
        c.execute("CREATE INDEX IF NOT EXISTS runs_inputs_hash ON runs(inputs_hash, created_at)")
        c.commit()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _loads(v: str | None) -> Any:
    if not v:
        return None
    try:
        return json.loads(v)
    except (json.JSONDecodeError, TypeError):
        return None


def now() -> float:
    return time.time()


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------

def create_run(run_id: str, idem_key: str, inputs: dict, call_mode: str,
               steps: list[dict], inputs_hash: str | None = None) -> None:
    """Insert a new run + its initial (pending) steps, atomically."""
    with _LOCK:
        c = _conn()
        ts = now()
        c.execute(
            "INSERT INTO runs (run_id, idem_key, inputs_hash, vc_name, vc_phone, "
            "founder_name, founder_company, call_mode, status, result_json, error, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, idem_key, inputs_hash, inputs.get("vc_name"), inputs.get("vc_phone"),
             inputs.get("founder_name"), inputs.get("founder_company"), call_mode,
             "running", None, None, ts, ts),
        )
        for i, s in enumerate(steps):
            c.execute(
                "INSERT INTO steps (run_id, step, idx, label, status, result_json, "
                "execution_id, phase2) VALUES (?,?,?,?,?,?,?,?)",
                (run_id, s["step"], i, s["label"], "pending", None, None,
                 1 if s.get("phase2") else 0),
            )
        c.commit()


def find_run_id_by_idem(idem_key: str) -> str | None:
    with _LOCK:
        row = _conn().execute(
            "SELECT run_id FROM runs WHERE idem_key=?", (idem_key,)
        ).fetchone()
        return row["run_id"] if row else None


def latest_run_by_hash(inputs_hash: str) -> dict | None:
    """Most recent run with these exact inputs: {run_id, status, created_at, updated_at}."""
    with _LOCK:
        row = _conn().execute(
            "SELECT run_id, status, created_at, updated_at FROM runs "
            "WHERE inputs_hash=? ORDER BY created_at DESC LIMIT 1", (inputs_hash,)
        ).fetchone()
        return dict(row) if row else None


def update_run(run_id: str, *, status: str | None = None,
               result: Any = None, error: str | None = None) -> None:
    with _LOCK:
        c = _conn()
        sets, vals = [], []
        if status is not None:
            sets.append("status=?"); vals.append(status)
        if result is not None:
            sets.append("result_json=?"); vals.append(json.dumps(result))
        if error is not None:
            sets.append("error=?"); vals.append(error)
        sets.append("updated_at=?"); vals.append(now())
        vals.append(run_id)
        c.execute(f"UPDATE runs SET {', '.join(sets)} WHERE run_id=?", vals)
        c.commit()


def upsert_step(run_id: str, step: str, *, status: str | None = None,
                label: str | None = None, result: Any = None,
                execution_id: str | None = None) -> None:
    """Update a step's status/label/result/execution_id (only the fields given)."""
    with _LOCK:
        c = _conn()
        sets, vals = [], []
        if status is not None:
            sets.append("status=?"); vals.append(status)
        if label is not None:
            sets.append("label=?"); vals.append(label)
        if result is not None:
            sets.append("result_json=?"); vals.append(json.dumps(result))
        if execution_id is not None:
            sets.append("execution_id=?"); vals.append(execution_id)
        if not sets:
            return
        vals.extend([run_id, step])
        c.execute(f"UPDATE steps SET {', '.join(sets)} WHERE run_id=? AND step=?", vals)
        c.execute("UPDATE runs SET updated_at=? WHERE run_id=?", (now(), run_id))
        c.commit()


def get_run(run_id: str) -> dict | None:
    """Full serialized run: inputs + status + result + ordered steps. None if absent."""
    with _LOCK:
        c = _conn()
        r = c.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if not r:
            return None
        srows = c.execute(
            "SELECT * FROM steps WHERE run_id=? ORDER BY idx", (run_id,)
        ).fetchall()
    steps = [
        {
            "step": s["step"],
            "idx": s["idx"],
            "label": s["label"],
            "status": s["status"],
            "result": _loads(s["result_json"]),
            "execution_id": s["execution_id"],
            "phase2": bool(s["phase2"]),
        }
        for s in srows
    ]
    return {
        "run_id": r["run_id"],
        "status": r["status"],
        "call_mode": r["call_mode"],
        "inputs": {
            "vc_name": r["vc_name"],
            "vc_phone": r["vc_phone"],
            "founder_name": r["founder_name"],
            "founder_company": r["founder_company"],
        },
        "result": _loads(r["result_json"]),
        "error": r["error"],
        "created_at": r["created_at"],
        "updated_at": r["updated_at"],
    } | {"steps": steps}


def running_run_ids() -> list[str]:
    with _LOCK:
        rows = _conn().execute(
            "SELECT run_id FROM runs WHERE status=? ORDER BY created_at", ("running",)
        ).fetchall()
        return [r["run_id"] for r in rows]

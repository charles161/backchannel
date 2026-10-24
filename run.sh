#!/usr/bin/env bash
# run.sh — launch the Backchannel orchestrator (default 0.0.0.0:8900).
#
#   ./run.sh                 # mock mode (default): no real calls, free, demo-safe
#   CALL_MODE=live ./run.sh  # opt-in: three real Bolna calls (reference, VC, founder),
#                            # each to a verified, allowlisted number
#
# Sources the env files (keys stay in the environment — never printed/committed).
# Personal values (reference phone, demo founder, form defaults) live in
# ./.env.local, which is git-ignored. See README.md for the variables.
set -euo pipefail
cd "$(dirname "$0")"

# --- secrets (read-only; keys never echoed) ---
CRUSTDATA_ENV_FILE="${CRUSTDATA_ENV_FILE:-$HOME/.crustdata.env}"
BOLNA_ENV_FILE="${BOLNA_ENV_FILE:-$HOME/.bolna.env}"
RESEND_ENV_FILE="${RESEND_ENV_FILE:-$HOME/.resend.env}"   # founder summary email (optional)
for f in "$CRUSTDATA_ENV_FILE" "$BOLNA_ENV_FILE" "$RESEND_ENV_FILE" ./.env.local; do
  if [[ -f "$f" ]]; then set -a; . "$f"; set +a; else echo "WARN: missing env file $f" >&2; fi
done

# --- config (override via environment / .env.local) ---
export CALL_MODE="${CALL_MODE:-mock}"                          # mock | live
export CRUSTDATA_LIVE="${CRUSTDATA_LIVE:-1}"                   # 1 = live lookups (cached) | 0 = cache-only
export BACKCHANNEL_REF_PHONE="${BACKCHANNEL_REF_PHONE:-}"      # number the live reference call dials
export BACKCHANNEL_FOUNDER_PHONE="${BACKCHANNEL_FOUNDER_PHONE:-}"  # number the live founder call dials
export BACKCHANNEL_ALLOWED_PHONES="${BACKCHANNEL_ALLOWED_PHONES:-}"  # comma-separated allowlist
export BACKCHANNEL_DEMO_FOUNDER="${BACKCHANNEL_DEMO_FOUNDER:-}"  # the pre-cached founder

HOST="${HOST:-0.0.0.0}"; PORT="${PORT:-8900}"

# Live mode places real phone calls: never expose it unauthenticated.
if [[ "$CALL_MODE" == "live" ]]; then
  if [[ -z "$BACKCHANNEL_REF_PHONE" ]]; then
    echo "ERROR: CALL_MODE=live needs BACKCHANNEL_REF_PHONE (set it in .env.local)." >&2; exit 1
  fi
  if [[ -z "$BACKCHANNEL_FOUNDER_PHONE" ]]; then
    echo "ERROR: CALL_MODE=live needs BACKCHANNEL_FOUNDER_PHONE (set it in .env.local)." >&2; exit 1
  fi
  if [[ -z "${BACKCHANNEL_VC_AGENT_ID:-}" || -z "${BACKCHANNEL_FOUNDER_AGENT_ID:-}" ]]; then
    echo "ERROR: CALL_MODE=live needs BACKCHANNEL_VC_AGENT_ID and BACKCHANNEL_FOUNDER_AGENT_ID" >&2
    echo "       (run: python3 setup_agents.py, then add its two lines to .env.local)." >&2
    exit 1
  fi
  if [[ "$HOST" != "127.0.0.1" && "$HOST" != "localhost" && -z "${BACKCHANNEL_DEMO_TOKEN:-}" ]]; then
    echo "ERROR: CALL_MODE=live on $HOST needs BACKCHANNEL_DEMO_TOKEN (open the UI as /?token=...)," >&2
    echo "       or bind locally behind a tunnel: HOST=127.0.0.1 CALL_MODE=live ./run.sh" >&2
    exit 1
  fi
fi

echo "Backchannel  ·  CALL_MODE=$CALL_MODE  ·  CRUSTDATA_LIVE=$CRUSTDATA_LIVE  ·  http://$HOST:$PORT"
exec python3 -m uvicorn app:app --host "$HOST" --port "$PORT" "$@"

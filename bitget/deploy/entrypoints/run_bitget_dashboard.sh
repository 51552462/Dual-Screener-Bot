#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
cd "$ROOT"
export TZ="${TZ:-Asia/Seoul}"
export PYTHONUNBUFFERED=1
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
if [[ -f "${ROOT}/.env" ]]; then set -a; source "${ROOT}/.env"; set +a; fi
if [[ -f "${BITGET_ROOT}/.env" ]]; then set -a; source "${BITGET_ROOT}/.env"; set +a; fi
if [[ -f "${ROOT}/venv/bin/activate" ]]; then source "${ROOT}/venv/bin/activate"; fi
PORT="${BITGET_DASHBOARD_PORT:-8511}"
STREAMLIT="${ROOT}/venv/bin/streamlit"
if [[ ! -x "$STREAMLIT" ]]; then STREAMLIT="streamlit"; fi
exec "$STREAMLIT" run "${BITGET_ROOT}/dashboard.py" \
  --server.address=0.0.0.0 \
  --server.port="${PORT}" \
  --server.headless=true

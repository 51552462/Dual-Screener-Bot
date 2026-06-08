#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
if [[ -f "${ROOT}/.env" ]]; then set -a; source "${ROOT}/.env"; set +a; fi
if [[ -f "${BITGET_ROOT}/.env" ]]; then set -a; source "${BITGET_ROOT}/.env"; set +a; fi
if [[ -f "${ROOT}/venv/bin/activate" ]]; then source "${ROOT}/venv/bin/activate"; fi
exec python -m bitget.async_telegram_daemon

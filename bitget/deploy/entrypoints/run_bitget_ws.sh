#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
cd "$ROOT"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
if [[ -f "${ROOT}/.env" ]]; then set -a; source "${ROOT}/.env"; set +a; fi
if [[ -f "${BITGET_ROOT}/.env" ]]; then set -a; source "${BITGET_ROOT}/.env"; set +a; fi
if [[ -f "${ROOT}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/venv/bin/activate"
elif [[ -f "${ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.venv/bin/activate"
fi
WS_EXTRA=()
# 가상매매(기본): private WS 불필요. 실거래 켤 때만 BITGET_WS_PRIVATE=1 + 유효 API 키.
_ws_priv="$(echo "${BITGET_WS_PRIVATE:-0}" | tr '[:upper:]' '[:lower:]')"
_real="$(echo "${ENABLE_REAL_EXECUTION:-false}" | tr '[:upper:]' '[:lower:]')"
if [[ "$_ws_priv" != "1" && "$_ws_priv" != "true" && "$_ws_priv" != "yes" ]]; then
  WS_EXTRA+=(--public-only)
elif [[ "$_real" != "true" && "$_real" != "1" && "$_real" != "yes" ]]; then
  WS_EXTRA+=(--public-only)
fi
exec python -m bitget.data.ws_supervisor "${WS_EXTRA[@]}" "$@"

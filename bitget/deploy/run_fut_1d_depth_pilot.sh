#!/usr/bin/env bash
# FUT 1D history depth pilot (BTC/ETH/SOL only). Default mtf data-refresh unchanged.
#
# WRITE TARGET (Claude 조건1):
#   default  → BITGET_DB_STORAGE_PATH 프로덕션 OHLCV에 직접 merge-write (atomic table replace)
#   staging  → BITGET_FUT_DEPTH_DB=/path/staging.sqlite  (권장: COUNT 확인용)
#
# Usage (coin VPS) — COUNT용 staging 권장:
#   cd ~/dante_bots/Dual-Screener-Bot && git pull
#   export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
#   export BITGET_FUT_DEPTH_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
#   bash bitget/deploy/run_fut_1d_depth_pilot.sh
#
# FULL-BT=1 은 디렉터·Claude 조건1 확인 후만 (프로덕션 write 시 mtf refresh 일시 정지).
#   BITGET_FUT_DEPTH_RUN_FULL_BT=1 BITGET_FULL_BT_MAX_SYMBOLS=3 bash ...
set -euo pipefail

ROOT="${BITGET_INSTALL_ROOT:-${INSTALL_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}}"
cd "$ROOT"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:$PYTHONPATH}"

PY="${ROOT}/venv/bin/python"
[[ -x "$PY" ]] || PY="${ROOT}/.venv/bin/python"
[[ -x "$PY" ]] || PY="$(command -v python3 || true)"
if [[ -z "${PY}" || ! -x "${PY}" ]]; then
  echo "ERROR: venv/python not found" >&2
  exit 1
fi

if [[ -z "${BITGET_DB_STORAGE_PATH:-}" ]]; then
  if [[ -d /var/lib/quant-bitget/data ]]; then
    export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
  fi
fi

export BITGET_FUT_DEPTH_TARGET_BARS="${BITGET_FUT_DEPTH_TARGET_BARS:-300}"
export BITGET_FUT_DEPTH_SINCE="${BITGET_FUT_DEPTH_SINCE:-2024-01-01}"

echo "[fut-depth] ROOT=$ROOT"
echo "[fut-depth] BITGET_DB_STORAGE_PATH=${BITGET_DB_STORAGE_PATH:-unset}"
echo "[fut-depth] BITGET_FUT_DEPTH_DB=${BITGET_FUT_DEPTH_DB:-<production default>}"
echo "[fut-depth] target=${BITGET_FUT_DEPTH_TARGET_BARS} since=${BITGET_FUT_DEPTH_SINCE}"

if [[ -z "${BITGET_FUT_DEPTH_DB:-}" ]]; then
  echo "[fut-depth] WARNING: no staging DB — will write PRODUCTION OHLCV tables" >&2
fi

"$PY" - <<'PY'
from __future__ import annotations
import json
import os
import sys

ROOT = os.environ.get("BITGET_INSTALL_ROOT") or os.getcwd()
sys.path.insert(0, ROOT)

from bitget.data.ohlcv_history_backfill import run_fut_1d_depth_pilot

out = run_fut_1d_depth_pilot()
print(json.dumps(out, ensure_ascii=False, indent=2))
if not out.get("all_warmup_ok"):
    print("WARN: not all symbols reached warmup>=240", file=sys.stderr)
    sys.exit(2)
print("OK: all_warmup_ok write_mode=", out.get("write_mode"))
PY

if [[ "${BITGET_FUT_DEPTH_RUN_FULL_BT:-0}" == "1" ]]; then
  if [[ -n "${BITGET_FUT_DEPTH_DB:-}" ]]; then
    echo "[fut-depth] FULL-BT against staging: export BITGET_DB_STORAGE_PATH to staging or pass market_db" >&2
    export BITGET_DB_STORAGE_PATH="$(dirname "${BITGET_FUT_DEPTH_DB}")"
    # Prefer pointing market read at staging file via BITGET_FULL_BT market — pilot uses storage path
    echo "[fut-depth] NOTE: for staging FULL-BT, set BITGET_DB_STORAGE_PATH to staging dir carefully" >&2
  fi
  export BITGET_FULL_BT_MAX_SYMBOLS="${BITGET_FULL_BT_MAX_SYMBOLS:-3}"
  echo "[fut-depth] running FULL-BT pilot MAX_SYMBOLS=${BITGET_FULL_BT_MAX_SYMBOLS}"
  bash "${ROOT}/bitget/deploy/run_full_bt_hist_pilot.sh"
fi

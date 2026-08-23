#!/usr/bin/env bash
# UNIVERSE-BT L0 — real historical run on coin VPS (paper DB untouched).
# Usage (on Bot-2 / coin server):
#   cd ~/dante_bots/Dual-Screener-Bot   # or INSTALL_ROOT
#   export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
#   bash bitget/deploy/run_universe_bt_l0.sh
# Optional:
#   BITGET_UNIVERSE_BT_MAX_SYMBOLS=20  # default 10; omit or 0 = all OHLCV∩universe
#   BITGET_UNIVERSE_BT_OHLCV_ONLY=1    # skip live exchange filter (use OHLCV tables only)
set -euo pipefail

ROOT="${BITGET_INSTALL_ROOT:-${INSTALL_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}}"
cd "$ROOT"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:$PYTHONPATH}"
export BITGET_UNIVERSE_BT_OHLCV_ONLY="${BITGET_UNIVERSE_BT_OHLCV_ONLY:-1}"

if [[ -z "${BITGET_DB_STORAGE_PATH:-}" ]]; then
  if [[ -d /var/lib/quant-bitget/data ]]; then
    export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
  fi
fi

echo "[universe-bt] ROOT=$ROOT"
echo "[universe-bt] BITGET_DB_STORAGE_PATH=${BITGET_DB_STORAGE_PATH:-unset}"
echo "[universe-bt] MAX_SYMBOLS=${BITGET_UNIVERSE_BT_MAX_SYMBOLS:-10}"

python3 - <<'PY'
import os, sys
os.environ.setdefault("BITGET_UNIVERSE_BT_OHLCV_ONLY", "1")
# default cap for first VPS pass; 0 or empty = no cap
if "BITGET_UNIVERSE_BT_MAX_SYMBOLS" not in os.environ:
    os.environ["BITGET_UNIVERSE_BT_MAX_SYMBOLS"] = "10"
sys.path.insert(0, os.environ.get("BITGET_INSTALL_ROOT") or os.getcwd())
from bitget.analysis.universe_bt.run_live_u2_u3 import main
raise SystemExit(main())
PY

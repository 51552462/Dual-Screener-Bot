#!/bin/bash
# CAT-L-CUTOVER-01 Phase 0 — read-only
set -u
export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT" || exit 1
OUT=/tmp/cutover01_p0.out
exec > >(tee "$OUT") 2>&1
echo "=== TS $(date -u +%Y-%m-%dT%H:%M:%SZ) HEAD=$(git rev-parse --short HEAD) ==="
echo "=== Step 1 bitget.sh --cutover-check ==="
set +e
timeout 120 ./bitget/deploy/bitget.sh --cutover-check
echo "CUTOVER_CHECK_RC=$?"
set -e
echo "=== Step 1 JSON dump ==="
set +u
set +a
# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a
[ -f bitget/.env ] && set -a && . ./bitget/.env && set +a
set -u
export PYTHONPATH="${INSTALL_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
PY="${INSTALL_ROOT}/venv/bin/python"
[ -x "$PY" ] || PY=python3
"$PY" -c "import json; from bitget.validation.cutover import check_cutover_readiness; print(json.dumps(check_cutover_readiness(), ensure_ascii=False, indent=2, default=str))"
echo "JSON_DUMP_RC=$?"
echo "=== Step 2 env keys ==="
grep -E '^BITGET_PIPELINE_SSOT=|^BITGET_ASYNC_TELEGRAM=|^BITGET_WATCHDOG_HEARTBEAT_COMPONENT=' .env 2>/dev/null || echo 'ROOT_ENV: NOT_SET (해당 줄 없음)'
grep -E '^BITGET_PIPELINE_SSOT=|^BITGET_ASYNC_TELEGRAM=|^BITGET_WATCHDOG_HEARTBEAT_COMPONENT=' bitget/.env 2>/dev/null || echo 'BITGET_ENV: NOT_SET (해당 줄 없음)'
echo "=== Step 3 processes ==="
pgrep -af 'bitget.main' || echo 'pgrep_bitget.main=none'
pgrep -af 'factory_launcher' || echo 'pgrep_factory_launcher=none'
systemctl list-units --type=service --no-legend --all | grep -i bitget || echo 'NO_BITGET_UNITS'
echo "=== DONE (no start-parallel, no env write) ==="
echo "WROTE $OUT"

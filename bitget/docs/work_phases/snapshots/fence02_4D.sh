#!/bin/bash
# CAT-L-FENCE-02 Step 4D — pull fence commit + fence-check. NO installer / update_bitget.sh
set -u
cd /home/ubuntu/dante_bots/Dual-Screener-Bot || exit 1
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
LIVE=/etc/cron.d/dual-screener-bitget
PY="$INSTALL_ROOT/venv/bin/python"
[ -x "$PY" ] || PY=python3
export PYTHONPATH="$INSTALL_ROOT${PYTHONPATH:+:$PYTHONPATH}"
echo "=== 4D BEFORE ==="
git log -1 --format='%h %ad %s' --date=iso
echo "=== 4D git pull --ff-only ==="
git pull --ff-only
echo "PULL_EXIT=$?"
echo "=== 4D AFTER ==="
git log -1 --format='%h %ad %s' --date=iso
git rev-parse --short HEAD
echo "=== 4D fence-check (expect DRIFTED, WRAPPED_COUNT=28, LIVE_WRAPPED_COUNT=0) ==="
"$PY" "$INSTALL_ROOT/bitget/deploy/generate_bitget_crontab.py" --install-root "$INSTALL_ROOT" --fence-check "$LIVE"
echo "=== live systemd-run count (pre-install, expect 0) ==="
grep -c systemd-run "$LIVE" || true
echo "=== installer NOT run ==="

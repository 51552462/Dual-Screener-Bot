#!/bin/bash
# CAT-L-FENCE-02 Step 4E retry — installer only. Gate = python fence-check, not raw grep -c
# (grep -c counts a header comment → 29; wrapped_count ignores comments → 28)
set -u
cd /home/ubuntu/dante_bots/Dual-Screener-Bot || exit 1
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
LIVE=/etc/cron.d/dual-screener-bitget
PY="$INSTALL_ROOT/venv/bin/python"
[ -x "$PY" ] || PY=python3
export PYTHONPATH="$INSTALL_ROOT${PYTHONPATH:+:$PYTHONPATH}"
P0="$INSTALL_ROOT/bitget/docs/work_phases/snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron"
GEN="$INSTALL_ROOT/bitget/deploy/generate_bitget_crontab.py"

echo "=== 4E-RETRY PRE fence-check ==="
PRE=$("$PY" "$GEN" --install-root "$INSTALL_ROOT" --fence-check "$LIVE")
printf '%s\n' "$PRE"

echo "=== 4E-RETRY INSTALL ==="
sudo bash "$INSTALL_ROOT/bitget/deploy/install_bitget_cron.sh"
echo "INSTALL_EXIT=$?"
sudo systemctl daemon-reload

echo "=== Handoff grep -c (includes comment lines; expect 29 not 28) ==="
echo "GREP_C_SYSTEMD_RUN=$(grep -c systemd-run "$LIVE" || true)"
echo "GREP_C_JOBS=$(grep -c systemd-run "$LIVE" | cat; grep -vE '^[[:space:]]*#' "$LIVE" | grep -c systemd-run || true)"
echo "NONCOMMENT_SYSTEMD_RUN=$(grep -vE '^[[:space:]]*#' "$LIVE" | grep -c systemd-run || true)"

echo "=== 4E-RETRY POST fence-check ==="
POST=$("$PY" "$GEN" --install-root "$INSTALL_ROOT" --fence-check "$LIVE")
printf '%s\n' "$POST"

echo "=== 4E-RETRY slice ==="
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax

OK=1
echo "$POST" | grep -q 'FENCE_STATUS=FENCE_OK' || OK=0
echo "$POST" | grep -q 'LIVE_WRAPPED_COUNT=28' || OK=0
echo "$POST" | grep -q 'WRAPPED_COUNT=28' || OK=0

if [ "$OK" != "1" ]; then
  echo "=== 4E-RETRY ROLLBACK to P0 (cron only; keep slice unit if present) ==="
  sudo install -m 0644 "$P0" "$LIVE"
  echo "ROLLBACK_DONE LIVE_WRAPPED=$("$PY" "$GEN" --install-root "$INSTALL_ROOT" --fence-check "$LIVE" | grep LIVE_WRAPPED)"
  exit 1
fi
echo "=== 4E-RETRY VERIFY_OK ==="

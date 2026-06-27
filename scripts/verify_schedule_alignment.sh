#!/usr/bin/env bash
# 로그 wall_clock(KST)으로 cron 스케줄 정렬 여부 검사 (staggered scan SSOT)
set -eu -o pipefail

INSTALL_ROOT="${INSTALL_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
LOG_DIR="${FACTORY_LOG_DIR:-${INSTALL_ROOT}/logs}"
CRON_KR="${FACTORY_CRON_KR_PATH:-/etc/cron.d/dual-screener-factory-kr}"
CRON_US="${FACTORY_CRON_US_PATH:-/etc/cron.d/dual-screener-factory-us}"

echo "=== Schedule alignment check ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"

check_cron_file() {
  local f="$1" tz="$2" label="$3"
  if [[ -f "$f" ]]; then
    if grep -q $'\r' "$f" 2>/dev/null; then
      echo "✗ $label has CRLF — CRON_TZ ignored"
    elif grep -q "^CRON_TZ=${tz}" "$f"; then
      echo "✓ $label CRON_TZ=${tz} (LF)"
    else
      echo "✗ $label missing CRON_TZ=${tz}"
    fi
    if grep -q 'factory_slot_dispatcher.py --market US' "$f" 2>/dev/null; then
      echo "✓ $label uses factory_slot_dispatcher (ET slots, KST poll)"
    elif grep -q 'factory.sh --scan-us-' "$f" 2>/dev/null; then
      echo "⚠ $label legacy per-slot ET cron — needs working CRON_TZ=America/New_York"
    fi
  else
    echo "✗ $label missing: $f"
  fi
}

check_cron_file "$CRON_KR" "Asia/Seoul" "KR cron"
check_cron_file "$CRON_US" "Asia/Seoul" "US cron"

echo ""
echo "--- Expected staggered KR (KST wall_clock in logs) ---"
echo "  scan_kr_supernova: ~10:00 · nulrim ~10:50 · … · ema5_r2 ~17:30"
echo "  daily_kr: Mon-Fri ~18:45 · daily_us: Tue-Sat ~06:45"
echo ""
echo "--- Expected staggered US (KST wall_clock ≈ ET session night) ---"
echo "  factory_scan_us_* wall_clock KST ~23:00–06:00 (EDT) during US Mon–Fri ET slots"
echo ""

_misaligned=0

# factory.sh logs KST wall_clock on every run — authoritative for trigger time
_log_kst_hour() {
  local f="$1"
  grep -m1 '^\[factory\.sh\] wall_clock=' "$f" 2>/dev/null \
    | sed -n 's/.*wall_clock=[0-9-]* \([0-9][0-9]\):.*/\1/p'
}

_check_us_logs() {
  local pattern="$1"
  local desc="$2"
  local f hh
  for f in $(ls -t "${LOG_DIR}/${pattern}" 2>/dev/null | head -20); do
    hh="$(_log_kst_hour "$f")"
    [[ -z "$hh" ]] && continue
    if [[ "$hh" =~ ^(0[89]|1[0-9]|2[0-2])$ ]]; then
      echo "  ✗ MISALIGNED $desc: $(basename "$f") (KST wall_clock hour=${hh})"
      _misaligned=1
    fi
  done
}

_check_us_logs 'factory_scan_us_supernova_*' 'us_supernova'
_check_us_logs 'factory_scan_us_bowl_*' 'us_bowl'
_check_us_logs 'factory_scan_us_*_r2_*' 'us_r2'

if [[ "$_misaligned" -eq 0 ]]; then
  echo "✓ No US scan logs with KST daytime wall_clock (08–22h) in recent history"
else
  echo ""
  echo "US scans at KST 08–22h → SKIPPED_SESSION + no data. Fix:"
  echo "  sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
  echo "  bash scripts/diag_cron_tz_effective.sh"
fi

echo ""
echo "=== Recent logs (newest 5 each) ==="
for p in factory_scan_kr_supernova factory_scan_kr_nulrim_r2 factory_scan_us_supernova factory_scan_us_ema5_r2 factory_daily_audit_kr factory_daily_audit_us; do
  echo "[$p]"
  ls -lt "${LOG_DIR}/${p}_"* 2>/dev/null | head -5 | awk '{print "  "$NF}' || echo "  (none)"
done

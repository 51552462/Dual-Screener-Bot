#!/usr/bin/env bash
# 로그 파일명 시각(KST)으로 cron 스케줄 정렬 여부 검사 (staggered scan SSOT)
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
  else
    echo "✗ $label missing: $f"
  fi
}

check_cron_file "$CRON_KR" "Asia/Seoul" "KR cron"
check_cron_file "$CRON_US" "America/New_York" "US cron"

echo ""
echo "--- Expected staggered KR (KST log hour) ---"
echo "  scan_kr_supernova: ~10:00 · nulrim ~10:30 · … · ema5_r2 ~14:30"
echo "  daily_kr: Mon-Fri ~16:35 · daily_us: Tue-Sat ~06:45"
echo ""
echo "--- Expected staggered US (log filename uses server TZ; US cron = ET) ---"
echo "  factory_scan_us_supernova_* during US regular session (ET 10:00–14:30)"
echo ""

_misaligned=0

_check_logs() {
  local pattern="$1"
  local desc="$2"
  local bad_hours_re="$3"
  local f hh
  for f in $(ls -t "${LOG_DIR}/${pattern}" 2>/dev/null | head -20); do
    hh="$(basename "$f" | sed -n 's/.*_\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)\.log/\2/p')"
    [[ -z "$hh" ]] && continue
    if [[ "$hh" =~ $bad_hours_re ]]; then
      echo "  ✗ MISALIGNED $desc: $(basename "$f") (hour=${hh} in filename)"
      _misaligned=1
    fi
  done
}

# KR staggered: should not run overnight or post-16 except daily
_check_logs 'factory_scan_kr_supernova_*' 'kr_supernova' '^(0[0-9]|1[67]|1[89]|2[0-3])$'
_check_logs 'factory_scan_kr_ema5_r2_*' 'kr_ema5_r2' '^(0[0-9]|1[56789]|2[0-3])$'

if [[ "$_misaligned" -eq 0 ]]; then
  echo "✓ No obvious misaligned pattern in recent staggered KR log filenames"
else
  echo ""
  echo "Fix: sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
fi

echo ""
echo "=== Recent logs (newest 5 each) ==="
for p in factory_scan_kr_supernova factory_scan_kr_ema5_r2 factory_scan_us_supernova factory_daily_audit_kr factory_daily_audit_us; do
  echo "[$p]"
  ls -lt "${LOG_DIR}/${p}_"* 2>/dev/null | head -5 | awk '{print "  "$NF}' || echo "  (none)"
done

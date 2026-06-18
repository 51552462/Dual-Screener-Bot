#!/usr/bin/env bash
# 로그 파일명 시각(KST)으로 cron UTC 오설정( KR/US 스케줄 뒤바뀜) 여부 검사
set -eu -o pipefail

INSTALL_ROOT="${INSTALL_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
LOG_DIR="${FACTORY_LOG_DIR:-${INSTALL_ROOT}/logs}"
CRON_PATH="${FACTORY_CRON_PATH:-/etc/cron.d/dual-screener-factory}"

echo "=== Schedule alignment check (KST) ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"

if [[ -f "$CRON_PATH" ]]; then
  if grep -q $'\r' "$CRON_PATH" 2>/dev/null; then
    echo "✗ $CRON_PATH has CRLF — CRON_TZ ignored → KR/US schedules SWAPPED"
  elif grep -q '^CRON_TZ=Asia/Seoul' "$CRON_PATH"; then
    echo "✓ $CRON_PATH CRON_TZ=Asia/Seoul (LF)"
  else
    echo "✗ $CRON_PATH missing CRON_TZ=Asia/Seoul"
  fi
else
  echo "✗ cron file missing: $CRON_PATH"
fi

echo ""
echo "--- Expected (correct) ---"
echo "  scan_kr logs:  Mon-Fri hour 09-15 in filename (factory_scan_kr_YYYYMMDD_HHMMSS)"
echo "  scan_us logs:  Tue-Sat hour 22-23 or 00-06 in filename"
echo "  daily_kr:      Mon-Fri ~16:35"
echo "  daily_us:      Tue-Sat ~06:45"
echo ""
echo "--- Misaligned pattern (UTC cron bug) ---"
echo "  scan_kr at hour 00-08 or 18-23  → KR ran when US should (or off-hours)"
echo "  scan_us at hour 09-15            → US ran during Korean daytime (wrong)"
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
      echo "  ✗ MISALIGNED $desc: $(basename "$f") (hour=${hh} KST in filename)"
      _misaligned=1
    fi
  done
}

_check_logs 'factory_scan_kr_*' 'scan_kr' '^(0[0-8]|1[89]|2[0-3])$'
_check_logs 'factory_scan_us_*' 'scan_us' '^(09|10|11|12|13|14|15)$'

if [[ "$_misaligned" -eq 0 ]]; then
  echo "✓ No obvious swapped-schedule pattern in recent log filenames"
else
  echo ""
  echo "Fix: sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
  echo "     bash deploy/audit_factory_stack.sh"
fi

echo ""
echo "=== Recent logs (newest 5 each) ==="
for p in factory_scan_kr factory_scan_us factory_daily_audit_kr factory_daily_audit_us; do
  echo "[$p]"
  ls -lt "${LOG_DIR}/${p}_"* 2>/dev/null | head -5 | awk '{print "  "$NF}' || echo "  (none)"
done

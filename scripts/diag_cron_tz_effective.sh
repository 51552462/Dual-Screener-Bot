#!/usr/bin/env bash
# =============================================================================
# CRON_TZ 실효 여부 진단 — US 스캔이 KST 주간에 깨지는지 로그·시계·cron 패키지로 판별
#   cd INSTALL_ROOT && bash scripts/diag_cron_tz_effective.sh
#   sudo bash scripts/diag_cron_tz_effective.sh   # cron.d 직접 읽기
# =============================================================================
set -eu -o pipefail

INSTALL_ROOT="${INSTALL_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
LOG_DIR="${FACTORY_LOG_DIR:-${INSTALL_ROOT}/logs}"
CRON_KR="${FACTORY_CRON_KR_PATH:-/etc/cron.d/dual-screener-factory-kr}"
CRON_US="${FACTORY_CRON_US_PATH:-/etc/cron.d/dual-screener-factory-us}"
PY="${INSTALL_ROOT}/venv/bin/python"
[[ -x "$PY" ]] || PY="${INSTALL_ROOT}/.venv/bin/python"
[[ -x "$PY" ]] || PY="$(command -v python3 2>/dev/null || true)"

pass() { echo "  ✓ $*"; }
warn() { echo "  ⚠ $*"; }
fail() { echo "  ✗ $*"; }

_misaligned_us=0
_cron_tz_ignored=0

echo "=== CRON_TZ effectiveness diagnosis ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"
echo "time: $(date) ($(timedatectl show -p Timezone --value 2>/dev/null || echo "${TZ:-?}"))"
echo ""

# --- 1. Cron package ---
echo "[1] Cron daemon"
if command -v cron >/dev/null 2>&1; then
  cron -V 2>&1 | sed 's/^/  /' || true
elif dpkg -l cron 2>/dev/null | grep -q ^ii; then
  dpkg -l cron 2>/dev/null | awk '/^ii/{print "  "$0}'
else
  warn "cron binary/version unknown"
fi
if systemctl is-active cron >/dev/null 2>&1 || systemctl is-active crond >/dev/null 2>&1; then
  pass "cron service active"
else
  warn "cron service not active (or non-systemd host)"
fi
echo ""

# --- 2. Installed cron files ---
echo "[2] /etc/cron.d factory files"
for f in "$CRON_KR" "$CRON_US"; do
  if [[ ! -f "$f" ]]; then
    fail "missing $f"
    continue
  fi
  if grep -q $'\r' "$f" 2>/dev/null; then
    fail "$f has CRLF — CRON_TZ is ignored by cron"
    _cron_tz_ignored=1
  else
    pass "$f LF ok"
  fi
  grep -E '^(CRON_TZ|TZ|SHELL)=' "$f" 2>/dev/null | sed 's/^/    /' || true
  if grep -q 'factory_slot_dispatcher' "$f" 2>/dev/null; then
    pass "$f uses factory_slot_dispatcher (ET-aware, CRON_TZ-independent)"
  elif grep -q 'factory.sh --scan-us-' "$f" 2>/dev/null; then
    if grep -q '^CRON_TZ=America/New_York' "$f"; then
      warn "$f uses per-slot ET cron — requires working CRON_TZ=America/New_York"
    else
      fail "$f has US scan lines but no CRON_TZ=America/New_York"
      _cron_tz_ignored=1
    fi
  fi
done
if [[ -f /etc/cron.d/dual-screener-factory ]]; then
  fail "legacy /etc/cron.d/dual-screener-factory still present — duplicate schedules"
fi
if crontab -l -u ubuntu 2>/dev/null | grep -qE 'factory\.sh|factory_slot_dispatcher'; then
  fail "ubuntu user crontab also schedules factory — remove duplicates"
fi
echo ""

# --- 3. Live ET/KST session ---
echo "[3] Market clocks (runtime gate uses ET for US)"
if [[ -n "$PY" && -f "${INSTALL_ROOT}/market_session_gate.py" ]]; then
  "$PY" -c "
from datetime import datetime
import pytz
from market_session_gate import is_market_open
kst = pytz.timezone('Asia/Seoul')
et = pytz.timezone('America/New_York')
now_kst = datetime.now(kst)
now_et = datetime.now(et)
print(f'  KST: {now_kst.strftime(\"%Y-%m-%d %H:%M %a\")}')
print(f'  ET : {now_et.strftime(\"%Y-%m-%d %H:%M %a\")}')
for m in ('KR', 'US'):
    ok, detail = is_market_open(m)
    print(f'  {m} open={ok}')
" 2>/dev/null || warn "market_session_gate check failed"
else
  warn "python/venv not found — skip session check"
fi
echo ""

# --- 4. US log wall_clock analysis (SSOT — factory.sh prints KST wall_clock) ---
echo "[4] Recent US scan logs — KST wall_clock vs ET filename stamp"
if [[ ! -d "$LOG_DIR" ]]; then
  warn "log dir missing: $LOG_DIR"
else
  _analyze_us_log() {
    local f="$1"
    local base wc hh_stamp et_hh kst_hh
    base="$(basename "$f")"
    wc="$(grep -m1 '^\[factory\.sh\] wall_clock=' "$f" 2>/dev/null \
      | sed -n 's/.*wall_clock=\([0-9-]*\) \([0-9][0-9]\):\([0-9][0-9]\):.*/\2/p')"
    hh_stamp="$(echo "$base" | sed -n 's/.*_\([0-9]\{8\}\)_\([0-9]\{6\}\)\.log$/\2/p' | cut -c1-2)"
    kst_hh="${wc:-??}"
    et_hh="${hh_stamp:-??}"
    if [[ "$kst_hh" =~ ^(0[89]|1[0-9]|2[0-2])$ ]]; then
      fail "KST daytime trigger: $base (wall_clock KST hour=${kst_hh}, log ET stamp hour=${et_hh})"
      _misaligned_us=1
      _cron_tz_ignored=1
    elif [[ "$kst_hh" != "??" ]]; then
      pass "KST off-hours trigger: $base (wall_clock hour=${kst_hh}, ET stamp=${et_hh})"
    else
      warn "no wall_clock line: $base (ET stamp hour=${et_hh})"
    fi
    local st
    st="$(grep -E 'finished status=' "$f" 2>/dev/null | tail -1 || true)"
    [[ -n "$st" ]] && echo "      $st"
  }
  shopt -s nullglob
  files=("${LOG_DIR}"/factory_scan_us_*.log)
  if [[ ${#files[@]} -eq 0 ]]; then
    warn "no factory_scan_us_* logs yet"
  else
    ls -t "${files[@]}" 2>/dev/null | head -12 | while read -r f; do
      _analyze_us_log "$f"
    done
  fi
fi
echo ""

# --- 5. Expected slot map (KST trigger if CRON_TZ ignored) ---
echo "[5] If CRON_TZ is IGNORED, US jobs fire at these KST times (matches SKIPPED_SESSION alerts)"
if [[ -x "$PY" && -f "${INSTALL_ROOT}/factory_scan_schedule.py" ]]; then
  "$PY" -c "
from factory_scan_schedule import US_SCAN_SLOTS
for s in US_SCAN_SLOTS:
    print(f'  KST {s.hour:02d}:{s.minute:02d} (wrong) → intended ET {s.hour:02d}:{s.minute:02d} → {s.mode}')
" 2>/dev/null || true
fi
echo "  Intended (CRON_TZ=America/New_York or slot dispatcher): ET 10:00–16:40 Mon–Fri"
echo "  Intended KST wall_clock (EDT): ~23:00–05:40 spanning Mon night → Sat morning"
echo ""

# --- 6. Verdict ---
echo "[6] Verdict"
if [[ "$_misaligned_us" -eq 1 ]]; then
  fail "US scans are triggering during KST 08:00–22:59 — CRON_TZ=America/New_York is NOT effective"
  echo ""
  echo "  Root cause (typical): cron runs /etc/cron.d lines in system TZ (Asia/Seoul), not CRON_TZ."
  echo "  Impact: every US scan → SKIPPED_SESSION → no funnel/data → staleness alerts."
  echo ""
  echo "  Fix (recommended):"
  echo "    sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
  echo "    (installs factory_slot_dispatcher — ET slots without CRON_TZ)"
  echo "    bash scripts/verify_schedule_alignment.sh"
  echo "    bash scripts/reset_factory_pipeline.sh   # if stale lock"
elif grep -q 'factory_slot_dispatcher' "$CRON_US" 2>/dev/null; then
  pass "US cron uses slot dispatcher — CRON_TZ not required"
else
  warn "no KST-daytime US misalignment in recent logs — CRON_TZ may be working (or no US runs yet)"
  echo "  Re-run after tonight's US session (KST ~23:00+) to confirm."
fi
echo ""
echo "=== diagnosis done ==="

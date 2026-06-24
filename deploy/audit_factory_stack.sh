#!/usr/bin/env bash
# =============================================================================
# Factory stack 전체 점검 — cron · lock · systemd · 텔레그램 · 최근 로그
#   cd INSTALL_ROOT && bash deploy/audit_factory_stack.sh
#   sudo bash deploy/audit_factory_stack.sh   (cron.d 읽기 등)
# =============================================================================
set -eu -o pipefail

INSTALL_ROOT="${INSTALL_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$INSTALL_ROOT"

CRON_KR="${FACTORY_CRON_KR_PATH:-/etc/cron.d/dual-screener-factory-kr}"
CRON_US="${FACTORY_CRON_US_PATH:-/etc/cron.d/dual-screener-factory-us}"
LEGACY_CRON="${FACTORY_CRON_PATH:-/etc/cron.d/dual-screener-factory}"
LOCK="${INSTALL_ROOT}/.factory_runtime.lock"
LOG_DIR="${FACTORY_LOG_DIR:-${INSTALL_ROOT}/logs}"
PY="${INSTALL_ROOT}/venv/bin/python"
[[ -x "$PY" ]] || PY="${INSTALL_ROOT}/.venv/bin/python"

pass() { echo "  ✓ $*"; }
warn() { echo "  ⚠ $*"; }
fail() { echo "  ✗ $*"; }

echo "=== Factory stack audit ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"
echo "time: $(date) ($(timedatectl show -p Timezone --value 2>/dev/null || echo '?'))"
echo ""

# --- 1. Cron SSOT ---
echo "[1] Cron (KR=${CRON_KR}, US=${CRON_US})"
_cron_ok=0
if [[ ! -f "$CRON_KR" ]]; then
  fail "KR cron missing — run: sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
else
  _cron_ok=1
  if grep -q $'\r' "$CRON_KR" 2>/dev/null; then
    fail "CRLF in KR cron — CRON_TZ ignored"
  else
    pass "KR cron LF ok"
  fi
  if grep -q '^CRON_TZ=Asia/Seoul' "$CRON_KR"; then
    pass "CRON_TZ=Asia/Seoul (KR file)"
  else
    fail "CRON_TZ=Asia/Seoul missing in KR cron"
  fi
  if grep -q 'factory.sh --scan-kr-supernova' "$CRON_KR"; then
    pass "staggered scan-kr-supernova scheduled"
  else
    fail "no staggered --scan-kr-supernova (legacy monolithic scan-kr?)"
  fi
  if grep -q 'factory.sh --daily-kr' "$CRON_KR"; then
    pass "daily-kr scheduled"
  else
    warn "no daily-kr line in KR cron"
  fi
  if grep -q "${INSTALL_ROOT}" "$CRON_KR"; then
    pass "KR cron paths match INSTALL_ROOT"
  else
    warn "KR cron paths may not match INSTALL_ROOT"
  fi
fi

if [[ ! -f "$CRON_US" ]]; then
  fail "US cron missing — run: sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
else
  _cron_ok=1
  if grep -q '^CRON_TZ=America/New_York' "$CRON_US"; then
    pass "CRON_TZ=America/New_York (US file)"
  elif grep -q '^CRON_TZ=Asia/Seoul' "$CRON_US" \
    && grep -q 'factory_slot_dispatcher.py --market US' "$CRON_US"; then
    pass "US slot dispatcher + CRON_TZ=Asia/Seoul"
  else
    fail "US cron missing dispatcher or CRON_TZ"
  fi
  if grep -q 'factory_slot_dispatcher.py --market US' "$CRON_US"; then
    pass "US factory_slot_dispatcher scheduled"
  elif grep -q 'factory.sh --scan-us-supernova' "$CRON_US"; then
    warn "legacy per-slot US cron — CRON_TZ=America/New_York must work on this host"
  else
    fail "no US scan schedule"
  fi
fi

if [[ -f "$LEGACY_CRON" ]]; then
  fail "legacy ${LEGACY_CRON} still present — duplicate schedules; reinstall cron"
fi

if crontab -l -u ubuntu 2>/dev/null | grep -q 'factory\.sh'; then
  fail "ubuntu user crontab also runs factory.sh — duplicate schedules likely"
else
  pass "no factory.sh in ubuntu user crontab"
fi
echo ""

# --- 2. factory.sh ---
echo "[2] factory.sh"
if [[ -x "${INSTALL_ROOT}/factory.sh" ]]; then
  pass "factory.sh executable"
else
  warn "factory.sh not executable — use: bash factory.sh or chmod +x"
fi
if [[ -f "${INSTALL_ROOT}/factory.sh" ]] && grep -q $'\r' "${INSTALL_ROOT}/factory.sh" 2>/dev/null; then
  warn "factory.sh has CRLF — run: bash deploy/fix_shell_lf.sh"
fi
echo ""

# --- 3. Lock ---
echo "[3] Global lock ($LOCK)"
if [[ -f "$LOCK" ]]; then
  age=$(( $(date +%s) - $(stat -c %Y "$LOCK" 2>/dev/null || stat -f %m "$LOCK") ))
  echo "  lock contents:"
  sed 's/^/    /' "$LOCK" || true
  holder_pid="$(sed -n '3p' "$LOCK" 2>/dev/null || true)"
  if [[ -n "${holder_pid:-}" ]] && kill -0 "$holder_pid" 2>/dev/null; then
    warn "lock held by live pid=$holder_pid age=${age}s — concurrent cron may SKIPPED_LOCK"
  else
    warn "stale lock file age=${age}s — run: bash scripts/reset_factory_pipeline.sh"
  fi
else
  pass "no lock file"
fi
echo ""

# --- 4. systemd ---
echo "[4] systemd"
for u in dante-factory dante-async dante-dashboard; do
  st="$(systemctl is-active "$u" 2>/dev/null || echo inactive)"
  if [[ "$st" == "active" ]]; then
    pass "$u active"
  else
    warn "$u is $st"
  fi
done
if systemctl is-active dante-main 2>/dev/null | grep -q active; then
  fail "dante-main still active — duplicate daemon with dante-factory"
fi
echo ""

# --- 5. Market session (now) ---
echo "[5] Market session (runtime gate)"
if [[ -x "$PY" ]]; then
  "$PY" -c "
from datetime import datetime
import pytz
from market_session_gate import is_market_open
kst = datetime.now(pytz.timezone('Asia/Seoul'))
et = datetime.now(pytz.timezone('America/New_York'))
print(f'  KST now: {kst.strftime(\"%Y-%m-%d %H:%M %a\")}')
print(f'  ET  now: {et.strftime(\"%Y-%m-%d %H:%M %a\")}')
for m in ('KR', 'US'):
    ok, detail = is_market_open(m)
    print(f'  {m} open={ok} — {detail}')
" 2>/dev/null || warn "market_session_gate check failed (venv?)"
else
  warn "venv python not found"
fi
echo ""

# --- 6. Telegram credentials ---
echo "[6] Telegram (.env keys — values not printed)"
if [[ -f "${INSTALL_ROOT}/.env" ]]; then
  # shellcheck disable=SC1091
  set -a; source "${INSTALL_ROOT}/.env"; set +a
fi
if [[ -x "$PY" ]]; then
  "$PY" -c "
import telegram_env as t
def chk(label, tok, chat):
    ok = bool(tok and chat)
    mark = '✓' if ok else '✗'
    print(f'  {mark} {label}: token={\"set\" if tok else \"MISSING\"} chat={\"set\" if chat else \"MISSING\"}')

chk('supernova/report (get_report_*)', t.get_report_token(), t.get_report_chat_id())
chk('equity KR scanners', t.get_equity_kr_main_token(), t.get_equity_kr_factory_chat_id())
chk('equity US scanners', t.get_equity_us_main_token(), t.get_equity_us_factory_chat_id())
if t.get_equity_kr_main_token() and not t.get_report_token():
    print('  ⚠ EQUITY_KR creds only — supernova funnel report may NOT send (needs MAIN/REPORT_*)')
" 2>/dev/null || warn "telegram_env check failed"
else
  warn "skip telegram check (no python)"
fi
echo ""

# --- 7. Recent logs ---
echo "[7] Recent factory logs ($LOG_DIR)"
for mode in scan_kr scan_us scan_us_supernova scan_us_nulrim scan_us_bowl daily_audit_kr daily_audit_us; do
  latest="$(ls -t "${LOG_DIR}/factory_${mode}_"* 2>/dev/null | head -1 || true)"
  if [[ -z "$latest" ]]; then
    warn "no logs for factory_${mode}_*"
    continue
  fi
  base="$(basename "$latest")"
  status="$(grep -E 'finished status=' "$latest" 2>/dev/null | tail -1 || true)"
  if [[ -z "$status" ]]; then
    warn "$base — no finished status (still running or crashed?)"
  else
    echo "  $base"
    echo "    $status"
    if echo "$status" | grep -q SKIPPED_LOCK; then
      warn "    → cron overlap or stale lock; no scan report sent"
    fi
    if echo "$status" | grep -q SKIPPED_SESSION; then
      warn "    → ran outside market hours (or cron TZ was UTC)"
    fi
  fi
done
echo ""

# --- 8. US scan KST daytime smell test (wall_clock in logs) ---
echo "[8] US scan KST daytime smell test"
if [[ -d "$LOG_DIR" ]]; then
  us_daytime=0
  for f in $(ls -t "${LOG_DIR}"/factory_scan_us_*.log 2>/dev/null | head -30); do
    hh="$(grep -m1 '^\[factory\.sh\] wall_clock=' "$f" 2>/dev/null \
      | sed -n 's/.*wall_clock=[0-9-]* \([0-9][0-9]\):.*/\1/p')"
    [[ -z "$hh" ]] && continue
    if [[ "$hh" =~ ^(0[89]|1[0-9]|2[0-2])$ ]]; then
      us_daytime=$((us_daytime + 1))
    fi
  done
  if [[ "${us_daytime:-0}" -gt 2 ]]; then
    fail "${us_daytime} recent US scan logs at KST 08–22h (wall_clock) — cron TZ or dispatcher broken"
    echo "       Fix: sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
    echo "            bash scripts/diag_cron_tz_effective.sh"
  else
    pass "no US scan KST daytime wall_clock pattern in recent logs"
  fi
fi
echo ""

echo "=== audit done ==="
echo "Quick recovery:"
echo "  bash scripts/diag_cron_tz_effective.sh"
echo "  sudo INSTALL_ROOT=$INSTALL_ROOT bash deploy/install_factory_cron.sh"
echo "  bash scripts/reset_factory_pipeline.sh"
echo "  bash factory.sh --scan-kr-supernova   # staggered KR slot (cron SSOT)"
echo "  bash factory.sh --scan-us-supernova   # staggered US slot (ET cron)"

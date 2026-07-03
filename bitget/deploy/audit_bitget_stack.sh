#!/usr/bin/env bash
# =============================================================================
# Bitget stack 전체 점검 — cron · lock · systemd · 텔레그램 · 최근 로그
#   cd INSTALL_ROOT && bash bitget/deploy/audit_bitget_stack.sh
#   sudo bash bitget/deploy/audit_bitget_stack.sh   (cron.d 읽기 등)
# =============================================================================
set -eu -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-$(cd "${BITGET_ROOT}/.." && pwd)}"
cd "$INSTALL_ROOT"

CRON_BITGET="${BITGET_CRON_PATH:-/etc/cron.d/dual-screener-bitget}"
LOG_DIR="${BITGET_LOG_DIR:-${BITGET_ROOT}/logs}"
PY="${INSTALL_ROOT}/venv/bin/python"
[[ -x "$PY" ]] || PY="${INSTALL_ROOT}/.venv/bin/python"

pass() { echo "  ✓ $*"; }
warn() { echo "  ⚠ $*"; }
fail() { echo "  ✗ $*"; }

echo "=== Bitget stack audit ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"
echo "time: $(date) ($(timedatectl show -p Timezone --value 2>/dev/null || echo '?'))"
echo ""

# --- env ---
if [[ -f "${INSTALL_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${INSTALL_ROOT}/.env"
  set +a
fi
if [[ -f "${BITGET_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${BITGET_ROOT}/.env"
  set +a
fi

DATA_DIR=""
if [[ -x "$PY" ]]; then
  DATA_DIR="$("$PY" -c 'from bitget.infra.data_paths import bitget_data_dir; print(bitget_data_dir())' 2>/dev/null || true)"
fi
DATA_DIR="${DATA_DIR:-${BITGET_DB_STORAGE_PATH:-$BITGET_ROOT}}"
RUNTIME_LOCK="${DATA_DIR}/.bitget_runtime.lock"
DR_LOCK="${DATA_DIR}/.bitget_data_refresh.lock"

# --- 1. Cron ---
echo "[1] Cron ($CRON_BITGET)"
if [[ ! -f "$CRON_BITGET" ]]; then
  fail "Bitget cron missing — run: sudo INSTALL_ROOT=$INSTALL_ROOT bash bitget/deploy/install_bitget_cron.sh"
else
  if grep -q $'\r' "$CRON_BITGET" 2>/dev/null; then
    fail "CRLF in bitget cron — CRON_TZ ignored"
  else
    pass "bitget cron LF ok"
  fi
  if grep -q '^CRON_TZ=UTC' "$CRON_BITGET"; then
    pass "CRON_TZ=UTC"
  else
    fail "CRON_TZ=UTC missing"
  fi
  if grep -q 'bitget.sh --scan-spot-supernova' "$CRON_BITGET"; then
    pass "staggered scan-spot-supernova scheduled"
  else
    fail "no staggered scan lines"
  fi
  if grep -q 'bitget.sh --daily-audit' "$CRON_BITGET"; then
    pass "daily-audit scheduled"
  else
    warn "no daily-audit line"
  fi
  if grep -q "${INSTALL_ROOT}" "$CRON_BITGET"; then
    pass "cron paths match INSTALL_ROOT"
  else
    warn "cron paths may not match INSTALL_ROOT (check INSTALL_ROOT)"
  fi
fi

if crontab -l -u ubuntu 2>/dev/null | grep -q 'bitget\.sh'; then
  fail "ubuntu user crontab also runs bitget.sh — duplicate schedules likely"
else
  pass "no bitget.sh in ubuntu user crontab"
fi
echo ""

# --- 2. bitget.sh ---
echo "[2] bitget.sh"
if [[ -x "${BITGET_ROOT}/deploy/bitget.sh" ]]; then
  pass "bitget.sh executable"
else
  warn "bitget.sh not executable"
fi
if [[ -f "${BITGET_ROOT}/deploy/bitget.sh" ]] && grep -q $'\r' "${BITGET_ROOT}/deploy/bitget.sh" 2>/dev/null; then
  warn "bitget.sh has CRLF"
fi
echo ""

# --- 3. Lock ---
echo "[3] Global lock ($RUNTIME_LOCK)"
for lock in "$RUNTIME_LOCK" "$DR_LOCK"; do
  if [[ -f "$lock" ]]; then
    age=$(( $(date +%s) - $(stat -c %Y "$lock" 2>/dev/null || stat -f %m "$lock") ))
    echo "  lock: $lock age=${age}s"
    sed 's/^/    /' "$lock" || true
    holder_pid="$(sed -n '3p' "$lock" 2>/dev/null || true)"
    if [[ -n "${holder_pid:-}" ]] && kill -0 "$holder_pid" 2>/dev/null; then
      warn "lock held by live pid=$holder_pid — cron may skip"
    else
      warn "stale lock — run: bash bitget/deploy/reset_bitget_pipeline.sh"
    fi
  fi
done
if [[ ! -f "$RUNTIME_LOCK" && ! -f "$DR_LOCK" ]]; then
  pass "no lock files"
fi
echo ""

# --- 4. systemd ---
echo "[4] systemd (Bitget)"
for u in dante-bitget-factory dante-bitget-ws dante-bitget-async dante-bitget-queue-worker; do
  st="$(systemctl is-active "$u" 2>/dev/null || echo inactive)"
  en="$(systemctl is-enabled "$u" 2>/dev/null || echo disabled)"
  if [[ "$st" == "active" ]]; then
    pass "$u active (enabled=$en)"
  else
    if [[ "$u" == "dante-bitget-queue-worker" && ! grep -q '\-\-enqueue' "$CRON_BITGET" 2>/dev/null ]]; then
      warn "$u is $st (optional unless cron uses --enqueue)"
    else
      fail "$u is $st (enabled=$en) — Telegram/scans need this"
    fi
  fi
done
for u in dante-bitget-watchdog.timer dante-bitget-snapshot.timer; do
  st="$(systemctl is-active "$u" 2>/dev/null || echo inactive)"
  if [[ "$st" == "active" ]]; then
    pass "$u active"
  else
    warn "$u is $st"
  fi
done
if systemctl is-active dante-factory 2>/dev/null | grep -q active; then
  warn "dante-factory (equity) active on coin server — should be disabled on Bot-2"
fi
if systemctl is-enabled dante-bitget-factory 2>/dev/null | grep -q enabled; then
  if ! systemctl is-active dante-bitget-factory 2>/dev/null | grep -q active; then
    fail "dante-bitget-factory enabled but NOT active — run deploy_bitget_factory then systemctl start"
  fi
fi
echo ""

# --- 5. Data paths ---
echo "[5] Data paths"
echo "  data_dir=$DATA_DIR"
if [[ -x "$PY" ]]; then
  "$PY" -c "
from bitget.infra import data_paths
import os
for label, fn in (
    ('market_db', data_paths.market_data_db_path),
    ('message_queue', data_paths.message_queue_db_path),
    ('ops_events', lambda: data_paths.ops_events_db_path() if hasattr(data_paths, 'ops_events_db_path') else ''),
):
    try:
        p = fn() if callable(fn) else fn
        if not p: continue
        sz = os.path.getsize(p) if os.path.isfile(p) else 0
        mark = '✓' if os.path.isfile(p) and sz > 0 else '✗'
        print(f'  {mark} {label}: {p} ({sz} bytes)')
    except Exception as e:
        print(f'  ✗ {label}: {e}')
" 2>/dev/null || warn "data_paths check failed"
else
  warn "venv python not found"
fi
echo ""

# --- 6. Telegram credentials ---
echo "[6] Telegram (bitget/.env — values not printed)"
if [[ -x "$PY" ]]; then
  "$PY" -c "
import telegram_env as t
tok = t.get_bitget_bot_token()
chat = t.get_bitget_chat_id()
ok = bool(tok and chat)
mark = '✓' if ok else '✗'
print(f'  {mark} BITGET bot: token={\"set\" if tok else \"MISSING\"} chat={\"set\" if chat else \"MISSING\"}')
if not ok:
    print('  → dante-bitget-async exits immediately; no Telegram will be sent')
    print('  → copy bitget/.env from old server or set BITGET_BOT_TOKEN + BITGET_BOT_CHAT_ID')
" 2>/dev/null || warn "telegram_env check failed"
else
  warn "skip telegram check (no python)"
fi

# async daemon journal smell
if command -v journalctl >/dev/null 2>&1; then
  async_err="$(journalctl -u dante-bitget-async -n 5 --no-pager 2>/dev/null | grep -E '큐 데몬 등록 없음|MISSING|exit-code=2' || true)"
  if [[ -n "$async_err" ]]; then
    fail "dante-bitget-async journal shows missing credentials:"
    echo "$async_err" | sed 's/^/    /'
  fi
fi
echo ""

# --- 7. Message queue backlog ---
echo "[7] Telegram queue backlog"
if [[ -x "$PY" ]]; then
  "$PY" -c "
import sqlite3, os
from bitget.infra.data_paths import message_queue_db_path
p = message_queue_db_path()
if not os.path.isfile(p):
    print('  ⚠ message_queue DB missing — no messages queued yet')
else:
    conn = sqlite3.connect(p, timeout=5)
    try:
        pending = conn.execute(\"SELECT COUNT(*) FROM message_queue WHERE status='pending'\").fetchone()[0]
        failed = conn.execute(\"SELECT COUNT(*) FROM message_queue WHERE status='failed'\").fetchone()[0]
        print(f'  pending={pending} failed={failed}')
        if pending > 10:
            print('  ⚠ large backlog — is dante-bitget-async running?')
        if failed > 0:
            print('  ⚠ failed messages — check journalctl -u dante-bitget-async')
    except Exception as e:
        print(f'  ⚠ queue check error: {e}')
    finally:
        conn.close()
" 2>/dev/null || warn "queue backlog check failed"
fi
echo ""

# --- 8. Recent logs ---
echo "[8] Recent bitget logs ($LOG_DIR)"
for mode in scan_spot_supernova scan_futures_supernova daily_audit canary track_positions; do
  latest="$(ls -t "${LOG_DIR}/bitget_${mode}_"* 2>/dev/null | head -1 || true)"
  if [[ -z "$latest" ]]; then
    warn "no logs for bitget_${mode}_*"
    continue
  fi
  base="$(basename "$latest")"
  mtime="$(stat -c %y "$latest" 2>/dev/null | cut -d. -f1 || stat -f %Sm "$latest" 2>/dev/null || echo '?')"
  tail3="$(tail -3 "$latest" 2>/dev/null || true)"
  echo "  $base (mtime=$mtime)"
  if echo "$tail3" | grep -qiE 'SKIPPED|error|traceback|database is locked'; then
    warn "    $(echo "$tail3" | tail -1)"
  else
    echo "    $(echo "$tail3" | tail -1)"
  fi
done
echo ""

# --- 9. Disk / RAM ---
echo "[9] Resources"
df -h / 2>/dev/null | tail -1 | awk '{print "  disk: "$3" used / "$2" ("$5")"}' || true
free -h 2>/dev/null | awk '/^Mem:/{print "  mem: "$3" used / "$2}' || true
echo ""

echo "=== audit done ==="
echo "Quick recovery (Bot-2 coin server only):"
echo "  sudo INSTALL_ROOT=$INSTALL_ROOT ./bitget/deploy/update_bitget.sh"
echo "  bash bitget/deploy/master_sync_bitget.sh"
echo "  sudo systemctl start dante-bitget-ws dante-bitget-async dante-bitget-factory dante-bitget-queue-worker"
echo "  sudo INSTALL_ROOT=$INSTALL_ROOT bash bitget/deploy/install_bitget_cron.sh"

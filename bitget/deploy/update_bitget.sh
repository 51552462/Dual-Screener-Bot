#!/usr/bin/env bash
# =============================================================================
# Bitget stack zero-downtime update (equity dante-* units untouched)
#
#   sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/update_bitget.sh
#
# Steps:
#   1. Backup bitget *.sqlite + config to /var/backups/bitget-pre-update/
#   2. git pull (DEPLOY_USER)
#   3. Re-install bitget systemd units
#   4. Graceful stop → restart bitget services + timers
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-$REPO_ROOT}"
export INSTALL_ROOT
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"

# shellcheck source=../../deploy/dante_venv.sh
source "${REPO_ROOT}/deploy/dante_venv.sh"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "Run as root: sudo ./bitget/deploy/update_bitget.sh" >&2
  exit 1
fi

DANTE_PY="$(dante_resolve_python "$INSTALL_ROOT" || true)"
if [[ -z "${DANTE_PY:-}" ]]; then
  echo "venv not found under ${INSTALL_ROOT}/venv — abort" >&2
  exit 1
fi
echo "[update_bitget] INSTALL_ROOT=$INSTALL_ROOT venv=$DANTE_PY"

_bitget_pre_update_backup() {
  local stamp dest
  stamp="$(date -u +%Y%m%d_%H%M%S_utc)"
  dest="/var/backups/bitget-pre-update/${stamp}"
  mkdir -p "$dest"

  sudo -E -u "$DEPLOY_USER" env \
    INSTALL_ROOT="$INSTALL_ROOT" \
    PYTHONPATH="$INSTALL_ROOT" \
    _BG_BACKUP_DEST="$dest" \
    "$DANTE_PY" -c "
import os, shutil, sqlite3
from bitget.infra.data_paths import bitget_data_dir
dest = os.environ['_BG_BACKUP_DEST']
data = bitget_data_dir()
os.makedirs(dest, exist_ok=True)

def backup_sqlite(src, out_name):
    if not os.path.isfile(src):
        return
    out = os.path.join(dest, out_name)
    try:
        s = sqlite3.connect(src, timeout=60)
        d = sqlite3.connect(out, timeout=60)
        try:
            s.backup(d)
        finally:
            d.close()
            s.close()
    except Exception:
        shutil.copy2(src, out)
    print(f'  sqlite: {out_name}')

for name in (
    'bitget_market_data.sqlite',
    'bitget_market_data_snapshot.sqlite',
    'bitget_system_config.sqlite',
    'bitget_ops_events.sqlite',
    'bitget_message_queue.sqlite',
):
    backup_sqlite(os.path.join(data, name), name)

for rel in ('bitget_system_config.json', 'bitget_schedule_lock_state.json'):
    src = os.path.join(data, rel)
    if os.path.isfile(src):
        shutil.copy2(src, os.path.join(dest, rel))
        print(f'  file: {rel}')
print(f'  data_dir={data}')
"
  echo "[update_bitget] backup -> $dest"
}

_bitget_stop_services() {
  systemctl stop dante-bitget-factory.service 2>/dev/null || true
  systemctl stop dante-bitget-dashboard.service dante-bitget-heatmap.service 2>/dev/null || true
  systemctl stop dante-bitget-ws.service dante-bitget-async.service 2>/dev/null || true
  sleep 2
}

_bitget_start_services() {
  systemctl daemon-reload
  systemctl start dante-bitget-ws.service
  sleep 1
  systemctl start dante-bitget-async.service
  systemctl start dante-bitget-factory.service
  systemctl start dante-bitget-dashboard.service
  systemctl start dante-bitget-heatmap.service
  systemctl restart dante-bitget-watchdog.timer dante-bitget-snapshot.timer 2>/dev/null || true
}

echo "[1/5] pre-update backup"
_bitget_pre_update_backup

echo "[2/5] git pull ($DEPLOY_USER)"
if [[ -d "$INSTALL_ROOT/.git" ]]; then
  sudo -u "$DEPLOY_USER" git -C "$INSTALL_ROOT" pull --ff-only
else
  echo "  (warn) no .git — skip pull"
fi

echo "[3/5] reinstall bitget systemd units"
sudo INSTALL_ROOT="$INSTALL_ROOT" "${SCRIPT_DIR}/deploy_bitget_factory.sh"

echo "[4/5] graceful stop bitget stack"
_bitget_stop_services

echo "[5/5] restart bitget stack"
_bitget_start_services

echo ""
echo "=== is-active ==="
systemctl is-active \
  dante-bitget-ws.service \
  dante-bitget-factory.service \
  dante-bitget-async.service \
  dante-bitget-dashboard.service \
  dante-bitget-heatmap.service \
  2>/dev/null || true
echo ""
echo "=== logs ==="
echo "sudo journalctl -u dante-bitget-factory -u dante-bitget-ws -u dante-bitget-dashboard -f"
echo ""
echo "[update_bitget] done — equity dante-* untouched"

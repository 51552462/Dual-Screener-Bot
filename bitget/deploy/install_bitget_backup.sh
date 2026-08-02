#!/usr/bin/env bash
# L-2 — install dante-bitget-backup.timer (P0-5).
# Usage:
#   sudo INSTALL_ROOT=... ./install_bitget_backup.sh
#   sudo ... ./install_bitget_backup.sh --test   # dry-run backup + restore drill

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"

_load_env() {
  local f
  for f in "${INSTALL_ROOT}/.env" "${INSTALL_ROOT}/bitget/.env"; do
    if [[ -f "$f" ]]; then
      set -a
      # shellcheck disable=SC1090
      source "$f"
      set +a
    fi
  done
}

_load_env

echo "[install_bitget_backup] INSTALL_ROOT=$INSTALL_ROOT"
BACKUP_DIR="${BITGET_BACKUP_DIR:-${BITGET_DB_BACKUP_DIR:-}}"
if [[ -n "$BACKUP_DIR" ]]; then
  echo "[install_bitget_backup] BITGET_BACKUP_DIR=$BACKUP_DIR"
  sudo mkdir -p "$BACKUP_DIR"
  sudo chown ubuntu:ubuntu "$BACKUP_DIR" 2>/dev/null || true
fi

sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/backup_bitget_db.sh"
sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/scripts/bitget_restore_drill.sh"

SVC="/etc/systemd/system/dante-bitget-backup.service"
sed "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" \
  "${SCRIPT_DIR}/systemd/dante-bitget-backup.service.in" | sudo tee "$SVC" >/dev/null
sudo cp "${SCRIPT_DIR}/systemd/dante-bitget-backup.timer" \
  "/etc/systemd/system/dante-bitget-backup.timer"
echo "  installed dante-bitget-backup.{service,timer}"

sudo systemctl daemon-reload
sudo systemctl enable dante-bitget-backup.timer
sudo systemctl start dante-bitget-backup.timer 2>/dev/null || true

if [[ "${1:-}" == "--test" ]]; then
  echo "[install_bitget_backup] backup dry-run:"
  INSTALL_ROOT="$INSTALL_ROOT" bash "${INSTALL_ROOT}/bitget/deploy/backup_bitget_db.sh" || true
  echo "[install_bitget_backup] restore drill dry-run:"
  INSTALL_ROOT="$INSTALL_ROOT" bash "${INSTALL_ROOT}/bitget/deploy/scripts/bitget_restore_drill.sh" || true
fi

echo "[install_bitget_backup] defaults (override in bitget/.env):"
echo "  BITGET_BACKUP_ENABLED=true"
echo "  BITGET_BACKUP_RETENTION_DAYS=7"
echo "  BITGET_BACKUP_DIR=<optional separate partition>"

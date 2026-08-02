#!/usr/bin/env bash
# L-1 — install logrotate + journal vacuum timer for dante-bitget-* (P0-1).
# Usage:
#   sudo INSTALL_ROOT=... BITGET_LOG_DIR=... ./install_bitget_logrotate.sh
#   sudo ... ./install_bitget_logrotate.sh --test   # logrotate -d dry-run

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

LOG_DIR="${BITGET_LOG_DIR:-${INSTALL_ROOT}/bitget/logs}"
JOURNAL_MAX_USE="${BITGET_JOURNAL_MAX_USE:-400M}"
JOURNAL_RETENTION="${BITGET_JOURNAL_MAX_RETENTION:-30d}"
# Parse retention for journald MaxRetentionSec (default 30d → 2592000s)
JOURNAL_RETENTION_SEC="${BITGET_JOURNAL_MAX_RETENTION_SEC:-2592000}"
JOURNAL_SYSTEM_MAX_USE="${BITGET_JOURNAL_SYSTEM_MAX_USE:-500M}"
INSTALL_JOURNALD_DROPIN="${BITGET_INSTALL_JOURNALD_DROPIN:-0}"

echo "[install_bitget_logrotate] INSTALL_ROOT=$INSTALL_ROOT"
echo "[install_bitget_logrotate] BITGET_LOG_DIR=$LOG_DIR"

sudo mkdir -p "$LOG_DIR"
sudo chown ubuntu:ubuntu "$LOG_DIR" 2>/dev/null || true

LR_OUT="/etc/logrotate.d/bitget-dante"
sed -e "s|@@BITGET_LOG_DIR@@|${LOG_DIR}|g" \
  "${SCRIPT_DIR}/logrotate/bitget-dante.conf.in" | sudo tee "$LR_OUT" >/dev/null
echo "  installed $LR_OUT"

sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/scripts/bitget_journal_vacuum.sh"

# systemd journal vacuum unit + timer
JV_SVC="/etc/systemd/system/dante-bitget-journal-vacuum.service"
sed "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" \
  "${SCRIPT_DIR}/systemd/dante-bitget-journal-vacuum.service.in" | sudo tee "$JV_SVC" >/dev/null
sudo cp "${SCRIPT_DIR}/systemd/dante-bitget-journal-vacuum.timer" \
  "/etc/systemd/system/dante-bitget-journal-vacuum.timer"
echo "  installed dante-bitget-journal-vacuum.{service,timer}"

if [[ "$INSTALL_JOURNALD_DROPIN" == "1" || "$INSTALL_JOURNALD_DROPIN" == "true" ]]; then
  sudo mkdir -p /etc/systemd/journald.conf.d
  sed -e "s|@@BITGET_JOURNAL_SYSTEM_MAX_USE@@|${JOURNAL_SYSTEM_MAX_USE}|g" \
      -e "s|@@BITGET_JOURNAL_MAX_RETENTION_SEC@@|${JOURNAL_RETENTION_SEC}|g" \
    "${SCRIPT_DIR}/journald/99-bitget-journal.conf.in" \
    | sudo tee /etc/systemd/journald.conf.d/99-bitget-journal.conf >/dev/null
  echo "  installed journald drop-in (restart systemd-journald on server)"
  sudo systemctl restart systemd-journald 2>/dev/null || true
fi

sudo systemctl daemon-reload
sudo systemctl enable dante-bitget-journal-vacuum.timer
sudo systemctl start dante-bitget-journal-vacuum.timer 2>/dev/null || true

if [[ "${1:-}" == "--test" ]]; then
  echo "[install_bitget_logrotate] logrotate dry-run:"
  if command -v logrotate >/dev/null 2>&1; then
    sudo logrotate -d "$LR_OUT" 2>&1 | head -n 40 || true
  else
    echo "  logrotate binary not found (dev host OK)"
  fi
  echo "[install_bitget_logrotate] journal vacuum dry-run:"
  BITGET_JOURNAL_MAX_USE="$JOURNAL_MAX_USE" BITGET_JOURNAL_MAX_RETENTION="$JOURNAL_RETENTION" \
    bash "${INSTALL_ROOT}/bitget/deploy/scripts/bitget_journal_vacuum.sh" || true
fi

echo ""
echo "[install_bitget_logrotate] defaults (override in bitget/.env):"
echo "  BITGET_JOURNAL_MAX_USE=${JOURNAL_MAX_USE}"
echo "  BITGET_JOURNAL_MAX_RETENTION=${JOURNAL_RETENTION}"
echo "  BITGET_STAMPED_LOG_RETENTION_DAYS=${BITGET_STAMPED_LOG_RETENTION_DAYS:-14}"
echo "  BITGET_INSTALL_JOURNALD_DROPIN=${INSTALL_JOURNALD_DROPIN}"
echo ""
echo "  verify: systemctl list-timers dante-bitget-journal-vacuum.timer"
echo "  verify: sudo logrotate -d /etc/logrotate.d/bitget-dante"

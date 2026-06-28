#!/usr/bin/env bash

# Install Bitget systemd units (does NOT touch dante-factory / equity stack)

set -euo pipefail



SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"

INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"



echo "[deploy_bitget] INSTALL_ROOT=$INSTALL_ROOT"



for f in bitget/deploy/systemd/*.in; do

  base="$(basename "$f" .in)"

  out="/etc/systemd/system/${base}"

  sed "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" "$f" | sudo tee "$out" >/dev/null

  echo "  installed $out"

done



sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/bitget.sh"
sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/update_bitget.sh"
sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/deploy_bitget_factory.sh"
sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/install_bitget_cron.sh"
sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/reset_bitget_pipeline.sh"
sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/master_sync_bitget.sh"
sudo chmod +x "${INSTALL_ROOT}/bitget/deploy/entrypoints/"*.sh



sudo systemctl daemon-reload



BITGET_SERVICES=(

  dante-bitget-ws.service

  dante-bitget-factory.service

  dante-bitget-queue-worker.service

  dante-bitget-async.service

  dante-bitget-dashboard.service

  dante-bitget-heatmap.service

)

BITGET_TIMERS=(

  dante-bitget-watchdog.timer

  dante-bitget-snapshot.timer

)



sudo systemctl enable "${BITGET_SERVICES[@]}" "${BITGET_TIMERS[@]}"



echo "[deploy_bitget] enabled units:"

printf '  %s\n' "${BITGET_SERVICES[@]}" "${BITGET_TIMERS[@]}"

echo ""

echo "[deploy_bitget] first start:"

echo "  sudo systemctl start dante-bitget-ws dante-bitget-async dante-bitget-factory dante-bitget-queue-worker"

echo "  sudo systemctl start dante-bitget-dashboard dante-bitget-heatmap"

echo "  sudo systemctl start dante-bitget-watchdog.timer dante-bitget-snapshot.timer"

echo ""

echo "[deploy_bitget] ensure bitget/.env (or root .env) contains:"
echo "  BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot"
echo "  BITGET_DB_STORAGE_PATH=<optional data root>"
echo ""
echo "  template: bitget/deploy/bitget_resource_limits.env.example"
echo ""
echo "[deploy_bitget] update after git pull:"
echo "  sudo INSTALL_ROOT=${INSTALL_ROOT} ${INSTALL_ROOT}/bitget/deploy/update_bitget.sh"
echo ""

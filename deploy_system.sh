#!/usr/bin/env bash
# =============================================================================
# Dante 팩토리 — systemd 유닛 원클릭 배포 (Docker 없음)
#   sudo INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ./deploy_system.sh
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
UNITS=(dante-main dante-streamlit)

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root(sudo)로 실행하세요." >&2
  echo "예: sudo INSTALL_ROOT=${INSTALL_ROOT} ${REPO_ROOT}/deploy_system.sh" >&2
  exit 1
fi

if [[ ! -d "${INSTALL_ROOT}" ]]; then
  echo "INSTALL_ROOT 가 없습니다: ${INSTALL_ROOT}" >&2
  exit 1
fi

ENV_FILE="${INSTALL_ROOT}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  chmod 600 "${ENV_FILE}" || true
  echo "✓ chmod 600 ${ENV_FILE}"
else
  echo "경고: ${ENV_FILE} 없음 — EnvironmentFile=- 이므로 유닛은 기동 가능합니다." >&2
fi

TMPD="$(mktemp -d)"
trap 'rm -rf "${TMPD}"' EXIT

for name in "${UNITS[@]}"; do
  src="${REPO_ROOT}/deploy/systemd/${name}.service.in"
  if [[ ! -f "${src}" ]]; then
    echo "누락: ${src}" >&2
    exit 1
  fi
  sed "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" "${src}" >"${TMPD}/${name}.service"
  install -m 0644 "${TMPD}/${name}.service" "/etc/systemd/system/${name}.service"
  echo "✓ installed /etc/systemd/system/${name}.service (0644)"
done

if [[ -f "${REPO_ROOT}/RUNBOOK.md" ]]; then
  install -m 0644 "${REPO_ROOT}/RUNBOOK.md" "${INSTALL_ROOT}/RUNBOOK.md"
  echo "✓ RUNBOOK.md -> ${INSTALL_ROOT}/RUNBOOK.md"
fi

systemctl daemon-reload
echo "✓ systemctl daemon-reload"

for name in "${UNITS[@]}"; do
  systemctl enable --now "${name}.service"
  echo "✓ enable --now ${name}.service"
done

echo ""
systemctl --no-pager --full status dante-main.service dante-streamlit.service || true

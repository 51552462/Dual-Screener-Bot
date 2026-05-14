#!/usr/bin/env bash
# =============================================================================
# Dante 팩토리 — systemd 멀티 유닛 원클릭 설치 (Docker 없음)
#
# 사용법:
#   sudo INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot /path/to/repo/deploy/ubuntu/install.sh
#
# INSTALL_ROOT 기본값: /home/ubuntu/dante_bots/Dual-Screener-Bot
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
UNITS=(dante-main dante-streamlit)

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "이 스크립트는 systemd 설치를 위해 root(sudo)로 실행해야 합니다." >&2
  echo "예: sudo INSTALL_ROOT=${INSTALL_ROOT} ${SCRIPT_DIR}/install.sh" >&2
  exit 1
fi

if [[ ! -d "${INSTALL_ROOT}" ]]; then
  echo "INSTALL_ROOT 디렉터리가 없습니다: ${INSTALL_ROOT}" >&2
  exit 1
fi

ENV_FILE="${INSTALL_ROOT}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  chmod 600 "${ENV_FILE}" || true
  echo "[install] chmod 600 ${ENV_FILE}"
else
  echo "[install] 경고: ${ENV_FILE} 없음 — EnvironmentFile=- 이므로 유닛 기동은 가능합니다." >&2
fi

TMPD="$(mktemp -d)"
trap 'rm -rf "${TMPD}"' EXIT

for name in "${UNITS[@]}"; do
  src="${SCRIPT_DIR}/${name}.service"
  if [[ ! -f "${src}" ]]; then
    echo "[install] 누락: ${src}" >&2
    exit 1
  fi
  sed "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" "${src}" >"${TMPD}/${name}.service"
  install -m 0644 "${TMPD}/${name}.service" "/etc/systemd/system/${name}.service"
  echo "[install] -> /etc/systemd/system/${name}.service"
done

systemctl daemon-reload
echo "[install] systemctl daemon-reload"

for name in "${UNITS[@]}"; do
  systemctl enable --now "${name}.service"
  echo "[install] enable --now ${name}.service"
done

echo ""
echo "[install] 상태 확인:"
systemctl --no-pager --full status dante-main.service dante-streamlit.service || true

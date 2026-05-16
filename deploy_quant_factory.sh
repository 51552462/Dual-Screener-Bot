#!/usr/bin/env bash
# =============================================================================
# Dante 퀀트 팩토리 — Ubuntu systemd 네이티브 배포 (Docker 없음)
#   sudo INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot ./deploy_quant_factory.sh
#
# 설치 대상: dante-factory, dante-dashboard, dante-async, dante-snapshot, dante-watchdog, dante-backup (timer)
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"

SERVICES=(dante-factory dante-dashboard dante-async dante-snapshot dante-watchdog dante-backup)
TIMERS=(dante-snapshot.timer dante-watchdog.timer dante-backup.timer)

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root(sudo)로 실행하세요." >&2
  echo "예: sudo INSTALL_ROOT=${INSTALL_ROOT} ${REPO_ROOT}/deploy_quant_factory.sh" >&2
  exit 1
fi

if [[ ! -d "${INSTALL_ROOT}" ]]; then
  echo "INSTALL_ROOT 디렉터리 없음: ${INSTALL_ROOT}" >&2
  exit 1
fi

ENV_FILE="${INSTALL_ROOT}/.env"
if [[ -f "${ENV_FILE}" ]]; then
  chmod 600 "${ENV_FILE}" || true
  echo "✓ chmod 600 ${ENV_FILE}"
else
  echo "경고: ${ENV_FILE} 없음 — EnvironmentFile=- 로 유닛은 기동 가능하나 시크릿을 채우세요." >&2
fi

install -d -m 0700 -o root -g root /var/lib/dante-watchdog
echo "✓ state dir /var/lib/dante-watchdog (워치독 상태)"

# --- journald 디스크 상한 (SystemMaxUse=2G) ---
JCONF=/etc/systemd/journald.conf
if [[ -f "${JCONF}" ]]; then
  if grep -qE '^[[:space:]]*SystemMaxUse=2G[[:space:]]*$' "${JCONF}"; then
    echo "✓ journald SystemMaxUse=2G (이미 설정됨)"
  else
    cp -a "${JCONF}" "${JCONF}.bak-dante-$(date +%s)"
    if grep -qE '^[[:space:]]*SystemMaxUse=' "${JCONF}"; then
      sed -i 's/^[[:space:]]*SystemMaxUse=.*/SystemMaxUse=2G/' "${JCONF}"
    elif grep -qE '^[[:space:]]*\[Journal\]' "${JCONF}"; then
      sed -i '/^[[:space:]]*\[Journal\]/a SystemMaxUse=2G' "${JCONF}"
    else
      printf '\n[Journal]\nSystemMaxUse=2G\n' >>"${JCONF}"
    fi
    systemctl restart systemd-journald.service || true
    echo "✓ journald SystemMaxUse=2G 적용 및 systemd-journald 재시작"
  fi
else
  echo "경고: ${JCONF} 없음 — journald 설정 스킵" >&2
fi

if [[ -f "${REPO_ROOT}/RUNBOOK.md" ]]; then
  install -m 0644 "${REPO_ROOT}/RUNBOOK.md" "${INSTALL_ROOT}/RUNBOOK.md"
  echo "✓ RUNBOOK.md -> ${INSTALL_ROOT}/RUNBOOK.md"
fi

TMPD="$(mktemp -d)"
trap 'rm -rf "${TMPD}"' EXIT

for name in "${SERVICES[@]}"; do
  src="${REPO_ROOT}/deploy/systemd/${name}.service.in"
  if [[ ! -f "${src}" ]]; then
    echo "누락: ${src}" >&2
    exit 1
  fi
  sed "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" "${src}" >"${TMPD}/${name}.service"
  install -m 0644 "${TMPD}/${name}.service" "/etc/systemd/system/${name}.service"
  echo "✓ installed /etc/systemd/system/${name}.service"
done

for timer in "${TIMERS[@]}"; do
  timer_src="${REPO_ROOT}/deploy/systemd/${timer}"
  if [[ ! -f "${timer_src}" ]]; then
    echo "누락: ${timer_src}" >&2
    exit 1
  fi
  install -m 0644 "${timer_src}" "/etc/systemd/system/${timer}"
  echo "✓ installed /etc/systemd/system/${timer}"
done

systemctl daemon-reload
echo "✓ systemctl daemon-reload"

systemctl enable --now dante-factory.service
systemctl enable --now dante-dashboard.service
systemctl enable --now dante-async.service
echo "✓ enable --now dante-factory dante-dashboard dante-async"

for timer in "${TIMERS[@]}"; do
  systemctl enable --now "${timer}"
  echo "✓ enable --now ${timer}"
done

echo ""
systemctl --no-pager --full status dante-factory.service dante-dashboard.service dante-async.service || true
echo ""
echo "=== timers ==="
systemctl list-timers "${TIMERS[@]}" --no-pager || true

#!/usr/bin/env bash
# =============================================================================
# Factory cron SSOT → /etc/cron.d/dual-screener-factory
#   - CRON_TZ=Asia/Seoul, LF only (CRLF 제거)
#   - INSTALL_ROOT 경로 치환
#   sudo INSTALL_ROOT=/path/to/repo ./deploy/install_factory_cron.sh
# =============================================================================
set -eu -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
TEMPLATE="${REPO_ROOT}/deploy/factory.crontab.example"
DEST="/etc/cron.d/dual-screener-factory"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root(sudo)로 실행하세요." >&2
  exit 1
fi

if [[ ! -f "${TEMPLATE}" ]]; then
  echo "템플릿 없음: ${TEMPLATE}" >&2
  exit 1
fi

DEFAULT_ROOT="/home/ubuntu/dante_bots/Dual-Screener-Bot"
TMP="$(mktemp)"
trap 'rm -f "${TMP}"' EXIT

sed "s|${DEFAULT_ROOT}|${INSTALL_ROOT}|g" "${TEMPLATE}" | sed 's/\r$//' >"${TMP}"
install -m 0644 "${TMP}" "${DEST}"
chmod +x "${INSTALL_ROOT}/factory.sh" 2>/dev/null || true

if grep -q $'\r' "${DEST}" 2>/dev/null; then
  echo "ERROR: ${DEST} still contains CRLF — CRON_TZ will be ignored; cron runs on UTC." >&2
  exit 1
fi
if ! grep -q '^CRON_TZ=Asia/Seoul' "${DEST}"; then
  echo "ERROR: ${DEST} missing CRON_TZ=Asia/Seoul" >&2
  exit 1
fi

echo "✓ installed ${DEST} (CRON_TZ=Asia/Seoul, INSTALL_ROOT=${INSTALL_ROOT})"

if command -v systemctl >/dev/null 2>&1; then
  systemctl reload cron 2>/dev/null || systemctl reload crond 2>/dev/null || service cron reload 2>/dev/null || true
fi

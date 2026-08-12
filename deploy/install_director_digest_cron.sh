#!/usr/bin/env bash
# =============================================================================
# Director digest cron SSOT → /etc/cron.d/dual-screener-director-digest
#   - North Star daily/weekly/monthly only (REPORT_BOT)
#   - 코인 전용 서버(Bot-2): factory-kr 없이 north-star만 (deploy-watch/iv-obs = equity factory-kr)
#   sudo INSTALL_ROOT=/path/to/repo bash deploy/install_director_digest_cron.sh
# =============================================================================
set -eu -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
TEMPLATE="${REPO_ROOT}/deploy/director.digest.crontab.example"
DEST="/etc/cron.d/dual-screener-director-digest"
DEFAULT_ROOT="/home/ubuntu/dante_bots/Dual-Screener-Bot"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root(sudo)로 실행하세요." >&2
  exit 1
fi

FACTORY_KR="/etc/cron.d/dual-screener-factory-kr"
if [[ -f "${FACTORY_KR}" ]] && grep -q 'factory.sh --north-star-digest daily' "${FACTORY_KR}" 2>/dev/null; then
  echo "✓ skip: ${FACTORY_KR} already schedules north-star-digest (stock server — no duplicate)"
  exit 0
fi

echo "=== Generate director digest cron from generate_factory_crontab.py (SSOT) ==="
GEN_PY="${REPO_ROOT}/deploy/generate_factory_crontab.py"
if [[ ! -f "${GEN_PY}" ]]; then
  echo "ERROR: ${GEN_PY} missing" >&2
  exit 1
fi
if [[ -x "${INSTALL_ROOT}/venv/bin/python" ]]; then
  "${INSTALL_ROOT}/venv/bin/python" "${GEN_PY}" --install-root "${INSTALL_ROOT}"
elif command -v python3 >/dev/null 2>&1; then
  python3 "${GEN_PY}" --install-root "${INSTALL_ROOT}"
else
  echo "ERROR: python3 required" >&2
  exit 1
fi

if [[ ! -f "${TEMPLATE}" ]]; then
  echo "템플릿 없음: ${TEMPLATE}" >&2
  exit 1
fi

tmp="$(mktemp)"
sed "s|${DEFAULT_ROOT}|${INSTALL_ROOT}|g" "${TEMPLATE}" | sed 's/\r$//' >"${tmp}"
install -m 0644 "${tmp}" "${DEST}"
rm -f "${tmp}"

if grep -q $'\r' "${DEST}" 2>/dev/null; then
  echo "ERROR: ${DEST} contains CRLF — CRON_TZ will break." >&2
  exit 1
fi
if ! grep -q "^CRON_TZ=Asia/Seoul" "${DEST}"; then
  echo "ERROR: ${DEST} missing CRON_TZ=Asia/Seoul" >&2
  exit 1
fi
if ! grep -q 'factory.sh --north-star-digest daily' "${DEST}"; then
  echo "ERROR: ${DEST} missing north-star-digest daily line" >&2
  exit 1
fi

chmod +x "${INSTALL_ROOT}/factory.sh" 2>/dev/null || true

echo "✓ installed ${DEST} (CRON_TZ=Asia/Seoul)"
echo "  INSTALL_ROOT=${INSTALL_ROOT}"
echo "  daily north-star: 19:30 KST (deploy-watch/iv-obs = stock factory-kr only)"
echo "  Verify: python deploy/generate_factory_crontab.py --check"

if command -v systemctl >/dev/null 2>&1; then
  systemctl reload cron 2>/dev/null || systemctl reload crond 2>/dev/null || service cron reload 2>/dev/null || true
fi

#!/usr/bin/env bash
# =============================================================================
# Bitget cron SSOT → /etc/cron.d/dual-screener-bitget
#   - CRON_TZ=UTC staggered spot/futures scans (50 min slots)
#   - LF only, INSTALL_ROOT path substitution
#   sudo INSTALL_ROOT=/path/to/repo bash bitget/deploy/install_bitget_cron.sh
# =============================================================================
set -eu -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
TEMPLATE="${REPO_ROOT}/bitget/deploy/bitget.crontab.example"
DEST="/etc/cron.d/dual-screener-bitget"
DEFAULT_ROOT="/home/ubuntu/dante_bots/Dual-Screener-Bot"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root(sudo)로 실행하세요." >&2
  exit 1
fi

echo "=== Generate Bitget cron from bitget_scan_schedule.py (SSOT) ==="
GEN_PY="${REPO_ROOT}/bitget/deploy/generate_bitget_crontab.py"
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
if ! grep -q "^CRON_TZ=UTC" "${DEST}"; then
  echo "ERROR: ${DEST} missing CRON_TZ=UTC" >&2
  exit 1
fi
if ! grep -q 'bitget.sh --scan-spot-supernova' "${DEST}"; then
  echo "ERROR: ${DEST} missing staggered scan lines" >&2
  exit 1
fi
if grep -E '^[^#[:space:]].*--scan-all' "${DEST}" >/dev/null 2>&1; then
  echo "ERROR: ${DEST} still has active --scan-all cron (use staggered slots only)" >&2
  exit 1
fi

chmod +x "${INSTALL_ROOT}/bitget/deploy/bitget.sh" 2>/dev/null || true

echo "✓ installed ${DEST} (CRON_TZ=UTC)"
echo "  INSTALL_ROOT=${INSTALL_ROOT}"
echo ""
echo "=== Staggered scan schedule (50 min, one scanner per cron) ==="
echo "  SPOT    UTC 01:00→ supernova → nulrim → … → shadow → 2nd pass ×4"
echo "  FUTURES UTC 01:00→ supernova → nulrim → … → shadow → 2nd pass ×4"
echo "  Legacy --scan-all: manual recovery only"
echo ""
echo "  SSOT: bitget/bitget_scan_schedule.py"
echo "  Verify: python bitget/deploy/generate_bitget_crontab.py --check"

if command -v systemctl >/dev/null 2>&1; then
  systemctl reload cron 2>/dev/null || systemctl reload crond 2>/dev/null || service cron reload 2>/dev/null || true
fi

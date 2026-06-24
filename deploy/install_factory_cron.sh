#!/usr/bin/env bash
# =============================================================================
# Factory cron SSOT → /etc/cron.d/dual-screener-factory-{kr,us}
#   - KR: CRON_TZ=Asia/Seoul (staggered scans + daily-kr + daily-us + weekly)
#   - US: factory_slot_dispatcher (KST poll + ET slot SSOT; CRON_TZ=Asia/Seoul)
#   - LF only (CRLF 제거), INSTALL_ROOT 경로 치환
#   sudo INSTALL_ROOT=/path/to/repo ./deploy/install_factory_cron.sh
# =============================================================================
set -eu -o pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
KR_TEMPLATE="${REPO_ROOT}/deploy/factory.kr.crontab.example"
US_TEMPLATE="${REPO_ROOT}/deploy/factory.us.crontab.example"
DEST_KR="/etc/cron.d/dual-screener-factory-kr"
DEST_US="/etc/cron.d/dual-screener-factory-us"
LEGACY_DEST="/etc/cron.d/dual-screener-factory"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root(sudo)로 실행하세요." >&2
  exit 1
fi

DEFAULT_ROOT="/home/ubuntu/dante_bots/Dual-Screener-Bot"

echo "=== Generate cron templates from factory_scan_schedule.py (SSOT) ==="
GEN_PY="${REPO_ROOT}/deploy/generate_factory_crontab.py"
if [[ ! -f "${GEN_PY}" ]]; then
  echo "ERROR: ${GEN_PY} missing — cannot install cron without SSOT generator." >&2
  exit 1
fi
if [[ -x "${INSTALL_ROOT}/venv/bin/python" ]]; then
  "${INSTALL_ROOT}/venv/bin/python" "${GEN_PY}" --install-root "${INSTALL_ROOT}"
elif command -v python3 >/dev/null 2>&1; then
  python3 "${GEN_PY}" --install-root "${INSTALL_ROOT}"
else
  echo "ERROR: python3 required to generate cron templates." >&2
  exit 1
fi

if [[ ! -f "${KR_TEMPLATE}" ]] || [[ ! -f "${US_TEMPLATE}" ]]; then
  echo "템플릿 없음: ${KR_TEMPLATE} 또는 ${US_TEMPLATE}" >&2
  exit 1
fi
_install_one() {
  local template="$1"
  local dest="$2"
  local expect_tz="$3"
  local tmp
  tmp="$(mktemp)"
  sed "s|${DEFAULT_ROOT}|${INSTALL_ROOT}|g" "${template}" | sed 's/\r$//' >"${tmp}"
  install -m 0644 "${tmp}" "${dest}"
  rm -f "${tmp}"
  if grep -q $'\r' "${dest}" 2>/dev/null; then
    echo "ERROR: ${dest} still contains CRLF — CRON_TZ will be ignored." >&2
    exit 1
  fi
  if ! grep -q "^CRON_TZ=${expect_tz}" "${dest}"; then
    echo "ERROR: ${dest} missing CRON_TZ=${expect_tz}" >&2
    exit 1
  fi
  if grep -q 'factory_slot_dispatcher.py' "${dest}" 2>/dev/null; then
    if ! grep -q 'factory_slot_dispatcher.py --market US' "${dest}"; then
      echo "ERROR: ${dest} missing factory_slot_dispatcher.py --market US" >&2
      exit 1
    fi
    return 0
  fi
  if ! grep -q 'TZ=America/New_York bash ./factory.sh' "${dest}" 2>/dev/null \
    && [[ "${expect_tz}" == "America/New_York" ]]; then
    echo "ERROR: ${dest} missing per-line TZ=America/New_York on factory.sh" >&2
    exit 1
  fi
  if ! grep -q 'TZ=Asia/Seoul bash ./factory.sh' "${dest}" 2>/dev/null \
    && [[ "${expect_tz}" == "Asia/Seoul" ]]; then
    echo "ERROR: ${dest} missing per-line TZ=Asia/Seoul on factory.sh" >&2
    exit 1
  fi
}

_install_one "${KR_TEMPLATE}" "${DEST_KR}" "Asia/Seoul"
_install_one "${US_TEMPLATE}" "${DEST_US}" "Asia/Seoul"

# 레거시 단일 파일 제거 (중복 스케줄 방지)
if [[ -f "${LEGACY_DEST}" ]]; then
  rm -f "${LEGACY_DEST}"
  echo "✓ removed legacy ${LEGACY_DEST}"
fi

chmod +x "${INSTALL_ROOT}/factory.sh" 2>/dev/null || true

echo "✓ installed ${DEST_KR} (CRON_TZ=Asia/Seoul)"
echo "✓ installed ${DEST_US} (slot dispatcher + CRON_TZ=Asia/Seoul)"
echo "  INSTALL_ROOT=${INSTALL_ROOT}"
echo ""
echo "=== Staggered scan schedule (50 min slots, one scanner per cron) ==="
echo "  KR (KST Mon-Fri): 10:00 supernova → … → 14:10 bowl → 15:00–17:30 2nd pass"
echo "  US (ET  Mon-Fri): 10:00 supernova → … → 13:20 bowl → 14:10–16:40 2nd pass (via factory_slot_dispatcher)"
echo "  daily-kr: 18:45 KST · daily-us: 06:45 KST (Tue-Sat) · weekly: Sat 10:05 KST"
echo ""
echo "  SSOT code: factory_scan_schedule.py · factory_slot_dispatcher.py"
echo "  Diagnose: bash ${INSTALL_ROOT}/scripts/diag_cron_tz_effective.sh"
echo "  Verify: bash ${INSTALL_ROOT}/scripts/verify_schedule_alignment.sh"

if command -v systemctl >/dev/null 2>&1; then
  systemctl reload cron 2>/dev/null || systemctl reload crond 2>/dev/null || service cron reload 2>/dev/null || true
fi

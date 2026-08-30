#!/usr/bin/env bash
# Secret-safe runtime audit. Prints presence/status only, never credential values.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROLE="${1:-}"
RUN_USER="${AUTONOMY_RUN_USER:-ubuntu}"

if [[ "${ROLE}" != "stock" && "${ROLE}" != "bitget" ]]; then
  echo "usage: $0 stock|bitget" >&2
  exit 2
fi

USER_HOME="$(getent passwd "${RUN_USER}" 2>/dev/null | cut -d: -f6)"
AI_PATH="${USER_HOME}/.local/bin:${USER_HOME}/.cursor/bin:/usr/local/bin:/usr/bin:/bin"
failures=0

for env_file in "${ROOT}/.env" "${ROOT}/bitget/.env"; do
  if [[ -f "${env_file}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${env_file}"
    set +a
  fi
done

if [[ -z "${REPORT_BOT_TOKEN:-}" && -n "${BITGET_BOT_TOKEN:-}" ]]; then
  REPORT_BOT_TOKEN="${BITGET_BOT_TOKEN}"
fi
if [[ -z "${REPORT_BOT_CHAT_ID:-}" && -n "${BITGET_BOT_CHAT_ID:-}" ]]; then
  REPORT_BOT_CHAT_ID="${BITGET_BOT_CHAT_ID}"
fi

telegram_configured() {
  [[ -n "${REPORT_BOT_TOKEN:-${TELEGRAM_TOKEN_MAIN:-${TELEGRAM_BOT_TOKEN:-}}}" ]] &&
    [[ -n "${REPORT_BOT_CHAT_ID:-${TELEGRAM_CHAT_ID:-}}" ]]
}

check() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "[OK] ${label}"
  else
    echo "[FAIL] ${label}"
    failures=$((failures + 1))
  fi
}

check "merged weekday runner" test -f "${ROOT}/dev_autonomy/weekday_runner.py"
check "service unit installed" test -f /etc/systemd/system/quant-dev-autonomy@.service
check "timer enabled (${ROLE})" systemctl is-enabled "quant-dev-autonomy@${ROLE}.timer"
check "timer active (${ROLE})" systemctl is-active "quant-dev-autonomy@${ROLE}.timer"
check "Claude CLI installed" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${AI_PATH}" claude --version
check "Claude authenticated" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${AI_PATH}" claude auth status
check "Cursor CLI installed" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${AI_PATH}" agent --version
check "Cursor authenticated" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${AI_PATH}" agent status
check "Telegram credentials configured" telegram_configured

if [[ "${ROLE}" == "stock" ]]; then
  check "North Star ledger" test -r "${DB_STORAGE_PATH:-/var/lib/quant-factory/data}/dual_north_star_ledger.json"
else
  check "Bitget ops DB" test -r "${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}/bitget_ops_events.sqlite"
fi

if [[ "${failures}" -eq 0 ]]; then
  echo "[READY] ${ROLE} autonomy runtime"
else
  echo "[NOT READY] ${failures} check(s) failed"
fi
exit "${failures}"

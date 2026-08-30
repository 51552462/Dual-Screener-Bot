#!/usr/bin/env bash
# Role-isolated entrypoint for quant-dev-autonomy@{stock,bitget}.service.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROLE="${1:-}"
EXTRA_MODE="${2:-}"
PYTHON_BIN="${AUTONOMY_PYTHON:-${ROOT}/venv/bin/python}"
RUNTIME_ROOT="${AUTONOMY_RUNTIME_ROOT:-/var/lib/quant-dev-autonomy}"

if [[ "${ROLE}" != "stock" && "${ROLE}" != "bitget" ]]; then
  echo "role must be stock or bitget" >&2
  exit 2
fi
if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "python executable missing: ${PYTHON_BIN}" >&2
  exit 2
fi

export TZ="${TZ:-Asia/Seoul}"
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

for env_file in "${ROOT}/.env" "${ROOT}/bitget/.env"; do
  if [[ -f "${env_file}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${env_file}"
    set +a
  fi
done

# Bitget-only servers commonly keep Telegram names in bitget/.env. Map them
# inside this process without copying or printing any secret.
if [[ -z "${REPORT_BOT_TOKEN:-}" && -n "${BITGET_BOT_TOKEN:-}" ]]; then
  export REPORT_BOT_TOKEN="${BITGET_BOT_TOKEN}"
fi
if [[ -z "${REPORT_BOT_CHAT_ID:-}" && -n "${BITGET_BOT_CHAT_ID:-}" ]]; then
  export REPORT_BOT_CHAT_ID="${BITGET_BOT_CHAT_ID}"
fi

ROLE_RUNTIME="${RUNTIME_ROOT}/${ROLE}"
ARGS=(
  --queue-db "${ROLE_RUNTIME}/control_plane.sqlite"
  --outbox-dir "${ROLE_RUNTIME}/outbox"
  --json
)

ENVELOPE_PATH="${AUTONOMY_ENVELOPE_PATH:-/etc/quant-dev-autonomy/envelope.json}"
if [[ -f "${ENVELOPE_PATH}" ]]; then
  ARGS+=(--envelope "${ENVELOPE_PATH}")
fi

if [[ "${ROLE}" == "stock" ]]; then
  STOCK_DATA="${DB_STORAGE_PATH:-/var/lib/quant-factory/data}"
  ARGS+=(--north-star-ledger "${STOCK_DATA}/dual_north_star_ledger.json")
else
  BITGET_DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
  ARGS+=(--bitget-ops-db "${BITGET_DATA}/bitget_ops_events.sqlite" --no-ssot)
fi

if [[ "${EXTRA_MODE}" == "--dry-run" ]]; then
  ARGS+=(--dry-run)
else
  ARGS+=(--run-claude-review --notify-telegram)
fi

exec "${PYTHON_BIN}" -m dev_autonomy.weekday_runner "${ARGS[@]}"

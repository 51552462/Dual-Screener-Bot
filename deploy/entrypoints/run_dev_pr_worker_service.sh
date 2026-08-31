#!/usr/bin/env bash
# Role-isolated dispatcher for vetted Cursor draft-PR jobs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROLE="${1:-}"
MODE="${2:-}"
PYTHON_BIN="${AUTONOMY_PYTHON:-${ROOT}/venv/bin/python}"
AUTONOMY_ROOT="${AUTONOMY_RUNTIME_ROOT:-/var/lib/quant-dev-autonomy}"
PR_ROOT="${PR_WORKER_RUNTIME_ROOT:-/var/lib/quant-dev-pr-worker}"
ENVELOPE_PATH="${AUTONOMY_ENVELOPE_PATH:-/etc/quant-dev-autonomy/envelope.json}"

if [[ "${ROLE}" != "stock" && "${ROLE}" != "bitget" ]]; then
  echo "role must be stock or bitget" >&2
  exit 2
fi
if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "python executable missing: ${PYTHON_BIN}" >&2
  exit 2
fi
if [[ ! -r "${ENVELOPE_PATH}" ]]; then
  echo "readable autonomy envelope missing" >&2
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

if [[ -z "${REPORT_BOT_TOKEN:-}" && -n "${BITGET_BOT_TOKEN:-}" ]]; then
  export REPORT_BOT_TOKEN="${BITGET_BOT_TOKEN}"
fi
if [[ -z "${REPORT_BOT_CHAT_ID:-}" && -n "${BITGET_BOT_CHAT_ID:-}" ]]; then
  export REPORT_BOT_CHAT_ID="${BITGET_BOT_CHAT_ID}"
fi

ARGS=(
  --outbox-dir "${AUTONOMY_ROOT}/${ROLE}/outbox"
  --envelope "${ENVELOPE_PATH}"
  --repo-root "${ROOT}"
  --runtime-root "${PR_ROOT}/${ROLE}"
  --role "${ROLE}"
  --max-jobs 1
  --json
)

if [[ "${MODE}" != "--dry-run" ]]; then
  ARGS+=(--publish-draft-pr --notify-telegram)
fi

exec "${PYTHON_BIN}" -m dev_autonomy.pr_worker "${ARGS[@]}"

#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
cd "$ROOT"

export TZ="${TZ:-Asia/Seoul}"
export PYTHONUNBUFFERED=1
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi

if [[ -f "${BITGET_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${BITGET_ROOT}/.env"
  set +a
fi

if [[ -f "${ROOT}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/venv/bin/activate"
elif [[ -f "${BITGET_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${BITGET_ROOT}/.venv/bin/activate"
fi

# 단일 직렬 워커: cron 이 --enqueue 로 적재한 작업을 권력 이양 우선순위대로 실행.
exec python -m bitget.pipelines.queue_worker \
  --poll-sec "${BITGET_QUEUE_POLL_SEC:-5}" \
  --retry-backoff-sec "${BITGET_QUEUE_RETRY_BACKOFF_SEC:-300}"

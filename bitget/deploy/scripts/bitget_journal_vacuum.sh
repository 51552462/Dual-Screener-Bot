#!/usr/bin/env bash
# L-1 — journal vacuum for dante-bitget-* stack (P0-1).
# Reads BITGET_JOURNAL_MAX_USE / BITGET_JOURNAL_MAX_RETENTION from bitget/.env (defaults below).
# Does not restart units or touch trading code paths.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"

_load_env() {
  local f
  for f in "${ROOT}/.env" "${BITGET_ROOT}/.env"; do
    if [[ -f "$f" ]]; then
      set -a
      # shellcheck disable=SC1090
      source "$f"
      set +a
    fi
  done
}

_load_env

MAX_USE="${BITGET_JOURNAL_MAX_USE:-400M}"
MAX_TIME="${BITGET_JOURNAL_MAX_RETENTION:-30d}"

echo "[bitget_journal_vacuum] vacuum-size=${MAX_USE} vacuum-time=${MAX_TIME}"

if ! command -v journalctl >/dev/null 2>&1; then
  echo "[bitget_journal_vacuum] journalctl not found — skip (non-Linux dev host)"
  exit 0
fi

# Global journal store (shared server: tune BITGET_JOURNAL_* conservatively).
journalctl --vacuum-size="${MAX_USE}" || true
journalctl --vacuum-time="${MAX_TIME}" || true

# Report dante-bitget unit log footprint (observability — parsing pipelines use journalctl -u).
units=(
  dante-bitget-factory
  dante-bitget-ws
  dante-bitget-async
  dante-bitget-queue-worker
  dante-bitget-dashboard
  dante-bitget-heatmap
  dante-bitget-watchdog
  dante-bitget-snapshot
)
for u in "${units[@]}"; do
  if systemctl list-units --all --no-legend "${u}.service" 2>/dev/null | grep -q .; then
  journalctl -u "${u}.service" --disk-usage 2>/dev/null || true
  fi
done

echo "[bitget_journal_vacuum] done"

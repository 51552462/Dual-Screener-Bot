#!/usr/bin/env bash
# L-2 — Bitget SQLite integrity backup (BITGET_DB_STORAGE_PATH only, P0-5).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"

_load_env() {
  local f
  for f in "${INSTALL_ROOT}/.env" "${INSTALL_ROOT}/bitget/.env"; do
    if [[ -f "$f" ]]; then
      set -a
      # shellcheck disable=SC1090
      source "$f"
      set +a
    fi
  done
}

_load_env

export PYTHONPATH="${INSTALL_ROOT}:${PYTHONPATH:-}"
cd "$INSTALL_ROOT"

if [[ "${BITGET_BACKUP_ENABLED:-true}" == "0" || "${BITGET_BACKUP_ENABLED:-true}" == "false" ]]; then
  echo "[backup_bitget_db] BITGET_BACKUP_ENABLED=false — skip"
  exit 0
fi

python -m bitget.infra.integrity_backup_l2 --job backup "$@"

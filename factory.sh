#!/usr/bin/env bash
# Dual-Screener Factory — Ubuntu one-shot wrapper (venv · .env · TZ · logs)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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

if [[ -f "${ROOT}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/venv/bin/activate"
elif [[ -f "${ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.venv/bin/activate"
fi

LOG_DIR="${FACTORY_LOG_DIR:-${ROOT}/logs}"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"

usage() {
  cat <<'EOF'
Usage: ./factory.sh <flag>

  --scan-kr       supernova KR funnel + optional kr bowl scan
  --scan-us       supernova US funnel + optional usa bowl scan
  --daily-kr      guard → sentiment → track → deep dive → comprehensive → overseer (KR)
  --daily-us      guard → sentiment → track → deep dive → comprehensive → overseer (US)
  --daily         full daily chain (KR then US, single overseer)
  --weekly        weekly Flow master report + baseline persist

Environment:
  FACTORY_LOG_DIR   log directory (default: ./logs)
  TZ                default Asia/Seoul
EOF
}

MODE=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scan-kr)   MODE="scan_kr" ;;
    --scan-us)   MODE="scan_us" ;;
    --daily-kr)  MODE="daily_audit_kr" ;;
    --daily-us)  MODE="daily_audit_us" ;;
    --daily)     MODE="daily_audit" ;;
    --weekly)    MODE="weekly_master" ;;
    --dry-run)   EXTRA_ARGS+=("--dry-run") ;;
    --skip-telegram) EXTRA_ARGS+=("--skip-telegram") ;;
    -h|--help)   usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
  shift
done

if [[ -z "$MODE" ]]; then
  echo "Error: mode flag required." >&2
  usage
  exit 2
fi

LOG_FILE="${LOG_DIR}/factory_${MODE}_${STAMP}.log"
echo "[factory.sh] mode=${MODE} log=${LOG_FILE} TZ=${TZ}"

exec python "${ROOT}/system_auto_pilot.py" --mode "$MODE" "${EXTRA_ARGS[@]}" \
  >>"$LOG_FILE" 2>&1

#!/usr/bin/env bash
# Dual-Screener Factory — Ubuntu one-shot wrapper (venv · .env · TZ · logs)
set -eu -o pipefail

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

  Staggered intraday scans (30 min slots, one scanner per run — cron SSOT):
    KR KST 10:00–14:30  supernova → nulrim → dante → ema5 → master → bowl → (2nd pass ×4)
    US ET  10:00–14:30  supernova → nulrim → dante → ema5 → bowl → (2nd pass ×4)
    --scan-kr-supernova | --scan-kr-nulrim | --scan-kr-dante | --scan-kr-ema5
    --scan-kr-master | --scan-kr-bowl
    --scan-kr-supernova-r2 | --scan-kr-nulrim-r2 | --scan-kr-dante-r2 | --scan-kr-ema5-r2
    --scan-us-supernova | --scan-us-nulrim | ... (same pattern, no master)

  Legacy full chain (manual only — do not use in cron):
    --scan-kr       all KR scanners in one job
    --scan-us       all US scanners in one job

  Daily / weekly:
    --daily-kr      guard → track → deep dive → report (KR)
    --daily-us      guard → track → deep dive → report (US)
    --daily         full daily chain (KR then US)
    --weekly        weekly Flow master report

  --force-scan-outside-session
                  bypass market_session_gate (manual recovery)
  --lock-timeout SEC
                  factory job lock wait (default in system_auto_pilot: 120s)

Environment:
  FACTORY_LOG_DIR   log directory (default: ./logs)
  FACTORY_FORCE_SCAN_OUTSIDE_SESSION=1  same as flag above
  TZ                default Asia/Seoul (US staggered cron uses America/New_York in cron.d)
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
    --scan-kr-*|--scan-us-*)
      MODE="${1#--}"
      MODE="${MODE//-/_}"
      ;;
    --dry-run)   EXTRA_ARGS+=("--dry-run") ;;
    --skip-telegram) EXTRA_ARGS+=("--skip-telegram") ;;
    --lock-timeout)
      shift
      EXTRA_ARGS+=("--lock-timeout" "${1:?--lock-timeout requires seconds}")
      ;;
    --force-scan-outside-session) export FACTORY_FORCE_SCAN_OUTSIDE_SESSION=1 ;;
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
echo "[factory.sh] wall_clock=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S %Z %a')"

# set -e: non-zero Python exit → shell abort (cron must not reach a manual overseer rerun).
python "${ROOT}/system_auto_pilot.py" --mode "$MODE" "${EXTRA_ARGS[@]}" \
  >>"$LOG_FILE" 2>&1
_exit=$?
if [[ $_exit -ne 0 ]]; then
  echo "[factory.sh] PIPELINE ABORT exit=${_exit} — critical step failed; ai_overseer skipped. log=${LOG_FILE}" >&2
  exit "${_exit}"
fi

#!/usr/bin/env bash
# Bitget Factory — Ubuntu one-shot wrapper (venv · .env · TZ · logs)
# All Bitget ops live under bitget/ — do not mix with root factory.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
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
elif [[ -f "${ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.venv/bin/activate"
elif [[ -f "${BITGET_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${BITGET_ROOT}/.venv/bin/activate"
fi

LOG_DIR="${BITGET_LOG_DIR:-${BITGET_ROOT}/logs}"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"

usage() {
  cat <<'EOF'
Usage: bitget/deploy/bitget.sh <flag>

  --health            infra self-check
  --watchdog          heartbeat stale detector (cron/timer)
  --daemon            24/7 sentinel (python -m bitget.factory_launcher)
  --scan-all          data refresh + full MTF scan + track
  --scan-spot         spot scan + track
  --scan-futures      futures scan + track
  --track-positions   virtual position tracking (spot + futures)
  --daily-audit       sentiment + track + deep dive + report + reconcile
  --weekly-evolution  autonomous tuning / brain surgery
  --reconcile         OMS reconciliation
  --data-refresh      full MTF OHLCV update
  --gap-heal          WS stale -> REST backfill
  --snapshot          CQRS market DB backup (read replica)
  --record-baseline   save signal + PnL validation baselines
  --validate          parity check vs baselines
  --load-test         DB scan capacity benchmark
  --cutover-check     pipeline SSOT readiness report
  --validate-all      parity + load test + cutover info
  --start-parallel    begin 48h parallel-run window
  --ws-supervisor     public WebSocket daemon (foreground)

  --dry-run           pass through to Python job runner
  --skip-telegram     suppress failure notifications

Environment:
  BITGET_DB_STORAGE_PATH   data root SSOT
  BITGET_LOG_DIR           default: bitget/logs
  BITGET_DASHBOARD_PORT    default 8511
  BITGET_HEATMAP_PORT      default 8512
EOF
}

MODE=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --health)           MODE="health" ;;
    --watchdog)         MODE="watchdog" ;;
    --daemon)           MODE="daemon" ;;
    --scan-all)         MODE="scan_all" ;;
    --scan-spot)        MODE="scan_spot" ;;
    --scan-futures)     MODE="scan_futures" ;;
    --track-positions)  MODE="track_positions" ;;
    --daily-audit)      MODE="daily_audit" ;;
    --weekly-evolution) MODE="weekly_evolution" ;;
    --reconcile)        MODE="reconcile" ;;
    --data-refresh)     MODE="data_refresh" ;;
    --gap-heal)         MODE="gap_heal" ;;
    --snapshot)         MODE="snapshot" ;;
    --record-baseline)  MODE="record_baseline" ;;
    --validate)         MODE="validate" ;;
    --load-test)        MODE="load_test" ;;
    --cutover-check)    MODE="cutover_check" ;;
    --validate-all)     MODE="validate_all" ;;
    --start-parallel)   MODE="start_parallel" ;;
    --ws-supervisor)    MODE="ws_supervisor" ;;
    --dry-run)          EXTRA_ARGS+=("--dry-run") ;;
    --skip-telegram)    EXTRA_ARGS+=("--skip-telegram") ;;
    -h|--help)          usage; exit 0 ;;
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

if [[ "$MODE" == "daemon" ]]; then
  LOG_FILE="${LOG_DIR}/bitget_daemon_${STAMP}.log"
  echo "[bitget.sh] mode=daemon log=${LOG_FILE} TZ=${TZ}"
  exec python -m bitget.factory_launcher >>"$LOG_FILE" 2>&1
fi

if [[ "$MODE" == "ws_supervisor" ]]; then
  LOG_FILE="${LOG_DIR}/bitget_ws_${STAMP}.log"
  echo "[bitget.sh] mode=ws_supervisor log=${LOG_FILE}"
  exec python -m bitget.data.ws_supervisor >>"$LOG_FILE" 2>&1
fi

LOG_FILE="${LOG_DIR}/bitget_${MODE}_${STAMP}.log"
echo "[bitget.sh] mode=${MODE} log=${LOG_FILE} TZ=${TZ}"

exec python -m bitget.pipelines.runner --mode "$MODE" "${EXTRA_ARGS[@]}" >>"$LOG_FILE" 2>&1

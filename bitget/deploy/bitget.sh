#!/usr/bin/env bash
# Bitget Factory — Ubuntu one-shot wrapper (venv · .env · TZ · logs)
# All Bitget ops live under bitget/ — do not mix with root factory.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
cd "$ROOT"

_BITGET_TZ_FROM_CALLER="${TZ:-}"

export TZ="${TZ:-Asia/Seoul}"
export PYTHONUNBUFFERED=1
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
export BITGET_LOCK_BREAK_ON_MAX_AGE="${BITGET_LOCK_BREAK_ON_MAX_AGE:-1}"
export BITGET_LOCK_MAX_AGE_SEC="${BITGET_LOCK_MAX_AGE_SEC:-7200}"

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

if [[ -n "${_BITGET_TZ_FROM_CALLER}" ]]; then
  export TZ="${_BITGET_TZ_FROM_CALLER}"
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
  --daemon            24/7 pipeline daemon (python -m bitget.pipelines.bitget_auto_pilot)
  --scan-all          LEGACY: data refresh + full MTF scan + track (manual only)
  --scan-spot         LEGACY: spot scan + track (manual only)
  --scan-futures      LEGACY: futures scan + track (manual only)

  Staggered intraday (24h spread, non-%5 minutes, one scanner per run — cron SSOT):
    SPOT/FUTURES interleaved across 24h UTC; never share a minute with KR/US stock
    cron (those run at :00..:50 / :45). Heavy scans yield to factory (server-safe).
    SPOT    supernova → nulrim → dante → ema5 → master → shadow → 2nd pass
    FUTURES supernova → nulrim → dante → ema5 → shadow → 2nd pass
    --scan-spot-supernova | --scan-spot-nulrim | --scan-spot-dante | --scan-spot-ema5
    --scan-spot-master | --scan-spot-shadow
    --scan-spot-supernova-r2 | ... (same pattern)
    --scan-futures-supernova | --scan-futures-nulrim | ... (no master)

  --lock-timeout SEC    job flock wait (default from bitget_scan_schedule SSOT)
  --force-scan          bypass BITGET_FACTORY_SCAN_DISABLED / maintenance gate
  --track-positions   virtual position tracking (spot + futures)
  --daily-audit       sentiment + track + deep dive + report + reconcile
  --weekly-evolution  autonomous tuning / brain surgery
  --reconcile         OMS reconciliation
  --data-refresh      full MTF OHLCV update
  --canary            export crypto canary state JSON (file bridge → stock regime)
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
  --enqueue           enqueue the job into task_queue.sqlite (queue worker runs it)

Environment:
  BITGET_DB_STORAGE_PATH   data root SSOT
  BITGET_LOG_DIR           default: bitget/logs
  BITGET_DASHBOARD_PORT    default 8511
  BITGET_HEATMAP_PORT      default 8512
  BITGET_DAEMON_SNIPER     daemon 24/7 supernova sniper (default 0; cron staggered is SSOT)
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
    --scan-spot-*|--scan-futures-*)
      MODE="${1#--}"
      MODE="${MODE//-/_}"
      export TZ="UTC"
      ;;
    --track-positions)  MODE="track_positions" ;;
    --daily-audit)      MODE="daily_audit" ;;
    --weekly-evolution) MODE="weekly_evolution" ;;
    --reconcile)        MODE="reconcile" ;;
    --data-refresh)     MODE="data_refresh" ;;
    --canary)           MODE="canary" ;;
    --gap-heal)         MODE="gap_heal" ;;
    --snapshot)         MODE="snapshot" ;;
    --record-baseline)  MODE="record_baseline" ;;
    --validate)         MODE="validate" ;;
    --load-test)        MODE="load_test" ;;
    --cutover-check)    MODE="cutover_check" ;;
    --validate-all)     MODE="validate_all" ;;
    --start-parallel)   MODE="start_parallel" ;;
    --ws-supervisor)    MODE="ws_supervisor" ;;
    --lock-timeout)
      shift
      EXTRA_ARGS+=("--lock-timeout" "${1:?--lock-timeout requires seconds}")
      ;;
    --force-scan) export BITGET_FORCE_SCAN=1 ;;
    --dry-run)          EXTRA_ARGS+=("--dry-run") ;;
    --skip-telegram)    EXTRA_ARGS+=("--skip-telegram") ;;
    --enqueue)          EXTRA_ARGS+=("--enqueue") ;;
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

# --- daily_audit 중복 실행 가드 (주식 factory.sh pgrep 패턴) ---
_bitget_live_daily_audit_lines() {
  local line pid state
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pid="${line%% *}"
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    # 동시에 두 번 bitget.sh --daily-audit 이 뜨는 경우 자기 자신 제외
    if [[ "$pid" -eq "$$" ]]; then
      continue
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      continue
    fi
    state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ' | cut -c1)"
    if [[ "$state" == "Z" ]]; then
      continue
    fi
    printf '%s\n' "$line"
  done < <(
    {
      pgrep -af 'bitget\.pipelines\.runner --mode daily_audit' 2>/dev/null || true
      pgrep -af '[/ ]bitget\.sh --daily-audit' 2>/dev/null || true
      pgrep -af '[/ ]bitget/deploy/bitget\.sh --daily-audit' 2>/dev/null || true
    } | awk '!seen[$0]++'
  )
}

case "$MODE" in
  daily_audit)
    other_daily="$(_bitget_live_daily_audit_lines)"
    if [[ -n "$other_daily" ]]; then
      _lock_path="${BITGET_DB_STORAGE_PATH:-}"
      if [[ -n "$_lock_path" ]]; then
        _lock_path="${_lock_path%/}/.bitget_runtime.lock"
      else
        _lock_path="${BITGET_ROOT}/data/.bitget_runtime.lock (default if BITGET_DB_STORAGE_PATH unset)"
      fi
      echo "[bitget.sh] SKIP: another daily_audit job is already running (DB lock / OHLCV / RAM contention)." >&2
      echo "$other_daily" >&2
      echo "[bitget.sh] Wait for it to finish before starting a second daily_audit." >&2
      echo "[bitget.sh] If stuck: kill -9 <pid>; rm -f ${_lock_path}; retry." >&2
      exit 0
    fi
    ;;
esac

if [[ "$MODE" == "daemon" ]]; then
  LOG_FILE="${LOG_DIR}/bitget_daemon_${STAMP}.log"
  echo "[bitget.sh] mode=daemon log=${LOG_FILE} TZ=${TZ}"
  exec python -m bitget.pipelines.bitget_auto_pilot --daemon >>"$LOG_FILE" 2>&1
fi

if [[ "$MODE" == "ws_supervisor" ]]; then
  LOG_FILE="${LOG_DIR}/bitget_ws_${STAMP}.log"
  echo "[bitget.sh] mode=ws_supervisor log=${LOG_FILE}"
  exec python -m bitget.data.ws_supervisor >>"$LOG_FILE" 2>&1
fi

# canary 선행 레이더: 경량 공개 API 만 사용 → 파이프라인/락 무접촉, 단독 실행(독립 */15 cron).
if [[ "$MODE" == "canary" ]]; then
  LOG_FILE="${LOG_DIR}/bitget_canary_${STAMP}.log"
  echo "[bitget.sh] mode=canary log=${LOG_FILE} TZ=${TZ}"
  exec python -m bitget.canary_exporter >>"$LOG_FILE" 2>&1
fi

LOG_FILE="${LOG_DIR}/bitget_${MODE}_${STAMP}.log"
WALL_UTC="$(TZ=UTC date '+%Y-%m-%d %H:%M:%S %Z')"
echo "[bitget.sh] mode=${MODE} log=${LOG_FILE} TZ=${TZ} wall_utc=${WALL_UTC}"

if [[ "$MODE" == "data_refresh" ]]; then
  DR_TIMEOUT="${BITGET_DATA_REFRESH_TIMEOUT_SEC:-3600}"
  echo "[bitget.sh] data_refresh hard timeout=${DR_TIMEOUT}s"
  exec timeout --signal=TERM --kill-after=120 "${DR_TIMEOUT}" \
    python -m bitget.pipelines.runner --mode "$MODE" "${EXTRA_ARGS[@]}" >>"$LOG_FILE" 2>&1
fi

exec python -m bitget.pipelines.runner --mode "$MODE" "${EXTRA_ARGS[@]}" >>"$LOG_FILE" 2>&1

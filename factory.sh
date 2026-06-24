#!/usr/bin/env bash
# Dual-Screener Factory — Ubuntu one-shot wrapper (venv · .env · TZ · logs)
set -eu -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

# cron 줄 TZ=America/New_York / TZ=Asia/Seoul — .env 가 덮어쓰지 못하게 고정
_FACTORY_TZ_FROM_CALLER="${TZ:-}"

export TZ="${TZ:-Asia/Seoul}"
export PYTHONUNBUFFERED=1
export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

# 유령 락 자동 해제 — daily_audit 장시간 점유 시 다음 크론이 영구 대기하지 않도록
export FACTORY_LOCK_BREAK_ON_MAX_AGE="${FACTORY_LOCK_BREAK_ON_MAX_AGE:-1}"
export FACTORY_LOCK_MAX_AGE_SEC="${FACTORY_LOCK_MAX_AGE_SEC:-7200}"

if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi

# .env 의 TZ=Asia/Seoul 이 US cron TZ 를 망가뜨리는 것 방지 (로그 시각·STAMP SSOT)
if [[ -n "${_FACTORY_TZ_FROM_CALLER}" ]]; then
  export TZ="${_FACTORY_TZ_FROM_CALLER}"
fi

if [[ -f "${ROOT}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/venv/bin/activate"
elif [[ -f "${ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.venv/bin/activate"
fi

usage() {
  cat <<'EOF'
Usage: ./factory.sh <flag>

  Staggered intraday scans (50 min slots, one scanner per run — cron SSOT):
    KR KST 10:00–17:30  supernova → nulrim → … → bowl → (2nd pass ×4)
    US ET  10:00–16:40  supernova → nulrim → … → bowl → (2nd pass ×4)
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

# 시장별 TZ SSOT — 수동 실행·cron 공통 (US=ET, KR=KST)
case "$MODE" in
  scan_us|scan_us_*)
    export TZ="America/New_York"
    ;;
  scan_kr|scan_kr_*)
    export TZ="Asia/Seoul"
    ;;
esac

LOG_DIR="${FACTORY_LOG_DIR:-${ROOT}/logs}"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
_factory_live_daily_audit_lines() {
  local line pid state
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pid="${line%% *}"
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    if ! kill -0 "$pid" 2>/dev/null; then
      continue
    fi
    state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ' | cut -c1)"
    if [[ "$state" == "Z" ]]; then
      continue
    fi
    printf '%s\n' "$line"
  done < <(pgrep -af 'system_auto_pilot\.py --mode daily_audit' 2>/dev/null || true)
}

case "$MODE" in
  daily_audit|daily_audit_kr|daily_audit_us)
    other_daily="$(_factory_live_daily_audit_lines)"
    if [[ -n "$other_daily" ]]; then
      echo "[factory.sh] SKIP: another daily_audit job is already running (DB lock / OHLCV contention)." >&2
      echo "$other_daily" >&2
      echo "[factory.sh] Wait for it to finish, or use --daily for a single combined run." >&2
      echo "[factory.sh] If stuck: kill -9 <pid>; rm -f ${ROOT}/.factory_runtime.lock; retry." >&2
      exit 0
    fi
    ;;
esac

LOG_FILE="${LOG_DIR}/factory_${MODE}_${STAMP}.log"
echo "[factory.sh] mode=${MODE} log=${LOG_FILE} TZ=${TZ}"
echo "[factory.sh] wall_clock=$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S %Z %a')"

# set -e: non-zero Python exit → shell abort. tee → 터미널 + 로그 동시 (3시간 무발성 방지).
python "${ROOT}/system_auto_pilot.py" --mode "$MODE" "${EXTRA_ARGS[@]}" \
  2>&1 | tee -a "$LOG_FILE"
_exit=$?
if [[ $_exit -ne 0 ]]; then
  echo "[factory.sh] PIPELINE ABORT exit=${_exit} — critical step failed; ai_overseer skipped. log=${LOG_FILE}" >&2
  exit "${_exit}"
fi

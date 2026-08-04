#!/usr/bin/env bash
# Bitget 서버 복구 + 아티팩트(CSV/DB) 상태 확인 (Ubuntu)
#   cd ~/dante_bots/Dual-Screener-Bot && bash bitget/deploy/bitget_server_recover.sh
#   bash bitget/deploy/bitget_server_recover.sh --repair   # data_refresh + CSV 재생성
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
cd "$ROOT"

REPAIR=0
for arg in "$@"; do
  case "$arg" in
    --repair) REPAIR=1 ;;
    -h|--help)
      echo "Usage: bash bitget/deploy/bitget_server_recover.sh [--repair]"
      echo "  --repair  data_refresh 완료 후 Supernova CSV(data_miner) 재생성"
      exit 0
      ;;
  esac
done

# bitget.sh 와 동일한 .env / venv 로딩
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
fi

export PYTHONPATH="${ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
export PYTHONUNBUFFERED=1

PY="python"
command -v python >/dev/null 2>&1 || PY="python3"

BG="${BITGET_ROOT}/deploy/bitget.sh"
LOG_DIR="${BITGET_LOG_DIR:-${BITGET_ROOT}/logs}"
if ! mkdir -p "$LOG_DIR" 2>/dev/null; then
  LOG_DIR="${BITGET_ROOT}/logs"
  mkdir -p "$LOG_DIR"
fi
DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
CSV_PATH=""

if DATA_DIR="$("$PY" -c 'from bitget.infra.data_paths import bitget_data_dir, flow_csv_path; print(bitget_data_dir()); print(flow_csv_path())' 2>/dev/null)"; then
  DATA="$(echo "$DATA_DIR" | sed -n '1p')"
  CSV_PATH="$(echo "$DATA_DIR" | sed -n '2p')"
fi

echo "=== Bitget recover ==="
echo "ROOT=$ROOT"
echo "DATA=$DATA"
echo "CSV_PATH=${CSV_PATH:-<unknown>}"
echo "LOG_DIR=$LOG_DIR"
echo "PYTHON=$PY ($(command -v "$PY" || echo missing))"
echo ""

# 1) syntax fix 배포 여부
if grep -q 'signal_engines as bitget_signal_engines' "${BITGET_ROOT}/master_scanner.py" 2>/dev/null; then
  echo "[FAIL] master_scanner.py 구버전 — git pull 필요"
else
  echo "[OK] master_scanner.py syntax"
fi

# 2) .env SSOT
echo "--- env ---"
if [[ -f "${BITGET_ROOT}/.env" ]]; then
  echo "[OK] bitget/.env exists"
elif [[ -f "${ROOT}/.env" ]]; then
  echo "[OK] ROOT/.env exists (bitget/.env 권장: cp bitget/deploy/bitget.env.example bitget/.env)"
else
  echo "[FAIL] .env 없음 — cp bitget/deploy/bitget.env.example bitget/.env 후 BITGET_DB_STORAGE_PATH 설정"
fi
if [[ -n "${BITGET_DB_STORAGE_PATH:-}" ]]; then
  echo "[OK] BITGET_DB_STORAGE_PATH=${BITGET_DB_STORAGE_PATH}"
else
  echo "[WARN] BITGET_DB_STORAGE_PATH 미설정 — legacy bitget/ 패키지 폴더 사용"
fi

# 3) 핵심 아티팩트
echo "--- artifacts ---"
for f in \
  "${DATA}/bitget_market_data.sqlite" \
  "${DATA}/bitget_system_config.sqlite" \
  "${CSV_PATH:-${DATA}/Supernova_Flow_Tracking_Master.csv}"; do
  if [[ -f "$f" ]]; then
    echo "[OK] $(basename "$f") ($(du -h "$f" 2>/dev/null | cut -f1))"
  else
    echo "[MISSING] $f"
  fi
done

# 4) stuck lock
echo "--- locks ---"
for lf in "${DATA}/.bitget_runtime.lock" "${DATA}/.bitget_data_refresh.lock"; do
  if [[ -f "$lf" ]]; then
    echo "lock: $lf"
    head -3 "$lf" 2>/dev/null || true
    pid="$(sed -n '3p' "$lf" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "  pid $pid alive — mode=$(sed -n '1p' "$lf")"
    else
      echo "  stale lock — rm -f $lf"
    fi
  else
    echo "no lock: $lf"
  fi
done

# 5) 최근 로그
echo "--- recent logs (${LOG_DIR}) ---"
ls -lt "$LOG_DIR"/bitget_*.log 2>/dev/null | head -8 || echo "(no logs yet)"

# 6) systemd
echo "--- systemd ---"
systemctl is-active dante-bitget-factory dante-bitget-ws dante-bitget-async 2>/dev/null || true

# 7) repair
if [[ "$REPAIR" -eq 1 ]]; then
  echo ""
  echo "=== REPAIR: data_refresh → recover-artifacts ==="
  export BITGET_YIELD_TO_FACTORY=0
  export BITGET_FORCE_SCAN=1
  bash "$BG" --data-refresh --force-scan || echo "(data_refresh degraded — 계속)"
  bash "$BG" --recover-artifacts || echo "(recover-artifacts 실패 — 로그 확인)"
  if [[ -f "${CSV_PATH:-${DATA}/Supernova_Flow_Tracking_Master.csv}" ]]; then
    echo "[OK] CSV 복구 완료"
  else
    echo "[FAIL] CSV 여전히 없음 — OHLCV 부족 가능. tail ${LOG_DIR}/bitget_recover_artifacts_*.log"
  fi
else
  echo ""
  echo "--- 수동 복구 (placeholder /path/to 사용 금지) ---"
  echo "  cd $ROOT"
  echo "  bash bitget/deploy/bitget_server_recover.sh --repair"
  echo "  # 또는:"
  echo "  bash bitget/deploy/master_sync_bitget.sh"
  echo "  tail -40 ${LOG_DIR}/bitget_data_refresh_*.log"
fi

#!/usr/bin/env bash
# reset_bitget_pipeline.sh — 유령 락·좀비 bitget 프로세스 정리 후 서비스 재기동
#
# 주식 scripts/reset_factory_pipeline.sh 의 Bitget 트윈.
#   · 주식 dante-* (KR/US) 스택은 절대 건드리지 않는다 (완전 격리).
#   · DB/JSON 데이터는 삭제하지 않는다 — .bitget_runtime.lock / .bitget_data_refresh.lock
#     락 파일과 schedule_lock_state 만 정리.
#
#   bash bitget/deploy/reset_bitget_pipeline.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"
cd "$INSTALL_ROOT"

# .env (루트 → bitget) 로드: BITGET_DB_STORAGE_PATH 등 락 경로 해석에 필요
[[ -f "${ROOT}/.env" ]] && { set -a; source "${ROOT}/.env"; set +a; }
[[ -f "${BITGET_ROOT}/.env" ]] && { set -a; source "${BITGET_ROOT}/.env"; set +a; }

# venv (락 경로를 파이썬 SSOT 로 해석)
if [[ -f "${ROOT}/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/venv/bin/activate"
elif [[ -f "${ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT}/.venv/bin/activate"
fi

# 락이 위치한 bitget 데이터 디렉터리 SSOT 해석 (env → json → legacy bitget/)
DATA="$(PYTHONPATH="$ROOT" python -c 'from bitget.infra.data_paths import bitget_data_dir; print(bitget_data_dir())' 2>/dev/null || true)"
DATA="${DATA:-${BITGET_DB_STORAGE_PATH:-$BITGET_ROOT}}"
RUNTIME_LOCK="${DATA}/.bitget_runtime.lock"
DR_LOCK="${DATA}/.bitget_data_refresh.lock"
SCHED_STATE="${DATA}/bitget_schedule_lock_state.json"

echo "=== reset_bitget_pipeline ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"
echo "DATA=$DATA"

echo "=== [1/6] bitget 관련 프로세스 현황 ==="
pgrep -af "bitget.pipelines.runner|bitget.pipelines.bitget_auto_pilot|${BITGET_ROOT}/deploy/bitget.sh" || true

echo "=== [2/6] systemd 장기 데몬 정지 (DB untouched) ==="
sudo systemctl stop dante-bitget-factory.service 2>/dev/null || true
sleep 2

echo "=== [3/6] 락 홀더 PID 종료 (SIGTERM → SIGKILL) ==="
for LOCK in "$RUNTIME_LOCK" "$DR_LOCK"; do
  [[ -f "$LOCK" ]] || { echo "no lock: $LOCK"; continue; }
  echo "--- $LOCK ---"
  cat "$LOCK" 2>/dev/null || true
  LOCK_AGE=$(( $(date +%s) - $(stat -c %Y "$LOCK" 2>/dev/null || echo 0) ))
  echo "lock age sec: $LOCK_AGE"
  LOCK_PID="$(sed -n '3p' "$LOCK" 2>/dev/null || true)"
  if [[ -n "${LOCK_PID:-}" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
    echo "SIGTERM holder pid=$LOCK_PID"
    kill -TERM "$LOCK_PID" 2>/dev/null || true
    sleep 3
    if kill -0 "$LOCK_PID" 2>/dev/null; then
      echo "SIGKILL (잔존): $LOCK_PID"
      kill -KILL "$LOCK_PID" 2>/dev/null || true
    fi
  fi
done

echo "=== [4/6] 좀비 cron 스캔/러너 정리 (이 설치 경로 한정) ==="
pkill -f "${BITGET_ROOT}/deploy/bitget.sh" 2>/dev/null || true
pkill -f "bitget.pipelines.runner --mode" 2>/dev/null || true
sleep 2

echo "=== [5/6] 락 파일 제거 + 스케줄 락 상태 리셋 ==="
rm -f "$RUNTIME_LOCK" "$DR_LOCK"
if [[ -f "$SCHED_STATE" ]]; then
  printf '{}\n' >"$SCHED_STATE"
  echo "schedule_lock_state reset: $SCHED_STATE"
fi
echo "locks cleared"

echo "=== [6/6] systemd 재기동 (ws → async → factory) ==="
sudo systemctl daemon-reload 2>/dev/null || true
for unit in dante-bitget-ws dante-bitget-async dante-bitget-factory; do
  if systemctl list-unit-files 2>/dev/null | grep -q "^${unit}.service"; then
    sudo systemctl restart "$unit" 2>/dev/null || true
    sudo systemctl is-active "$unit" 2>/dev/null || true
  fi
done

echo "=== 완료 — 락 재확인 ==="
ls -la "$RUNTIME_LOCK" "$DR_LOCK" 2>/dev/null || echo "lock clear"
echo "DONE. 데이터 100% 보존 · 주식 dante-* 스택 무변경."

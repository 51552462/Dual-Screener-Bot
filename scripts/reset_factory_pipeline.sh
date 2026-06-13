#!/usr/bin/env bash
# reset_factory_pipeline.sh — 유령 락·좀비 factory 프로세스 정리 후 서비스 재기동
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT"

if [[ -f .env ]]; then set -a; source .env; set +a; fi

LOCK="${INSTALL_ROOT}/.factory_runtime.lock"
FACTORY_LOCK_MAX_AGE_SEC="${FACTORY_LOCK_MAX_AGE_SEC:-7200}"

echo "=== [1/5] factory 관련 프로세스 확인 ==="
pgrep -af "system_auto_pilot.py --mode|factory.sh|factory_runtime" || true

echo "=== [2/5] 락 파일 메타데이터 ==="
if [[ -f "$LOCK" ]]; then
  cat "$LOCK"
  LOCK_AGE=$(( $(date +%s) - $(stat -c %Y "$LOCK") ))
  echo "lock age sec: $LOCK_AGE (max=$FACTORY_LOCK_MAX_AGE_SEC)"
else
  echo "no lock file"
fi

echo "=== [3/5] PID 기반 정리 ==="
if [[ -f "$LOCK" ]]; then
  LOCK_PID="$(sed -n '3p' "$LOCK" 2>/dev/null || true)"
  if [[ -n "${LOCK_PID:-}" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
    echo "SIGTERM holder pid=$LOCK_PID"
    kill -TERM "$LOCK_PID" 2>/dev/null || true
    sleep 3
    kill -0 "$LOCK_PID" 2>/dev/null && kill -KILL "$LOCK_PID" 2>/dev/null || true
  fi
  rm -f "$LOCK"
  echo "lock file removed"
fi

pkill -f "${INSTALL_ROOT}/factory.sh" 2>/dev/null || true
sleep 2

echo "=== [4/5] systemd graceful restart ==="
for unit in dante-factory dante-async; do
  if systemctl list-unit-files 2>/dev/null | grep -q "^${unit}.service"; then
    sudo systemctl restart "$unit" || true
    sudo systemctl is-active "$unit" || true
  fi
done

echo "=== [5/5] 완료 — lock 재확인 ==="
ls -la "$LOCK" 2>/dev/null || echo "lock clear"
echo "DONE. 이후 master_sync_kr_us.sh 실행."

#!/usr/bin/env bash
# Bitget 서버 복구 + 상태 확인 (Ubuntu)
#   bash bitget/deploy/bitget_server_recover.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
cd "$ROOT"

DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/bitget-factory/data}"
LOG_DIR="${BITGET_LOG_DIR:-/var/lib/bitget-factory/logs}"

echo "=== Bitget recover ==="
echo "ROOT=$ROOT"
echo "DATA=$DATA"
echo "LOG_DIR=$LOG_DIR"

# 1) syntax fix 배포 여부
if grep -q 'signal_engines as bitget_signal_engines' "${BITGET_ROOT}/master_scanner.py" 2>/dev/null; then
  echo "[FAIL] master_scanner.py 구버전 — git pull 필요"
else
  echo "[OK] master_scanner.py syntax"
fi

# 2) stuck data_refresh / lock
echo "--- locks ---"
for lf in "${DATA}/.bitget_runtime.lock" "${DATA}/.bitget_data_refresh.lock"; do
  if [[ -f "$lf" ]]; then
    echo "lock: $lf"
    head -3 "$lf" 2>/dev/null || true
    pid="$(sed -n '3p' "$lf" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "  pid $pid alive — mode=$(sed -n '1p' "$lf")"
    fi
  else
    echo "no lock: $lf"
  fi
done

echo "--- optional: kill stuck data_refresh (sudo) ---"
echo "  sudo pkill -f 'runner --mode data_refresh' || true"
echo "  sudo rm -f ${DATA}/.bitget_runtime.lock ${DATA}/.bitget_data_refresh.lock"

# 3) 최근 로그
mkdir -p "$LOG_DIR"
echo "--- recent logs (${LOG_DIR}) ---"
ls -lt "$LOG_DIR"/bitget_*.log 2>/dev/null | head -8 || echo "(no logs yet)"

# 4) 서비스
echo "--- systemd ---"
systemctl is-active dante-bitget-factory dante-bitget-ws 2>/dev/null || true

# 5) 수동 스캔 테스트
echo "--- manual scan test ---"
echo "  TZ=UTC ${BITGET_ROOT}/deploy/bitget.sh --scan-spot-nulrim --force-scan"
echo "  tail -40 ${LOG_DIR}/bitget_scan_spot_nulrim_*.log"

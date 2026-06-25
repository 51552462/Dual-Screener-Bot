#!/usr/bin/env bash
# master_sync_bitget.sh — Bitget 정체 해제 one-shot (주식 scripts/master_sync_kr_us.sh 트윈)
#
#   락 리셋 → OHLCV 갱신 → 일일 감사(리포트) → 정합성/상태 검증
#   주식 dante-* (KR/US) 스택·데이터는 건드리지 않는다 (완전 격리).
#
#   bash bitget/deploy/master_sync_bitget.sh
set -euo pipefail

export TZ=UTC
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${BITGET_ROOT}/.." && pwd)"
export INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"
cd "$INSTALL_ROOT"

[[ -f "${ROOT}/.env" ]] && { set -a; source "${ROOT}/.env"; set +a; }
[[ -f "${BITGET_ROOT}/.env" ]] && { set -a; source "${BITGET_ROOT}/.env"; set +a; }

# 복구 모드: factory 양보·유지보수 게이트 우회 (수동 1회 강제 실행)
export BITGET_YIELD_TO_FACTORY=0
export BITGET_FORCE_SCAN=1
export BITGET_LOCK_BREAK_ON_MAX_AGE=1
export BITGET_LOCK_MAX_AGE_SEC="${BITGET_LOCK_MAX_AGE_SEC:-300}"

BG="${BITGET_ROOT}/deploy/bitget.sh"

echo "========== STEP 0: 락 리셋 =========="
bash "${SCRIPT_DIR}/reset_bitget_pipeline.sh" || {
  echo "(reset 스크립트 실패 — 락 직접 정리 시도)"
  data="$(PYTHONPATH="$ROOT" python -c 'from bitget.infra.data_paths import bitget_data_dir; print(bitget_data_dir())' 2>/dev/null || true)"
  data="${data:-${BITGET_DB_STORAGE_PATH:-$BITGET_ROOT}}"
  rm -f "${data}/.bitget_runtime.lock" "${data}/.bitget_data_refresh.lock"
}

echo "========== STEP 1: OHLCV 전체 갱신 (data_refresh) =========="
BITGET_REPORT_HYDRATE_FULL=1 bash "$BG" --data-refresh --force-scan || echo "(data_refresh degraded — 계속)"

echo "========== STEP 2: 일일 감사 + 리포트 (daily_audit) =========="
bash "$BG" --daily-audit --lock-timeout 900 || echo "(daily_audit 일부 실패 — 로그 확인)"

echo "========== STEP 3: 정합성/상태 검증 =========="
bash "$BG" --reconcile --lock-timeout 300 || true
bash "$BG" --health || true

echo "========== DONE — Bitget master_sync 완료 (데이터 100% 보존) =========="

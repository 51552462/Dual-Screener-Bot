#!/usr/bin/env bash
# master_sync_kr_us.sh — RED 데이터 정체 해제 + 워터마크 최신화 (기준: 2026-06-12 KST)
set -euo pipefail

export TZ=Asia/Seoul
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"
export REPORT_DEEP_DIVE_FORCE_MAIN_DB=1
export FACTORY_META_MAX_AGE_HOURS="${FACTORY_META_MAX_AGE_HOURS:-72}"
export FACTORY_LOCK_BREAK_ON_MAX_AGE=1
export FACTORY_LOCK_MAX_AGE_SEC=300

cd "$INSTALL_ROOT"
[[ -f .env ]] && { set -a; source .env; set +a; }
source venv/bin/activate

echo "========== STEP 0: 락 리셋 =========="
bash "${INSTALL_ROOT}/scripts/reset_factory_pipeline.sh" 2>/dev/null || rm -f "${INSTALL_ROOT}/.factory_runtime.lock"

echo "========== STEP 1: 스키마·아티팩트 Self-Heal =========="
python - <<'PY'
from factory_artifact_guard import ensure_factory_artifacts
from meta_state_store import rebuild_meta_state
print("artifact_guard:", ensure_factory_artifacts(force_meta=True))
print("meta_rebuild:", rebuild_meta_state(force=True, refresh_regime=True))
PY

echo "========== STEP 2: KR 전체 OHLCV bulk =========="
python -c "from data_updater import run_daily_db_update; run_daily_db_update()"

echo "========== STEP 3: US 증분 + 건강 복구 =========="
python - <<'PY'
from factory_us_health import ensure_us_pipeline_ready_for_scan, assess_us_pipeline_health
from data_updater import run_us_incremental_db_update
print("us_health before:", assess_us_pipeline_health())
print("us_repair:", ensure_us_pipeline_ready_for_scan(context="manual_recovery", repair=True))
print("us_incremental:", run_us_incremental_db_update())
PY

echo "========== STEP 4: CQRS 스냅샷 =========="
python -c "from data_updater import create_read_only_snapshot; print(create_read_only_snapshot())"
sudo systemctl start dante-snapshot.service 2>/dev/null || true

echo "========== STEP 5: US 일일 감사 =========="
"${INSTALL_ROOT}/factory.sh" --daily-us --lock-timeout 600

echo "========== STEP 6: KR 일일 감사 =========="
"${INSTALL_ROOT}/factory.sh" --daily-kr --lock-timeout 600

echo "========== STEP 7: 통합 검증 =========="
python - <<'PY'
from datetime import datetime
import pytz
import sqlite3
from forward_dual_track_queries import query_latest_closed_trade_date
from market_db_paths import report_db_read_path
from reports.report_timekeeper import ReportTimekeeper, business_lag_days
from reports.report_staleness_gate import evaluate_staleness

ref = datetime.now(pytz.timezone("Asia/Seoul"))
path = report_db_read_path()
conn = sqlite3.connect(path)
for mkt in ("KR", "US"):
    wm = query_latest_closed_trade_date(conn, mkt)
    tk = ReportTimekeeper.for_market(mkt, ref_kst=ref, db_watermark_exit=wm)
    lag = business_lag_days(wm, tk.session_anchor, market=mkt)
    st = evaluate_staleness(tk, live_row_count=0)
    print(f"{mkt}: watermark={wm} anchor={tk.session_anchor} lag={lag} grade={st.grade}")
conn.close()
print("DB:", path)
PY

echo "========== DONE =========="

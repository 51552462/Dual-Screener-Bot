#!/usr/bin/env bash
# force_sample_recovery.sh — 리포트 표본 0건 고착 해제 (exit_date 백필 + track + daily audit)
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

DB="$(python -c "from market_db_paths import MARKET_DATA_DB_PATH; print(MARKET_DATA_DB_PATH)")"
echo "DB=$DB"

echo "========== STEP 1: 백업 =========="
BK="${DB}.bak_sample_recovery_$(date +%Y%m%d_%H%M%S)"
cp -a "$DB" "$BK"
echo "backup=$BK"

echo "========== STEP 2: exit_date 백필 (trade_date → entry_date) =========="
sqlite3 "$DB" "
UPDATE forward_trades
SET exit_date = substr(trim(trade_date),1,10)
WHERE status LIKE 'CLOSED%'
  AND (exit_date IS NULL OR trim(exit_date) = '')
  AND trade_date IS NOT NULL AND trim(trade_date) != '';

UPDATE forward_trades
SET exit_date = substr(trim(entry_date),1,10)
WHERE status LIKE 'CLOSED%'
  AND (exit_date IS NULL OR trim(exit_date) = '')
  AND entry_date IS NOT NULL AND trim(entry_date) != '';
"
sqlite3 "$DB" "
SELECT market,
       MAX(substr(COALESCE(NULLIF(trim(exit_date),''), entry_date),1,10)) AS max_exit,
       SUM(CASE WHEN status LIKE 'CLOSED%' THEN 1 ELSE 0 END) AS closed_n,
       SUM(CASE WHEN status IN ('OPEN','ACTIVE') THEN 1 ELSE 0 END) AS open_n
FROM forward_trades GROUP BY market;
"

echo "========== STEP 3: MetaGovernor + 스키마 Self-Heal =========="
python - <<'PY'
from factory_artifact_guard import ensure_factory_artifacts
from meta_state_store import rebuild_meta_state
print("artifact:", ensure_factory_artifacts(force_meta=True))
print("meta:", rebuild_meta_state(force=True, refresh_regime=True))
PY

echo "========== STEP 4: OHLCV bulk (휴장 스킵 방지) =========="
python -c "from data_updater import run_daily_db_update; run_daily_db_update()"
python -c "from data_updater import run_us_incremental_db_update; run_us_incremental_db_update()"

echo "========== STEP 5: track_daily_positions (장부 exit_date 전진) =========="
python - <<'PY'
from auto_forward_tester import track_daily_positions
for m in ("US", "KR"):
    print(f"--- track {m} ---")
    track_daily_positions(m)
PY

echo "========== STEP 6: daily audit (리포트 재생성) =========="
"${INSTALL_ROOT}/factory.sh" --daily-us --lock-timeout 600
"${INSTALL_ROOT}/factory.sh" --daily-kr --lock-timeout 600

echo "========== STEP 7: 표본 검증 =========="
python - <<'PY'
from datetime import datetime
import pytz
from reports.daily_report_context import DailyReportContext
from forward.shared import _open_market_db_ro, _daily_report_trades_for_market, _reporter_valid_holding_mask
from reports.report_collectors import _df_long_only

ref = datetime.now(pytz.timezone("Asia/Seoul"))
ctx = DailyReportContext.build(ref_kst=ref)
conn = _open_market_db_ro()
ok = True
for mkt in ("KR", "US"):
    sl = ctx.load_market_slice(
        conn, mkt,
        df_long_only_fn=_df_long_only,
        normalize_market_fn=_daily_report_trades_for_market,
        valid_open_mask_fn=_reporter_valid_holding_mask,
    )
    nr, nc, no = len(sl.df_real), sl.n_closed_window, sl.n_open_valid
    print(f"{mkt}: real={nr} closed={nc} open={no} anchor={ctx.anchor_for(mkt)} wm={ctx.timekeeper_for(mkt).db_watermark_exit}")
    if nr == 0 and nc == 0 and no == 0:
        ok = False
conn.close()
if not ok:
    raise SystemExit("표본 여전히 전부 0 — §4.1 백필 3·FORWARD_DEEP_DIVE_EXIT_WINDOW_DAYS=180 검토")
print("표본 복구 OK")
PY

echo "========== DONE =========="

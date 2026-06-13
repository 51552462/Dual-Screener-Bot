#!/usr/bin/env bash
# force_data_sync.sh — OHLCV·워터마크만 강제 동기화 (검색기/리포트 제외)
set -euo pipefail
export TZ=Asia/Seoul
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"
cd "$INSTALL_ROOT"
[[ -f .env ]] && { set -a; source .env; set +a; }
source venv/bin/activate

export FACTORY_META_MAX_AGE_HOURS="${FACTORY_META_MAX_AGE_HOURS:-72}"
export FACTORY_LOCK_BREAK_ON_MAX_AGE=1
export REPORT_DEEP_DIVE_FORCE_MAIN_DB=1
rm -f .factory_runtime.lock

DB=$(python -c "from market_db_paths import MARKET_DATA_DB_PATH; print(MARKET_DATA_DB_PATH)")
cp -a "$DB" "${DB}.bak_force_sync_$(date +%Y%m%d_%H%M%S)"

sqlite3 "$DB" "
UPDATE forward_trades SET exit_date=substr(trim(trade_date),1,10)
WHERE status LIKE 'CLOSED%' AND (exit_date IS NULL OR trim(exit_date)='') AND trade_date IS NOT NULL AND trim(trade_date)!='';
UPDATE forward_trades SET exit_date=substr(trim(entry_date),1,10)
WHERE status LIKE 'CLOSED%' AND (exit_date IS NULL OR trim(exit_date)='') AND entry_date IS NOT NULL AND trim(entry_date)!='';
"

python -c "from meta_state_store import rebuild_meta_state; rebuild_meta_state(force=True,refresh_regime=True)"
python -c "from data_updater import run_daily_db_update; run_daily_db_update()"
python -c "from data_updater import run_us_incremental_db_update; run_us_incremental_db_update()"
python -c "from data_updater import create_read_only_snapshot; create_read_only_snapshot()"
python -c "from auto_forward_tester import track_daily_positions; track_daily_positions('US'); track_daily_positions('KR')"

echo "FORCE SYNC DONE — DB=$DB"

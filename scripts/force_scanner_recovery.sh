#!/usr/bin/env bash
# force_scanner_recovery.sh — 데이터 강제 동기화 + 검색기 강제 구동 + 일일 리포트 텔레그램 발송
# 기준일: 2026-06-12 KST | INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
set -euo pipefail

export TZ=Asia/Seoul
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export INSTALL_ROOT="${INSTALL_ROOT:-$ROOT}"
export PYTHONPATH="${INSTALL_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"
export REPORT_DEEP_DIVE_FORCE_MAIN_DB=1
export FACTORY_META_MAX_AGE_HOURS="${FACTORY_META_MAX_AGE_HOURS:-72}"
export FACTORY_LOCK_BREAK_ON_MAX_AGE=1
export FACTORY_LOCK_MAX_AGE_SEC=300
# 장외 수동 복구 시 세션 게이트 우회 (1=허용)
export FACTORY_FORCE_SCAN_OUTSIDE_SESSION="${FACTORY_FORCE_SCAN_OUTSIDE_SESSION:-1}"

cd "$INSTALL_ROOT"
[[ -f .env ]] && { set -a; source .env; set +a; }
source venv/bin/activate

MARKETS="${1:-KR}"   # KR | US | BOTH
SEND_DAILY="${SEND_DAILY:-1}"  # 1=일일 9분할 리포트도 발송

echo "========== [0] 락·좀비 프로세스 정리 =========="
bash "${INSTALL_ROOT}/scripts/reset_factory_pipeline.sh" 2>/dev/null || rm -f "${INSTALL_ROOT}/.factory_runtime.lock"

DB="$(python -c "from market_db_paths import MARKET_DATA_DB_PATH; print(MARKET_DATA_DB_PATH)")"
echo "DB=$DB"
cp -a "$DB" "${DB}.bak_scanner_recovery_$(date +%Y%m%d_%H%M%S)"

echo "========== [1] exit_date 백필 (워터마크·윈도우 정렬) =========="
sqlite3 "$DB" "
UPDATE forward_trades SET exit_date=substr(trim(trade_date),1,10)
WHERE status LIKE 'CLOSED%' AND (exit_date IS NULL OR trim(exit_date)='') AND trade_date IS NOT NULL AND trim(trade_date)!='';
UPDATE forward_trades SET exit_date=substr(trim(entry_date),1,10)
WHERE status LIKE 'CLOSED%' AND (exit_date IS NULL OR trim(exit_date)='') AND entry_date IS NOT NULL AND trim(entry_date)!='';
"

echo "========== [2] MetaGovernor + 스키마 Self-Heal =========="
python - <<'PY'
from factory_artifact_guard import ensure_factory_artifacts
from meta_state_store import rebuild_meta_state
print("artifact:", ensure_factory_artifacts(force_meta=True))
print("meta:", rebuild_meta_state(force=True, refresh_regime=True))
PY

echo "========== [3] OHLCV 강제 동기화 (KR bulk + US 증분) =========="
python -c "from data_updater import run_daily_db_update; print(run_daily_db_update())"
python - <<'PY'
from factory_us_health import ensure_us_pipeline_ready_for_scan, assess_us_pipeline_health
from data_updater import run_us_incremental_db_update, create_read_only_snapshot
print("us_health:", assess_us_pipeline_health())
print("us_repair:", ensure_us_pipeline_ready_for_scan(context="manual_recovery", repair=True))
print("us_incr:", run_us_incremental_db_update())
print("snapshot:", create_read_only_snapshot())
PY
sudo systemctl start dante-snapshot.service 2>/dev/null || true

echo "========== [4] track_daily_positions (워터마크 전진) =========="
python - <<'PY'
from auto_forward_tester import track_daily_positions
for m in ("US", "KR"):
    print(f"--- track {m} ---")
    track_daily_positions(m)
PY

_run_scan() {
  local MKT="$1"
  local MODE="scan_$(echo "$MKT" | tr '[:upper:]' '[:lower:]')"
  echo "========== [5] factory.sh --${MODE//_/-} (검색기 파이프라인) =========="
  FACTORY_FORCE_SCAN_OUTSIDE_SESSION="${FACTORY_FORCE_SCAN_OUTSIDE_SESSION}" \
    "${INSTALL_ROOT}/factory.sh" "--${MODE//_/-}" --force-scan-outside-session --lock-timeout 900
}

_run_daily() {
  local MKT="$1"
  local FLAG="daily-$(echo "$MKT" | tr '[:upper:]' '[:lower:]')"
  echo "========== [6] factory.sh --${FLAG} (9분할 리포트 + 텔레그램) =========="
  "${INSTALL_ROOT}/factory.sh" "--${FLAG}" --lock-timeout 900
}

case "$(echo "$MARKETS" | tr '[:lower:]' '[:upper:]')" in
  KR)
    _run_scan KR
    [[ "$SEND_DAILY" == "1" ]] && _run_daily KR
    ;;
  US)
    _run_scan US
    [[ "$SEND_DAILY" == "1" ]] && _run_daily US
    ;;
  BOTH|ALL|*)
    _run_scan US
    _run_scan KR
    if [[ "$SEND_DAILY" == "1" ]]; then
      _run_daily US
      _run_daily KR
    fi
    ;;
esac

echo "========== [7] 검증 =========="
python - <<'PY'
from datetime import datetime
import pytz, sqlite3
from forward_dual_track_queries import query_latest_closed_trade_date
from market_db_paths import MARKET_DATA_DB_PATH
from reports.daily_report_context import DailyReportContext
from reports.report_staleness_gate import evaluate_staleness

ref = datetime.now(pytz.timezone("Asia/Seoul"))
ctx = DailyReportContext.build(ref_kst=ref)
conn = sqlite3.connect(MARKET_DATA_DB_PATH)
for tbl in ("KR_KOSPI_IDX", "US_SPY"):
    try:
        r = conn.execute(f'SELECT MAX("Date") FROM "{tbl}"').fetchone()
        print(f"OHLCV {tbl} max_date={r[0]}")
    except Exception as e:
        print(f"OHLCV {tbl} ERR {e}")
for mkt in ("KR", "US"):
    wm = query_latest_closed_trade_date(conn, mkt)
    tk = ctx.timekeeper_for(mkt)
    st = evaluate_staleness(tk, live_row_count=0)
    print(f"{mkt}: wm={wm} anchor={tk.session_anchor} staleness={st.grade} lag={ctx.lag_for(mkt)}")
conn.close()
PY

echo "========== DONE — 텔레그램 SUPERNOVA 퍼널 + 일일 리포트 확인 =========="

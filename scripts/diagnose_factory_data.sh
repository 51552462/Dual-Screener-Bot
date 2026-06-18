#!/usr/bin/env bash
# 데이터·연결·스캔 로그 한눈에 — "데이터 삭제 vs 나사 풀림" 판별
set -eu -o pipefail

INSTALL_ROOT="${INSTALL_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$INSTALL_ROOT"

if [[ -f .env ]]; then set -a; source .env; set +a; fi
PY="${INSTALL_ROOT}/venv/bin/python"

echo "=== Factory data & link diagnosis ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"
echo ""

"$PY" <<'PY'
import os
import sqlite3
from datetime import datetime

import pytz

from factory_data_paths import factory_data_dir, market_data_db_path, flow_csv_path

root = factory_data_dir()
md = market_data_db_path()
kst = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M KST")

print(f"time: {kst}")
print(f"factory_data_dir: {root}")
print(f"market_data_db:   {md}")
print(f"DB_STORAGE_PATH:  {os.environ.get('DB_STORAGE_PATH') or '(default)'}")
print("")

def size_mb(p):
    if not os.path.isfile(p):
        return "MISSING"
    return f"{os.path.getsize(p) / (1024*1024):.2f} MB"

for label, p in [
    ("market_data", md),
    ("ops_events", os.path.join(root, "ops_events.sqlite")),
    ("message_queue", os.path.join(root, "message_queue.sqlite")),
    ("flow_csv", flow_csv_path()),
]:
    print(f"  {label}: {size_mb(p)}")

print("")
if not os.path.isfile(md):
    print("✗ market_data.sqlite 없음 — OHLCV·forward_trades 전부 끊김")
else:
    conn = sqlite3.connect(md, timeout=15)
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        for t in ("forward_trades", "stock_ohlcv_kr", "stock_ohlcv_us"):
            if t not in tables:
                print(f"  ✗ table missing: {t}")
                continue
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t}: {n:,} rows")
        if "forward_trades" in tables:
            row = conn.execute(
                "SELECT COUNT(*), MIN(entry_date), MAX(entry_date) FROM forward_trades"
            ).fetchone()
            closed = conn.execute(
                "SELECT COUNT(*) FROM forward_trades WHERE status LIKE 'CLOSED%'"
            ).fetchone()[0]
            print(f"  forward_trades CLOSED: {closed:,}")
            print(f"  forward_trades dates: min={row[1]} max={row[2]}")
            if row[0] == 0:
                print("  ⚠ forward_trades 비어 있음 → 딥다이브 '표본 0건'·데몬 워밍업 무한 대기 원인")
    finally:
        conn.close()

# heartbeat
ops = os.path.join(root, "ops_events.sqlite")
if os.path.isfile(ops):
    c = sqlite3.connect(f"file:{ops}?mode=ro", uri=True, timeout=10)
    try:
        hb = c.execute(
            "SELECT ts_utc, component FROM ops_events "
            "WHERE event='heartbeat.tick' AND component='system_auto_pilot' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        print("")
        if hb:
            print(f"  last system_auto_pilot heartbeat: {hb[0]}")
        else:
            print("  ✗ system_auto_pilot heartbeat 없음 → WATCHDOG 알림 원인")
    finally:
        c.close()
PY

echo ""
echo "=== Scan logs (last 5 each) ==="
LOG_DIR="${FACTORY_LOG_DIR:-${INSTALL_ROOT}/logs}"
echo "LOG_DIR=$LOG_DIR"
for p in factory_scan_kr factory_scan_us factory_daily_audit_kr factory_daily_audit_us; do
  echo "[$p]"
  ls -lt "${LOG_DIR}/${p}_"* 2>/dev/null | head -5 | awk '{print "  "$NF}' || echo "  (none)"
done

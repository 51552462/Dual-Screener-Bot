#!/usr/bin/env python3
"""US 스캔·일일감사·워터마크 정체 일괄 진단."""
from __future__ import annotations

import argparse
import glob
import os
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _tail_logs(log_dir: Path, pattern: str, n: int = 3) -> list[str]:
    files = sorted(glob.glob(str(log_dir / pattern)), key=os.path.getmtime, reverse=True)
    return [os.path.basename(f) for f in files[:n]]


def main() -> int:
    p = argparse.ArgumentParser(description="US scan pipeline diagnosis")
    p.parse_args()

    import pytz
    from forward_dual_track_queries import query_latest_closed_trade_date
    from forward.forward_book_integrity import diagnose_open_book_from_db
    from market_db_paths import MARKET_DATA_DB_PATH, report_db_read_path
    from market_session_gate import is_market_open
    from reports.report_staleness_gate import evaluate_staleness
    from reports.report_timekeeper import ReportTimekeeper
    from session_deduplication_guard import SessionDeduplicationGuard

    kst = pytz.timezone("Asia/Seoul")
    et = pytz.timezone("America/New_York")
    now_kst = datetime.now(kst)
    now_et = datetime.now(et)

    print("=== US Scan Pipeline Diagnosis ===")
    print(f"time KST={now_kst.strftime('%Y-%m-%d %H:%M')} ET={now_et.strftime('%Y-%m-%d %H:%M')}")

    lock = _ROOT / ".factory_runtime.lock"
    if lock.is_file():
        age = int(now_kst.timestamp() - lock.stat().st_mtime)
        body = lock.read_text(encoding="utf-8", errors="ignore").strip().splitlines()
        print(f"\n[LOCK] age={age}s")
        for ln in body[:4]:
            print(f"  {ln}")
        if age > 7200:
            print("  >> FIX: bash scripts/reset_factory_pipeline.sh")
    else:
        print("\n[LOCK] clear")

    ok, sess = is_market_open("US")
    print(f"\n[SESSION] US open={ok} — {sess}")

    guard = SessionDeduplicationGuard()
    dec = guard.evaluate("US")
    print(f"[DEDUP] abort={dec.abort_scan} reason={dec.reason}")
    print(f"        valid_open={dec.open_count_session} funnel={dec.funnel_slots_session}")

    db = report_db_read_path()
    print(f"\n[DB] {db}")
    import sqlite3

    conn = sqlite3.connect(db, timeout=30)
    try:
        wm = query_latest_closed_trade_date(conn, "US")
    finally:
        conn.close()

    tk = ReportTimekeeper.for_market("US", ref_kst=now_kst, db_watermark_exit=wm)
    st = evaluate_staleness(tk, live_row_count=0)
    print(f"[STALENESS] {st.grade} lag={st.lag_business_days}d")
    print(f"  watermark={wm} anchor={tk.session_anchor}")
    if st.grade == "RED":
        print("  >> FIX: bash scripts/master_sync_kr_us.sh")

    ob = diagnose_open_book_from_db("US", session_anchor=tk.session_anchor)
    print(f"[OPEN] raw={ob.open_raw} valid={ob.open_valid} ghost={ob.open_ghost}")
    print(f"       note={ob.integrity_note}")

    log_dir = Path(os.environ.get("FACTORY_LOG_DIR", str(_ROOT / "logs")))
    print(f"\n[LOGS] dir={log_dir}")
    for pat in (
        "factory_scan_us_supernova_*",
        "factory_scan_us_*_r2_*",
        "factory_daily_audit_us_*",
    ):
        print(f"  {pat}: {_tail_logs(log_dir, pat)}")

    print("\n[RECOVERY COMMANDS]")
    print("  bash scripts/reset_factory_pipeline.sh")
    print("  bash scripts/master_sync_kr_us.sh")
    print("  FACTORY_ALLOW_SESSION_RESCAN=1 ./factory.sh --scan-us-supernova --lock-timeout 600")
    return 0


if __name__ == "__main__":
    sys.exit(main())

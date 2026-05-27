#!/usr/bin/env python3
"""
systemd `dante-snapshot` oneshot: CQRS 읽기용 `market_data_snapshot.sqlite` 갱신 +
`ops_snapshot` 1행 기록 (`shadow_tracking.record_ops_snapshot_from_live_state`).
매매 코어는 호출하지 않는다.
"""
from __future__ import annotations

import os
import sqlite3
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))


def _backup_market_db() -> None:
    from market_db_paths import MARKET_DATA_DB_PATH, MARKET_DATA_SNAPSHOT_PATH

    if not os.path.isfile(MARKET_DATA_DB_PATH):
        print(f"[dante-snapshot] skip backup (no main db): {MARKET_DATA_DB_PATH}")
        return
    src = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=90.0)
    try:
        dst = sqlite3.connect(MARKET_DATA_SNAPSHOT_PATH, timeout=90.0)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    print(f"[dante-snapshot] SQLite backup ok -> {MARKET_DATA_SNAPSHOT_PATH}")


def main() -> int:
    os.chdir(_ROOT)
    if _ROOT not in sys.path:
        sys.path.insert(0, _ROOT)
    try:
        _backup_market_db()
    except Exception as e:
        print(f"[dante-snapshot] backup 실패: {e}", file=sys.stderr)

    try:
        from shadow_tracking import record_ops_snapshot_from_live_state

        if record_ops_snapshot_from_live_state():
            print("[dante-snapshot] ops_snapshot row ok")
        else:
            print("[dante-snapshot] ops_snapshot 기록 실패(재시도 소진)", file=sys.stderr)
    except Exception as e:
        print(f"[dante-snapshot] ops_snapshot 예외: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
In-memory / SQLite growth caps — 주식·코인 공용 (4GB RAM 서버 OOM 완화).

한국/미국 경로의 `HISTORY_CAP`·`LIMIT 300`·`keep_last` 패턴을 모듈화한다.
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, MutableMapping

# OHLCV·시그널 엔진 공통 바 수 (forward/shared.py · data_miner.py 와 동일)
OHLCV_BAR_LIMIT: int = 300

# forward_trades / bitget_forward_trades 분석용 최근 청산 표본
FORWARD_CLOSED_TRADES_LIMIT: int = 500

# per-market daily report DataFrame 상한 (CLOSED 최근 N + OPEN 전건)
FORWARD_REPORT_CLOSED_LIMIT: int = 1200


def forward_trades_bounded_sql(
    *,
    table: str = "bitget_forward_trades",
    market_type: str | None = None,
    closed_limit: int | None = None,
) -> tuple[str, tuple]:
    """OPEN/ACTIVE(비-CLOSED) 전건 + CLOSED 최 recent N건.

    `ORDER BY id DESC LIMIT` 단독 사용 시 오래된 OPEN 포지션이 누락되는 버그를 방지한다.
    """
    lim = int(closed_limit if closed_limit is not None else FORWARD_REPORT_CLOSED_LIMIT)
    lim = max(1, min(lim, 10_000))
    if market_type is not None:
        mkt = str(market_type).lower()
        sql = f"""
            SELECT * FROM {table}
            WHERE market_type = ?
              AND (
                IFNULL(status, '') NOT LIKE 'CLOSED%'
                OR id IN (
                    SELECT id FROM (
                        SELECT id FROM {table}
                        WHERE market_type = ? AND status LIKE 'CLOSED%'
                        ORDER BY id DESC
                        LIMIT ?
                    )
                )
              )
        """
        return sql, (mkt, mkt, lim)
    sql = f"""
        SELECT * FROM {table}
        WHERE (
            IFNULL(status, '') NOT LIKE 'CLOSED%'
            OR id IN (
                SELECT id FROM (
                    SELECT id FROM {table}
                    WHERE status LIKE 'CLOSED%'
                    ORDER BY id DESC
                    LIMIT ?
                )
            )
        )
    """
    return sql, (lim,)


def evict_oldest_dict_keys(
    store: MutableMapping[str, Any],
    max_keys: int,
    *,
    ts_getter=None,
) -> None:
    """dict 크기 상한 — ts_getter(key)->float mono/time, 없으면 삽입 순서 대체."""
    excess = len(store) - int(max_keys)
    if excess <= 0:
        return
    if ts_getter is None:
        for k in list(store.keys())[:excess]:
            store.pop(k, None)
        return
    ranked = sorted(store.keys(), key=lambda k: float(ts_getter(k) or 0.0))
    for k in ranked[:excess]:
        store.pop(k, None)


def prune_sqlite_by_date_prefix(
    conn: sqlite3.Connection,
    table: str,
    date_col: str,
    keep_days: int,
) -> int:
    """YYYY-MM-DD 접두 ts 컬럼 — keep_days 일 이전 행 DELETE."""
    if keep_days <= 0:
        return 0
    cutoff = (datetime.now() - timedelta(days=int(keep_days))).strftime("%Y-%m-%d")
    cur = conn.execute(
        f"DELETE FROM {table} WHERE substr({date_col}, 1, 10) < ?",
        (cutoff,),
    )
    return int(cur.rowcount or 0)


def prune_sqlite_keep_last_ids(
    conn: sqlite3.Connection,
    table: str,
    keep_last: int,
    *,
    id_col: str = "id",
) -> int:
    """최근 keep_last 행만 유지 (meta_state_market_db 패턴)."""
    if keep_last <= 0:
        return 0
    before = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    n_before = int(before[0] or 0) if before else 0
    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE {id_col} NOT IN (
            SELECT {id_col} FROM {table}
            ORDER BY {id_col} DESC
            LIMIT ?
        )
        """,
        (int(keep_last),),
    )
    after = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    n_after = int(after[0] or 0) if after else 0
    return max(0, n_before - n_after)


def prune_ops_events_older_than_days(
    conn: sqlite3.Connection,
    *,
    keep_days: int = 60,
    table: str = "ops_events",
    ts_col: str = "ts_utc",
) -> int:
    """ISO ts_utc 컬럼 — keep_days 일 이전 ops_events DELETE."""
    if keep_days <= 0:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(keep_days))).isoformat()
    cur = conn.execute(
        f"DELETE FROM {table} WHERE {ts_col} < ?",
        (cutoff,),
    )
    return int(cur.rowcount or 0)


class ThrottledCallback:
    """주기적 flush — 매 INSERT 마다 돌리지 않도록 최소 간격(sec) 적용."""

    __slots__ = ("_interval_sec", "_last_mono")

    def __init__(self, interval_sec: float = 3600.0) -> None:
        self._interval_sec = max(60.0, float(interval_sec))
        self._last_mono = 0.0

    def due(self) -> bool:
        now = time.monotonic()
        if (now - self._last_mono) < self._interval_sec:
            return False
        self._last_mono = now
        return True


def ohlcv_limit_sql(*, bar_limit: int | None = None, order: str = "DESC") -> str:
    """OHLCV SELECT tail — ORDER BY Date {order} LIMIT N."""
    n = int(bar_limit if bar_limit is not None else OHLCV_BAR_LIMIT)
    n = max(50, min(n, 2000))
    ord_u = "ASC" if str(order).upper() == "ASC" else "DESC"
    return f" ORDER BY Date {ord_u} LIMIT {n}"


def ohlcv_entry_window_sql(
    *,
    bar_limit: int | None = None,
    lookback_days: int = 120,
) -> str:
    """entry_date 기준 lookback_days 창 — DESC LIMIT 300 단독보다 장기 TF/저주기 안전."""
    n = int(bar_limit if bar_limit is not None else OHLCV_BAR_LIMIT)
    n = max(50, min(n, 2000))
    days = max(30, min(int(lookback_days), 365))
    return (
        f" WHERE Date >= date(?, '-{days} days') ORDER BY Date ASC LIMIT {n}"
    )


def ohlcv_date_range_sql(
    *,
    start: str,
    end: str,
    bar_limit: int | None = None,
) -> tuple[str, tuple]:
    """백테스트·크래시 구간 — [start, end] + safety LIMIT (전체 히스토리 금지)."""
    s = str(start)[:10]
    e = str(end)[:10]
    try:
        from bitget.infra.memory_policy import TIME_MACHINE_MAX_BARS_PER_TABLE

        default_cap = int(TIME_MACHINE_MAX_BARS_PER_TABLE)
    except Exception:
        default_cap = 5_000
    n = int(bar_limit if bar_limit is not None else default_cap)
    n = max(50, min(n, 20_000))
    sql = (
        " WHERE substr(Date, 1, 10) >= ? AND substr(Date, 1, 10) <= ? "
        f"ORDER BY Date ASC LIMIT {n}"
    )
    return sql, (s, e)


def truncate_orderbook_levels(levels: Any, max_depth: int) -> Any:
    """WebSocket orderbook bids/asks — top-N 레벨만 유지 (spread gate 용)."""
    if not levels or not isinstance(levels, list):
        return levels
    n = max(1, min(int(max_depth), 200))
    return levels[:n]


def bounded_dict_view(store: Mapping[str, Any], max_keys: int) -> Mapping[str, Any]:
    """읽기 전용 — 초과 키는 무시 (테스트/디버그용)."""
    if len(store) <= max_keys:
        return store
    keys = list(store.keys())[-max_keys:]
    return {k: store[k] for k in keys}

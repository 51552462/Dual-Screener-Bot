"""Unit tests — ohlcv history pagination + merge (no live API)."""
from __future__ import annotations

from datetime import datetime, timezone

from bitget.data.ohlcv_history_backfill import (
    fetch_ohlcv_paginated,
    merge_ohlcv_rows,
)


def _ts(y, m, d) -> int:
    return int(datetime(y, m, d, tzinfo=timezone.utc).timestamp() * 1000)


def test_merge_ohlcv_prefer_fetched():
    existing = [[_ts(2026, 1, 1), 1, 1, 1, 1, 1]]
    fetched = [[_ts(2026, 1, 1), 2, 2, 2, 2, 2], [_ts(2026, 1, 2), 3, 3, 3, 3, 3]]
    m = merge_ohlcv_rows(existing, fetched)
    assert len(m) == 2
    assert m[0][4] == 2.0  # close from fetched
    assert m[1][0] == _ts(2026, 1, 2)


def test_fetch_ohlcv_paginated_keeps_recent_tail():
    day = 86_400_000
    start = _ts(2024, 1, 1)
    until = start + 19 * day

    def fake_fetch(sym, tf, since, lim):
        out = []
        for i in range(3):
            ts = int(since) + i * day
            out.append([ts, 1, 1, 1, 1, 1])
        return out

    class _Ex:
        pass

    rows = fetch_ohlcv_paginated(
        _Ex(),
        "BTC/USDT:USDT",
        "1d",
        since_ms=start,
        target_bars=10,
        until_ms=until,
        page_limit=3,
        sleep_sec=0,
        fetch_fn=fake_fetch,
    )
    assert len(rows) == 10
    assert rows[0][0] < rows[-1][0]
    # most-recent window relative to until
    assert rows[-1][0] >= until - 2 * day

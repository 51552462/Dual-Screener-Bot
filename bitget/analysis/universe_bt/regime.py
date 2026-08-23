"""C3 regime resolver — historical CURRENT_REGIME_KEY unavailable (Ask path c)."""
from __future__ import annotations


def resolve_historical_regime(symbol: str, market_type: str, bar_ts: int) -> str:
    """Return regime label for a historical bar.

    C3: no point-in-time CURRENT_REGIME_KEY archive and no HIGH_VOL-capable
    deterministic reapply — always ``UNKNOWN``. crash_window metric is deferred;
    callers must leave ``exit_trigger`` NULL.
    """
    _ = (symbol, market_type, bar_ts)
    return "UNKNOWN"

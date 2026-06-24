"""
Scanner hook layer (Phase 2).

Delegates to existing scanner modules — **no signal logic here**.
All conditionals, engines, and entry algorithms stay in:
  - bitget/master_scanner.py
  - bitget/supernova_hunter.py
  - bitget/signal_engines.py
"""
from __future__ import annotations

from typing import Optional


def run_master_scan(
    *,
    market_filter: Optional[str] = None,
    engine_filter: Optional[str] = None,
    include_embedded_supernova: Optional[bool] = None,
) -> None:
    """MTF master scan — calls master_scanner.run_scan unchanged."""
    from bitget.master_scanner import run_scan

    run_scan(
        market_filter=market_filter,
        engine_filter=engine_filter,
        include_embedded_supernova=include_embedded_supernova,
    )


def run_engine_scan(*, market: str, scanner_key: str) -> None:
    """Staggered slot — single engine family per cron invocation."""
    mkt = str(market or "spot").strip().lower()
    key = str(scanner_key or "").strip().lower()
    if key == "shadow":
        from bitget.shadow_performance_tracker import run_shadow_performance_evaluation

        run_shadow_performance_evaluation()
        return
    mf = "spot" if mkt in ("spot", "sp") else "futures"
    run_master_scan(
        market_filter=mf,
        engine_filter=key if key not in ("supernova",) else None,
        include_embedded_supernova=False,
    )


def run_master_mtf_scheduler() -> None:
    """Candle-close scheduler — calls master_scanner.run_mtf_scheduler unchanged."""
    from bitget.master_scanner import run_mtf_scheduler

    run_mtf_scheduler()


def run_supernova_live(market_type: str, timeframe: str = "1H") -> None:
    """Supernova live scan — calls supernova_hunter.execute_supernova_live_scan unchanged."""
    from bitget.supernova_hunter import execute_supernova_live_scan

    execute_supernova_live_scan(market_type, timeframe)


def run_supernova_spot(timeframe: str = "1H") -> None:
    run_supernova_live("spot", timeframe)


def run_supernova_futures(timeframe: str = "1H") -> None:
    run_supernova_live("futures", timeframe)


def run_supernova_sniper_scheduler() -> None:
    """24/7 sniper loop — calls supernova_hunter.run_live_sniper_scheduler unchanged."""
    from bitget.supernova_hunter import run_live_sniper_scheduler

    run_live_sniper_scheduler()

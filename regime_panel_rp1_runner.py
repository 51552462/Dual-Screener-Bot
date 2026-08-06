"""RP-1 runner — wires time_machine_backtester to regime_panel (optional live fdr)."""
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from typing import Any, Dict, List

import pandas as pd

from time_machine_backtester import (
    _backtest_one_ticker,
    _process_pool_max_workers,
    load_factory_brain_readonly,
)


def default_run_backtest_for_period(
    regime_name: str,
    stock_list: List[str],
    start_dt: str,
    end_dt: str,
) -> Dict[str, Any]:
    """Run supernova template backtest for one window; returns {trades: [...]}."""
    config = load_factory_brain_readonly()
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {})
    ud_templates = config.get("UNDERDOG_CLUSTER_TEMPLATES", {})
    all_templates = {**ml_templates, **ud_templates}
    evolved_factors = config.get("EVOLVED_ALPHA_FACTORS")
    if not isinstance(evolved_factors, dict):
        evolved_factors = {}

    if not all_templates:
        return {"trades": [], "gate": "no_templates"}

    fetch_start = (pd.to_datetime(start_dt) - timedelta(days=40)).strftime("%Y-%m-%d")
    results: List[dict] = []
    max_workers = _process_pool_max_workers()

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [
            ex.submit(
                _backtest_one_ticker,
                code,
                fetch_start,
                end_dt,
                start_dt,
                all_templates,
                evolved_factors,
            )
            for code in stock_list
        ]
        for fut in as_completed(futs):
            try:
                p = fut.result()
                if isinstance(p, dict):
                    results.extend(p.get("trades", []))
            except Exception:
                pass

    return {"trades": results, "regime_name": regime_name}

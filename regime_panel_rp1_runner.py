"""RP-1 runner — wires time_machine_backtester to regime_panel (optional live fdr)."""
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from time_machine_backtester import (
    _backtest_one_ticker,
    _process_pool_max_workers,
    load_factory_brain_readonly,
)

_RP1_BRAIN_CACHE: Optional[Dict[str, Any]] = None


def load_rp1_brain_cached(*, force_reload: bool = False) -> Dict[str, Any]:
    """RP-1 session cache — avoid 15× reload races during live run."""
    global _RP1_BRAIN_CACHE
    if _RP1_BRAIN_CACHE is None or force_reload:
        _RP1_BRAIN_CACHE = load_factory_brain_readonly()
    return _RP1_BRAIN_CACHE


def clear_rp1_brain_cache() -> None:
    global _RP1_BRAIN_CACHE
    _RP1_BRAIN_CACHE = None


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def _run_ticker_batch(
    stock_list: List[str],
    fetch_start: str,
    end_dt: str,
    start_dt: str,
    all_templates: Dict[str, Any],
    evolved_factors: Dict[str, Any],
    *,
    use_pool: bool,
) -> tuple[List[dict], Dict[str, int]]:
    """Run backtests; default sequential (RP-1 batch). Set RP1_PARALLEL=1 for ProcessPool."""
    results: List[dict] = []
    gate_summary: Dict[str, int] = {}

    def _consume(pack: Any) -> None:
        if not isinstance(pack, dict):
            gate_summary["invalid_pack"] = gate_summary.get("invalid_pack", 0) + 1
            return
        gate = str(pack.get("gate") or "unknown")
        gate_summary[gate] = gate_summary.get(gate, 0) + 1
        results.extend(pack.get("trades", []))

    if use_pool:
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
                    _consume(fut.result())
                except Exception:
                    gate_summary["pool_error"] = gate_summary.get("pool_error", 0) + 1
    else:
        for code in stock_list:
            try:
                _consume(
                    _backtest_one_ticker(
                        code,
                        fetch_start,
                        end_dt,
                        start_dt,
                        all_templates,
                        evolved_factors,
                    )
                )
            except Exception:
                gate_summary["worker_error"] = gate_summary.get("worker_error", 0) + 1

    return results, gate_summary


def default_run_backtest_for_period(
    regime_name: str,
    stock_list: List[str],
    start_dt: str,
    end_dt: str,
) -> Dict[str, Any]:
    """Run supernova template backtest for one window; returns {trades: [...]}."""
    config = load_rp1_brain_cached()
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {}) or {}
    ud_templates = config.get("UNDERDOG_CLUSTER_TEMPLATES", {}) or {}
    all_templates = {**ml_templates, **ud_templates}
    evolved_factors = config.get("EVOLVED_ALPHA_FACTORS")
    if not isinstance(evolved_factors, dict):
        evolved_factors = {}

    if not all_templates:
        return {
            "trades": [],
            "gate": "no_templates",
            "gate_summary": {"no_templates": 1},
            "template_ml": len(ml_templates),
            "template_ud": len(ud_templates),
            "config_empty": not bool(config),
        }

    fetch_start = (pd.to_datetime(start_dt) - timedelta(days=40)).strftime("%Y-%m-%d")
    # RP-1 live run: sequential default — ProcessPool + large universe often yields silent 0 trades.
    use_pool = _env_flag("RP1_PARALLEL")
    results, gate_summary = _run_ticker_batch(
        stock_list,
        fetch_start,
        end_dt,
        start_dt,
        all_templates,
        evolved_factors,
        use_pool=use_pool,
    )

    out: Dict[str, Any] = {
        "trades": results,
        "regime_name": regime_name,
        "gate_summary": gate_summary,
        "execution_mode": "parallel" if use_pool else "sequential",
    }
    if not results and gate_summary:
        out["gate"] = max(gate_summary, key=gate_summary.get)
    return out

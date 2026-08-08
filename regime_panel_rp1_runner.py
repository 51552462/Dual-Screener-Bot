"""RP-1 runner — wires time_machine_backtester to regime_panel (optional live fdr)."""
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from time_machine_backtester import (
    _backtest_one_ticker,
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


def _env_int(name: str, default: int, *, lo: int = 1, hi: int = 10_000) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(lo, min(hi, int(raw)))
    except ValueError:
        return default


def resolve_rp1_max_workers() -> int:
    """Default 2 — Lightsail RAM-safe; override via RP1_MAX_WORKERS or MAX_WORKERS."""
    raw = (os.environ.get("RP1_MAX_WORKERS") or os.environ.get("MAX_WORKERS") or "2").strip()
    try:
        return max(1, min(8, int(raw)))
    except ValueError:
        return 2


def resolve_rp1_chunk_size() -> int:
    """ProcessPool submit batch size (avoids 400 pending futures)."""
    return _env_int("RP1_CHUNK_SIZE", 25, lo=5, hi=200)


def resolve_rp1_use_parallel() -> bool:
    if _env_flag("RP1_SEQUENTIAL"):
        return False
    if os.environ.get("RP1_PARALLEL", "").strip().lower() in ("0", "false", "no"):
        return False
    return True


def log_rp1(msg: str) -> None:
    print(msg, flush=True)


def build_rp1_universe() -> List[str]:
    """
    KR+US ticker list for live run.
    RP1_FAST=1 → 50+50 smoke. RP1_KR_LIMIT / RP1_US_LIMIT override (default 200 each).
    """
    if _env_flag("RP1_FAST"):
        kr_lim, us_lim = 50, 50
    else:
        kr_lim = _env_int("RP1_KR_LIMIT", 200, lo=1, hi=3000)
        us_lim = _env_int("RP1_US_LIMIT", 200, lo=0, hi=3000)

    from blackhole_hunter import get_us_ticker_list
    from supernova_hunter import get_krx_list

    kr = get_krx_list()["Code"].astype(str).tolist()[:kr_lim]
    us_df = get_us_ticker_list()
    us = us_df["Symbol"].astype(str).tolist()[:us_lim] if us_df is not None else []
    universe = kr + us
    log_rp1(f"[RP-1] universe={len(universe)} (KR={len(kr)}, US={len(us)})")
    return universe


def _run_ticker_batch(
    stock_list: List[str],
    fetch_start: str,
    end_dt: str,
    start_dt: str,
    all_templates: Dict[str, Any],
    evolved_factors: Dict[str, Any],
    *,
    use_pool: bool,
    regime_name: str = "",
) -> tuple[List[dict], Dict[str, int]]:
    """Chunked ProcessPool (default) or sequential (RP1_SEQUENTIAL=1)."""
    results: List[dict] = []
    gate_summary: Dict[str, int] = {}
    n_total = len(stock_list)
    progress_every = _env_int("RP1_PROGRESS_EVERY", 25, lo=1, hi=500)
    label = regime_name or "period"

    def _consume(pack: Any) -> None:
        if not isinstance(pack, dict):
            gate_summary["invalid_pack"] = gate_summary.get("invalid_pack", 0) + 1
            return
        gate = str(pack.get("gate") or "unknown")
        gate_summary[gate] = gate_summary.get(gate, 0) + 1
        results.extend(pack.get("trades", []))

    def _maybe_log(done: int) -> None:
        if done % progress_every == 0 or done == n_total:
            log_rp1(f"  [{label}] tickers {done}/{n_total} trades={len(results)}")

    if use_pool:
        max_workers = resolve_rp1_max_workers()
        chunk = resolve_rp1_chunk_size()
        done = 0
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            for batch_start in range(0, n_total, chunk):
                batch = stock_list[batch_start : batch_start + chunk]
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
                    for code in batch
                ]
                for fut in as_completed(futs):
                    done += 1
                    try:
                        _consume(fut.result())
                    except Exception:
                        gate_summary["pool_error"] = gate_summary.get("pool_error", 0) + 1
                    _maybe_log(done)
    else:
        for i, code in enumerate(stock_list, 1):
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
            _maybe_log(i)

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
    use_pool = resolve_rp1_use_parallel()
    results, gate_summary = _run_ticker_batch(
        stock_list,
        fetch_start,
        end_dt,
        start_dt,
        all_templates,
        evolved_factors,
        use_pool=use_pool,
        regime_name=regime_name,
    )

    mode = "parallel_chunked" if use_pool else "sequential"
    out: Dict[str, Any] = {
        "trades": results,
        "regime_name": regime_name,
        "gate_summary": gate_summary,
        "execution_mode": mode,
        "max_workers": resolve_rp1_max_workers() if use_pool else 1,
        "chunk_size": resolve_rp1_chunk_size() if use_pool else None,
    }
    if not results and gate_summary:
        out["gate"] = max(gate_summary, key=gate_summary.get)
    return out

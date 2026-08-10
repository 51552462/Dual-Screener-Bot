"""RP-1 runner — wires time_machine_backtester to regime_panel (optional live fdr)."""
from __future__ import annotations

import glob
import hashlib
import os
import pickle
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from time_machine_backtester import (
    REGIME_PERIODS,
    _backtest_one_ticker,
    _window_cache_key,
    backtest_ticker_rp1_multi_window,
    collect_rp1_ohlcv_windows,
    compute_rp1_global_ohlcv_bounds,
    load_factory_brain_readonly,
)

_RP1_BRAIN_CACHE: Optional[Dict[str, Any]] = None
_MATRIX_BY_WINDOW: Optional[Dict[str, Dict[str, Any]]] = None


def load_rp1_brain_cached(*, force_reload: bool = False) -> Dict[str, Any]:
    """RP-1 session cache — avoid 15× reload races during live run."""
    global _RP1_BRAIN_CACHE
    if _RP1_BRAIN_CACHE is None or force_reload:
        _RP1_BRAIN_CACHE = load_factory_brain_readonly()
    return _RP1_BRAIN_CACHE


def clear_rp1_brain_cache() -> None:
    global _RP1_BRAIN_CACHE
    _RP1_BRAIN_CACHE = None


def clear_rp1_matrix_cache() -> None:
    global _MATRIX_BY_WINDOW
    _MATRIX_BY_WINDOW = None


def matrix_cache_is_ready() -> bool:
    return _MATRIX_BY_WINDOW is not None


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def resolve_rp1_use_matrix_cache() -> bool:
    """Default on — set RP1_MATRIX=0 to force legacy per-period fetch."""
    if _env_flag("RP1_MATRIX_DISABLE") or _env_flag("RP1_NO_MATRIX"):
        return False
    if os.environ.get("RP1_MATRIX", "").strip().lower() in ("0", "false", "no"):
        return False
    return True


def resolve_rp1_matrix_reuse() -> bool:
    """Persist matrix trades to disk — second smoke run skips FDR fetch (~minutes)."""
    return _env_flag("RP1_MATRIX_REUSE") or _env_flag("RP1_MATRIX_SNAPSHOT")


def resolve_rp1_metrics_only() -> bool:
    """Skip matrix prime; load existing snapshot and recompute 15-period metrics only (~10min)."""
    return _env_flag("RP1_METRICS_ONLY")


def resolve_rp1_matrix_snapshot_path() -> str:
    """Explicit snapshot path for RP1_METRICS_ONLY (optional)."""
    return os.environ.get("RP1_MATRIX_SNAPSHOT_PATH", "").strip()


def _matrix_snapshot_dir() -> str:
    base = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "reports",
        "regime_panel",
        "matrix_cache",
    )
    os.makedirs(base, exist_ok=True)
    return base


def _matrix_snapshot_path(stock_list: List[str], fetch_start: str, fetch_end: str) -> str:
    digest = hashlib.sha256(
        (",".join(sorted(stock_list)) + f"|{fetch_start}|{fetch_end}").encode("utf-8")
    ).hexdigest()[:20]
    return os.path.join(_matrix_snapshot_dir(), f"matrix_{digest}.pkl")


def _load_matrix_snapshot_file(path: str) -> Optional[Dict[str, Dict[str, Any]]]:
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, "rb") as fh:
            data = pickle.load(fh)
        if isinstance(data, dict) and data:
            log_rp1(f"[RP-1] matrix snapshot loaded: {path}")
            return data
    except Exception as exc:
        log_rp1(f"[RP-1] matrix snapshot load failed ({path}): {exc}")
    return None


def _find_latest_matrix_snapshot() -> Optional[str]:
    pattern = os.path.join(_matrix_snapshot_dir(), "matrix_*.pkl")
    candidates = glob.glob(pattern)
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def _load_matrix_snapshot(stock_list: List[str], fetch_start: str, fetch_end: str) -> Optional[Dict[str, Dict[str, Any]]]:
    path = _matrix_snapshot_path(stock_list, fetch_start, fetch_end)
    if not os.path.isfile(path):
        return None
    return _load_matrix_snapshot_file(path)


def _load_matrix_snapshot_forced() -> Optional[Dict[str, Dict[str, Any]]]:
    """Metrics-only: load explicit path or newest snapshot (no universe hash check)."""
    explicit = resolve_rp1_matrix_snapshot_path()
    if explicit:
        return _load_matrix_snapshot_file(explicit)
    latest = _find_latest_matrix_snapshot()
    if latest:
        log_rp1(f"[RP-1] matrix snapshot auto-pick (metrics-only): {latest}")
        return _load_matrix_snapshot_file(latest)
    return None


def _save_matrix_snapshot(
    stock_list: List[str],
    fetch_start: str,
    fetch_end: str,
    matrix: Dict[str, Dict[str, Any]],
) -> None:
    path = _matrix_snapshot_path(stock_list, fetch_start, fetch_end)
    try:
        with open(path, "wb") as fh:
            pickle.dump(matrix, fh, protocol=4)
        log_rp1(f"[RP-1] matrix snapshot saved: {path}")
    except Exception as exc:
        log_rp1(f"[RP-1] matrix snapshot save failed: {exc}")


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


def _brain_templates_and_factors(
    config: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {}) or {}
    ud_templates = config.get("UNDERDOG_CLUSTER_TEMPLATES", {}) or {}
    all_templates = {**ml_templates, **ud_templates}
    evolved_factors = config.get("EVOLVED_ALPHA_FACTORS")
    if not isinstance(evolved_factors, dict):
        evolved_factors = {}
    return all_templates, evolved_factors


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


def _run_matrix_ticker_batch(
    stock_list: List[str],
    global_fetch_start: str,
    global_end_dt: str,
    ohlcv_windows: List[Tuple[str, str]],
    all_templates: Dict[str, Any],
    evolved_factors: Dict[str, Any],
    *,
    use_pool: bool,
) -> Dict[str, Dict[str, Any]]:
    """One FDR fetch per ticker; merge trades into per-window cache buckets."""
    merged: Dict[str, Dict[str, Any]] = {
        _window_cache_key(s, e): {"trades": [], "gate_summary": {}} for s, e in ohlcv_windows
    }
    n_total = len(stock_list)
    progress_every = _env_int("RP1_PROGRESS_EVERY", 25, lo=1, hi=500)
    trade_count = 0

    def _consume(pack: Any) -> None:
        nonlocal trade_count
        if not isinstance(pack, dict):
            return
        fetch_gate = str(pack.get("fetch_gate") or "unknown")
        by_window = pack.get("by_window") or {}
        for wkey, win_pack in by_window.items():
            bucket = merged.setdefault(wkey, {"trades": [], "gate_summary": {}})
            if not isinstance(win_pack, dict):
                continue
            gate = str(win_pack.get("gate") or fetch_gate)
            bucket["gate_summary"][gate] = bucket["gate_summary"].get(gate, 0) + 1
            new_trades = win_pack.get("trades", [])
            trade_count += len(new_trades)
            bucket["trades"].extend(new_trades)

    def _maybe_log(done: int) -> None:
        if done % progress_every == 0 or done == n_total:
            log_rp1(
                f"  [matrix] tickers {done}/{n_total} windows={len(merged)} trades={trade_count}"
            )

    if use_pool:
        max_workers = resolve_rp1_max_workers()
        chunk = resolve_rp1_chunk_size()
        done = 0
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            for batch_start in range(0, n_total, chunk):
                batch = stock_list[batch_start : batch_start + chunk]
                futs = [
                    ex.submit(
                        backtest_ticker_rp1_multi_window,
                        code,
                        global_fetch_start,
                        global_end_dt,
                        ohlcv_windows,
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
                        pass
                    _maybe_log(done)
    else:
        for i, code in enumerate(stock_list, 1):
            try:
                _consume(
                    backtest_ticker_rp1_multi_window(
                        code,
                        global_fetch_start,
                        global_end_dt,
                        ohlcv_windows,
                        all_templates,
                        evolved_factors,
                        batch_mode=False,
                    )
                )
            except Exception:
                pass
            _maybe_log(i)

    return merged


def prime_rp1_matrix_cache(stock_list: List[str]) -> Dict[str, Any]:
    """
    Pre-fetch OHLCV once per ticker and simulate all RP-1 windows (primary + backup).
    Subsequent default_run_backtest_for_period calls read from RAM cache.
    """
    global _MATRIX_BY_WINDOW
    if not resolve_rp1_use_matrix_cache():
        log_rp1("[RP-1] matrix cache disabled (RP1_MATRIX=0)")
        return {"enabled": False}

    config = load_rp1_brain_cached()
    all_templates, evolved_factors = _brain_templates_and_factors(config)
    if not all_templates:
        raise RuntimeError("RP-1 matrix prime aborted: LIVE_CLUSTER_TEMPLATES empty")

    ohlcv_windows = collect_rp1_ohlcv_windows(REGIME_PERIODS)
    global_fetch_start, global_end_dt = compute_rp1_global_ohlcv_bounds(REGIME_PERIODS)
    use_pool = resolve_rp1_use_parallel()
    log_rp1(
        f"[RP-1] matrix prime: tickers={len(stock_list)} windows={len(ohlcv_windows)} "
        f"fetch={global_fetch_start}~{global_end_dt} parallel={use_pool}"
    )
    if resolve_rp1_metrics_only():
        loaded = _load_matrix_snapshot_forced()
        if loaded is None:
            raise RuntimeError(
                "RP1_METRICS_ONLY=1 but no snapshot found — set RP1_MATRIX_SNAPSHOT_PATH "
                "or run a full prime with RP1_MATRIX_REUSE=1 first"
            )
        _MATRIX_BY_WINDOW = loaded
        total_trades = sum(len(v.get("trades", [])) for v in _MATRIX_BY_WINDOW.values())
        log_rp1(f"[RP-1] matrix prime skipped (metrics-only): total_trades={total_trades}")
        return {
            "enabled": True,
            "windows": len(ohlcv_windows),
            "tickers": len(stock_list),
            "total_trades": total_trades,
            "fetch_range": (global_fetch_start, global_end_dt),
            "snapshot": "metrics_only",
        }
    if resolve_rp1_matrix_reuse():
        loaded = _load_matrix_snapshot(stock_list, global_fetch_start, global_end_dt)
        if loaded is not None:
            _MATRIX_BY_WINDOW = loaded
            total_trades = sum(len(v.get("trades", [])) for v in _MATRIX_BY_WINDOW.values())
            log_rp1(f"[RP-1] matrix prime skipped (snapshot): total_trades={total_trades}")
            return {
                "enabled": True,
                "windows": len(ohlcv_windows),
                "tickers": len(stock_list),
                "total_trades": total_trades,
                "fetch_range": (global_fetch_start, global_end_dt),
                "snapshot": "load",
            }

    _MATRIX_BY_WINDOW = _run_matrix_ticker_batch(
        stock_list,
        global_fetch_start,
        global_end_dt,
        ohlcv_windows,
        all_templates,
        evolved_factors,
        use_pool=use_pool,
    )
    total_trades = sum(len(v.get("trades", [])) for v in _MATRIX_BY_WINDOW.values())
    log_rp1(f"[RP-1] matrix prime done: total_trades={total_trades}")
    if resolve_rp1_matrix_reuse():
        _save_matrix_snapshot(stock_list, global_fetch_start, global_end_dt, _MATRIX_BY_WINDOW)
    return {
        "enabled": True,
        "windows": len(ohlcv_windows),
        "tickers": len(stock_list),
        "total_trades": total_trades,
        "fetch_range": (global_fetch_start, global_end_dt),
        "snapshot": "save" if resolve_rp1_matrix_reuse() else None,
    }


def default_run_backtest_for_period(
    regime_name: str,
    stock_list: List[str],
    start_dt: str,
    end_dt: str,
) -> Dict[str, Any]:
    """Run supernova template backtest for one window; returns {trades: [...]}."""
    wkey = _window_cache_key(start_dt, end_dt)
    if _MATRIX_BY_WINDOW is not None and wkey in _MATRIX_BY_WINDOW:
        cached = _MATRIX_BY_WINDOW[wkey]
        return {
            "trades": list(cached.get("trades", [])),
            "regime_name": regime_name,
            "gate_summary": dict(cached.get("gate_summary", {})),
            "execution_mode": "matrix_cached",
        }

    config = load_rp1_brain_cached()
    all_templates, evolved_factors = _brain_templates_and_factors(config)
    ml_templates = config.get("LIVE_CLUSTER_TEMPLATES", {}) or {}
    ud_templates = config.get("UNDERDOG_CLUSTER_TEMPLATES", {}) or {}

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

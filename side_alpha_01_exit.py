"""SIDE-ALPHA-01 stage-2 — CAT-E SIDEWAYS exit single lever (SL relax).

Env gate: SIDE_ALPHA_01_EXIT=1
Lever (택1): DYNAMIC_MAE_SL -3.5 → -4.5 on SIDEWAYS bucket windows only.
Does NOT touch TP, hold bars, CLUSTER_1, BULL/BEAR, config_kv live.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import pandas as pd

# Baseline CAT-E RP-1 hardcodes (time_machine_backtester._simulate_trades_on_ohlcv)
BASE_MAE_SL = -3.5
BASE_MFE_TP = 10.0
BASE_HOLD_BARS = 15

# Single lever: widen SL so fewer early STAT_MAE cuts (cause B: SL-heavy)
SIDEWAYS_MAE_SL = -4.5

SIDE_TARGETS = ("SIDE_02_2015횡보", "SIDE_03_2021-22혼조")


def resolve_side_alpha_01_exit() -> bool:
    return os.environ.get("SIDE_ALPHA_01_EXIT", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _env_float(name: str, default: float, *, lo: float, hi: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(lo, min(hi, float(raw)))
    except ValueError:
        return default


def resolve_sideways_mae_sl() -> float:
    """SIDEWAYS-only MAE SL when patch active (more negative = looser stop)."""
    return _env_float("SIDE_ALPHA_01_MAE_SL", SIDEWAYS_MAE_SL, lo=-8.0, hi=-3.6)


def sideways_window_keys(
    regime_periods: Optional[Mapping[str, Any]] = None,
) -> Set[str]:
    """Primary + backup (start|end) keys whose bucket == SIDEWAYS."""
    if regime_periods is None:
        from time_machine_backtester import REGIME_PERIODS

        regime_periods = REGIME_PERIODS
    keys: Set[str] = set()
    for meta in regime_periods.values():
        if str(meta.get("bucket") or "").upper() != "SIDEWAYS":
            continue
        keys.add(f"{meta['start']}|{meta['end']}")
        backup = meta.get("backup")
        if isinstance(backup, dict) and backup.get("start") and backup.get("end"):
            keys.add(f"{backup['start']}|{backup['end']}")
    return keys


def is_sideways_window(start_dt: str, end_dt: str) -> bool:
    return f"{start_dt}|{end_dt}" in sideways_window_keys()


def resolve_mae_sl_for_window(start_dt: str, end_dt: str) -> float:
    """Return MAE SL for this OHLCV window (SIDEWAYS overlay when gated)."""
    if resolve_side_alpha_01_exit() and is_sideways_window(start_dt, end_dt):
        return resolve_sideways_mae_sl()
    return BASE_MAE_SL


def simulate_exit_on_bars(
    future: pd.DataFrame,
    entry_price: float,
    *,
    mae_sl: float = BASE_MAE_SL,
    mfe_tp: float = BASE_MFE_TP,
) -> float:
    """Path-dependent exit matching time_machine_backtester (SL / TP / TIME)."""
    if future is None or getattr(future, "empty", True) or entry_price <= 0:
        return 0.0
    final_ret = 0.0
    for _, f_row in future.iterrows():
        cur_mfe = (float(f_row["High"]) - entry_price) / entry_price * 100.0
        cur_mae = (float(f_row["Low"]) - entry_price) / entry_price * 100.0
        if cur_mae <= mae_sl:
            return float(mae_sl)
        if cur_mfe >= mfe_tp:
            return float(mfe_tp)
    final_ret = (float(future.iloc[-1]["Close"]) - entry_price) / entry_price * 100.0
    return float(final_ret)


def replay_trade_exit(
    df: pd.DataFrame,
    entry_date: str,
    *,
    mae_sl: float,
    hold_bars: int = BASE_HOLD_BARS,
    mfe_tp: float = BASE_MFE_TP,
) -> Optional[Dict[str, float]]:
    """Recompute mfe/mae/final_ret from entry_date using OHLCV (no re-match)."""
    if df is None or getattr(df, "empty", True):
        return None
    work = df.sort_index()
    if work.index.has_duplicates:
        work = work[~work.index.duplicated(keep="last")]
    if not isinstance(work.index, pd.DatetimeIndex):
        work.index = pd.to_datetime(work.index)

    entry_ts = pd.Timestamp(str(entry_date)[:10])
    # Entry bar = matching day; forward path = next hold_bars sessions
    loc = work.index.get_indexer([entry_ts], method="pad")
    if loc is None or len(loc) == 0 or int(loc[0]) < 0:
        return None
    i = int(loc[0])
    if work.index[i] > entry_ts + pd.Timedelta(days=3):
        return None
    if i + hold_bars >= len(work):
        return None

    entry_price = float(work.iloc[i]["Close"])
    future = work.iloc[i + 1 : i + 1 + hold_bars]
    if len(future) < hold_bars:
        return None
    max_high = float(future["High"].max())
    min_low = float(future["Low"].min())
    mfe = (max_high - entry_price) / entry_price * 100.0
    mae = (min_low - entry_price) / entry_price * 100.0
    final_ret = simulate_exit_on_bars(future, entry_price, mae_sl=mae_sl, mfe_tp=mfe_tp)
    return {"mfe": mfe, "mae": mae, "final_ret": final_ret}


def patch_matrix_sideways_exits(
    matrix: Dict[str, Dict[str, Any]],
    *,
    mae_sl: Optional[float] = None,
    hold_bars: int = BASE_HOLD_BARS,
) -> Dict[str, Any]:
    """In-place: replay exit on SIDEWAYS window trades; leave BULL/BEAR untouched."""
    from rp1_ohlcv_cache import fetch_ohlcv_cached
    from time_machine_backtester import compute_rp1_global_ohlcv_bounds

    if not resolve_side_alpha_01_exit():
        return {"applied": False, "reason": "SIDE_ALPHA_01_EXIT off"}

    sl = float(mae_sl if mae_sl is not None else resolve_sideways_mae_sl())
    side_keys = sideways_window_keys()
    fetch_start, fetch_end = compute_rp1_global_ohlcv_bounds()

    codes: Set[str] = set()
    for wkey in side_keys:
        bucket = matrix.get(wkey) or {}
        for t in bucket.get("trades") or []:
            c = str(t.get("code") or "")
            if c:
                codes.add(c)

    ohlcv_by_code: Dict[str, pd.DataFrame] = {}
    fetch_fail = 0
    for code in sorted(codes):
        df, _gate = fetch_ohlcv_cached(code, fetch_start, fetch_end)
        if df is None or getattr(df, "empty", True):
            fetch_fail += 1
            continue
        ohlcv_by_code[code] = df

    changed = 0
    unchanged = 0
    missing_ohlcv = 0
    replay_fail = 0
    windows_patched: List[str] = []

    for wkey in sorted(side_keys):
        bucket = matrix.get(wkey)
        if not isinstance(bucket, dict):
            continue
        trades = bucket.get("trades") or []
        if not trades:
            continue
        windows_patched.append(wkey)
        new_trades: List[Dict[str, Any]] = []
        for t in trades:
            row = dict(t)
            code = str(row.get("code") or "")
            entry = str(row.get("date") or "")
            df = ohlcv_by_code.get(code)
            if df is None:
                missing_ohlcv += 1
                new_trades.append(row)
                continue
            replayed = replay_trade_exit(
                df, entry, mae_sl=sl, hold_bars=hold_bars, mfe_tp=BASE_MFE_TP
            )
            if replayed is None:
                replay_fail += 1
                new_trades.append(row)
                continue
            old = float(row.get("final_ret") or 0.0)
            row["mfe"] = replayed["mfe"]
            row["mae"] = replayed["mae"]
            row["final_ret"] = replayed["final_ret"]
            row["_side_alpha_01_exit"] = True
            row["_mae_sl"] = sl
            if abs(old - replayed["final_ret"]) > 1e-9:
                changed += 1
            else:
                unchanged += 1
            new_trades.append(row)
        bucket["trades"] = new_trades

    return {
        "applied": True,
        "lever": "SIDEWAYS_MAE_SL",
        "mae_sl_before": BASE_MAE_SL,
        "mae_sl_after": sl,
        "hold_bars": hold_bars,
        "mfe_tp": BASE_MFE_TP,
        "windows_patched": windows_patched,
        "codes_loaded": len(ohlcv_by_code),
        "codes_fetch_fail": fetch_fail,
        "trades_changed": changed,
        "trades_unchanged": unchanged,
        "trades_missing_ohlcv": missing_ohlcv,
        "trades_replay_fail": replay_fail,
    }

"""
BTC-proxy correlation / same-side concentration gate.

Invariants:
  - Proxy only: pearson(corr(symbol, BTC)) ≥ τ → high-β (no N×N VaR)
  - Cap: sum of high-β same-side OPEN notional / portfolio NAV
  - Block new high-β entries only when cluster already at/over cap
  - Never auto-flatten; soft-pass when series too short / missing
"""
from __future__ import annotations

import time
from typing import Any, Optional

import numpy as np
import pandas as pd

from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import (
    CORR_BENCH_SYMBOL,
    CORR_BENCH_TF,
    CORR_BTC_MIN,
    CORR_BTC_MIN_OVERLAP,
    CORR_BTC_WINDOW,
    CORR_CLUSTER_MAX_PCT,
    GATES_BREADTH_BENCH_BAR_LIMIT,
)

logger = get_logger("bitget.trading.concentration_gate")

# (mono_ts, Close series indexed by Date)
_BTC_CLOSE_CACHE: tuple[float, pd.Series] | None = None
_BTC_CACHE_TTL_SEC = 300.0


def ohlcv_symbol_key(symbol: str) -> str:
    """Normalize exchange / ledger symbols to BITGET_*_BTC_USDT_1D table key."""
    s = str(symbol or "").strip().upper().replace("-", "_").replace("/", "_")
    s = s.replace(":USDT", "")
    if s.endswith("_USDTUSDT"):
        s = s[: -len("USDT")]
    if not s.endswith("_USDT") and s.endswith("USDT") and "_" not in s:
        s = s[:-4] + "_USDT"
    return s


def _cfg_float(cfg: dict, key: str, default: float) -> float:
    try:
        raw = (cfg or {}).get(key, default)
        if raw is None or raw == "":
            return float(default)
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _table_name(market_type: str, symbol: str, timeframe: str) -> str:
    prefix = "SPOT" if str(market_type).lower() == "spot" else "FUT"
    return f"BITGET_{prefix}_{ohlcv_symbol_key(symbol)}_{timeframe}"


def _load_close_series(
    conn,
    *,
    symbol: str,
    market_type: str,
    timeframe: str,
    limit: int,
) -> Optional[pd.Series]:
    import memory_bounds

    tbl = _table_name(market_type, symbol, timeframe)
    tail = memory_bounds.ohlcv_limit_sql(bar_limit=int(limit))
    try:
        df = pd.read_sql(f'SELECT Date, Close FROM "{tbl}"{tail}', conn)
    except Exception:
        return None
    if df is None or len(df) < 5:
        return None
    df = df.sort_values("Date")
    df["Date"] = pd.to_datetime(df["Date"])
    s = pd.to_numeric(df["Close"], errors="coerce")
    s.index = df["Date"]
    s = s.dropna()
    return s if len(s) >= 5 else None


def _btc_close_series(conn) -> Optional[pd.Series]:
    global _BTC_CLOSE_CACHE
    now = time.monotonic()
    if _BTC_CLOSE_CACHE is not None:
        ts, ser = _BTC_CLOSE_CACHE
        if now - ts < _BTC_CACHE_TTL_SEC and ser is not None and len(ser) >= 5:
            return ser
    lim = max(int(GATES_BREADTH_BENCH_BAR_LIMIT), int(CORR_BTC_WINDOW) + 10)
    for mkt in ("futures", "spot"):
        ser = _load_close_series(
            conn,
            symbol=CORR_BENCH_SYMBOL,
            market_type=mkt,
            timeframe=CORR_BENCH_TF,
            limit=lim,
        )
        if ser is not None and len(ser) >= int(CORR_BTC_MIN_OVERLAP):
            _BTC_CLOSE_CACHE = (now, ser)
            return ser
    return None


def corr_vs_btc(
    conn,
    *,
    symbol: str,
    market_type: str,
    window: int = CORR_BTC_WINDOW,
    min_overlap: int = CORR_BTC_MIN_OVERLAP,
) -> Optional[float]:
    """Pearson corr of daily returns vs BTC. None = insufficient data (soft-pass)."""
    key = ohlcv_symbol_key(symbol)
    if key == ohlcv_symbol_key(CORR_BENCH_SYMBOL):
        return 1.0
    btc = _btc_close_series(conn)
    if btc is None:
        return None
    lim = max(int(window) + 10, int(GATES_BREADTH_BENCH_BAR_LIMIT))
    sym = _load_close_series(
        conn,
        symbol=key,
        market_type=market_type,
        timeframe=CORR_BENCH_TF,
        limit=lim,
    )
    if sym is None:
        # try alternate market for OHLCV
        alt = "spot" if str(market_type).lower() != "spot" else "futures"
        sym = _load_close_series(
            conn,
            symbol=key,
            market_type=alt,
            timeframe=CORR_BENCH_TF,
            limit=lim,
        )
    if sym is None:
        return None
    joined = pd.concat([btc.rename("btc"), sym.rename("sym")], axis=1, join="inner").dropna()
    if len(joined) < int(min_overlap):
        return None
    ret = joined.pct_change().dropna()
    if len(ret) < int(min_overlap):
        return None
    ret = ret.tail(int(window))
    if len(ret) < int(min_overlap):
        return None
    try:
        c = float(ret["sym"].corr(ret["btc"]))
    except Exception:
        return None
    if c != c:  # NaN
        return None
    return float(np.clip(c, -1.0, 1.0))


def load_open_concentration_book() -> list[dict[str, Any]]:
    try:
        from bitget.infra.bounded_reads import forward_open_concentration_book_sql
        from bitget.infra.data_paths import market_data_db_path
        from bitget.infra.shared_db_connector import get_connection

        q, p = forward_open_concentration_book_sql()
        conn = get_connection(market_data_db_path())
        try:
            rows = conn.execute(q, p).fetchall()
        finally:
            conn.close()
        out: list[dict[str, Any]] = []
        for r in rows:
            if hasattr(r, "keys"):
                sym, mkt, side, inv = r["symbol"], r["market_type"], r["position_side"], r["sim_kelly_invest"]
            else:
                sym, mkt, side, inv = r[0], r[1], r[2], r[3]
            try:
                notion = float(inv or 0.0)
            except (TypeError, ValueError):
                notion = 0.0
            out.append(
                {
                    "symbol": str(sym or ""),
                    "market_type": str(mkt or "futures").lower(),
                    "position_side": str(side or "LONG").upper(),
                    "sim_kelly_invest": notion,
                }
            )
        return out
    except Exception as e:
        logger.debug("concentration book load failed: %s", e)
        return []


def high_beta_same_side_notional(
    cfg: dict,
    *,
    position_side: str,
    book: Optional[list[dict[str, Any]]] = None,
) -> tuple[float, dict[str, Any]]:
    """Sum sim_kelly_invest of OPEN rows on side with corr(BTC) ≥ threshold."""
    side = str(position_side or "LONG").upper()
    thr = _cfg_float(cfg, "CORR_BTC_MIN", CORR_BTC_MIN)
    window = int(_cfg_float(cfg, "CORR_BTC_WINDOW", float(CORR_BTC_WINDOW)))
    min_ov = int(_cfg_float(cfg, "CORR_BTC_MIN_OVERLAP", float(CORR_BTC_MIN_OVERLAP)))
    rows = book if book is not None else load_open_concentration_book()
    meta: dict[str, Any] = {"corr_btc_min": thr, "side": side, "members": []}
    if not rows:
        return 0.0, meta

    try:
        from bitget.infra.data_paths import market_data_db_path
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(market_data_db_path())
    except Exception as e:
        meta["error"] = str(e)[:120]
        return 0.0, meta

    total = 0.0
    try:
        for row in rows:
            if str(row.get("position_side") or "LONG").upper() != side:
                continue
            notion = float(row.get("sim_kelly_invest") or 0.0)
            if notion <= 0:
                continue
            c = corr_vs_btc(
                conn,
                symbol=str(row.get("symbol") or ""),
                market_type=str(row.get("market_type") or "futures"),
                window=window,
                min_overlap=min_ov,
            )
            if c is None:
                continue  # soft-skip unknown
            if c >= thr:
                total += notion
                if len(meta["members"]) < 12:
                    meta["members"].append(
                        f"{row.get('symbol')} corr={c:.2f} n={notion:.0f}"
                    )
    finally:
        try:
            conn.close()
        except Exception:
            pass
    meta["cluster_usdt"] = round(total, 4)
    return total, meta


def candidate_is_high_beta(
    cfg: dict,
    *,
    symbol: str,
    market_type: str,
) -> tuple[Optional[bool], Optional[float]]:
    """(True/False/None, corr). None = insufficient data → soft-pass caller."""
    thr = _cfg_float(cfg, "CORR_BTC_MIN", CORR_BTC_MIN)
    window = int(_cfg_float(cfg, "CORR_BTC_WINDOW", float(CORR_BTC_WINDOW)))
    min_ov = int(_cfg_float(cfg, "CORR_BTC_MIN_OVERLAP", float(CORR_BTC_MIN_OVERLAP)))
    try:
        from bitget.infra.data_paths import market_data_db_path
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(market_data_db_path())
        try:
            c = corr_vs_btc(
                conn,
                symbol=symbol,
                market_type=market_type,
                window=window,
                min_overlap=min_ov,
            )
        finally:
            conn.close()
    except Exception:
        return None, None
    if c is None:
        return None, None
    return bool(c >= thr), c


def concentration_entry_blocked(
    cfg: dict,
    *,
    symbol: str,
    position_side: str,
    market_type: str = "futures",
) -> tuple[bool, dict[str, Any]]:
    """
    True → block new entry.
    High-β candidate + same-side high-β cluster ≥ CORR_CLUSTER_MAX_PCT of NAV.
    """
    max_pct = _cfg_float(cfg, "CORR_CLUSTER_MAX_PCT", CORR_CLUSTER_MAX_PCT)
    if max_pct <= 0:
        return False, {"concentration_gate": "disabled", "corr_cluster_max_pct": max_pct}

    is_hb, corr = candidate_is_high_beta(cfg, symbol=symbol, market_type=market_type)
    meta: dict[str, Any] = {
        "corr_cluster_max_pct": max_pct,
        "candidate_symbol": ohlcv_symbol_key(symbol),
        "candidate_side": str(position_side or "LONG").upper(),
        "candidate_corr_btc": corr,
        "candidate_high_beta": is_hb,
    }
    if is_hb is None:
        meta["concentration_gate"] = "soft_pass_insufficient_ohlcv"
        return False, meta
    if not is_hb:
        meta["concentration_gate"] = "ok_low_beta"
        return False, meta

    cluster, cmeta = high_beta_same_side_notional(cfg, position_side=position_side)
    meta.update(cmeta)

    try:
        from bitget.live_nav_manager import portfolio_nav_snapshot

        nav = float((portfolio_nav_snapshot() or {}).get("nav") or 0.0)
    except Exception:
        nav = 0.0
    meta["nav"] = nav
    if nav <= 0:
        meta["concentration_gate"] = "soft_pass_nav_unavailable"
        return False, meta

    cluster_pct = cluster / nav * 100.0
    meta["cluster_pct"] = round(cluster_pct, 4)
    if cluster_pct >= max_pct:
        meta["concentration_gate"] = "block"
        return True, meta
    meta["concentration_gate"] = "ok"
    return False, meta

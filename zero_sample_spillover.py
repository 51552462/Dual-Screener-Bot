"""
Zero-Sample US Spillover — forward_trades 없을 때 OHLCV 비지도 다크호스 섹터 추론.

DBSCAN(가능 시) 또는 섹터별 거래대금 급등 랭킹으로 US_SPILLOVER_* · CROSS_MARKET_SSOT 유지.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from sector_spillover_refresh import map_standard_sector


def _db_path() -> str:
    from market_db_paths import MARKET_DATA_DB_PATH

    return MARKET_DATA_DB_PATH


def _load_universe_sectors() -> pd.DataFrame:
    try:
        from us_list_survival import collect_us_list_survival

        udf, _ = collect_us_list_survival()
        if udf is None or udf.empty:
            return pd.DataFrame()
        cols = [c for c in ("Code", "Symbol", "Sector", "Industry") if c in udf.columns]
        return udf[cols].copy()
    except Exception:
        return pd.DataFrame()


def _ticker_features(
    conn: sqlite3.Connection,
    table: str,
    *,
    lookback: int = 5,
) -> Optional[Dict[str, float]]:
    try:
        df = pd.read_sql(
            f'SELECT Date, Close, Volume FROM "{table}" ORDER BY Date DESC LIMIT ?',
            conn,
            params=(lookback + 2,),
        )
    except sqlite3.Error:
        return None
    if df is None or len(df) < 3:
        return None
    df = df.sort_values("Date")
    close = pd.to_numeric(df["Close"], errors="coerce")
    vol = pd.to_numeric(df["Volume"], errors="coerce")
    if close.isna().all():
        return None
    c0 = float(close.iloc[-1])
    c1 = float(close.iloc[-2])
    if c0 <= 0 or c1 <= 0:
        return None
    ret_1d = (c0 / c1) - 1.0
    dv = c0 * float(vol.iloc[-1] or 0)
    dv_prev = c1 * float(vol.iloc[-2] or 0) if len(vol) >= 2 else dv
    dv_ratio = dv / max(1.0, dv_prev)
    vol_z = float(vol.iloc[-1] or 0) / max(1.0, float(vol.mean() or 1))
    return {
        "ret_1d": ret_1d,
        "dv_ratio": dv_ratio,
        "dollar_vol": dv,
        "vol_z": vol_z,
    }


def _dbscan_dark_horse_labels(X: np.ndarray) -> np.ndarray:
    try:
        from sklearn.cluster import DBSCAN
        from sklearn.preprocessing import StandardScaler

        if len(X) < 8:
            return np.zeros(len(X), dtype=int)
        Xs = StandardScaler().fit_transform(X)
        labels = DBSCAN(eps=0.85, min_samples=3).fit_predict(Xs)
        return labels
    except Exception:
        return np.zeros(len(X), dtype=int)


def infer_dark_horse_sector_from_ohlcv(
    *,
    max_tickers: int = 120,
    min_dollar_vol: float = 5e7,
) -> Dict[str, Any]:
    """
    US_* 테이블 스캔 → 섹터별 다크호스 스코어.
    """
    out: Dict[str, Any] = {
        "ok": False,
        "sector": "",
        "sector_std": "",
        "confidence": 0.0,
        "method": "",
        "n_tickers": 0,
        "reason": "",
    }
    db = _db_path()
    if not os.path.isfile(db):
        out["reason"] = "no_db"
        return out

    uni = _load_universe_sectors()
    code_to_sector: Dict[str, str] = {}
    if not uni.empty:
        for _, row in uni.iterrows():
            sym = str(row.get("Symbol") or row.get("Code") or "").strip().upper()
            sec = str(row.get("Sector") or row.get("Industry") or "Unknown").strip()
            if sym:
                code_to_sector[sym] = sec

    rows: List[Dict[str, Any]] = []
    try:
        conn = sqlite3.connect(db, timeout=60)
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'US_%'"
            ).fetchall()
            names = [
                t[0]
                for t in tables
                if t[0] not in ("US_SPY", "US_QQQ", "US_VIX")
            ][: max_tickers * 3]
            for tbl in names:
                sym = tbl[3:].replace("_", ".")
                feats = _ticker_features(conn, tbl)
                if not feats or feats["dollar_vol"] < min_dollar_vol:
                    continue
                sector_raw = code_to_sector.get(sym, code_to_sector.get(sym.replace("-", "."), "Unknown"))
                rows.append(
                    {
                        "symbol": sym,
                        "sector": sector_raw,
                        "sector_std": map_standard_sector(sector_raw),
                        **feats,
                    }
                )
                if len(rows) >= max_tickers:
                    break
        finally:
            conn.close()
    except Exception as ex:
        out["reason"] = f"scan_error:{ex}"
        return out

    out["n_tickers"] = len(rows)
    if len(rows) < 5:
        out["reason"] = "insufficient_tickers"
        return out

    df = pd.DataFrame(rows)
    df["score"] = (
        df["ret_1d"].clip(-0.15, 0.25) * 2.0
        + np.log1p(df["dv_ratio"].clip(0.5, 10)) * 1.5
        + np.log1p(df["vol_z"].clip(0.5, 20)) * 0.5
    )

    X = df[["ret_1d", "dv_ratio", "vol_z"]].to_numpy(dtype=float)
    labels = _dbscan_dark_horse_labels(X)
    df["cluster"] = labels

    method = "sector_aggregate"
    if (labels >= 0).sum() and len(set(labels[labels >= 0])) > 1:
        method = "dbscan+sector"
        # 노이즈(-1) 제외, 클러스터별 평균 score 최대
        valid = df[df["cluster"] >= 0]
        if not valid.empty:
            best_cluster = valid.groupby("cluster")["score"].mean().idxmax()
            df = valid[valid["cluster"] == best_cluster]

    sector_scores = (
        df.groupby("sector_std")
        .agg(
            score=("score", "mean"),
            n=("symbol", "count"),
            dv=("dollar_vol", "sum"),
        )
        .sort_values("score", ascending=False)
    )
    if sector_scores.empty:
        out["reason"] = "no_sector_scores"
        return out

    top_std = str(sector_scores.index[0])
    top_row = sector_scores.iloc[0]
    conf = float(min(0.92, 0.45 + top_row["score"] * 0.08 + min(top_row["n"], 8) * 0.03))

    out.update(
        {
            "ok": True,
            "sector": top_std,
            "sector_std": top_std,
            "confidence": round(conf, 3),
            "method": method,
            "reason": "dark_horse_ok",
            "leaderboard": sector_scores.head(5).to_dict(),
        }
    )
    return out


def apply_zero_sample_spillover(
    cfg: Dict[str, Any],
    *,
    force_if_closed_zero: bool = True,
) -> Dict[str, Any]:
    """
    ledger US closed=0 이거나 refresh_us_spillover 실패 시 OHLCV 기반 스필오버 발행.
    """
    from config_manager import save_system_config

    result: Dict[str, Any] = {"applied": False, "reason": "skip"}
    closed = 0
    try:
        db = _db_path()
        if os.path.isfile(db):
            conn = sqlite3.connect(db, timeout=20)
            try:
                closed = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM forward_trades WHERE market='US' AND status LIKE 'CLOSED%'"
                    ).fetchone()[0]
                    or 0
                )
            finally:
                conn.close()
    except Exception:
        pass

    from sector_spillover_refresh import refresh_us_spillover_from_db

    spill = refresh_us_spillover_from_db(cfg)
    if spill.get("reason") == "ok" and not force_if_closed_zero:
        result["reason"] = "ledger_spillover_ok"
        return result
    if spill.get("reason") == "ok" and closed > 0:
        result["reason"] = "ledger_spillover_ok"
        return result

    dark = infer_dark_horse_sector_from_ohlcv()
    if not dark.get("ok"):
        result["reason"] = f"dark_horse_fail:{dark.get('reason')}"
        return result

    sector_s = str(dark.get("sector_std") or dark.get("sector") or "")
    as_of = datetime.now().strftime("%Y-%m-%d")
    cfg["US_SPILLOVER_SECTOR"] = sector_s
    cfg["US_SPILLOVER_SECTOR_LAST_GOOD"] = sector_s
    cfg["US_SPILLOVER_SECTOR_AS_OF"] = as_of
    cfg["US_ZERO_SAMPLE_SPILLOVER"] = {
        "method": dark.get("method"),
        "confidence": dark.get("confidence"),
        "n_tickers": dark.get("n_tickers"),
        "as_of": as_of,
    }
    try:
        save_system_config(cfg)
    except Exception:
        pass

    result.update(
        {
            "applied": True,
            "sector": sector_s,
            "confidence": dark.get("confidence"),
            "method": dark.get("method"),
            "reason": "zero_sample_dark_horse",
        }
    )
    return result


def publish_zero_sample_cross_market(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """CROSS_MARKET_SSOT 발행 — zero-sample 경로."""
    from config_manager import load_system_config
    from cross_market_ssot import MODE_US_ONLINE, publish_us_market_snapshot

    config = dict(cfg) if isinstance(cfg, dict) else (load_system_config() or {})
    apply_zero_sample_spillover(config, force_if_closed_zero=True)
    return publish_us_market_snapshot(cfg=config, source="zero_sample_ohlcv", save=True)

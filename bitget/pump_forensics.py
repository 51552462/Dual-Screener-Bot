"""
코인 급등(+20% 이상) 포렌식 부검 → PUMP_DNA 저장.
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import memory_bounds

from bitget.config_hub import load_config, save_config
from bitget.infra.bounded_reads import sqlite_bitget_ohlcv_1d_tables_sql
from bitget.infra.clock import utc_hm_key
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.gc_cycle import flush_gc, heavy_data_cycle
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import GC_AFTER_OHLCV_BATCH, OHLCV_FORENSICS_BAR_LIMIT
from bitget.infra.shared_db_connector import get_connection

DB_PATH = market_data_db_path()
logger = get_logger("bitget.pump_forensics")
PATTERN_KEYS = [
    "vol_compression",
    "ma_convergence",
    "narrow_range",
    "volume_dry_then_lift",
    "close_near_ma20",
    "higher_lows_tail",
    "pressed_under_prior_high",
]

PUMP_THRESHOLD_PCT = 20.0


def _extract_flags(ohlc: pd.DataFrame, t_idx: int) -> Optional[Dict[str, bool]]:
    if t_idx < 12:
        return None
    w = ohlc.iloc[t_idx - 10:t_idx - 1].copy()
    if len(w) < 8:
        return None
    close = ohlc["Close"].astype(float)
    vol = ohlc["Volume"].astype(float).replace(0, np.nan)
    ma5 = close.rolling(5, min_periods=3).mean()
    ma20 = close.rolling(20, min_periods=5).mean()

    base = ohlc.iloc[max(0, t_idx - 35):t_idx - 10]
    med_w = float(w["Volume"].median())
    med_b = float(base["Volume"].median()) if len(base) else med_w
    if not np.isfinite(med_b) or med_b <= 0:
        med_b = med_w if med_w > 0 else 1.0

    v_tm3 = float(vol.iloc[t_idx - 3]) if t_idx >= 3 else med_w
    v_tm2 = float(vol.iloc[t_idx - 2]) if t_idx >= 2 else med_w
    c_tm2 = float(close.iloc[t_idx - 2])
    ma20_tm2 = float(ma20.iloc[t_idx - 2]) if np.isfinite(ma20.iloc[t_idx - 2]) else c_tm2
    prior = ohlc.iloc[max(0, t_idx - 35):t_idx - 10]
    ph = float(prior["Close"].max()) if len(prior) else c_tm2

    rng = (w["High"].astype(float) - w["Low"].astype(float)) / w["Close"].astype(float).replace(0, np.nan)
    flags = {
        "vol_compression": med_w < med_b * 0.65,
        "ma_convergence": abs((float(ma5.iloc[t_idx - 2]) / ma20_tm2) - 1.0) < 0.025 if ma20_tm2 > 0 else False,
        "narrow_range": float((rng < 0.03).mean()) >= 0.35,
        "volume_dry_then_lift": (v_tm3 < med_w * 1.05) and (v_tm2 > v_tm3 * 1.15),
        "close_near_ma20": abs((c_tm2 / ma20_tm2) - 1.0) < 0.05 if ma20_tm2 > 0 else False,
        "higher_lows_tail": float(ohlc["Low"].iloc[t_idx - 2]) > float(ohlc["Low"].iloc[t_idx - 4]) > float(ohlc["Low"].iloc[t_idx - 6]),
        "pressed_under_prior_high": (ph > 0) and ((float(close.iloc[t_idx - 8]) / ph) < 0.94),
    }
    return flags


def run_pump_forensics() -> None:
    logger.info("[Pump Forensics] +20%% pump DNA reverse scan start")
    conn = get_connection(DB_PATH, read_only=True)
    tbl_sql, tbl_params = sqlite_bitget_ohlcv_1d_tables_sql(exclude_btc=False)
    tables = [r[0] for r in conn.execute(tbl_sql, tbl_params).fetchall()]

    rows: List[Dict[str, bool]] = []
    used_symbols: List[str] = []
    for tbl in tables:
        try:
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}"'
                f"{memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_FORENSICS_BAR_LIMIT)}",
                conn,
            )
            if not df.empty:
                df = df.sort_values("Date")
            if len(df) < 30:
                continue
            close = pd.to_numeric(df["Close"], errors="coerce")
            ret = (close / close.shift(1) - 1.0) * 100.0
            hits = ret[ret >= PUMP_THRESHOLD_PCT].index.tolist()
            if not hits:
                continue
            t_idx = int(hits[-1])
            flags = _extract_flags(df, t_idx)
            if not flags:
                continue
            rows.append(flags)
            sym = "_".join(tbl.split("_")[2:-1])
            used_symbols.append(sym)
            del df
            flush_gc(label=GC_AFTER_OHLCV_BATCH)
        except Exception as e:
            log_exception(logger, "pump forensics table skip %s: %s", tbl, e)
            continue
    conn.close()
    flush_gc(label="pump_forensics_scan")

    if not rows:
        logger.warning("pump forensics sample insufficient")
        return

    n = len(rows)
    hit_counts = {k: 0 for k in PATTERN_KEYS}
    for r in rows:
        for k in PATTERN_KEYS:
            if r.get(k):
                hit_counts[k] += 1
    threshold = max(1, int(np.ceil(0.7 * n)))
    rule = {k: hit_counts[k] >= threshold for k in PATTERN_KEYS}
    consensus_hits = sum(1 for v in rule.values() if v)

    payload = {
        "updated_at": utc_hm_key(),
        "pump_threshold_pct": PUMP_THRESHOLD_PCT,
        "samples_analyzed": n,
        "symbols_analyzed": used_symbols[:120],
        "pattern_hit_counts": hit_counts,
        "pre_emptive_rule": rule,
        "consensus_pattern_hits": consensus_hits,
        "consensus_met": consensus_hits >= 4,
    }

    cfg = load_config()
    dna = cfg.get("PUMP_DNA", {})
    if not isinstance(dna, dict):
        dna = {}
    dna["GLOBAL"] = payload
    dna["updated_at_global"] = utc_hm_key()
    cfg["PUMP_DNA"] = dna
    save_config(cfg)
    del rows
    flush_gc(label="pump_forensics_complete")
    logger.info(
        "PUMP_DNA saved (consensus %s/%s)",
        consensus_hits,
        len(PATTERN_KEYS),
    )


if __name__ == "__main__":
    run_pump_forensics()

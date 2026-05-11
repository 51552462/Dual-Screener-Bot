"""
코인 급등(+20% 이상) 포렌식 부검 → PUMP_DNA 저장.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")

PUMP_THRESHOLD_PCT = 20.0
PATTERN_KEYS = [
    "vol_compression",
    "ma_convergence",
    "narrow_range",
    "volume_dry_then_lift",
    "close_near_ma20",
    "higher_lows_tail",
    "pressed_under_prior_high",
]


def load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_config(cfg: Dict[str, Any]) -> bool:
    temp_path = f"{CONFIG_PATH}.temp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CONFIG_PATH)
        return True
    except Exception:
        return False


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
    print("🔬 [Bitget Pump Forensics] +20% 급등 코인 DNA 역추적...")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_%_1D'").fetchall()]

    rows: List[Dict[str, bool]] = []
    used_symbols: List[str] = []
    for tbl in tables:
        try:
            df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC', conn)
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
        except Exception:
            continue
    conn.close()

    if not rows:
        print("⚠️ 급등 포렌식 표본 부족.")
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
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
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
    dna["updated_at_global"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    cfg["PUMP_DNA"] = dna
    save_config(cfg)
    print(f"✅ PUMP_DNA 저장 완료 (합의 {consensus_hits}/{len(PATTERN_KEYS)})")


if __name__ == "__main__":
    run_pump_forensics()

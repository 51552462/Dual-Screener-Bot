"""
스캔 파이프라인 회복력 — ML/알파/지표 계산 실패 시 안전 기본값으로 폴백.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from pipeline_error_util import log_pipeline_exception

logger = logging.getLogger(__name__)

# DNA 3D 기본값 (유동성 통과 후 계산 실패 시 — 종목 전체 폐기 방지)
DEFAULT_CPV = 0.5
DEFAULT_TB = 1.0
DEFAULT_BBE = 1.0


def safe_last_series(ser: Any, default: float = float("nan")) -> float:
    if ser is None:
        return default
    try:
        if hasattr(ser, "iloc"):
            v = float(ser.iloc[-1])
        else:
            v = float(ser[-1])
        return v if np.isfinite(v) else default
    except Exception:
        return default


def safe_supernova_dna_features(
    df: pd.DataFrame,
    *,
    market: str,
    now_mkt: Any,
) -> Optional[Dict[str, float]]:
    """
    cpv / tb / bbe 산출. 실패 시 None(호출자가 폴백 dict 사용).
    """
    try:
        c = df["Close"].values
        o = df["Open"].values
        h = df["High"].values
        l = df["Low"].values
        v = df["Volume"].values
        v_ma20 = pd.Series(v).rolling(20).mean().values
        cpv = float(np.where(h != l, (c - o) / (h - l), 0.5)[-1])
        if not np.isfinite(cpv):
            cpv = DEFAULT_CPV

        open_h = 9
        open_m = 0 if str(market).upper() == "KR" else 30
        elapsed_mins = (now_mkt.hour - open_h) * 60 + (now_mkt.minute - open_m)
        elapsed_mins = max(1, min(390, int(elapsed_mins)))
        est_daily_volume = float(v[-1]) * (390.0 / elapsed_mins)
        denom = max(cpv, 0.01)
        tb = float(
            (est_daily_volume / v_ma20[-1]) / denom
            if v_ma20[-1] > 0
            else est_daily_volume / 0.01
        )
        if not np.isfinite(tb):
            tb = DEFAULT_TB

        bb_std = float(pd.Series(c).rolling(20).std().values[-1])
        bb_mid = float(pd.Series(c).rolling(20).mean().values[-1])
        bb_width = (4 * bb_std) / bb_mid if bb_mid > 0 else 0.01
        vol_mult = (
            (est_daily_volume / v_ma20[-1]) if v_ma20[-1] > 0 else 1.0
        )
        bbe = float((1.0 / bb_width) * vol_mult if bb_width > 0 else DEFAULT_BBE)
        if not np.isfinite(bbe):
            bbe = DEFAULT_BBE
        return {"cpv": cpv, "tb": tb, "bbe": bbe, "current_close": float(c[-1])}
    except Exception as ex:
        log_pipeline_exception(
            f"safe_supernova_dna_features market={market}",
            ex,
            component="supernova_scan",
        )
        return None


def fallback_dna_features(df: pd.DataFrame) -> Dict[str, float]:
    close = DEFAULT_CPV
    try:
        close = float(df["Close"].iloc[-1])
    except Exception:
        pass
    return {
        "cpv": DEFAULT_CPV,
        "tb": DEFAULT_TB,
        "bbe": DEFAULT_BBE,
        "current_close": close,
    }

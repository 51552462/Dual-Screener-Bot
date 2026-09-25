"""SUPERNOVA LIQ 앞문 NA 게이트 — pd.NA를 if에 넣지 않는다.

US-COSINE-NA-FIX-01. NA/nan은 0으로 채우지 않고 평가 불가로 명시 스킵.
"""
from __future__ import annotations

from typing import Any, Tuple

import numpy as np
import pandas as pd

EVAL_UNAVAILABLE = "EVAL_UNAVAILABLE"
LIQUIDITY = "LIQUIDITY"
PASS = "PASS"

VOL_WINDOW = 5
VOL_MAX_NA = 3  # 5봉 중 NA 3 이상 → 평가 불가
VOL_MIN_VALID = VOL_WINDOW - VOL_MAX_NA + 1  # 3

US_MIN_PX = 0.5
KR_MIN_PX = 1000.0
US_DOLLAR_FLOOR_LIVE = 30_000.0
KR_USD_FX = 1350.0


def is_missing_scalar(value: Any) -> bool:
    """스칼라 pd.NA / NaT / None / nan. `if value` 금지."""
    if value is None:
        return True
    if isinstance(
        value, (list, tuple, dict, np.ndarray, pd.Series, pd.Index, pd.DataFrame)
    ):
        return True
    try:
        flag = pd.isna(value)
    except (TypeError, ValueError):
        return True
    if isinstance(flag, (bool, np.bool_)):
        if bool(flag):
            return True
    else:
        try:
            if bool(np.any(flag)):
                return True
        except (TypeError, ValueError):
            return True
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return True
    return not bool(np.isfinite(parsed))


def finite_vol_tail(
    vol_values: Any, *, window: int = VOL_WINDOW
) -> Tuple[np.ndarray, int, int]:
    """마지막 window봉에서 유한 거래량만. 결측은 0으로 채우지 않는다."""
    if vol_values is None:
        return np.asarray([], dtype=float), 0, 0
    arr = np.asarray(vol_values, dtype=object).reshape(-1)
    if arr.size == 0:
        return np.asarray([], dtype=float), 0, 0
    tail = arr[-int(window) :] if arr.size >= int(window) else arr
    finite: list[float] = []
    for item in tail.tolist():
        if not is_missing_scalar(item):
            finite.append(float(item))
    n_win = int(len(tail))
    n_na = int(n_win - len(finite))
    return np.asarray(finite, dtype=float), n_na, n_win


def classify_liq_front_window(
    *,
    market: str,
    close_last: Any,
    vol_tail: Any,
    us_dollar_floor: float = US_DOLLAR_FLOOR_LIVE,
) -> Tuple[str, str]:
    """(reason, why). reason은 EVAL_UNAVAILABLE | LIQUIDITY | PASS."""
    if is_missing_scalar(close_last):
        return EVAL_UNAVAILABLE, "close_na"
    px = float(close_last)
    finite, n_na, n_win = finite_vol_tail(vol_tail)
    if n_win <= 0 or n_na >= VOL_MAX_NA or finite.size < VOL_MIN_VALID:
        return EVAL_UNAVAILABLE, "vol_na"
    mean_vol = float(np.mean(finite))
    mk = str(market or "").upper()
    if mk == "KR" and px < KR_MIN_PX:
        return LIQUIDITY, "kr_px"
    if mk == "US" and px < US_MIN_PX:
        return LIQUIDITY, "us_px"
    if mk == "US":
        min_vol = float(us_dollar_floor) / max(px, 0.01)
    else:
        min_vol = (float(us_dollar_floor) * KR_USD_FX) / max(px, 1.0)
    if mean_vol < min_vol:
        return LIQUIDITY, "vol_mean"
    return PASS, "ok"


def old_liq_front_raises_or_leaks(
    *,
    market: str,
    close_last: Any,
    vol_tail: Any,
    us_dollar_floor: float = US_DOLLAR_FLOOR_LIVE,
) -> str:
    """라이브 구경로 재현: 예외=UNEVAL_RAISE, nan 비교 누수=LEAK_PASS, 그외 LIQ/PASS."""
    mk = str(market or "").upper()
    try:
        current_close = close_last
        if mk == "KR" and current_close < KR_MIN_PX:
            return "LIQUIDITY"
        if mk == "US" and current_close < US_MIN_PX:
            return "LIQUIDITY"
        min_vol = 50_000.0
        if mk == "US":
            min_vol = max(
                2_000.0,
                300_000.0 / max(float(current_close), 0.01),
            )
        if np.mean(vol_tail) < min_vol:
            return "LIQUIDITY"
        return "PASS"
    except (TypeError, ValueError):
        return "UNEVAL_RAISE"

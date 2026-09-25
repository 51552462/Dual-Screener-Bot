"""US-COSINE-NA-FIX-01 — NA는 스킵, 0 채움 없음, LIQ와 평가불가 분리."""
from __future__ import annotations

import numpy as np
import pandas as pd

from scan_liq_front_gate import (
    EVAL_UNAVAILABLE,
    LIQUIDITY,
    PASS,
    classify_liq_front_window,
    is_missing_scalar,
    old_liq_front_raises_or_leaks,
)


def test_pd_na_is_missing_not_truthy():
    assert is_missing_scalar(pd.NA) is True
    assert is_missing_scalar(np.nan) is True
    assert is_missing_scalar(None) is True
    assert is_missing_scalar(12.3) is False
    assert is_missing_scalar(0.0) is False


def test_us_close_na_is_eval_not_liq():
    reason, why = classify_liq_front_window(
        market="US",
        close_last=pd.NA,
        vol_tail=np.array([1e6, 1e6, 1e6, 1e6, 1e6], dtype=object),
    )
    assert reason == EVAL_UNAVAILABLE
    assert why == "close_na"
    assert old_liq_front_raises_or_leaks(
        market="US",
        close_last=pd.NA,
        vol_tail=np.array([1e6] * 5, dtype=object),
    ) == "UNEVAL_RAISE"


def test_us_close_nan_does_not_leak_as_liq_pass():
    vol = np.array([1e6, 1e6, 1e6, 1e6, 1e6], dtype=float)
    reason, why = classify_liq_front_window(
        market="US", close_last=np.nan, vol_tail=vol
    )
    assert reason == EVAL_UNAVAILABLE
    assert why == "close_na"
    leaked = old_liq_front_raises_or_leaks(
        market="US", close_last=np.nan, vol_tail=vol
    )
    assert leaked == "PASS"


def test_vol_three_na_is_eval_not_filled():
    tail = np.array([1e6, 1e6, pd.NA, pd.NA, pd.NA], dtype=object)
    reason, why = classify_liq_front_window(
        market="US", close_last=10.0, vol_tail=tail
    )
    assert reason == EVAL_UNAVAILABLE
    assert why == "vol_na"


def test_vol_two_na_uses_finite_mean_only():
    tail = np.array([1e6, 1e6, 1e6, pd.NA, pd.NA], dtype=object)
    reason, why = classify_liq_front_window(
        market="US", close_last=10.0, vol_tail=tail
    )
    assert reason == PASS
    assert why == "ok"


def test_vol_zero_is_valid_not_na_fill():
    tail = np.array([0.0, 0.0, 0.0, 0.0, 0.0], dtype=float)
    reason, why = classify_liq_front_window(
        market="US", close_last=10.0, vol_tail=tail
    )
    assert reason == LIQUIDITY
    assert why == "vol_mean"


def test_penny_is_liq_not_eval():
    tail = np.array([1e6] * 5, dtype=float)
    reason, why = classify_liq_front_window(
        market="US", close_last=0.2, vol_tail=tail
    )
    assert reason == LIQUIDITY
    assert why == "us_px"


def test_us_30k_no_share_min():
    tail = np.array([12_000.0] * 5, dtype=float)
    reason, why = classify_liq_front_window(
        market="US", close_last=3.0, vol_tail=tail
    )
    assert reason == PASS
    assert why == "ok"
    thin = np.array([5_000.0] * 5, dtype=float)
    reason2, why2 = classify_liq_front_window(
        market="US", close_last=3.0, vol_tail=thin
    )
    assert reason2 == LIQUIDITY
    assert why2 == "vol_mean"


def test_kr_30k_fx_not_50k_shares():
    tail = np.array([40_000.0] * 5, dtype=float)
    reason, why = classify_liq_front_window(
        market="KR", close_last=50_000.0, vol_tail=tail
    )
    assert reason == PASS
    assert why == "ok"


def test_old_object_vol_na_raises():
    tail = np.array([1e6, 1e6, 1e6, 1e6, pd.NA], dtype=object)
    assert (
        old_liq_front_raises_or_leaks(
            market="US", close_last=10.0, vol_tail=tail
        )
        == "UNEVAL_RAISE"
    )
    reason, why = classify_liq_front_window(
        market="US", close_last=10.0, vol_tail=tail
    )
    assert reason == PASS
    assert why == "ok"

"""
TV_SHORT_V1/V2 연속 스코어링 회귀 테스트.

과거 버그: 두 엔진 모두 score=100.0 으로 시작해 도플갱어(±10/-30/0) 또는
Decision Tree 기각(0)만으로 값이 바뀌어, 사실상 0점 또는 100점 두 값만
나올 수 있었다(리포트의 "점수"가 항상 0 또는 100으로만 보이는 현상의 원인).

이 테스트는 (1) 합성 OHLCV로 실제 진입 조건을 트리거해 (2) score가 롱(MASTER)과
동형의 연속 스케일링(scale_score 가중합)으로 계산되어 0/100 이진값에 갇히지
않는지, (3) 롱과 동일한 dbg 필드(파생 점수·마켓캡·코사인 등)가 채워지는지,
(4) Thompson Kelly 네임스페이스가 숏 전용으로 분리되는지를 검증한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bitget.signal_engines import compute_tv_short_v1, compute_tv_short_v2
from bitget.forward.gates import _thompson_ns_prefix


def _build_trend_df(n_up: int, n_down: int, up_rate: float = 1.006, down_rate: float = 0.97) -> pd.DataFrame:
    n = n_up + n_down
    dates = pd.date_range("2023-01-01", periods=n, freq="D")
    prices = []
    p = 100.0
    for i in range(n):
        p *= up_rate if i < n_up else down_rate
        prices.append(p)
    close = np.array(prices)
    open_ = np.roll(close, 1)
    open_[0] = close[0]
    high = np.maximum(open_, close) * 1.003
    low = np.minimum(open_, close) * 0.997
    vol = np.full(n, 1_000_000.0)
    return pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": vol}, index=dates
    )


def _flat_benchmark(df: pd.DataFrame, level: float = 50000.0) -> pd.Series:
    return pd.Series(level, index=df.index)


@pytest.fixture
def short_v1_df() -> pd.DataFrame:
    """장기 상승 후 급락 → EMA160 하향 크로스(entry2) 트리거."""
    return _build_trend_df(n_up=200, n_down=13)


@pytest.fixture
def short_v2_df() -> pd.DataFrame:
    """장기 하락으로 전체 EMA 역배열 형성 + 마지막 2봉을 EMA120 재이탈 패턴으로 조정."""
    df = _build_trend_df(n_up=250, n_down=60)
    ema120_est = df["Close"].ewm(span=120, adjust=False, min_periods=0).mean().iloc[-3]
    df.iloc[-2, df.columns.get_loc("Open")] = ema120_est * 0.97
    df.iloc[-2, df.columns.get_loc("High")] = ema120_est * 1.02
    df.iloc[-2, df.columns.get_loc("Low")] = ema120_est * 0.90
    df.iloc[-2, df.columns.get_loc("Close")] = ema120_est * 0.93
    df.iloc[-1, df.columns.get_loc("Open")] = ema120_est * 0.92
    df.iloc[-1, df.columns.get_loc("High")] = ema120_est * 0.94
    df.iloc[-1, df.columns.get_loc("Low")] = ema120_est * 0.85
    df.iloc[-1, df.columns.get_loc("Close")] = ema120_est * 0.88
    return df


_LONG_PARITY_KEYS = (
    "dyn_rs_score",
    "dyn_cpv_score",
    "dyn_tb_score",
    "score_marcap",
    "marcap_tier",
    "trade_value_24h",
    "marcap_eok",
    "freq_count",
    "recommend",
    "sn_score",
    "tree_rejected",
    "tree_reason",
)


class TestTvShortV1Scoring:
    def test_hit_and_continuous_score(self, short_v1_df):
        hit, sig_type, out_df, dbg = compute_tv_short_v1(short_v1_df, _flat_benchmark(short_v1_df))
        assert hit is True
        assert sig_type == "[TV_SHORT_V1] SHORT"
        score = dbg["score"]
        assert isinstance(score, float)
        assert 0.0 <= score <= 100.0
        # 과거 버그 회귀 방지: Decision Tree 기각이 아니면 0/100 양극단에 갇히지 않아야 한다.
        assert dbg["tree_rejected"] is False
        assert score not in (0.0, 100.0)

    def test_dbg_has_long_parity_fields(self, short_v1_df):
        hit, _, _, dbg = compute_tv_short_v1(short_v1_df, _flat_benchmark(short_v1_df))
        assert hit is True
        for key in _LONG_PARITY_KEYS:
            assert key in dbg, f"missing long-parity field: {key}"

    def test_timeframe_param_accepted(self, short_v1_df):
        hit, _, _, dbg = compute_tv_short_v1(short_v1_df, _flat_benchmark(short_v1_df), "4H")
        assert hit is True
        assert isinstance(dbg["dyn_rs_score"], float)


class TestTvShortV2Scoring:
    def test_hit_and_continuous_score(self, short_v2_df):
        hit, sig_type, out_df, dbg = compute_tv_short_v2(short_v2_df, _flat_benchmark(short_v2_df))
        assert hit is True
        assert sig_type == "[TV_SHORT_V2] SHORT"
        score = dbg["score"]
        assert isinstance(score, float)
        assert 0.0 <= score <= 100.0
        assert dbg["tree_rejected"] is False
        assert score not in (0.0, 100.0)

    def test_dbg_has_long_parity_fields(self, short_v2_df):
        hit, _, _, dbg = compute_tv_short_v2(short_v2_df, _flat_benchmark(short_v2_df))
        assert hit is True
        for key in _LONG_PARITY_KEYS:
            assert key in dbg, f"missing long-parity field: {key}"


class TestShortVsLongScoresDiffer:
    def test_v1_and_v2_scores_are_not_forced_equal(self, short_v1_df, short_v2_df):
        """서로 다른 합성 데이터가 서로 다른 점수를 내야 한다(고정 상수 버그 회귀 방지)."""
        _, _, _, dbg1 = compute_tv_short_v1(short_v1_df, _flat_benchmark(short_v1_df))
        _, _, _, dbg2 = compute_tv_short_v2(short_v2_df, _flat_benchmark(short_v2_df))
        assert dbg1["score"] != dbg2["score"]


class TestThompsonNamespaceIsolation:
    @pytest.mark.parametrize(
        "sig_type,expected_suffix",
        [
            ("[STANDARD][TV_SHORT_V1] [TV_SHORT_V1] SHORT", "TV_SHORT_V1"),
            ("[STANDARD][TV_SHORT_V2] [TV_SHORT_V2] SHORT", "TV_SHORT_V2"),
        ],
    )
    def test_short_sig_types_get_dedicated_namespace(self, sig_type, expected_suffix):
        ns = _thompson_ns_prefix("1D", sig_type)
        assert ns == f"1D_{expected_suffix}"

    def test_long_master_s1_namespace_unaffected(self):
        ns = _thompson_ns_prefix("1D", "[1D] \U0001f525 S1 (\ub9c8\uc2a4\ud0c0 \ucd94\uc138)")
        assert ns == "1D_MASTER_S1"

    def test_short_and_long_namespaces_never_collide(self):
        short_ns = _thompson_ns_prefix("1D", "[STANDARD][TV_SHORT_V1] [TV_SHORT_V1] SHORT")
        long_ns = _thompson_ns_prefix("1D", "[1D] S1 master")
        assert short_ns != long_ns

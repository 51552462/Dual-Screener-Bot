"""DNA 플래그(is_tenbagger/is_top_dna/is_worst_dna/is_death_combo) 코인 이식 회귀 테스트.

과거 상태: 스키마엔 컬럼이 있었지만 코인 시그널 엔진 어디서도 계산해서 채우지
않아 항상 0으로 고정되어 있었다(한국/미국 스캐너는 계산해서 채움). 이 테스트는
(1) 순수 함수 _compute_dna_flags 의 경계 동작, (2) 실제 엔진(SHORT V1/V2, 합성
OHLCV로 트리거 가능)이 dbg 에 4개 플래그를 채우는지, (3) ledger.py 의 INSERT
컬럼/파라미터 순서가 어긋나지 않는지를 검증한다.
"""
from __future__ import annotations

import os
import re
import sqlite3
import tempfile

import numpy as np
import pandas as pd
import pytest

from bitget.signal_engines import _compute_dna_flags, compute_tv_short_v1, compute_tv_short_v2


class TestComputeDnaFlags:
    def test_top_dna_requires_all_three_high(self):
        flags = _compute_dna_flags(dyn_rs_score=9.0, dyn_cpv_score=9.0, dyn_tb_score=9.0, score_bbe=5.0, cur_rs=100.0)
        assert flags["is_top_dna"] is True

    def test_top_dna_false_if_one_dimension_weak(self):
        flags = _compute_dna_flags(dyn_rs_score=9.0, dyn_cpv_score=9.0, dyn_tb_score=3.0, score_bbe=5.0, cur_rs=100.0)
        assert flags["is_top_dna"] is False

    def test_worst_dna_requires_cpv_tb_bbe_all_bad(self):
        flags = _compute_dna_flags(dyn_rs_score=5.0, dyn_cpv_score=1.0, dyn_tb_score=1.0, score_bbe=1.0, cur_rs=-10.0)
        assert flags["is_worst_dna"] is True

    def test_death_combo_long_needs_negative_rs(self):
        flags_bad_rs = _compute_dna_flags(dyn_rs_score=5.0, dyn_cpv_score=1.0, dyn_tb_score=5.0, score_bbe=5.0, cur_rs=-5.0, short=False)
        assert flags_bad_rs["is_death_combo"] is True
        flags_good_rs = _compute_dna_flags(dyn_rs_score=5.0, dyn_cpv_score=1.0, dyn_tb_score=5.0, score_bbe=5.0, cur_rs=5.0, short=False)
        assert flags_good_rs["is_death_combo"] is False

    def test_death_combo_short_is_mirrored(self):
        """숏은 롱과 반대로 RS > 0(벤치마크 대비 여전히 강세)일 때가 '추세 역행'이다."""
        flags_bad_rs = _compute_dna_flags(dyn_rs_score=5.0, dyn_cpv_score=1.0, dyn_tb_score=5.0, score_bbe=5.0, cur_rs=5.0, short=True)
        assert flags_bad_rs["is_death_combo"] is True
        flags_good_rs = _compute_dna_flags(dyn_rs_score=5.0, dyn_cpv_score=1.0, dyn_tb_score=5.0, score_bbe=5.0, cur_rs=-5.0, short=True)
        assert flags_good_rs["is_death_combo"] is False

    def test_tenbagger_needs_extreme_rs_and_good_cpv(self):
        flags = _compute_dna_flags(dyn_rs_score=9.5, dyn_cpv_score=9.0, dyn_tb_score=5.0, score_bbe=5.0, cur_rs=100.0)
        assert flags["is_tenbagger"] is True
        flags_weak = _compute_dna_flags(dyn_rs_score=8.7, dyn_cpv_score=9.0, dyn_tb_score=5.0, score_bbe=5.0, cur_rs=100.0)
        assert flags_weak["is_tenbagger"] is False

    def test_all_flags_are_plain_bool(self):
        flags = _compute_dna_flags(dyn_rs_score=9.0, dyn_cpv_score=9.0, dyn_tb_score=9.0, score_bbe=9.0, cur_rs=1.0)
        for key in ("is_top_dna", "is_worst_dna", "is_death_combo", "is_tenbagger"):
            assert isinstance(flags[key], bool)


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
    return pd.DataFrame({"Open": open_, "High": high, "Low": low, "Close": close, "Volume": vol}, index=dates)


def _flat_benchmark(df: pd.DataFrame, level: float = 50000.0) -> pd.Series:
    return pd.Series(level, index=df.index)


_DNA_KEYS = ("is_top_dna", "is_worst_dna", "is_death_combo", "is_tenbagger")


class TestEngineDnaFlagsPresent:
    def test_short_v1_dbg_has_dna_flags(self):
        df = _build_trend_df(n_up=200, n_down=13)
        hit, _, _, dbg = compute_tv_short_v1(df, _flat_benchmark(df))
        assert hit is True
        for key in _DNA_KEYS:
            assert key in dbg
            assert isinstance(dbg[key], bool)

    def test_short_v2_dbg_has_dna_flags(self):
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
        hit, _, _, dbg = compute_tv_short_v2(df, _flat_benchmark(df))
        assert hit is True
        for key in _DNA_KEYS:
            assert key in dbg
            assert isinstance(dbg[key], bool)


class TestLedgerInsertColumnAlignment:
    def test_insert_statement_columns_and_placeholders_match(self):
        """컬럼 목록·VALUES(?) 개수·파라미터 튜플 길이가 정확히 일치하는지, 그리고
        is_tenbagger/is_top_dna/is_worst_dna/is_death_combo 가 올바른 위치에
        기록되는지 실제 스키마에 INSERT 해서 검증한다."""
        from bitget.forward import shared

        with open(os.path.join("bitget", "forward", "ledger.py"), encoding="utf-8") as f:
            src = f.read()
        m = re.search(r"(INSERT INTO bitget_forward_trades.*?VALUES \([^)]*\))", src, re.DOTALL)
        assert m is not None, "INSERT 문을 찾지 못함 (ledger.py 리팩터링 여부 확인 필요)"
        sql = m.group(1)
        cols_blob = re.search(r"\((.*?)\)\s*VALUES", sql, re.DOTALL).group(1)
        cols = [c.strip() for c in cols_blob.split(",")]
        n_placeholders = sql.count("?")
        assert len(cols) == n_placeholders, "컬럼 수와 VALUES(?) 개수가 어긋남"

        for flag in ("is_tenbagger", "is_top_dna", "is_worst_dna", "is_death_combo"):
            assert flag in cols, f"{flag} 가 INSERT 컬럼 목록에서 빠짐"

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.sqlite")
            conn = sqlite3.connect(db_path)
            shared._init_forward_db_schema(conn)
            conn.commit()
            params = tuple(range(1, len(cols) + 1))
            conn.execute(sql, params)
            conn.commit()
            row = dict(
                zip(
                    ("is_tenbagger", "is_top_dna", "is_worst_dna", "is_death_combo"),
                    conn.execute(
                        "SELECT is_tenbagger, is_top_dna, is_worst_dna, is_death_combo FROM bitget_forward_trades"
                    ).fetchone(),
                )
            )
            conn.close()

        expected = {flag: cols.index(flag) + 1 for flag in row}
        assert row == expected, f"컬럼 위치가 파라미터 위치와 어긋남: {row} vs {expected}"


class TestMasterScannerFactsPassthrough:
    def test_process_scan_hit_facts_includes_dna_flags(self):
        import inspect

        from bitget import master_scanner

        src = inspect.getsource(master_scanner._process_scan_hit)
        for key in ("is_top_dna", "is_worst_dna", "is_death_combo", "is_tenbagger"):
            assert f'"{key}"' in src, f"facts 딕셔너리에 {key} 전달 누락"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

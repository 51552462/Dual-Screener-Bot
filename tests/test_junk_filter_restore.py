"""3중 junk/ETF 필터 복구 — krx_equity_universe · reject_kr_virtual_entry."""
from __future__ import annotations

import pandas as pd

from krx_equity_universe import (
    DEFAULT_JUNK_NAME_PATTERN,
    filter_krx_equity_universe,
    reject_kr_virtual_entry,
)


def test_junk_name_pattern_excludes_kodex():
    raw = pd.DataFrame(
        [
            {"Code": "069500", "Name": "KODEX 200", "Market": "KOSPI", "MarketId": "STK"},
            {"Code": "005930", "Name": "삼성전자", "Market": "KOSPI", "MarketId": "STK"},
        ]
    )
    out = filter_krx_equity_universe(
        raw, derivative_exclude=frozenset(), junk_pattern=DEFAULT_JUNK_NAME_PATTERN
    )
    assert set(out["Code"].tolist()) == {"005930"}


def test_tier3_style_code_only_name_filtered_by_derivative_set():
    raw = pd.DataFrame(
        [
            {"Code": "252670", "Name": "252670", "Market": "KRX", "Marcap": 0.0},
            {"Code": "005930", "Name": "005930", "Market": "KRX", "Marcap": 0.0},
        ]
    )
    out = filter_krx_equity_universe(
        raw,
        derivative_exclude=frozenset({"252670"}),
        junk_pattern=DEFAULT_JUNK_NAME_PATTERN,
    )
    assert set(out["Code"].tolist()) == {"005930"}


def test_reject_kr_virtual_entry_blocks_junk_and_penny():
    ok, _ = reject_kr_virtual_entry("069500", "KODEX 200", entry_price=50000)
    assert not ok

    ok2, _ = reject_kr_virtual_entry("005935", "삼성전자우", entry_price=50000)
    assert not ok2

    ok3, _ = reject_kr_virtual_entry("123456", "테스트", entry_price=500)
    assert not ok3

    ok4, reason4 = reject_kr_virtual_entry("005930", "삼성전자", entry_price=70000)
    assert ok4 and reason4 == ""

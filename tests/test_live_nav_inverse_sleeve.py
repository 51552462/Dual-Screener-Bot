"""live_nav_manager — inverse sleeve RL rolling log tests."""
import json
import os

import live_nav_manager as lnm


def test_record_and_stats_profitable(tmp_path, monkeypatch):
    path = tmp_path / "treasury_state.json"
    monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(path))

    lnm.record_inverse_sleeve_closure(
        "US",
        final_ret_pct=4.5,
        invest_amount=10000.0,
        exit_date="2026-07-08",
        sig_type="Dante[INVERSE_ETF]",
    )
    stats = lnm.get_inverse_sleeve_rl_stats("US")
    assert stats["n_closed"] == 1
    assert stats["verdict"] == "profitable"
    assert stats["weighted_ret_pct"] == 4.5
    assert stats["total_net_pnl_abs"] == 450.0


def test_record_whipsaw_verdict(tmp_path, monkeypatch):
    path = tmp_path / "treasury_state.json"
    monkeypatch.setattr(lnm, "treasury_state_path", lambda: str(path))

    lnm.record_inverse_sleeve_closure(
        "KR",
        final_ret_pct=-2.0,
        invest_amount=5000000.0,
        exit_date="2026-07-08",
    )
    stats = lnm.get_inverse_sleeve_rl_stats("KR")
    assert stats["verdict"] == "whipsaw"
    assert stats["total_net_pnl_abs"] < 0


def test_is_inverse_trade_sig():
    assert lnm.is_inverse_trade_sig("Dante[INVERSE_ETF]")
    assert not lnm.is_inverse_trade_sig("SUPERNOVA RANK_A")

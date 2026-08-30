"""FULL-BT-FUT-DEFCON-1 — Adapter A 3중 게이팅 (harness-only bypass)."""
from __future__ import annotations

from bitget.full_bt.defcon_bypass import (
    should_bypass_fullbt_doomsday,
    wrap_doomsday_long_entry_blocked,
)


def _real_block(_cfg, *, position_side="LONG"):
    side = str(position_side or "LONG").upper()
    if side != "LONG":
        return False, {"doomsday_gate": "ok_short_allowed"}
    return True, {"doomsday_gate": "block", "defcon_level": 1}


def test_bypass_requires_all_three_and_fut(tmp_path, monkeypatch):
    monkeypatch.setenv("FULLBT_DEFCON_BYPASS_ENABLED", "true")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    assert (
        should_bypass_fullbt_doomsday(
            {
                "isolated": True,
                "full_bt_db_path": full,
                "ledger_db_path": full,
                "market_type": "futures",
            }
        )
        is True
    )


def test_kill_switch_off_blocks_bypass(tmp_path, monkeypatch):
    monkeypatch.setenv("FULLBT_DEFCON_BYPASS_ENABLED", "false")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    assert (
        should_bypass_fullbt_doomsday(
            {
                "isolated": True,
                "full_bt_db_path": full,
                "ledger_db_path": full,
                "market_type": "futures",
            }
        )
        is False
    )


def test_not_isolated_blocks_bypass(tmp_path, monkeypatch):
    monkeypatch.setenv("FULLBT_DEFCON_BYPASS_ENABLED", "true")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    assert (
        should_bypass_fullbt_doomsday(
            {
                "isolated": False,
                "full_bt_db_path": full,
                "ledger_db_path": full,
                "market_type": "futures",
            }
        )
        is False
    )


def test_wrong_db_blocks_bypass(tmp_path, monkeypatch):
    monkeypatch.setenv("FULLBT_DEFCON_BYPASS_ENABLED", "true")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    prod = str(tmp_path / "bitget_market_data.sqlite")
    assert (
        should_bypass_fullbt_doomsday(
            {
                "isolated": True,
                "full_bt_db_path": full,
                "ledger_db_path": prod,
                "market_type": "futures",
            }
        )
        is False
    )


def test_spot_market_type_no_bypass(tmp_path, monkeypatch):
    monkeypatch.setenv("FULLBT_DEFCON_BYPASS_ENABLED", "true")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    assert (
        should_bypass_fullbt_doomsday(
            {
                "isolated": True,
                "full_bt_db_path": full,
                "ledger_db_path": full,
                "market_type": "spot",
            }
        )
        is False
    )


def test_wrap_bypasses_when_context_ok(tmp_path, monkeypatch):
    monkeypatch.setenv("FULLBT_DEFCON_BYPASS_ENABLED", "true")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    seen = []

    def provider():
        return {
            "isolated": True,
            "full_bt_db_path": full,
            "ledger_db_path": full,
            "market_type": "futures",
            "on_bypass": lambda m: seen.append(m),
        }

    wrapped = wrap_doomsday_long_entry_blocked(_real_block, context_provider=provider)
    ok_block, meta = wrapped({}, position_side="LONG")
    assert ok_block is False
    assert meta.get("defcon_bypassed") is True
    assert meta.get("iv_note") == "IV L1 참고용"
    assert len(seen) == 1


def test_wrap_keeps_block_when_kill_off(tmp_path, monkeypatch):
    monkeypatch.setenv("FULLBT_DEFCON_BYPASS_ENABLED", "false")
    full = str(tmp_path / "bitget_full_bt.sqlite")

    def provider():
        return {
            "isolated": True,
            "full_bt_db_path": full,
            "ledger_db_path": full,
            "market_type": "futures",
        }

    wrapped = wrap_doomsday_long_entry_blocked(_real_block, context_provider=provider)
    blocked, meta = wrapped({}, position_side="LONG")
    assert blocked is True
    assert meta.get("defcon_bypassed") is not True

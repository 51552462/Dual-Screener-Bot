"""S5-HARNESS-SCOPE-01 — read-only contribution log."""
from __future__ import annotations

import inspect
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from meta_governor_consumer import resolve_defense_arm_weight
from reports.s5_defense_contribution import (
    compute_s5_defense_contribution_log,
    write_s5_contribution_json,
)


def _fwd_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY,
            market TEXT,
            code TEXT,
            sig_type TEXT,
            status TEXT,
            entry_date TEXT,
            exit_date TEXT,
            entry_regime TEXT,
            final_ret REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE meta_state_log (
            id INTEGER PRIMARY KEY,
            scope TEXT,
            state_json TEXT,
            updated_at_utc TEXT,
            regime_key TEXT
        )
        """
    )
    conn.commit()


def _short_schema(conn: sqlite3.Connection, *, with_pnl: bool = False) -> None:
    extra = ", final_ret REAL" if with_pnl else ""
    conn.execute(
        f"""
        CREATE TABLE short_forward_trades (
            id INTEGER PRIMARY KEY,
            entry_date TEXT,
            market TEXT,
            code TEXT,
            status TEXT,
            matched_pattern TEXT
            {extra}
        )
        """
    )
    conn.commit()


def _insert_fwd(conn, **kw) -> None:
    conn.execute(
        """
        INSERT INTO forward_trades
        (market, code, sig_type, status, entry_date, exit_date, entry_regime, final_ret)
        VALUES (:market, :code, :sig_type, :status, :entry_date, :exit_date, :entry_regime, :final_ret)
        """,
        kw,
    )
    conn.commit()


def test_compute_source_has_no_writes():
    src = inspect.getsource(compute_s5_defense_contribution_log)
    for token in ("INSERT ", "UPDATE ", "CREATE TABLE", "ALTER TABLE", "config_kv"):
        assert token not in src


def test_n_lt_20_sample_flag_no_judgment(tmp_path: Path):
    fwd = tmp_path / "market_data.sqlite"
    short = tmp_path / "short_data.sqlite"
    with sqlite3.connect(fwd) as c:
        _fwd_schema(c)
        c.execute(
            "INSERT INTO meta_state_log (scope, state_json, updated_at_utc, regime_key) VALUES (?,?,?,?)",
            ("GLOBAL", "{}", "2026-08-17T00:00:00+00:00", "BEAR"),
        )
        _insert_fwd(
            c,
            market="KR",
            code="114800",
            sig_type="Dante[INVERSE_ETF]",
            status="CLOSED_WIN",
            entry_date="2026-08-17",
            exit_date="2026-08-17",
            entry_regime="BEAR",
            final_ret=1.5,
        )
        c.commit()
    with sqlite3.connect(short) as c:
        _short_schema(c)

    with patch(
        "reports.s5_defense_contribution.resolve_defense_arm_weight",
        wraps=resolve_defense_arm_weight,
    ) as spy:
        out = compute_s5_defense_contribution_log(
            "2026-08-17",
            "2026-08-17",
            market="KR",
            forward_db_path=str(fwd),
            short_db_path=str(short),
            sys_config={"ENABLE_S5_REGIME_GATE": True},
        )
        assert spy.called

    win = out["windows"][0]
    assert win["s5_trade_count"] == 1
    assert win["sample_insufficient"] is True
    assert "표본 부족" in win["notes"]
    blob = json.dumps(out, ensure_ascii=False).lower()
    assert "pass" not in blob
    assert "fail" not in blob
    assert "cagr" not in blob
    assert "period_return_pct" not in blob
    assert out.get("verdict") is None


def test_bull_inverse_excluded_bear_included(tmp_path: Path):
    fwd = tmp_path / "market_data.sqlite"
    short = tmp_path / "short_data.sqlite"
    with sqlite3.connect(fwd) as c:
        _fwd_schema(c)
        _insert_fwd(
            c,
            market="US",
            code="SH",
            sig_type="Dante[INVERSE_ETF]",
            status="CLOSED_LOSS",
            entry_date="2026-08-17",
            exit_date="2026-08-17",
            entry_regime="BULL",
            final_ret=-2.0,
        )
        _insert_fwd(
            c,
            market="US",
            code="SDS",
            sig_type="Dante[INVERSE_ETF]",
            status="CLOSED_WIN",
            entry_date="2026-08-17",
            exit_date="2026-08-17",
            entry_regime="HIGH_VOL",
            final_ret=3.0,
        )
        c.commit()
    with sqlite3.connect(short) as c:
        _short_schema(c)

    out = compute_s5_defense_contribution_log(
        "2026-08-17",
        "2026-08-17",
        market="US",
        forward_db_path=str(fwd),
        short_db_path=str(short),
        sys_config={"ENABLE_S5_REGIME_GATE": True},
    )
    win = out["windows"][0]
    assert win["s5_trade_count"] == 1
    assert win["realized_pnl_sum"] == pytest.approx(3.0)
    assert win["contributed"] is True


def test_market_none_branches_kr_and_us(tmp_path: Path):
    fwd = tmp_path / "market_data.sqlite"
    short = tmp_path / "short_data.sqlite"
    with sqlite3.connect(fwd) as c:
        _fwd_schema(c)
        _insert_fwd(
            c,
            market="KR",
            code="A",
            sig_type="FOO_BLACKHOLE",
            status="CLOSED_WIN",
            entry_date="2026-08-17",
            exit_date="2026-08-17",
            entry_regime="BEAR",
            final_ret=0.5,
        )
        c.commit()
    with sqlite3.connect(short) as c:
        _short_schema(c)

    out = compute_s5_defense_contribution_log(
        "2026-08-17",
        "2026-08-17",
        market=None,
        forward_db_path=str(fwd),
        short_db_path=str(short),
        sys_config={},
    )
    assert [w["market"] for w in out["windows"]] == ["KR", "US"]
    assert out["windows"][0]["s5_trade_count"] == 1
    assert out["windows"][1]["s5_trade_count"] == 0


def test_write_json_artifact(tmp_path: Path):
    payload = {
        "sub_phase": "S5-HARNESS-SCOPE-01",
        "windows": [{"s5_trade_count": 0, "sample_insufficient": True, "notes": ["표본 부족"]}],
        "numeric_judgment_omitted": True,
    }
    path = write_s5_contribution_json(payload, as_of="20260817", out_dir=str(tmp_path))
    assert Path(path).name == "s5_contribution_20260817.json"
    loaded = json.loads(Path(path).read_text(encoding="utf-8"))
    assert loaded["sub_phase"] == "S5-HARNESS-SCOPE-01"

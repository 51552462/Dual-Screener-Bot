"""B1-LADDER-R1a-FASTCHECK — read-only weekly R1a verdict."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

from bitget.infra import ops_logger
from bitget.observability import b1_ladder_fastcheck_bg as bg


def _ensure_ops_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_utc TEXT NOT NULL,
                component TEXT NOT NULL,
                severity TEXT NOT NULL,
                event TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_forward_schema(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bitget_forward_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_date TEXT,
                exit_date TEXT,
                market_type TEXT,
                symbol TEXT,
                status TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _insert_trade(
    db_path: str,
    *,
    market_type: str,
    status: str,
    entry_date: str,
    exit_date: str | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO bitget_forward_trades (
                entry_date, exit_date, market_type, symbol, status
            ) VALUES (?,?,?,?,?)
            """,
            (entry_date, exit_date, market_type, "BTC_USDT", status),
        )
        conn.commit()
    finally:
        conn.close()


def test_pass_when_open_gt_zero(tmp_path, monkeypatch):
    forward_db = str(tmp_path / "market.sqlite")
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_forward_schema(forward_db)
    _ensure_ops_schema(ops_db)
    _insert_trade(
        forward_db,
        market_type="futures",
        status="OPEN",
        entry_date="2026-08-27",
    )
    monkeypatch.setattr(
        bg,
        "_blocked_short_by_mt",
        lambda **_k: {"SPOT": 0, "FUT": 0},
    )
    monkeypatch.setattr(bg, "_prior_week_payload", lambda *_a, **_k: None)

    by_mt = bg.compute_b1_ladder_fastcheck_bg(
        window_days=7,
        forward_db_path=forward_db,
        ops_db_path=ops_db,
        now=datetime(2026, 8, 28, tzinfo=timezone.utc),
    )
    assert by_mt["FUT"]["open_count"] == 1
    assert by_mt["FUT"]["verdict"] == "PASS"
    assert by_mt["SPOT"]["verdict"] == "관측유지"
    assert set(by_mt.keys()) == {"SPOT", "FUT"}


def test_fail_a_after_four_weeks_open_zero(tmp_path, monkeypatch):
    forward_db = str(tmp_path / "market.sqlite")
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_forward_schema(forward_db)
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(
        bg,
        "_blocked_short_by_mt",
        lambda **_k: {"SPOT": 0, "FUT": 0},
    )
    monkeypatch.setattr(bg, "_prior_week_payload", lambda *_a, **_k: None)

    # R0=2026-08-23 → +28d = 2026-09-20
    by_mt = bg.compute_b1_ladder_fastcheck_bg(
        window_days=7,
        forward_db_path=forward_db,
        ops_db_path=ops_db,
        now=datetime(2026, 9, 20, tzinfo=timezone.utc),
    )
    assert by_mt["SPOT"]["open_count"] == 0
    assert by_mt["SPOT"]["days_since_r0"] >= 28
    assert by_mt["SPOT"]["verdict"] == "FAIL(a)"
    assert by_mt["FUT"]["verdict"] == "FAIL(a)"


def test_fail_b_when_blocked_repeats(tmp_path, monkeypatch):
    forward_db = str(tmp_path / "market.sqlite")
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_forward_schema(forward_db)
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(
        bg,
        "_blocked_short_by_mt",
        lambda **_k: {"SPOT": 0, "FUT": 5},
    )
    monkeypatch.setattr(
        bg,
        "_prior_week_payload",
        lambda mt, **_k: (
            {"open_count": 0, "blocked_short_total": 3} if mt == "FUT" else None
        ),
    )

    by_mt = bg.compute_b1_ladder_fastcheck_bg(
        window_days=7,
        forward_db_path=forward_db,
        ops_db_path=ops_db,
        now=datetime(2026, 8, 28, tzinfo=timezone.utc),  # < 4 weeks
    )
    assert by_mt["FUT"]["verdict"] == "FAIL(b)"
    assert by_mt["SPOT"]["verdict"] == "관측유지"  # no prior / blocked=0


def test_closed_weekly_delta_and_pace_flag(tmp_path, monkeypatch):
    forward_db = str(tmp_path / "market.sqlite")
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_forward_schema(forward_db)
    _ensure_ops_schema(ops_db)
    today = datetime(2026, 8, 28, tzinfo=timezone.utc)
    recent = (today - timedelta(days=2)).strftime("%Y-%m-%d")
    old = (today - timedelta(days=20)).strftime("%Y-%m-%d")
    _insert_trade(
        forward_db,
        market_type="spot",
        status="CLOSED_WIN",
        entry_date=recent,
        exit_date=recent,
    )
    _insert_trade(
        forward_db,
        market_type="spot",
        status="CLOSED_LOSS",
        entry_date=old,
        exit_date=old,
    )
    monkeypatch.setattr(
        bg,
        "_blocked_short_by_mt",
        lambda **_k: {"SPOT": 0, "FUT": 0},
    )
    monkeypatch.setattr(bg, "_prior_week_payload", lambda *_a, **_k: None)

    by_mt = bg.compute_b1_ladder_fastcheck_bg(
        window_days=7,
        forward_db_path=forward_db,
        ops_db_path=ops_db,
        now=today,
    )
    assert by_mt["SPOT"]["closed_weekly_delta"] == 1
    assert by_mt["SPOT"]["r6_pace_flag"] == "페이스부족"  # 1/7*56 ≈ 8 < 30


def test_kill_switch_skips_job(monkeypatch):
    monkeypatch.setattr(bg, "b1_ladder_fastcheck_enabled", lambda: False)
    assert bg.run_b1_ladder_fastcheck_job() is None


def test_persist_one_event_per_mt(tmp_path, monkeypatch):
    ops_db = str(tmp_path / "ops.sqlite")
    _ensure_ops_schema(ops_db)
    monkeypatch.setattr(ops_logger, "ops_events_db_path", lambda: ops_db)
    monkeypatch.setenv("BITGET_OPS_EVENTS_DB", ops_db)

    # Patch insert to write local sqlite if needed
    inserted = []

    def _fake_insert(*, component, severity, event, payload):
        inserted.append((event, payload.get("market_type")))
        conn = sqlite3.connect(ops_db)
        try:
            conn.execute(
                "INSERT INTO ops_events (ts_utc, component, severity, event, payload_json) "
                "VALUES (?,?,?,?,?)",
                ("2026-08-28T00:00:00+00:00", component, severity, event, json.dumps(payload)),
            )
            conn.commit()
        finally:
            conn.close()
        return True

    monkeypatch.setattr(ops_logger, "insert_ops_event", _fake_insert)
    # Also patch the import path used inside persist
    monkeypatch.setattr(
        "bitget.infra.ops_logger.insert_ops_event",
        _fake_insert,
    )

    by_mt = {
        "SPOT": {
            "open_count": 0,
            "closed_weekly_delta": 0,
            "blocked_short_total": 0,
            "r6_pace_flag": "페이스부족",
            "verdict": "관측유지",
        },
        "FUT": {
            "open_count": 0,
            "closed_weekly_delta": 0,
            "blocked_short_total": 0,
            "r6_pace_flag": "페이스부족",
            "verdict": "관측유지",
        },
    }
    results = bg.persist_b1_ladder_fastcheck_weekly(by_mt, ops_db_path=ops_db)
    assert results["SPOT"] is True
    assert results["FUT"] is True
    assert sorted(m for _, m in inserted) == ["FUT", "SPOT"]
    assert all(e == "b1_ladder_fastcheck_weekly" for e, _ in inserted)


def test_weekly_pipeline_includes_fastcheck():
    from bitget.pipelines import bitget_pipelines as bp

    steps = bp._pipeline_weekly_evolution()
    names = [s.name for s in steps]
    assert "b1_ladder_fastcheck" in names
    assert names.index("b1_ladder_fastcheck") > names.index("gmm_dna_alpha_report")

"""Proprietary Regime — friction store & weekly PRI."""
from proprietary_friction_store import (
    ensure_proprietary_friction_schema,
    insert_regime_friction_event,
    insert_scan_funnel_snapshot,
)
from weekly_proprietary_regime import (
    _decay_z,
    build_weekly_shadow_pri_html,
    compute_weekly_proprietary_regime,
    internal_ledger_volatility_proxy,
)


def test_decay_z_cold_start():
    assert _decay_z(2.0, 0) == 0.0
    assert _decay_z(2.0, 1) == 0.0
    z_partial = _decay_z(2.0, 3)
    assert 0 < abs(z_partial) < 2.0
    assert _decay_z(2.0, 10) == 2.0


def test_friction_schema_and_insert(tmp_path):
    db = tmp_path / "t.sqlite"
    ensure_proprietary_friction_schema(db_path=str(db))
    insert_scan_funnel_snapshot(
        ts="2026-06-10 10:00",
        market="US",
        universe_size=5000,
        survivors=3,
        pass_rate_pct=0.06,
        db_path=str(db),
    )
    insert_regime_friction_event(
        date="2026-06-10",
        market="US",
        event_type="DM_A_ZERO_CLOSED",
        db_path=str(db),
    )
    import sqlite3

    conn = sqlite3.connect(str(db))
    n1 = conn.execute("SELECT COUNT(*) FROM scan_funnel_snapshot").fetchone()[0]
    n2 = conn.execute("SELECT COUNT(*) FROM regime_friction_event").fetchone()[0]
    conn.close()
    assert n1 == 1
    assert n2 == 1


def test_weekly_pri_empty_db(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "weekly_proprietary_regime._db_path",
        lambda: str(tmp_path / "missing.sqlite"),
    )
    out = compute_weekly_proprietary_regime(
        week_start="2026-06-09", week_end="2026-06-13"
    )
    assert out.get("shadow_mode") is True
    assert out.get("markets")
    for mk in ("KR", "US"):
        assert out["markets"][mk]["cold_start"] is True
        assert out["markets"][mk]["regime"] == "SIDEWAYS"


def test_shadow_html_has_header():
    html = build_weekly_shadow_pri_html(
        week_start="2026-06-09",
        week_end="2026-06-13",
    )
    assert "Shadow PRI" in html or "스킵" in html

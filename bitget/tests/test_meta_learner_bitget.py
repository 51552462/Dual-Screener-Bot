"""bitget.meta_learner_bg — PRI↔REGIME trust matrix Clock SSOT."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest import mock

import pytest


def test_meta_learner_bg_module_uses_clock_ssot():
    import inspect

    from bitget import meta_learner_bg as ml

    src = inspect.getsource(ml)
    assert "datetime.utcnow()" not in src
    assert "datetime.now()" not in src
    assert "utc_now" in src
    assert "utc_datetime_str" in src
    assert "parse_utc_iso" in src


def test_save_trust_matrix_stamps_utc(tmp_path, monkeypatch):
    from bitget import meta_learner_bg as ml

    path = tmp_path / "trust.json"
    monkeypatch.setattr(ml, "trust_matrix_path", lambda: str(path))
    fixed = datetime(2026, 7, 11, 3, 30, 0, tzinfo=timezone.utc)
    with mock.patch("bitget.meta_learner_bg.utc_datetime_str", return_value="2026-07-11 03:30:00"):
        assert ml.save_trust_matrix(ml.load_trust_matrix()) is True
    import json

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["updated_at"] == "2026-07-11 03:30:00"


def test_divergence_maturity_uses_utc_anchor(tmp_path, monkeypatch):
    import sqlite3

    from bitget import meta_learner_bg as ml

    db_path = tmp_path / "forward.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            entry_date TEXT, exit_date TEXT, status TEXT, final_ret REAL, sig_type TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO bitget_forward_trades VALUES ('2026-07-01','2026-07-08','CLOSED_TP', 2.5, 'ENGINE1')"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(ml, "trust_matrix_path", lambda: str(tmp_path / "trust.json"))
    monkeypatch.setattr(ml, "DB_PATH", str(db_path))
    anchor = datetime(2026, 7, 11, 12, 0, 0, tzinfo=timezone.utc)
    state = ml.load_trust_matrix()
    state["pending_events"] = [
        {
            "date": "2026-07-01",
            "internal_dir": "UP",
            "external_dir": "DOWN",
            "external_key": "BEAR",
            "internal_z": 0.5,
        }
    ]
    ml.save_trust_matrix(state)
    with mock.patch("bitget.meta_learner_bg.utc_now", return_value=anchor):
        out = ml.run_bitget_meta_learning_cycle(meta={"META_REGIME_KEY": "BEAR"}, now=anchor)
    assert out["ok"] is True
    assert out["evaluated"] == 1

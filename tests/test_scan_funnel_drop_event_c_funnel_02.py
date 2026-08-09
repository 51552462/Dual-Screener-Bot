"""C-FUNNEL-02 — scan funnel snapshot 회귀 + near-miss drop events."""
from __future__ import annotations

import json
import sqlite3
from unittest import mock

import pytest

from proprietary_friction_store import (
    ensure_proprietary_friction_schema,
    insert_scan_funnel_drop_events,
    insert_scan_funnel_snapshot,
)
from scanner_funnel import NEAR_MISS_CAP_PER_SLOT, ScanFunnelTracker


def test_snapshot_insert_and_drops_json_roundtrip(tmp_path):
    db = tmp_path / "f.sqlite"
    ensure_proprietary_friction_schema(db_path=str(db))
    drops = {"DNA_FAIL": 12, "LIQUIDITY": 3}
    insert_scan_funnel_snapshot(
        ts="2026-08-09 10:00",
        market="KR",
        universe_size=100,
        survivors=2,
        pass_rate_pct=2.0,
        scanner="SUPERNOVA",
        drops_json=json.dumps(drops),
        db_path=str(db),
    )
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT scanner, drops_json FROM scan_funnel_snapshot"
    ).fetchone()
    conn.close()
    assert row[0] == "SUPERNOVA"
    assert json.loads(row[1]) == drops


def test_drop_events_batch_insert(tmp_path):
    db = tmp_path / "e.sqlite"
    ensure_proprietary_friction_schema(db_path=str(db))
    insert_scan_funnel_drop_events(
        [
            {
                "ts": "2026-08-09T01:00:00Z",
                "market": "US",
                "scanner": "SUPERNOVA",
                "code": "AAPL",
                "reason": "DNA_FAIL",
                "final_score": 0.48,
                "eff_cos_cutoff": 0.50,
                "eff_ml_cutoff": 0.55,
                "regime_key": "BULL",
                "rank_in_slot": 1,
            }
        ],
        db_path=str(db),
    )
    conn = sqlite3.connect(str(db))
    n = conn.execute("SELECT COUNT(*) FROM scan_funnel_drop_event").fetchone()[0]
    conn.close()
    assert n == 1


def test_near_miss_cap_and_ranking(tmp_path):
    funnel = ScanFunnelTracker(
        scanner_id="SN",
        market="KR",
        universe_size=200,
        profile="SUPERNOVA",
    )
    # 55 candidates — only 50 kept; closest to cutoff (0.50) should rank first
    for i in range(55):
        score = 0.50 + (i * 0.001)
        funnel.drop(
            "DNA_FAIL",
            code=f"C{i:03d}",
            final_score=score,
            eff_cos_cutoff=0.50,
        )
    with mock.patch(
        "scanner_funnel.read_current_regime_key_for_funnel",
        return_value="SIDEWAYS",
    ), mock.patch(
        "proprietary_friction_store.insert_scan_funnel_snapshot"
    ), mock.patch(
        "proprietary_friction_store.insert_scan_funnel_drop_events"
    ) as mock_events:
        funnel.finalize()
        assert mock_events.called
        rows = mock_events.call_args[0][0]
    assert len(rows) == NEAR_MISS_CAP_PER_SLOT
    assert rows[0]["rank_in_slot"] == 1
    assert rows[0]["final_score"] == pytest.approx(0.50)
    assert rows[0]["regime_key"] == "SIDEWAYS"


def test_drop_without_optional_kwargs_regression():
    funnel = ScanFunnelTracker(
        scanner_id="SN",
        market="US",
        universe_size=10,
        profile="SUPERNOVA",
    )
    funnel.drop("DATA_FAIL", 3)
    with mock.patch(
        "proprietary_friction_store.insert_scan_funnel_snapshot"
    ), mock.patch(
        "proprietary_friction_store.insert_scan_funnel_drop_events"
    ) as mock_events:
        report = funnel.finalize()
    assert ("DATA_FAIL", 3) in report.drop_summary
    mock_events.assert_not_called()


def test_regime_read_once_per_finalize(tmp_path):
    funnel = ScanFunnelTracker(
        scanner_id="SN",
        market="KR",
        universe_size=5,
        profile="SUPERNOVA",
    )
    funnel.drop(
        "LIQUIDITY",
        code="005930",
        final_score=0.1,
        eff_cos_cutoff=0.5,
    )
    with mock.patch(
        "scanner_funnel.read_current_regime_key_for_funnel",
        return_value="BEAR",
    ) as mock_regime, mock.patch(
        "proprietary_friction_store.insert_scan_funnel_snapshot"
    ), mock.patch(
        "proprietary_friction_store.insert_scan_funnel_drop_events"
    ) as mock_events:
        funnel.finalize()
    mock_regime.assert_called_once_with("KR")
    assert mock_events.call_args[0][0][0]["regime_key"] == "BEAR"

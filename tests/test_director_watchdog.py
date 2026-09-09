"""DIRECTOR-WATCHDOG-01 — display-only North Star daily panel."""
from __future__ import annotations

import ast
import inspect
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from family_sleeve_demote import FAMILY_SLEEVE_DEMOTE_KEYS, FAMILY_SLEEVE_DEMOTE_MULT
from reports.director_watchdog import (
    NAV_HOOK_FAIL_EVENT,
    build_director_watchdog_payload,
    count_nav_hook_failures_24h,
    format_director_watchdog_section_from_snap,
    inspect_demote_effective,
    leaderboard_core_group,
    previous_budget_bands_from_history,
    read_iv_observation_latest,
)
from dual_north_star_telegram import format_north_star_digest_html


def _snap(**kw):
    return {
        "cadence": kw.get("cadence", "daily"),
        "date_kst": kw.get("date_kst", "2026-09-09"),
        "tracks": {
            "A": {
                "label": "주식 KR+US",
                "phase_label": "운영",
                "mdd_cap_pct": 10,
                "cagr_target_lo": 40,
                "cagr_target_hi": 70,
                "cagr_year1_checkpoint_lo": 20,
                "cagr_year1_checkpoint_hi": 30,
                "available": True,
                "forward_book": {
                    "open_total": 0,
                    "closed_total": 10,
                    "open_by_market": {},
                    "closed_by_market": {"KR": 5, "US": 5},
                    "ok": True,
                },
                "forward_trades_count": 10,
                "markets": {
                    "KR": {
                        "nav": 1,
                        "mdd_pct": 2.0,
                        "return_pct": -1.0,
                        "budget_band": kw.get("kr_band", "LOCKDOWN"),
                        "exhaustion_pct": 95.0,
                        "n_closed": 5,
                    },
                    "US": {
                        "nav": 1,
                        "mdd_pct": 4.0,
                        "return_pct": 0.5,
                        "budget_band": kw.get("us_band", "NORMAL"),
                        "exhaustion_pct": 10.0,
                        "n_closed": 5,
                    },
                },
                "aggregate": {
                    "composite_score": 4.0,
                    "return_pace_score": 10,
                    "max_mdd_pct": 4.0,
                },
            }
        },
        "ledger": {"A": {"gate": "G0", "gate_label": ""}},
        "meta": {
            "daily_n": 8,
            "obs_hold_recall_n": 20,
            "obs_hold_remaining": 12,
            "cursor_action": "OBSERVE_HOLD",
        },
        "period_returns": {"A": {}},
        "track_a_health": {
            "forward_book": {
                "open_total": 0,
                "closed_total": 10,
                "open_by_market": {},
                "closed_by_market": {"KR": 5, "US": 5},
                "ok": True,
            },
            "deploy_watch": {
                "available": True,
                "overall": "PASS",
                "stale": False,
                "phase": "x",
                "age_hours": 1,
                "path": "/tmp",
                "error": None,
            },
        },
    }


def _payload(**over):
    snap = _snap()
    return build_director_watchdog_payload(
        snap,
        load_history=False,
        previous_bands=over.get("previous_bands", {"KR": "LOCKDOWN", "US": "NORMAL"}),
        nav_fail_count=over.get("nav_fail_count", 0),
        iv_report=over.get(
            "iv_report",
            {
                "v2": {"readiness": "NOT_READY"},
                "observation": {"days_elapsed": 26, "min_days": 28},
            },
        ),
        rank_counts=over.get("rank_counts", {"US_RANK_B": 7, "US_RANK_D": 3}),
        regimes=over.get("regimes", {"KR": "SIDEWAYS", "US": "BULL"}),
        group_map=over.get("group_map", {k: 1.0 for k in FAMILY_SLEEVE_DEMOTE_KEYS}),
        sys_config=over.get("sys_config", {"ENABLE_WEIGHT_S5_MERGE": True}),
    )


class DirectorWatchdogTests(unittest.TestCase):
    def test_demote_uses_effective_not_raw_map(self) -> None:
        raw = {k: 1.0 for k in FAMILY_SLEEVE_DEMOTE_KEYS}
        raw["🔥 US S1 (5선 관통 / 448 완전정배열)"] = 0.0
        ins = inspect_demote_effective(raw)
        self.assertTrue(ins["ok"])
        for k in FAMILY_SLEEVE_DEMOTE_KEYS:
            self.assertEqual(ins["effective"][k], FAMILY_SLEEVE_DEMOTE_MULT)

    def test_demote_red_only_if_effective_wrong(self) -> None:
        with patch(
            "reports.director_watchdog.apply_family_sleeve_group_mult",
            side_effect=lambda key, cur: 1.0,
        ):
            ins = inspect_demote_effective({k: 0.25 for k in FAMILY_SLEEVE_DEMOTE_KEYS})
        self.assertFalse(ins["ok"])
        p = _payload()
        self.assertIn("데모션 유지", p["html"])

    def test_band_unchanged_green_changed_red(self) -> None:
        same = _payload(previous_bands={"KR": "LOCKDOWN", "US": "NORMAL"})
        self.assertTrue(same["html"].startswith("🔭"))
        self.assertIn("🟢 안전장치", same["html"])
        self.assertIn("변화없음", same["html"])
        changed = _payload(previous_bands={"KR": "LOCKDOWN", "US": "DEFENSE"})
        self.assertIn("🔴 안전장치", changed["html"])
        self.assertIn("DEFENSE→NORMAL", changed["html"])

    def test_nav_count_ops_events_not_market_data(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "ops_events.sqlite")
            conn = sqlite3.connect(db)
            conn.execute(
                "CREATE TABLE ops_events (id INTEGER PRIMARY KEY, ts_utc TEXT, "
                "component TEXT, severity TEXT, event TEXT, payload_json TEXT)"
            )
            conn.execute(
                "INSERT INTO ops_events VALUES (1, ?, 'fwd', 'ERROR', ?, '{}')",
                ("2099-01-01T00:00:00+00:00", NAV_HOOK_FAIL_EVENT),
            )
            conn.commit()
            conn.close()
            n = count_nav_hook_failures_24h(db_path=db)
            self.assertGreaterEqual(n, 0)
        p = _payload(nav_fail_count=2)
        self.assertIn("🔴 NAV 훅", p["html"])
        self.assertIn("실패 2건", p["html"])
        z = _payload(nav_fail_count=0)
        self.assertIn("🟢 NAV 훅", z["html"])

    def test_iv_reads_json_not_recompute(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "iv_observation_latest.json")
            Path(path).write_text(
                '{"v2":{"readiness":"NOT_READY"},'
                '"observation":{"days_elapsed":26,"min_days":28}}',
                encoding="utf-8",
            )
            data = read_iv_observation_latest(path)
        self.assertEqual((data or {}).get("v2", {}).get("readiness"), "NOT_READY")
        src = Path("reports/director_watchdog.py").read_text(encoding="utf-8")
        self.assertNotIn("assess_v2_readiness", src)
        self.assertNotIn("build_iv_observation_report", src)

    def test_rank_thirty_display_only_no_branch(self) -> None:
        html = _payload(rank_counts={"US_RANK_B": 7, "US_RANK_D": 1})["html"]
        self.assertIn("n=7 (참고 관찰선 30)", html)
        self.assertIn("US_RANK_D n=1 (참고 관찰선 30)", html)
        tree = ast.parse(Path("reports/director_watchdog.py").read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Compare):
                for cmp_ in node.comparators:
                    if isinstance(cmp_, ast.Constant) and cmp_.value == 30:
                        self.fail("30 used in a comparison")

    def test_s5_waiting_when_not_bear_high_vol(self) -> None:
        html = _payload(regimes={"KR": "BULL", "US": "SIDEWAYS"})["html"]
        self.assertIn("⚪ S5 방어팔", html)
        self.assertIn("측정 대기", html)
        bear = _payload(regimes={"KR": "BEAR", "US": "BULL"})["html"]
        self.assertIn("🟡 S5 방어팔", bear)
        self.assertNotIn("측정 대기 중", bear)

    def test_core_group_strips_all_tags_like_deep_dive(self) -> None:
        self.assertEqual(
            leaderboard_core_group("[SUPERNOVA_COSINE] US_RANK_B_중기스윙"),
            "US_RANK_B_중기스윙",
        )

    def test_prev_history_skips_today(self) -> None:
        hist = [
            {
                "date_kst": "2026-09-08",
                "tracks": {"A": {"markets": {"KR": {"budget_band": "LOCKDOWN"}, "US": {"budget_band": "NORMAL"}}}},
            },
            {
                "date_kst": "2026-09-09",
                "tracks": {"A": {"markets": {"KR": {"budget_band": "NORMAL"}, "US": {"budget_band": "NORMAL"}}}},
            },
        ]
        prev = previous_budget_bands_from_history(hist, date_kst="2026-09-09")
        self.assertEqual(prev["KR"], "LOCKDOWN")

    def test_digest_order_and_length(self) -> None:
        snap = _snap()
        snap["director_watchdog"] = _payload()
        full = format_north_star_digest_html(snap)
        i_wd = full.find("[디렉터 워치독]")
        i_ez = full.find("[쉬운판]")
        i_ta = full.find("Track A")
        i_obs = full.find("[OBS_HOLD]")
        self.assertGreaterEqual(i_wd, 0)
        self.assertGreater(i_ez, i_wd)
        self.assertGreater(i_ta, i_ez)
        self.assertGreater(i_obs, i_ta)
        wd = format_director_watchdog_section_from_snap(snap)
        self.assertLessEqual(len(wd), 900)
        self.assertLessEqual(wd.count("\n") + 1, 8)
        weekly = _snap(cadence="weekly")
        self.assertEqual(format_director_watchdog_section_from_snap(weekly), "")
        self.assertNotIn("[디렉터 워치독]", format_north_star_digest_html(weekly))

    def test_source_no_if_n_ge_30(self) -> None:
        src = inspect.getsource(build_director_watchdog_payload)
        self.assertNotIn("n >= 30", src)
        self.assertNotIn("n>=30", src)


if __name__ == "__main__":
    unittest.main()

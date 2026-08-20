"""OPS-LIQ-TG-01 — [LIQ_BAND] panel unit tests."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import dual_north_star_ledger as ledger
from dual_north_star_telegram import format_north_star_digest_html, format_obs_hold_section_html
from reports.liq_band_panel import (
    LIQ_BAND_MIN_N,
    append_liq_band_history,
    bucket_percentile,
    format_liq_band_panel_html,
    format_liq_band_section_from_snap,
    load_liq_band_history,
    midhigh_share_from_counts,
    percentile_of_universe,
    resolve_liq_band_cursor_action,
)


def _snap_mkt(
    market: str,
    *,
    n: int,
    midhigh_share: float,
    scan_date: str = "2026-08-20",
    insufficient: bool | None = None,
) -> dict:
    insuf = bool(n < LIQ_BAND_MIN_N) if insufficient is None else bool(insufficient)
    mid_n = int(round(midhigh_share * n)) if n else 0
    low_n = max(0, n - mid_n)
    return {
        "market": market,
        "scan_date": scan_date,
        "n": n,
        "low_n": low_n,
        "mid_n": mid_n,
        "high_n": 0,
        "unknown_n": 0,
        "midhigh_share": float(midhigh_share),
        "insufficient": insuf,
        "sample_source": "scan_funnel_drop_event",
    }


def _ns_snap(*, cadence: str = "daily", daily_n: int = 8) -> dict:
    snap = {
        "cadence": cadence,
        "date_kst": "2026-08-20",
        "tracks": {
            "A": {
                "label": "주식 KR+US",
                "phase_label": "운영",
                "mdd_cap_pct": 10,
                "cagr_target_lo": 40,
                "cagr_target_hi": 70,
                "available": True,
                "aggregate": {
                    "max_mdd_pct": 2.0,
                    "avg_return_pct": 1.0,
                    "return_pace_score": 5.0,
                    "mdd_safety_score": 80.0,
                    "composite_score": 4.09,
                },
            },
            "B": {
                "label": "Bitget",
                "phase_label": "B0",
                "mdd_cap_pct": 5,
                "cagr_target_lo": 12,
                "cagr_target_hi": 25,
                "aggregate": {
                    "max_mdd_pct": 1.0,
                    "avg_return_pct": 0.0,
                    "return_pace_score": 0.0,
                    "mdd_safety_score": 80.0,
                    "composite_score": 40.0,
                    "measure_only": True,
                },
            },
        },
        "comparison": {"leader_mode": "side_by_side", "leader_reason": "B0"},
        "ledger": {"A": {"gate": "G0", "gate_label": "측정·구조"}, "B": {"gate": "G0"}},
        "meta": {},
        "period_returns": {"A": {}, "B": {}},
        "track_a_health": {
            "forward_book": {
                "open_total": 0,
                "closed_total": 10,
                "open_by_market": {},
                "closed_by_market": {"KR": 5, "US": 5},
                "ok": True,
                "error": None,
            },
            "deploy_watch": {
                "available": True,
                "overall": "PASS",
                "phase": "post_bear_underdog_01",
                "age_hours": 2.0,
                "stale": False,
                "path": "/tmp/fake",
                "error": None,
            },
        },
    }
    ledger.enrich_obs_hold_meta(snap, daily_n=daily_n)
    return snap


class TestLiqBandPanelOpsLiqTg01(unittest.TestCase):
    def test_a_insufficient_none_no_numeric_judgment(self) -> None:
        kr = _snap_mkt("KR", n=5, midhigh_share=0.9)
        us = _snap_mkt("US", n=25, midhigh_share=0.1)
        action, detail = resolve_liq_band_cursor_action(kr, us, [])
        self.assertEqual(action, "NONE")
        self.assertTrue(detail["kr_insufficient"])
        html = format_liq_band_panel_html(kr, us, action, detail=detail)
        self.assertIn("[LIQ_BAND]", html)
        self.assertIn("표본이 부족", html)
        self.assertNotIn("정상비중", html)
        self.assertNotIn("과반", html)
        self.assertNotIn("Phase2", html)

    def test_b_midhigh_share_and_buckets(self) -> None:
        self.assertAlmostEqual(midhigh_share_from_counts(5, 10, 5), 0.75)
        self.assertEqual(bucket_percentile(10.0), "low")
        self.assertEqual(bucket_percentile(50.0), "mid")
        self.assertEqual(bucket_percentile(80.0), "high")
        univ = [10.0, 20.0, 30.0, 40.0, 50.0]
        # value 35 → strictly below: 10,20,30 → 60%
        self.assertAlmostEqual(percentile_of_universe(35.0, univ), 60.0)
        self.assertEqual(bucket_percentile(percentile_of_universe(35.0, univ)), "mid")

    def test_c_phase2_five_day_streak_kr(self) -> None:
        hist = []
        for i, day in enumerate(
            ["2026-08-14", "2026-08-15", "2026-08-16", "2026-08-17", "2026-08-18"]
        ):
            hist.append(
                {
                    "scan_date": day,
                    "markets": {
                        "KR": _snap_mkt("KR", n=40, midhigh_share=0.60, scan_date=day),
                        "US": _snap_mkt("US", n=40, midhigh_share=0.10, scan_date=day),
                    },
                }
            )
        # history already has 5; today continues → still PHASE2
        kr = _snap_mkt("KR", n=40, midhigh_share=0.55, scan_date="2026-08-19")
        us = _snap_mkt("US", n=40, midhigh_share=0.10, scan_date="2026-08-19")
        action, detail = resolve_liq_band_cursor_action(kr, us, hist)
        self.assertEqual(action, "PHASE2_CANDIDATE")
        self.assertIn("KR", detail["phase2_markets"])
        self.assertNotIn("US", detail["phase2_markets"])
        html = format_liq_band_panel_html(kr, us, action, detail=detail)
        self.assertIn("KR", html)
        self.assertIn("---CLAUDE---", html)
        self.assertIn("OPS-LIQ-TG-01", html)

    def test_c_phase2_five_day_streak_us(self) -> None:
        hist = []
        for day in ["2026-08-14", "2026-08-15", "2026-08-16", "2026-08-17", "2026-08-18"]:
            hist.append(
                {
                    "scan_date": day,
                    "markets": {
                        "KR": _snap_mkt("KR", n=40, midhigh_share=0.10, scan_date=day),
                        "US": _snap_mkt("US", n=40, midhigh_share=0.70, scan_date=day),
                    },
                }
            )
        kr = _snap_mkt("KR", n=40, midhigh_share=0.10, scan_date="2026-08-19")
        us = _snap_mkt("US", n=40, midhigh_share=0.65, scan_date="2026-08-19")
        action, detail = resolve_liq_band_cursor_action(kr, us, hist)
        self.assertEqual(action, "PHASE2_CANDIDATE")
        self.assertIn("US", detail["phase2_markets"])

    def test_d_insufficient_day_skips_streak_no_reset(self) -> None:
        # 4 high days, 1 insufficient gap, then today high → streak from valid days = 5 with today
        hist = []
        for day, share, n in [
            ("2026-08-14", 0.60, 40),
            ("2026-08-15", 0.60, 40),
            ("2026-08-16", 0.60, 5),  # insufficient — skip
            ("2026-08-17", 0.60, 40),
            ("2026-08-18", 0.60, 40),
        ]:
            hist.append(
                {
                    "scan_date": day,
                    "markets": {
                        "KR": _snap_mkt(
                            "KR", n=n, midhigh_share=share, scan_date=day
                        ),
                        "US": _snap_mkt("US", n=40, midhigh_share=0.05, scan_date=day),
                    },
                }
            )
        kr = _snap_mkt("KR", n=40, midhigh_share=0.55, scan_date="2026-08-19")
        us = _snap_mkt("US", n=40, midhigh_share=0.05, scan_date="2026-08-19")
        action, detail = resolve_liq_band_cursor_action(kr, us, hist)
        self.assertEqual(action, "PHASE2_CANDIDATE")
        self.assertGreaterEqual(detail["streaks"]["KR"], 5)

    def test_observe_when_both_valid_below_phase2(self) -> None:
        kr = _snap_mkt("KR", n=40, midhigh_share=0.20)
        us = _snap_mkt("US", n=40, midhigh_share=0.10)
        action, _detail = resolve_liq_band_cursor_action(kr, us, [])
        self.assertEqual(action, "OBSERVE_LIQ_BAND")
        html = format_liq_band_panel_html(kr, us, action)
        self.assertIn("지켜보기", html)
        self.assertIn("정상비중", html)
        self.assertNotIn("---CLAUDE---", html)

    def test_e_enabled_false_no_panel(self) -> None:
        snap = _ns_snap()
        base = format_north_star_digest_html(snap)
        with patch("reports.liq_band_panel.LIQ_BAND_ENABLED", False):
            from reports.liq_band_panel import build_liq_band_payload_for_digest

            payload = build_liq_band_payload_for_digest(
                scan_date="2026-08-20",
                persist_history=False,
                enabled=False,
                kr_snapshot=_snap_mkt("KR", n=40, midhigh_share=0.9),
                us_snapshot=_snap_mkt("US", n=40, midhigh_share=0.9),
            )
            self.assertIsNone(payload)
        snap2 = _ns_snap()
        snap2["liq_band"] = None
        self.assertEqual(format_north_star_digest_html(snap2), base)
        self.assertNotIn("[LIQ_BAND]", base)

    def test_f_obs_hold_and_nine_section_unchanged_without_liq(self) -> None:
        snap = _ns_snap(daily_n=8)
        obs = format_obs_hold_section_html(snap)
        full = format_north_star_digest_html(snap)
        self.assertIn("[OBS_HOLD]", full)
        self.assertIn("[쉬운판]", full)
        self.assertIn("Track A", full)
        self.assertNotIn("━━ Track B", full)
        self.assertNotIn("Track B · Bitget", full)
        self.assertEqual(obs, format_obs_hold_section_html(snap))
        self.assertNotIn("[LIQ_BAND]", full)
        # with liq payload present → panel appears AFTER obs content
        snap["liq_band"] = {
            "enabled": True,
            "cursor_action": "OBSERVE_LIQ_BAND",
            "KR": _snap_mkt("KR", n=40, midhigh_share=0.2),
            "US": _snap_mkt("US", n=40, midhigh_share=0.1),
            "detail": {},
            "html": format_liq_band_panel_html(
                _snap_mkt("KR", n=40, midhigh_share=0.2),
                _snap_mkt("US", n=40, midhigh_share=0.1),
                "OBSERVE_LIQ_BAND",
            ),
        }
        full2 = format_north_star_digest_html(snap)
        self.assertIn("[LIQ_BAND]", full2)
        self.assertLess(full2.index("[OBS_HOLD]"), full2.index("[LIQ_BAND]"))
        # OBS_HOLD section function itself unchanged
        self.assertEqual(format_obs_hold_section_html(snap), obs)

    def test_history_rolling_and_section_helper(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = str(Path(td) / "liq_band_history.json")
            for i in range(12):
                day = f"2026-08-{i+1:02d}"
                append_liq_band_history(
                    {
                        "scan_date": day,
                        "markets": {
                            "KR": _snap_mkt("KR", n=40, midhigh_share=0.1, scan_date=day),
                            "US": _snap_mkt("US", n=40, midhigh_share=0.1, scan_date=day),
                        },
                    },
                    path=path,
                    keep_days=10,
                )
            days = load_liq_band_history(path=path)
            self.assertEqual(len(days), 10)
            self.assertEqual(days[0]["scan_date"], "2026-08-03")
        self.assertEqual(format_liq_band_section_from_snap({}), "")


if __name__ == "__main__":
    unittest.main()

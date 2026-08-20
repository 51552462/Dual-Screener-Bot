"""OBS-HOLD panel + Track A diagnostic easy dashboard on North Star digest."""
from __future__ import annotations

import unittest

import dual_north_star_ledger as ledger
from dual_north_star_telegram import (
    build_goal_dashboard,
    build_obs_hold_claude_prompt,
    build_obs_hold_cursor_prompt,
    format_goal_dashboard_html,
    format_north_star_digest_html,
    format_obs_hold_section_html,
)


def _health(
    *,
    closed: int = 10,
    open_n: int = 0,
    watch_overall: str = "PASS",
    watch_available: bool = True,
    watch_stale: bool = False,
) -> dict:
    return {
        "forward_book": {
            "open_total": open_n,
            "closed_total": closed,
            "open_by_market": {"KR": open_n} if open_n else {},
            "closed_by_market": {"KR": closed // 2, "US": closed - closed // 2} if closed else {},
            "ok": closed > 0,
            "error": None,
        },
        "deploy_watch": {
            "available": watch_available,
            "overall": watch_overall if watch_available else None,
            "phase": "post_bear_underdog_01",
            "age_hours": 2.0,
            "stale": watch_stale,
            "path": "/tmp/fake",
            "error": None if watch_available else "missing",
        },
    }


def _snap(*, cadence: str = "daily", daily_n: int = 8, composite: float = 4.09, **kw) -> dict:
    mdd = float(kw.pop("mdd", 2.0))
    available = kw.pop("available", True)
    error = kw.pop("error", None)
    health = kw.pop("health", None)
    health_payload = health if health is not None else _health()
    book = health_payload["forward_book"]
    snap = {
        "cadence": cadence,
        "date_kst": "2026-08-17",
        "tracks": {
            "A": {
                "label": "주식 KR+US",
                "phase_label": "운영",
                "mdd_cap_pct": 10,
                "cagr_target_lo": 40,
                "cagr_target_hi": 70,
                "available": available,
                "forward_book": book,
                "forward_trades_count": int(book.get("closed_total", 0) or 0)
                + int(book.get("open_total", 0) or 0),
                "markets": {
                    "KR": {
                        "nav": 1_000_000,
                        "mdd_pct": mdd,
                        "return_pct": -1.0,
                        "budget_band": "NORMAL",
                        "exhaustion_pct": 10.0,
                        "n_closed": 5,
                    },
                    "US": {
                        "nav": 50_000,
                        "mdd_pct": 1.0,
                        "return_pct": 0.5,
                        "budget_band": "NORMAL",
                        "exhaustion_pct": 5.0,
                        "n_closed": 5,
                    },
                },
                "aggregate": {
                    "max_mdd_pct": mdd,
                    "avg_return_pct": 1.0,
                    "return_pace_score": 5.0,
                    "mdd_safety_score": 80.0,
                    "composite_score": composite,
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
        "period_returns": {"A": {"total_pct": -2.0}, "B": {}},
        "track_a_health": health_payload,
    }
    if error is not None:
        snap["tracks"]["A"]["error"] = error
        snap["tracks"]["A"]["available"] = False
    ledger.enrich_obs_hold_meta(snap, daily_n=daily_n)
    return snap


def _ids(bucket: list) -> set:
    return {x["id"] for x in bucket if isinstance(x, dict)}


class TestObsHoldTelegram(unittest.TestCase):
    def test_action_observe_at_n8(self) -> None:
        self.assertEqual(ledger.resolve_obs_hold_action(cadence="daily", daily_n=8), "OBSERVE_HOLD")
        snap = _snap(daily_n=8)
        self.assertEqual(snap["meta"]["cursor_action"], "OBSERVE_HOLD")
        self.assertEqual(snap["meta"]["obs_hold_remaining"], 12)
        self.assertTrue(snap["meta"]["obs_hold_active"])
        html = format_obs_hold_section_html(snap)
        self.assertIn("[OBS_HOLD]", html)
        self.assertIn("8", html)
        self.assertIn("/20", html)
        self.assertIn("---CURSOR---", html)
        self.assertIn("---CLAUDE---", html)
        self.assertIn("OBSERVE_HOLD", html)
        self.assertIn("Alpha Handoff", build_obs_hold_cursor_prompt(snap))
        self.assertIn("신규 Handoff 없음", build_obs_hold_claude_prompt(snap))

    def test_action_recall_at_n20(self) -> None:
        self.assertEqual(ledger.resolve_obs_hold_action(cadence="daily", daily_n=20), "RECALL_FORK")
        snap = _snap(daily_n=20)
        self.assertEqual(snap["meta"]["cursor_action"], "RECALL_FORK")
        self.assertEqual(snap["meta"]["obs_hold_remaining"], 0)
        self.assertFalse(snap["meta"]["obs_hold_active"])
        html = format_obs_hold_section_html(snap)
        self.assertIn("RECALL_FORK", html)
        self.assertIn("재소집", html)
        self.assertIn("갈림길 3택", build_obs_hold_claude_prompt(snap))

    def test_weekly_omits_obs_section(self) -> None:
        self.assertEqual(ledger.resolve_obs_hold_action(cadence="weekly", daily_n=8), "NONE")
        snap = _snap(cadence="weekly", daily_n=8)
        self.assertEqual(snap["meta"]["cursor_action"], "NONE")
        self.assertEqual(format_obs_hold_section_html(snap), "")
        full = format_north_star_digest_html(snap)
        self.assertNotIn("[OBS_HOLD]", full)
        self.assertNotIn("---CURSOR---", full)

    def test_daily_digest_includes_obs(self) -> None:
        snap = _snap(daily_n=8)
        full = format_north_star_digest_html(snap)
        self.assertIn("[OBS_HOLD]", full)
        self.assertIn("---CURSOR---", full)
        self.assertIn("---CLAUDE---", full)
        self.assertIn("[쉬운판]", full)
        self.assertIn("잘 되고 있어요", full)
        self.assertIn("구멍", full)
        self.assertIn("나중", full)
        self.assertIn("주식 북극성", full)
        self.assertIn("시장별", full)
        self.assertIn("가상매매 장부", full)
        self.assertNotIn("Track B", full)
        self.assertNotIn("━━ Track B", full)
        self.assertNotIn("비교 모드", full)
        self.assertIn("POST_DEPLOY_OBS", full)  # 분리 안내만

    def test_goal_dashboard_n8(self) -> None:
        snap = _snap(daily_n=8, composite=4.09)
        d = build_goal_dashboard(snap)
        self.assertEqual(d["n"], 8)
        self.assertEqual(d["remaining"], 12)
        self.assertIn("obs", _ids(d["missing"]))
        self.assertNotIn("obs", _ids(d["problem"]))
        self.assertFalse(d["checklist"][2]["done"])  # n/20
        html = format_goal_dashboard_html(snap)
        self.assertIn("8", html)
        self.assertIn("/20", html)
        self.assertIn("구멍", html)
        self.assertIn("관측 기간", html)
        self.assertIn("잘 되고", html)

    def test_open_zero_closed_ok_not_problem(self) -> None:
        snap = _snap(daily_n=8, health=_health(closed=10, open_n=0))
        d = build_goal_dashboard(snap)
        self.assertIn("book", _ids(d["working"]))
        self.assertNotIn("book", _ids(d["problem"]))

    def test_closed_zero_is_problem(self) -> None:
        snap = _snap(daily_n=8, health=_health(closed=0, open_n=0))
        d = build_goal_dashboard(snap)
        self.assertIn("book", _ids(d["problem"]))

    def test_mdd_over_cap_is_problem(self) -> None:
        snap = _snap(daily_n=8, mdd=12.5)
        d = build_goal_dashboard(snap)
        self.assertIn("mdd", _ids(d["problem"]))
        self.assertTrue(any("한도" in p["plain"] for p in d["problem"]))

    def test_nav_error_surface(self) -> None:
        snap = _snap(daily_n=8, error="boom")
        d = build_goal_dashboard(snap)
        self.assertIn("nav", _ids(d["problem"]))
        self.assertTrue(any("boom" in e for e in d["errors"]))

    def test_deploy_watch_break_is_problem(self) -> None:
        snap = _snap(daily_n=8, health=_health(watch_overall="BREAK"))
        d = build_goal_dashboard(snap)
        self.assertIn("watch", _ids(d["problem"]))

    def test_later_forbids_mega_and_live(self) -> None:
        d = build_goal_dashboard(_snap(daily_n=8))
        lids = _ids(d["later"])
        self.assertIn("mega", lids)
        self.assertIn("live", lids)
        self.assertIn("cagr", lids)


if __name__ == "__main__":
    unittest.main()

"""Mega-Trend Ignition (1번) — 거래대금 쏠림 + 수급 Z-Score 점화 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

import pandas as pd

from mega_trend_ignition import (
    MEGA_TREND_CONFIG_KEY,
    detect_mega_trend_sectors,
    is_mega_trend_sector,
    refresh_mega_trend_ignition,
)


def _flow_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE kr_investor_flow (
            date TEXT, code TEXT, foreign_inst_krw REAL
        )
        """
    )
    sector = "반도체/IT"
    for d in range(1, 65):
        day = f"2026-01-{d:02d}" if d <= 31 else f"2026-02-{d-31:02d}"
        for code in ("005930", "000660", "035420"):
            base = 1e9 if d >= 55 else 1e7
            conn.execute(
                "INSERT INTO kr_investor_flow VALUES (?,?,?)",
                (day, code, base * (1 + 0.02 * (d - 55))),
            )
    conn.commit()
    return conn


class TestMegaTrendIgnition(unittest.TestCase):
    def test_detect_requires_both_turnover_and_flow(self):
        turnover = {
            "sectors": {
                "반도체/IT": {"share": 0.35, "trade_value": 1e12, "n_stocks": 10},
                "금융/지주": {"share": 0.10, "trade_value": 3e11, "n_stocks": 8},
            },
            "top_sector": "반도체/IT",
            "top_share": 0.35,
            "reason": "computed",
        }
        flow_block = {
            "sector": "반도체/IT",
            "z_score": 2.5,
            "window_krw": 5e10,
            "neutral": False,
            "reason": "computed",
        }
        weak_flow = {**flow_block, "z_score": 1.2}

        with patch(
            "mega_trend_ignition.compute_sector_turnover_concentration",
            return_value=turnover,
        ):
            with patch(
                "kr_flow_factor.compute_sector_flow_zscore",
                side_effect=[flow_block, weak_flow],
            ):
                out = detect_mega_trend_sectors(conn=_flow_conn())
        self.assertTrue(out["ignited"])
        self.assertEqual(out["primary_sector"], "반도체/IT")
        self.assertEqual(out["sectors"], ["반도체/IT"])

    def test_no_ignition_when_turnover_low(self):
        turnover = {
            "sectors": {
                "반도체/IT": {"share": 0.18, "trade_value": 4e11, "n_stocks": 10},
            },
            "reason": "computed",
        }
        with patch(
            "mega_trend_ignition.compute_sector_turnover_concentration",
            return_value=turnover,
        ):
            out = detect_mega_trend_sectors()
        self.assertFalse(out["ignited"])
        self.assertIsNone(out["primary_sector"])

    def test_refresh_persists_config(self):
        detection = {
            "ignited": True,
            "sectors": ["에너지/화학"],
            "primary_sector": "에너지/화학",
            "primary_detail": {
                "sector": "에너지/화학",
                "turnover_share_pct": 32.0,
                "flow_z": 2.8,
            },
            "candidates_checked": 1,
            "turnover_snapshot": {},
            "ignition_details": [],
        }
        saved = {}

        def _save(cfg):
            saved.update(cfg)
            return True

        with patch(
            "mega_trend_ignition.detect_mega_trend_sectors",
            return_value=detection,
        ):
            state = refresh_mega_trend_ignition({}, save_config_fn=_save)
        self.assertTrue(state["active"])
        self.assertTrue(state.get("rotation_advantage_active"))
        self.assertEqual(saved[MEGA_TREND_CONFIG_KEY]["primary_sector"], "에너지/화학")

    def test_is_mega_trend_sector(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "sectors": ["반도체/IT"],
                "primary_sector": "반도체/IT",
            }
        }
        self.assertTrue(is_mega_trend_sector("반도체/IT", cfg))
        self.assertFalse(is_mega_trend_sector("금융/지주", cfg))


class TestTurnoverConcentration(unittest.TestCase):
    def test_compute_sector_shares(self):
        from mega_trend_ignition import compute_sector_turnover_concentration

        block = pd.DataFrame(
            [
                {"code": "005930", "sector": "반도체/IT", "trade_value": 500.0},
                {"code": "000660", "sector": "반도체/IT", "trade_value": 300.0},
                {"code": "035420", "sector": "반도체/IT", "trade_value": 100.0},
                {"code": "105560", "sector": "금융/지주", "trade_value": 100.0},
            ]
        )
        with patch(
            "mega_trend_ignition._fetch_market_turnover_block",
            side_effect=[block, pd.DataFrame()],
        ):
            with patch("mega_trend_ignition._recent_trade_ymd", return_value="20260201"):
                out = compute_sector_turnover_concentration("20260201")
        self.assertAlmostEqual(out["top_share"], 0.9, places=2)
        self.assertEqual(out["top_sector"], "반도체/IT")


if __name__ == "__main__":
    unittest.main()

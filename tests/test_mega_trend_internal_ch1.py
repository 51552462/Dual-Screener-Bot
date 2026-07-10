"""Mega-Trend Internal Kill-Switch 1번 — PnL·승률 자가 진단 테스트."""
from __future__ import annotations

import sqlite3
import unittest

import exit_dynamics as xd
from mega_trend_ignition import MEGA_TREND_CONFIG_KEY
from mega_trend_internal_monitor import (
    fetch_mega_trend_sector_trades,
    is_internal_momentum_lost,
    refresh_mega_trend_internal_diagnostics,
)


def _closed_trade(
    final_ret: float,
    mfe: float = 8.0,
    exit_type: str = "STAT_MFE",
    exit_reason: str = "",
) -> dict:
    return {
        "status": "CLOSED_WIN" if final_ret > 0 else "CLOSED_LOSS",
        "final_ret": final_ret,
        "mfe": mfe,
        "exit_type": exit_type,
        "exit_reason": exit_reason,
        "sim_stat_ret": final_ret,
    }


class TestClassifyOutcome(unittest.TestCase):
    def test_win(self):
        self.assertEqual(
            xd.classify_mega_trend_trade_outcome(_closed_trade(5.0)), "win"
        )

    def test_bounce_stop_mae(self):
        t = _closed_trade(-2.0, mfe=6.0, exit_type="STAT_MAE")
        self.assertEqual(xd.classify_mega_trend_trade_outcome(t), "bounce_stop")

    def test_bounce_stop_near_breakeven(self):
        t = _closed_trade(0.5, mfe=8.0)
        self.assertEqual(xd.classify_mega_trend_trade_outcome(t), "bounce_stop")

    def test_gave_back_mfe(self):
        t = _closed_trade(1.0, mfe=12.0)
        self.assertEqual(xd.classify_mega_trend_trade_outcome(t), "bounce_stop")

    def test_mfe_reached(self):
        self.assertTrue(xd.is_mfe_target_reached(_closed_trade(3.0, mfe=7.0)))
        self.assertFalse(xd.is_mfe_target_reached(_closed_trade(3.0, mfe=2.0)))


class TestInternalMetrics(unittest.TestCase):
    def test_metrics_on_mixed_trades(self):
        trades = [
            _closed_trade(6.0),
            _closed_trade(4.0),
            _closed_trade(-1.0, exit_type="STAT_MAE"),
            _closed_trade(0.3),
            _closed_trade(-3.0),
        ]
        m = xd.compute_internal_trade_metrics(trades)
        self.assertEqual(m["n_trades"], 5)
        self.assertEqual(m["wins"], 2)
        self.assertGreaterEqual(m["bounce_stops"], 1)
        self.assertIsNotNone(m["win_rate"])

    def test_pnl_acceleration_negative(self):
        prior = [_closed_trade(5.0), _closed_trade(4.0), _closed_trade(6.0)]
        recent = [
            _closed_trade(-1.0, exit_type="STAT_MAE"),
            _closed_trade(0.2),
            _closed_trade(-2.0, exit_type="STAT_MAE"),
        ]
        accel = xd.compute_pnl_acceleration(prior + recent)
        self.assertIsNotNone(accel["accel_win_rate"])
        self.assertLess(float(accel["accel_win_rate"]), 0.0)


class TestSelfDiagnosis(unittest.TestCase):
    def test_momentum_lost_on_win_rate_collapse(self):
        trades = [
            _closed_trade(-2.0, exit_type="STAT_MAE"),
            _closed_trade(0.5),
            _closed_trade(-1.0, exit_type="STAT_MAE"),
            _closed_trade(-3.0),
            _closed_trade(0.2),
        ]
        diag = xd.evaluate_internal_momentum_loss(
            trades,
            thresholds={
                "window_n_min": 5,
                "win_rate_min": 0.40,
                "mfe_reach_min": 0.0,
                "bounce_stop_max_rate": 0.99,
                "pnl_accel_drop_min": 0.99,
            },
        )
        self.assertTrue(diag["momentum_lost"])
        self.assertTrue(diag["self_diagnosis"])
        self.assertTrue(any("win_rate" in t for t in diag["triggers"]))

    def test_momentum_ok_on_strong_run(self):
        trades = [_closed_trade(5.0 + i) for i in range(6)]
        diag = xd.evaluate_internal_momentum_loss(trades)
        self.assertFalse(diag["momentum_lost"])
        self.assertEqual(diag["reason"], "internal_momentum_ok")

    def test_insufficient_sample_neutral(self):
        diag = xd.evaluate_internal_momentum_loss([_closed_trade(1.0)])
        self.assertFalse(diag["momentum_lost"])
        self.assertIn("insufficient_sample", diag["reason"])


class TestMonitorIntegration(unittest.TestCase):
    def _mk_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE forward_trades (
                id INTEGER PRIMARY KEY,
                market TEXT, code TEXT, sector TEXT, sig_type TEXT,
                status TEXT, entry_date TEXT, exit_date TEXT,
                final_ret REAL, mfe REAL, max_high REAL, entry_price REAL,
                sim_stat_ret REAL, exit_type TEXT, exit_reason TEXT,
                invest_amount REAL, sim_kelly_invest REAL
            )
            """
        )
        rows = [
            ("CLOSED_LOSS", "2026-02-01", -2.0, 6.0, "STAT_MAE", "#MegaTrend언락"),
            ("CLOSED_LOSS", "2026-02-02", 0.3, 5.0, "", "#MegaTrend자본합산"),
            ("CLOSED_LOSS", "2026-02-03", -1.5, 4.0, "STAT_MAE", ""),
            ("CLOSED_WIN", "2026-02-04", 0.8, 7.0, "", ""),
            ("CLOSED_LOSS", "2026-02-05", -2.5, 3.0, "STAT_MAE", ""),
        ]
        for i, (st, ed, fr, mfe, et, sig) in enumerate(rows, 1):
            conn.execute(
                """
                INSERT INTO forward_trades (
                    market, code, sector, sig_type, status, entry_date, exit_date,
                    final_ret, mfe, max_high, entry_price, sim_stat_ret,
                    exit_type, exit_reason, invest_amount, sim_kelly_invest
                ) VALUES ('KR','005930','반도체/IT',?, ?, '2026-02-01', ?,
                          ?, ?, 70000, 65000, ?, ?, '', 1e6, 1e6)
                """,
                (sig, st, ed, fr, mfe, fr, et),
            )
        conn.commit()
        return conn

    def test_fetch_sector_trades(self):
        conn = self._mk_conn()
        trades = fetch_mega_trend_sector_trades(
            conn, "반도체/IT", ignited_at="2026-02-01", window_n=8
        )
        self.assertGreaterEqual(len(trades), 5)

    def test_refresh_persists_diagnostics(self):
        conn = self._mk_conn()
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "primary_sector": "반도체/IT",
                "sectors": ["반도체/IT"],
                "ignited_at": "2026-02-01",
            }
        }
        saved = {}

        def _save(c):
            saved.update(c)
            return True

        out = refresh_mega_trend_internal_diagnostics(
            cfg, save_config_fn=_save, conn=conn
        )
        self.assertTrue(out["updated"])
        diag = saved[MEGA_TREND_CONFIG_KEY]["internal_diagnostics"]
        self.assertIn("sectors", diag)
        self.assertIn("반도체/IT", diag["sectors"])
        sec_diag = diag["sectors"]["반도체/IT"]
        self.assertGreaterEqual(sec_diag.get("n_trades_fetched", 0), 5)

    def test_is_internal_momentum_lost_helper(self):
        cfg = {
            MEGA_TREND_CONFIG_KEY: {
                "active": True,
                "internal_diagnostics": {
                    "any_momentum_lost": True,
                    "momentum_lost_sectors": ["반도체/IT"],
                },
            }
        }
        self.assertTrue(is_internal_momentum_lost(cfg, "반도체/IT"))
        self.assertFalse(is_internal_momentum_lost(cfg, "금융/지주"))


if __name__ == "__main__":
    unittest.main()

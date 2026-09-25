"""MEGATREND-SCOPE-DEFINE-01 — 독립 MEGA_TREND_ALPHA 섀도우 (실자본 0)."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from mega_trend_alpha_shadow import (
    EXIT_TYPE_MEGA_TREND_ALPHA_SHADOW,
    MEGA_TREND_ALPHA_TAG,
    climax_should_close_alpha,
    close_mega_trend_alpha_shadow,
    mega_trend_alpha_live_fire_allowed,
    open_mega_trend_alpha_shadow,
    shadow_zero_notional,
    sync_mega_trend_alpha_shadow,
)
from mega_trend_climax import liquidate_mega_trend_sector_positions
from mega_trend_ignition import MEGA_TREND_CONFIG_KEY
from mega_trend_trade_filter import is_mega_trend_sig_type, is_mega_trend_unlock_trade


class TestMegaTrendAlphaShadow01(unittest.TestCase):
    def test_live_fire_gate_closed(self) -> None:
        self.assertFalse(mega_trend_alpha_live_fire_allowed())

    def test_zero_notional_reuse(self) -> None:
        sig, sh, inv, k = shadow_zero_notional()
        self.assertIn("OBSERVE_ONLY", sig)
        self.assertIn(MEGA_TREND_ALPHA_TAG, sig)
        self.assertNotIn("RE_EVOL_SHADOW", sig)
        self.assertEqual(sh, 0)
        self.assertEqual(inv, 0.0)
        self.assertEqual(k, 0.0)

    def test_alpha_not_unlock_booster_sig(self) -> None:
        sig = f"[OBSERVE_ONLY] {MEGA_TREND_ALPHA_TAG}"
        self.assertFalse(is_mega_trend_sig_type(sig))
        self.assertFalse(
            is_mega_trend_unlock_trade(sig_type=sig, entry_date="2026-09-25")
        )

    def test_synthetic_open_close_nav_and_exit_type(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        tdir = tempfile.mkdtemp()
        try:
            with patch("forward.shared.DB_PATH", db_path), patch(
                "live_nav_manager.factory_data_dir", return_value=tdir
            ):
                from forward.shared import init_forward_db

                init_forward_db()
                treas = os.path.join(tdir, "treasury_state.json")
                payload = {
                    "KR": {
                        "currency": "KRW",
                        "base_capital": 300_000_000.0,
                        "nav": 300_000_000.0,
                        "hwm": 300_000_000.0,
                        "mdd_pct": 1.25,
                        "n_closed": 7,
                        "last_exit_date": None,
                    }
                }
                with open(treas, "w", encoding="utf-8") as f:
                    json.dump(payload, f)

                from live_nav_manager import get_market_state

                before = get_market_state("KR")
                conn = sqlite3.connect(db_path)
                cfg = {
                    MEGA_TREND_CONFIG_KEY: {
                        "active": True,
                        "primary_sector": "반도체/IT",
                        "sectors": ["반도체/IT"],
                    }
                }
                opened = open_mega_trend_alpha_shadow(conn, config=cfg, today="2026-09-25")
                self.assertEqual(opened.get("opened"), 1)
                row = conn.execute(
                    "SELECT shares, invest_amount, sim_kelly_invest, sig_type, status "
                    "FROM forward_trades WHERE id=?",
                    (opened["trade_id"],),
                ).fetchone()
                self.assertEqual(int(row[0] or 0), 0)
                self.assertEqual(float(row[1] or 0), 0.0)
                self.assertEqual(float(row[2] or 0), 0.0)
                self.assertIn(MEGA_TREND_ALPHA_TAG, str(row[3]))
                self.assertEqual(row[4], "OPEN")

                closed = close_mega_trend_alpha_shadow(
                    conn, today="2026-09-25", call_record_closure=True
                )
                self.assertEqual(closed.get("closed"), 1)
                crow = conn.execute(
                    "SELECT status, exit_type, final_ret FROM forward_trades WHERE id=?",
                    (opened["trade_id"],),
                ).fetchone()
                conn.close()
                self.assertEqual(crow[0], "CLOSED")
                self.assertEqual(crow[1], EXIT_TYPE_MEGA_TREND_ALPHA_SHADOW)
                self.assertNotEqual(str(crow[1] or "").upper(), "UNKNOWN")
                self.assertEqual(float(crow[2] or 0), 0.0)

                after = get_market_state("KR")
                self.assertEqual(float(after["nav"]), float(before["nav"]))
                self.assertEqual(float(after["hwm"]), float(before["hwm"]))
                self.assertEqual(float(after["mdd_pct"]), float(before["mdd_pct"]))
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass

    def test_climax_does_not_capital_liquidate_alpha(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE forward_trades (
                id INTEGER PRIMARY KEY,
                market TEXT, code TEXT, sector TEXT, sig_type TEXT,
                scaled_out_frac REAL, sim_stat_ret REAL, free_runner INTEGER,
                status TEXT, sim_kelly_invest REAL, invest_amount REAL,
                realized_partial_ret REAL, exit_date TEXT, exit_reason TEXT,
                final_ret REAL, sim_stat_status TEXT, sim_tech_status TEXT,
                sim_breadth_status TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO forward_trades (
                id, market, code, sector, sig_type, scaled_out_frac, sim_stat_ret,
                free_runner, status, sim_kelly_invest, invest_amount,
                realized_partial_ret
            ) VALUES (1,'KR','005930','반도체/IT',?,0,3.0,0,'OPEN',0,0,0)
            """,
            (f"[OBSERVE_ONLY] {MEGA_TREND_ALPHA_TAG}",),
        )
        conn.commit()
        with patch("mega_trend_climax.resolve_kr_code_sector", return_value="반도체/IT"):
            out = liquidate_mega_trend_sector_positions(
                conn, ["반도체/IT"], exit_mode="full", exit_reason="test"
            )
        st = conn.execute("SELECT status FROM forward_trades WHERE id=1").fetchone()[0]
        conn.close()
        self.assertEqual(st, "OPEN")
        self.assertEqual(int(out.get("liquidated") or 0), 0)

    def test_sync_close_on_climax_flag(self) -> None:
        self.assertTrue(
            climax_should_close_alpha({"climax_kill_at": "2026-09-25 16:10:00"})
        )
        fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        tdir = tempfile.mkdtemp()
        try:
            with patch("forward.shared.DB_PATH", db_path), patch(
                "live_nav_manager.factory_data_dir", return_value=tdir
            ):
                from forward.shared import init_forward_db

                init_forward_db()
                conn = sqlite3.connect(db_path)
                cfg = {
                    MEGA_TREND_CONFIG_KEY: {
                        "active": True,
                        "primary_sector": "반도체/IT",
                    }
                }
                open_mega_trend_alpha_shadow(conn, config=cfg, today="2026-09-25")
                conn.close()
                cfg[MEGA_TREND_CONFIG_KEY]["climax_kill_at"] = "2026-09-25"
                sync_mega_trend_alpha_shadow(cfg)
                conn = sqlite3.connect(db_path)
                st, et = conn.execute(
                    "SELECT status, exit_type FROM forward_trades LIMIT 1"
                ).fetchone()
                conn.close()
                self.assertEqual(st, "CLOSED")
                self.assertEqual(et, EXIT_TYPE_MEGA_TREND_ALPHA_SHADOW)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass


if __name__ == "__main__":
    unittest.main()

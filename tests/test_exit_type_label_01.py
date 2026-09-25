"""EXIT-TYPE-LABEL-01 — heal / V-Recovery close writes exit_type (not UNKNOWN)."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from forward.shared import EXIT_TYPE_ZOMBIE_HEAL, _reporter_cleanup_zombie_forward_trades
from inverse_etf_sniper import (
    EXIT_TYPE_INVERSE_RECOVERY_KILL,
    INVERSE_SIG_MARKER,
    _close_inverse_row_at_market,
)


class TestExitTypeLabel01(unittest.TestCase):
    def test_zombie_heal_sets_exit_type(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                """
                CREATE TABLE forward_trades (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    shares REAL DEFAULT 0,
                    sim_kelly_invest REAL DEFAULT 0,
                    invest_amount REAL DEFAULT 0,
                    sig_type TEXT,
                    exit_date TEXT,
                    exit_reason TEXT,
                    exit_type TEXT DEFAULT 'UNKNOWN',
                    final_ret REAL
                )
                """
            )
            conn.execute(
                "INSERT INTO forward_trades (id, status, shares, sig_type, exit_type) "
                "VALUES (1, 'OPEN', 0, '[INCUBATOR] x', 'UNKNOWN')"
            )
            conn.commit()
            conn.close()
            with patch("forward.shared.DB_PATH", path), patch(
                "forward.shared.init_forward_db"
            ):
                n = _reporter_cleanup_zombie_forward_trades()
            self.assertEqual(n, 1)
            conn = sqlite3.connect(path)
            row = conn.execute(
                "SELECT status, exit_reason, exit_type, final_ret FROM forward_trades WHERE id=1"
            ).fetchone()
            conn.close()
            self.assertEqual(row[0], "CLOSED_ZOMBIE")
            self.assertEqual(row[2], EXIT_TYPE_ZOMBIE_HEAL)
            self.assertNotEqual(str(row[2] or "").upper(), "UNKNOWN")
            self.assertEqual(float(row[3] or 0), 0.0)
        finally:
            os.unlink(path)

    def test_v_recovery_close_sets_exit_type(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE forward_trades (
                id INTEGER PRIMARY KEY,
                market TEXT, code TEXT, sig_type TEXT,
                entry_price REAL, invest_amount REAL,
                max_high REAL, min_low REAL, status TEXT,
                exit_date TEXT, exit_reason TEXT, exit_type TEXT, final_ret REAL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO forward_trades
            (id, market, code, sig_type, entry_price, invest_amount,
             max_high, min_low, status, exit_type)
            VALUES (1, 'KR', '252670', ?, 10.0, 1000.0, 10.0, 10.0, 'OPEN', 'UNKNOWN')
            """,
            (f"Dante{INVERSE_SIG_MARKER}",),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM forward_trades WHERE id=1").fetchone()
        _close_inverse_row_at_market(
            conn,
            row,
            9.5,
            "V_RECOVERY_KILL_SWITCH",
            exit_type=EXIT_TYPE_INVERSE_RECOVERY_KILL,
        )
        out = conn.execute(
            "SELECT status, exit_reason, exit_type, final_ret FROM forward_trades WHERE id=1"
        ).fetchone()
        self.assertEqual(out["exit_reason"], "V_RECOVERY_KILL_SWITCH")
        self.assertEqual(out["exit_type"], EXIT_TYPE_INVERSE_RECOVERY_KILL)
        self.assertNotEqual(str(out["exit_type"] or "").upper(), "UNKNOWN")
        self.assertAlmostEqual(float(out["final_ret"]), -5.0, places=4)
        conn.close()


if __name__ == "__main__":
    unittest.main()

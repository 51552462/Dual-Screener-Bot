"""forward_trades 식별자 진단·백필 단위 테스트."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime

import pandas as pd
import pytz

from forward.forward_trade_identity import (
    IdentityDiagnosticReport,
    backfill_forward_trade_names,
    build_name_lookup,
    classify_identity_row,
    diagnose_forward_trade_identity,
    is_blank_identity_name,
    normalize_ticker_code,
    resolve_trade_name,
)


def _mk_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT, market TEXT, code TEXT, name TEXT,
            status TEXT, exit_date TEXT, final_ret REAL, flow_tags TEXT, sig_type TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE KR_005930 (date TEXT, Close REAL)
        """
    )
    return conn


class TestIdentityHelpers(unittest.TestCase):
    def test_blank_names(self):
        self.assertTrue(is_blank_identity_name(None))
        self.assertTrue(is_blank_identity_name("nan"))
        self.assertTrue(is_blank_identity_name("종목미상"))
        self.assertFalse(is_blank_identity_name("삼성전자"))

    def test_normalize_kr_code(self):
        self.assertEqual(normalize_ticker_code("KR", "5930"), "005930")
        self.assertEqual(normalize_ticker_code("US", "brk.b"), "BRK-B")

    def test_classify(self):
        self.assertEqual(classify_identity_row("삼성", "005930", market="KR"), "ok")
        self.assertEqual(classify_identity_row("", "005930", market="KR"), "name_missing_code_ok")
        self.assertEqual(classify_identity_row("", "", market="KR"), "both_missing")


class TestLookupAndBackfill(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = _mk_db()
        anchor = "2026-06-20"
        rows = [
            (1, anchor, "KR", "005930", "Samsung", "CLOSED", anchor, 2.0, "#tag", "SIG"),
            (2, anchor, "KR", "000660", "", "CLOSED", anchor, -4.0, "#tag", "SIG"),
            (5, anchor, "KR", "000660", "SK", "CLOSED", anchor, 1.0, "#tag", "SIG"),
            (3, anchor, "KR", "", "name_only", "CLOSED", anchor, 1.0, "#test", "SIG"),
            (4, anchor, "KR", "035420", None, "OPEN", "", None, "", "SIG"),
        ]
        for r in rows:
            self.conn.execute(
                """
                INSERT INTO forward_trades
                (id, entry_date, market, code, name, status, exit_date, final_ret, flow_tags, sig_type)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                r,
            )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_ledger_lookup_resolves_blank_name(self):
        _combined, _stats, ledger, _uni, tables = build_name_lookup(self.conn, "KR")
        nm, src = resolve_trade_name(
            "KR",
            "000660",
            "",
            ledger=ledger,
            universe={},
            table_codes=tables,
        )
        self.assertEqual(nm, "SK")
        self.assertEqual(src, "ledger")

    def test_backfill_dry_run_then_apply(self):
        ref = pytz.timezone("Asia/Seoul").localize(datetime(2026, 6, 23, 9, 0))
        dry = backfill_forward_trade_names(
            self.conn,
            "KR",
            dry_run=True,
            only_window=False,
            rolling_days=90,
            ref_kst=ref,
        )
        self.assertGreaterEqual(dry.candidates, 1)

        applied = backfill_forward_trade_names(
            self.conn,
            "KR",
            dry_run=False,
            only_window=False,
            rolling_days=90,
            ref_kst=ref,
        )
        self.assertGreater(applied.updated, 0)
        row = self.conn.execute(
            "SELECT name FROM forward_trades WHERE code='000660'"
        ).fetchone()
        self.assertEqual(row[0], "SK")

    def test_diagnose_flow_tag_gap_summary(self):
        ref = pytz.timezone("Asia/Seoul").localize(datetime(2026, 6, 23, 9, 0))
        rep = diagnose_forward_trade_identity(
            self.conn,
            "KR",
            rolling_days=90,
            ref_kst=ref,
            row_limit=10,
        )
        self.assertIsInstance(rep, IdentityDiagnosticReport)
        self.assertGreaterEqual(rep.n_gap_all, 2)
        tags = {ft.tag for ft in rep.flow_tag_gaps}
        self.assertIn("#tag", tags)


if __name__ == "__main__":
    unittest.main()

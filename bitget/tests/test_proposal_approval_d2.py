"""D-2 — LLM proposal human approval gate."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

from bitget.governance import proposal_approval_bg as pa
from bitget.governance.ai_proposal_schema_bg import ensure_llm_proposals_schema
from bitget.infra import config_manager as cm


def _insert_proposal(
    db_path: str,
    *,
    risk_class: str = "low",
    category: str = "CAT-M",
    params: dict | None = None,
) -> int:
    ensure_llm_proposals_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            """
            INSERT INTO bitget_llm_proposals (
                recorded_at, category, risk_class, rationale, params_json, payload_json
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                "2026-08-04T00:00:00+00:00",
                category,
                risk_class,
                "test rationale",
                json.dumps(params or {}),
                "{}",
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


class TestProposalApprovalD2(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self._market_db = os.path.join(self._td.name, "market.sqlite")
        self._cfg_path = os.path.join(self._td.name, "config.sqlite")
        sqlite3.connect(self._market_db).close()

        self._cfg_patch = mock.patch.object(cm, "CONFIG_DB_PATH", self._cfg_path)
        self._cfg_patch.start()
        self.addCleanup(self._cfg_patch.stop)
        self.addCleanup(self._td.cleanup)

        self._chat_patch = mock.patch.object(
            pa, "allowed_report_bot_chat_ids", return_value={"12345"}
        )
        self._chat_patch.start()
        self.addCleanup(self._chat_patch.stop)

        cm.set_config_value("DYNAMIC_KELLY_RISK", 0.01)
        cm.set_config_value("MAX_LEVERAGE", 5)

    def test_approve_records_event_and_applies_config(self) -> None:
        pid = _insert_proposal(
            self._market_db,
            params={"DYNAMIC_KELLY_RISK": 0.025},
        )
        ref = pa.proposal_public_ref(pid)
        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            out = pa.process_proposal_telegram_command(
                f"/proposal_approve {ref}",
                chat_id="12345",
                telegram_user_id="user1",
                market_db_path=self._market_db,
            )
        self.assertTrue(out["ok"])
        self.assertEqual(out["action"], "approve")
        self.assertAlmostEqual(float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.025)

        conn = sqlite3.connect(self._market_db)
        try:
            row = conn.execute(
                "SELECT event_type, telegram_user_id FROM bitget_llm_proposal_approvals"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], "approve")
        self.assertEqual(row[1], "user1")

    def test_reject_does_not_touch_config(self) -> None:
        pid = _insert_proposal(self._market_db, params={"DYNAMIC_KELLY_RISK": 0.04})
        ref = pa.proposal_public_ref(pid)
        before = float(cm.get_config_value("DYNAMIC_KELLY_RISK"))

        out = pa.process_proposal_telegram_command(
            f"/proposal_reject {ref}",
            chat_id="12345",
            telegram_user_id="user1",
            market_db_path=self._market_db,
        )

        self.assertTrue(out["ok"])
        self.assertEqual(out["action"], "reject")
        self.assertAlmostEqual(float(cm.get_config_value("DYNAMIC_KELLY_RISK")), before)

        conn = sqlite3.connect(self._market_db)
        try:
            row = conn.execute(
                "SELECT event_type FROM bitget_llm_proposal_approvals"
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], "reject")

    def test_unauthorized_chat_ignored(self) -> None:
        pid = _insert_proposal(self._market_db, params={"DYNAMIC_KELLY_RISK": 0.04})
        ref = pa.proposal_public_ref(pid)
        before = float(cm.get_config_value("DYNAMIC_KELLY_RISK"))

        out = pa.process_proposal_telegram_command(
            f"/proposal_approve {ref}",
            chat_id="99999",
            telegram_user_id="user1",
            market_db_path=self._market_db,
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["reason"], "unauthorized")
        self.assertAlmostEqual(float(cm.get_config_value("DYNAMIC_KELLY_RISK")), before)

        conn = sqlite3.connect(self._market_db)
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_llm_proposal_approvals'"
            ).fetchall()
        finally:
            conn.close()
        self.assertEqual(tables, [])

    def test_duplicate_approve_ignored(self) -> None:
        pid = _insert_proposal(self._market_db, params={"TREASURY_SPOT_USDT": 100.0})
        ref = pa.proposal_public_ref(pid)

        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            first = pa.process_proposal_telegram_command(
                f"/proposal_approve {ref}",
                chat_id="12345",
                telegram_user_id="user1",
                market_db_path=self._market_db,
            )
            second = pa.process_proposal_telegram_command(
                f"/proposal_approve {ref}",
                chat_id="12345",
                telegram_user_id="user2",
                market_db_path=self._market_db,
            )

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertEqual(second["reason"], "duplicate_ignored")

        conn = sqlite3.connect(self._market_db)
        try:
            n = conn.execute("SELECT COUNT(*) FROM bitget_llm_proposal_approvals").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(int(n), 1)

    def test_partial_apply_a5_bounds(self) -> None:
        pid = _insert_proposal(
            self._market_db,
            params={"DYNAMIC_KELLY_RISK": 0.025, "MAX_LEVERAGE": 25},
        )
        ref = pa.proposal_public_ref(pid)

        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            out = pa.process_proposal_telegram_command(
                f"/proposal_approve {ref}",
                chat_id="12345",
                telegram_user_id="user1",
                market_db_path=self._market_db,
            )

        self.assertTrue(out["ok"])
        keys = out["apply"]["keys"]
        self.assertEqual(keys["DYNAMIC_KELLY_RISK"]["status"], "applied")
        self.assertEqual(keys["MAX_LEVERAGE"]["status"], "rejected")
        self.assertAlmostEqual(float(cm.get_config_value("DYNAMIC_KELLY_RISK")), 0.025)
        self.assertAlmostEqual(float(cm.get_config_value("MAX_LEVERAGE")), 5.0)

    def test_gate_disabled_no_op(self) -> None:
        pid = _insert_proposal(self._market_db, params={"TREASURY_SPOT_USDT": 200.0})
        ref = pa.proposal_public_ref(pid)

        with mock.patch.dict(os.environ, {"AI_PROPOSAL_APPROVAL_GATE_ENABLED": "0"}):
            out = pa.process_proposal_telegram_command(
                f"/proposal_approve {ref}",
                chat_id="12345",
                telegram_user_id="user1",
                market_db_path=self._market_db,
            )

        self.assertFalse(out["ok"])
        self.assertEqual(out["reason"], "disabled")

        conn = sqlite3.connect(self._market_db)
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_llm_proposal_approvals'"
            ).fetchall()
        finally:
            conn.close()
        self.assertEqual(tables, [])

    def test_critical_rejects_prefix_match(self) -> None:
        pid = _insert_proposal(
            self._market_db,
            risk_class="critical",
            category="CAT-F",
            params={"TREASURY_SPOT_USDT": 50.0},
        )
        ref = pa.proposal_public_ref(pid)
        prefix = ref[:4]

        with mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            bad = pa.process_proposal_telegram_command(
                f"/proposal_approve {prefix}",
                chat_id="12345",
                telegram_user_id="user1",
                market_db_path=self._market_db,
            )
            good = pa.process_proposal_telegram_command(
                f"/proposal_approve {ref}",
                chat_id="12345",
                telegram_user_id="user1",
                market_db_path=self._market_db,
            )

        self.assertFalse(bad["ok"])
        self.assertTrue(good["ok"])

    def test_approvals_table_append_only_insert(self) -> None:
        pa.ensure_proposal_approvals_schema(self._market_db)
        pa.record_approval_decision(1, "reject", "u1", db_path=self._market_db)
        pa.record_approval_decision(1, "approve", "u2", db_path=self._market_db)

        conn = sqlite3.connect(self._market_db)
        try:
            n = conn.execute("SELECT COUNT(*) FROM bitget_llm_proposal_approvals").fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(int(n), 2)

    def test_derived_status_pending_until_event(self) -> None:
        pid = _insert_proposal(self._market_db)
        self.assertEqual(pa.get_proposal_status(pid, db_path=self._market_db), "pending")
        pa.record_approval_decision(pid, "reject", "u1", db_path=self._market_db)
        self.assertEqual(pa.get_proposal_status(pid, db_path=self._market_db), "rejected")


if __name__ == "__main__":
    unittest.main()

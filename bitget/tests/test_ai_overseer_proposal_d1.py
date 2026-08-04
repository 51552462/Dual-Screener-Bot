"""D-1 — structured LLM proposal validate/persist (config_kv 미접촉)."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

from bitget.governance.ai_proposal_schema_bg import (
    ProposalError,
    ProposalResult,
    ai_proposal_structured_enabled,
    persist_proposal_bg,
    process_structured_llm_proposal,
    validate_llm_proposal,
)
from bitget.infra import config_manager as cm
from bitget.infra import ops_logger


def _valid_proposal_text(
    *,
    category: str = "CAT-F",
    params: dict | None = None,
    rationale: str = "Reduce Kelly during elevated vol.",
    risk_class: str = "low",
) -> str:
    payload = {
        "category": category,
        "params": params if params is not None else {"DYNAMIC_KELLY_RISK": 0.02},
        "rationale": rationale,
        "risk_class": risk_class,
    }
    return "```json\n" + json.dumps(payload) + "\n```"


class TestAiOverseerProposalD1(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self._cfg_path = os.path.join(self._td.name, "bitget_system_config.sqlite")
        self._db_path = os.path.join(self._td.name, "market.sqlite")
        sqlite3.connect(self._db_path).close()
        self._ops_path = os.path.join(self._td.name, "ops.sqlite")

        self._cfg_patch = mock.patch.object(cm, "CONFIG_DB_PATH", self._cfg_path)
        self._cfg_patch.start()
        self.addCleanup(self._cfg_patch.stop)

        self._ops_patch = mock.patch.object(ops_logger, "OPS_EVENTS_DB_PATH", self._ops_path)
        self._ops_health_patch = mock.patch.object(
            ops_logger, "OPS_HEALTH_DB_PATH", self._ops_path
        )
        self._ops_bot_patch = mock.patch.object(ops_logger, "_BOT_DIR", self._td.name)
        self._ops_patch.start()
        self._ops_health_patch.start()
        self._ops_bot_patch.start()
        self.addCleanup(self._ops_patch.stop)
        self.addCleanup(self._ops_health_patch.stop)
        self.addCleanup(self._ops_bot_patch.stop)
        self.addCleanup(self._td.cleanup)

        cm.set_config_value("DYNAMIC_KELLY_RISK", 0.01)

    def test_valid_proposal_persisted(self) -> None:
        raw = _valid_proposal_text()
        outcome = validate_llm_proposal(raw)
        self.assertIsInstance(outcome, ProposalResult)
        persist_proposal_bg(outcome.proposal, db_path=self._db_path, source_text=raw)

        conn = sqlite3.connect(self._db_path)
        try:
            row = conn.execute(
                """
                SELECT category, risk_class, rationale, params_json
                FROM bitget_llm_proposals
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], "CAT-F")
        self.assertEqual(row[1], "critical")
        self.assertEqual(row[2], "Reduce Kelly during elevated vol.")
        params = json.loads(row[3])
        self.assertEqual(params["DYNAMIC_KELLY_RISK"], 0.02)

    def test_parse_fail_no_persist_config_kv_untouched_ops_event_recorded(self) -> None:
        before = float(cm.get_config_value("DYNAMIC_KELLY_RISK"))
        alerts: list[str] = []

        result = process_structured_llm_proposal(
            '{"category": "CAT-F", "params": {}, "rationale": "x"',
            db_path=self._db_path,
            telegram_alert=alerts.append,
        )

        self.assertIsNone(result)
        self.assertAlmostEqual(float(cm.get_config_value("DYNAMIC_KELLY_RISK")), before)
        self.assertEqual(len(alerts), 1)

        conn = sqlite3.connect(self._db_path)
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_llm_proposals'"
            ).fetchall()
        finally:
            conn.close()
        self.assertEqual(tables, [])

        ops_conn = sqlite3.connect(self._ops_path)
        try:
            row = ops_conn.execute(
                """
                SELECT event, payload_json
                FROM ops_events
                WHERE event = ?
                """,
                ("llm_proposal_parse_error",),
            ).fetchone()
        finally:
            ops_conn.close()

        self.assertIsNotNone(row)
        payload = json.loads(row[1])
        self.assertEqual(payload["code"], "parse_error")

    def test_disabled_is_no_op(self) -> None:
        with mock.patch.dict(os.environ, {"AI_PROPOSAL_STRUCTURED_ENABLED": "0"}, clear=False):
            self.assertFalse(ai_proposal_structured_enabled())
            result = process_structured_llm_proposal(
                _valid_proposal_text(),
                db_path=self._db_path,
            )
        self.assertIsNone(result)

        conn = sqlite3.connect(self._db_path)
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_llm_proposals'"
            ).fetchall()
        finally:
            conn.close()
        self.assertEqual(tables, [])

    def test_server_risk_class_overrides_llm_value(self) -> None:
        raw = _valid_proposal_text(category="CAT-M", risk_class="critical")
        outcome = validate_llm_proposal(raw)
        self.assertIsInstance(outcome, ProposalResult)
        self.assertEqual(outcome.proposal["risk_class"], "low")
        self.assertEqual(outcome.proposal["llm_risk_class_ignored"], "critical")

    def test_no_json_block_is_silent_skip(self) -> None:
        alerts: list[str] = []
        with mock.patch(
            "bitget.governance.ai_proposal_schema_bg.record_proposal_parse_error"
        ) as mock_record:
            result = process_structured_llm_proposal(
                "plain audit text without proposal json",
                db_path=self._db_path,
                telegram_alert=alerts.append,
            )
        self.assertIsNone(result)
        self.assertEqual(alerts, [])
        mock_record.assert_not_called()


if __name__ == "__main__":
    unittest.main()

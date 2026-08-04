"""D-2 poll — REPORT_BOT getUpdates wiring."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

from bitget.governance import proposal_approval_bg as pa
from bitget.governance import proposal_approval_poll_bg as poll
from bitget.governance.ai_proposal_schema_bg import ensure_llm_proposals_schema
from bitget.infra import config_manager as cm


def _insert_proposal(db_path: str, *, risk_class: str = "low") -> int:
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
                "CAT-M",
                risk_class,
                "test",
                json.dumps({"TREASURY_SPOT_USDT": 100.0}),
                "{}",
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


class TestProposalApprovalPollD2(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory()
        self._market_db = os.path.join(self._td.name, "market.sqlite")
        self._cfg_path = os.path.join(self._td.name, "config.sqlite")
        self._state_path = os.path.join(self._td.name, "poll_state.json")
        sqlite3.connect(self._market_db).close()

        self._cfg_patch = mock.patch.object(cm, "CONFIG_DB_PATH", self._cfg_path)
        self._cfg_patch.start()
        self.addCleanup(self._cfg_patch.stop)

        self._chat_patch = mock.patch.object(pa, "allowed_report_bot_chat_ids", return_value={"12345"})
        self._chat_patch.start()
        self.addCleanup(self._chat_patch.stop)
        self.addCleanup(self._td.cleanup)

        cm.set_config_value("TREASURY_SPOT_USDT", 50.0)

    def test_poll_disabled_no_fetch(self) -> None:
        with mock.patch.dict(os.environ, {"AI_PROPOSAL_APPROVAL_POLL_ENABLED": "0"}):
            with mock.patch.object(poll, "fetch_report_bot_updates") as mock_fetch:
                out = poll.poll_proposal_approval_updates_once(
                    state_path=self._state_path,
                    token="tok",
                )
        self.assertFalse(out["polled"])
        mock_fetch.assert_not_called()

    def test_getupdates_dispatches_approve_and_replies(self) -> None:
        pid = _insert_proposal(self._market_db)
        ref = pa.proposal_public_ref(pid)
        update = {
            "update_id": 99,
            "message": {
                "message_id": 1,
                "chat": {"id": 12345},
                "from": {"id": 777},
                "text": f"/proposal_approve {ref}",
            },
        }

        with mock.patch.object(
            poll,
            "fetch_report_bot_updates",
            return_value=[update],
        ), mock.patch.object(
            poll,
            "send_report_bot_message",
            return_value=True,
        ) as mock_send, mock.patch(
            "bitget.infra.config_bounds.config_write_validation_enabled",
            return_value=True,
        ):
            out = poll.poll_proposal_approval_updates_once(
                market_db_path=self._market_db,
                state_path=self._state_path,
                token="tok",
            )

        self.assertTrue(out["polled"])
        self.assertEqual(out["last_update_id"], 99)
        self.assertEqual(len(out["handled"]), 1)
        self.assertTrue(out["handled"][0]["ok"])
        mock_send.assert_called_once()
        self.assertAlmostEqual(float(cm.get_config_value("TREASURY_SPOT_USDT")), 100.0)
        self.assertEqual(poll.load_poll_offset(state_path=self._state_path), 99)

    def test_unauthorized_chat_no_reply(self) -> None:
        pid = _insert_proposal(self._market_db)
        ref = pa.proposal_public_ref(pid)
        update = {
            "update_id": 5,
            "message": {
                "chat": {"id": 99999},
                "from": {"id": 1},
                "text": f"/proposal_approve {ref}",
            },
        }

        with mock.patch.object(
            poll, "fetch_report_bot_updates", return_value=[update]
        ), mock.patch.object(poll, "send_report_bot_message") as mock_send:
            out = poll.poll_proposal_approval_updates_once(
                market_db_path=self._market_db,
                state_path=self._state_path,
                token="tok",
            )

        self.assertEqual(len(out["handled"]), 1)
        self.assertFalse(out["handled"][0]["ok"])
        mock_send.assert_not_called()

    def test_non_proposal_update_ignored(self) -> None:
        update = {
            "update_id": 2,
            "message": {
                "chat": {"id": 12345},
                "from": {"id": 1},
                "text": "/start",
            },
        }
        with mock.patch.object(
            poll, "fetch_report_bot_updates", return_value=[update]
        ), mock.patch.object(poll, "send_report_bot_message") as mock_send:
            out = poll.poll_proposal_approval_updates_once(
                state_path=self._state_path,
                token="tok",
            )
        self.assertEqual(out["handled"], [])
        mock_send.assert_not_called()
        self.assertEqual(poll.load_poll_offset(state_path=self._state_path), 2)


if __name__ == "__main__":
    unittest.main()

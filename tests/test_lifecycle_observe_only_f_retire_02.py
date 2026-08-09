"""F-RETIRE-02 — COOLED/RETIRED lifecycle observe-only."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from lifecycle_observe_only import (
    _OBSERVE_TAG,
    apply_lifecycle_observe_only_entry_zero_notional,
    evaluate_lifecycle_observe_only_redemption,
    fetch_lifecycle_observe_only_closed_rows,
    is_lifecycle_observe_only_group,
    is_lifecycle_observe_only_row,
    is_within_observe_only_retention,
    resolve_observe_only_retention_days,
    try_promote_lifecycle_observe_only_redemption,
)
from re_evolution_strike_guard import apply_shadow_entry_zero_notional


def _meta_with_registry(rows: list[dict]) -> dict:
    return {"META_STRATEGY_REGISTRY": rows}


class TestLifecycleObserveOnlyRow(unittest.TestCase):
    def test_cooled_is_observe_row(self):
        self.assertTrue(
            is_lifecycle_observe_only_row({"state": "COOLED", "market": "KR"})
        )

    def test_retired_is_observe_row(self):
        self.assertTrue(
            is_lifecycle_observe_only_row({"state": "RETIRED", "market": "US"})
        )

    def test_live_is_not(self):
        self.assertFalse(
            is_lifecycle_observe_only_row({"state": "LIVE", "market": "KR"})
        )

    def test_bg_excluded(self):
        self.assertFalse(
            is_lifecycle_observe_only_row({"state": "COOLED", "market": "BG"})
        )


class TestZeroNotionalTag(unittest.TestCase):
    def test_a_lifecycle_tag_and_zero_notional(self):
        sig, shares, invest, kelly = apply_lifecycle_observe_only_entry_zero_notional(
            "RANK_A breakout",
            strategy_id="sid_kr_rank_a",
        )
        self.assertIn(_OBSERVE_TAG, sig)
        self.assertIn("OBSERVE_ONLY", sig)
        self.assertEqual(shares, 0)
        self.assertEqual(invest, 0.0)
        self.assertEqual(kelly, 0.0)

    def test_d_shadow_tag_namespace_separate(self):
        shadow_sig, _, _, _ = apply_shadow_entry_zero_notional(
            "RANK_A breakout", strategy_id="sid_a"
        )
        life_sig, _, _, _ = apply_lifecycle_observe_only_entry_zero_notional(
            "RANK_A breakout", strategy_id="sid_a"
        )
        self.assertIn("RE_EVOL_SHADOW", shadow_sig)
        self.assertNotIn("LIFECYCLE_OBSERVE_ONLY", shadow_sig)
        self.assertIn("LIFECYCLE_OBSERVE_ONLY", life_sig)
        self.assertNotIn("RE_EVOL_SHADOW", life_sig)


class TestRetention(unittest.TestCase):
    def test_b_kr_30_us_90_defaults(self):
        self.assertEqual(resolve_observe_only_retention_days("KR", {}), 30)
        self.assertEqual(resolve_observe_only_retention_days("US", {}), 90)

    def test_b_kr_expired_stops_active(self):
        old = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
        row = {
            "state": "COOLED",
            "market": "KR",
            "lifecycle_observe_only_started_at": old,
        }
        self.assertFalse(is_within_observe_only_retention(row, sys_config={}))

    def test_b_us_still_active_at_89d(self):
        recent = (datetime.now(timezone.utc) - timedelta(days=89)).isoformat()
        row = {
            "state": "RETIRED",
            "market": "US",
            "lifecycle_observe_only_started_at": recent,
        }
        self.assertTrue(is_within_observe_only_retention(row, sys_config={}))

    def test_group_lookup_respects_retention(self):
        old = (datetime.now(timezone.utc) - timedelta(days=95)).isoformat()
        meta = _meta_with_registry(
            [
                {
                    "market": "US",
                    "group_key": "MEGA",
                    "state": "RETIRED",
                    "lifecycle_observe_only_started_at": old,
                }
            ]
        )
        self.assertFalse(
            is_lifecycle_observe_only_group(meta, "US", "MEGA", sys_config={})
        )


class TestRedemptionPromotion(unittest.TestCase):
    def test_c_promotes_to_candidate_not_live(self):
        row = {
            "strategy_id": "kr:rank_a",
            "market": "KR",
            "group_key": "RANK_A",
            "state": "COOLED",
            "lifecycle_observe_only_started_at": datetime.now(timezone.utc).isoformat(),
        }
        gate_detail = {
            "passes": True,
            "n_closed": 20,
            "win_rate": 0.6,
            "shadow_stats": {"n_closed": 20, "win_rate": 0.6},
        }
        with patch(
            "lifecycle_observe_only.evaluate_lifecycle_observe_only_redemption",
            return_value=(True, gate_detail),
        ):
            promoted, _ = try_promote_lifecycle_observe_only_redemption(
                row, sys_config={"LIFECYCLE_OBSERVE_ONLY_ENABLED": True}
            )
        self.assertTrue(promoted)
        self.assertEqual(row["state"], "CANDIDATE")
        self.assertNotEqual(row["state"], "LIVE")
        self.assertEqual(row["promote_reason"], "lifecycle_observe_redemption")
        self.assertEqual(float(row["capital_mult"]), 0.0)


class TestFetchClosedRowsNamespace(unittest.TestCase):
    def test_d_fetch_excludes_re_evol_shadow(self):
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
            path = tmp.name
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                """
                CREATE TABLE forward_trades (
                    sig_type TEXT, final_ret REAL, exit_date TEXT, entry_date TEXT,
                    invest_amount REAL, sim_kelly_invest REAL, market TEXT, status TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO forward_trades VALUES
                (?, 1.5, '2026-08-01', '2026-08-01', 0, 0, 'KR', 'CLOSED'),
                (?, 2.0, '2026-08-01', '2026-08-01', 0, 0, 'KR', 'CLOSED')
                """,
                (
                    "[OBSERVE_ONLY][LIFECYCLE_OBSERVE_ONLY][sid] RANK_A",
                    "[OBSERVE_ONLY][RE_EVOL_SHADOW][sid] RANK_A",
                ),
            )
            conn.commit()
            conn.close()

            rows = fetch_lifecycle_observe_only_closed_rows(
                "KR", "RANK_A", lookback_days=30, db_path=path
            )
            self.assertEqual(len(rows), 1)
            self.assertIn("LIFECYCLE_OBSERVE_ONLY", rows[0]["sig_type"])
            self.assertNotIn("RE_EVOL_SHADOW", rows[0]["sig_type"])
        finally:
            import os

            try:
                os.unlink(path)
            except OSError:
                pass


class TestFGateBypassScope(unittest.TestCase):
    """F-GATE-01은 observe_only 예외 없음 — bypass는 forward/shared 오케스트레이션만."""

    def test_registry_state_block_unaffected_by_observe_config(self):
        from meta_treasury_entry_guard import evaluate_meta_group_entry_gate

        meta = _meta_with_registry(
            [
                {
                    "market": "KR",
                    "group_key": "RANK_A",
                    "state": "COOLED",
                }
            ]
        )
        gg = evaluate_meta_group_entry_gate(
            meta,
            "RANK_A",
            market="KR",
            sys_config={"LIFECYCLE_OBSERVE_ONLY_ENABLED": True},
        )
        self.assertTrue(gg.get("block_entry"))
        self.assertEqual(gg.get("source"), "registry_state_block")

    def test_real_notional_path_still_blocked_at_gate(self):
        """observe 활성 그룹이라도 evaluate_meta_group_entry_gate는 항상 block."""
        from meta_treasury_entry_guard import (
            evaluate_meta_group_entry_gate,
            resolve_registry_state_block,
        )

        rows = [
            {
                "market": "US",
                "group_key": "MEGA",
                "state": "RETIRED",
                "lifecycle_observe_only_started_at": datetime.now(
                    timezone.utc
                ).isoformat(),
            }
        ]
        blocked, reason = resolve_registry_state_block(
            "US", "MEGA", registry_rows=rows
        )
        self.assertTrue(blocked)
        self.assertEqual(reason, "registry_state_block")

        meta = _meta_with_registry(rows)
        gg = evaluate_meta_group_entry_gate(meta, "MEGA", market="US")
        self.assertTrue(gg.get("block_entry"))
        self.assertEqual(gg.get("source"), "registry_state_block")


class TestRedemptionDisabled(unittest.TestCase):
    def test_disabled_returns_false(self):
        row = {"state": "COOLED", "market": "KR"}
        ok, detail = evaluate_lifecycle_observe_only_redemption(
            row, sys_config={"LIFECYCLE_OBSERVE_ONLY_ENABLED": False}
        )
        self.assertFalse(ok)
        self.assertEqual(detail.get("reason"), "disabled")


if __name__ == "__main__":
    unittest.main()

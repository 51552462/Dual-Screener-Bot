"""Ch.5 — MetaGovernor / Treasury 진입 가드 테스트."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from meta_treasury_entry_guard import (
    build_meta_treasury_dossier_extras,
    count_zero_group_entry_hits,
    detect_meta_treasury_audit_anomalies,
    evaluate_meta_global_entry_gate,
    evaluate_meta_group_entry_gate,
    extract_core_group_name,
    governor_freshness,
    resolve_group_treasury_mult,
    summarize_treasury_health,
)


def _meta_base(**overrides) -> dict:
    base = {
        "META_TREASURY_MODE": "NORMAL",
        "META_OPERATOR_FLAGS": {},
        "META_REGIME_ACTION": {
            "block_trade_sources": [],
            "allow_trade_sources": [],
        },
        "META_GROUP_KELLY_MULT": {},
        "META_STRATEGY_HEALTH": {"__meta__": {"window_days_kst": 18}},
        "META_GOVERNOR_LAST_RUN_AT": datetime.now(timezone.utc).isoformat(),
        "META_GOVERNOR_LAST_RUN_STATUS": "OK",
    }
    base.update(overrides)
    return base


class TestExtractCoreGroup(unittest.TestCase):
    def test_strips_headers(self):
        sig = "[STANDARD] RANK_A [🔥주도주 편대] [🌟시계열]"
        self.assertEqual(extract_core_group_name(sig), "RANK_A")


class TestGlobalEntryGate(unittest.TestCase):
    def test_kill_switch_blocks(self):
        meta = _meta_base(META_OPERATOR_FLAGS={"KILL_SWITCH": True})
        ev = evaluate_meta_global_entry_gate(meta, "STANDARD", sys_config={})
        self.assertTrue(ev["block_entry"])
        self.assertEqual(ev["code"], "KILL_SWITCH")

    def test_defense_blocks(self):
        meta = _meta_base(
            META_TREASURY_MODE="DEFENSE",
            META_STRATEGY_HEALTH={
                "__meta__": {},
                "KR|RANK_A": {"n": 12, "mult": 0.0},
            },
        )
        ev = evaluate_meta_global_entry_gate(meta, "STANDARD", sys_config={})
        self.assertTrue(ev["block_entry"])
        self.assertEqual(ev["code"], "TREASURY_DEFENSE")

    def test_block_trade_source(self):
        meta = _meta_base(
            META_REGIME_ACTION={
                "block_trade_sources": ["SUPERNOVA"],
                "allow_trade_sources": [],
            }
        )
        ev = evaluate_meta_global_entry_gate(meta, "supernova", sys_config={})
        self.assertTrue(ev["block_entry"])
        self.assertEqual(ev["code"], "BLOCK_TRADE_SOURCE")

    def test_allow_whitelist_denies(self):
        meta = _meta_base(
            META_REGIME_ACTION={
                "block_trade_sources": [],
                "allow_trade_sources": ["STANDARD"],
            }
        )
        ev = evaluate_meta_global_entry_gate(meta, "RND", sys_config={})
        self.assertTrue(ev["block_entry"])
        self.assertEqual(ev["code"], "ALLOW_TRADE_SOURCE_DENY")

    def test_disabled_guard_passes(self):
        meta = _meta_base(META_OPERATOR_FLAGS={"KILL_SWITCH": True})
        ev = evaluate_meta_global_entry_gate(
            meta,
            "STANDARD",
            sys_config={"ENABLE_META_TREASURY_ENTRY_GUARD": False},
        )
        self.assertFalse(ev["block_entry"])


class TestGroupEntryGate(unittest.TestCase):
    def test_zero_group_mult_blocks(self):
        meta = _meta_base(META_GROUP_KELLY_MULT={"RANK_A": 0.0})
        ev = evaluate_meta_group_entry_gate(
            meta, "RANK_A", market="KR", sys_config={}
        )
        self.assertTrue(ev["block_entry"])
        self.assertEqual(ev["group_mult"], 0.0)

    def test_positive_mult_passes(self):
        meta = _meta_base(META_GROUP_KELLY_MULT={"RANK_A": 0.5})
        ev = evaluate_meta_group_entry_gate(
            meta, "RANK_A", market="KR", sys_config={}
        )
        self.assertFalse(ev["block_entry"])


class TestTreasuryHealthSummary(unittest.TestCase):
    def test_counts_zeroed_actionable(self):
        meta = _meta_base(
            META_TREASURY_MODE="DEFENSE",
            META_STRATEGY_HEALTH={
                "__meta__": {},
                "KR|GOOD": {"n": 15, "mult": 1.0},
                "KR|BAD": {"n": 20, "mult": 0.0},
                "KR|LOW_N": {"n": 3, "mult": 0.0},
            },
        )
        s = summarize_treasury_health(meta)
        self.assertEqual(s["actionable_groups"], 2)
        self.assertEqual(s["zeroed_groups"], 1)
        self.assertEqual(s["treasury_mode"], "DEFENSE")


class TestGovernorFreshness(unittest.TestCase):
    def test_stale_when_old(self):
        old = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        meta = _meta_base(
            META_GOVERNOR_LAST_RUN_AT=old,
            META_GOVERNOR_LAST_RUN_STATUS="OK",
        )
        f = governor_freshness(meta, stale_hours=24.0)
        self.assertTrue(f["is_stale"])

    def test_fresh_when_recent(self):
        meta = _meta_base()
        f = governor_freshness(meta, stale_hours=24.0)
        self.assertFalse(f["is_stale"])


class TestZeroGroupEntryHits(unittest.TestCase):
    def test_counts_matching_entries(self):
        meta = _meta_base(META_GROUP_KELLY_MULT={"RANK_A": 0.0, "RANK_B": 1.0})
        df = pd.DataFrame(
            {
                "sig_type": [
                    "[STANDARD] RANK_A [🔥]",
                    "[STANDARD] RANK_B [🔥]",
                ]
            }
        )
        self.assertEqual(count_zero_group_entry_hits(df, meta), 1)


class TestAuditAnomalies(unittest.TestCase):
    def test_kill_switch_leak(self):
        hits = detect_meta_treasury_audit_anomalies(
            kill_switch_active=True,
            treasury_mode="NORMAL",
            treasury_zeroed_groups=0,
            treasury_actionable_groups=5,
            governor_is_stale=False,
            governor_hours_since_run=1.0,
            trades_entry_today=3,
            trades_closed_today=0,
            win_rate_today_pct=None,
            catastrophic_clutch_active=False,
            zero_group_entry_hits=0,
            block_trade_sources=(),
        )
        codes = [h["code"] for h in hits]
        self.assertIn("KILL_SWITCH_LEAK", codes)

    def test_treasury_catastrophic_split(self):
        hits = detect_meta_treasury_audit_anomalies(
            kill_switch_active=False,
            treasury_mode="NORMAL",
            treasury_zeroed_groups=0,
            treasury_actionable_groups=8,
            governor_is_stale=False,
            governor_hours_since_run=1.0,
            trades_entry_today=2,
            trades_closed_today=13,
            win_rate_today_pct=0.0,
            catastrophic_clutch_active=True,
            zero_group_entry_hits=0,
            block_trade_sources=(),
        )
        codes = [h["code"] for h in hits]
        self.assertIn("TREASURY_CATASTROPHIC_SPLIT", codes)


class TestDossierExtras(unittest.TestCase):
    def test_build_extras(self):
        meta = _meta_base(
            META_OPERATOR_FLAGS={"KILL_SWITCH": False},
            META_GROUP_KELLY_MULT={"RANK_A": 0.0},
        )
        df = pd.DataFrame({"sig_type": ["[STANDARD] RANK_A"]})
        ex = build_meta_treasury_dossier_extras(meta, df_entry_today=df)
        self.assertFalse(ex["kill_switch_active"])
        self.assertEqual(ex["zero_group_entry_hits_today"], 1)


if __name__ == "__main__":
    unittest.main()

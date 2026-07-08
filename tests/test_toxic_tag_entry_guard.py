"""Ch.1 — 진입 독성 태그 가드 + 감사관 진입/청산 분리 집계."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta

import pandas as pd

from forward_flow_tag_deep_dive import (
    extract_flow_tags_from_text,
    resolve_flow_tag_entry_guard,
)
from overseer_audit_binder import (
    _count_toxic_tag_entry_hits,
    _count_toxic_tag_hits,
)


class TestExtractFlowTags(unittest.TestCase):
    def test_sig_type_with_score_suffix(self):
        sig = "ORIGINAL #수급모멘텀(+1.0) #독성태그방어(x0.5:#나쁨)"
        tags = extract_flow_tags_from_text(sig)
        self.assertIn("#수급모멘텀", tags)
        self.assertIn("#독성태그방어", tags)

    def test_dedup_preserves_order(self):
        sig = "#A foo #B #A"
        self.assertEqual(extract_flow_tags_from_text(sig), ["#A", "#B"])


class TestResolveFlowTagEntryGuard(unittest.TestCase):
    def _cfg(self, *, mult: float = 0.5) -> dict:
        today = datetime.now().strftime("%Y-%m-%d")
        tag = "#건전한조정_매집우위"
        return {
            "FLOW_TAG_PENALTY_MULT": {tag: mult},
            "FLOW_TAG_TOXIC_REGISTRY": {
                f"flow_tag:KR:{tag}": {
                    "tag": tag,
                    "market": "KR",
                    "registered_at": today,
                    "kelly_mult": mult,
                }
            },
            "FLOW_TAG_TOXIC_GUARD_DECAY_DAYS": 5,
        }

    def test_no_tags_neutral(self):
        mult, reason = resolve_flow_tag_entry_guard(self._cfg(), "KR", "ORIGINAL LONG")
        self.assertEqual(mult, 1.0)
        self.assertEqual(reason, "")

    def test_matching_entry_tag_penalized(self):
        sig = "ORIGINAL #건전한조정_매집우위"
        mult, reason = resolve_flow_tag_entry_guard(self._cfg(), "KR", sig)
        self.assertAlmostEqual(mult, 0.5)
        self.assertIn("#건전한조정_매집우위", reason)

    def test_expired_registry_uses_penalty_map_only(self):
        tag = "#건전한조정_매집우위"
        old = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        cfg = {
            "FLOW_TAG_PENALTY_MULT": {tag: 0.5},
            "FLOW_TAG_TOXIC_REGISTRY": {
                f"flow_tag:KR:{tag}": {
                    "tag": tag,
                    "market": "KR",
                    "registered_at": old,
                    "kelly_mult": 0.0,
                }
            },
            "FLOW_TAG_TOXIC_GUARD_DECAY_DAYS": 5,
        }
        mult, _ = resolve_flow_tag_entry_guard(cfg, "KR", f"X {tag}")
        self.assertAlmostEqual(mult, 0.5)

    def test_zero_mult_for_block(self):
        tag = "#완전차단"
        cfg = {"FLOW_TAG_PENALTY_MULT": {tag: 0.0}}
        mult, reason = resolve_flow_tag_entry_guard(cfg, "KR", tag)
        self.assertEqual(mult, 0.0)
        self.assertIn(tag, reason)


class TestOverseerToxicHitSplit(unittest.TestCase):
    def setUp(self) -> None:
        self.penalty = {"#건전한조정_매집우위": 0.5}

    def test_entry_hits_sig_type_only(self):
        df = pd.DataFrame(
            [
                {"sig_type": "ORIGINAL #건전한조정_매집우위", "flow_tags": ""},
                {"sig_type": "ORIGINAL #수급모멘텀", "flow_tags": ""},
            ]
        )
        self.assertEqual(_count_toxic_tag_entry_hits(df, self.penalty), 1)

    def test_exit_echo_not_counted_as_entry(self):
        df_entry = pd.DataFrame(
            [{"sig_type": "ORIGINAL #수급모멘텀", "flow_tags": ""}]
        )
        df_closed = pd.DataFrame(
            [
                {
                    "sig_type": "ORIGINAL #수급모멘텀",
                    "flow_tags": "#건전한조정_매집우위",
                }
            ]
        )
        self.assertEqual(_count_toxic_tag_entry_hits(df_entry, self.penalty), 0)
        self.assertEqual(_count_toxic_tag_hits(df_closed, self.penalty), 1)


if __name__ == "__main__":
    unittest.main()

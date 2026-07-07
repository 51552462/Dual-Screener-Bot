"""weekly_evolution pipeline invariants (주식 weekly_master 패리티)."""
from __future__ import annotations

import unittest

from bitget.pipelines.bitget_pipelines import get_pipeline


class TestBitgetWeeklyEvolutionPipeline(unittest.TestCase):
    def test_weekly_evolution_step_order(self):
        names = [s.name for s in get_pipeline("weekly_evolution")]
        self.assertEqual(
            names,
            [
                "config_bootstrap",
                "artifact_guard",
                "weekly_evolution",
                "weekly_coin_regime_archive",
                "weekly_flow_master",
                "genesis_backfill_weekly",
                "weekend_grand_report",
                "weekly_action_plan",
                "weekly_executive_summary",
            ],
        )

    def test_weekly_flow_master_is_critical(self):
        steps = {s.name: s for s in get_pipeline("weekly_evolution")}
        self.assertTrue(steps["weekly_flow_master"].critical)
        self.assertTrue(steps["weekly_evolution"].critical)

    def test_weekly_evolution_before_flow_report(self):
        names = [s.name for s in get_pipeline("weekly_evolution")]
        self.assertLess(names.index("weekly_evolution"), names.index("weekly_flow_master"))


if __name__ == "__main__":
    unittest.main()

"""Phase 7 — pipeline E2E, runner CLI, architecture checks."""
from __future__ import annotations

import os
import unittest
from unittest import mock

from bitget.infra.runtime import StepSpec, dispatch_bitget_mode


class TestPipelineStructure(unittest.TestCase):
    def test_daily_audit_has_prelude_and_pil(self):
        from bitget.pipelines.bitget_pipelines import get_pipeline

        names = [s.name for s in get_pipeline("daily_audit")]
        self.assertEqual(names[:4], ["meta_governor_sync", "artifact_guard", "config_bootstrap", "sentiment_mining"])
        for key in (
            "deep_dive_spot",
            "deep_dive_futures",
            "pil_practitioner_reports",
            "comprehensive_report",
        ):
            self.assertIn(key, names)

    def test_scan_spot_has_meta_prelude(self):
        from bitget.pipelines.bitget_pipelines import get_pipeline

        names = [s.name for s in get_pipeline("scan_spot")]
        self.assertEqual(names[:3], ["meta_governor_sync_scan", "artifact_guard", "config_bootstrap"])


class TestDispatchRuntime(unittest.TestCase):
    def test_dispatch_runs_steps_in_order(self):
        calls: list[str] = []
        pipeline = [
            StepSpec("alpha", lambda: calls.append("alpha"), critical=True),
            StepSpec("beta", lambda: calls.append("beta"), critical=False),
        ]
        report = dispatch_bitget_mode("track_positions", pipeline, skip_telegram=True)
        self.assertEqual(calls, ["alpha", "beta"])
        self.assertEqual(report.status_label, "OK")
        self.assertTrue(report.all_critical_ok)

    def test_dispatch_dry_run_skips_execution(self):
        calls: list[str] = []
        pipeline = [StepSpec("only", lambda: calls.append("only"), critical=True)]
        report = dispatch_bitget_mode(
            "health",
            pipeline,
            skip_telegram=True,
            dry_run=True,
        )
        self.assertEqual(calls, [])
        self.assertEqual(report.steps, [])


class TestRunnerCli(unittest.TestCase):
    def test_runner_health_dry_run_exit_zero(self):
        from bitget.pipelines.runner import run_factory_cli

        rc = run_factory_cli(["--mode", "health", "--dry-run", "--skip-telegram"])
        self.assertEqual(rc, 0)

    def test_runner_cutover_check_mode(self):
        from bitget.pipelines.runner import run_factory_cli

        with mock.patch("builtins.print"):
            rc = run_factory_cli(["--mode", "cutover_check", "--skip-telegram"])
        self.assertIn(rc, (0, 1))


class TestArchitectureChecks(unittest.TestCase):
    def test_legacy_entrypoints_blocked(self):
        from bitget.validation.architecture_checks import check_legacy_entrypoints_blocked

        r = check_legacy_entrypoints_blocked()
        self.assertTrue(r["ok"])
        self.assertTrue(all(r["details"].values()))

    def test_pipeline_structure_ok(self):
        from bitget.validation.architecture_checks import check_pipeline_structure

        r = check_pipeline_structure()
        self.assertTrue(r["ok"], r)

    def test_satellite_no_json_runtime_io(self):
        from bitget.validation.architecture_checks import check_satellite_config_hub

        r = check_satellite_config_hub()
        self.assertTrue(r["ok"], r.get("offenders"))

    def test_config_meta_alignment_with_mock(self):
        from bitget.validation.architecture_checks import check_config_meta_alignment

        cfg = {"CURRENT_REGIME_KEY": "BULL", "DYNAMIC_KELLY_RISK": 0.02}
        meta = {
            "META_REGIME_KEY": "BULL",
            "META_GOVERNOR_LAST_RUN_STATUS": "OK",
            "META_GOVERNOR_LAST_RUN_AT": "2026-06-14T00:00:00+00:00",
        }
        with mock.patch("bitget.config_hub.load_config", return_value=cfg), mock.patch(
            "bitget.governance.meta_consumer.load_meta_state_resolved",
            return_value=meta,
        ), mock.patch(
            "bitget.governance.meta_sync.is_config_regime_misaligned",
            return_value=False,
        ), mock.patch(
            "bitget.governance.meta_sync.is_bitget_meta_degraded",
            return_value=False,
        ):
            r = check_config_meta_alignment()
        self.assertTrue(r["ok"], r)

    def test_cutover_includes_architecture(self):
        from bitget.validation.cutover import check_cutover_readiness

        old = os.environ.get("BITGET_PIPELINE_SSOT")
        try:
            os.environ["BITGET_PIPELINE_SSOT"] = "0"
            r = check_cutover_readiness()
        finally:
            if old is None:
                os.environ.pop("BITGET_PIPELINE_SSOT", None)
            else:
                os.environ["BITGET_PIPELINE_SSOT"] = old
        self.assertIn("architecture", r)
        self.assertIn("architecture_ok", r["checks"])


if __name__ == "__main__":
    unittest.main()

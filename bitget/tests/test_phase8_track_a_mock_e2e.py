"""Phase 8 Track A — mock E2E pipeline + regime/kelly audit (no API/DB required)."""
from __future__ import annotations

import contextlib
import os
import unittest
from unittest import mock

from bitget.infra.runtime import StepSpec, bitget_exit_code, dispatch_bitget_mode


def _daily_audit_ssot_names() -> list[str]:
    from bitget.pipelines.bitget_pipelines import get_pipeline

    return [s.name for s in get_pipeline("daily_audit")]


def _mirror_pipeline(mode: str, recorder: list[str]) -> list[StepSpec]:
    """SSOT step 이름·critical 플래그를 유지한 no-op 파이프라인."""
    from bitget.pipelines.bitget_pipelines import get_pipeline

    real = get_pipeline(mode)
    out: list[StepSpec] = []
    for spec in real:
        name = spec.name

        def _record(step_name: str = name) -> None:
            recorder.append(step_name)

        out.append(
            StepSpec(
                name,
                _record,
                critical=spec.critical,
                delay_after_sec=0.0,
            )
        )
    return out


_DAILY_PATCH_TARGETS = (
    "_step_meta_governor_sync",
    "_step_artifact_guard",
    "_step_config_bootstrap",
    "_step_sentiment",
    "_step_doomsday_radar",
    "_step_report_pipeline_hydrate",
    "_step_track_spot",
    "_step_track_futures",
    "_step_deep_dive_spot",
    "_step_deep_dive_futures",
    "_step_doomsday_bridge_sync",
    "_step_reporter_cleanup_zombie",
    "_step_forward_trade_identity",
    "_step_pil_practitioner_reports",
    "_step_comprehensive_report",
    "_step_executive_summary_daily",
    "_step_genesis_radar_daily",
    "_step_ai_overseer",
    "_step_reconcile",
)

_DAILY_AUDIT_STEP_COUNT = 19


class TestDailyAuditMockE2E(unittest.TestCase):
    def test_mirror_pipeline_full_order(self):
        calls: list[str] = []
        pipeline = _mirror_pipeline("daily_audit", calls)
        report = dispatch_bitget_mode("daily_audit", pipeline, skip_telegram=True)

        expected = [s.name for s in pipeline]
        ssot = _daily_audit_ssot_names()
        self.assertEqual(expected, ssot)
        self.assertEqual(calls, expected)
        self.assertEqual(len(calls), _DAILY_AUDIT_STEP_COUNT)
        self.assertEqual(calls[:4], ["meta_governor_sync", "artifact_guard", "config_bootstrap", "sentiment_mining"])
        self.assertEqual(
            calls[4:],
            [
                "doomsday_radar",
                "report_pipeline_hydrate",
                "track_spot",
                "track_futures",
                "deep_dive_spot",
                "deep_dive_futures",
                "doomsday_bridge_sync",
                "reporter_cleanup_zombie_forward_trades",
                "forward_trade_identity",
                "pil_practitioner_reports",
                "comprehensive_report",
                "executive_summary_daily",
                "genesis_radar_daily",
                "ai_overseer",
                "reconcile",
            ],
        )
        self.assertEqual(report.status_label, "OK")
        self.assertEqual(bitget_exit_code(report), 0)

    def test_critical_failure_aborts_zombie_pipeline(self):
        """주식 factory_runtime 패턴: critical 실패 시 후속 step은 zombie guard로 스킵."""
        calls: list[str] = []

        def _fail() -> None:
            raise RuntimeError("meta_governor_sync aborted")

        pipeline = [
            StepSpec("meta_governor_sync", _fail, critical=True),
            StepSpec("artifact_guard", lambda: calls.append("artifact_guard"), critical=True),
        ]
        report = dispatch_bitget_mode("daily_audit", pipeline, skip_telegram=True)
        self.assertEqual(calls, [])
        self.assertEqual(report.status_label, "FAIL")
        self.assertEqual(bitget_exit_code(report), 1)
        self.assertFalse(report.steps[0].ok)
        self.assertFalse(report.steps[1].ok)
        self.assertIn("skipped", report.steps[1].error or "")

    def test_optional_failure_is_partial(self):
        calls: list[str] = []

        def _ok(name: str):
            return lambda: calls.append(name)

        def _raise_optional() -> None:
            raise RuntimeError("net")

        pipeline = [
            StepSpec("meta_governor_sync", _ok("meta"), critical=True),
            StepSpec("sentiment_mining", _raise_optional, critical=False),
            StepSpec("track_spot", _ok("track"), critical=True),
        ]
        report = dispatch_bitget_mode("daily_audit", pipeline, skip_telegram=True)
        self.assertEqual(calls, ["meta", "track"])
        self.assertEqual(report.status_label, "PARTIAL_FAIL")
        self.assertEqual(bitget_exit_code(report), 0)

    def test_daily_audit_dispatch_visits_all_ssot_steps_without_bodies(self):
        """get_pipeline SSOT step 이름 전체를 run_step mock으로 방문 (API/DB 없음)."""
        from bitget.infra import runtime
        from bitget.infra.runtime import StepResult
        from bitget.pipelines.bitget_pipelines import get_pipeline

        visited: list[str] = []

        def _fake_run_step(spec: StepSpec) -> StepResult:
            visited.append(spec.name)
            return StepResult(name=spec.name, ok=True, critical=spec.critical, elapsed_sec=0.0)

        pipeline = get_pipeline("daily_audit")
        with mock.patch.object(runtime, "run_step", side_effect=_fake_run_step):
            report = dispatch_bitget_mode("daily_audit", pipeline, skip_telegram=True)

        expected = [s.name for s in pipeline]
        self.assertEqual(visited, expected)
        self.assertEqual(visited, _daily_audit_ssot_names())
        self.assertEqual(len(visited), _DAILY_AUDIT_STEP_COUNT)
        self.assertEqual(report.status_label, "OK")


class TestScanMockE2E(unittest.TestCase):
    def test_scan_spot_mirror_order(self):
        calls: list[str] = []
        pipeline = _mirror_pipeline("scan_spot", calls)
        report = dispatch_bitget_mode("scan_spot", pipeline, skip_telegram=True)

        self.assertEqual(
            calls[:3],
            ["meta_governor_sync_scan", "artifact_guard", "config_bootstrap"],
        )
        self.assertEqual(calls[-1], "track_spot")
        self.assertEqual(report.status_label, "OK")

    def test_scan_all_includes_shadow_eval(self):
        from bitget.pipelines.bitget_pipelines import get_pipeline

        names = [s.name for s in get_pipeline("scan_all")]
        self.assertIn("shadow_eval", names)
        self.assertEqual(names[0], "meta_governor_sync_scan")


class TestRegimeKellyAudit(unittest.TestCase):
    def test_audit_passes_aligned_mock(self):
        from bitget.validation.regime_audit import run_regime_kelly_audit

        cfg = {
            "CURRENT_REGIME_KEY": "BULL",
            "DYNAMIC_KELLY_RISK": 0.015,
            "REGIME_ANALYSIS": {"regime_key": "BULL", "updated_at": "2026-06-14"},
        }
        meta = {
            "META_REGIME_KEY": "BULL",
            "META_GOVERNOR_LAST_RUN_STATUS": "OK",
            "META_GOVERNOR_LAST_RUN_AT": "2026-06-14T12:00:00+00:00",
            "META_REGIME_ACTION": {"kelly_cap": 0.02, "kelly_floor": 0.005},
        }
        with mock.patch(
            "bitget.governance.meta_sync.is_config_regime_misaligned",
            return_value=False,
        ), mock.patch(
            "bitget.governance.meta_sync.is_bitget_meta_degraded",
            return_value=False,
        ):
            r = run_regime_kelly_audit(sys_config=cfg, meta=meta)
        self.assertTrue(r["ok"])
        self.assertTrue(r["passed"], r.get("failed"))
        self.assertTrue(r["isolation"]["config_db_is_bitget_sqlite"])

    def test_audit_fails_misaligned_regime(self):
        from bitget.validation.regime_audit import run_regime_kelly_audit

        cfg = {"CURRENT_REGIME_KEY": "BEAR", "DYNAMIC_KELLY_RISK": 0.01, "REGIME_ANALYSIS": {"regime_key": "BEAR"}}
        meta = {
            "META_REGIME_KEY": "BULL",
            "META_GOVERNOR_LAST_RUN_STATUS": "OK",
            "META_GOVERNOR_LAST_RUN_AT": "2026-06-14T12:00:00+00:00",
        }
        with mock.patch(
            "bitget.governance.meta_sync.is_config_regime_misaligned",
            return_value=True,
        ), mock.patch(
            "bitget.governance.meta_sync.is_bitget_meta_degraded",
            return_value=False,
        ):
            r = run_regime_kelly_audit(sys_config=cfg, meta=meta)
        self.assertTrue(r["ok"])
        self.assertFalse(r["passed"])
        self.assertIn("regime_aligned", r["failed"])

    def test_audit_kelly_respects_meta_cap(self):
        from bitget.validation.regime_audit import run_regime_kelly_audit

        cfg = {"CURRENT_REGIME_KEY": "CHOP", "DYNAMIC_KELLY_RISK": 0.05}
        meta = {
            "META_REGIME_KEY": "CHOP",
            "META_GOVERNOR_LAST_RUN_STATUS": "OK",
            "META_GOVERNOR_LAST_RUN_AT": "2026-06-14T12:00:00+00:00",
            "META_REGIME_ACTION": {"kelly_cap": 0.01},
        }
        with mock.patch(
            "bitget.governance.meta_sync.is_config_regime_misaligned",
            return_value=False,
        ), mock.patch(
            "bitget.governance.meta_sync.is_bitget_meta_degraded",
            return_value=False,
        ):
            r = run_regime_kelly_audit(sys_config=cfg, meta=meta)
        self.assertLessEqual(r["kelly"]["resolve_trading_kelly_base"], 0.01)

    def test_audit_live_config_if_present(self):
        from bitget.validation.regime_audit import run_regime_kelly_audit
        from bitget.infra.data_paths import system_config_db_path

        if not os.path.isfile(system_config_db_path()):
            self.skipTest("no bitget config sqlite")
        r = run_regime_kelly_audit()
        self.assertTrue(r["ok"])
        self.assertIn("bitget_system_config.sqlite", r["config_db_path"])


class TestRunnerMockDailyAudit(unittest.TestCase):
    def test_runner_daily_audit_all_steps_mocked_exit_zero(self):
        import bitget.pipelines.bitget_pipelines as bp
        from bitget.pipelines.runner import run_factory_cli

        with contextlib.ExitStack() as stack:
            for attr in _DAILY_PATCH_TARGETS:
                stack.enter_context(mock.patch.object(bp, attr))
            with mock.patch("builtins.print"):
                rc = run_factory_cli(["--mode", "daily_audit", "--skip-telegram"])
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()

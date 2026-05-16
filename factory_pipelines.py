"""
Factory mode → Step 파이프라인 매핑 (순차 실행 SSOT).
"""
from __future__ import annotations

from typing import Dict, List, Sequence

from factory_runtime import StepSpec


# --- Step implementations (lazy import) ---


def _step_supernova_kr() -> None:
    from supernova_hunter import execute_supernova_live_scan

    execute_supernova_live_scan("KR")


def _step_supernova_us() -> None:
    from supernova_hunter import execute_supernova_live_scan

    execute_supernova_live_scan("US")


def _step_kr_bowl_optional() -> None:
    import kr

    kr.scan_market_1d()


def _step_us_bowl_optional() -> None:
    import usa

    usa.scan_market_1d()


def _step_track_kr() -> None:
    from auto_forward_tester import track_daily_positions

    track_daily_positions("KR")


def _step_track_us() -> None:
    from auto_forward_tester import track_daily_positions

    track_daily_positions("US")


def _step_deep_dive_kr() -> None:
    from auto_forward_tester import run_deep_dive_analysis

    run_deep_dive_analysis("KR")


def _step_deep_dive_us() -> None:
    from auto_forward_tester import run_deep_dive_analysis

    run_deep_dive_analysis("US")


def _step_overseer_optional() -> None:
    from ai_overseer import run_ai_auditor

    run_ai_auditor()


def _step_comprehensive_optional() -> None:
    from auto_forward_tester import send_comprehensive_daily_report

    send_comprehensive_daily_report()


def _step_weekly_master() -> None:
    import system_auto_pilot as sap
    from weekly_flow_report import send_weekly_flow_master_report

    send_weekly_flow_master_report(
        db_path=sap.DB_PATH,
        sys_config=sap.load_or_create_config(),
        send_fn=sap.send_telegram_report,
    )


def _pipeline_daily_audit_kr() -> List[StepSpec]:
    return [
        StepSpec("track_daily_positions_kr", _step_track_kr, critical=True, delay_after_sec=3.0),
        StepSpec("deep_dive_kr", _step_deep_dive_kr, critical=True, delay_after_sec=3.0),
        StepSpec("ai_overseer", _step_overseer_optional, critical=False, delay_after_sec=0),
    ]


def _pipeline_daily_audit_us() -> List[StepSpec]:
    return [
        StepSpec("track_daily_positions_us", _step_track_us, critical=True, delay_after_sec=3.0),
        StepSpec("deep_dive_us", _step_deep_dive_us, critical=True, delay_after_sec=3.0),
        StepSpec("ai_overseer", _step_overseer_optional, critical=False, delay_after_sec=0),
    ]


def _pipeline_daily_audit_combined() -> List[StepSpec]:
    """수동 ./factory.sh --daily 용 — KR→US 순차, overseer 1회."""
    return [
        StepSpec("track_daily_positions_kr", _step_track_kr, critical=True, delay_after_sec=3.0),
        StepSpec("deep_dive_kr", _step_deep_dive_kr, critical=True, delay_after_sec=8.0),
        StepSpec("track_daily_positions_us", _step_track_us, critical=True, delay_after_sec=3.0),
        StepSpec("deep_dive_us", _step_deep_dive_us, critical=True, delay_after_sec=3.0),
        StepSpec("ai_overseer", _step_overseer_optional, critical=False),
    ]


def build_factory_pipelines() -> Dict[str, List[StepSpec]]:
    daily_kr = _pipeline_daily_audit_kr()
    daily_us = _pipeline_daily_audit_us()
    return {
        "scan_kr": [
            StepSpec("supernova_scan_kr", _step_supernova_kr, critical=True, delay_after_sec=5.0),
            StepSpec("kr_bowl_scan", _step_kr_bowl_optional, critical=False),
        ],
        "scan_us": [
            StepSpec("supernova_scan_us", _step_supernova_us, critical=True, delay_after_sec=5.0),
            StepSpec("us_bowl_scan", _step_us_bowl_optional, critical=False),
        ],
        "daily_audit_kr": daily_kr,
        "daily_audit_us": daily_us,
        "daily_audit": _pipeline_daily_audit_combined(),
        "weekly_master": [
            StepSpec("weekly_flow_master", _step_weekly_master, critical=True),
        ],
    }


PIPELINE: Dict[str, List[StepSpec]] = build_factory_pipelines()
FACTORY_MODES = frozenset(PIPELINE.keys())


def get_pipeline(mode: str) -> Sequence[StepSpec]:
    key = str(mode).strip().lower()
    if key not in PIPELINE:
        raise KeyError(f"Unknown factory mode {mode!r}; known: {sorted(PIPELINE)}")
    return PIPELINE[key]

"""
Phase 7 validation orchestrator — called from pipeline / CLI.
"""
from __future__ import annotations

import json
import os
from typing import Any

from bitget.infra import ops_logger
from bitget.infra.logging_setup import get_logger
from bitget.validation.cutover import check_cutover_readiness, parallel_run_status, start_parallel_run
from bitget.validation.load_test import run_load_test
from bitget.validation.pnl_parity import compare_pnl_parity, save_pnl_baseline
from bitget.validation.signal_parity import compare_signal_parity, save_signal_baseline

logger = get_logger("bitget.validation.runner")


def _max_signal_diff_pct() -> float:
    raw = os.environ.get("BITGET_SIGNAL_MAX_DIFF_PCT", "1.0")
    try:
        return float(raw)
    except ValueError:
        return 1.0


def run_record_baseline() -> dict[str, Any]:
    sig = save_signal_baseline()
    pnl = save_pnl_baseline()
    report = {"signal": sig, "pnl": pnl}
    ops_logger.record_gauge_snapshot("bitget.validation", {"action": "record_baseline", **report})
    logger.info("%s", json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return report


def run_validate_parity() -> dict[str, Any]:
    sig = compare_signal_parity(max_diff_pct=_max_signal_diff_pct())
    pnl = compare_pnl_parity()
    passed = bool(sig.get("passed")) and bool(pnl.get("passed"))
    report = {
        "passed": passed,
        "signal_parity": sig,
        "pnl_parity": pnl,
        "parallel_run": parallel_run_status(),
    }
    ops_logger.record_gauge_snapshot("bitget.validation", report)
    logger.info("[validate] signal: %s", sig.get("message"))
    logger.info("[validate] pnl: %s", pnl.get("message"))
    logger.info("[validate] overall: %s", "PASS" if passed else "FAIL")
    if not passed:
        raise RuntimeError("validation parity FAILED")
    return report


def run_load_test_job() -> dict[str, Any]:
    min_sym = int(os.environ.get("BITGET_LOAD_TEST_MIN_SYMBOLS", "500"))
    max_sec = float(os.environ.get("BITGET_LOAD_TEST_MAX_SEC", "600"))
    report = run_load_test(min_symbols=min_sym, max_elapsed_sec=max_sec)
    ops_logger.record_gauge_snapshot("bitget.validation.load_test", report)
    logger.info("%s", json.dumps(report, ensure_ascii=False, indent=2, default=str))
    if not report.get("passed"):
        raise RuntimeError("load test FAILED")
    return report


def run_cutover_check() -> dict[str, Any]:
    report = check_cutover_readiness()
    ops_logger.record_gauge_snapshot("bitget.validation.cutover", report)
    logger.info(
        "[cutover] passed=%s message=%s",
        report.get("passed"),
        report.get("message"),
    )
    logger.info("[cutover] checks=%s", report.get("checks"))
    arch = report.get("architecture") or {}
    if arch:
        logger.info(
            "[cutover] architecture_passed=%s failed=%s",
            arch.get("passed"),
            arch.get("failed"),
        )
    if not report.get("passed"):
        logger.warning(
            "[cutover] not ready (informational unless BITGET_PIPELINE_SSOT=1)"
        )
    return report


def run_validate_all() -> dict[str, Any]:
    parity = run_validate_parity()
    load = run_load_test(
        min_symbols=int(os.environ.get("BITGET_LOAD_TEST_MIN_SYMBOLS", "100")),
        max_elapsed_sec=float(os.environ.get("BITGET_LOAD_TEST_MAX_SEC", "600")),
    )
    cutover = check_cutover_readiness()
    report = {"parity": parity, "load_test": load, "cutover": cutover}
    if not load.get("passed"):
        raise RuntimeError("validate_all: load test failed")
    return report


def run_start_parallel_run(note: str = "") -> dict[str, Any]:
    st = start_parallel_run(mode="pipeline", note=note)
    ops_logger.record_gauge_snapshot("bitget.validation.parallel", st)
    logger.info(
        "[parallel] started %s target=%sh",
        st["started_at_utc"],
        st["target_hours"],
    )
    return st

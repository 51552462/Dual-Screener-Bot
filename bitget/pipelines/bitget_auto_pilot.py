"""
Bitget 24/7 daemon — pipeline SSOT orchestrator (Phase 2).

- Cron one-shot jobs: `bitget.sh --mode` / `pipelines.runner`
- This daemon: OMS, satellites, daily/weekly pipelines, supernova sniper thread
- Does NOT spawn legacy main.py periodic_runner threads
"""
from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger("bitget.auto_pilot")

# Watchdog SSOT — must match BITGET_WATCHDOG_HEARTBEAT_COMPONENT default in watchdog.py
HEARTBEAT_COMPONENT = "bitget_auto_pilot"
HEARTBEAT_INTERVAL_SEC = 60.0


def _heartbeat_loop(stop: threading.Event) -> None:
    from bitget.infra import ops_logger

    def _tick() -> None:
        try:
            ops_logger.record_heartbeat(HEARTBEAT_COMPONENT)
        except Exception:
            logger.exception("heartbeat tick failed")

    _tick()
    while not stop.wait(HEARTBEAT_INTERVAL_SEC):
        _tick()


def _run_pipeline(mode: str, *, skip_telegram: bool = True) -> None:
    from bitget.infra.runtime import bitget_exit_code, dispatch_bitget_mode
    from bitget.pipelines.bitget_pipelines import get_pipeline

    report = dispatch_bitget_mode(
        mode,
        get_pipeline(mode),
        send_fn=None,
        skip_telegram=skip_telegram,
    )
    code = bitget_exit_code(report)
    if code != 0:
        logger.warning("pipeline %s finished with exit=%s status=%s", mode, code, report.overall_status)
    else:
        logger.info("pipeline %s OK", mode)


def _supernova_sniper_thread() -> None:
    try:
        from bitget.pipelines.scanner_hooks import run_supernova_sniper_scheduler

        run_supernova_sniper_scheduler()
    except Exception as e:
        logger.exception("supernova sniper thread stopped: %s", e)


def _oms_cycle(*, state: dict) -> None:
    try:
        import bitget.oms as bitget_oms
        from bitget.schedule_lock import acquire as schedule_acquire

        mono_now = time.monotonic()
        if not state.get("oms_cold_done"):
            try:
                bitget_oms.run_scheduled_reconciliation()
            except Exception as e:
                logger.warning("OMS cold reconciliation: %s", e)
            state["oms_cold_done"] = True
            state["oms_last_mono"] = mono_now
        elif (mono_now - float(state.get("oms_last_mono", 0.0))) >= 3600.0 and schedule_acquire(
            "oms::hourly_recon", 3500
        ):
            try:
                bitget_oms.run_scheduled_reconciliation()
            except Exception as e:
                logger.warning("OMS hourly reconciliation: %s", e)
            state["oms_last_mono"] = mono_now
    except Exception as e:
        logger.warning("OMS loader: %s", e)


def _satellite_cycle(now: datetime, hm_key: str, flags: dict) -> None:
    from bitget.auto_pilot import _safe_run_satellite

    hour, minute = now.hour, now.minute
    if hour % 2 == 0 and minute == 10 and flags.get("sentiment") != hm_key:
        _safe_run_satellite("satellite::sentiment", 7200, "bitget.sentiment_miner", "run_sentiment_mining")
        flags["sentiment"] = hm_key
    if hour % 2 == 0 and minute == 12 and flags.get("altdata") != hm_key:
        _safe_run_satellite("satellite::altdata", 7200, "bitget.alt_data_miner", "run_alternative_data_mining")
        flags["altdata"] = hm_key
    if hour % 3 == 0 and minute == 18 and flags.get("blackhole") != hm_key:
        _safe_run_satellite("satellite::blackhole", 10800, "bitget.blackhole_hunter", "scan_blackhole_targets")
        flags["blackhole"] = hm_key
    if hour % 6 == 0 and minute == 15 and flags.get("shadow_perf") != hm_key:
        _safe_run_satellite(
            "satellite::shadow_perf",
            21600,
            "bitget.shadow_performance_tracker",
            "run_shadow_performance_evaluation",
        )
        flags["shadow_perf"] = hm_key
    if hour == 0 and minute == 15 and flags.get("underdog") != hm_key:
        _safe_run_satellite("satellite::underdog", 86400, "bitget.underdog_miner", "run_underdog_mining")
        flags["underdog"] = hm_key
    if hour == 0 and minute == 20 and flags.get("pump_forensics") != hm_key:
        _safe_run_satellite("satellite::pump_forensics", 86400, "bitget.pump_forensics", "run_pump_forensics")
        flags["pump_forensics"] = hm_key
    if hour == 0 and minute == 25 and flags.get("forensics_pioneer") != hm_key:
        _safe_run_satellite(
            "satellite::forensics_pioneer", 86400, "bitget.forensics_pioneer", "run_forensics_pioneer"
        )
        flags["forensics_pioneer"] = hm_key
    if now.weekday() == 5 and hour == 1 and minute == 5 and flags.get("synthetic_lab") != hm_key:
        _safe_run_satellite(
            "satellite::synthetic_lab", 86400, "bitget.synthetic_data_generator", "stress_test_mutants"
        )
        flags["synthetic_lab"] = hm_key
    if now.weekday() == 6 and hour == 1 and minute == 30 and flags.get("time_machine") != hm_key:
        _safe_run_satellite(
            "satellite::time_machine",
            86400,
            "bitget.time_machine_backtester",
            "run_time_machine_backtest",
            "FTX_COLLAPSE_2022",
            3.0,
        )
        flags["time_machine"] = hm_key


def _daily_pipeline_cycle(now: datetime, *, last_daily_key: str) -> str:
    """Run daily_audit pipeline once per UTC day (after warm-up)."""
    daily_key = now.strftime("%Y-%m-%d")
    if daily_key == last_daily_key:
        return last_daily_key
    if last_daily_key == "":
        return daily_key
    logger.info("daily pipeline trigger UTC date=%s", daily_key)
    try:
        _run_pipeline("daily_audit")
    except Exception as e:
        logger.exception("daily_audit pipeline failed: %s", e)
    if now.weekday() == 0:
        try:
            from bitget.auto_pilot import send_weekly_flow_master_report

            send_weekly_flow_master_report()
        except Exception as e:
            logger.warning("weekly flow report: %s", e)
    return daily_key


def system_main_loop() -> None:
    from bitget.infra.logging_setup import setup_logging
    from bitget.infra import ops_logger

    setup_logging(default_component="bitget.auto_pilot")
    ops_logger.install_unhandled_exception_hooks()
    ops_logger.record_heartbeat(
        HEARTBEAT_COMPONENT,
        extra={"event": "daemon_start", "orchestrator": "pipeline"},
    )

    stop = threading.Event()
    threading.Thread(target=_heartbeat_loop, args=(stop,), daemon=True, name="bitget_hb").start()
    threading.Thread(target=_supernova_sniper_thread, daemon=True, name="bitget_supernova_sniper").start()

    print("[bitget_auto_pilot] pipeline orchestrator started (OMS + satellites + daily_audit)")
    print("  - scan/track/reconcile: cron via bitget.sh (not inline threads)")
    print("  - supernova sniper: hooked run_live_sniper_scheduler thread")

    oms_state: dict = {"oms_cold_done": False, "oms_last_mono": 0.0}
    satellite_flags: dict = {}
    last_daily_key = ""

    while True:
        try:
            now = datetime.now(timezone.utc)
            hm_key = now.strftime("%Y-%m-%d %H:%M")
            _oms_cycle(state=oms_state)
            _satellite_cycle(now, hm_key, satellite_flags)
            last_daily_key = _daily_pipeline_cycle(now, last_daily_key=last_daily_key)
            time.sleep(20)
        except Exception as e:
            logger.exception("daemon loop error: %s", e)
            time.sleep(60)


def run_daemon_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bitget pipeline auto pilot daemon")
    parser.add_argument("--daemon", action="store_true", help="Run pipeline orchestrator loop")
    args = parser.parse_args(argv)
    if not args.daemon:
        parser.error("Specify --daemon")
    system_main_loop()
    return 0


if __name__ == "__main__":
    raise SystemExit(run_daemon_cli())

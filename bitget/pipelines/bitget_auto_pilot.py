"""
Bitget 24/7 daemon — pipeline SSOT orchestrator (Phase 2).

- Cron one-shot jobs: `bitget.sh --mode` / `pipelines.runner`
- This daemon: OMS + satellites (+ optional supernova sniper when BITGET_DAEMON_SNIPER=1)
- daily_audit / weekly_evolution: cron SSOT only (DUAL_EXECUTION_FIX — 주식 factory 동일)
- Does NOT spawn legacy main.py periodic_runner threads
"""
from __future__ import annotations

import argparse
import logging
import os
import threading
import time

from bitget.infra.daemon_loop import (
    DAEMON_ERROR_SLEEP_SEC,
    DAEMON_TICK_SLEEP_SEC,
    DaemonLoopFrame,
    satellite_flag_once,
    sleep_or_backoff,
)

logger = logging.getLogger("bitget.auto_pilot")

# Lazy-once imports for hot 20s tick — avoid per-iteration import overhead.
_OMS_MODULE = None

# Watchdog SSOT — must match BITGET_WATCHDOG_HEARTBEAT_COMPONENT default in watchdog.py
HEARTBEAT_COMPONENT = "bitget_auto_pilot"
HEARTBEAT_INTERVAL_SEC = 60.0


def _daemon_sniper_enabled() -> bool:
    """
    Cron staggered supernova slots are SSOT (bitget_scan_schedule).
    Legacy 24/7 run_live_sniper_scheduler duplicates cron — opt-in only.
    """
    raw = str(os.environ.get("BITGET_DAEMON_SNIPER", "0")).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _daemon_public_ws_enabled() -> bool:
    return False
    """Tier-1 public WS → StreamBuffer. Opt-in — slippage_guard soft-skips when empty."""
  # from bitget.data.ws_market_service import public_ws_daemon_enabled

    return public_ws_daemon_enabled()


def _daemon_private_ws_enabled() -> bool:
    return False
    """Tier-1 private WS → PrivateStreamBuffer. Opt-in — requires API credentials."""
    from bitget.data.ws_private_service import private_ws_daemon_enabled

    return private_ws_daemon_enabled()


def _heartbeat_loop(stop: threading.Event) -> None:
    from bitget.infra import ops_logger

    def _tick() -> None:
        try:
            public_ws = None
            private_ws = None
            try:
                from bitget.data.ws_market_service import heartbeat_public_ws_snapshot

                public_ws = heartbeat_public_ws_snapshot()
            except Exception:
                public_ws = {"enabled": False, "error": "snapshot_import_failed"}
            try:
                from bitget.data.ws_private_service import heartbeat_private_ws_snapshot

                private_ws = heartbeat_private_ws_snapshot()
            except Exception:
                private_ws = {"enabled": False, "error": "snapshot_import_failed"}
            oms_book = None
            try:
                from bitget.trading.oms_source_stats import oms_source_heartbeat_snapshot

                oms_book = oms_source_heartbeat_snapshot()
            except Exception:
                oms_book = {"error": "oms_source_snapshot_failed"}
            kwargs = {}
            if public_ws is not None:
                kwargs["public_ws"] = public_ws
            if private_ws is not None:
                kwargs["private_ws"] = private_ws
            if oms_book is not None:
                kwargs["oms_book"] = oms_book
            try:
                from bitget.trading.oms_source_stats import maybe_warn_oms_rest_share

                maybe_warn_oms_rest_share(
                    oms_book if isinstance(oms_book, dict) else None,
                    private_ws_enabled=bool(
                        isinstance(private_ws, dict) and private_ws.get("enabled")
                    ),
                    public_ws_enabled=bool(
                        isinstance(public_ws, dict) and public_ws.get("enabled")
                    ),
                )
            except Exception:
                pass
            if kwargs:
                ops_logger.record_heartbeat(HEARTBEAT_COMPONENT, **kwargs)
            else:
                ops_logger.record_heartbeat(HEARTBEAT_COMPONENT)
        except Exception:
            logger.exception("heartbeat tick failed")

    _tick()
    while not stop.wait(HEARTBEAT_INTERVAL_SEC):
        _tick()


def _supernova_sniper_thread() -> None:
    try:
        from bitget.pipelines.scanner_hooks import run_supernova_sniper_scheduler

        run_supernova_sniper_scheduler()
    except Exception as e:
        logger.exception("supernova sniper thread stopped: %s", e)


def _oms_module():
    global _OMS_MODULE
    if _OMS_MODULE is None:
        import bitget.oms as bitget_oms

        _OMS_MODULE = bitget_oms
    return _OMS_MODULE


def _oms_cycle(*, state: dict) -> None:
    try:
        from bitget.schedule_lock import acquire as schedule_acquire

        bitget_oms = _oms_module()
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


def _satellite_cycle(tick, flags: dict) -> None:
    from bitget.auto_pilot import _safe_run_satellite

    hour, minute, hm_key = tick.hour, tick.minute, tick.hm_key
    if hour % 2 == 0 and minute == 10 and satellite_flag_once(flags, "sentiment", hm_key):
        _safe_run_satellite("satellite::sentiment", 7200, "bitget.sentiment_miner", "run_sentiment_mining")
    if hour % 2 == 0 and minute == 12 and satellite_flag_once(flags, "altdata", hm_key):
        _safe_run_satellite("satellite::altdata", 7200, "bitget.alt_data_miner", "run_alternative_data_mining")
    if hour % 3 == 0 and minute == 18 and satellite_flag_once(flags, "blackhole", hm_key):
        _safe_run_satellite("satellite::blackhole", 10800, "bitget.blackhole_hunter", "scan_blackhole_targets")
    if hour % 6 == 0 and minute == 15 and satellite_flag_once(flags, "shadow_perf", hm_key):
        _safe_run_satellite(
            "satellite::shadow_perf",
            21600,
            "bitget.shadow_performance_tracker",
            "run_shadow_performance_evaluation",
        )
    if hour == 0 and minute == 15 and satellite_flag_once(flags, "underdog", hm_key):
        _safe_run_satellite("satellite::underdog", 86400, "bitget.underdog_miner", "run_underdog_mining")
    if hour == 0 and minute == 20 and satellite_flag_once(flags, "pump_forensics", hm_key):
        _safe_run_satellite("satellite::pump_forensics", 86400, "bitget.pump_forensics", "run_pump_forensics")
    if hour == 0 and minute == 25 and satellite_flag_once(flags, "forensics_pioneer", hm_key):
        _safe_run_satellite(
            "satellite::forensics_pioneer", 86400, "bitget.forensics_pioneer", "run_forensics_pioneer"
        )
    # [크론 비충돌] Sat 03:05 UTC(=12:05 KST). KR 주간 뇌수술·Flow 리포트(Sat 01:00–01:10 UTC)와 분리.
    if tick.weekday == 5 and hour == 3 and minute == 5 and satellite_flag_once(flags, "synthetic_lab", hm_key):
        _safe_run_satellite(
            "satellite::synthetic_lab", 86400, "bitget.synthetic_data_generator", "stress_test_mutants"
        )
    if tick.weekday == 6 and hour == 1 and minute == 30 and satellite_flag_once(flags, "time_machine", hm_key):
        _safe_run_satellite(
            "satellite::time_machine",
            86400,
            "bitget.time_machine_backtester",
            "run_time_machine_backtest",
            "FTX_COLLAPSE_2022",
            3.0,
        )


def _disk_manager_loop() -> None:
    try:
        from bitget.disk_manager import run_daily_cleanup_loop

        run_daily_cleanup_loop()
    except Exception as e:
        logger.exception("disk manager thread stopped: %s", e)


def system_main_loop() -> None:
    from bitget.infra.logging_setup import setup_logging
    from bitget.infra import ops_logger

    setup_logging(default_component="bitget.auto_pilot")
    ops_logger.install_unhandled_exception_hooks()
    try:
        from bitget.infra.artifact_guard import ensure_bitget_artifacts

        boot = ensure_bitget_artifacts()
        logger.info("daemon boot artifact guard: %s", boot)
    except Exception as e:
        logger.warning("daemon boot artifact guard skipped: %s", e)
    ops_logger.record_heartbeat(
        HEARTBEAT_COMPONENT,
        extra={"event": "daemon_start", "orchestrator": "pipeline"},
    )

    stop = threading.Event()
    threading.Thread(target=_heartbeat_loop, args=(stop,), daemon=True, name="bitget_hb").start()
    threading.Thread(
        target=_disk_manager_loop,
        daemon=True,
        name="bitget_disk_mgr",
    ).start()
    if _daemon_sniper_enabled():
        threading.Thread(
            target=_supernova_sniper_thread,
            daemon=True,
            name="bitget_supernova_sniper",
        ).start()
        sniper_line = "  - supernova sniper: ENABLED (BITGET_DAEMON_SNIPER=1 — legacy 24/7 loop)"
    else:
        sniper_line = (
            "  - supernova sniper: DISABLED (cron staggered SSOT; set BITGET_DAEMON_SNIPER=1 to opt in)"
        )

    if _daemon_public_ws_enabled():
        try:
            from bitget.data.ws_market_service import start_public_ws_market_service

            ws_ok = start_public_ws_market_service()
            ws_line = (
                "  - public WS market: ENABLED"
                if ws_ok
                else "  - public WS market: opt-in but soft-failed (check websocket-client / logs)"
            )
        except Exception as e:
            logger.warning("public WS market start failed (daemon continues): %s", e)
            ws_line = "  - public WS market: FAILED soft (daemon continues)"
    else:
        ws_line = (
            "  - public WS market: DISABLED "
            "(set BITGET_DAEMON_PUBLIC_WS=1 for Tier-1 StreamBuffer / slippage gates)"
        )

    if _daemon_private_ws_enabled():
        try:
          # from bitget.data.ws_private_service import start_private_ws_market_service

            pws_ok = start_private_ws_market_service()
            pws_line = (
                "  - private WS oms: ENABLED"
                if pws_ok
                else "  - private WS oms: opt-in but soft-failed (creds / websocket-client)"
            )
        except Exception as e:
            logger.warning("private WS start failed (daemon continues): %s", e)
            pws_line = "  - private WS oms: FAILED soft (daemon continues)"
    else:
        pws_line = (
            "  - private WS oms: DISABLED "
            "(set BITGET_DAEMON_PRIVATE_WS=1 for positions/orders/account cache)"
        )

    logger.info("[bitget_auto_pilot] pipeline orchestrator started (OMS + satellites)")
    logger.info("  - scan/track/daily/weekly: cron via bitget.sh (not inline daemon)")
    logger.info("%s", sniper_line)
    logger.info("%s", ws_line)
    logger.info("%s", pws_line)

    oms_state: dict = {"oms_cold_done": False, "oms_last_mono": 0.0}
    satellite_flags: dict = {}
    frame = DaemonLoopFrame()

    while True:
        try:
            frame.refresh_utc()
            _oms_cycle(state=oms_state)
            _satellite_cycle(frame.tick, satellite_flags)
            frame.mark_ok()
            sleep_or_backoff(normal_sec=DAEMON_TICK_SLEEP_SEC, after_error=frame.loop_error)
        except Exception as e:
            logger.exception("daemon loop error: %s", e)
            frame.mark_error()
            sleep_or_backoff(
                normal_sec=DAEMON_TICK_SLEEP_SEC,
                after_error=frame.loop_error,
                error_sec=DAEMON_ERROR_SLEEP_SEC,
            )


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

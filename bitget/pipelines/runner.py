"""
Bitget factory CLI — bitget.sh / cron single entrypoint.
"""
from __future__ import annotations

import argparse
import sys


def step_infra_health() -> None:
    from bitget.infra import config_manager, data_paths, ops_logger, runtime
    from bitget.infra.logging_setup import get_logger, setup_logging

    setup_logging(default_component="bitget.health")
    get_logger("bitget.health").info("infra health check")
    config_manager.bootstrap_from_json_if_empty()
    ops_logger.record_heartbeat("bitget.health", extra={"phase": "2"})
    print("[OK] bitget infra")
    print(f"  data_dir      = {data_paths.bitget_data_dir()}")
    print(f"  market_db     = {data_paths.market_data_db_path()}")
    print(f"  ops_events_db = {ops_logger.OPS_EVENTS_DB_PATH}")
    print(f"  config_db     = {data_paths.system_config_db_path()}")
    print(f"  dashboard     = :{data_paths.dashboard_port()}")
    print(f"  heatmap       = :{data_paths.heatmap_port()}")
    print(f"  runtime_modes = {sorted(runtime.BITGET_MODES)}")


def _telegram_send_fn():
    try:
        from bitget.forward_tester import send_telegram_msg

        return send_telegram_msg
    except Exception:
        return None


def _engine_for_mode(mode: str) -> str:
    """이 러너가 다루는 모드는 전부 코인(Bitget) 측이므로 BITGET 엔진으로 적재.

    (KR/US 엔진은 주식 팩토리 스케줄러가 별도 적재한다.)
    """
    from bitget.infra.task_orchestrator import ENGINE_BITGET

    return ENGINE_BITGET


def run_factory_cli(argv: list[str] | None = None) -> int:
    from bitget.bitget_scan_schedule import resolve_lock_timeout_sec
    from bitget.infra.logging_setup import setup_logging
    from bitget.infra.runtime import (
        BITGET_MODES,
        bitget_exit_code,
        dispatch_bitget_mode,
    )
    from bitget.pipelines.bitget_pipelines import get_pipeline

    setup_logging(default_component="bitget.runner")

    parser = argparse.ArgumentParser(description="Bitget factory job runner")
    parser.add_argument(
        "--mode",
        choices=sorted(BITGET_MODES | {"watchdog"}),
        help="Job pipeline to run once and exit",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-telegram", action="store_true")
    parser.add_argument("--lock-timeout", type=float, default=None)
    parser.add_argument(
        "--enqueue",
        action="store_true",
        help="Enqueue the mode into task_queue.sqlite instead of running it inline "
        "(cron→queue adapter; a queue worker executes it later).",
    )
    args = parser.parse_args(argv)

    if args.mode == "watchdog":
        from bitget.watchdog import main as watchdog_main

        return watchdog_main()

    if not args.mode:
        parser.error("--mode is required (or use bitget.sh --daemon for 24/7)")

    if args.enqueue:
        from bitget.infra.task_orchestrator import enqueue

        engine = _engine_for_mode(args.mode)
        task_id = enqueue(engine, args.mode)
        if task_id is None:
            print(f"[QUEUE] dedupe — {engine}:{args.mode} already PENDING/RUNNING")
        else:
            print(f"[QUEUE] enqueued #{task_id} {engine}:{args.mode}")
        return 0

    send_fn = None if args.skip_telegram else _telegram_send_fn()
    pipeline = get_pipeline(args.mode)
    lock_timeout = resolve_lock_timeout_sec(args.mode, explicit=args.lock_timeout)
    report = dispatch_bitget_mode(
        args.mode,
        pipeline,
        send_fn=send_fn,
        skip_telegram=args.skip_telegram,
        dry_run=args.dry_run,
        lock_timeout_sec=lock_timeout,
    )
    return bitget_exit_code(report)


def main(argv: list[str] | None = None) -> int:
    return run_factory_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())

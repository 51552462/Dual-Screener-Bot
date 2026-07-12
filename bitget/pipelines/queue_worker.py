"""
큐 워커 데몬 — bitget_task_queue.sqlite 의 PENDING 작업을 단일 직렬로 실행.

[역할] cron 이 `runner.py --enqueue <mode>` 로 적재만 하고 즉시 종료하면,
이 워커가 `task_orchestrator` 큐에서 권력 이양(KST/ET/Bitget) 우선순위대로
한 건씩 꺼내(`BEGIN EXCLUSIVE`) `dispatch_bitget_mode` 로 실행한다.

[효과]
  - 주식/코인 동시각 충돌 시 SKIP(증발) 대신 PENDING 대기 → 순차 실행 → 차수 누락 0.
  - 단일 직렬 워커라 scan ↔ data_refresh 동시 write 가 사라져 `database is locked` 완화.
  - 후순위(비-주인) 엔진은 os.nice(10) 스로틀 후 실행 → 서버 다운 방지.
  - FAIL 은 큐 재시도/서킷 브레이커와 결합되어 좀비 루프 차단.

상태 머신은 task_orchestrator 가 관리한다(PENDING→RUNNING→DONE|FAILED).
"""
from __future__ import annotations

import argparse
import os

from bitget.infra.daemon_loop import (
    QUEUE_WORKER_ERROR_SLEEP_SEC,
    QUEUE_WORKER_IDLE_POLL_SEC,
    sleep_or_backoff,
)

# 기본 폴링 간격(초): PENDING 이 없을 때 다음 확인까지 대기.
DEFAULT_POLL_SEC = QUEUE_WORKER_IDLE_POLL_SEC
# [#2] yield 양보 시 재시도까지 기본 대기(초). 주식 슬롯이 풀리길 기다리는 간격.
DEFAULT_YIELD_DEFER_BACKOFF_SEC = 60.0


def _enqueue_on_yield_enabled() -> bool:
    """주식 factory 양보 스킵을 Drop 대신 '대기 후 재시도'로 전환할지(기본 OFF)."""
    return str(os.environ.get("BITGET_ENQUEUE_ON_YIELD", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _yield_defer_backoff_sec() -> float:
    try:
        return max(5.0, float(os.environ.get("BITGET_YIELD_DEFER_BACKOFF_SEC", "60")))
    except ValueError:
        return DEFAULT_YIELD_DEFER_BACKOFF_SEC
# 한 번의 drain 에서 연속 처리할 최대 작업 수(폭주 방지·주기적 폴링 복귀).
DEFAULT_DRAIN_BATCH = 50
# 실패 작업 재시도까지의 backoff(초).
DEFAULT_RETRY_BACKOFF_SEC = 300.0


def _make_executor(*, skip_telegram: bool, dry_run: bool):
    """task → dispatch_bitget_mode 실행 클로저. FAIL 일 때만 예외를 던져 큐 재시도."""
    from bitget.infra.logging_setup import get_logger
    from bitget.infra.runtime import dispatch_bitget_mode
    from bitget.pipelines.bitget_pipelines import get_pipeline

    logger = get_logger("bitget.queue_worker")

    send_fn = None
    if not skip_telegram:
        try:
            from bitget.forward_tester import send_telegram_msg

            send_fn = send_telegram_msg
        except Exception:
            send_fn = None

    def _executor(task) -> None:
        from bitget.bitget_scan_schedule import resolve_lock_timeout_sec
        from bitget.infra.task_orchestrator import touch_worker_heartbeat

        logger.info(
            "queue exec start id=%s engine=%s mode=%s prio=%s attempt=%s/%s",
            task.id, task.engine, task.mode, task.priority, task.attempts, task.max_attempts,
        )
        # 작업 착수 시점에도 생존 신호를 찍어, 단일 작업이 길어져도 워치독 오탐을 줄인다.
        touch_worker_heartbeat(status="running", extra={"task_id": task.id, "mode": task.mode})
        pipeline = get_pipeline(task.mode)
        lock_timeout = resolve_lock_timeout_sec(task.mode)
        report = dispatch_bitget_mode(
            task.mode,
            pipeline,
            send_fn=send_fn,
            skip_telegram=skip_telegram,
            dry_run=dry_run,
            lock_timeout_sec=lock_timeout,
        )
        logger.info("queue exec done id=%s mode=%s status=%s", task.id, task.mode, report.status_label)

        # [#2] 주식 factory 가 바빠 '양보(yield)'로 스킵된 경우: Drop 하지 말고 대기 후 재시도.
        #   BITGET_ENQUEUE_ON_YIELD=1 일 때만 활성(기본 OFF → 기존 동작 보존).
        #   attempts 를 소모하지 않는 TaskDeferred 로 신호 → 주식 락 풀리면 순차 실행.
        if _enqueue_on_yield_enabled():
            detail = str(report.skipped_session_detail or "")
            if report.skipped_session and "yield_to_factory" in detail:
                from bitget.infra.task_orchestrator import TaskDeferred

                backoff = _yield_defer_backoff_sec()
                logger.info(
                    "queue defer id=%s mode=%s — yield_to_factory; retry in %.0fs",
                    task.id, task.mode, backoff,
                )
                raise TaskDeferred(detail[:300], available_in_sec=backoff)

        # OK / PARTIAL_FAIL(critical ok) / SKIPPED_*(yield 제외) → DONE(완료, 재시도 안 함).
        # FAIL(critical 실패) → 예외 → 큐 fail()(backoff 재시도 or FAILED).
        if report.status_label == "FAIL":
            raise RuntimeError(f"{task.mode} -> FAIL")

    return _executor


def run_worker(
    *,
    poll_sec: float = DEFAULT_POLL_SEC,
    drain_batch: int = DEFAULT_DRAIN_BATCH,
    retry_backoff_sec: float = DEFAULT_RETRY_BACKOFF_SEC,
    skip_telegram: bool = False,
    dry_run: bool = False,
    once: bool = False,
) -> int:
    """워커 루프. once=True 면 한 번 drain 후 종료(헬스/테스트용)."""
    from bitget.infra.logging_setup import setup_logging, get_logger
    from bitget.infra.task_orchestrator import drain, init_queue, touch_worker_heartbeat

    setup_logging(default_component="bitget.queue_worker")
    logger = get_logger("bitget.queue_worker")
    init_queue()

    executor = _make_executor(skip_telegram=skip_telegram, dry_run=dry_run)
    logger.info(
        "queue worker started (poll=%.1fs batch=%d backoff=%.0fs once=%s dry_run=%s)",
        poll_sec, drain_batch, retry_backoff_sec, once, dry_run,
    )
    touch_worker_heartbeat(status="started")

    # on_tick: 매 작업 처리 직후 생존 신호 갱신 (배치 내 진행 증명).
    def _tick() -> None:
        touch_worker_heartbeat(status="draining")

    loop_error = False
    while True:
        touch_worker_heartbeat(status="idle")
        try:
            processed = drain(
                executor,
                max_tasks=drain_batch,
                backoff_sec=retry_backoff_sec,
                on_tick=_tick,
            )
            loop_error = False
        except Exception:  # noqa: BLE001
            logger.exception("queue worker drain crashed — continuing")
            processed = 0
            loop_error = True
        if once:
            return processed
        if processed == 0:
            sleep_or_backoff(
                normal_sec=poll_sec,
                after_error=loop_error,
                error_sec=QUEUE_WORKER_ERROR_SLEEP_SEC,
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bitget task queue worker daemon")
    parser.add_argument("--poll-sec", type=float, default=DEFAULT_POLL_SEC)
    parser.add_argument("--drain-batch", type=int, default=DEFAULT_DRAIN_BATCH)
    parser.add_argument("--retry-backoff-sec", type=float, default=DEFAULT_RETRY_BACKOFF_SEC)
    parser.add_argument("--skip-telegram", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Drain the queue once and exit (for smoke tests / cron-driven workers).",
    )
    args = parser.parse_args(argv)
    # run_worker 는 처리 건수를 반환(테스트/로깅용)하지만, 프로세스 종료코드는
    # 성공=0 으로 고정한다(--once 처리 건수를 실패로 오인하지 않도록).
    run_worker(
        poll_sec=args.poll_sec,
        drain_batch=args.drain_batch,
        retry_backoff_sec=args.retry_backoff_sec,
        skip_telegram=args.skip_telegram,
        dry_run=args.dry_run,
        once=args.once,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

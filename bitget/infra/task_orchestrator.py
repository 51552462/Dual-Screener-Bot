"""
Zero-Dependency 무중단 우선순위 큐 (Task Orchestrator).

[문제] 파일 락(`.factory_runtime.lock`) 공유로 우선순위가 밀리면 작업이
대기하지 못하고 증발(Yield/Skip)했다. (코인 스캔이 주식 잡에 양보하며 사라짐)

[해결] 외부 라이브러리 없이 내장 `sqlite3` 로 엔진별 큐 DB를 분리한다.
  - 주식: `factory_data_dir()/task_queue.sqlite`
  - 코인: `bitget_data_dir()/bitget_task_queue.sqlite`
  - `BEGIN EXCLUSIVE TRANSACTION` 으로 다중 프로세스 동시 픽업(Race) 차단(원자성).
  - [타임존 기반 권력 이양] 큐에서 작업을 꺼낼 때 서버 시간으로 우선순위 부여:
        KST 09:00~15:30  → KR  Priority 1
        ET  09:30~16:00  → US  Priority 1
        그 외 / 주말      → BITGET Priority 1
  - 후순위 작업이 강제 동시 실행될 때 `os.nice(10)` 로 CPU 스로틀링 → 서버 다운 방지.

상태 머신:  PENDING → RUNNING → DONE | FAILED
            (실패 시 attempts < max_attempts 면 backoff 후 PENDING 재진입)
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterator, List, Optional

import pytz

import low_ram_sqlite_pragmas
import memory_bounds

from bitget.infra.bounded_reads import task_queue_claim_next_sql
from bitget.infra.clock import utc_now, utc_now_iso
from bitget.infra.memory_policy import (
    TASK_QUEUE_DONE_KEEP_DAYS,
    TASK_QUEUE_DONE_KEEP_LAST,
    TASK_QUEUE_FAILED_KEEP_DAYS,
    TASK_QUEUE_FAILED_KEEP_LAST,
)
from bitget.infra.shared_db_connector import BUSY_TIMEOUT_MS

# --- 엔진 식별자 ---
ENGINE_KR = "KR"
ENGINE_US = "US"
ENGINE_BITGET = "BITGET"
VALID_ENGINES = (ENGINE_KR, ENGINE_US, ENGINE_BITGET)

# 우선순위 숫자: 작을수록 먼저 실행 (1 = 최우선).
PRIORITY_PRIMARY = 1     # 현재 세션의 주인 엔진
PRIORITY_SECONDARY = 5   # 그 외 엔진 (대기 후 스로틀 실행)

# 후순위 강제 동시 실행 시 적용할 nice 증가분.
THROTTLE_NICE = 10

_KST = pytz.timezone("Asia/Seoul")
_ET = pytz.timezone("America/New_York")


def queue_db_path_for_engine(engine: Optional[str] = None) -> str:
    """엔진별 큐 DB — KR/US·BITGET 물리 분리 (Two-Track air-gap)."""
    eng = str(engine or ENGINE_BITGET).strip().upper()
    if eng in (ENGINE_KR, ENGINE_US):
        try:
            from factory_data_paths import task_queue_db_path

            return task_queue_db_path()
        except Exception:
            return os.path.join(os.getcwd(), "task_queue.sqlite")
    try:
        from bitget.infra.data_paths import task_queue_db_path

        return task_queue_db_path()
    except Exception:
        return os.path.join(os.getcwd(), "bitget_task_queue.sqlite")


def _queue_db_path() -> str:
    """Bitget 큐 워커·모니터 기본 경로 (코인 전용 큐)."""
    return queue_db_path_for_engine(ENGINE_BITGET)


# ---------------------------------------------------------------------------
# 타임존 기반 권력 이양 (Priority handover)
# ---------------------------------------------------------------------------
def _in_window(now_local: datetime, start: tuple[int, int], end: tuple[int, int]) -> bool:
    cur = now_local.hour * 60 + now_local.minute
    lo = start[0] * 60 + start[1]
    hi = end[0] * 60 + end[1]
    return lo <= cur <= hi


def primary_engine_now(*, now_utc: Optional[datetime] = None) -> str:
    """현재 서버 시각 기준으로 Priority 1 을 가져야 할 엔진을 반환."""
    base = now_utc or utc_now()
    if base.tzinfo is None:
        base = pytz.UTC.localize(base)

    kst = base.astimezone(_KST)
    et = base.astimezone(_ET)

    # 주말(양 시장 휴장)에는 코인이 주인.
    kst_weekday = kst.weekday() < 5  # Mon-Fri
    et_weekday = et.weekday() < 5

    if kst_weekday and _in_window(kst, (9, 0), (15, 30)):
        return ENGINE_KR
    if et_weekday and _in_window(et, (9, 30), (16, 0)):
        return ENGINE_US
    return ENGINE_BITGET


def engine_priority(engine: str, *, now_utc: Optional[datetime] = None) -> int:
    eng = str(engine or "").strip().upper()
    return PRIORITY_PRIMARY if eng == primary_engine_now(now_utc=now_utc) else PRIORITY_SECONDARY


# ---------------------------------------------------------------------------
# 큐 코어
# ---------------------------------------------------------------------------
@dataclass
class Task:
    id: int
    engine: str
    mode: str
    payload: dict
    priority: int
    status: str
    attempts: int
    max_attempts: int


def _now_iso() -> str:
    return datetime.now(_KST).isoformat()


@contextmanager
def _raw_conn(db_path: Optional[str] = None) -> Iterator[sqlite3.Connection]:
    """
    수동 트랜잭션(BEGIN EXCLUSIVE)을 쓰기 위해 autocommit(isolation_level=None) 연결.
    Traffic Rule(WAL/synchronous/busy_timeout) 은 동일하게 강제.
    """
    path = db_path or _queue_db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path, timeout=60.0, isolation_level=None)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS};")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def init_queue(db_path: Optional[str] = None) -> None:
    with _raw_conn(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_queue (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                engine       TEXT NOT NULL,
                mode         TEXT NOT NULL,
                payload      TEXT NOT NULL DEFAULT '{}',
                priority     INTEGER NOT NULL DEFAULT 5,
                status       TEXT NOT NULL DEFAULT 'PENDING',
                attempts     INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                enqueued_at  TEXT NOT NULL,
                available_at TEXT NOT NULL,
                picked_at    TEXT,
                finished_at  TEXT,
                worker       TEXT,
                last_error   TEXT
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tq_status_prio "
            "ON task_queue(status, priority, available_at, id);"
        )


class TaskDeferred(Exception):
    """작업을 '실패'가 아니라 '나중에 다시(대기)' 로 처리하라는 신호.

    executor 가 이 예외를 던지면 process_one 이 attempts 를 소모하지 않고
    available_in_sec 뒤 PENDING 으로 되돌린다(주식 factory 바쁠 때 코인 대기용).
    """

    def __init__(self, message: str = "", *, available_in_sec: float = 60.0) -> None:
        super().__init__(message)
        self.available_in_sec = max(0.0, float(available_in_sec))


def _future_iso(seconds: float) -> str:
    from datetime import timedelta

    return (datetime.now(_KST) + timedelta(seconds=max(0.0, seconds))).isoformat()


def enqueue(
    engine: str,
    mode: str,
    *,
    payload: Optional[dict] = None,
    priority: Optional[int] = None,
    max_attempts: int = 3,
    dedupe: bool = True,
    available_in_sec: float = 0.0,
    db_path: Optional[str] = None,
) -> Optional[int]:
    """
    작업을 큐에 적재한다. (Drop 금지 — 겹치면 대기열에 쌓는다)

    dedupe=True 면 동일 (engine, mode) 가 이미 PENDING/RUNNING 이면 재적재하지 않는다.
    priority 미지정 시 현재 시각 기준 권력 이양 규칙으로 자동 산정.
    available_in_sec>0 이면 그 시간 뒤부터 픽업 가능(지연 적재).
    반환: 새 task id (dedupe 로 건너뛰면 None).
    """
    eng = str(engine or "").strip().upper()
    prio = int(priority) if priority is not None else engine_priority(eng)
    path = db_path or queue_db_path_for_engine(eng)
    init_queue(path)
    with _raw_conn(path) as conn:
        if dedupe:
            row = conn.execute(
                "SELECT id FROM task_queue WHERE engine=? AND mode=? "
                "AND status IN ('PENDING','RUNNING') LIMIT 1",
                (eng, mode),
            ).fetchone()
            if row:
                return None
        now = _now_iso()
        avail = _future_iso(available_in_sec) if available_in_sec > 0 else now
        cur = conn.execute(
            "INSERT INTO task_queue "
            "(engine, mode, payload, priority, status, attempts, max_attempts, "
            " enqueued_at, available_at) "
            "VALUES (?, ?, ?, ?, 'PENDING', 0, ?, ?, ?)",
            (eng, mode, json.dumps(payload or {}, ensure_ascii=False), prio,
             int(max_attempts), now, avail),
        )
        return int(cur.lastrowid)


def defer(
    task_id: int,
    *,
    available_in_sec: float = 60.0,
    reason: str = "",
    db_path: Optional[str] = None,
) -> str:
    """작업을 attempts 소모 없이 available_in_sec 뒤 PENDING 으로 되돌린다.

    claim_next 가 픽업 시 attempts+1 한 것을 되돌려(−1) 차수를 중립으로 유지한다 →
    주식 factory 가 바쁜 동안 코인 스캔이 '실패 폐기' 없이 무한정 대기 후 실행되게 한다.
    반환: 'DEFERRED' | 'MISSING'.
    """
    with _raw_conn(db_path) as conn:
        row = conn.execute(
            "SELECT attempts FROM task_queue WHERE id=?", (int(task_id),)
        ).fetchone()
        if row is None:
            return "MISSING"
        new_attempts = max(0, int(row["attempts"]) - 1)
        conn.execute(
            "UPDATE task_queue SET status='PENDING', available_at=?, attempts=?, "
            "picked_at=NULL, worker=NULL, last_error=? WHERE id=?",
            (_future_iso(available_in_sec), new_attempts, str(reason)[:1000], int(task_id)),
        )
        return "DEFERRED"


def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


_WORKER_ID_CACHE: Optional[str] = None
_HEARTBEAT_PAYLOAD: Optional["ReusableDictPayload"] = None


def _heartbeat_writer() -> "ReusableDictPayload":
    global _HEARTBEAT_PAYLOAD, _WORKER_ID_CACHE
    from bitget.infra.daemon_loop import ReusableDictPayload

    if _WORKER_ID_CACHE is None:
        _WORKER_ID_CACHE = _worker_id()
    if _HEARTBEAT_PAYLOAD is None:
        _HEARTBEAT_PAYLOAD = ReusableDictPayload(
            pid=os.getpid(),
            worker=_WORKER_ID_CACHE,
            status="alive",
            ts_utc="",
            ts_epoch=0.0,
        )
    return _HEARTBEAT_PAYLOAD


def claim_next(*, db_path: Optional[str] = None) -> Optional[Task]:
    """
    가장 높은 우선순위의 PENDING 작업을 **원자적으로** 픽업하여 RUNNING 으로 전환.

    BEGIN EXCLUSIVE 로 다중 프로세스가 동시에 호출해도 같은 작업을 두 번
    가져가지 않는다. 픽업 시점에 현재 권력 이양 규칙으로 priority 를 재평가하여
    세션이 바뀌면(예: 장 마감) 자동으로 후순위로 밀린다.
    """
    init_queue(db_path)
    with _raw_conn(db_path) as conn:
        try:
            conn.execute("BEGIN EXCLUSIVE TRANSACTION;")
            now = _now_iso()
            row = conn.execute(
                task_queue_claim_next_sql(),
                (now,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT;")
                return None
            # 픽업 순간 우선순위 재평가 (권력 이양 반영)
            live_prio = engine_priority(row["engine"])
            conn.execute(
                "UPDATE task_queue SET status='RUNNING', picked_at=?, worker=?, "
                "attempts=attempts+1, priority=? WHERE id=?",
                (now, _worker_id(), live_prio, row["id"]),
            )
            conn.execute("COMMIT;")
        except Exception:
            try:
                conn.execute("ROLLBACK;")
            except Exception:
                pass
            raise

        return Task(
            id=int(row["id"]),
            engine=str(row["engine"]),
            mode=str(row["mode"]),
            payload=json.loads(row["payload"] or "{}"),
            priority=live_prio,
            status="RUNNING",
            attempts=int(row["attempts"]) + 1,
            max_attempts=int(row["max_attempts"]),
        )


# retention hook — complete/fail 후 throttled purge
_task_queue_retention_gate = memory_bounds.ThrottledCallback(interval_sec=3600.0)


def _maybe_purge_terminal_tasks(db_path: Optional[str] = None) -> None:
    if not _task_queue_retention_gate.due():
        return
    try:
        purge_terminal_tasks(db_path=db_path)
    except Exception:
        pass


def purge_terminal_tasks(*, db_path: Optional[str] = None) -> dict[str, int]:
    """
    DONE/FAILED 터미널 상태 정리 + stuck RUNNING heal.

    기관급 큐 hygiene:
      - 날짜 기반 purge (finished_at 접두)
      - 상태별 keep_last (최근 N건 audit trail)
      - RUNNING > 6h → FAILED (워커 crash orphan)
    """
    from datetime import timedelta

    path = db_path or _queue_db_path()
    init_queue(path)
    stats: dict[str, int] = {}
    with _raw_conn(path) as conn:
        try:
            conn.execute("SELECT 1 FROM task_queue LIMIT 1")
        except sqlite3.OperationalError:
            return stats

        for status, days in (
            ("DONE", TASK_QUEUE_DONE_KEEP_DAYS),
            ("FAILED", TASK_QUEUE_FAILED_KEEP_DAYS),
        ):
            cutoff = (datetime.now(_KST) - timedelta(days=int(days))).strftime("%Y-%m-%d")
            try:
                cur = conn.execute(
                    "DELETE FROM task_queue WHERE status=? AND substr(COALESCE(finished_at,''),1,10) < ?",
                    (status, cutoff),
                )
                stats[f"{status.lower()}_by_age"] = int(cur.rowcount or 0)
            except sqlite3.Error:
                pass

        for status, keep in (
            ("DONE", TASK_QUEUE_DONE_KEEP_LAST),
            ("FAILED", TASK_QUEUE_FAILED_KEEP_LAST),
        ):
            try:
                before = conn.execute(
                    "SELECT COUNT(*) FROM task_queue WHERE status=?", (status,)
                ).fetchone()
                n_before = int(before[0] or 0) if before else 0
                conn.execute(
                    f"""
                    DELETE FROM task_queue
                    WHERE status=? AND id NOT IN (
                        SELECT id FROM (
                            SELECT id FROM task_queue
                            WHERE status=?
                            ORDER BY id DESC
                            LIMIT ?
                        )
                    )
                    """,
                    (status, status, int(keep)),
                )
                after = conn.execute(
                    "SELECT COUNT(*) FROM task_queue WHERE status=?", (status,)
                ).fetchone()
                n_after = int(after[0] or 0) if after else 0
                removed = max(0, n_before - n_after)
                if removed:
                    stats[f"{status.lower()}_keep_last"] = removed
            except sqlite3.Error:
                pass

        stuck_cutoff = (datetime.now(_KST) - timedelta(hours=6)).isoformat()
        try:
            cur = conn.execute(
                """
                UPDATE task_queue
                SET status='FAILED', finished_at=?, last_error='STUCK_RUNNING_HEAL'
                WHERE status='RUNNING'
                  AND picked_at IS NOT NULL
                  AND picked_at < ?
                """,
                (_now_iso(), stuck_cutoff),
            )
            stats["running_healed"] = int(cur.rowcount or 0)
        except sqlite3.Error:
            pass

    total = sum(v for v in stats.values() if v > 0)
    if total:
        import logging

        logging.getLogger(__name__).info("bitget task_queue retention: %s", stats)
    return stats


def complete(task_id: int, *, db_path: Optional[str] = None) -> None:
    with _raw_conn(db_path) as conn:
        conn.execute(
            "UPDATE task_queue SET status='DONE', finished_at=? WHERE id=?",
            (_now_iso(), int(task_id)),
        )
    _maybe_purge_terminal_tasks(db_path=db_path)


def fail(
    task_id: int,
    error: str,
    *,
    backoff_sec: float = 0.0,
    db_path: Optional[str] = None,
) -> str:
    """
    실패 처리. attempts >= max_attempts 면 FAILED(영구 폐기), 아니면 backoff 후 PENDING 재진입.
    반환: 최종 상태('FAILED' | 'PENDING').
    """
    result = "PENDING"
    with _raw_conn(db_path) as conn:
        row = conn.execute(
            "SELECT attempts, max_attempts FROM task_queue WHERE id=?", (int(task_id),)
        ).fetchone()
        if row is None:
            return "MISSING"
        attempts = int(row["attempts"])
        max_attempts = int(row["max_attempts"])
        if attempts >= max_attempts:
            conn.execute(
                "UPDATE task_queue SET status='FAILED', finished_at=?, last_error=? WHERE id=?",
                (_now_iso(), str(error)[:1000], int(task_id)),
            )
            result = "FAILED"
        else:
            from datetime import timedelta

            avail = (datetime.now(_KST) + timedelta(seconds=max(0.0, backoff_sec))).isoformat()
            conn.execute(
                "UPDATE task_queue SET status='PENDING', available_at=?, last_error=? WHERE id=?",
                (avail, str(error)[:1000], int(task_id)),
            )
            result = "PENDING"
    if result == "FAILED":
        _maybe_purge_terminal_tasks(db_path=db_path)
    return result


# ---------------------------------------------------------------------------
# CPU 스로틀링 (후순위 강제 동시 실행 방지용)
# ---------------------------------------------------------------------------
def apply_cpu_throttle(*, nice_inc: int = THROTTLE_NICE) -> bool:
    """현재 프로세스 우선순위를 낮춘다(nice +10). Linux 전용, 실패 시 False."""
    if not hasattr(os, "nice"):
        return False
    try:
        os.nice(int(nice_inc))
        return True
    except OSError:
        return False


def should_throttle(task: Task) -> bool:
    """이 작업의 엔진이 현재 세션의 주인이 아니면 스로틀 대상."""
    return task.engine.upper() != primary_engine_now()


# ---------------------------------------------------------------------------
# 워커 루프
# ---------------------------------------------------------------------------
def process_one(
    executor: Callable[[Task], None],
    *,
    backoff_sec: float = 300.0,
    db_path: Optional[str] = None,
) -> Optional[str]:
    """
    한 건을 픽업하여 실행한다.
      - 후순위(비-주인) 엔진이면 os.nice(10) 스로틀 적용 후 실행.
      - 성공 → DONE, 실패 → fail() (재시도 or FAILED).
    반환: 처리한 작업의 최종 상태 (없으면 None).
    """
    task = claim_next(db_path=db_path)
    if task is None:
        return None

    throttled = False
    if should_throttle(task):
        throttled = apply_cpu_throttle()

    try:
        executor(task)
        complete(task.id, db_path=db_path)
        return "DONE"
    except TaskDeferred as d:
        # '실패'가 아니라 '대기 후 재시도' — attempts 소모 없이 PENDING 복귀.
        defer(
            task.id,
            available_in_sec=d.available_in_sec,
            reason=str(d) or "deferred",
            db_path=db_path,
        )
        return "DEFERRED"
    except Exception as e:  # noqa: BLE001
        state = fail(task.id, f"{type(e).__name__}: {e}", backoff_sec=backoff_sec, db_path=db_path)
        return state
    finally:
        if throttled:
            # 자식 프로세스로 분리 실행하지 않는 한 nice 는 되돌릴 수 없음(권한 필요).
            # process_one 은 보통 cron 1회성 프로세스이므로 종료와 함께 해제됨.
            pass


def drain(
    executor: Callable[[Task], None],
    *,
    max_tasks: int = 1000,
    backoff_sec: float = 300.0,
    db_path: Optional[str] = None,
    on_tick: Optional[Callable[[], None]] = None,
) -> int:
    """PENDING 이 없을 때까지(or max_tasks) 순차 처리. 처리 건수 반환.

    on_tick: 매 작업 처리 직후 호출되는 진행 콜백(워커 하트비트 발신 등).
    """
    n = 0
    while n < max_tasks:
        state = process_one(executor, backoff_sec=backoff_sec, db_path=db_path)
        if state is None:
            break
        n += 1
        if on_tick is not None:
            try:
                on_tick()
            except Exception:  # noqa: BLE001 — 하트비트 실패가 작업 처리를 막지 않도록.
                pass
    return n


def queue_stats(*, db_path: Optional[str] = None) -> dict:
    init_queue(db_path)
    with _raw_conn(db_path) as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS c FROM task_queue GROUP BY status"
        ).fetchall()
    return {str(r["status"]): int(r["c"]) for r in rows}


# ===========================================================================
# 안전망: 백로그(적체) 모니터 + 워커 하트비트 (Option C / D)
# ===========================================================================
def oldest_pending_age_sec(*, db_path: Optional[str] = None) -> Optional[float]:
    """가장 오래 대기 중인 PENDING 작업의 대기 시간(초). PENDING 없으면 None."""
    init_queue(db_path)
    with _raw_conn(db_path) as conn:
        row = conn.execute(
            "SELECT enqueued_at FROM task_queue WHERE status='PENDING' "
            "ORDER BY id ASC LIMIT 1"
        ).fetchone()
    if row is None or not row["enqueued_at"]:
        return None
    try:
        enq = datetime.fromisoformat(str(row["enqueued_at"]))
    except ValueError:
        return None
    if enq.tzinfo is None:
        enq = _KST.localize(enq)
    return max(0.0, (datetime.now(_KST) - enq).total_seconds())


def backlog_stats(*, db_path: Optional[str] = None) -> dict:
    """큐 상태 카운트 + 가장 오래된 PENDING 대기시간을 한 번에 반환.

    반환: {pending, running, done, failed, oldest_pending_age_sec}
    """
    stats = queue_stats(db_path=db_path)
    return {
        "pending": int(stats.get("PENDING", 0)),
        "running": int(stats.get("RUNNING", 0)),
        "done": int(stats.get("DONE", 0)),
        "failed": int(stats.get("FAILED", 0)),
        "oldest_pending_age_sec": oldest_pending_age_sec(db_path=db_path),
    }


def worker_heartbeat_path() -> str:
    """워커 생존 신호 파일 경로(큐 DB 와 같은 데이터 디렉터리에 co-locate)."""
    env = (os.environ.get("BITGET_QUEUE_WORKER_HEARTBEAT_PATH") or "").strip()
    if env:
        return os.path.abspath(os.path.expanduser(env))
    try:
        from bitget.infra.data_paths import bitget_data_dir

        return os.path.join(bitget_data_dir(), ".queue_worker_heartbeat")
    except Exception:
        return os.path.join(os.getcwd(), ".queue_worker_heartbeat")


def touch_worker_heartbeat(*, status: str = "alive", extra: Optional[dict] = None) -> None:
    """워커 생존 신호를 원자적으로 기록(타임스탬프/epoch/PID/상태). 실패는 무시."""
    path = worker_heartbeat_path()
    payload = _heartbeat_writer().fill_with_extra(
        extra=extra,
        ts_utc=utc_now_iso(),
        ts_epoch=time.time(),
        status=str(status),
    )
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)
    except OSError:
        pass


def read_worker_heartbeat() -> Optional[dict]:
    path = worker_heartbeat_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def worker_heartbeat_age_sec() -> Optional[float]:
    """마지막 하트비트 이후 경과 초. 파일이 없으면 None(=워커가 한 번도 안 떴음)."""
    hb = read_worker_heartbeat()
    if not hb:
        return None
    epoch = hb.get("ts_epoch")
    if isinstance(epoch, (int, float)):
        return max(0.0, time.time() - float(epoch))
    return None

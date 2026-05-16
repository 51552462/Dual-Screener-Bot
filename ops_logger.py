"""
중앙 집중형 운영 이벤트: append-only `ops_events.sqlite` (WAL).

- 스키마: (ts_utc, component, severity, event, payload_json)
  · 로그 채널: event = `log.python` (logging.Handler)
  · 메트릭 채널: event = `gauge.snapshot` (주기 게이지, payload에 게이지 dict)
  · 생존 채널: event = `heartbeat.tick` (`record_heartbeat(component_name)`)
- 스레드/프로세스/asyncio 공통: 단일 락 + INSERT-only (가벼운 래퍼)
"""
from __future__ import annotations

import json
import logging
import os
import random
import sqlite3
import sys
import threading
import traceback
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import low_ram_sqlite_pragmas
import sqlite_schema_guard
from factory_data_paths import factory_data_dir

_BOT_DIR = factory_data_dir()
OPS_EVENTS_DB_PATH = os.path.join(_BOT_DIR, "ops_events.sqlite")
# 하위 호환: 기존 import 경로
OPS_HEALTH_DB_PATH = OPS_EVENTS_DB_PATH

_write_lock = threading.RLock()
_MAX_PAYLOAD_CHARS = 32000


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            component TEXT NOT NULL,
            severity TEXT NOT NULL,
            event TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_events_ts ON ops_events (ts_utc DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_events_event_ts ON ops_events (event, ts_utc DESC)"
    )
    sqlite_schema_guard.apply_column_migrations(conn, "ops_events")
    conn.commit()


def _connect_write() -> sqlite3.Connection:
    os.makedirs(_BOT_DIR, exist_ok=True)
    conn = sqlite3.connect(OPS_EVENTS_DB_PATH, timeout=30.0, check_same_thread=False)
    _ensure_schema(conn)
    low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
    return conn


def insert_ops_event(
    *,
    component: str,
    severity: str,
    event: str,
    payload: Optional[dict[str, Any]] = None,
    ts_utc: Optional[str] = None,
    max_retries: int = 6,
) -> bool:
    """
    초경량 append INSERT. 스레드·프로세스·asyncio 어디서든 호출 가능(동기).
    """
    ts_use = ts_utc or _utc_now_iso()
    comp = (component or "unknown")[:128]
    sev = (severity or "INFO")[:32]
    ev = (event or "misc")[:256]
    try:
        blob = json.dumps(payload if isinstance(payload, dict) else {}, ensure_ascii=False, default=str)
    except Exception:
        blob = "{}"
    if len(blob) > _MAX_PAYLOAD_CHARS:
        blob = blob[: _MAX_PAYLOAD_CHARS - 20] + '","_truncated":true}'

    for attempt in range(max_retries):
        try:
            with _write_lock:
                conn = _connect_write()
                try:
                    conn.execute(
                        """
                        INSERT INTO ops_events (ts_utc, component, severity, event, payload_json)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (ts_use, comp, sev, ev, blob),
                    )
                    conn.commit()
                finally:
                    conn.close()
            return True
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() or "busy" in str(e).lower():
                time.sleep(0.05 + random.uniform(0, 0.1) * (attempt + 1))
                continue
            return False
        except Exception:
            return False
    return False


def record_gauge_snapshot(component: str, metrics: dict[str, Any]) -> None:
    """메트릭 전용 채널 (로그와 분리)."""
    insert_ops_event(
        component=component,
        severity="INFO",
        event="gauge.snapshot",
        payload=metrics if isinstance(metrics, dict) else {"value": metrics},
    )


def record_heartbeat(component_name: str, **extra: Any) -> None:
    """
    스캐너/데몬 생존 신호 (INSERT 1건, payload 최소화).
    `extra` 는 선택적 키(예: thread=...)만 넣을 것.
    """
    comp = (component_name or "unknown").strip()[:128]
    payload: dict[str, Any] = {"kind": "liveness"}
    for k, v in extra.items():
        if k and str(k)[:64]:
            try:
                payload[str(k)[:64]] = v
            except Exception:
                pass
    insert_ops_event(
        component=comp,
        severity="INFO",
        event="heartbeat.tick",
        payload=payload,
    )


class OpsEventsSQLiteHandler(logging.Handler):
    """logging.Handler → ops_events (`log.python` 채널)."""

    def __init__(self, default_component: str = "root") -> None:
        super().__init__(level=logging.DEBUG)
        self.default_component = (default_component or "root")[:128]

    def emit(self, record: logging.LogRecord) -> None:
        try:
            comp = getattr(record, "ops_component", None) or self.default_component
            comp = str(comp)[:128]
            sev = record.levelname[:32] if record.levelname else "NOTSET"
            payload: dict[str, Any] = {
                "logger": record.name,
                "message": (record.getMessage() or "")[:8000],
                "pathname": getattr(record, "pathname", "") or "",
                "lineno": int(getattr(record, "lineno", 0) or 0),
            }
            if record.exc_info:
                try:
                    payload["exc_info"] = "".join(
                        traceback.format_exception(*record.exc_info)
                    )[:12000]
                except Exception:
                    payload["exc_info"] = "exc_format_failed"
            insert_ops_event(
                component=comp,
                severity=sev,
                event="log.python",
                payload=payload,
            )
        except Exception:
            self.handleError(record)


# 구 호환 클래스명
OpsHealthSQLiteHandler = OpsEventsSQLiteHandler


def configure_root_ops_logging(
    *,
    default_component: str = "main",
    level: int = logging.INFO,
) -> None:
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, OpsEventsSQLiteHandler):
            return
    root.setLevel(min(level, root.level or logging.DEBUG))
    h = OpsEventsSQLiteHandler(default_component=default_component)
    h.setLevel(logging.WARNING)
    root.addHandler(h)
    if not any(type(x).__name__ == "StreamHandler" for x in root.handlers if x is not h):
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(level)
        sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        root.addHandler(sh)


def install_unhandled_exception_hooks() -> None:
    def _thread_excepthook(args) -> None:  # type: ignore[no-untyped-def]
        logging.getLogger("unhandled").error(
            "Unhandled in thread %r",
            getattr(args.thread, "name", "?"),
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    def _sys_excepthook(exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        logging.getLogger("unhandled").critical(
            "Unhandled exception (main)",
            exc_info=(exc_type, exc, tb),
        )

    if hasattr(threading, "excepthook"):
        threading.excepthook = _thread_excepthook  # type: ignore[assignment]
    sys.excepthook = _sys_excepthook


def _since_utc_iso(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def fetch_recent_rows(
    *,
    hours: float = 1.0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    if not os.path.isfile(OPS_EVENTS_DB_PATH):
        return []
    since = _since_utc_iso(hours)
    out: list[dict[str, Any]] = []
    try:
        uri = f"file:{OPS_EVENTS_DB_PATH.replace(os.sep, '/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=15.0, check_same_thread=False)
        try:
            conn.execute("PRAGMA query_only=ON;")
        except sqlite3.OperationalError:
            pass
        low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
        cur = conn.execute(
            """
            SELECT ts_utc, component, severity, event, payload_json
            FROM ops_events
            WHERE ts_utc >= ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (since, int(limit)),
        )
        for r in cur.fetchall():
            try:
                pj = json.loads(r[4] or "{}")
            except json.JSONDecodeError:
                pj = {"_raw": (r[4] or "")[:500]}
            out.append(
                {
                    "ts_utc": r[0],
                    "component": r[1],
                    "severity": r[2],
                    "event": r[3],
                    "payload": pj,
                }
            )
        conn.close()
    except Exception:
        return []
    return out


def fetch_heartbeat_ticks(
    *,
    hours: float = 2.0,
    limit: int = 4000,
) -> list[dict[str, Any]]:
    """event=heartbeat.tick 만 조회 (대시보드 SLO·심장박동)."""
    if not os.path.isfile(OPS_EVENTS_DB_PATH):
        return []
    since = _since_utc_iso(hours)
    out: list[dict[str, Any]] = []
    try:
        uri = f"file:{OPS_EVENTS_DB_PATH.replace(os.sep, '/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=15.0, check_same_thread=False)
        try:
            conn.execute("PRAGMA query_only=ON;")
        except sqlite3.OperationalError:
            pass
        low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
        cur = conn.execute(
            """
            SELECT ts_utc, component, severity, event, payload_json
            FROM ops_events
            WHERE ts_utc >= ? AND event = 'heartbeat.tick'
            ORDER BY id DESC
            LIMIT ?
            """,
            (since, int(limit)),
        )
        for r in cur.fetchall():
            try:
                pj = json.loads(r[4] or "{}")
            except json.JSONDecodeError:
                pj = {}
            out.append(
                {
                    "ts_utc": r[0],
                    "component": r[1],
                    "severity": r[2],
                    "event": r[3],
                    "payload": pj,
                }
            )
        conn.close()
    except Exception:
        return []
    return out


def recent_error_summaries_for_console(*, hours: float = 1.0, limit: int = 5) -> list[str]:
    rows = fetch_recent_rows(hours=hours, limit=400)
    errs: list[str] = []
    for row in rows:
        if row.get("event") != "log.python":
            continue
        lv = str(row.get("severity") or "").upper()
        if lv in ("ERROR", "CRITICAL", "WARNING"):
            msg = (row.get("payload") or {}).get("message") or row.get("event")
            ts = row.get("ts_utc", "")[:19]
            errs.append(f"[{ts}] {row.get('component')}: {str(msg)[:160]}")
        if len(errs) >= limit:
            break
    return errs

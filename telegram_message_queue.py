"""
영속 텔레그램 발송 큐 (message_queue.sqlite).

스캐너는 INSERT 만 수행한다. async 데몬은 인메모리 asyncio.Queue 로 우선 스케줄링하고,
SQLite PENDING 은 영속·재기동 장애 복구용으로 유지한다. 전송 성공 시 행 DELETE.
429 등은 기존과 유사한 백오프로 재시도한다.

send_profile:
  - 'default': verify=False, 레거시 스캐너(kr/usa/…) 스타일
  - 'html':    parse_mode=HTML, verify=True (bitget_master_scanner)
  - 'html_ro': parse_mode=HTML, verify=False + 400 시 평문 재시도 (master/nulrim/ema5 등)
"""
from __future__ import annotations

import os
import random
import sqlite3
import threading
import time
from typing import Any, Optional

import asyncio

import low_ram_sqlite_pragmas
import sqlite_schema_guard
from factory_data_paths import factory_data_dir

_BOT_DIR = factory_data_dir()
MESSAGE_QUEUE_DB_PATH = os.path.join(_BOT_DIR, "message_queue.sqlite")

_mq_lock = threading.RLock()
_schema_ready = False

# async_telegram_daemon 인메모리 큐 연동 (SQLite 는 영속·장애 복구용 유지)
_BRIDGE_LOOP: Optional[asyncio.AbstractEventLoop] = None
_BRIDGE_QUEUE: Optional[asyncio.Queue] = None

# DANTE_ASYNC_TELEGRAM_DAEMON=1 일 때 동기 스레드 대신 async_telegram_daemon 이 소비.
_LAST_DAEMON_REGISTRATION: Optional[tuple[str, str, str, bool]] = None


def set_telegram_memory_bridge(
    loop: Optional[asyncio.AbstractEventLoop],
    q: Optional[asyncio.Queue],
) -> None:
    """async 데몬이 기동되면 (loop, asyncio.Queue) 등록. 종료 시 (None, None)."""
    global _BRIDGE_LOOP, _BRIDGE_QUEUE
    _BRIDGE_LOOP = loop
    _BRIDGE_QUEUE = q


def _telegram_mem_queue_maxsize() -> int:
    try:
        from bitget.infra.memory_policy import TELEGRAM_MEM_QUEUE_MAXSIZE

        return max(500, int(TELEGRAM_MEM_QUEUE_MAXSIZE))
    except Exception:
        return 4000


def _notify_telegram_memory_bridge(target: str, msg_id: int) -> None:
    loop = _BRIDGE_LOOP
    q = _BRIDGE_QUEUE
    if loop is None or q is None:
        return
    item = (str(target), int(msg_id))

    def _schedule() -> None:
        try:
            q.put_nowait(item)
        except asyncio.QueueFull:
            # SQLite PENDING 유지 — async dispatcher 가 claim_next 로 회수
            pass

    try:
        loop.call_soon_threadsafe(_schedule)
    except Exception:
        pass


def list_all_pending_message_refs() -> list[tuple[str, int]]:
    """장애 복구: 재기동 시 PENDING 전부 (target, id) 순서대로."""
    if not os.path.isfile(MESSAGE_QUEUE_DB_PATH):
        return []
    with _mq_lock:
        conn = _connect()
        try:
            _ensure_schema(conn)
            cur = conn.execute(
                "SELECT target, id FROM msg_queue WHERE status = 'PENDING' ORDER BY id ASC"
            )
            return [(str(r["target"]), int(r["id"])) for r in cur.fetchall()]
        finally:
            conn.close()


def claim_pending_message_by_id(target: str, msg_id: int) -> Optional[dict[str, Any]]:
    """특정 id 를 PENDING → SENDING 으로 원자 점유. 실패 시 None."""
    with _mq_lock:
        conn = _connect()
        try:
            _ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            cur2 = conn.execute(
                """
                UPDATE msg_queue SET status = 'SENDING'
                WHERE id = ? AND target = ? AND status = 'PENDING'
                """,
                (int(msg_id), str(target)),
            )
            if cur2.rowcount != 1:
                conn.rollback()
                return None
            cur = conn.execute(
                """
                SELECT id, img_path, caption, send_profile
                FROM msg_queue WHERE id = ?
                """,
                (int(msg_id),),
            )
            row = cur.fetchone()
            conn.commit()
            if row is None:
                return None
            return {
                "id": int(row["id"]),
                "img_path": row["img_path"],
                "caption": str(row["caption"] or ""),
                "send_profile": str(row["send_profile"] or "default"),
            }
        except sqlite3.OperationalError:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass


def message_queue_db_path() -> str:
    return MESSAGE_QUEUE_DB_PATH


def _connect() -> sqlite3.Connection:
    os.makedirs(_BOT_DIR, exist_ok=True)
    conn = sqlite3.connect(MESSAGE_QUEUE_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    low_ram_sqlite_pragmas.apply_oom_safe_pragmas(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    global _schema_ready
    if _schema_ready:
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS msg_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target TEXT NOT NULL,
            img_path TEXT,
            caption TEXT,
            status TEXT NOT NULL DEFAULT 'PENDING',
            send_profile TEXT NOT NULL DEFAULT 'default'
        )
        """
    )
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(msg_queue)").fetchall()}
    if "send_profile" not in cols:
        try:
            conn.execute(
                "ALTER TABLE msg_queue ADD COLUMN send_profile TEXT NOT NULL DEFAULT 'default'"
            )
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE msg_queue ADD COLUMN send_profile TEXT DEFAULT 'default'")
    sqlite_schema_guard.apply_column_migrations(conn, "msg_queue")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_msg_queue_pending "
        "ON msg_queue (target, status, id)"
    )
    conn.commit()
    _schema_ready = True


def ensure_message_queue_schema() -> None:
    with _mq_lock:
        conn = _connect()
        try:
            _ensure_schema(conn)
        finally:
            conn.close()


def enqueue_telegram(
    target: str,
    img_path: Optional[str],
    caption: str,
    *,
    enabled: bool = True,
    send_profile: str = "default",
) -> Optional[int]:
    """PENDING 행 INSERT. enabled False 이면 DB에 넣지 않음."""
    if not enabled or not target:
        return None
    ensure_message_queue_schema()
    cap = caption or ""
    img = img_path if img_path else None
    prof = send_profile if send_profile in ("default", "html", "html_ro") else "default"
    last_id: Optional[int] = None
    with _mq_lock:
        conn = _connect()
        try:
            _ensure_schema(conn)
            cur = conn.execute(
                """
                INSERT INTO msg_queue (target, img_path, caption, status, send_profile)
                VALUES (?, ?, ?, 'PENDING', ?)
                """,
                (str(target), img, cap, prof),
            )
            conn.commit()
            last_id = int(cur.lastrowid) if cur.lastrowid is not None else None
        finally:
            conn.close()
    if last_id is not None:
        _notify_telegram_memory_bridge(str(target), last_id)
    return last_id


def count_pending_for_target(target: str) -> int:
    if not os.path.isfile(MESSAGE_QUEUE_DB_PATH):
        return 0
    with _mq_lock:
        conn = _connect()
        try:
            _ensure_schema(conn)
            cur = conn.execute(
                "SELECT COUNT(*) AS c FROM msg_queue WHERE target = ? AND status = 'PENDING'",
                (target,),
            )
            row = cur.fetchone()
            return int(row["c"]) if row else 0
        finally:
            conn.close()


def count_all_pending_messages() -> int:
    """MAIN+PROMO PENDING 건수 합계 (관제·헬스용)."""
    return int(count_pending_for_target("MAIN")) + int(count_pending_for_target("PROMO"))


def get_telegram_daemon_registration() -> Optional[tuple[str, str, str, bool]]:
    """start_telegram_queue_daemons 마지막 호출 인자 (async 타워가 기동 시 사용)."""
    return _LAST_DAEMON_REGISTRATION


def claim_next_pending_message(target: str) -> Optional[dict[str, Any]]:
    """
    한 건을 PENDING → SENDING 으로 원자 갱신 후 메타 dict 반환.
    없거나 경합 실패 시 None.
    """
    with _mq_lock:
        conn = _connect()
        try:
            _ensure_schema(conn)
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                SELECT id, img_path, caption, send_profile
                FROM msg_queue
                WHERE target = ? AND status = 'PENDING'
                ORDER BY id ASC
                LIMIT 1
                """,
                (target,),
            )
            row = cur.fetchone()
            if row is None:
                conn.commit()
                return None
            msg_id = int(row["id"])
            cur2 = conn.execute(
                "UPDATE msg_queue SET status = 'SENDING' WHERE id = ? AND status = 'PENDING'",
                (msg_id,),
            )
            if cur2.rowcount != 1:
                conn.rollback()
                return None
            conn.commit()
            return {
                "id": msg_id,
                "img_path": row["img_path"],
                "caption": str(row["caption"] or ""),
                "send_profile": str(row["send_profile"] or "default"),
            }
        except sqlite3.OperationalError:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass


def finalize_queue_send(msg_id: int, *, success: bool, send_enabled: bool) -> None:
    """전송 결과 반영: 성공(또는 발송 비활성) 시 DELETE, 실패 시 PENDING 복귀."""
    with _mq_lock:
        conn = _connect()
        try:
            _ensure_schema(conn)
            if success or not send_enabled:
                conn.execute("DELETE FROM msg_queue WHERE id = ?", (msg_id,))
            else:
                conn.execute(
                    "UPDATE msg_queue SET status = 'PENDING' WHERE id = ?",
                    (msg_id,),
                )
            conn.commit()
        finally:
            conn.close()


def wait_telegram_queue_drained(
    targets: tuple[str, ...] = ("MAIN", "PROMO"),
    *,
    timeout_sec: float = 7200.0,
    poll_sec: float = 0.25,
) -> bool:
    """지정 target 들에 PENDING 이 없을 때까지 대기. 타임아웃 시 False."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if all(count_pending_for_target(t) == 0 for t in targets):
            return True
        time.sleep(poll_sec)
    return False


def _send_one(
    img_path: Optional[str],
    caption: str,
    token: str,
    chat_id: str,
    send_enabled: bool,
    send_profile: str,
) -> bool:
    import re as _re
    import requests

    if not send_enabled or not token or not chat_id:
        return True

    safe_caption = (
        caption[:1000] + "\n...(글자수 제한으로 요약됨)"
        if len(caption) > 1000
        else caption
    )
    use_html = send_profile in ("html", "html_ro")
    verify = send_profile == "html"
    html_ro = send_profile == "html_ro"

    res = None
    for attempt in range(5):
        try:
            if img_path and os.path.exists(img_path):
                with open(img_path, "rb") as f:
                    if use_html:
                        res = requests.post(
                            f"https://api.telegram.org/bot{token}/sendPhoto",
                            data={
                                "chat_id": chat_id,
                                "caption": safe_caption,
                                "parse_mode": "HTML",
                            },
                            files={"photo": f},
                            timeout=60,
                            verify=verify,
                        )
                        if (
                            html_ro
                            and res is not None
                            and res.status_code == 400
                        ):
                            plain_caption = (
                                _re.sub(r"<[^>]+>", "", safe_caption)
                                .replace("&", "and")
                                .replace("<", "〈")
                                .replace(">", "〉")
                            )
                            with open(img_path, "rb") as f2:
                                res = requests.post(
                                    f"https://api.telegram.org/bot{token}/sendPhoto",
                                    data={
                                        "chat_id": chat_id,
                                        "caption": plain_caption,
                                    },
                                    files={"photo": f2},
                                    timeout=60,
                                    verify=False,
                                )
                    else:
                        res = requests.post(
                            f"https://api.telegram.org/bot{token}/sendPhoto",
                            params={"chat_id": chat_id, "caption": safe_caption},
                            files={"photo": f},
                            timeout=60,
                            verify=False,
                        )
            else:
                payload: dict[str, Any] = {
                    "chat_id": chat_id,
                    "text": safe_caption,
                }
                if use_html:
                    payload["parse_mode"] = "HTML"
                res = requests.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json=payload,
                    timeout=60,
                    verify=verify if use_html else False,
                )
                if (
                    html_ro
                    and res is not None
                    and res.status_code == 400
                ):
                    plain_caption = (
                        _re.sub(r"<[^>]+>", "", safe_caption)
                        .replace("&", "and")
                        .replace("<", "〈")
                        .replace(">", "〉")
                    )
                    res = requests.post(
                        f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id, "text": plain_caption},
                        timeout=60,
                        verify=False,
                    )

            if res is not None and res.status_code == 200:
                return True
            if res is not None and res.status_code == 429:
                time.sleep(10 + random.uniform(0, 2))
                continue
        except requests.exceptions.ReadTimeout:
            break
        except Exception:
            time.sleep(2)
    if res is not None and getattr(res, "status_code", None) not in (200, None):
        print(f"🚨 텔레그램 발송 에러 ({res.status_code}): {getattr(res, 'text', '')}")
    return False


def _daemon_loop(
    target: str,
    token: str,
    chat_id: str,
    send_enabled: bool,
) -> None:
    while True:
        row_dict = claim_next_pending_message(target)
        if row_dict is None:
            time.sleep(0.25)
            continue
        msg_id = int(row_dict["id"])
        img = row_dict["img_path"]
        img_s = str(img) if img is not None else None
        cap = str(row_dict["caption"] or "")
        prof = str(row_dict["send_profile"] or "default")

        ok = _send_one(
            img_s, cap, token, chat_id, send_enabled, prof
        )
        time.sleep(1.2 if prof == "html" else 1.5)

        finalize_queue_send(msg_id, success=ok, send_enabled=send_enabled)


def start_telegram_queue_daemons(
    token_main: str,
    token_promo: str,
    chat_id: str,
    send_enabled: bool,
) -> None:
    """
    MAIN / PROMO 폴링.
    환경변수 DANTE_ASYNC_TELEGRAM_DAEMON=1 이면 동기 스레드는 기동하지 않고
    인자만 등록한다(async_telegram_daemon 또는 main 이 소비).
    """
    global _LAST_DAEMON_REGISTRATION
    ensure_message_queue_schema()
    tok_p = token_promo or token_main
    _LAST_DAEMON_REGISTRATION = (token_main, tok_p, chat_id, send_enabled)
    if os.environ.get("DANTE_ASYNC_TELEGRAM_DAEMON") == "1":
        return
    threading.Thread(
        target=_daemon_loop,
        args=("MAIN", token_main, chat_id, send_enabled),
        daemon=True,
    ).start()
    threading.Thread(
        target=_daemon_loop,
        args=("PROMO", tok_p, chat_id, send_enabled),
        daemon=True,
    ).start()

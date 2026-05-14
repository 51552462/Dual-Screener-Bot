"""
message_queue.sqlite 기반 비동기 텔레그램 발송기 (aiohttp + asyncio).

- PENDING → (인메모리 asyncio.Queue 우선) → SENDING(claim) → aiohttp 전송 → 성공 시 DELETE
- 재기동 시 SQLite PENDING 전부를 메모리 큐로 적재(장애 복구)
- 429: 해당 메시지만 지수형 백오프 후 재시도, 한도 초과 시 PENDING 복귀(fail finalize)
- aiohttp 경로 실패 시 동기 `requests`(`telegram_message_queue._send_one`) 1회 폴백
- DB 접근: asyncio.to_thread (이벤트 루프 비블로킹)
- 스캐너 스레드 → 루프: `loop.call_soon_threadsafe` + `create_task` 로 인메모리 큐 적재
- aiohttp: 세마포어 + TCPConnector 로 동시 8~16 커넥션 병렬 전송, 워커 코루틴 풀
"""
from __future__ import annotations

import asyncio
import functools
import os
import random
import sys
import re as _re
import threading
import time
from collections import deque
from typing import Any, Optional

import aiohttp

import ops_logger
from telegram_message_queue import (
    _send_one,
    claim_next_pending_message,
    claim_pending_message_by_id,
    ensure_message_queue_schema,
    finalize_queue_send,
    list_all_pending_message_refs,
    set_telegram_memory_bridge,
    count_all_pending_messages,
)


# Telegram API 429 이벤트 타임스탬프 (monotonic, 최근 60초 건수용)
_recent_429_mono: deque[float] = deque(maxlen=4000)


def _register_telegram_429() -> None:
    _recent_429_mono.append(time.monotonic())


def _count_429_last_60s() -> int:
    now = time.monotonic()
    while _recent_429_mono and (now - _recent_429_mono[0]) > 60.0:
        _recent_429_mono.popleft()
    return len(_recent_429_mono)


def _telegram_http_concurrency() -> int:
    """동시 텔레그램 HTTP 연결 수. TELEGRAM_CONCURRENCY_LIMIT 기본 4, 레거시 TELEGRAM_AIOHTTP_CONCURRENCY 병행."""
    lim_raw = os.environ.get("TELEGRAM_CONCURRENCY_LIMIT", "").strip()
    if lim_raw:
        try:
            v = int(lim_raw)
            return max(1, min(32, v))
        except ValueError:
            pass
    try:
        raw = int(os.environ.get("TELEGRAM_AIOHTTP_CONCURRENCY", "0") or 0)
    except ValueError:
        raw = 0
    if 8 <= raw <= 16:
        return raw
    return 4


def _safe_caption(caption: str) -> str:
    cap = caption or ""
    return cap[:1000] + "\n...(글자수 제한으로 요약됨)" if len(cap) > 1000 else cap


async def _read_photo_bytes(path: str) -> bytes:
    def _read() -> bytes:
        with open(path, "rb") as f:
            return f.read()

    return await asyncio.to_thread(_read)


async def _post_json(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict[str, Any],
    *,
    ssl_flag: bool | None,
) -> aiohttp.ClientResponse:
    return await session.post(url, json=payload, ssl=ssl_flag)


async def _post_multipart(
    session: aiohttp.ClientSession,
    url: str,
    data: aiohttp.FormData,
    *,
    ssl_flag: bool | None,
) -> aiohttp.ClientResponse:
    return await session.post(url, data=data, ssl=ssl_flag)


async def _send_one_message_async(
    session: aiohttp.ClientSession,
    *,
    img_path: Optional[str],
    caption: str,
    token: str,
    chat_id: str,
    send_enabled: bool,
    send_profile: str,
) -> str:
    """
    반환값:
      'ok' — 전송 성공
      '429' — Rate limit (해당 메시지만 백오프 재시도)
      'fail' — 그 외 실패(재시도 없이 PENDING 복귀)
    """
    if not send_enabled or not token or not chat_id:
        return "ok"

    safe_caption = _safe_caption(caption)
    use_html = send_profile in ("html", "html_ro")
    verify = send_profile == "html"
    html_ro = send_profile == "html_ro"
    ssl_flag = True if verify else False

    res: Optional[aiohttp.ClientResponse] = None
    try:
        if img_path and os.path.exists(img_path):
            photo_bytes = await _read_photo_bytes(img_path)
            form = aiohttp.FormData()
            form.add_field("chat_id", chat_id)
            form.add_field("caption", safe_caption)
            if use_html:
                form.add_field("parse_mode", "HTML")
                form.add_field(
                    "photo",
                    photo_bytes,
                    filename="chart.png",
                    content_type="image/png",
                )
                res = await _post_multipart(
                    session,
                    f"https://api.telegram.org/bot{token}/sendPhoto",
                    form,
                    ssl_flag=ssl_flag,
                )
                if html_ro and res.status == 400:
                    await res.release()
                    plain_caption = (
                        _re.sub(r"<[^>]+>", "", safe_caption)
                        .replace("&", "and")
                        .replace("<", "〈")
                        .replace(">", "〉")
                    )
                    form2 = aiohttp.FormData()
                    form2.add_field("chat_id", chat_id)
                    form2.add_field("caption", plain_caption)
                    form2.add_field(
                        "photo",
                        photo_bytes,
                        filename="chart.png",
                        content_type="image/png",
                    )
                    res = await _post_multipart(
                        session,
                        f"https://api.telegram.org/bot{token}/sendPhoto",
                        form2,
                        ssl_flag=False,
                    )
            else:
                form = aiohttp.FormData()
                form.add_field("chat_id", chat_id)
                form.add_field("caption", safe_caption)
                form.add_field(
                    "photo",
                    photo_bytes,
                    filename="chart.png",
                    content_type="image/png",
                )
                res = await _post_multipart(
                    session,
                    f"https://api.telegram.org/bot{token}/sendPhoto",
                    form,
                    ssl_flag=False,
                )
        else:
            payload: dict[str, Any] = {"chat_id": chat_id, "text": safe_caption}
            if use_html:
                payload["parse_mode"] = "HTML"
            res = await _post_json(
                session,
                f"https://api.telegram.org/bot{token}/sendMessage",
                payload,
                ssl_flag=ssl_flag if use_html else False,
            )
            if html_ro and res.status == 400:
                await res.release()
                plain_caption = (
                    _re.sub(r"<[^>]+>", "", safe_caption)
                    .replace("&", "and")
                    .replace("<", "〈")
                    .replace(">", "〉")
                )
                res = await _post_json(
                    session,
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    {"chat_id": chat_id, "text": plain_caption},
                    ssl_flag=False,
                )

        if res is None:
            return "fail"
        try:
            status = res.status
            if status == 200:
                return "ok"
            if status == 429:
                _register_telegram_429()
                return "429"
            if status not in (200, 429):
                txt = await res.text()
                print(f"🚨 [async_telegram] 발송 에러 ({status}): {txt[:500]}")
            return "fail"
        finally:
            res.release()
    except asyncio.TimeoutError:
        return "fail"
    except aiohttp.ClientError:
        return "fail"
    except Exception:
        return "fail"


async def _send_with_429_retries(
    session: aiohttp.ClientSession,
    row: dict[str, Any],
    token: str,
    chat_id: str,
    send_enabled: bool,
) -> bool:
    """단일 큐 행에 대해 429 시에만 메시지 단위 백오프 재시도. 그 외 실패는 False."""
    img = row.get("img_path")
    img_s = str(img) if img is not None else None
    cap = str(row.get("caption") or "")
    prof = str(row.get("send_profile") or "default")
    max_429_rounds = 14
    r429 = 0
    while True:
        code = await _send_one_message_async(
            session,
            img_path=img_s,
            caption=cap,
            token=token,
            chat_id=chat_id,
            send_enabled=send_enabled,
            send_profile=prof,
        )
        if code == "ok":
            return True
        if code == "429":
            r429 += 1
            if r429 > max_429_rounds:
                return False
            delay = min(120.0, 10.0 * (2 ** min(r429 - 1, 4)) + random.uniform(0, 2.5))
            await asyncio.sleep(delay)
            continue
        return False


async def _send_row_with_aiohttp_and_requests_fallback(
    session: aiohttp.ClientSession,
    row: dict[str, Any],
    token: str,
    chat_id: str,
    send_enabled: bool,
) -> bool:
    """aiohttp(+429 재시도) → 실패 시 requests `_send_one` 1회."""
    ok = await _send_with_429_retries(session, row, token, chat_id, send_enabled)
    if ok:
        return True
    img = row.get("img_path")
    img_s = str(img) if img is not None else None
    cap = str(row.get("caption") or "")
    prof = str(row.get("send_profile") or "default")
    return await asyncio.to_thread(
        _send_one, img_s, cap, token, chat_id, send_enabled, prof
    )


async def run_async_telegram_daemon(
    token_main: str,
    token_promo: str,
    chat_id: str,
    send_enabled: bool,
) -> None:
    await asyncio.to_thread(ensure_message_queue_schema)
    tok_p = token_promo or token_main
    conc = _telegram_http_concurrency()

    mem_q: asyncio.Queue = asyncio.Queue()
    work_q: asyncio.Queue = asyncio.Queue(maxsize=4000)

    flight_lock = asyncio.Lock()
    flight_cell = [0]

    refs = await asyncio.to_thread(list_all_pending_message_refs)
    for t, mid in refs:
        await mem_q.put((t, mid))

    loop = asyncio.get_running_loop()
    set_telegram_memory_bridge(loop, mem_q)

    http_sem = asyncio.Semaphore(conc)
    connector = aiohttp.TCPConnector(limit=conc)
    timeout = aiohttp.ClientTimeout(total=60)

    async def ops_heartbeat_loop() -> None:
        while True:
            await asyncio.sleep(60.0)
            try:
                pending_sql = int(await asyncio.to_thread(count_all_pending_messages))
                q_mem = int(mem_q.qsize())
                q_work = int(work_q.qsize())
                n429 = _count_429_last_60s()
                async with flight_lock:
                    inflight = int(flight_cell[0])
                payload = {
                    "telegram_queue_pending": pending_sql,
                    "telegram_queue_pending_sqlite": pending_sql,
                    "mem_q_size": q_mem,
                    "work_q_size": q_work,
                    "telegram_http_429_last_60s": n429,
                    "telegram_http_concurrency": int(conc),
                    "telegram_http_in_flight": inflight,
                }

                def _write_hb() -> None:
                    ops_logger.record_gauge_snapshot("async_telegram_daemon", payload)
                    ops_logger.record_heartbeat("async_telegram_daemon")

                await asyncio.to_thread(_write_hb)
            except Exception:
                pass

    async def dispatcher() -> None:
        while True:
            row: Optional[dict[str, Any]] = None
            tok_use: Optional[str] = None
            try:
                t_wake, mid_wake = await asyncio.wait_for(mem_q.get(), timeout=0.22)
            except asyncio.TimeoutError:
                t_wake, mid_wake = None, None

            if t_wake is not None and mid_wake is not None:
                row = await asyncio.to_thread(
                    claim_pending_message_by_id, str(t_wake), int(mid_wake)
                )
                if row:
                    tok_use = token_main if str(t_wake) == "MAIN" else tok_p

            if row is None:
                for tgt, tok in (("MAIN", token_main), ("PROMO", tok_p)):
                    row = await asyncio.to_thread(claim_next_pending_message, tgt)
                    if row:
                        tok_use = tok
                        break

            if row is None or tok_use is None:
                await asyncio.sleep(0.04)
                continue

            await work_q.put((row, tok_use))

    async def worker(session: aiohttp.ClientSession, wid: int) -> None:
        while True:
            row, tok_use = await work_q.get()
            msg_id = int(row["id"])
            prof = str(row.get("send_profile") or "default")
            ok = False
            try:
                async with http_sem:
                    async with flight_lock:
                        flight_cell[0] += 1
                    try:
                        ok = await _send_row_with_aiohttp_and_requests_fallback(
                            session, row, tok_use, chat_id, send_enabled
                        )
                    finally:
                        async with flight_lock:
                            flight_cell[0] -= 1
            except Exception as ex:
                print(f"⚠️ [async_telegram] worker {wid}: {ex}")
            finally:
                await asyncio.to_thread(
                    functools.partial(
                        finalize_queue_send,
                        msg_id,
                        success=ok,
                        send_enabled=send_enabled,
                    )
                )
                work_q.task_done()
                await asyncio.sleep(1.2 if prof == "html" else 0.25)

    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            d_task = asyncio.create_task(dispatcher(), name="telegram_dispatcher")
            hb_task = asyncio.create_task(ops_heartbeat_loop(), name="telegram_ops_heartbeat")
            w_tasks = [
                asyncio.create_task(worker(session, i), name=f"telegram_worker_{i}")
                for i in range(conc)
            ]
            await asyncio.gather(d_task, hb_task, *w_tasks)
    finally:
        set_telegram_memory_bridge(None, None)


def start_async_telegram_daemon_thread(
    token_main: str,
    token_promo: str,
    chat_id: str,
    send_enabled: bool,
) -> threading.Thread:
    """별도 데몬 스레드에서 asyncio.run 으로 이벤트 루프 가동."""

    def _runner() -> None:
        asyncio.run(
            run_async_telegram_daemon(
                token_main,
                token_promo,
                chat_id,
                send_enabled,
            )
        )

    t = threading.Thread(
        target=_runner,
        daemon=True,
        name="async_telegram_daemon",
    )
    t.start()
    return t


def main() -> None:
    """systemd 등 단독 프로세스 진입점."""
    os.environ.setdefault("DANTE_ASYNC_TELEGRAM_DAEMON", "1")
    from telegram_message_queue import get_telegram_daemon_registration

    reg = get_telegram_daemon_registration()
    if not reg:
        print(
            "⚠️ [async_telegram_daemon] 큐 데몬 등록 없음 — 토큰/채팅 ID 환경변수를 확인하세요.",
            file=sys.stderr,
        )
        sys.exit(2)
    tm, tp, cid, en = reg
    asyncio.run(run_async_telegram_daemon(tm, tp, cid, en))


if __name__ == "__main__":
    main()

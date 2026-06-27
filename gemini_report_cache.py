"""
리포트 생성 파사드: SQLite 캐시 + REPORT_BACKEND(None|template|gemini) 백엔드 분기.
- google.generativeai 는 GeminiBackend 메서드 내부에서만 지연 로드.
- `GEMINI_API_KEY="k1,k2,k3"` 콤마 구분, 3회 시도마다 키 순환
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime
from typing import Any, Callable, List, Optional, Protocol, Tuple, runtime_checkable

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ------- Gemini API: 다중 스캐너 스레드 → 전용 asyncio 루프에서 Semaphore(2) + to_thread -------
_gem_gate_lock = threading.Lock()
_gem_gate_loop: Optional[asyncio.AbstractEventLoop] = None
_gem_gate_sem: Optional[asyncio.Semaphore] = None
_gem_gate_ready = threading.Event()

# ops_events 게이지: 게이트 동시성·대기 (스캐너 스레드 → gate 루프)
_gem_metrics_lock = threading.Lock()
_gem_outstanding_calls = 0
_gem_inside_semaphore = 0
_gem_ops_reporter_started = False
_reporter_start_lock = threading.Lock()


def get_gemini_gate_metrics() -> dict[str, Any]:
    """telegram 데몬·대시보드용 스냅샷 (락 짧게)."""
    with _gem_metrics_lock:
        return {
            "gemini_gate_inflight": int(_gem_inside_semaphore),
            "gemini_gate_outstanding": int(_gem_outstanding_calls),
            "gemini_gate_sem_limit": 2,
        }


def _gemini_ops_metrics_reporter_loop() -> None:
    while True:
        time.sleep(60.0)
        try:
            import ops_logger

            ops_logger.record_gauge_snapshot("gemini_report_cache", get_gemini_gate_metrics())
            ops_logger.record_heartbeat("gemini_report_cache")
        except Exception:
            pass


def _maybe_start_gemini_ops_reporter() -> None:
    global _gem_ops_reporter_started
    with _reporter_start_lock:
        if _gem_ops_reporter_started:
            return
        _gem_ops_reporter_started = True
    t = threading.Thread(
        target=_gemini_ops_metrics_reporter_loop,
        daemon=True,
        name="gemini_ops_metrics_reporter",
    )
    t.start()


def _gem_gate_thread_main() -> None:
    global _gem_gate_loop, _gem_gate_sem
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _gem_gate_sem = asyncio.Semaphore(2)
    _gem_gate_loop = loop

    def _signal_ready() -> None:
        _gem_gate_ready.set()

    loop.call_soon(_signal_ready)
    try:
        loop.run_forever()
    except Exception:
        pass


def _ensure_gemini_async_api_gate() -> asyncio.AbstractEventLoop:
    """백그라운드 이벤트 루프 1개(멱등)."""
    global _gem_gate_loop
    loop: Optional[asyncio.AbstractEventLoop] = None
    with _gem_gate_lock:
        if _gem_gate_loop is not None:
            loop = _gem_gate_loop
        else:
            t = threading.Thread(
                target=_gem_gate_thread_main,
                daemon=True,
                name="gemini_async_api_gate",
            )
            t.start()
            if not _gem_gate_ready.wait(timeout=15.0):
                raise RuntimeError("gemini async API gate: loop start timeout")
            assert _gem_gate_loop is not None
            loop = _gem_gate_loop
    _maybe_start_gemini_ops_reporter()
    assert loop is not None
    return loop


async def _gemini_guarded_api_call(fn: Callable[[], Any]) -> Any:
    global _gem_inside_semaphore
    async with _gem_gate_sem:  # type: ignore[union-attr]
        with _gem_metrics_lock:
            _gem_inside_semaphore += 1
        try:
            return await asyncio.to_thread(fn)
        finally:
            with _gem_metrics_lock:
                _gem_inside_semaphore -= 1


def run_gemini_network_gated(fn: Callable[[], Any], *, timeout_sec: float = 300.0) -> Any:
    """스캐너 스레드에서 안전 호출: 동시 Google API I/O 최대 2."""
    global _gem_outstanding_calls
    loop = _ensure_gemini_async_api_gate()
    with _gem_metrics_lock:
        _gem_outstanding_calls += 1
    try:
        fut = asyncio.run_coroutine_threadsafe(_gemini_guarded_api_call(fn), loop)
        return fut.result(timeout=timeout_sec)
    finally:
        with _gem_metrics_lock:
            _gem_outstanding_calls -= 1


_AI_CACHE_LOCK = threading.Lock()

_AI_CACHE_DIR = os.path.dirname(os.path.abspath(__file__))
AI_CACHE_DB = os.path.join(_AI_CACHE_DIR, "ai_cache.sqlite")

# 팩토리 캐시 (환경변수 변경 없이 동일 인스턴스 재사용)
_rp: Optional["ReportProvider"] = None
_rp_backend: Optional[str] = None


def _ensure_cache_dir() -> None:
    d = os.path.dirname(AI_CACHE_DB)
    if d:
        os.makedirs(d, exist_ok=True)


def _cache_connection() -> sqlite3.Connection:
    _ensure_cache_dir()
    conn = sqlite3.connect(AI_CACHE_DB, timeout=30.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS report_cache (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            report_text TEXT NOT NULL,
            PRIMARY KEY (date, code)
        )
        """
    )
    return conn


def init_ai_report_cache_db() -> None:
    """선행 초기화용 (스크리너 기동 시 선택 호출)."""
    conn = _cache_connection()
    conn.close()


def load_gemini_api_keys() -> List[str]:
    from llm_gemini_core import load_gemini_api_keys as _load_keys

    return _load_keys()


def is_retryable_gemini_error(exc: BaseException) -> bool:
    from llm_gemini_core import is_retryable_gemini_error as _retry

    return _retry(exc)


def cache_get_payload(date_str: str, cache_key: str) -> Optional[str]:
    with _AI_CACHE_LOCK:
        conn = _cache_connection()
        try:
            row = conn.execute(
                "SELECT report_text FROM report_cache WHERE date = ? AND code = ?",
                (date_str, cache_key),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()


def cache_put_payload(date_str: str, cache_key: str, payload: str) -> None:
    with _AI_CACHE_LOCK:
        conn = _cache_connection()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO report_cache (date, code, report_text)
                VALUES (?, ?, ?)
                """,
                (date_str, cache_key, payload),
            )
            conn.commit()
        finally:
            conn.close()


def cache_delete_row(date_str: str, cache_key: str) -> None:
    """손상된 캐시 행 제거."""
    with _AI_CACHE_LOCK:
        conn = _cache_connection()
        try:
            conn.execute(
                "DELETE FROM report_cache WHERE date = ? AND code = ?",
                (date_str, cache_key),
            )
            conn.commit()
        finally:
            conn.close()


def _decode_tuple_payload(raw: str) -> Optional[Tuple[str, str]]:
    """
    캐시 페이로드는 JSON 배열만 허용. eval / ast.literal_eval 미사용.
    파싱·형식 오류 시 None (호출부에서 API 재조회).
    """
    try:
        data = json.loads(raw)
    except Exception:
        return None
    if not isinstance(data, list):
        return None
    if len(data) >= 2:
        return str(data[0]), str(data[1])
    if len(data) == 1:
        return str(data[0]), ""
    return None


def _try_read_stock_cache(date_str: str, cache_key: str) -> Optional[Tuple[str, str]]:
    """캐시 조회+디코딩 전체를 보호. 실패·손상 시 행 삭제 후 None → API로 진행."""
    try:
        raw = cache_get_payload(date_str, cache_key)
        if raw is None:
            return None
        pair = _decode_tuple_payload(raw)
        if pair is None:
            logger.warning(
                "ai_cache invalid or non-JSON payload (date=%s code=%s); row removed",
                date_str,
                cache_key,
            )
            try:
                cache_delete_row(date_str, cache_key)
            except Exception as de:
                logger.warning("ai_cache delete failed: %s", de)
            return None
        return pair
    except Exception as e:
        logger.warning(
            "ai_cache read failed (date=%s code=%s): %s",
            date_str,
            cache_key,
            e,
        )
        try:
            cache_delete_row(date_str, cache_key)
        except Exception as de:
            logger.warning("ai_cache delete failed: %s", de)
        return None


def _try_read_bitget_cache(date_str: str, cache_key: str) -> Optional[str]:
    try:
        raw = cache_get_payload(date_str, cache_key)
        if raw is None:
            return None
        pair = _decode_tuple_payload(raw)
        if pair is None:
            logger.warning(
                "ai_cache invalid or non-JSON payload (date=%s code=%s); row removed",
                date_str,
                cache_key,
            )
            try:
                cache_delete_row(date_str, cache_key)
            except Exception as de:
                logger.warning("ai_cache delete failed: %s", de)
            return None
        main, _ = pair
        return main
    except Exception as e:
        logger.warning(
            "ai_cache read failed (date=%s code=%s): %s",
            date_str,
            cache_key,
            e,
        )
        try:
            cache_delete_row(date_str, cache_key)
        except Exception as de:
            logger.warning("ai_cache delete failed: %s", de)
        return None


def _sector_block_for_prompt(code: str) -> str:
    try:
        if str(code).isdigit():
            import requests
            from bs4 import BeautifulSoup

            res = requests.get(
                f"https://finance.naver.com/item/main.naver?code={code}",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5,
                verify=False,
            )
            soup = BeautifulSoup(res.text, "html.parser")
            el = soup.select_one("h4.h_sub.sub_tit7 a")
            return el.text.strip() if el else "국내 증시"
        import yfinance as yf

        tk = yf.Ticker(code)
        sector = tk.info.get("sector", "글로벌 산업")
        sector_kr_map = {
            "Technology": "테크/기술",
            "Healthcare": "헬스케어",
            "Financial Services": "금융",
            "Consumer Cyclical": "소비재",
            "Industrials": "산업재",
            "Energy": "에너지",
            "Basic Materials": "원자재",
        }
        return sector_kr_map.get(sector, sector)
    except Exception:
        return "유망 섹터"


@runtime_checkable
class ReportProvider(Protocol):
    def generate(self, report_type: str, **kwargs: Any) -> Any:
        ...


class NoneBackend:
    """초고속 스캔: 리포트 비활성화."""

    def generate(self, report_type: str, **kwargs: Any) -> Any:
        if report_type == "stock":
            return "", ""
        if report_type == "bitget":
            return ""
        raise ValueError(f"unknown report_type: {report_type!r}")


class TemplateBackend:
    """AI 실패·오프라인 폴백용 포맷 템플릿."""

    def generate(self, report_type: str, **kwargs: Any) -> Any:
        if report_type == "stock":
            code = str(kwargs.get("code", "")).strip()
            company_name = str(kwargs.get("company_name", "")).strip()
            sector = _sector_block_for_prompt(code)
            facts = kwargs.get("facts")
            fact_line = str(facts).strip() if facts is not None else "팩트 수치는 스캐너 코어·DB 기준으로 집계 중입니다."
            main = (
                f"1. 섹터: {sector}\n"
                f"2. 실적: {company_name} ({code}) — {fact_line}\n"
                f"3. 모멘텀: 수급·차트 신호는 스캐너 탐지 로직을 참조하세요. (템플릿 모드)"
            )
            return main, ""
        if report_type == "bitget":
            sym = str(kwargs.get("symbol", "")).strip()
            tf = str(kwargs.get("timeframe", "")).strip()
            return (
                f"[본캐 · 템플릿]\n"
                f"1. 섹터/테마: {sym} — 암호화폐 시장군(요약 대기)\n"
                f"2. 내러티브: {tf} 구간 시장 서사 — 스캐너 팩트 연동 대기\n"
                f"3. 모멘텀: 코어 지표 기준으로 판독하세요."
            )
        raise ValueError(f"unknown report_type: {report_type!r}")


class GeminiBackend:
    """기존 캐시 + GenerativeModel 호출 (지연 import)."""

    @staticmethod
    def _configure(genai: Any, attempt: int) -> bool:
        keys = load_gemini_api_keys()
        if not keys:
            return False
        genai.configure(api_key=keys[attempt % len(keys)])
        return True

    def generate(self, report_type: str, **kwargs: Any) -> Any:
        if report_type == "stock":
            return self._generate_stock(str(kwargs["code"]), str(kwargs.get("company_name", "")))
        if report_type == "bitget":
            return self._generate_bitget(str(kwargs["symbol"]), str(kwargs.get("timeframe", "")))
        raise ValueError(f"unknown report_type: {report_type!r}")

    def _generate_stock(self, code: str, company_name: str) -> Tuple[str, str]:
        today = datetime.now().strftime("%Y-%m-%d")
        cache_key = str(code).strip()

        cached_pair = _try_read_stock_cache(today, cache_key)
        if cached_pair is not None:
            return cached_pair

        sector_kr = _sector_block_for_prompt(code)
        fb_main = f"1. 섹터: {sector_kr}\n2. 실적: 데이터 분석 중\n3. 모멘텀: 수급 유입 및 차트 반등 포착"

        prompt = f"""
            너는 주식 전문 마케터야. [{company_name} ({code})] 종목과 관련된 오늘자 최신 이슈나 테마를 검색해서 아래 양식에 맞게 딱 출력해.
            ⚠️ [매우 중요 규칙]
            1. 대괄호 [ ] 로만 정확히 섹션을 구분해. 굵은 글씨(**) 금지.

            [본캐]
            1. 섹터: (어떤 테마인지 한글로 1줄 요약)
            2. 실적: (팩트 수치 한글 1줄 요약)
            3. 모멘텀: (앞으로의 호재 한글 1줄 요약)
            """

        from llm_gemini_core import LlmCallSpec, generate_text_sync

        spec = LlmCallSpec(
            task_id="stock_report",
            user_payload=prompt.strip(),
            model="gemini-2.5-flash",
            cache_key=cache_key,
            timeout_sec=30.0,
        )
        res = generate_text_sync(spec, max_wait_sec=3.0)
        report = (res.text or "").replace("*", "").strip()
        if report and "[본캐]" in report:
            m_part = re.search(r"\[본캐\](.*)", report, re.DOTALL)
            if m_part:
                main = m_part.group(1).strip()
                out: Tuple[str, str] = (main, "")
                cache_put_payload(today, cache_key, json.dumps(list(out), ensure_ascii=False))
                return out
        if report and "[본캐]" not in report and res.ok:
            out = (report, "")
            cache_put_payload(today, cache_key, json.dumps(list(out), ensure_ascii=False))
            return out

        return fb_main, ""

    def _generate_bitget(self, symbol: str, timeframe: str) -> str:
        today = datetime.now().strftime("%Y-%m-%d")
        cache_key = f"{str(symbol).strip()}|{str(timeframe).strip()}"

        cached_main = _try_read_bitget_cache(today, cache_key)
        if cached_main is not None:
            return cached_main

        fb_main = (
            f"[본캐 · 템플릿]\n"
            f"1. 섹터/테마: {symbol} — 암호화폐 시장군(요약 대기)\n"
            f"2. 내러티브: {timeframe} 구간 시장 서사 — 스캐너 팩트 연동 대기\n"
            f"3. 모멘텀: 코어 지표 기준으로 판독하세요."
        )
        prompt = f"""
너는 암호화폐 리서치 마케터다.
[{symbol}] 코인의 최신 정보를 검색해 아래 형식만 출력하라.

[본캐]
1. 섹터/테마: (코인의 산업군/서사 1줄)
2. 내러티브: (현재 시장이 반응하는 재료 1줄)
3. 모멘텀: ({timeframe} 기준 수급/변동성 관점 1줄)
"""
        if not load_gemini_api_keys():
            return fb_main

        from llm_gemini_core import LlmCallSpec, generate_text_sync

        spec = LlmCallSpec(
            task_id="bitget_report",
            user_payload=prompt.strip(),
            model="gemini-2.5-flash",
            cache_key=cache_key,
            timeout_sec=30.0,
        )
        res = generate_text_sync(spec, max_wait_sec=3.0)
        txt = (res.text or "").replace("*", "").strip()
        if txt and "[본캐]" in txt:
            body = txt.split("[본캐]", 1)[1].strip()
            cache_put_payload(today, cache_key, json.dumps([body, ""], ensure_ascii=False))
            return body
        if txt and res.ok:
            cache_put_payload(today, cache_key, json.dumps([txt, ""], ensure_ascii=False))
            return txt

        return fb_main


def get_report_provider() -> ReportProvider:
    """
    REPORT_BACKEND: gemini(기본) | none | template
    (대소문자 무시, 공백 무시)
    """
    global _rp, _rp_backend
    name = (os.environ.get("REPORT_BACKEND") or "gemini").strip().lower()
    if _rp is not None and _rp_backend == name:
        return _rp
    _rp_backend = name
    if name in ("none", "off", "disabled", "0"):
        _rp = NoneBackend()
    elif name in ("template", "fallback", "mock", "stub"):
        _rp = TemplateBackend()
    else:
        _rp = GeminiBackend()
    return _rp


def reset_report_provider_cache() -> None:
    """테스트·런타임 환경 전환 시 팩토리 캐시 무효화."""
    global _rp, _rp_backend
    _rp = None
    _rp_backend = None


def generate_stock_ai_report_cached(code: str, company_name: str) -> Tuple[str, str]:
    """하위 호환: 동일 시그니처."""
    return get_report_provider().generate("stock", code=code, company_name=company_name)


def generate_bitget_ai_report_cached(symbol: str, timeframe: str) -> str:
    """하위 호환: 동일 시그니처."""
    return get_report_provider().generate("bitget", symbol=symbol, timeframe=timeframe)

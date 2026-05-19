"""
LLM Gemini SSOT — KeyPool 로테이션, asyncio 백오프, Sanitizer, Deterministic Fallback.

- 모든 백오프는 asyncio.sleep (메인/스캐너 스레드 time.sleep 금지).
- 동기 호출부는 run_coroutine_threadsafe + 짧은 timeout 또는 fire-and-forget.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional, Tuple

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_LLM_CACHE_LOCK = threading.Lock()
_LLM_CACHE_DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "llm_call_cache.sqlite")

# ---- Prompt leak guard ----
_PROMPT_LEAK_NEEDLES = (
    "다음은 퀀트",
    "너는 주식",
    "너는 암호화폐",
    "출력은 설명문",
    "[facts]",
    "매매 논리를 설명",
    "매우 중요 규칙",
    "google_search",
    "[본캐]",
    "following is a quant",
)
_FORMULA_TOKEN_RE = re.compile(
    r"\b(rolling_std|rolling_mean|ts_rank|ts_max|ts_min|delay|delta|div|add|sub|mul|"
    r"ma|sma|ema|std|corr|rank|max|min|V)\b",
    re.I,
)


class LlmSource(str, Enum):
    LLM = "llm"
    DETERMINISTIC = "deterministic"
    CACHED = "cached"
    DISABLED = "disabled"
    TIMEOUT = "timeout"


@dataclass
class LlmCallSpec:
    task_id: str
    user_payload: str
    system_prompt: str = ""
    model: str = "gemini-2.0-flash"
    timeout_sec: float = 25.0
    cache_key: Optional[str] = None
    use_cache: bool = True
    max_attempts: int = 0


@dataclass
class LlmResult:
    ok: bool
    text: str
    source: LlmSource
    keys_tried: int = 0
    latency_ms: int = 0
    error_class: Optional[str] = None


@dataclass
class _KeySlot:
    key: str
    cooldown_until: float = 0.0
    consecutive_429: int = 0


class GeminiKeyPool:
    def __init__(self, keys: List[str]) -> None:
        self._slots = [_KeySlot(k) for k in keys if k.strip()]
        self._rr = 0
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._slots)

    def acquire(self) -> Optional[_KeySlot]:
        if not self._slots:
            return None
        now = time.monotonic()
        with self._lock:
            n = len(self._slots)
            for i in range(n):
                idx = (self._rr + i) % n
                slot = self._slots[idx]
                if slot.cooldown_until <= now:
                    self._rr = (idx + 1) % n
                    return slot
        return None

    def mark_success(self, slot: _KeySlot) -> None:
        with self._lock:
            slot.consecutive_429 = 0
            slot.cooldown_until = 0.0

    def mark_rate_limited(self, slot: _KeySlot, *, retry_after_sec: Optional[float] = None) -> None:
        with self._lock:
            slot.consecutive_429 += 1
            base = float(retry_after_sec) if retry_after_sec and retry_after_sec > 0 else 60.0
            extra = min(30.0 * slot.consecutive_429, 300.0)
            slot.cooldown_until = time.monotonic() + min(base + extra, 600.0)


def load_gemini_api_keys() -> List[str]:
    load_dotenv()
    raw = os.environ.get("GEMINI_API_KEY") or ""
    return [x.strip() for x in raw.split(",") if x.strip()]


def is_retryable_gemini_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    needles = (
        "429",
        "resource exhausted",
        "resourceexhausted",
        "quota",
        "rate limit",
        "too many requests",
        "503",
        "504",
        "deadline",
        "unavailable",
        "exhausted",
    )
    if any(n in msg for n in needles):
        return True
    name = type(exc).__name__.lower()
    return "resourceexhausted" in name or "aborted" in name or "deadline" in name


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _cache_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_LLM_CACHE_DB, timeout=15.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_cache (
            cache_key TEXT PRIMARY KEY,
            response_text TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    return conn


def _cache_get(key: str) -> Optional[str]:
    with _LLM_CACHE_LOCK:
        conn = _cache_conn()
        try:
            row = conn.execute(
                "SELECT response_text FROM llm_cache WHERE cache_key = ?", (key,)
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()


def _cache_put(key: str, text: str) -> None:
    with _LLM_CACHE_LOCK:
        conn = _cache_conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO llm_cache (cache_key, response_text, created_at) VALUES (?,?,datetime('now'))",
                (key, text),
            )
            conn.commit()
        finally:
            conn.close()


def _make_cache_key(spec: LlmCallSpec) -> str:
    if spec.cache_key:
        return f"{spec.task_id}:{spec.cache_key}"
    h = hashlib.sha256(
        (spec.task_id + "|" + spec.system_prompt + "|" + spec.user_payload).encode("utf-8")
    ).hexdigest()[:32]
    return f"{spec.task_id}:{h}"


def sanitize_user_visible_text(text: str, *, task_id: str = "") -> str:
    """프롬프트·수식 덤프·API 에러 원문 유출 차단."""
    if not text or not str(text).strip():
        return ""
    t = str(text).strip()
    low = t.lower()
    if any(n in low for n in _PROMPT_LEAK_NEEDLES):
        return ""
    if len(_FORMULA_TOKEN_RE.findall(t)) >= 3:
        return ""
    if any(x in t for x in ("429", "RESOURCE_EXHAUSTED", "Too Many Requests", "quota exceeded")):
        return ""
    if "아래는 원본 데이터" in t or "API 한도 초과] 아래" in t:
        return ""
    return t[:500] if task_id == "alpha_explain" else t[:2000]


class AlphaFormulaFallbackParser:
    """수식 → 사람이 읽는 한 줄 (영문 함수명·프롬프트 미포함)."""

    _TAG_RULES: Tuple[Tuple[re.Pattern, str], ...] = (
        (re.compile(r"div\s*\(", re.I), "거래량 대비 가격 위치를 비율로 압축한 수급 비대칭"),
        (re.compile(r"\bV\b"), "거래량·체결 강도"),
        (re.compile(r"rolling_std|ts_std|\bstd\b", re.I), "최근 변동성(표준편차) 구간의 과열·눌림"),
        (re.compile(r"\b(ma|sma|ema)\b", re.I), "이동평균 추세와 단기 요인의 합성"),
        (re.compile(r"ts_rank|\brank\b", re.I), "단면·시계열 순위 기반 상대 강도"),
        (re.compile(r"\bdelay\b|\bdelta\b", re.I), "시차·변화율 모멘텀 전환"),
        (re.compile(r"\bcorr\b", re.I), "상관·공분산 구조"),
        (re.compile(r"\bmax\b|\bmin\b", re.I), "구간 극값·돌파·되돌림"),
        (re.compile(r"\badd\s*\(", re.I), "복수 요인 가중 합성"),
    )

    @classmethod
    def explain(cls, formula: str, ic: float = 0.0) -> str:
        ftxt = str(formula or "")
        tags: List[str] = []
        for pat, phrase in cls._TAG_RULES:
            if pat.search(ftxt) and phrase not in tags:
                tags.append(phrase)
        if not tags:
            core = "가격·거래량·변동성을 결합한 단기 모멘텀·눌림 복합 신호"
        elif len(tags) == 1:
            core = f"{tags[0]}를 활용한 선별 로직"
        else:
            core = f"{tags[0]}와 {tags[1]}를 결합한 선별 로직"
        ic_note = ""
        try:
            if float(ic) >= 0.05:
                ic_note = " 최근 표본에서 미래 수익과 양(+)의 상관이 관측되었습니다."
        except (TypeError, ValueError):
            pass
        suffix = " (규칙 기반 요약 · AI 해석 지연)"
        line = f"{core}.{ic_note}" if ic_note else f"{core}."
        line = line.replace("..", ".")
        if len(line) > 88:
            line = line[:85] + "…"
        return line + suffix


def deterministic_fallback(spec: LlmCallSpec) -> str:
    tid = spec.task_id
    if tid == "alpha_explain":
        m_ic = re.search(r"IC:\s*([-\d.]+)", spec.user_payload)
        m_f = re.search(r"수식:\s*(.+)$", spec.user_payload, re.M)
        ic = float(m_ic.group(1)) if m_ic else 0.0
        formula = m_f.group(1).strip() if m_f else spec.user_payload
        return AlphaFormulaFallbackParser.explain(formula, ic)
    if tid in ("pil_brief", "weekly_action_plan"):
        return "실무·주간 지표 기반 요약(통계 폴백) — LLM 일시 불가."
    if tid == "bitget_overseer":
        return "Bitget 관제 요약은 규칙 감사 본문을 유지합니다 (LLM 일시 불가)."
    if tid in ("stock_report", "bitget_report"):
        return "섹터·실적·모멘텀 요약은 템플릿으로 대체되었습니다 (AI 해석 지연)."
    if tid == "overseer_audit":
        return "규칙 감사 본문은 이미 전송되었습니다 (LLM 해석 지연)."
    return "AI 요약을 규칙 기반 문구로 대체했습니다 (AI 해석 지연)."


_pool: Optional[GeminiKeyPool] = None
_pool_lock = threading.Lock()


def get_key_pool() -> GeminiKeyPool:
    global _pool
    with _pool_lock:
        if _pool is None or len(_pool) == 0:
            _pool = GeminiKeyPool(load_gemini_api_keys())
        return _pool


def _full_prompt(spec: LlmCallSpec) -> str:
    if spec.system_prompt:
        return f"{spec.system_prompt.strip()}\n\n{spec.user_payload.strip()}"
    return spec.user_payload.strip()


async def _call_gemini_once(slot: _KeySlot, spec: LlmCallSpec) -> str:
    """단일 키·단일 시도 — blocking genai는 to_thread."""
    import google.generativeai as genai

    from gemini_report_cache import _gemini_guarded_api_call

    genai.configure(api_key=slot.key)
    prompt = _full_prompt(spec)

    def _sync_call() -> Any:
        gmodel = genai.GenerativeModel(spec.model)
        return gmodel.generate_content(prompt)

    resp = await _gemini_guarded_api_call(_sync_call)
    text = (getattr(resp, "text", None) or "").strip()
    if not text:
        raise RuntimeError("empty_llm_response")
    return text


async def generate_text_async(spec: LlmCallSpec) -> LlmResult:
    t0 = time.monotonic()
    keys = load_gemini_api_keys()
    if not keys:
        txt = deterministic_fallback(spec)
        return LlmResult(
            ok=False,
            text=txt,
            source=LlmSource.DISABLED,
            latency_ms=int((time.monotonic() - t0) * 1000),
            error_class="no_keys",
        )

    ck = _make_cache_key(spec)
    if spec.use_cache:
        cached = _cache_get(ck)
        if cached:
            safe = sanitize_user_visible_text(cached, task_id=spec.task_id)
            if safe:
                return LlmResult(
                    ok=True,
                    text=safe,
                    source=LlmSource.CACHED,
                    latency_ms=int((time.monotonic() - t0) * 1000),
                )

    pool = get_key_pool()
    max_attempts = spec.max_attempts or min(len(pool) * 3 + 2, _env_int("LLM_MAX_ATTEMPTS", 12))
    backoff_base = _env_float("LLM_BACKOFF_BASE_SEC", 5.0)
    keys_tried = 0
    last_err = ""
    global_round = 0

    while global_round < max_attempts:
        slot = pool.acquire()
        if slot is None:
            wait = min(backoff_base * (2**min(global_round, 4)), 60.0)
            wait *= random.uniform(0.85, 1.15)
            logger.warning(
                "LLM all keys cooling; asyncio backoff %.1fs (task=%s round=%s)",
                wait,
                spec.task_id,
                global_round,
            )
            await asyncio.sleep(wait)
            global_round += 1
            continue

        keys_tried += 1
        try:
            raw = await asyncio.wait_for(
                _call_gemini_once(slot, spec),
                timeout=spec.timeout_sec,
            )
            safe = sanitize_user_visible_text(raw, task_id=spec.task_id)
            if not safe:
                last_err = "sanitizer_rejected"
                pool.mark_rate_limited(slot, retry_after_sec=30.0)
                global_round += 1
                continue
            pool.mark_success(slot)
            if spec.use_cache:
                _cache_put(ck, safe)
            return LlmResult(
                ok=True,
                text=safe,
                source=LlmSource.LLM,
                keys_tried=keys_tried,
                latency_ms=int((time.monotonic() - t0) * 1000),
            )
        except asyncio.TimeoutError:
            last_err = "timeout"
            pool.mark_rate_limited(slot, retry_after_sec=45.0)
        except Exception as ex:
            last_err = type(ex).__name__
            if is_retryable_gemini_error(ex):
                pool.mark_rate_limited(slot)
                await asyncio.sleep(random.uniform(0.2, 0.8))
            else:
                logger.info("LLM non-retryable %s: %s", spec.task_id, ex)
                break
        global_round += 1

    txt = deterministic_fallback(spec)
    return LlmResult(
        ok=False,
        text=txt,
        source=LlmSource.DETERMINISTIC,
        keys_tried=keys_tried,
        latency_ms=int((time.monotonic() - t0) * 1000),
        error_class=last_err or "exhausted",
    )


def schedule_coroutine(coro) -> asyncio.Future:
    """스캐너 스레드에서 fire-and-forget (블로킹 없음)."""
    from gemini_report_cache import _ensure_gemini_async_api_gate

    loop = _ensure_gemini_async_api_gate()
    return asyncio.run_coroutine_threadsafe(coro, loop)


def generate_text_sync(spec: LlmCallSpec, *, max_wait_sec: float) -> LlmResult:
    """짧은 대기만 허용 — 초과 시 즉시 deterministic (호출 스레드)."""
    from gemini_report_cache import _ensure_gemini_async_api_gate

    loop = _ensure_gemini_async_api_gate()
    fut = asyncio.run_coroutine_threadsafe(generate_text_async(spec), loop)
    try:
        return fut.result(timeout=max_wait_sec)
    except Exception as ex:
        logger.info("LLM sync timeout/fail task=%s: %s", spec.task_id, ex)
        return LlmResult(
            ok=False,
            text=deterministic_fallback(spec),
            source=LlmSource.TIMEOUT,
            error_class="sync_wait_exceeded",
        )


def safe_generate_content(
    *,
    model: str,
    contents: str,
    max_retries: int = 5,
    task_id: str = "legacy",
    system_prompt: str = "",
    max_wait_sec: Optional[float] = None,
) -> SimpleNamespace:
    """ai_overseer / PIL 호환 래퍼."""
    wait = max_wait_sec
    if wait is None:
        wait = _env_float("LLM_SYNC_WAIT_SEC", 45.0)
    spec = LlmCallSpec(
        task_id=task_id,
        user_payload=contents,
        system_prompt=system_prompt,
        model=model,
        max_attempts=max_retries,
        timeout_sec=_env_float("LLM_CALL_TIMEOUT_SEC", 25.0),
    )
    res = generate_text_sync(spec, max_wait_sec=wait)
    body = res.text if res.text else deterministic_fallback(spec)
    return SimpleNamespace(text=body)


# ---- Telegram edit (알파 진화 enrich) ----

def send_telegram_html_chunks_return_first_id(
    text: str,
    *,
    token: str,
    chat_id: str,
) -> Optional[int]:
    import requests

    if not token or not chat_id:
        return None
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    max_len = 4000
    chunks = [text[i : i + max_len] for i in range(0, len(text), max_len)]
    first_id: Optional[int] = None
    for i, chunk in enumerate(chunks):
        payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}
        try:
            res = requests.post(url, json=payload, timeout=12)
            if res.status_code == 400:
                res = requests.post(
                    url,
                    json={"chat_id": chat_id, "text": chunk},
                    timeout=12,
                )
            if res.ok:
                data = res.json()
                mid = data.get("result", {}).get("message_id")
                if i == 0 and mid is not None:
                    first_id = int(mid)
        except Exception as ex:
            logger.warning("telegram send failed: %s", ex)
    return first_id


def edit_telegram_html_message(
    *,
    token: str,
    chat_id: str,
    message_id: int,
    text: str,
) -> bool:
    import requests

    if not token or not chat_id or not message_id:
        return False
    url = f"https://api.telegram.org/bot{token}/editMessageText"
    chunk = text[:4000]
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": chunk,
        "parse_mode": "HTML",
    }
    try:
        res = requests.post(url, json=payload, timeout=12)
        if res.status_code == 400:
            res = requests.post(
                url,
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": chunk,
                },
                timeout=12,
            )
        if res.ok:
            return True
        logger.warning("editMessageText failed: %s", res.text[:200])
    except Exception as ex:
        logger.warning("editMessageText exception: %s", ex)
    return False


def build_alpha_evolution_telegram_html(
    top3: List[Tuple[str, float]],
    explains: List[str],
    *,
    enriched: bool = False,
) -> str:
    msg = "🧬 <b>[알파 진화 완료]</b>\n"
    if enriched:
        msg += "<i>✨ AI 해석이 반영되었습니다.</i>\n"
    for i, ((formula, ic), explain) in enumerate(zip(top3, explains), 1):
        msg += f"▪️ ALPHA_{i} (IC {ic:.4f}): <code>{formula}</code>\n"
        msg += f"  - AI 해석: {explain}\n"
    return msg


async def enrich_alpha_evolution_message_async(
    *,
    top3: List[Tuple[str, float]],
    token: str,
    chat_id: str,
    message_id: int,
) -> None:
    explains: List[str] = []
    for formula, ic in top3:
        user = f"- IC: {float(ic):.4f}\n- 수식: {formula}\n출력은 설명문 한 줄만."
        sys_p = (
            "다음은 퀀트 알파 수식이다. 사람이 이해하기 쉬운 한국어 한 줄(40자~90자)로 "
            "매매 논리를 설명해라. 과장 금지, 단정적 투자권유 금지."
        )
        spec = LlmCallSpec(
            task_id="alpha_explain",
            user_payload=user,
            system_prompt=sys_p,
            model=os.environ.get("ALPHA_EVOLUTION_LLM_MODEL", "gemini-2.0-flash"),
            cache_key=hashlib.sha256(formula.encode()).hexdigest()[:16],
            timeout_sec=_env_float("LLM_CALL_TIMEOUT_SEC", 25.0),
        )
        res = await generate_text_async(spec)
        line = res.text if res.ok and res.text else AlphaFormulaFallbackParser.explain(formula, ic)
        if res.source == LlmSource.LLM:
            line = line.replace("AI 해석 지연", "AI 해석 완료")
        explains.append(line)

    html_msg = build_alpha_evolution_telegram_html(top3, explains, enriched=True)
    edit_telegram_html_message(
        token=token,
        chat_id=chat_id,
        message_id=message_id,
        text=html_msg,
    )


def schedule_alpha_evolution_enrich(
    *,
    top3: List[Tuple[str, float]],
    token: str,
    chat_id: str,
    message_id: int,
) -> None:
    mode = (os.environ.get("ALPHA_EVOLUTION_LLM_MODE") or "background").strip().lower()
    if mode in ("off", "0", "false", "no"):
        return
    if not message_id:
        return

    async def _job() -> None:
        try:
            await enrich_alpha_evolution_message_async(
                top3=top3,
                token=token,
                chat_id=chat_id,
                message_id=message_id,
            )
        except Exception as ex:
            logger.warning("alpha evolution enrich failed: %s", ex)

    schedule_coroutine(_job())

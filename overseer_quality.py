"""
M-R0-2 / M-R0-3 — Overseer 품질 KPI · degraded audit mode.

- ops_events.sqlite 재사용 (신규 테이블 금지)
- AI_CIO_AUDIT_LOG(D-2)와 분리 — component=overseer_quality
"""
from __future__ import annotations

import html
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

_OVERSEER_QUALITY_COMPONENT = "overseer_quality"
_SESSION_LLM_FAILURES = 0
_SESSION_LOCK = threading.Lock()
_PIPELINE_CRITICAL_FAILURES: List[str] = []
_PIPELINE_MODE: str = ""


def reset_session_llm_failures() -> None:
    global _SESSION_LLM_FAILURES
    with _SESSION_LOCK:
        _SESSION_LLM_FAILURES = 0


def record_session_llm_failure() -> int:
    global _SESSION_LLM_FAILURES
    with _SESSION_LOCK:
        _SESSION_LLM_FAILURES += 1
        return _SESSION_LLM_FAILURES


def get_session_llm_failures() -> int:
    with _SESSION_LOCK:
        return _SESSION_LLM_FAILURES


def begin_pipeline_run(mode: str) -> None:
    global _PIPELINE_CRITICAL_FAILURES, _PIPELINE_MODE
    _PIPELINE_MODE = str(mode or "")
    _PIPELINE_CRITICAL_FAILURES = []


def record_pipeline_critical_failure(step_name: str) -> None:
    name = (step_name or "").strip()
    if name and name not in _PIPELINE_CRITICAL_FAILURES:
        _PIPELINE_CRITICAL_FAILURES.append(name)


def get_pipeline_critical_failures() -> List[str]:
    return list(_PIPELINE_CRITICAL_FAILURES)


def _llm_failure_threshold(sys_config: Optional[Mapping[str, Any]]) -> int:
    cfg = sys_config or {}
    raw = cfg.get("OVERSEER_LLM_FAILURE_THRESHOLD")
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return 3


def _quality_window_days(sys_config: Optional[Mapping[str, Any]]) -> int:
    cfg = sys_config or {}
    raw = cfg.get("OVERSEER_QUALITY_WINDOW_DAYS")
    try:
        return max(1, min(30, int(raw)))
    except (TypeError, ValueError):
        return 7


def _insert_quality_event(event: str, payload: Optional[Dict[str, Any]] = None) -> None:
    try:
        import ops_logger

        ops_logger.insert_ops_event(
            component=_OVERSEER_QUALITY_COMPONENT,
            severity="INFO",
            event=event,
            payload=payload or {},
        )
    except Exception:
        pass


def record_overseer_llm_call(*, task: str = "overseer_audit", provider: str = "gemini") -> None:
    _insert_quality_event("overseer_llm_call", {"task": task, "provider": provider})


def record_overseer_deterministic_fallback(*, task: str = "overseer_audit", reason: str = "") -> None:
    _insert_quality_event(
        "overseer_deterministic_fallback",
        {"task": task, "reason": (reason or "")[:200]},
    )


def record_overseer_anomaly(*, kind: str, detail: str = "") -> None:
    _insert_quality_event("overseer_anomaly", {"kind": kind, "detail": (detail or "")[:300]})


def record_narrative_outcome(
    *,
    source: str,
    violations: Sequence[str] = (),
    task: str = "overseer_audit",
    provider: str = "gemini",
) -> None:
    src = (source or "").strip().lower()
    if src == "llm":
        record_overseer_llm_call(task=task, provider=provider)
    else:
        reason = src
        if violations:
            reason = f"{src}:{','.join(violations[:3])}"
        record_overseer_deterministic_fallback(task=task, reason=reason)
    if violations:
        record_overseer_anomaly(kind="narrative_violation", detail=",".join(violations[:5]))


def resolve_overseer_audit_mode(
    sys_config: Optional[Mapping[str, Any]] = None,
    meta: Optional[Mapping[str, Any]] = None,
) -> Tuple[str, str]:
    cfg = sys_config or {}
    meta = meta or {}

    if str(cfg.get("OVERSEER_FORCE_DEGRADED", "")).strip().lower() in ("1", "true", "yes", "on"):
        return "degraded_rules_only", "director_forced"

    try:
        from meta_state_store import is_meta_state_degraded

        if is_meta_state_degraded(meta):
            return "degraded_rules_only", "meta_degraded"
    except Exception:
        pass

    crit = get_pipeline_critical_failures()
    if crit:
        return "degraded_rules_only", f"pipeline_critical:{crit[0]}"

    if get_session_llm_failures() >= _llm_failure_threshold(cfg):
        return "degraded_rules_only", "llm_consecutive_failures"

    return "full", ""


def format_degraded_audit_banner(reason: str) -> str:
    r = html.escape((reason or "unknown").replace("_", " "), quote=False)
    return f"⚠️ <b>규칙 기반 감사</b> (LLM 비활성 — 사유: <code>{r}</code>)\n"


def _since_utc_iso(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def fetch_quality_stats(*, window_days: int = 7) -> Dict[str, Any]:
    try:
        import ops_logger

        db_path = ops_logger.OPS_EVENTS_DB_PATH
    except Exception:
        return {"llm": 0, "deterministic": 0, "anomaly": 0, "total": 0}

    if not os.path.isfile(db_path):
        return {"llm": 0, "deterministic": 0, "anomaly": 0, "total": 0}

    since = _since_utc_iso(window_days)
    llm = det = anomaly = 0
    try:
        uri = f"file:{db_path.replace(os.sep, '/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=10.0)
        cur = conn.execute(
            """
            SELECT event, COUNT(*) FROM ops_events
            WHERE component = ? AND ts_utc >= ?
              AND event IN ('overseer_llm_call', 'overseer_deterministic_fallback', 'overseer_anomaly')
            GROUP BY event
            """,
            (_OVERSEER_QUALITY_COMPONENT, since),
        )
        for ev, cnt in cur.fetchall():
            if ev == "overseer_llm_call":
                llm = int(cnt)
            elif ev == "overseer_deterministic_fallback":
                det = int(cnt)
            elif ev == "overseer_anomaly":
                anomaly = int(cnt)
        conn.close()
    except Exception:
        return {"llm": 0, "deterministic": 0, "anomaly": 0, "total": 0}

    total = llm + det
    return {"llm": llm, "deterministic": det, "anomaly": anomaly, "total": total}


def build_overseer_quality_footer(sys_config: Optional[Mapping[str, Any]] = None) -> str:
    window = _quality_window_days(sys_config)
    stats = fetch_quality_stats(window_days=window)
    total = stats["total"]
    if total <= 0:
        llm_pct = det_pct = 0
    else:
        llm_pct = round(100.0 * stats["llm"] / total)
        det_pct = 100 - llm_pct
    line = (
        f"──────────\n"
        f"🔎 Overseer 품질: LLM {llm_pct}% · 규칙 {det_pct}% ({window}d) · "
        f"anomaly {stats['anomaly']}건"
    )
    try:
        from llm_gemini_core import sanitize_user_visible_text

        safe = sanitize_user_visible_text(line, task_id="overseer_quality_audit")
        return safe or line[:200]
    except Exception:
        return line[:200]


def format_overseer_quality_footer_html(sys_config: Optional[Mapping[str, Any]] = None) -> str:
    body = build_overseer_quality_footer(sys_config)
    if not body:
        return ""
    return f"<i>{html.escape(body, quote=False)}</i>\n"

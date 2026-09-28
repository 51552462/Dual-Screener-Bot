"""A5-EVENTLOG-01 — emit A-1~A-5 ops_events without changing gate results."""
from __future__ import annotations

import os
from typing import Any, Optional


def a1a5_event_log_enabled() -> bool:
    env = os.environ.get("A1A5_EVENT_LOG_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("A1A5_EVENT_LOG_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import A1A5_EVENT_LOG_ENABLED

    return bool(A1A5_EVENT_LOG_ENABLED)


def emit_a1a5_event(
    event: str,
    payload: Optional[dict[str, Any]] = None,
    *,
    component: str,
    severity: str = "WARNING",
) -> None:
    if not a1a5_event_log_enabled():
        return
    try:
        from bitget.infra.ops_logger import insert_ops_event

        insert_ops_event(
            component=component,
            severity=severity,
            event=event,
            payload=payload if isinstance(payload, dict) else {},
        )
    except Exception:
        pass

"""Append-only structured audit log (does not replace 05_진행로그)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from dev_autonomy.paths import AUDIT_LOG_PATH
from dev_autonomy.types import RoundRecord


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_audit(record: Dict[str, Any], path: Path = AUDIT_LOG_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe = {k: v for k, v in record.items() if k not in ("token", "api_key", "secret")}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(safe, ensure_ascii=False) + "\n")


def log_round(round_record: RoundRecord, extra: Dict[str, Any] | None = None) -> None:
    payload: Dict[str, Any] = {
        "round_id": round_record.round_id,
        "timestamp": round_record.timestamp or _utc_now(),
        "track": round_record.track,
        "subphase": round_record.subphase,
        "actor": round_record.actor,
        "action": round_record.action,
        "source_status": round_record.source_status,
        "files_touched": round_record.files_touched,
        "validation_commands": round_record.validation_commands,
        "exit_codes": round_record.exit_codes,
        "test_summary": round_record.test_summary,
        "safety_decision": round_record.safety_decision,
        "claude_verdict": round_record.claude_verdict,
        "retry_count": round_record.retry_count,
        "final_status": round_record.final_status,
    }
    if extra:
        payload.update(extra)
    append_audit(payload)

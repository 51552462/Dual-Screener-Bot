"""
D-1 — structured LLM proposal validate + persist (read-only w.r.t. config_kv).

Parses overseer LLM JSON blocks, recomputes risk_class from CAT-MAP §6, logs to
``bitget_llm_proposals``. Invalid proposals are never persisted.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from bitget.infra.clock import utc_now_iso

logger = logging.getLogger(__name__)

_PROPOSALS_TABLE = "bitget_llm_proposals"
_PARSE_ERROR_EVENT = "llm_proposal_parse_error"
_PARSE_ERROR_COMPONENT = "governance.ai_proposal"

# CAT-MAP §6 — category letter → server risk_class (LLM value ignored).
_CAT_RISK_CLASS: Dict[str, str] = {
    "F": "critical",
    "G": "critical",
    "N": "critical",
    "B": "critical",
    "D": "critical",
    "E": "high",
    "I": "high",
    "K": "high",
    "A": "medium",
    "C": "medium",
    "H": "medium",
    "P": "medium",
    "L": "medium",
    "J": "low",
    "M": "low",
    "O": "low",
    "Q": "low",
}

_PROPOSAL_DDL = f"""
CREATE TABLE IF NOT EXISTS {_PROPOSALS_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    category TEXT NOT NULL,
    risk_class TEXT NOT NULL,
    rationale TEXT NOT NULL DEFAULT '',
    params_json TEXT NOT NULL DEFAULT '{{}}',
    source_text_hash TEXT,
    payload_json TEXT NOT NULL DEFAULT '{{}}'
);
CREATE INDEX IF NOT EXISTS idx_llm_proposals_recorded
    ON {_PROPOSALS_TABLE}(recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_proposals_category
    ON {_PROPOSALS_TABLE}(category, recorded_at DESC);
"""


@dataclass(frozen=True)
class ProposalResult:
    proposal: Dict[str, Any]


@dataclass(frozen=True)
class ProposalError:
    message: str
    code: str = "parse_error"
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def silent_skip(self) -> bool:
        return self.code == "no_block"


def ai_proposal_structured_enabled() -> bool:
    env = os.environ.get("AI_PROPOSAL_STRUCTURED_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("AI_PROPOSAL_STRUCTURED_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import AI_PROPOSAL_STRUCTURED_ENABLED

    return bool(AI_PROPOSAL_STRUCTURED_ENABLED)


def normalize_proposal_category(raw: str) -> Optional[str]:
    text = str(raw or "").strip().upper()
    if not text:
        return None
    if text.startswith("CAT-"):
        text = text[4:]
    if text.startswith("CAT"):
        text = text[3:].lstrip("-_")
    letter = text.split("-", 1)[0].strip()
    if len(letter) == 1 and letter.isalpha():
        return letter
    return None


def risk_class_for_category(category: str) -> Optional[str]:
    letter = normalize_proposal_category(category)
    if not letter:
        return None
    return _CAT_RISK_CLASS.get(letter)


def _extract_json_blob(raw_text: str) -> Optional[str]:
    text = str(raw_text or "").strip()
    if not text:
        return None
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text, flags=re.IGNORECASE)
    if fence:
        candidate = fence.group(1).strip()
        return candidate or None
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return text[start : end + 1]
    return None


def validate_llm_proposal(raw_text: str) -> Union[ProposalResult, ProposalError]:
    blob = _extract_json_blob(raw_text)
    if blob is None:
        return ProposalError("no structured proposal JSON block", code="no_block")

    try:
        data = json.loads(blob)
    except json.JSONDecodeError as exc:
        return ProposalError(f"json decode failed: {exc}", code="parse_error")

    if not isinstance(data, dict):
        return ProposalError("proposal root must be a JSON object", code="parse_error")

    category_raw = data.get("category")
    if not isinstance(category_raw, str) or not str(category_raw).strip():
        return ProposalError("missing or invalid category", code="parse_error")

    category_letter = normalize_proposal_category(category_raw)
    if not category_letter:
        return ProposalError(
            f"unknown category: {category_raw!r}",
            code="parse_error",
            details={"category": category_raw},
        )

    risk_class = risk_class_for_category(category_letter)
    if not risk_class:
        return ProposalError(
            f"no CAT-MAP risk mapping for category: {category_raw!r}",
            code="parse_error",
            details={"category": category_raw},
        )

    params = data.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        return ProposalError("params must be a JSON object", code="parse_error")

    rationale = data.get("rationale", "")
    if not isinstance(rationale, str):
        return ProposalError("rationale must be a string", code="parse_error")
    rationale = rationale.strip()
    if not rationale:
        return ProposalError("rationale must be non-empty", code="parse_error")

    normalized = {
        "category": f"CAT-{category_letter}",
        "params": dict(params),
        "rationale": rationale,
        "risk_class": risk_class,
        "llm_risk_class_ignored": data.get("risk_class"),
    }
    return ProposalResult(proposal=normalized)


def _default_db_path() -> str:
    from bitget.infra.data_paths import market_data_db_path

    return market_data_db_path()


def ensure_llm_proposals_schema(db_path: Optional[str] = None) -> None:
    path = db_path or _default_db_path()
    if not path:
        return
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.executescript(_PROPOSAL_DDL)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("llm proposals schema skip: %s", ex)


def persist_proposal_bg(
    proposal: Dict[str, Any],
    *,
    db_path: Optional[str] = None,
    source_text: Optional[str] = None,
) -> None:
    path = db_path or _default_db_path()
    ensure_llm_proposals_schema(path)
    recorded_at = utc_now_iso()
    category = str(proposal.get("category") or "CAT-UNKNOWN")
    risk_class = str(proposal.get("risk_class") or "unknown")
    rationale = str(proposal.get("rationale") or "")
    params_json = json.dumps(proposal.get("params") or {}, ensure_ascii=False, sort_keys=True)
    payload_json = json.dumps(proposal, ensure_ascii=False, sort_keys=True)
    source_hash = None
    if source_text:
        source_hash = hashlib.sha256(source_text.encode("utf-8")).hexdigest()[:16]

    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.execute(
            f"""
            INSERT INTO {_PROPOSALS_TABLE} (
                recorded_at, category, risk_class, rationale,
                params_json, source_text_hash, payload_json
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                recorded_at,
                category,
                risk_class,
                rationale,
                params_json,
                source_hash,
                payload_json,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def record_proposal_parse_error(error: ProposalError, *, raw_excerpt: str = "") -> bool:
    from bitget.infra.ops_logger import insert_ops_event

    payload: Dict[str, Any] = {
        "code": error.code,
        "message": error.message,
    }
    if error.details:
        payload["details"] = dict(error.details)
    if raw_excerpt:
        payload["raw_excerpt"] = str(raw_excerpt)[:500]
    return bool(
        insert_ops_event(
            component=_PARSE_ERROR_COMPONENT,
            severity="WARN",
            event=_PARSE_ERROR_EVENT,
            payload=payload,
        )
    )


def process_structured_llm_proposal(
    raw_text: str,
    *,
    db_path: Optional[str] = None,
    telegram_alert=None,
) -> Optional[ProposalResult]:
    """
    Validate + persist when enabled. Parse errors → ops_events + optional telegram.
    Returns ProposalResult on success, None otherwise.
    """
    if not ai_proposal_structured_enabled():
        return None

    outcome = validate_llm_proposal(raw_text)
    if isinstance(outcome, ProposalError):
        if outcome.silent_skip:
            return None
        record_proposal_parse_error(outcome, raw_excerpt=raw_text)
        if telegram_alert is not None:
            try:
                telegram_alert(
                    f"⚠️ [LLM Proposal Parse Error] {outcome.message}"
                )
            except Exception:
                pass
        return None

    persist_proposal_bg(
        outcome.proposal,
        db_path=db_path,
        source_text=raw_text,
    )
    return outcome

"""
D-2 — LLM proposal human approval gate (telegram + append-only approvals log).

Reads ``bitget_llm_proposals`` (D-1). Writes ``bitget_llm_proposal_approvals`` only.
Approved proposals apply params via ``config_manager.set_config_value`` (CAT-K SSOT).
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from bitget.infra.clock import utc_now_iso

logger = logging.getLogger(__name__)

_PROPOSALS_TABLE = "bitget_llm_proposals"
_APPROVALS_TABLE = "bitget_llm_proposal_approvals"
_EVENT_APPROVE = "approve"
_EVENT_REJECT = "reject"
_CMD_APPROVE = "/proposal_approve"
_CMD_REJECT = "/proposal_reject"
_HIGH_RISK_CLASSES = frozenset({"critical", "high"})

_APPROVALS_DDL = f"""
CREATE TABLE IF NOT EXISTS {_APPROVALS_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    proposal_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    telegram_user_id TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{{}}'
);
CREATE INDEX IF NOT EXISTS idx_llm_proposal_approvals_proposal
    ON {_APPROVALS_TABLE}(proposal_id, id DESC);
"""


def proposal_approval_gate_enabled() -> bool:
    env = os.environ.get("AI_PROPOSAL_APPROVAL_GATE_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("AI_PROPOSAL_APPROVAL_GATE_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import AI_PROPOSAL_APPROVAL_GATE_ENABLED

    return bool(AI_PROPOSAL_APPROVAL_GATE_ENABLED)


def _market_db_path() -> str:
    from bitget.infra.data_paths import market_data_db_path

    return market_data_db_path()


def ensure_proposal_approvals_schema(db_path: Optional[str] = None) -> None:
    path = db_path or _market_db_path()
    if not path:
        return
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.executescript(_APPROVALS_DDL)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("proposal approvals schema skip: %s", ex)


def proposal_public_ref(proposal_id: int) -> str:
    return f"{int(proposal_id):08x}"


def risk_class_badge(risk_class: str) -> str:
    rc = str(risk_class or "unknown").lower()
    if rc == "critical":
        return "🔴 CRITICAL"
    if rc == "high":
        return "🟠 HIGH"
    if rc == "medium":
        return "🟡 MEDIUM"
    if rc == "low":
        return "🟢 LOW"
    return f"⚪ {rc.upper()}"


def allowed_report_bot_chat_ids() -> set[str]:
    ids: set[str] = set()
    for key in (
        "REPORT_BOT_CHAT_ID",
        "REPORT_BOT_CHAT_IDS",
        "REPORT_BOT_WHITELIST_CHAT_IDS",
    ):
        raw = os.environ.get(key, "")
        if not str(raw).strip():
            continue
        for part in str(raw).split(","):
            p = part.strip()
            if p:
                ids.add(p)
    if ids:
        return ids
    try:
        import telegram_env

        cid = telegram_env.get_report_chat_id()
        if cid:
            ids.add(str(cid).strip())
    except Exception:
        pass
    return ids


def is_authorized_report_bot_chat(chat_id: str) -> bool:
    allowed = allowed_report_bot_chat_ids()
    if not allowed:
        return False
    return str(chat_id).strip() in allowed


def _load_proposal_row(
    proposal_id: int,
    *,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    path = db_path or _market_db_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            row = conn.execute(
                f"""
                SELECT id, recorded_at, category, risk_class, rationale, params_json, payload_json
                FROM {_PROPOSALS_TABLE}
                WHERE id = ?
                """,
                (int(proposal_id),),
            ).fetchone()
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return None
    if not row:
        return None
    try:
        params = json.loads(row[5] or "{}")
    except json.JSONDecodeError:
        params = {}
    if not isinstance(params, dict):
        params = {}
    return {
        "id": int(row[0]),
        "recorded_at": row[1],
        "category": row[2],
        "risk_class": str(row[3] or "unknown"),
        "rationale": row[4],
        "params": dict(params),
        "public_ref": proposal_public_ref(int(row[0])),
    }


def _list_proposal_rows(*, db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    path = db_path or _market_db_path()
    if not path or not os.path.isfile(path):
        return []
    rows: List[Dict[str, Any]] = []
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (_PROPOSALS_TABLE,),
            ).fetchone()
            if not table:
                return []
            cur = conn.execute(
                f"""
                SELECT id, recorded_at, category, risk_class, rationale, params_json
                FROM {_PROPOSALS_TABLE}
                ORDER BY id DESC
                """
            )
            for pid, recorded_at, category, risk_class, rationale, params_json in cur.fetchall():
                try:
                    params = json.loads(params_json or "{}")
                except json.JSONDecodeError:
                    params = {}
                if not isinstance(params, dict):
                    params = {}
                rows.append(
                    {
                        "id": int(pid),
                        "recorded_at": recorded_at,
                        "category": category,
                        "risk_class": str(risk_class or "unknown"),
                        "rationale": rationale,
                        "params": dict(params),
                        "public_ref": proposal_public_ref(int(pid)),
                    }
                )
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return []
    return rows


def _token_matches_proposal(token: str, proposal: Dict[str, Any]) -> bool:
    tid = str(token or "").strip().lower()
    if not tid:
        return False
    pid = int(proposal["id"])
    ref = str(proposal.get("public_ref") or proposal_public_ref(pid)).lower()
    risk = str(proposal.get("risk_class") or "").lower()
    if risk in _HIGH_RISK_CLASSES:
        return tid == ref or tid == str(pid)
    if tid == ref or tid == str(pid):
        return True
    return ref.startswith(tid) or str(pid).startswith(tid)


def resolve_proposal_by_token(
    token: str,
    *,
    db_path: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    matches = [p for p in _list_proposal_rows(db_path=db_path) if _token_matches_proposal(token, p)]
    if not matches:
        return None, "not_found"
    if len(matches) > 1:
        return None, "ambiguous"
    return matches[0], None


def get_proposal_status(
    proposal_id: int,
    *,
    db_path: Optional[str] = None,
) -> str:
    """Derived status: pending | approved | rejected (not a DB column)."""
    path = db_path or _market_db_path()
    ensure_proposal_approvals_schema(path)
    if not path or not os.path.isfile(path):
        return "pending"
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            row = conn.execute(
                f"""
                SELECT event_type
                FROM {_APPROVALS_TABLE}
                WHERE proposal_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(proposal_id),),
            ).fetchone()
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return "pending"
    if not row:
        return "pending"
    ev = str(row[0] or "").lower()
    if ev == _EVENT_APPROVE:
        return "approved"
    if ev == _EVENT_REJECT:
        return "rejected"
    return "pending"


def record_approval_decision(
    proposal_id: int,
    event_type: str,
    telegram_user_id: str,
    *,
    db_path: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """Append-only INSERT into bitget_llm_proposal_approvals."""
    path = db_path or _market_db_path()
    ensure_proposal_approvals_schema(path)
    ev = str(event_type or "").strip().lower()
    if ev not in (_EVENT_APPROVE, _EVENT_REJECT):
        raise ValueError(f"invalid event_type: {event_type!r}")
    body = dict(payload or {})
    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.execute(
            f"""
            INSERT INTO {_APPROVALS_TABLE} (
                recorded_at, proposal_id, event_type, telegram_user_id, payload_json
            ) VALUES (?,?,?,?,?)
            """,
            (
                utc_now_iso(),
                int(proposal_id),
                ev,
                str(telegram_user_id or ""),
                json.dumps(body, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def apply_approved_proposal(proposal: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply proposal params via config_manager.set_config_value (A-5 bounds in CM).
    Returns per-key results — partial success when some keys are rejected/clamped.
    """
    from bitget.infra import config_manager as cm
    from bitget.infra.config_bounds import CONFIG_WRITE_REJECT_BOUNDS

    params = proposal.get("params") or {}
    if not isinstance(params, dict):
        params = {}
    results: Dict[str, Any] = {}
    for key, intended in params.items():
        k = str(key)
        try:
            before = cm.get_config_value(k)
            cm.set_config_value(k, intended)
            after = cm.get_config_value(k)
            if k in CONFIG_WRITE_REJECT_BOUNDS:
                try:
                    intended_cmp = float(intended)
                    after_cmp = float(after) if after is not None else None
                except (TypeError, ValueError):
                    intended_cmp = intended
                    after_cmp = after
                if after_cmp != intended_cmp:
                    results[k] = {
                        "status": "rejected",
                        "reason": "a5_bounds",
                        "before": before,
                        "after": after,
                        "intended": intended,
                    }
                    continue
            results[k] = {
                "status": "applied",
                "before": before,
                "after": after,
                "intended": intended,
            }
        except Exception as ex:
            results[k] = {
                "status": "error",
                "reason": str(ex),
                "intended": intended,
            }
    return {
        "proposal_id": proposal.get("id"),
        "keys": results,
    }


def _parse_command(text: str) -> Tuple[Optional[str], Optional[str]]:
    raw = str(text or "").strip()
    if not raw:
        return None, None
    parts = raw.split(maxsplit=1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    if cmd == _CMD_APPROVE:
        return _EVENT_APPROVE, arg
    if cmd == _CMD_REJECT:
        return _EVENT_REJECT, arg
    return None, None


def process_proposal_telegram_command(
    text: str,
    *,
    chat_id: str,
    telegram_user_id: str,
    market_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Handle /proposal_approve|reject <short_id>.
    Unauthorized chat or disabled gate → no-op (no config / no approval rows).
    """
    if not proposal_approval_gate_enabled():
        return {"ok": False, "reason": "disabled", "message": "approval gate disabled"}

    if not is_authorized_report_bot_chat(chat_id):
        return {"ok": False, "reason": "unauthorized", "message": "chat not whitelisted"}

    event_type, token = _parse_command(text)
    if not event_type or not token:
        return {"ok": False, "reason": "bad_command", "message": "usage: /proposal_approve|reject <id>"}

    proposal, match_err = resolve_proposal_by_token(token, db_path=market_db_path)
    if not proposal:
        msg = "proposal not found"
        if match_err == "ambiguous":
            msg = "ambiguous proposal id — use full id for high/critical"
        return {"ok": False, "reason": match_err or "not_found", "message": msg}

    pid = int(proposal["id"])
    status = get_proposal_status(pid, db_path=market_db_path)
    if status in ("approved", "rejected"):
        return {
            "ok": False,
            "reason": "duplicate_ignored",
            "message": f"proposal already {status}",
            "proposal_id": pid,
            "status": status,
        }

    badge = risk_class_badge(proposal.get("risk_class", ""))
    if event_type == _EVENT_REJECT:
        record_approval_decision(
            pid,
            _EVENT_REJECT,
            telegram_user_id,
            db_path=market_db_path,
            payload={"chat_id": str(chat_id)},
        )
        return {
            "ok": True,
            "action": "reject",
            "proposal_id": pid,
            "message": f"{badge} proposal {proposal['public_ref']} rejected",
        }

    record_approval_decision(
        pid,
        _EVENT_APPROVE,
        telegram_user_id,
        db_path=market_db_path,
        payload={"chat_id": str(chat_id)},
    )
    apply_result = apply_approved_proposal(proposal)
    return {
        "ok": True,
        "action": "approve",
        "proposal_id": pid,
        "message": f"{badge} proposal {proposal['public_ref']} approved",
        "apply": apply_result,
    }


def try_handle_telegram_update(
    update: Dict[str, Any],
    *,
    market_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Optional entry for REPORT_BOT getUpdates polling."""
    msg = update.get("message") or update.get("edited_message")
    if not isinstance(msg, dict):
        return None
    text = msg.get("text") or ""
    if not str(text).strip().startswith("/proposal_"):
        return None
    chat = msg.get("chat") or {}
    user = msg.get("from") or {}
    return process_proposal_telegram_command(
        str(text),
        chat_id=str(chat.get("id", "")),
        telegram_user_id=str(user.get("id", "")),
        market_db_path=market_db_path,
    )

"""
D-2 poll — REPORT_BOT getUpdates → proposal approve/reject commands.

Wiring only: no change to D-2 approval logic (`proposal_approval_bg.py`).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

_STATE_FILENAME = "proposal_approval_poll_state.json"
_TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


def proposal_approval_poll_enabled() -> bool:
    env = os.environ.get("AI_PROPOSAL_APPROVAL_POLL_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("AI_PROPOSAL_APPROVAL_POLL_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import AI_PROPOSAL_APPROVAL_POLL_ENABLED

    return bool(AI_PROPOSAL_APPROVAL_POLL_ENABLED)


def _state_path() -> str:
    from bitget.infra.data_paths import bitget_data_dir

    return os.path.join(bitget_data_dir(), _STATE_FILENAME)


def load_poll_offset(*, state_path: Optional[str] = None) -> int:
    path = state_path or _state_path()
    if not path or not os.path.isfile(path):
        return 0
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return int(data.get("last_update_id") or 0)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return 0


def save_poll_offset(update_id: int, *, state_path: Optional[str] = None) -> None:
    path = state_path or _state_path()
    if not path:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"last_update_id": int(update_id)}, f)


def get_report_bot_token() -> str:
    try:
        import telegram_env

        return str(telegram_env.get_report_token() or "").strip()
    except Exception:
        return ""


def send_report_bot_message(chat_id: str, text: str, *, token: Optional[str] = None) -> bool:
    tok = token or get_report_bot_token()
    if not tok or not chat_id or not str(text).strip():
        return False
    try:
        res = requests.post(
            _TELEGRAM_API.format(token=tok, method="sendMessage"),
            json={"chat_id": str(chat_id), "text": str(text)[:4000]},
            timeout=15,
        )
        return bool(res.ok)
    except Exception as ex:
        logger.warning("proposal poll sendMessage failed: %s", ex)
        return False


def _should_reply_to_result(result: Dict[str, Any]) -> bool:
    reason = str(result.get("reason") or "")
    if reason in ("unauthorized", "disabled"):
        return False
    return bool(str(result.get("message") or "").strip())


def _chat_id_from_update(update: Dict[str, Any]) -> str:
    msg = update.get("message") or update.get("edited_message") or {}
    chat = msg.get("chat") or {}
    return str(chat.get("id", ""))


def fetch_report_bot_updates(
    *,
    offset: int = 0,
    timeout: int = 0,
    token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    tok = token or get_report_bot_token()
    if not tok:
        return []
    params: Dict[str, Any] = {"timeout": int(timeout)}
    if offset > 0:
        params["offset"] = int(offset) + 1
    try:
        res = requests.get(
            _TELEGRAM_API.format(token=tok, method="getUpdates"),
            params=params,
            timeout=max(15, int(timeout) + 5),
        )
        payload = res.json()
    except Exception as ex:
        logger.warning("proposal poll getUpdates failed: %s", ex)
        return []
    if not isinstance(payload, dict) or not payload.get("ok"):
        return []
    result = payload.get("result")
    return list(result) if isinstance(result, list) else []


def poll_proposal_approval_updates_once(
    *,
    timeout: int = 0,
    market_db_path: Optional[str] = None,
    state_path: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    One getUpdates pass. Returns summary dict with last_update_id and handled commands.
    """
    from bitget.governance.proposal_approval_bg import (
        proposal_approval_gate_enabled,
        try_handle_telegram_update,
    )

    summary: Dict[str, Any] = {
        "polled": False,
        "last_update_id": load_poll_offset(state_path=state_path),
        "handled": [],
    }
    if not proposal_approval_poll_enabled():
        return summary
    if not proposal_approval_gate_enabled():
        return summary
    tok = token or get_report_bot_token()
    if not tok:
        return summary

    offset = int(summary["last_update_id"])
    updates = fetch_report_bot_updates(offset=offset, timeout=timeout, token=tok)
    summary["polled"] = True

    for update in updates:
        if not isinstance(update, dict):
            continue
        uid = int(update.get("update_id") or 0)
        if uid > offset:
            offset = uid
            save_poll_offset(offset, state_path=state_path)
            summary["last_update_id"] = offset

        result = try_handle_telegram_update(update, market_db_path=market_db_path)
        if result is None:
            continue
        summary["handled"].append(result)
        if _should_reply_to_result(result):
            send_report_bot_message(
                _chat_id_from_update(update),
                str(result.get("message") or ""),
                token=tok,
            )

    return summary


def run_proposal_approval_poll_job(
    *,
    timeout: int = 0,
    market_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Hook entry — overseer loop or manual invoke."""
    if not proposal_approval_poll_enabled():
        return None
    return poll_proposal_approval_updates_once(
        timeout=timeout,
        market_db_path=market_db_path,
    )

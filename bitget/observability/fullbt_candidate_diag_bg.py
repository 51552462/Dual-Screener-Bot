"""
FULL-BT-FUT-DIAG-1 — candidate→trade reject reason tag (read-only).

After ``try_add_virtual_position(...)`` returns, tag existing reject fields into
``ops_events``. Does not invent reject codes; does not touch CAT-D/N internals.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional, Tuple, Union

logger = logging.getLogger(__name__)

_EVENT = "fullbt_candidate_reject"
_COMPONENT = "observability.fullbt_candidate_diag"

TryAddResult = Union[Tuple[Any, Any], list, dict, Any]


def fullbt_candidate_diag_enabled() -> bool:
    env = os.environ.get("FULLBT_CANDIDATE_DIAG_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("FULLBT_CANDIDATE_DIAG_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import FULLBT_CANDIDATE_DIAG_ENABLED

    return bool(FULLBT_CANDIDATE_DIAG_ENABLED)


def _parse_try_add_result(try_add_result: TryAddResult) -> Optional[Tuple[bool, str]]:
    """Extract (ok, msg) from existing try_add return — no invented fields."""
    if try_add_result is None:
        return None
    if isinstance(try_add_result, dict):
        if "ok" not in try_add_result:
            return None
        ok = bool(try_add_result.get("ok"))
        # Prefer known ledger field names only (msg / message); do not invent codes.
        if "msg" in try_add_result:
            msg = try_add_result.get("msg")
        elif "message" in try_add_result:
            msg = try_add_result.get("message")
        else:
            msg = ""
        return ok, str(msg if msg is not None else "")
    if isinstance(try_add_result, (tuple, list)) and len(try_add_result) >= 2:
        ok_raw, msg_raw = try_add_result[0], try_add_result[1]
        return bool(ok_raw), str(msg_raw if msg_raw is not None else "")
    return None


def tag_candidate_reject_reason(
    run_id: str,
    symbol: str,
    market_type: str,
    try_add_result: TryAddResult,
) -> None:
    """Tag reject reason from try_add return into ops_events (no-op if accepted/disabled).

    ``market_type`` must come from candidate context — caller supplies it (no hardcode).
    """
    if not fullbt_candidate_diag_enabled():
        return
    parsed = _parse_try_add_result(try_add_result)
    if parsed is None:
        logger.warning(
            "fullbt_candidate_diag: unrecognized try_add_result shape; skip tag"
        )
        return
    ok, msg = parsed
    if ok:
        return
    payload = {
        "run_id": str(run_id or ""),
        "symbol": str(symbol or ""),
        "market_type": str(market_type or ""),
        "ok": False,
        "reject_msg": str(msg)[:500],
    }
    try:
        from bitget.infra.ops_logger import insert_ops_event

        insert_ops_event(
            component=_COMPONENT,
            severity="INFO",
            event=_EVENT,
            payload=payload,
        )
    except Exception:
        logger.exception(
            "fullbt_candidate_diag: insert_ops_event failed run_id=%s symbol=%s",
            run_id,
            symbol,
        )


def retag_rejects_from_full_bt_diag(
    *,
    run_id: str,
    db_path: str,
) -> int:
    """Read-only re-emit: full_bt_diag gate_reject.detail → ops_events (no re-run).

    Uses HIST-2 stored ``detail`` (= try_add msg) only — no invented reasons.
    Returns number of ops_events attempted.
    """
    if not fullbt_candidate_diag_enabled():
        return 0
    if not run_id or not db_path or not os.path.isfile(db_path):
        return 0
    import sqlite3

    n = 0
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT symbol, market_type, detail, count
            FROM full_bt_diag
            WHERE run_id=? AND metric='gate_reject'
            """,
            (str(run_id),),
        ).fetchall()
    finally:
        conn.close()
    for symbol, market_type, detail, count in rows:
        # Reconstruct try_add_result shape from stored detail (msg field only).
        times = max(1, int(count or 1))
        for _ in range(times):
            tag_candidate_reject_reason(
                str(run_id),
                str(symbol or ""),
                str(market_type or ""),
                (False, str(detail or "")),
            )
            n += 1
    return n

"""
B-3 — Walk-forward OOS shadow judgment (log-only).

Reads ``bitget_forward_trades`` CLOSED rows, groups by B-1 normalized market + group_key,
persists pass/fail to ``bitget_walk_forward_shadow`` only — registry/config/INCUBATOR unchanged.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bitget.validation.walk_forward_bg import evaluate_oos_pass_from_returns

logger = logging.getLogger(__name__)

_SHADOW_DDL = """
CREATE TABLE IF NOT EXISTS bitget_walk_forward_shadow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    run_id TEXT NOT NULL,
    market TEXT NOT NULL,
    group_key TEXT NOT NULL,
    oos_pass INTEGER NOT NULL,
    oos_mean REAL,
    oos_n INTEGER,
    oos_sharpe REAL,
    n_closed INTEGER,
    reason TEXT,
    payload_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_wf_shadow_run
    ON bitget_walk_forward_shadow(run_id, market);
CREATE INDEX IF NOT EXISTS idx_wf_shadow_group
    ON bitget_walk_forward_shadow(market, group_key, recorded_at DESC);
"""


def walk_forward_shadow_enabled() -> bool:
    env = os.environ.get("WALK_FORWARD_SHADOW_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("WALK_FORWARD_SHADOW_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import WALK_FORWARD_SHADOW_ENABLED

    return bool(WALK_FORWARD_SHADOW_ENABLED)


def walk_forward_promotion_block_enabled() -> bool:
    """Read-only kill-switch for future live block — B-3 shadow must leave default false."""
    env = os.environ.get("WALK_FORWARD_PROMOTION_BLOCK_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("WALK_FORWARD_PROMOTION_BLOCK_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import WALK_FORWARD_PROMOTION_BLOCK_ENABLED

    return bool(WALK_FORWARD_PROMOTION_BLOCK_ENABLED)


def ensure_walk_forward_shadow_schema(db_path: str) -> None:
    if not db_path:
        return
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            conn.executescript(_SHADOW_DDL)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("walk_forward shadow schema skip: %s", ex)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _load_closed_trade_groups(
    db_path: str,
) -> Dict[Tuple[str, str], List[float]]:
    """
    Group chronological decimal returns by (normalized_market, group_key).

    Uses B-1 ``normalize_market_key`` on ``market_type`` — no raw BG comparison.
    """
    from bitget.evolution.market_key_normalize import normalize_market_key
    from bitget.forward.gates import _extract_core_group

    groups: Dict[Tuple[str, str], List[Tuple[str, float]]] = {}
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute(
            """
            SELECT market_type, sig_type, final_ret,
                   COALESCE(NULLIF(TRIM(exit_date), ''), entry_date) AS sort_date
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%'
              AND final_ret IS NOT NULL
              AND sig_type IS NOT NULL
              AND TRIM(sig_type) != ''
            ORDER BY sort_date ASC, id ASC
            """
        )
        for market_type, sig_type, final_ret, _sort_date in cur.fetchall():
            try:
                ret = float(final_ret)
            except (TypeError, ValueError):
                continue
            if not (ret == ret):  # NaN
                continue
            mk = normalize_market_key(str(market_type or "spot"))
            gk = _extract_core_group(str(sig_type or ""))
            if not gk:
                continue
            groups.setdefault((mk, gk), []).append((_sort_date or "", ret / 100.0))
    finally:
        conn.close()

    out: Dict[Tuple[str, str], List[float]] = {}
    for key, rows in groups.items():
        out[key] = [r for _d, r in rows]
    return out


def judge_groups_walk_forward(
    grouped_returns: Dict[Tuple[str, str], List[float]],
) -> List[Dict[str, Any]]:
    judgments: List[Dict[str, Any]] = []
    for (market, group_key), returns in sorted(grouped_returns.items()):
        verdict = evaluate_oos_pass_from_returns(returns)
        judgments.append(
            {
                "market": market,
                "group_key": group_key,
                "oos_pass": bool(verdict.get("pass")),
                "oos_mean": float(verdict.get("oos_mean") or 0.0),
                "oos_n": int(verdict.get("oos_n") or 0),
                "oos_sharpe": float(verdict.get("oos_sharpe") or 0.0),
                "n_closed": int(verdict.get("n_closed") or len(returns)),
                "reason": str(verdict.get("reason") or ""),
            }
        )
    return judgments


def persist_walk_forward_shadow_rows(
    judgments: List[Dict[str, Any]],
    *,
    db_path: str,
    run_id: Optional[str] = None,
    source: str = "weekly_batch",
) -> str:
    ensure_walk_forward_shadow_schema(db_path)
    rid = run_id or _new_run_id()
    recorded_at = _now_iso()
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        for row in judgments:
            payload = json.dumps({"source": source}, ensure_ascii=False)
            conn.execute(
                """
                INSERT INTO bitget_walk_forward_shadow (
                    recorded_at, run_id, market, group_key,
                    oos_pass, oos_mean, oos_n, oos_sharpe,
                    n_closed, reason, payload_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    recorded_at,
                    rid,
                    str(row.get("market") or "SPOT").upper(),
                    str(row.get("group_key") or ""),
                    1 if row.get("oos_pass") else 0,
                    float(row.get("oos_mean") or 0.0),
                    int(row.get("oos_n") or 0),
                    float(row.get("oos_sharpe") or 0.0),
                    int(row.get("n_closed") or 0),
                    str(row.get("reason") or ""),
                    payload,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return rid


def run_walk_forward_shadow_job(
    *,
    forward_db_path: Optional[str] = None,
    shadow_db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Batch entry — weekly_evolution pipeline hook.

    No registry/config/INCUBATOR writes. Promotion block flag is read but not applied.
    """
    if not walk_forward_shadow_enabled():
        logger.info("walk_forward shadow disabled — skip batch")
        return None

    if walk_forward_promotion_block_enabled():
        logger.warning(
            "WALK_FORWARD_PROMOTION_BLOCK_ENABLED=true but B-3 live block not implemented — shadow log only"
        )

    from bitget.infra.data_paths import market_data_db_path

    fwd_path = forward_db_path or market_data_db_path()
    sh_path = shadow_db_path or fwd_path

    grouped = _load_closed_trade_groups(fwd_path)
    if not grouped:
        logger.info("walk_forward shadow: no closed trade groups")
        return {"run_id": None, "groups": 0, "pass_n": 0, "fail_n": 0}

    judgments = judge_groups_walk_forward(grouped)
    run_id = persist_walk_forward_shadow_rows(judgments, db_path=sh_path)
    pass_n = sum(1 for j in judgments if j.get("oos_pass"))
    fail_n = len(judgments) - pass_n
    logger.info(
        "walk_forward shadow recorded groups=%d pass=%d fail=%d run_id=%s",
        len(judgments),
        pass_n,
        fail_n,
        run_id,
    )
    return {
        "run_id": run_id,
        "groups": len(judgments),
        "pass_n": pass_n,
        "fail_n": fail_n,
        "judgments": judgments,
    }

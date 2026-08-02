"""
B-2 — Deathmatch allocation shadow (4w log-only).

Counterfactual Kelly mult from deathmatch overlay — shadow table only.
Production ``META_GROUP_KELLY_MULT`` and ``sim_kelly_invest`` must remain unchanged.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TYPE_CHECKING

from evolution.deathmatch_allocation import (
    compute_group_allocation_overlay,
    health_to_group_mult,
    merge_group_kelly_from_overlay,
)
from evolution.deathmatch_config import load_deathmatch_config, market_deathmatch_params

if TYPE_CHECKING:
    from evolution.deathmatch_battle_royale import BattleRoyaleResult

logger = logging.getLogger(__name__)

_SHADOW_DDL = """
CREATE TABLE IF NOT EXISTS bitget_deathmatch_alloc_shadow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    run_id TEXT NOT NULL,
    market TEXT NOT NULL,
    group_key TEXT NOT NULL,
    overlay_mult REAL NOT NULL,
    merged_kelly_mult REAL NOT NULL,
    source TEXT NOT NULL,
    counterfactual_kelly_pct REAL,
    production_kelly_pct REAL,
    payload_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_dm_alloc_shadow_run
    ON bitget_deathmatch_alloc_shadow(run_id, market);
CREATE INDEX IF NOT EXISTS idx_dm_alloc_shadow_group
    ON bitget_deathmatch_alloc_shadow(market, group_key, recorded_at DESC);
"""


def deathmatch_allocation_shadow_enabled() -> bool:
    env = os.environ.get("DEATHMATCH_ALLOCATION_SHADOW_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("DEATHMATCH_ALLOCATION_SHADOW_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import DEATHMATCH_ALLOCATION_SHADOW_ENABLED

    return bool(DEATHMATCH_ALLOCATION_SHADOW_ENABLED)


def ensure_deathmatch_shadow_schema(db_path: str) -> None:
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
        logger.warning("deathmatch shadow schema skip: %s", ex)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def build_shadow_allocation_proposal(
    br: "BattleRoyaleResult",
    *,
    market_type: str,
    sys_config: Optional[dict] = None,
    meta_health: Optional[dict] = None,
) -> Optional[Dict[str, Any]]:
    """Compute counterfactual overlay + merged group mult — no meta/config writes."""
    from bitget.evolution.market_key_normalize import normalize_market_key

    mk = normalize_market_key(market_type)
    dmcfg = market_deathmatch_params(
        load_deathmatch_config(sys_config if isinstance(sys_config, dict) else {}),
        mk,
    )
    champ_gk = None
    if br.champion:
        champ_gk = str(br.champion.group_key or "").strip() or None

    proposal = compute_group_allocation_overlay(
        br.arms,
        dmcfg=dmcfg,
        champion_group_key=champ_gk,
    )
    if int(proposal.get("eligible_n", 0) or 0) < 1:
        return None

    max_mult = float(dmcfg.get("allocation_max_group_mult", 1.5))
    health_mult = health_to_group_mult(meta_health if isinstance(meta_health, dict) else {})
    merged = merge_group_kelly_from_overlay(
        health_mult,
        dict(proposal.get("group_mult") or {}),
        max_mult=max_mult,
    )
    return {
        "market": mk,
        "proposal": proposal,
        "merged_group_mult": merged,
        "overlay": dict(proposal.get("group_mult") or {}),
    }


def persist_shadow_allocation_rows(
    shadow: Dict[str, Any],
    *,
    db_path: str,
    source: str = "deathmatch_run",
    run_id: Optional[str] = None,
) -> str:
    """Write shadow mult rows; returns run_id."""
    ensure_deathmatch_shadow_schema(db_path)
    rid = run_id or _new_run_id()
    recorded_at = _now_iso()
    market = str(shadow.get("market") or "SPOT").upper()
    overlay = dict(shadow.get("overlay") or {})
    merged = dict(shadow.get("merged_group_mult") or {})
    proposal = shadow.get("proposal") or {}
    payload = json.dumps(
        {
            "standby_groups": list(proposal.get("standby_groups") or []),
            "boost_groups": list(proposal.get("boost_groups") or []),
            "eligible_n": proposal.get("eligible_n", 0),
        },
        ensure_ascii=False,
    )
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        for gk in sorted(set(overlay) | set(merged)):
            conn.execute(
                """
                INSERT INTO bitget_deathmatch_alloc_shadow (
                    recorded_at, run_id, market, group_key,
                    overlay_mult, merged_kelly_mult, source,
                    counterfactual_kelly_pct, production_kelly_pct, payload_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    recorded_at,
                    rid,
                    market,
                    gk,
                    float(overlay.get(gk, 1.0)),
                    float(merged.get(gk, 1.0)),
                    source,
                    None,
                    None,
                    payload,
                ),
            )
        conn.commit()
    finally:
        conn.close()
    return rid


def record_deathmatch_shadow_from_battle_royale(
    br: "BattleRoyaleResult",
    *,
    market_type: str,
    sys_config: Optional[dict] = None,
    meta_health: Optional[dict] = None,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Post-deathmatch hook — shadow log only."""
    if not deathmatch_allocation_shadow_enabled():
        return None

    shadow = build_shadow_allocation_proposal(
        br,
        market_type=market_type,
        sys_config=sys_config,
        meta_health=meta_health,
    )
    if not shadow:
        return None

    from bitget.infra.data_paths import market_data_db_path

    path = db_path or market_data_db_path()
    run_id = persist_shadow_allocation_rows(shadow, db_path=path)
    logger.info(
        "deathmatch alloc shadow recorded market=%s groups=%d run_id=%s",
        shadow.get("market"),
        len(shadow.get("merged_group_mult") or {}),
        run_id,
    )
    return {"run_id": run_id, **shadow}


def _latest_shadow_mult(
    db_path: str,
    *,
    market: str,
    group_key: str,
) -> Optional[float]:
    try:
        conn = sqlite3.connect(db_path, timeout=15)
        try:
            row = conn.execute(
                """
                SELECT merged_kelly_mult FROM bitget_deathmatch_alloc_shadow
                WHERE market = ? AND group_key = ? AND source = 'deathmatch_run'
                ORDER BY recorded_at DESC, id DESC
                LIMIT 1
                """,
                (str(market).upper(), str(group_key)),
            ).fetchone()
        finally:
            conn.close()
        if row:
            return float(row[0])
    except (OSError, sqlite3.Error, TypeError, ValueError):
        pass
    return None


def observe_kelly_chain_shadow(
    kelly_risk_pct: float,
    *,
    core_group: Optional[str],
    market_type: str,
    meta_state: Optional[dict],
    cfg: Optional[dict],
) -> float:
    """
    Kelly chain hook — log counterfactual shadow mult only.

    **Always** returns ``kelly_risk_pct`` unchanged (production sizing isolation).
    """
    if not deathmatch_allocation_shadow_enabled():
        return kelly_risk_pct
    if not core_group:
        return kelly_risk_pct

    try:
        from bitget.evolution.market_key_normalize import normalize_market_key
        from bitget.infra.data_paths import market_data_db_path

        mk = normalize_market_key(market_type)
        meta = meta_state if isinstance(meta_state, dict) else {}
        grp_map = meta.get("META_GROUP_KELLY_MULT")
        prod_mult = 1.0
        if isinstance(grp_map, dict) and core_group in grp_map:
            prod_mult = float(grp_map[core_group] or 1.0)
        if prod_mult <= 0:
            prod_mult = 1.0

        db_path = market_data_db_path()
        shadow_mult = _latest_shadow_mult(db_path, market=mk, group_key=core_group)
        if shadow_mult is None:
            return kelly_risk_pct

        counterfactual = float(kelly_risk_pct) * float(shadow_mult) / prod_mult
        ensure_deathmatch_shadow_schema(db_path)
        conn = sqlite3.connect(db_path, timeout=15)
        try:
            conn.execute(
                """
                INSERT INTO bitget_deathmatch_alloc_shadow (
                    recorded_at, run_id, market, group_key,
                    overlay_mult, merged_kelly_mult, source,
                    counterfactual_kelly_pct, production_kelly_pct, payload_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    _now_iso(),
                    "kelly_observe",
                    mk,
                    core_group,
                    float(shadow_mult),
                    float(shadow_mult),
                    "kelly_observe",
                    float(counterfactual),
                    float(kelly_risk_pct),
                    json.dumps({"prod_group_mult": prod_mult}, ensure_ascii=False),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as ex:
        logger.debug("kelly shadow observe skip: %s", ex)

    return kelly_risk_pct

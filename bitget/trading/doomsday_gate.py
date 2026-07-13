"""
Bitget Doomsday DEFCON entry gate — capital survival plane.

Invariants:
  - DEFCON ≤ DOOMSDAY_BLOCK_LEVEL → block new LONG only (SHORT may hedge)
  - Never auto-flatten open inventory
  - Soft-pass if DOOMSDAY_DEFCON missing / unreadable
  - Contagion score feeds ARCR (LONG damp / SHORT hold-or-boost) via regime_capital_relay
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.memory_policy import DOOMSDAY_BLOCK_LEVEL


def crypto_contagion_score(
    btc_dominance: float,
    eth_btc_ratio: float,
    market_cap_change_24h: float,
) -> float:
    """
    0–100 proxy written as scores.Global_Contagion_Score so shared
    doomsday_dampener.global_score_from_config works without stock-only fields.
    """
    s = 0.0
    try:
        dom = float(btc_dominance or 0.0)
    except (TypeError, ValueError):
        dom = 0.0
    try:
        eth_btc = float(eth_btc_ratio or 0.0)
    except (TypeError, ValueError):
        eth_btc = 0.0
    try:
        mcap = float(market_cap_change_24h or 0.0)
    except (TypeError, ValueError):
        mcap = 0.0

    # Dominance flight-to-BTC: 50→0, 58→~27, 65+→50
    if dom >= 50.0:
        s += min(50.0, (dom - 50.0) / 15.0 * 50.0)
    # Alt weakness via ETH/BTC: 0.055→0, 0.045→20, 0.035→40
    if eth_btc > 0.0 and eth_btc < 0.055:
        s += min(40.0, (0.055 - eth_btc) / 0.020 * 40.0)
    # 24h crypto mcap crash: 0→0, −6→25, −12→50
    if mcap < 0.0:
        s += min(50.0, abs(mcap) / 12.0 * 50.0)
    return float(min(100.0, max(0.0, round(s, 3))))


def floor_score_for_defcon(defcon_level: int, raw_score: float) -> float:
    """Ensure DEFCON bands produce dampener-visible scores (floor 40 in dampener)."""
    try:
        lvl = int(defcon_level)
    except (TypeError, ValueError):
        lvl = 5
    try:
        s = float(raw_score)
    except (TypeError, ValueError):
        s = 0.0
    if lvl <= 1:
        return max(s, 95.0)
    if lvl <= 2:
        return max(s, 80.0)
    if lvl == 4:
        return max(s, 45.0)
    return s


def defcon_level_from_cfg(cfg: Optional[dict]) -> tuple[int, dict[str, Any]]:
    """Return (level, meta). Missing/malformed → soft level 5."""
    meta: dict[str, Any] = {"doomsday_gate": "ok"}
    blk = (cfg or {}).get("DOOMSDAY_DEFCON")
    if not isinstance(blk, dict):
        meta["doomsday_gate"] = "soft_pass_missing_defcon"
        return 5, meta
    try:
        level = int(blk.get("level", 5))
    except (TypeError, ValueError):
        meta["doomsday_gate"] = "soft_pass_bad_level"
        return 5, meta
    meta["defcon_level"] = level
    scores = blk.get("scores") if isinstance(blk.get("scores"), dict) else {}
    meta["global_contagion_score"] = scores.get("Global_Contagion_Score")
    return level, meta


def block_level(cfg: Optional[dict]) -> int:
    try:
        raw = (cfg or {}).get("DOOMSDAY_BLOCK_LEVEL", DOOMSDAY_BLOCK_LEVEL)
        return max(1, int(raw if raw is not None else DOOMSDAY_BLOCK_LEVEL))
    except (TypeError, ValueError):
        return int(DOOMSDAY_BLOCK_LEVEL)


def doomsday_long_entry_blocked(
    cfg: dict,
    *,
    position_side: str,
) -> tuple[bool, dict[str, Any]]:
    """
    True → block new LONG entry when DEFCON ≤ block level.
    SHORT never blocked here (hedge path). Never flatten.
    """
    side = str(position_side or "LONG").upper()
    thr = block_level(cfg)
    level, meta = defcon_level_from_cfg(cfg)
    meta["doomsday_block_level"] = thr
    meta["candidate_side"] = side
    soft_tag = str(meta.get("doomsday_gate") or "")
    if side != "LONG":
        meta["doomsday_gate"] = (
            soft_tag if soft_tag.startswith("soft_pass") else "ok_short_allowed"
        )
        return False, meta
    if level > thr:
        if not soft_tag.startswith("soft_pass"):
            meta["doomsday_gate"] = "ok"
        return False, meta
    meta["doomsday_gate"] = "block"
    return True, meta


def doomsday_size_mult(
    cfg: Optional[dict],
    meta_state: Optional[dict] = None,
    *,
    position_side: str = "LONG",
) -> float:
    """
    Live size mult via ARCR side-regime (paper≈live).
    LONG → damp ≤1.0; SHORT → 1.0 or soft boost ≤ ARCR_SHORT_RELAY_CAP.
    Soft-pass → 1.0 when score missing.
    """
    try:
        from bitget.trading.regime_capital_relay import resolve_side_regime_mult

        mult, _ = resolve_side_regime_mult(
            cfg, position_side=position_side, meta_state=meta_state
        )
        return float(mult)
    except Exception:
        return 1.0

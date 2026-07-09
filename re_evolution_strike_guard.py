"""
Re-Evolution Phase 1 — 3-Strike 강등 및 섀도우 전환 (Non-Destructive Demotion).

Architect 철학: LIVE 실패 로직을 삭제·정지하지 않고 자본만 회수(Kelly=0)한 뒤
OBSERVING(섀도우) 상태로 가상매매를 지속한다.

트리거:
  · registry state=LIVE 인 그룹의 실자본 청산(sim_kelly_invest>0)
  · final_ret ≤ loss_threshold_pct (기본 -5%) 연속 strike_need 회 (기본 3)
  · 승리 청산 시 연속 카운터 리셋

조치:
  · registry: LIVE → OBSERVING, capital_mult=0
  · meta: META_RE_EVOLUTION_SHADOW + Kelly overlay 0 (로직 유지·표본 수집)
  · 진입: OBSERVE_ONLY 태그 + notional 0 (가상 장부만)
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

_SHADOW_TAG = "RE_EVOL_SHADOW"


def _cfg_bool(cfg: Optional[Dict[str, Any]], key: str, default: bool) -> bool:
    if not isinstance(cfg, dict):
        return default
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def re_evolution_strike_thresholds(
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    block = cfg.get("RE_EVOLUTION_STRIKE") or {}
    base = block if isinstance(block, dict) else cfg

    def _f(key: str, default: float) -> float:
        try:
            return float(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        try:
            return int(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        "loss_threshold_pct": _f("RE_EVOLUTION_STRIKE_LOSS_PCT", -5.0),
        "strike_need": float(_i("RE_EVOLUTION_STRIKE_NEED", 3)),
        "enabled": 1.0 if _cfg_bool(cfg, "ENABLE_RE_EVOLUTION_STRIKE", True) else 0.0,
    }


def extract_core_group_name(sig_type: str) -> str:
    try:
        from meta_treasury_entry_guard import extract_core_group_name as _ext

        return _ext(sig_type)
    except Exception:
        clean = str(sig_type or "").replace("💀[기각/관찰용] ", "")
        clean = re.sub(r"^\[.*?\]\s*", "", clean)
        return clean.split(" [")[0].strip()


def is_live_capital_closure(
    sig_type: str,
    *,
    sim_kelly_invest: float = 0.0,
    invest_amount: float = 0.0,
) -> bool:
    """실자본 LIVE 청산 여부 — OBSERVE_ONLY / INCUBATOR 제외."""
    sig = str(sig_type or "")
    if "OBSERVE_ONLY" in sig or "INCUBATOR_" in sig or "기각/관찰용" in sig:
        return False
    if _SHADOW_TAG in sig:
        return False
    try:
        sk = float(sim_kelly_invest or 0.0)
        inv = float(invest_amount or 0.0)
    except (TypeError, ValueError):
        return False
    return sk > 0.0 or inv > 0.0


def _strike_bucket_key(market: str, group_key: str) -> str:
    return f"{str(market or 'KR').upper()}|{str(group_key or '').strip()}"


def _load_strike_map(meta: Mapping[str, Any]) -> Dict[str, Any]:
    raw = meta.get("META_RE_EVOLUTION_STRIKES")
    return dict(raw) if isinstance(raw, dict) else {}


def _load_shadow_set(meta: Mapping[str, Any]) -> set[str]:
    raw = meta.get("META_RE_EVOLUTION_SHADOW_GROUPS")
    if isinstance(raw, list):
        return {str(x).strip() for x in raw if str(x).strip()}
    return set()


def is_re_evolution_shadow_group(
    meta: Optional[Mapping[str, Any]],
    market: str,
    group_key: str,
) -> bool:
    """3-Strike 강등 후 섀도우(OBSERVING) 가상매매 대상."""
    if not isinstance(meta, Mapping):
        return False
    gk = str(group_key or "").strip()
    if not gk:
        return False
    if gk in _load_shadow_set(meta):
        return True
    bk = _strike_bucket_key(market, gk)
    rec = _load_strike_map(meta).get(bk)
    if isinstance(rec, dict) and rec.get("demoted"):
        return True
    try:
        from strategy_promotion_engine import is_group_live_in_registry

        if is_group_live_in_registry(meta, market, gk):
            return False
        reg = meta.get("META_STRATEGY_REGISTRY")
        if not isinstance(reg, list):
            return False
        mk = str(market or "KR").upper()
        for row in reg:
            if not isinstance(row, dict):
                continue
            if str(row.get("market") or "").upper() != mk:
                continue
            rg = str(row.get("group_key") or row.get("display_name") or "").strip()
            if rg != gk and gk not in rg and rg not in gk:
                continue
            st = str(row.get("state") or "").upper()
            if st == "OBSERVING" and str(row.get("demote_reason") or "").startswith(
                "re_evolution_3_strike"
            ):
                return True
    except Exception:
        pass
    return False


def format_re_evolution_shadow_sig_type(
    strategy_id: str,
    sig_body: str,
) -> str:
    sid = str(strategy_id or "UNKNOWN").strip()
    body = str(sig_body or "").strip()
    return f"[OBSERVE_ONLY][{_SHADOW_TAG}][{sid}] {body}".strip()


def apply_shadow_entry_zero_notional(
    sig_type: str,
    *,
    strategy_id: str = "",
) -> Tuple[str, int, float, float]:
    """섀도우 진입 — notional 0, OBSERVE_ONLY 태그 부착."""
    tagged = format_re_evolution_shadow_sig_type(strategy_id, sig_type)
    return tagged, 0, 0.0, 0.0


def _merge_re_evolution_kelly_overlay(meta: Dict[str, Any], group_key: str) -> None:
    """그룹 Kelly overlay=0 — Treasury health 와 독립(섀도우는 진입 허용)."""
    gk = str(group_key or "").strip()
    if not gk:
        return
    overlay = dict(meta.get("META_RE_EVOLUTION_KELLY_OVERLAY") or {})
    overlay[gk] = 0.0
    meta["META_RE_EVOLUTION_KELLY_OVERLAY"] = overlay

    dm = dict(meta.get("META_DEATHMATCH_KELLY_OVERLAY") or {})
    dm[gk] = 0.0
    meta["META_DEATHMATCH_KELLY_OVERLAY"] = dm

    try:
        from evolution.deathmatch_allocation import (
            health_to_group_mult,
            merge_group_kelly_from_overlay,
        )

        health_mult = health_to_group_mult(meta.get("META_STRATEGY_HEALTH") or {})
        cap = 1.5
        meta["META_GROUP_KELLY_MULT"] = merge_group_kelly_from_overlay(
            health_mult, dm, max_mult=cap
        )
    except Exception as ex:
        logger.warning("re_evolution kelly overlay merge skip: %s", ex)


def apply_three_strike_demotion(
    *,
    market: str,
    group_key: str,
    strategy_id: str,
    strikes: int,
    sys_config: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    LIVE → OBSERVING 비파괴 강등 + Kelly 회수.
    registry·meta 원자 갱신.
    """
    from meta_governor import save_meta_governor_state_atomic
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved
    from strategy_promotion_engine import stable_strategy_id
    from strategy_registry_store import upsert_registry_rows

    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    sid = str(strategy_id or stable_strategy_id(mk, gk))
    now_iso = datetime.now(timezone.utc).isoformat()
    reason = f"re_evolution_3_strike(x{int(strikes)})"

    meta = dict(load_meta_state_resolved())
    strikes_map = _load_strike_map(meta)
    bk = _strike_bucket_key(mk, gk)
    strikes_map[bk] = {
        "strategy_id": sid,
        "market": mk,
        "group_key": gk,
        "consecutive_strikes": int(strikes),
        "demoted": True,
        "demoted_at": now_iso,
        "demote_reason": reason,
    }
    meta["META_RE_EVOLUTION_STRIKES"] = strikes_map

    shadow = sorted(_load_shadow_set(meta) | {gk})
    meta["META_RE_EVOLUTION_SHADOW_GROUPS"] = shadow
    meta["META_RE_EVOLUTION_LAST_DEMOTION_AT"] = now_iso

    demoted_log: List[Dict[str, Any]] = list(meta.get("META_RE_EVOLUTION_DEMOTED") or [])
    demoted_log.append(
        {
            "strategy_id": sid,
            "market": mk,
            "group_key": gk,
            "strikes": int(strikes),
            "demoted_at": now_iso,
            "reason": reason,
            "mutation_pending": True,
            "mutation_done": False,
        }
    )
    meta["META_RE_EVOLUTION_DEMOTED"] = demoted_log[-50:]

    _merge_re_evolution_kelly_overlay(meta, gk)

    save_meta_governor_state_atomic(meta)
    invalidate_meta_state_cache()

    reg_row = {
        "strategy_id": sid,
        "market": mk.split("_")[0] if "_" in mk else mk,
        "group_key": gk,
        "display_name": gk,
        "state": "OBSERVING",
        "capital_mult": 0.0,
        "source": "re_evolution_strike",
        "last_demoted_at": now_iso,
        "demote_reason": reason,
        "updated_at": now_iso,
    }
    upsert_registry_rows([reg_row], forward_db_path)

    logger.info(
        "Re-Evolution 3-Strike demotion: %s %s → OBSERVING (strikes=%d)",
        mk,
        gk,
        strikes,
    )
    return {
        "demoted": True,
        "strategy_id": sid,
        "market": mk,
        "group_key": gk,
        "state": "OBSERVING",
        "reason": reason,
    }


def process_live_closure_strike(
    *,
    market: str,
    sig_type: str,
    final_ret_pct: float,
    sim_kelly_invest: float = 0.0,
    invest_amount: float = 0.0,
    sys_config: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    LIVE 청산 1건 평가 → strike 누적 또는 3-Strike 강등.
    ledger track_daily_positions 청산 직후 호출.
    """
    th = re_evolution_strike_thresholds(sys_config)
    if th["enabled"] < 0.5:
        return {"action": "disabled"}

    if not is_live_capital_closure(
        sig_type,
        sim_kelly_invest=sim_kelly_invest,
        invest_amount=invest_amount,
    ):
        return {"action": "skip_not_live_capital"}

    gk = extract_core_group_name(sig_type)
    if not gk:
        return {"action": "skip_no_group"}

    from meta_governor_consumer import load_meta_state_resolved
    from strategy_promotion_engine import is_group_live_in_registry, stable_strategy_id

    meta = load_meta_state_resolved()
    mk = str(market or "KR").upper()
    sid = stable_strategy_id(mk, gk)

    if is_re_evolution_shadow_group(meta, mk, gk):
        return {"action": "skip_already_shadow"}

    if not is_group_live_in_registry(meta, mk, gk):
        return {"action": "skip_not_registry_live", "group_key": gk}

    loss_th = float(th["loss_threshold_pct"])
    need = int(th["strike_need"])
    ret = float(final_ret_pct)

    strikes_map = _load_strike_map(meta)
    bk = _strike_bucket_key(mk, gk)
    rec = dict(strikes_map.get(bk) or {})
    streak = int(rec.get("consecutive_strikes", 0) or 0)

    if ret <= loss_th:
        streak += 1
        rec.update(
            {
                "strategy_id": sid,
                "market": mk,
                "group_key": gk,
                "consecutive_strikes": streak,
                "last_closure_ret": ret,
                "last_loss_at": datetime.now(timezone.utc).isoformat(),
                "demoted": False,
            }
        )
        strikes_map[bk] = rec

        from meta_governor import save_meta_governor_state_atomic
        from meta_governor_consumer import invalidate_meta_state_cache

        meta_u = dict(meta)
        meta_u["META_RE_EVOLUTION_STRIKES"] = strikes_map
        save_meta_governor_state_atomic(meta_u)
        invalidate_meta_state_cache()

        if streak >= need:
            dem = apply_three_strike_demotion(
                market=mk,
                group_key=gk,
                strategy_id=sid,
                strikes=streak,
                sys_config=sys_config,
                forward_db_path=forward_db_path,
            )
            return {
                "action": "demoted_observing",
                "strikes": streak,
                "final_ret_pct": ret,
                **dem,
            }
        return {
            "action": "strike_recorded",
            "strikes": streak,
            "strike_need": need,
            "final_ret_pct": ret,
            "group_key": gk,
        }

    if ret > 0.0 and streak > 0:
        strikes_map.pop(bk, None)
        from meta_governor import save_meta_governor_state_atomic
        from meta_governor_consumer import invalidate_meta_state_cache

        meta_u = dict(meta)
        meta_u["META_RE_EVOLUTION_STRIKES"] = strikes_map
        save_meta_governor_state_atomic(meta_u)
        invalidate_meta_state_cache()
        return {"action": "strike_reset_win", "group_key": gk, "final_ret_pct": ret}

    return {"action": "no_strike", "final_ret_pct": ret, "group_key": gk}

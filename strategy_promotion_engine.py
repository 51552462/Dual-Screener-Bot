"""
MetaGovernor 전략 무기고 — Discovery · LIVE Hard Gate · Whipsaw(일별) · Alpha TTL.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from strategy_lifecycle_config import load_strategy_lifecycle_config, market_params
from strategy_registry_store import (
    consecutive_below_live_days,
    load_registry_rows,
    merge_registry_sources,
    record_quality_daily,
    upsert_registry_rows,
    _kst_today,
)

logger = logging.getLogger(__name__)


def stable_strategy_id(market: str, group_key: str) -> str:
    raw = f"{str(market or 'KR').upper()}|{str(group_key or '').strip()}"
    return "strat:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def parse_health_key(key: str) -> Tuple[str, str]:
    s = str(key or "").strip()
    if "|" in s:
        mp, _, gk = s.partition("|")
        return mp.upper().strip() or "KR", gk.strip()
    return "KR", s


def profit_factor_from_returns(rets: List[float]) -> float:
    from reports.forward_report_scalar import profit_factor_from_returns as _pf_ssot

    return _pf_ssot(rets)


def _health_for_row(health: Dict[str, Any], market: str, group_key: str) -> Optional[Dict[str, Any]]:
    gk = str(group_key or "").strip()
    mp = str(market or "KR").upper()
    if not gk:
        return None
    full = f"{mp}|{gk}"
    hv = health.get(full)
    if isinstance(hv, dict):
        return hv
    for hk, v in health.items():
        if hk == "__meta__" or not isinstance(v, dict):
            continue
        m2, g2 = parse_health_key(hk)
        if g2 == gk and m2 == mp:
            return v
    return None


def passes_candidate_gate(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    n = int(hv.get("n", 0) or 0)
    if n < int(mp.get("candidate_min_trades", 15)):
        return False
    wr = float(hv.get("rolling_wr", 0) or 0)
    pf = float(hv.get("rolling_pf", 0) or 0)
    mdd = float(hv.get("mdd_pct", 0) or 0)
    if wr < float(mp.get("candidate_min_wr", 0.45)):
        return False
    if pf < float(mp.get("candidate_min_pf", 1.2)):
        return False
    if mdd < float(mp.get("candidate_max_mdd_pct", -28)):
        return False
    mult = float(hv.get("mult", 1.0) or 1.0)
    if mult <= 0.0:
        return False
    return True


def passes_live_hard_gate(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    """LIVE 승격 Hard Gate — 슬리피지 버퍼 반영."""
    n = int(hv.get("n", 0) or 0)
    if n < int(mp.get("promote_min_trades", 15)):
        return False
    wr = float(hv.get("rolling_wr", 0) or 0)
    pf = float(hv.get("rolling_pf", 0) or 0)
    mult = float(hv.get("mult", 1.0) or 1.0)
    if mult <= 0.0:
        return False

    live_wr = float(mp.get("live_min_wr", 0.50))
    mid_min = float(mp.get("live_wr_mid_min", 0.45))
    mid_max = float(mp.get("live_wr_mid_max", 0.499))
    mid_pf = float(mp.get("live_mid_min_pf", 1.50))
    pf_if_ok = float(mp.get("live_min_pf_if_wr_ok", 1.35))

    if wr >= live_wr:
        return pf >= pf_if_ok
    if mid_min <= wr <= mid_max:
        return pf >= mid_pf
    return False


_FAST_TRACK_PREFIXES = ("INCUBATOR_", "ACE_", "MUTANT_", "PLAYBOOK_", "HIDDEN_THEME_")


def is_fast_track_group(group_key: str) -> bool:
    """인큐베이터·ACE·뮤턴트 등 하이퍼-패스트트랙 대상 그룹."""
    gk = str(group_key or "").strip().upper()
    if not gk:
        return False
    if any(gk.startswith(p) for p in _FAST_TRACK_PREFIXES):
        return True
    if "ACE" in gk and ("PLAYBOOK" in gk or "EVOLUTION" in gk):
        return True
    return False


def passes_hard_threshold_auto_promotion(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    """
    Hard-Threshold Auto-Promotion — PF≥2.0 · 표본≥N 이면 WR 게이트 생략 LIVE.
    """
    if not bool(mp.get("fast_track_enabled", True)):
        return False
    n = int(hv.get("n", 0) or 0)
    n_min = int(mp.get("fast_track_min_trades", mp.get("promote_min_trades", 10)))
    pf_min = float(mp.get("fast_track_min_pf", 2.0))
    pf = float(hv.get("rolling_pf", 0) or 0)
    mult = float(hv.get("mult", 1.0) or 1.0)
    if mult <= 0.0:
        return False
    return n >= n_min and pf >= pf_min


def is_group_live_in_registry(
    meta: Optional[Dict[str, Any]],
    market: str,
    group_key: str,
) -> bool:
    """META_STRATEGY_REGISTRY 에 LIVE 로 등재된 그룹인지."""
    if not isinstance(meta, dict):
        return False
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    if not gk:
        return False
    reg = meta.get("META_STRATEGY_REGISTRY")
    if not isinstance(reg, list):
        return False
    for row in reg:
        if not isinstance(row, dict):
            continue
        if str(row.get("state") or "").upper() != "LIVE":
            continue
        if str(row.get("market") or "").upper() != mk:
            continue
        rg = str(row.get("group_key") or row.get("display_name") or "").strip()
        if rg == gk or gk in rg or rg in gk:
            return True
    return False


def is_below_live_threshold(hv: Dict[str, Any], mp: Dict[str, Any]) -> bool:
    """Whipsaw: rolling_wr 또는 rolling_pf 가 LIVE 최소 기준 미만."""
    wr = float(hv.get("rolling_wr", 0) or 0)
    pf = float(hv.get("rolling_pf", 0) or 0)
    live_wr = float(mp.get("live_min_wr", 0.50))
    mid_min = float(mp.get("live_wr_mid_min", 0.45))
    pf_if_ok = float(mp.get("live_min_pf_if_wr_ok", 1.35))
    mid_pf = float(mp.get("live_mid_min_pf", 1.50))

    if wr < mid_min:
        return True
    if wr >= live_wr:
        return pf < pf_if_ok
    if mid_min <= wr < live_wr:
        return pf < mid_pf
    return True


def _parse_iso_dt(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _days_since(iso_val: Any, now: datetime) -> Optional[int]:
    dt = _parse_iso_dt(iso_val)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now_a = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    return max(0, (now_a - dt.astimezone(timezone.utc)).days)


def _load_validated_promoted(path: Optional[str]) -> List[Dict[str, Any]]:
    import json
    import os

    p = path or ""
    if not p or not os.path.isfile(p):
        return []
    try:
        with open(p, "r", encoding="utf-8") as f:
            raw = json.load(f)
        prom = raw.get("promoted") if isinstance(raw, dict) else None
        return [x for x in prom if isinstance(x, dict)] if isinstance(prom, list) else []
    except Exception as e:
        logger.warning("validated_live_mutants read failed: %s", e)
        return []


def _stable_mutant_id(name: str) -> str:
    h = hashlib.sha256(str(name).encode("utf-8")).hexdigest()[:14]
    return f"mutant:{h}"


def run_registry_lifecycle(
    *,
    prior_registry: List[Dict[str, Any]],
    health: Dict[str, Any],
    system_cfg: Optional[Dict[str, Any]] = None,
    validated_mutants_path: Optional[str] = None,
    forward_db_path: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Treasury 헬스 → Discovery → 일별 품질 기록 → 승격/강등/은퇴.
    반환: (registry 리스트, META_REGISTRY_CYCLE_STATS)
    """
    now = now or datetime.now(timezone.utc)
    now_iso = now.isoformat()
    lc_cfg = load_strategy_lifecycle_config(system_cfg)
    today_kst = _kst_today()

    db_rows = load_registry_rows(forward_db_path)
    reg = merge_registry_sources(db_rows, prior_registry)
    by_sid: Dict[str, Dict[str, Any]] = {
        str(r["strategy_id"]): dict(r) for r in reg if r.get("strategy_id")
    }

    stats: Dict[str, Any] = {
        "discovery_new": 0,
        "promoted_live": 0,
        "fast_track_promoted": 0,
        "demoted_cooled": 0,
        "retired": 0,
        "promoted_live_by_market": {"KR": 0, "US": 0, "BG": 0},
        "demoted_7d": 0,
        "today_kst": today_kst,
    }

    # --- mutant CANDIDATE 입성 ---
    for prom in _load_validated_promoted(validated_mutants_path):
        name = str(prom.get("name") or "").strip()
        if not name:
            continue
        sid = _stable_mutant_id(name)
        if sid in by_sid:
            continue
        gk = f"INCUBATOR_{name}"
        row = {
            "strategy_id": sid,
            "market": "KR",
            "display_name": name,
            "group_key": gk,
            "state": "CANDIDATE",
            "capital_mult": 0.0,
            "source": "validated_live_mutants",
            "updated_at": now_iso,
            "oos_win_rate": prom.get("oos_win_rate"),
            "oos_avg_return": prom.get("oos_avg_return"),
            "regime_tag": prom.get("regime_tag"),
        }
        by_sid[sid] = row
        stats["discovery_new"] += 1

    # --- Discovery: health → OBSERVING / CANDIDATE ---
    if isinstance(health, dict):
        for hk, hv in health.items():
            if hk == "__meta__" or not isinstance(hv, dict):
                continue
            mkt, gk = parse_health_key(hk)
            if not gk:
                continue
            mp = market_params(lc_cfg, mkt)
            sid = stable_strategy_id(mkt, gk)
            if sid in by_sid:
                row = by_sid[sid]
                row["market"] = mkt
                row["group_key"] = gk
                row["rolling_wr"] = hv.get("rolling_wr")
                row["rolling_pf"] = hv.get("rolling_pf")
                row["n_closed"] = hv.get("n")
            else:
                if not passes_candidate_gate(hv, mp):
                    continue
                row = {
                    "strategy_id": sid,
                    "market": mkt,
                    "group_key": gk,
                    "display_name": gk,
                    "state": "OBSERVING",
                    "capital_mult": 0.0,
                    "source": "health_discovery",
                    "rolling_wr": hv.get("rolling_wr"),
                    "rolling_pf": hv.get("rolling_pf"),
                    "n_closed": hv.get("n"),
                    "updated_at": now_iso,
                }
                by_sid[sid] = row
                stats["discovery_new"] += 1

            st = str(row.get("state") or "").upper()
            if st in ("OBSERVING", "") and passes_candidate_gate(hv, mp):
                row["state"] = "CANDIDATE"
                row["updated_at"] = now_iso
                if not row.get("promote_reason"):
                    row["promote_reason"] = "discovery_candidate"

    # --- 일별 품질 스냅샷 + LIVE 처리 ---
    demote_cutoff = now - timedelta(days=7)
    for sid, row in list(by_sid.items()):
        mkt = str(row.get("market") or "KR").upper()
        gk = str(row.get("group_key") or row.get("display_name") or "").strip()
        if not gk and "|" in sid:
            continue
        mp = market_params(lc_cfg, mkt)
        hv = _health_for_row(health if isinstance(health, dict) else {}, mkt, gk)
        if hv:
            row["rolling_wr"] = hv.get("rolling_wr")
            row["rolling_pf"] = hv.get("rolling_pf")
            row["n_closed"] = hv.get("n")
            below = is_below_live_threshold(hv, mp)
            record_quality_daily(
                sid,
                mkt,
                rolling_wr=float(hv.get("rolling_wr") or 0),
                rolling_pf=float(hv.get("rolling_pf") or 0),
                below_live_threshold=below,
                trade_date=today_kst,
            )
            row["health_miss_streak"] = consecutive_below_live_days(sid)

        st = str(row.get("state") or "").upper()

        # --- Hard-Threshold Auto-Promotion (인큐베이터·ACE) ---
        if (
            hv
            and is_fast_track_group(gk)
            and st in ("OBSERVING", "CANDIDATE", "COOLED", "")
            and passes_hard_threshold_auto_promotion(hv, mp)
        ):
            row["state"] = "LIVE"
            row["capital_mult"] = 1.0
            row["promoted_at"] = row.get("promoted_at") or now_iso
            row["last_promoted_at"] = now_iso
            row["promote_reason"] = "fast_track_pf2"
            row["updated_at"] = now_iso
            row["demote_reason"] = None
            row["observe_only_released"] = True
            stats["promoted_live"] += 1
            stats["fast_track_promoted"] += 1
            mk_stat = mkt if mkt in stats["promoted_live_by_market"] else "KR"
            stats["promoted_live_by_market"][mk_stat] = (
                stats["promoted_live_by_market"].get(mk_stat, 0) + 1
            )
            continue

        if st == "LIVE" and hv:
            # Alpha TTL
            age = _days_since(row.get("promoted_at") or row.get("last_promoted_at"), now)
            half_life = int(mp.get("alpha_half_life_days", 10))
            whipsaw_need = int(mp.get("whipsaw_below_days", 2))
            streak = int(row.get("health_miss_streak", 0) or 0)
            hard_mult = float(hv.get("mult", 1.0) or 1.0) <= 0.0

            demote = False
            reason = ""
            if hard_mult:
                demote = True
                reason = "treasury_hard_cut"
            elif age is not None and age > half_life:
                demote = True
                reason = "alpha_half_life"
            elif streak >= whipsaw_need:
                demote = True
                reason = f"whipsaw_below_live_{streak}d"

            if demote:
                row["state"] = "COOLED"
                row["capital_mult"] = 0.0
                row["last_demoted_at"] = now_iso
                row["demote_reason"] = reason
                row["updated_at"] = now_iso
                stats["demoted_cooled"] += 1
                dt = _parse_iso_dt(row.get("last_demoted_at"))
                if dt and dt >= demote_cutoff.replace(tzinfo=timezone.utc):
                    stats["demoted_7d"] += 1

        elif st == "CANDIDATE" and hv and passes_live_hard_gate(hv, mp):
            row["state"] = "LIVE"
            row["capital_mult"] = 1.0
            row["promoted_at"] = row.get("promoted_at") or now_iso
            row["last_promoted_at"] = now_iso
            row["promote_reason"] = "live_hard_gate"
            row["updated_at"] = now_iso
            row["demote_reason"] = None
            stats["promoted_live"] += 1
            mk = mkt if mkt in stats["promoted_live_by_market"] else "KR"
            stats["promoted_live_by_market"][mk] = stats["promoted_live_by_market"].get(mk, 0) + 1

        elif st == "COOLED":
            cooloff = int(mp.get("cooloff_days", 3))
            since_dem = _days_since(row.get("last_demoted_at"), now)
            if since_dem is not None and since_dem >= cooloff:
                if hv and passes_live_hard_gate(hv, mp):
                    row["state"] = "CANDIDATE"
                    row["capital_mult"] = 0.0
                    row["promote_reason"] = "recovery_reobserve"
                    row["updated_at"] = now_iso
                else:
                    row["state"] = "RETIRED"
                    row["capital_mult"] = 0.0
                    row["demote_reason"] = "cooloff_expired"
                    row["updated_at"] = now_iso
                    stats["retired"] += 1

    out = list(by_sid.values())
    upsert_registry_rows(out, forward_db_path)

    # 집계
    counts = {"OBSERVING": 0, "CANDIDATE": 0, "LIVE": 0, "COOLED": 0, "RETIRED": 0}
    by_market: Dict[str, Dict[str, int]] = {}
    for r in out:
        st = str(r.get("state") or "").upper()
        counts[st] = counts.get(st, 0) + 1
        mk = str(r.get("market") or "KR").upper()
        if mk not in by_market:
            by_market[mk] = {"LIVE": 0, "CANDIDATE": 0, "COOLED": 0, "OBSERVING": 0, "RETIRED": 0}
        if st in by_market[mk]:
            by_market[mk][st] += 1

    stats["counts"] = counts
    stats["by_market"] = by_market
    stats["n_registry"] = len(out)
    return out, stats

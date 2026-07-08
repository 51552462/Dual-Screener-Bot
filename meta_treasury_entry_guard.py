"""
Ch.5 — MetaGovernor ↔ Treasury ↔ 진입 경로 SSOT.

감사관(overseer)은 DEFENSE_LEAK / BLOCK_SOURCE_LEAK 를 감지했으나
주식 forward 진입(try_add_virtual_position)에는 MetaGovernor·Treasury
하드 게이트가 없어 실행 경로와 자본 통제가 어긋날 수 있었다.

설계:
  · 조기 게이트: KILL_SWITCH · META_TREASURY_MODE=DEFENSE · block_trade_sources
  · 그룹 게이트: META_GROUP_KELLY_MULT / META_STRATEGY_HEALTH mult ≤ 0
  · 감사 보강: Governor 신선도 · Treasury zeroed 그룹 · 일관성 이상 탐지
"""
from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pytz


def _cfg_bool(cfg: Optional[Dict[str, Any]], key: str, default: bool) -> bool:
    if not isinstance(cfg, dict):
        return default
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def _normalize_trade_source(src: object) -> str:
    return str(src or "STANDARD").strip().upper()


def _operator_flags(meta: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(meta, Mapping):
        return {}
    fl = meta.get("META_OPERATOR_FLAGS")
    return fl if isinstance(fl, dict) else {}


def _regime_action(meta: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(meta, Mapping):
        return {}
    ra = meta.get("META_REGIME_ACTION")
    return ra if isinstance(ra, dict) else {}


def extract_core_group_name(sig_type: str) -> str:
    """forward/shared.py 와 동일한 그룹명 추출."""
    clean_sig = str(sig_type or "").replace("💀[기각/관찰용] ", "")
    clean_sig = re.sub(r"^\[.*?\]\s*", "", clean_sig)
    return clean_sig.split(" [")[0].strip()


def _safe_mult(raw: object, default: float = 1.0) -> float:
    try:
        if raw is None:
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


def resolve_group_treasury_mult(
    meta: Optional[Mapping[str, Any]],
    core_group_name: str,
    *,
    market: str = "",
) -> Tuple[float, str]:
    """
    그룹 Kelly 승수 — META_GROUP_KELLY_MULT 우선, health 스냅샷 보조.
    Returns (mult, source).
    """
    gk = str(core_group_name or "").strip()
    if not gk:
        return 1.0, "empty_group"

    grp_map = {}
    if isinstance(meta, Mapping):
        raw = meta.get("META_GROUP_KELLY_MULT")
        if isinstance(raw, dict):
            grp_map = raw

    if gk in grp_map:
        return _safe_mult(grp_map[gk], 1.0), "META_GROUP_KELLY_MULT"

    health = meta.get("META_STRATEGY_HEALTH") if isinstance(meta, Mapping) else None
    if isinstance(health, dict):
        mkt = str(market or "").upper()
        best: Optional[float] = None
        for key, hv in health.items():
            if key == "__meta__" or not isinstance(hv, dict):
                continue
            _, _, tail = str(key).rpartition("|")
            gk_key = tail or str(key)
            if gk_key != gk:
                continue
            if mkt:
                mk = str(hv.get("market") or key.split("|")[0] or "").upper()
                if mk and mk != mkt:
                    continue
            m = _safe_mult(hv.get("mult"), 1.0)
            best = m if best is None else min(best, m)
        if best is not None:
            return best, "META_STRATEGY_HEALTH"

    return 1.0, "default"


def summarize_treasury_health(meta: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Treasury 헬스 스냅샷 요약 — Governor _step_treasury 와 감사 교차검증용."""
    health = {}
    if isinstance(meta, Mapping):
        raw = meta.get("META_STRATEGY_HEALTH")
        if isinstance(raw, dict):
            health = raw

    meta_block = health.get("__meta__") if isinstance(health.get("__meta__"), dict) else {}
    min_trades = 10
    try:
        min_trades = int(meta_block.get("treasury_min_trades", 10) or 10)
    except (TypeError, ValueError):
        min_trades = 10

    actionable: List[Dict[str, Any]] = []
    zeroed_names: List[str] = []
    for key, hv in health.items():
        if key == "__meta__" or not isinstance(hv, dict):
            continue
        try:
            n = int(hv.get("n", 0) or 0)
        except (TypeError, ValueError):
            n = 0
        if n < min_trades:
            continue
        m = _safe_mult(hv.get("mult"), 1.0)
        actionable.append(hv)
        if m <= 0.0:
            _, _, tail = str(key).rpartition("|")
            zeroed_names.append(tail or str(key))

    treasury_mode = "NORMAL"
    if isinstance(meta, Mapping):
        treasury_mode = str(meta.get("META_TREASURY_MODE") or "NORMAL").upper()

    return {
        "treasury_mode": treasury_mode,
        "actionable_groups": len(actionable),
        "zeroed_groups": len(zeroed_names),
        "zeroed_group_names": tuple(zeroed_names[:12]),
        "lookback_days": meta_block.get("window_days_kst"),
        "n_rows": meta_block.get("n_rows"),
    }


def governor_freshness(
    meta: Optional[Mapping[str, Any]],
    *,
    stale_hours: float = 24.0,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """MetaGovernor 마지막 실행 시각 대비 신선도."""
    ts_raw = None
    status = "UNKNOWN"
    if isinstance(meta, Mapping):
        ts_raw = meta.get("META_GOVERNOR_LAST_RUN_AT")
        status = str(meta.get("META_GOVERNOR_LAST_RUN_STATUS") or "UNKNOWN")

    if not ts_raw:
        return {
            "last_run_at": None,
            "last_run_status": status,
            "hours_since_run": None,
            "is_stale": status.upper() in ("NEVER", "", "UNKNOWN") or not ts_raw,
            "stale_hours_threshold": stale_hours,
        }

    try:
        ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return {
            "last_run_at": str(ts_raw),
            "last_run_status": status,
            "hours_since_run": None,
            "is_stale": True,
            "stale_hours_threshold": stale_hours,
        }

    ref = now or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    hours = (ref - ts).total_seconds() / 3600.0
    is_stale = hours > float(stale_hours) or status.upper() in ("NEVER", "FAILED")

    return {
        "last_run_at": str(ts_raw),
        "last_run_status": status,
        "hours_since_run": round(hours, 2),
        "is_stale": is_stale,
        "stale_hours_threshold": stale_hours,
    }


def evaluate_meta_global_entry_gate(
    meta: Optional[Mapping[str, Any]],
    trade_source: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    조기 진입 게이트 — trade_source 수준 (그룹명 불필요).

    Returns:
      block_entry, code, reason, kill_switch, treasury_mode
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not _cfg_bool(cfg, "ENABLE_META_TREASURY_ENTRY_GUARD", True):
        return {
            "block_entry": False,
            "code": "disabled",
            "reason": "meta_treasury_guard_disabled",
            "kill_switch": False,
            "treasury_mode": "NORMAL",
        }

    flags = _operator_flags(meta)
    kill = bool(flags.get("KILL_SWITCH"))
    if kill and _cfg_bool(cfg, "ENABLE_META_KILL_SWITCH_BLOCK", True):
        return {
            "block_entry": True,
            "code": "KILL_SWITCH",
            "reason": "MetaGovernor KILL_SWITCH: 신규 진입 차단",
            "kill_switch": True,
            "treasury_mode": str(
                (meta or {}).get("META_TREASURY_MODE") or "NORMAL"
            ).upper(),
        }

    treasury_mode = str(
        (meta or {}).get("META_TREASURY_MODE") or "NORMAL"
    ).upper()
    if (
        treasury_mode == "DEFENSE"
        and _cfg_bool(cfg, "ENABLE_META_TREASURY_DEFENSE_BLOCK", True)
    ):
        health = summarize_treasury_health(meta)
        z = int(health.get("zeroed_groups") or 0)
        return {
            "block_entry": True,
            "code": "TREASURY_DEFENSE",
            "reason": (
                f"META_TREASURY_MODE=DEFENSE (zeroed_groups={z}) — 신규 진입 차단"
            ),
            "kill_switch": False,
            "treasury_mode": treasury_mode,
            "zeroed_groups": z,
        }

    ra = _regime_action(meta)
    block_src = ra.get("block_trade_sources")
    src_norm = _normalize_trade_source(trade_source)
    if isinstance(block_src, list) and block_src:
        blocked = {_normalize_trade_source(x) for x in block_src}
        if src_norm in blocked:
            return {
                "block_entry": True,
                "code": "BLOCK_TRADE_SOURCE",
                "reason": (
                    f"block_trade_sources에 '{src_norm}' 포함 — 진입 차단"
                ),
                "kill_switch": False,
                "treasury_mode": treasury_mode,
            }

    allow_src = ra.get("allow_trade_sources")
    if isinstance(allow_src, list) and allow_src:
        allowed = {_normalize_trade_source(x) for x in allow_src}
        if src_norm not in allowed:
            return {
                "block_entry": True,
                "code": "ALLOW_TRADE_SOURCE_DENY",
                "reason": (
                    f"allow_trade_sources 화이트리스트에 '{src_norm}' 없음"
                ),
                "kill_switch": False,
                "treasury_mode": treasury_mode,
            }

    return {
        "block_entry": False,
        "code": "ok",
        "reason": "",
        "kill_switch": kill,
        "treasury_mode": treasury_mode,
    }


def evaluate_meta_group_entry_gate(
    meta: Optional[Mapping[str, Any]],
    core_group_name: str,
    *,
    market: str = "",
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """그룹 단위 Treasury Kelly 승수 게이트."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not _cfg_bool(cfg, "ENABLE_META_TREASURY_ENTRY_GUARD", True):
        return {
            "block_entry": False,
            "kelly_mult": 1.0,
            "reason": "",
            "group_mult": 1.0,
            "source": "disabled",
        }

    mult, source = resolve_group_treasury_mult(
        meta, core_group_name, market=market
    )
    if mult <= 0.0 and _cfg_bool(cfg, "ENABLE_META_TREASURY_GROUP_ZERO_BLOCK", True):
        return {
            "block_entry": True,
            "kelly_mult": 0.0,
            "reason": (
                f"Treasury 그룹 '{core_group_name}' mult=0 ({source}) — 진입 차단"
            ),
            "group_mult": mult,
            "source": source,
        }

    return {
        "block_entry": False,
        "kelly_mult": mult,
        "reason": "",
        "group_mult": mult,
        "source": source,
    }


def count_zero_group_entry_hits(
    df_entry_today: Any,
    meta: Optional[Mapping[str, Any]],
) -> int:
    """당일 진입 중 Treasury mult=0 그룹 매칭 건수."""
    if df_entry_today is None or getattr(df_entry_today, "empty", True):
        return 0
    if "sig_type" not in getattr(df_entry_today, "columns", []):
        return 0
    n = 0
    for sig in df_entry_today["sig_type"].astype(str):
        gk = extract_core_group_name(sig)
        m, _ = resolve_group_treasury_mult(meta, gk)
        if m <= 0.0:
            n += 1
    return n


def detect_meta_treasury_audit_anomalies(
    *,
    kill_switch_active: bool,
    treasury_mode: str,
    treasury_zeroed_groups: int,
    treasury_actionable_groups: int,
    governor_is_stale: bool,
    governor_hours_since_run: Optional[float],
    trades_entry_today: int,
    trades_closed_today: int,
    win_rate_today_pct: Optional[float],
    catastrophic_clutch_active: bool,
    zero_group_entry_hits: int,
    block_trade_sources: Sequence[str],
    sys_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    """Ch.5 감사 이상 탐지 — overseer detect_audit_anomalies 에서 호출."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    rules = cfg.get("OVERSEER_AUDIT_RULES")
    base = rules if isinstance(rules, dict) else cfg
    try:
        stale_h = float(base.get("META_GOVERNOR_STALE_HOURS", 24))
    except (TypeError, ValueError):
        stale_h = 24.0
    try:
        cat_wr = float(base.get("OVERSEER_WIN_RATE_CATASTROPHIC_PCT", 5.0))
    except (TypeError, ValueError):
        cat_wr = 5.0

    out: List[Dict[str, str]] = []
    n_entry = int(trades_entry_today)
    n_closed = int(trades_closed_today)
    tm = str(treasury_mode or "NORMAL").upper()

    def _push(code: str, severity: str, headline: str, evidence: str) -> None:
        out.append(
            {
                "code": code,
                "severity": severity,
                "headline": headline,
                "evidence": evidence,
            }
        )

    if kill_switch_active and n_entry > 0:
        _push(
            "KILL_SWITCH_LEAK",
            "CRITICAL",
            "KILL_SWITCH 활성인데 당일 신규 진입 발생",
            (
                f"META_OPERATOR_FLAGS.KILL_SWITCH=<b>ON</b> · "
                f"진입 <b>{n_entry}</b>건 — 실행 경로 누출."
            ),
        )

    if zero_group_entry_hits > 0:
        _push(
            "TREASURY_GROUP_ZERO_LEAK",
            "CRITICAL",
            "Treasury mult=0 그룹인데 당일 진입 발생",
            (
                f"zeroed_groups=<b>{treasury_zeroed_groups}</b> · "
                f"당일 zero-group 진입 <b>{zero_group_entry_hits}</b>건."
            ),
        )

    if governor_is_stale and n_entry > 0:
        hrs = (
            f"{governor_hours_since_run:.1f}h"
            if governor_hours_since_run is not None
            else "—"
        )
        _push(
            "GOVERNOR_STALE_WITH_ENTRIES",
            "WARN",
            "MetaGovernor 미갱신/구식 상태에서 당일 진입 발생",
            (
                f"Governor stale (>{stale_h:.0f}h) · 경과 <b>{html.escape(hrs, quote=False)}</b> · "
                f"진입 <b>{n_entry}</b>건 — Treasury/Regime SSOT 신뢰도 저하."
            ),
        )

    if (
        tm == "NORMAL"
        and treasury_zeroed_groups == 0
        and catastrophic_clutch_active
        and n_closed >= 5
        and win_rate_today_pct is not None
        and win_rate_today_pct <= cat_wr
    ):
        _push(
            "TREASURY_CATASTROPHIC_SPLIT",
            "WARN",
            "Treasury NORMAL 이지만 당일 승률 붕괴·클러치 활성",
            (
                f"META_TREASURY_MODE=<b>NORMAL</b> · zeroed=<b>0</b> · "
                f"청산 <b>{n_closed}</b> · 승률 <b>{win_rate_today_pct:.1f}%</b> · "
                f"당일클러치=<b>ON</b>. "
                "<i>Governor Treasury 윈도우·그룹 헬스와 실시간 클러치 정합 점검.</i>"
            ),
        )

    if (
        tm == "NORMAL"
        and treasury_actionable_groups > 0
        and treasury_zeroed_groups == 0
        and n_closed >= 8
        and win_rate_today_pct is not None
        and win_rate_today_pct <= 1.0
        and not catastrophic_clutch_active
    ):
        _push(
            "TREASURY_HEALTH_LAG",
            "WARN",
            "당일 전패인데 Treasury DEFENSE 미전환",
            (
                f"청산 <b>{n_closed}</b> · 승률 <b>{win_rate_today_pct:.1f}%</b> · "
                f"Treasury=<b>NORMAL</b> · actionable=<b>{treasury_actionable_groups}</b>. "
                "<i>비대칭 롤링 윈도우·min_trades 임계로 DEFENSE 지연 가능.</i>"
            ),
        )

    if block_trade_sources and n_entry > 0:
        src = html.escape(",".join(str(x) for x in block_trade_sources[:5]), quote=False)
        _push(
            "BLOCK_SOURCE_LEAK",
            "CRITICAL",
            "차단 trade_source 설정인데 진입 발생",
            f"block_trade_sources=[{src}] · 진입 <b>{n_entry}</b>건.",
        )

    return out


def build_meta_treasury_dossier_extras(
    meta: Optional[Mapping[str, Any]],
    *,
    df_entry_today: Any = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """OverseerAuditDossier 보강 필드 일괄 산출."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    rules = cfg.get("OVERSEER_AUDIT_RULES")
    base = rules if isinstance(rules, dict) else cfg
    try:
        stale_h = float(base.get("META_GOVERNOR_STALE_HOURS", 24))
    except (TypeError, ValueError):
        stale_h = 24.0

    health = summarize_treasury_health(meta)
    fresh = governor_freshness(meta, stale_hours=stale_h)
    flags = _operator_flags(meta)

    return {
        "kill_switch_active": bool(flags.get("KILL_SWITCH")),
        "treasury_zeroed_groups": int(health.get("zeroed_groups") or 0),
        "treasury_actionable_groups": int(health.get("actionable_groups") or 0),
        "treasury_zeroed_group_names": health.get("zeroed_group_names") or (),
        "governor_stale_hours": fresh.get("hours_since_run"),
        "governor_is_stale": bool(fresh.get("is_stale")),
        "zero_group_entry_hits_today": count_zero_group_entry_hits(
            df_entry_today, meta
        ),
    }

"""
Kelly 탄력성(Elasticity) 오버레이 SSOT — Ch.4

문제:
  · DYNAMIC_KELLY × META_GLOBAL = 1% 가 감사 리포트에 고정 표시
  · Ch.2 당일 클러치는 try_add 에만 적용, effective_kelly 표시·NAV 드로다운 미반영
  · NAV -2.65% vs HWM 인데도 유효 Kelly 가 축소되지 않음

설계 (곱셈 오버레이, 보수적):
  · day_clutch  — catastrophic_day_guard (당일 승률 붕괴)
  · nav_dd      — treasury_state NAV vs HWM 드로다운
  · elasticity_mult = day_mult × nav_mult
  · effective_kelly_elastic = base_effective × elasticity_mult
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz

logger = logging.getLogger(__name__)

KELLY_DAY_CLUTCH_STATE_KEY = "KELLY_DAY_CLUTCH_STATE"


def _cfg_f(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        return float(cfg.get(key, default))
    except (TypeError, ValueError):
        return default


def elasticity_thresholds(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    rules = cfg.get("OVERSEER_AUDIT_RULES")
    base = rules if isinstance(rules, dict) else cfg
    return {
        "nav_dd_start_pct": _cfg_f(base, "KELLY_NAV_DD_START_PCT", 2.0),
        "nav_dd_full_pct": _cfg_f(base, "KELLY_NAV_DD_FULL_PCT", 8.0),
        "nav_dd_min_mult": _cfg_f(base, "KELLY_NAV_DD_MIN_MULT", 0.25),
        "inelastic_gap_ratio": _cfg_f(base, "KELLY_INELASTIC_GAP_RATIO", 0.90),
    }


def nav_drawdown_pct(
    nav: Optional[float],
    hwm: Optional[float],
    *,
    mdd_pct: Optional[float] = None,
) -> Optional[float]:
    """현재 NAV 대비 HWM 드로다운 % (양수)."""
    if mdd_pct is not None:
        try:
            return max(0.0, float(mdd_pct))
        except (TypeError, ValueError):
            pass
    try:
        n = float(nav)
        h = float(hwm)
    except (TypeError, ValueError):
        return None
    if h <= 0 or n <= 0:
        return None
    return max(0.0, (h - n) / h * 100.0)


def nav_drawdown_kelly_mult(
    nav: Optional[float],
    hwm: Optional[float],
    *,
    mdd_pct: Optional[float] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    NAV 드로다운 → Kelly 배수.

    dd < start → 1.0
    dd >= full → nav_dd_min_mult
    그 사이 선형 보간
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not cfg.get("ENABLE_KELLY_NAV_DD_OVERLAY", True):
        return {
            "active": False,
            "kelly_mult": 1.0,
            "nav_drawdown_pct": None,
            "reason": "disabled",
        }

    th = elasticity_thresholds(cfg)
    dd = nav_drawdown_pct(nav, hwm, mdd_pct=mdd_pct)
    if dd is None:
        return {
            "active": False,
            "kelly_mult": 1.0,
            "nav_drawdown_pct": None,
            "reason": "no_nav",
        }

    start = max(0.01, th["nav_dd_start_pct"])
    full = max(start + 0.01, th["nav_dd_full_pct"])
    min_mult = max(0.0, min(1.0, th["nav_dd_min_mult"]))

    if dd < start:
        return {
            "active": False,
            "kelly_mult": 1.0,
            "nav_drawdown_pct": dd,
            "reason": f"dd_ok({dd:.2f}%<{start:.1f}%)",
        }

    severity = min(1.0, (dd - start) / (full - start))
    mult = 1.0 - severity * (1.0 - min_mult)
    mult = max(min_mult, min(1.0, mult))
    return {
        "active": True,
        "kelly_mult": mult,
        "nav_drawdown_pct": dd,
        "severity": severity,
        "reason": f"nav_dd:{dd:.2f}%→×{mult:.3f}",
    }


def _today_kst(market: str) -> str:
    tz = pytz.timezone("Asia/Seoul") if str(market).upper() == "KR" else pytz.timezone(
        "America/New_York"
    )
    return datetime.now(tz).strftime("%Y-%m-%d")


def _load_day_clutch_state(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    raw = cfg.get(KELLY_DAY_CLUTCH_STATE_KEY)
    if isinstance(raw, dict):
        return dict(raw)
    try:
        from config_manager import get_config_value

        v = get_config_value(KELLY_DAY_CLUTCH_STATE_KEY)
        return dict(v) if isinstance(v, dict) else {}
    except Exception:
        return {}


def persist_day_clutch_state(
    payload: Dict[str, Any],
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> None:
    """진입 경로에서 당일 클러치 스냅샷 저장 — 리포트/감사 SSOT 폴백."""
    if not isinstance(payload, dict):
        return
    try:
        from config_manager import set_config_value

        set_config_value(KELLY_DAY_CLUTCH_STATE_KEY, payload)
    except Exception as e:
        logger.debug("kelly_elasticity: clutch state persist skip: %s", e)
    if isinstance(sys_config, dict):
        sys_config[KELLY_DAY_CLUTCH_STATE_KEY] = dict(payload)


def resolve_day_clutch_mult(
    *,
    conn: Optional[sqlite3.Connection] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    today_str: Optional[str] = None,
    markets: Tuple[str, ...] = ("KR", "US"),
) -> Dict[str, Any]:
    """
    당일 승률 붕괴 클러치 — KR/US 중 최소(가장 보수적) 배수.
    conn 없으면 KELLY_DAY_CLUTCH_STATE 폴백(당일 일치 시).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not cfg.get("ENABLE_CATASTROPHIC_DAY_CLUTCH", True):
        return {
            "active": False,
            "kelly_mult": 1.0,
            "block_entry": False,
            "reason": "disabled",
            "by_market": {},
        }

    ts = today_str or _today_kst("KR")
    worst_mult = 1.0
    any_active = False
    block = False
    reasons: List[str] = []
    by_mkt: Dict[str, Any] = {}

    own_conn = None
    try:
        from catastrophic_day_guard import evaluate_catastrophic_day_clutch

        use_conn = conn
        if use_conn is None:
            try:
                from forward.shared import DB_PATH

                own_conn = sqlite3.connect(DB_PATH, timeout=30)
                use_conn = own_conn
            except Exception:
                use_conn = None

        if use_conn is not None:
            for mkt in markets:
                ev = evaluate_catastrophic_day_clutch(
                    use_conn, mkt, ts, sys_config=cfg
                )
                by_mkt[mkt] = ev
                mult = float(ev.get("kelly_mult", 1.0) or 1.0)
                if ev.get("active"):
                    any_active = True
                    worst_mult = min(worst_mult, mult)
                    reasons.append(str(ev.get("reason") or mkt))
                if ev.get("block_entry"):
                    block = True

        if not any_active:
            st = _load_day_clutch_state(cfg)
            if str(st.get("as_of", ""))[:10] == ts and st:
                worst_mult = float(st.get("kelly_mult", 1.0) or 1.0)
                any_active = bool(st.get("active"))
                block = block or bool(st.get("block_entry"))
                if st.get("reason"):
                    reasons.append(str(st.get("reason")))
                if st.get("by_market"):
                    by_mkt = dict(st.get("by_market") or {})
    except Exception as ex:
        logger.debug("kelly_elasticity: day clutch resolve skip: %s", ex)
    finally:
        if own_conn is not None:
            own_conn.close()

    return {
        "active": any_active,
        "kelly_mult": worst_mult,
        "block_entry": block,
        "reason": ";".join(reasons) if reasons else "neutral",
        "by_market": by_mkt,
        "as_of": ts,
    }


def nav_overlay_from_market(
    market: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """treasury_state.json 에서 시장별 NAV/HWM 로드."""
    mkt = str(market or "KR").upper()
    try:
        from live_nav_manager import get_market_state

        st = get_market_state(mkt)
        return nav_drawdown_kelly_mult(
            st.get("nav"),
            st.get("hwm"),
            mdd_pct=st.get("mdd_pct"),
            sys_config=sys_config,
        )
    except Exception as ex:
        return {
            "active": False,
            "kelly_mult": 1.0,
            "nav_drawdown_pct": None,
            "reason": f"treasury_skip:{ex}",
        }


def combine_elasticity_mults(
    day_mult: float,
    nav_mult: float,
    *,
    markets_nav: Optional[List[Dict[str, Any]]] = None,
) -> float:
    """보수적 곱셈 — 다중 시장 NAV 오버레이는 최소값."""
    m = max(0.0, min(1.0, float(day_mult))) * max(0.0, min(1.0, float(nav_mult)))
    if markets_nav:
        for nv in markets_nav:
            try:
                nm = float(nv.get("kelly_mult", 1.0) or 1.0)
                m = min(m, max(0.0, min(1.0, nm)))
            except (TypeError, ValueError):
                continue
    return max(0.0, min(1.0, m))


def evaluate_kelly_elasticity_overlay(
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    market: str = "KR",
    conn: Optional[sqlite3.Connection] = None,
    today_str: Optional[str] = None,
    nav: Optional[float] = None,
    hwm: Optional[float] = None,
    mdd_pct: Optional[float] = None,
    markets: Tuple[str, ...] = ("KR", "US"),
) -> Dict[str, Any]:
    """
    전체 탄력성 오버레이 평가 — 감사·리포트·진입 공용.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    day = resolve_day_clutch_mult(
        conn=conn,
        sys_config=cfg,
        today_str=today_str,
        markets=markets,
    )

    nav_overlays: List[Dict[str, Any]] = []
    if nav is not None or hwm is not None:
        nav_overlays.append(
            nav_drawdown_kelly_mult(
                nav, hwm, mdd_pct=mdd_pct, sys_config=cfg
            )
        )
    else:
        for mkt in markets:
            nav_overlays.append(nav_overlay_from_market(mkt, sys_config=cfg))

    nav_mults = [float(x.get("kelly_mult", 1.0) or 1.0) for x in nav_overlays]
    nav_combined = min(nav_mults) if nav_mults else 1.0
    nav_active = any(bool(x.get("active")) for x in nav_overlays)
    nav_dd_vals = [
        x.get("nav_drawdown_pct")
        for x in nav_overlays
        if x.get("nav_drawdown_pct") is not None
    ]
    nav_dd_max = max(nav_dd_vals) if nav_dd_vals else None

    day_mult = float(day.get("kelly_mult", 1.0) or 1.0)
    elasticity_mult = combine_elasticity_mults(
        day_mult, nav_combined, markets_nav=nav_overlays
    )

    return {
        "day_clutch": day,
        "nav_overlays": nav_overlays,
        "day_mult": day_mult,
        "nav_mult": nav_combined,
        "elasticity_mult": elasticity_mult,
        "nav_drawdown_pct": nav_dd_max,
        "active": bool(day.get("active")) or nav_active,
        "block_entry": bool(day.get("block_entry")),
        "reason": (
            f"day×{day_mult:.3f}·nav×{nav_combined:.3f}"
            f"→×{elasticity_mult:.3f}"
        ),
    }


def apply_elasticity_to_effective_kelly(
    effective_kelly: float,
    overlay: Dict[str, Any],
) -> Tuple[float, Dict[str, Any]]:
    """base effective × elasticity_mult."""
    base = max(0.0, float(effective_kelly))
    mult = float(overlay.get("elasticity_mult", 1.0) or 1.0)
    mult = max(0.0, min(1.0, mult))
    out = base * mult
    detail = {
        "effective_pre_overlay": base,
        "effective_post_overlay": out,
        "elasticity_mult": mult,
        "day_mult": overlay.get("day_mult", 1.0),
        "nav_mult": overlay.get("nav_mult", 1.0),
        "nav_drawdown_pct": overlay.get("nav_drawdown_pct"),
        "overlay_active": overlay.get("active", False),
    }
    return out, detail


def evaluate_entry_elasticity_overlays(
    conn: sqlite3.Connection,
    market: str,
    today_str: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    try_add_virtual_position 진입 경로 — 당일+시장 NAV 오버레이 일괄 평가·상태 저장.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    mkt = str(market or "KR").upper()
    overlay = evaluate_kelly_elasticity_overlay(
        sys_config=cfg,
        market=mkt,
        conn=conn,
        today_str=today_str,
        markets=("KR", "US"),
    )
    persist_day_clutch_state(
        {
            "as_of": today_str,
            "active": overlay.get("active"),
            "kelly_mult": overlay.get("day_mult"),
            "elasticity_mult": overlay.get("elasticity_mult"),
            "block_entry": overlay.get("block_entry"),
            "reason": overlay.get("reason"),
            "by_market": (overlay.get("day_clutch") or {}).get("by_market", {}),
            "nav_drawdown_pct": overlay.get("nav_drawdown_pct"),
        },
        sys_config=cfg,
    )
    return overlay


def detect_kelly_inelastic_anomaly(
    *,
    effective_pre: float,
    effective_post: float,
    overlay: Dict[str, Any],
    catastrophic_clutch_active: bool = False,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, str]]:
    """감사 — 탄력성 오버레이가 effective_kelly 에 반영되지 않은 경우."""
    th = elasticity_thresholds(sys_config)
    pre = max(0.0, float(effective_pre))
    post = max(0.0, float(effective_post))
    mult = float(overlay.get("elasticity_mult", 1.0) or 1.0)
    active = bool(overlay.get("active")) or catastrophic_clutch_active

    if not active or mult >= 0.999:
        return None
    if pre <= 0:
        return None

    expected = pre * mult
    if expected <= 0:
        return None
    # 표시 유효가 기대치보다 유의미하게 높으면(오버레이 미반영)
    slack = 1.0 + (1.0 - th["inelastic_gap_ratio"]) * 0.5
    if post > expected * slack + 1e-9:
        return {
            "code": "KELLY_INELASTIC",
            "severity": "CRITICAL",
            "headline": "유효 Kelly가 탄력성 오버레이를 반영하지 않음",
            "evidence": (
                f"표시 유효=<b>{post*100:.2f}%</b> · 기대(×{mult:.3f})="
                f"<b>{expected*100:.2f}%</b> · 사전=<b>{pre*100:.2f}%</b> · "
                f"NAV dd={overlay.get('nav_drawdown_pct') or '—'}%"
            ),
        }
    return None

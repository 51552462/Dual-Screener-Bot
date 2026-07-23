"""
메타 거버너 상태 소비자 — Kelly·포지션 캡 병합 (스캐너 비사용).

system_config.json 과 분리된 meta_governor_state.json 만 읽는다.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

from meta_governor import load_meta_governor_state, meta_state_path
from toxic_antipattern_core import any_toxic_rule_matches

logger = logging.getLogger(__name__)

_META_CACHE: Dict[str, Any] = {"path": None, "mtime": None, "data": None}


def _meta_cache_fingerprint(path: str) -> tuple:
    """JSON mtime + config_kv META_GOVERNOR_STATE version — SQLite-only 갱신도 캐시 무효화."""
    try:
        mtime = os.path.getmtime(path) if os.path.isfile(path) else None
    except OSError:
        mtime = None
    kv_ver = None
    try:
        from config_manager import get_config_value

        raw = get_config_value("META_GOVERNOR_STATE")
        if isinstance(raw, dict):
            kv_ver = (
                raw.get("META_GOVERNOR_LAST_RUN_AT"),
                raw.get("META_REGIME_KEY"),
                raw.get("META_SCHEMA_VERSION"),
            )
    except Exception:
        pass
    return (mtime, kv_ver)


def load_meta_state_resolved(path: Optional[str] = None) -> Dict[str, Any]:
    """파일·config_kv 핑거프린트 기준 소프트 캐시 (try_add 고빈도 호출 대비)."""
    p = path or meta_state_path()
    fp = _meta_cache_fingerprint(p)
    if _META_CACHE["path"] == p and _META_CACHE["mtime"] == fp and isinstance(_META_CACHE["data"], dict):
        return _META_CACHE["data"]
    data = load_meta_governor_state(p)
    _META_CACHE["path"] = p
    _META_CACHE["mtime"] = fp
    _META_CACHE["data"] = data
    return data


def resolve_trading_kelly_base(
    sys_config: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    """실매매·try_add — config UNKNOWN 이어도 Meta·스냅샷 기반 Graceful Kelly 베이스."""
    try:
        from meta_state_store import resolve_config_regime_key
        from regime_kelly_failsafe import resolve_graceful_base_kelly

        rk_cfg = resolve_config_regime_key(sys_config)
        kelly, _reason = resolve_graceful_base_kelly(
            sys_config,
            meta,
            config_regime_unknown=rk_cfg in ("", "UNKNOWN"),
        )
        return float(kelly)
    except Exception:
        return float(sys_config.get("DYNAMIC_KELLY_RISK", 0.01) or 0.01)


def invalidate_meta_state_cache() -> None:
    _META_CACHE["path"] = None
    _META_CACHE["mtime"] = None
    _META_CACHE["data"] = None


def _flags(meta: Dict[str, Any]) -> Dict[str, Any]:
    f = meta.get("META_OPERATOR_FLAGS")
    return f if isinstance(f, dict) else {}


def _regime_action(meta: Dict[str, Any]) -> Dict[str, Any]:
    ra = meta.get("META_REGIME_ACTION")
    if isinstance(ra, dict):
        return ra
    return {}


def apply_meta_weight_bounds_clamp(
    w_s1: float,
    w_s4: float,
    meta: Optional[Dict[str, Any]],
) -> Tuple[float, float]:
    """
    META_REGIME_ACTION 의 weight_s1_bounds / weight_s4_bounds [lo, hi] 로 클램프.
    메타 키가 없거나 bounds 가 비어 있으면 입력 그대로 반환.
    """
    if not meta:
        return float(w_s1), float(w_s4)
    ra = _regime_action(meta)
    out1, out4 = float(w_s1), float(w_s4)
    b1 = ra.get("weight_s1_bounds")
    if isinstance(b1, (list, tuple)) and len(b1) == 2:
        try:
            lo, hi = float(b1[0]), float(b1[1])
            if lo <= hi:
                out1 = min(max(out1, lo), hi)
        except (TypeError, ValueError):
            pass
    b4 = ra.get("weight_s4_bounds")
    if isinstance(b4, (list, tuple)) and len(b4) == 2:
        try:
            lo, hi = float(b4[0]), float(b4[1])
            if lo <= hi:
                out4 = min(max(out4, lo), hi)
        except (TypeError, ValueError):
            pass
    return out1, out4


def apply_meta_kelly_merge(
    kelly_risk_pct: float,
    meta: Optional[Dict[str, Any]],
    *,
    ns_prefix: str,
    core_group_name: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    entry_facts: Optional[Dict[str, Any]] = None,
    sector_mapped: Optional[str] = None,
) -> float:
    """
    승인된 소비자 병합 규칙 (곱셈 + 캡/플로어):

    effective = base
              * META_GLOBAL_KELLY_MULT
              * META_NS_KELLY_MULT[ns_prefix] (없으면 1)
              * META_GROUP_KELLY_MULT[core_group_name] (이름 있을 때만, 없으면 1)
    그 후 META_REGIME_ACTION.kelly_cap / kelly_floor 로 clamp.
    KILL_SWITCH 가 참이면 0.
    오답노트(bbox) 일치 시 (sys_config+facts+sector 제공 시) 즉시 0 — 스캐너 합격 후에도 자본 차단.
    """
    if (
        sys_config is not None
        and entry_facts is not None
        and sector_mapped is not None
        and any_toxic_rule_matches(sys_config, entry_facts, str(sector_mapped))
    ):
        return 0.0

    if meta is None:
        return float(kelly_risk_pct)

    flags = _flags(meta)
    if bool(flags.get("KILL_SWITCH")):
        return 0.0

    # ===========================================================================
    # 👑 [자동 해고 (Auto-Firing) 엔진] 
    # 실무자(로직)의 KPI 점수(승률, 손익비 달성률)가 C급(50% 미만)으로 떨어지면
    # 기계가 스스로 자금줄(Kelly)을 '0'으로 수거하여 실전 투입을 원천 차단합니다.
    # ===========================================================================
    if core_group_name:
        health_dict = meta.get("META_STRATEGY_HEALTH")
        if isinstance(health_dict, dict):
            # 국가 코드(KR/US)가 붙은 키를 포괄적으로 검색
            h_info = health_dict.get(core_group_name) or health_dict.get(f"KR|{core_group_name}") or health_dict.get(f"US|{core_group_name}")
            
            if isinstance(h_info, dict):
                n_trades = int(h_info.get("n", 0))
                
                # 충분한 평가 기회(최소 10회 거래)를 받은 실무자만 심사 대상
                if n_trades >= 10:
                    _pf = float(h_info.get("rolling_pf", 0.0))
                    _wr = float(h_info.get("rolling_wr", 0.0)) * 100.0 
                    
                    pf_target = 2.0
                    wr_target = 60.0
                    
                    pf_ratio = min(100.0, max(0.0, (_pf / pf_target) * 100))
                    wr_ratio = min(100.0, max(0.0, (_wr / wr_target) * 100))
                    achievement = (pf_ratio + wr_ratio) / 2.0
                    
                    # C급 실무자 적발 시 자금 100% 압수
                    if achievement < 50.0:
                        print(f"🚨 [자동 해고 발동] {core_group_name}: KPI {achievement:.1f}% (C급) ➔ 자본 배분 즉시 차단(Kelly=0)")
                        return 0.0
    # ===========================================================================

    out = float(kelly_risk_pct)
    g = float(meta.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    out *= g

    # [켈리 클러치] 예측형 앙상블이 변곡점(1위 국면확률<임계)으로 판단하면, 전역 켈리를
    #   기하급수 축소(0.1~0.3)해 시스템이 스스로 리스크를 닫고 관망(Clutch)한다.
    if sys_config is not None:
        try:
            clutch = sys_config.get("REGIME_TRANSITION_CLUTCH")
            if isinstance(clutch, dict) and clutch.get("active"):
                cm = float(clutch.get("mult", 1.0) or 1.0)
                if 0.0 < cm < 1.0:
                    out *= cm
        except (TypeError, ValueError):
            pass

    # [진화형 둠스데이 형상변환 감쇠] GlobalScore × 동적 γ → 켈리 멱지수 감쇠.
    if sys_config is not None:
        try:
            from doomsday_dampener import apply_doomsday_dampening
            # 👑 [수정] 둠스데이 엔진에 현재 로직의 이름(core_group_name)을 전달하여 비대칭 밸브를 가동시킵니다.
            out = apply_doomsday_dampening(out, sys_config=sys_config, meta=meta, sig_type=str(core_group_name or ""))
        except Exception:
            pass

    ns_map = meta.get("META_NS_KELLY_MULT")
    if isinstance(ns_map, dict) and ns_prefix in ns_map:
        try:
            out *= float(ns_map[ns_prefix])
        except (TypeError, ValueError):
            logger.warning("META_NS_KELLY_MULT[%s] invalid, skip", ns_prefix)

    grp_map = meta.get("META_GROUP_KELLY_MULT")
    if core_group_name and isinstance(grp_map, dict) and core_group_name in grp_map:
        try:
            out *= float(grp_map[core_group_name])
        except (TypeError, ValueError):
            logger.warning("META_GROUP_KELLY_MULT[%s] invalid, skip", core_group_name)
    # ===========================================================================
    # 👑 [진화적 자본 배분 (Darwinian Capital Allocation)]
    # 기계가 예견한 국면(Regime)과, 해당 돌연변이의 '생존 유전자'가 일치하면 자본을 쏟아붓고, 
    # 불일치하면 가차 없이 자본을 회수하여 말려 죽입니다.
    # ===========================================================================
    current_regime = str(meta.get("META_REGIME_KEY", "")).upper()
    sig_upper = str(core_group_name or "").upper()
    
    if "BEAR" in current_regime or "HIGH_VOL" in current_regime:
        if "BEAR_ADAPTED" in sig_upper:
            out *= 1.5   # 📉 하락장 맞춤형 진화 개체에게 자본 1.5배 밀어주기
        elif "BULL_ADAPTED" in sig_upper:
            out *= 0.1   # 📉 하락장에 상승장 템플릿이 나대면 자본 90% 즉각 회수 (몰살 방지)
            
    elif "BULL" in current_regime:
        if "BULL_ADAPTED" in sig_upper:
            out *= 1.5   # 📈 상승장 맞춤형 진화 개체에게 자본 1.5배 밀어주기
        elif "BEAR_ADAPTED" in sig_upper:
            out *= 0.2   # 📈 상승장에 하락장 템플릿이 나오면 방어력이 과잉이므로 자본 80% 회수
            
    # 👑 [추가] 횡보장(CHOP/SIDEWAYS) 자본 편대 시너지 완성 (누수 복구)
    elif "CHOP" in current_regime or "SIDEWAYS" in current_regime:
        if "CHOP_ADAPTED" in sig_upper:
            out *= 1.5   # 👻 횡보장 전용 타이트한 유전자에게 자본 1.5배 밀어주기 (박스권 휩쏘 사냥)
        elif "BULL_ADAPTED" in sig_upper:
            out *= 0.3   # 👻 횡보장에서 돌파 매매(BULL)를 시도하면 가짜 돌파(Bull Trap)에 당하므로 자금 70% 압수
            
        # 횡보장에서는 돌파/모멘텀(S1) 템플릿의 자금도 구조적으로 절반(0.5)으로 억제합니다.
        if "S1" in sig_upper and "CHOP_ADAPTED" not in sig_upper:
            out *= 0.5   
    # ===========================================================================

    # [Regime Specialization Item 3] MAB group mult × regime_tag 호환 overlay
    if sys_config is not None:
        try:
            from evolution.regime_logic_crossmatrix import (
                evaluate_regime_tag_quarantine,
                resolve_regime_tag_for_signal,
            )

            _rtag = resolve_regime_tag_for_signal(
                sys_config,
                sig_type=core_group_name,
                group_key=core_group_name,
                facts=entry_facts if isinstance(entry_facts, dict) else None,
                meta_state=meta,
            )
            if _rtag:
                _rq = evaluate_regime_tag_quarantine(
                    meta.get("META_REGIME_KEY"),
                    _rtag,
                    sys_config=sys_config,
                )
                if _rq.get("quarantined"):
                    if _rq.get("reject_entry"):
                        return 0.0
                    if "kelly_mult" in _rq:
                        out *= float(_rq["kelly_mult"])
        except Exception:
            pass

    ra = _regime_action(meta)
    cap = ra.get("kelly_cap")
    floor = ra.get("kelly_floor")
    try:
        fval = float(floor) if floor is not None else None
    except (TypeError, ValueError):
        fval = None
    try:
        cval = float(cap) if cap is not None else None
    except (TypeError, ValueError):
        cval = None
    # ===========================================================================
    # 👑 [승자 독식 1] VIP 하이패스 자본 할당 (Limit-Break)
    # 사령탑이 켈리 캡(Cap)을 강제로 억눌렀더라도, 수급/공시/숏스퀴즈 등 강력한 
    # 펀더멘털 뒷배(Cross-Validation)를 가진 '진짜 대장주'라면 
    # 이 캡(Cap)을 박살내고(Bypass) 자본을 2배로 강제 투입합니다.
    # ===========================================================================
    is_vip_diamond = False
    if isinstance(entry_facts, dict):
        flow = float(entry_facts.get("flow_bonus", 0.0))
        short = float(entry_facts.get("short_net", 0.0))
        dart = float(entry_facts.get("dart_net", 0.0))
        sig = str(core_group_name or "")
        
        # 수급, 숏커버링, DART 공시, 또는 세력매집 태그 중 하나라도 강력하게 존재한다면
        if flow > 0.0 or short > 0.0 or dart > 0.0 or "세력매집_교차검증" in sig:
            is_vip_diamond = True

    if fval is not None:
        out = max(out, fval)
        
    if cval is not None:
        if is_vip_diamond:
            # VIP는 락다운(Cap)을 무시하고, 오히려 기존 한도의 2배까지 자금을 밀어줍니다.
            out = min(out, cval * 2.0) 
            logger.info(f"💎 [VIP 하이패스] {core_group_name}: 다이아몬드 감별 통과! 락다운 무시 및 자본 2배 할당 (Kelly={out:.4f})")
        else:
            out = min(out, cval) # 일반 종목은 그대로 락다운 적용
            
    out = max(out, 0.0)
    # ===========================================================================

    # [Ch.4 Kelly 탄력성] 당일 클러치 × NAV 드로다운 오버레이
    if sys_config is not None:
        try:
            from kelly_elasticity_overlay import (
                apply_elasticity_to_effective_kelly,
                evaluate_kelly_elasticity_overlay,
            )

            _ov = evaluate_kelly_elasticity_overlay(sys_config=sys_config, market="KR")
            out, _ = apply_elasticity_to_effective_kelly(out, _ov)
        except Exception:
            pass

    # [Re-Evolution Warm-Start] 불사조 복귀 그룹 — Base Confidence Kelly 배수
    if core_group_name:
        try:
            from re_evolution_warm_start import apply_warm_start_kelly_scaler

            mkt = "KR"
            if isinstance(entry_facts, dict):
                mkt = str(entry_facts.get("market") or entry_facts.get("MARKET") or mkt).upper()
            out = apply_warm_start_kelly_scaler(
                out,
                meta,
                market=mkt,
                group_key=str(core_group_name),
                sys_config=sys_config,
            )
        except Exception:
            pass

    return float(out)


def effective_max_position_pct(sys_config: Dict[str, Any], meta: Optional[Dict[str, Any]]) -> float:
    """min(sys MAX_POSITION_PCT, META_MAX_POSITION_PCT) — 메타가 None 이면 sys 만."""
    base = float(sys_config.get("MAX_POSITION_PCT", 0.25) or 0.25)
    if not meta:
        return base
    m = meta.get("META_MAX_POSITION_PCT")
    if m is None:
        return base
    try:
        return min(base, float(m))
    except (TypeError, ValueError):
        return base

"""
Live NAV Manager — 기관급 동적 자본(NAV) 엔진의 SSOT.

40만 원 고정 폴백을 폐기하고, 시장별 '진짜 누적 복리 자산(True NAV)'을 단일 상태 파일
(treasury_state.json)로 관리한다. 핵심 모델:

    E_t = E_{t-1} × (1 + f · R_t)      f = 그 거래 당시 유효 켈리, R_t = final_ret/100

- KR 기준 자본 300,000,000원(₩),  US 기준 자본 300,000달러($) — 완전 분리.
- 포지션 노출액(Notional)은 항상 `Live NAV × 유효 켈리`로 산출(평면 폴백 없음).
- 청산 시마다 실현 손익이 NAV 에 즉시 동기화되고, HWM/MDD 가 갱신된다.

이 모듈은 weekly_flow_pnl(노출 산출) / report_state_binder([1/9] 리포트) /
scripts/calculate_historical_nav(소급 복원) / forward.ledger(청산 훅) 의 단일 진입점이다.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

try:
    from factory_data_paths import factory_data_dir
except Exception:  # pragma: no cover - 경로 모듈 부재 시 홈 폴백
    def factory_data_dir() -> str:  # type: ignore[misc]
        d = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot")
        os.makedirs(d, exist_ok=True)
        return d


TREASURY_STATE_FILENAME = "treasury_state.json"

# 시장별 기준 자본(완전 분리).
BASE_CAPITAL: Dict[str, float] = {
    "KR": 300_000_000.0,  # 3억 원
    "US": 300_000.0,      # 30만 달러
}

# 통화 포맷 SSOT — 리포트 팩트시트가 그대로 사용.
CURRENCY: Dict[str, Dict[str, Any]] = {
    "KR": {"code": "KRW", "symbol": "₩", "suffix": "원", "decimals": 0},
    "US": {"code": "USD", "symbol": "$", "suffix": "달러", "decimals": 2},
}

DEFAULT_EFFECTIVE_KELLY = 0.02   # 켈리 미해결 시 보수적 기본(=sim_kelly_risk_pct 기본값과 동일)
MIN_EFFECTIVE_KELLY = 0.001
MAX_EFFECTIVE_KELLY = 0.50

# Axis 2 — Inverse Sleeve RL (Self-Evolution Hedge Engine)
INVERSE_SLEEVE_LOOKBACK_DAYS = 5
INVERSE_SLEEVE_MAX_EVENTS = 48
INVERSE_SIG_MARKER_NAV = "[INVERSE_ETF]"

_LOCK = threading.RLock()


def normalize_market(market: str) -> str:
    m = str(market or "").upper().strip()
    return "US" if "US" in m else "KR"


def base_capital_for(market: str) -> float:
    return BASE_CAPITAL.get(normalize_market(market), BASE_CAPITAL["KR"])


def currency_for(market: str) -> Dict[str, Any]:
    return CURRENCY.get(normalize_market(market), CURRENCY["KR"])


def treasury_state_path() -> str:
    return os.path.join(factory_data_dir(), TREASURY_STATE_FILENAME)


def _empty_inverse_sleeve_state() -> Dict[str, Any]:
    """Axis 2 — 인버스 슬리브 rolling 실현 PnL (테일 펀드 격리, 롱 NAV 와 분리)."""
    return {
        "events": [],
        "updated_at": None,
    }


def _empty_market_state(market: str) -> Dict[str, Any]:
    base = base_capital_for(market)
    cur = currency_for(market)
    return {
        "currency": cur["code"],
        "base_capital": base,
        "nav": base,
        "hwm": base,
        "mdd_pct": 0.0,
        "n_closed": 0,
        "last_exit_date": None,
        "inverse_sleeve": _empty_inverse_sleeve_state(),
        "updated_at": None,
    }


def _default_state() -> Dict[str, Any]:
    return {
        "schema": "treasury_state.v1",
        "updated_at": None,
        "KR": _empty_market_state("KR"),
        "US": _empty_market_state("US"),
    }


def load_treasury_state() -> Dict[str, Any]:
    """treasury_state.json 로드 — 없거나 손상 시 기준 자본으로 초기화된 상태 반환."""
    path = treasury_state_path()
    if not os.path.isfile(path):
        return _default_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_state()
        # 시장 키 보정(누락 시 기본 주입)
        for mkt in ("KR", "US"):
            if not isinstance(data.get(mkt), dict):
                data[mkt] = _empty_market_state(mkt)
            else:
                base = _empty_market_state(mkt)
                base.update({k: v for k, v in data[mkt].items() if v is not None})
                if not isinstance(base.get("inverse_sleeve"), dict):
                    base["inverse_sleeve"] = _empty_inverse_sleeve_state()
                data[mkt] = base
        return data
    except (OSError, json.JSONDecodeError, ValueError):
        return _default_state()


def save_treasury_state(state: Dict[str, Any]) -> bool:
    """원자적 저장(tmp → replace). 동시성 안전."""
    path = treasury_state_path()
    state = dict(state)
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with _LOCK:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd, tmp = tempfile.mkstemp(
                prefix=".treasury_", suffix=".json", dir=os.path.dirname(path)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
        return True
    except OSError:
        return False


def get_market_state(market: str) -> Dict[str, Any]:
    mkt = normalize_market(market)
    return load_treasury_state().get(mkt, _empty_market_state(mkt))


def live_nav(market: str) -> float:
    """현재 시장 NAV. 상태가 없으면 기준 자본."""
    st = get_market_state(market)
    try:
        v = float(st.get("nav", base_capital_for(market)))
        return v if v > 0 else base_capital_for(market)
    except (TypeError, ValueError):
        return base_capital_for(market)


def resolve_effective_kelly(
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    """
    현재 유효 켈리 비중 f = DYNAMIC_KELLY_RISK × META_GLOBAL_KELLY_MULT, 레짐 cap/floor 클램프.
    sys_config 미지정 시 config 로드 시도. 실패하면 보수적 기본값.
    """
    cfg = sys_config
    if not isinstance(cfg, dict):
        try:
            from config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = {}
    try:
        base = float(cfg.get("DYNAMIC_KELLY_RISK", DEFAULT_EFFECTIVE_KELLY) or DEFAULT_EFFECTIVE_KELLY)
    except (TypeError, ValueError):
        base = DEFAULT_EFFECTIVE_KELLY
    g = 1.0
    if isinstance(meta, dict):
        try:
            g = float(meta.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
        except (TypeError, ValueError):
            g = 1.0
    else:
        try:
            g = float(cfg.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
        except (TypeError, ValueError):
            g = 1.0
    eff = base * g
    if eff <= 0.0:
        eff = DEFAULT_EFFECTIVE_KELLY
    eff = float(min(MAX_EFFECTIVE_KELLY, max(MIN_EFFECTIVE_KELLY, eff)))
    try:
        from kelly_elasticity_overlay import (
            apply_elasticity_to_effective_kelly,
            evaluate_kelly_elasticity_overlay,
        )

        _ov = evaluate_kelly_elasticity_overlay(
            sys_config=cfg,
            market=market,
        )
        eff, _ = apply_elasticity_to_effective_kelly(eff, _ov)
    except Exception:
        pass
    return float(eff)


def live_notional(
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    """포지션 노출액 = Live NAV × 유효 켈리. (40만 평면 폴백을 대체)"""
    return live_nav(market) * resolve_effective_kelly(market, sys_config, meta)


def _apply_pnl_to_market_state(mst: Dict[str, Any], net_pnl: float, *, exit_date: Optional[str]) -> Dict[str, Any]:
    base = float(mst.get("base_capital", 0.0) or 0.0)
    nav = float(mst.get("nav", base) or base)
    hwm = float(mst.get("hwm", nav) or nav)
    mdd_pct = float(mst.get("mdd_pct", 0.0) or 0.0)
    n_closed = int(mst.get("n_closed", 0) or 0)

    nav = max(0.0, nav + float(net_pnl))
    hwm = max(hwm, nav)
    if hwm > 0:
        dd = (hwm - nav) / hwm * 100.0
        mdd_pct = max(mdd_pct, dd)
    n_closed += 1

    mst = dict(mst)
    mst.update(
        {
            "nav": nav,
            "hwm": hwm,
            "mdd_pct": mdd_pct,
            "n_closed": n_closed,
            "last_exit_date": exit_date or mst.get("last_exit_date"),
        }
    )
    return mst


def apply_realized_pnl(
    market: str, net_pnl: float, *, exit_date: Optional[str] = None
) -> Dict[str, Any]:
    """실현 손익(통화 절대액)을 NAV 에 즉시 반영하고 HWM/MDD 갱신 후 저장."""
    mkt = normalize_market(market)
    with _LOCK:
        state = load_treasury_state()
        state[mkt] = _apply_pnl_to_market_state(
            state.get(mkt, _empty_market_state(mkt)), net_pnl, exit_date=exit_date
        )
        save_treasury_state(state)
        return state[mkt]


def record_closure(
    market: str,
    *,
    final_ret_pct: float,
    kelly_pct: Optional[float] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    exit_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    청산 1건을 NAV 에 복리 반영하는 Live Update Hook.
    net_pnl = NAV × f × (R/100),  f = kelly_pct(있으면) 또는 현재 유효 켈리.
    """
    mkt = normalize_market(market)
    try:
        ret = float(final_ret_pct)
    except (TypeError, ValueError):
        return get_market_state(mkt)
    f = None
    if kelly_pct is not None:
        try:
            f = float(kelly_pct)
        except (TypeError, ValueError):
            f = None
    if f is None or f <= 0.0:
        f = resolve_effective_kelly(mkt, sys_config, meta)
    f = float(min(MAX_EFFECTIVE_KELLY, max(MIN_EFFECTIVE_KELLY, f)))
    with _LOCK:
        nav = live_nav(mkt)
        net_pnl = nav * f * (ret / 100.0)
        return apply_realized_pnl(mkt, net_pnl, exit_date=exit_date)


def overwrite_market_state(
    market: str,
    *,
    nav: float,
    hwm: float,
    mdd_pct: float,
    n_closed: int,
    last_exit_date: Optional[str],
) -> Dict[str, Any]:
    """소급 복원(Historical Replay) 결과를 시장 상태로 확정 기록."""
    mkt = normalize_market(market)
    cur = currency_for(mkt)
    with _LOCK:
        state = load_treasury_state()
        state[mkt] = {
            "currency": cur["code"],
            "base_capital": base_capital_for(mkt),
            "nav": float(nav),
            "hwm": float(hwm),
            "mdd_pct": float(mdd_pct),
            "n_closed": int(n_closed),
            "last_exit_date": last_exit_date,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        save_treasury_state(state)
        return state[mkt]


def format_currency(market: str, value: float, *, with_symbol: bool = True) -> str:
    """팩트시트용 통화 포맷 — KR ₩(정수), US $(소수 2자리)."""
    cur = currency_for(market)
    dec = int(cur["decimals"])
    try:
        v = float(value)
    except (TypeError, ValueError):
        v = 0.0
    body = f"{v:,.{dec}f}"
    if not with_symbol:
        return body
    return f"{cur['symbol']}{body}"


def _prune_inverse_sleeve_events(
    events: List[Dict[str, Any]],
    *,
    lookback_days: int = INVERSE_SLEEVE_LOOKBACK_DAYS,
) -> List[Dict[str, Any]]:
    cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    kept: List[Dict[str, Any]] = []
    for ev in events or []:
        if not isinstance(ev, dict):
            continue
        d = str(ev.get("exit_date") or "")[:10]
        if d and d >= cutoff:
            kept.append(ev)
    if len(kept) > INVERSE_SLEEVE_MAX_EVENTS:
        kept = kept[-INVERSE_SLEEVE_MAX_EVENTS:]
    return kept


def record_inverse_sleeve_closure(
    market: str,
    *,
    final_ret_pct: float,
    invest_amount: float,
    exit_date: Optional[str] = None,
    sig_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Axis 2 — 인버스 슬리브 청산 1건을 treasury_state rolling log 에 기록.

    롱 NAV(record_closure)와 분리 — 테일 펀드 인버스 실현손익만 RL 입력으로 축적.
    """
    mkt = normalize_market(market)
    try:
        ret = float(final_ret_pct)
    except (TypeError, ValueError):
        ret = 0.0
    try:
        inv = float(invest_amount)
    except (TypeError, ValueError):
        inv = 0.0
    if inv <= 0:
        inv = 0.0
    net_pnl_abs = inv * (ret / 100.0)
    exit_d = str(exit_date or datetime.now().strftime("%Y-%m-%d"))[:10]
    event = {
        "exit_date": exit_d,
        "final_ret_pct": round(ret, 4),
        "invest_amount": round(inv, 2),
        "net_pnl_abs": round(net_pnl_abs, 2),
        "sig_type": str(sig_type or "")[:200],
    }
    with _LOCK:
        state = load_treasury_state()
        mst = dict(state.get(mkt, _empty_market_state(mkt)))
        sleeve = mst.get("inverse_sleeve")
        if not isinstance(sleeve, dict):
            sleeve = _empty_inverse_sleeve_state()
        events = list(sleeve.get("events") or [])
        events.append(event)
        events = _prune_inverse_sleeve_events(events)
        sleeve = dict(sleeve)
        sleeve["events"] = events
        sleeve["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mst["inverse_sleeve"] = sleeve
        state[mkt] = mst
        save_treasury_state(state)
        return sleeve


def get_inverse_sleeve_rl_stats(
    market: str,
    *,
    lookback_days: int = INVERSE_SLEEVE_LOOKBACK_DAYS,
) -> Dict[str, Any]:
    """
    Axis 2 — live_nav_manager rolling log → RL 판정용 통계 (읽기 전용).

    반환: n_closed, weighted_ret_pct, total_invest, total_net_pnl_abs, verdict
    """
    mkt = normalize_market(market)
    out: Dict[str, Any] = {
        "market": mkt,
        "source": "live_nav_manager",
        "lookback_days": int(lookback_days),
        "n_closed": 0,
        "weighted_ret_pct": 0.0,
        "total_invest": 0.0,
        "total_net_pnl_abs": 0.0,
        "verdict": "insufficient",
    }
    st = get_market_state(mkt)
    sleeve = st.get("inverse_sleeve") if isinstance(st.get("inverse_sleeve"), dict) else {}
    events = _prune_inverse_sleeve_events(
        list(sleeve.get("events") or []),
        lookback_days=lookback_days,
    )
    if not events:
        return out

    weighted_sum = 0.0
    invest_sum = 0.0
    pnl_sum = 0.0
    for ev in events:
        try:
            ret = float(ev.get("final_ret_pct", 0.0) or 0.0)
            inv = float(ev.get("invest_amount", 0.0) or 0.0)
            pnl = float(ev.get("net_pnl_abs", inv * ret / 100.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if inv <= 0:
            inv = 1.0
        weighted_sum += ret * inv
        invest_sum += inv
        pnl_sum += pnl

    out["n_closed"] = len(events)
    out["total_invest"] = round(invest_sum, 2)
    out["total_net_pnl_abs"] = round(pnl_sum, 2)
    if invest_sum <= 0:
        return out

    wret = weighted_sum / invest_sum
    out["weighted_ret_pct"] = round(float(wret), 4)
    if wret > 1e-9:
        out["verdict"] = "profitable"
    else:
        out["verdict"] = "whipsaw"
    return out


def reset_inverse_sleeve_rl(market: Optional[str] = None) -> Dict[str, Any]:
    """
    Axis 3 — V-Recovery RL amnesia: rolling inverse sleeve 이벤트 전량 삭제.

    market=None 이면 KR/US 모두 reset.
    """
    with _LOCK:
        state = load_treasury_state()
        targets = [normalize_market(market)] if market else ["KR", "US"]
        cleared: list[str] = []
        for mkt in targets:
            mst = dict(state.get(mkt, _empty_market_state(mkt)))
            mst["inverse_sleeve"] = _empty_inverse_sleeve_state()
            state[mkt] = mst
            cleared.append(mkt)
        save_treasury_state(state)
        return {"cleared_markets": cleared, "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


def is_inverse_trade_sig(sig_type: Any) -> bool:
    s = str(sig_type or "")
    return INVERSE_SIG_MARKER_NAV in s


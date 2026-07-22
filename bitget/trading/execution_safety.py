"""
Execution safety gate chain — every live order must pass in order:

  1. ENABLE_REAL_EXECUTION (default false)
  2. REAL_EXECUTION_DRY_RUN (default true)
  3. MetaGovernor KILL_SWITCH
  4. GLOBAL_CIRCUIT_BREAKER (paper + live parity)
  5. OMS orphan active (exchange-only inventory — block new entries)
  6. Portfolio NAV drawdown (reduce → block → halt)
  7. Portfolio gross notional cap (open book / NAV — block new entries)
  8. Tail-risk reserve (underfund size / empty+DD block — never flatten)
  9. Doomsday DEFCON (≤ block level — block new LONG only; size dampen)
 10. BTC-proxy concentration (high-β same-side cluster — block new entries)
 11. Bad-tick / flash-crash price sanity (OHLCV outlier — block new entries)
 12. Pre-trade slippage gate (WS orderbook spread)
 13. Leverage / margin manager (futures — MAX_LEVERAGE in resolve_leverage)
 14. OMS market order (oms_core — defense-in-depth risk checks)

NAV / orphan / gross / tail / doomsday / concentration / price-sanity stages
never auto-flatten — block/shrink new entries only.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from bitget.governance.meta_consumer import load_meta_state_resolved
from bitget.infra.memory_policy import (
    DEFAULT_MAX_LEVERAGE,
    GROSS_NOTIONAL_MAX_PCT,
    NAV_DD_ALERT_MIN_INTERVAL_SEC,
    NAV_DD_BLOCK_PCT,
    NAV_DD_HALT_PCT,
    NAV_DD_REDUCE_PCT,
    NAV_DD_REDUCE_SIZE_MULT,
)
from bitget.trading.slippage_guard import run_pre_trade_gate

_NAV_HALT_ALERT_MONO: float = 0.0


class ExecutionGateOutcome(str, Enum):
    EXECUTION_DISABLED = "execution_disabled"
    DRY_RUN = "dry_run"
    META_BLOCKED = "meta_blocked"
    CIRCUIT_BLOCKED = "circuit_blocked"
    ORPHAN_BLOCKED = "orphan_blocked"
    NAV_BLOCKED = "nav_blocked"
    GROSS_BLOCKED = "gross_blocked"
    TAIL_RISK_BLOCKED = "tail_risk_blocked"
    DOOMSDAY_BLOCKED = "doomsday_blocked"
    CONCENTRATION_BLOCKED = "concentration_blocked"
    PRICE_SANITY_BLOCKED = "price_sanity_blocked"
    SLIPPAGE_BLOCKED = "slippage_blocked"
    CATASTROPHIC_BLOCKED = "catastrophic_blocked"  # [신규 추가] 승률 붕괴 차단 상태
    CLIMAX_KILL_BLOCKED = "climax_kill_blocked"    # [아키텍트 수술] 메가 트렌드 킬스위치 상태 추가
    APPROVED = "approved"

@dataclass
class GateResult:
    outcome: ExecutionGateOutcome
    message: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def proceed_to_exchange(self) -> bool:
        return self.outcome == ExecutionGateOutcome.APPROVED

    @property
    def is_dry_run(self) -> bool:
        return self.outcome == ExecutionGateOutcome.DRY_RUN

    @property
    def is_blocked(self) -> bool:
        return self.outcome in (
            ExecutionGateOutcome.EXECUTION_DISABLED,
            ExecutionGateOutcome.META_BLOCKED,
            ExecutionGateOutcome.CIRCUIT_BLOCKED,
            ExecutionGateOutcome.ORPHAN_BLOCKED,
            ExecutionGateOutcome.NAV_BLOCKED,
            ExecutionGateOutcome.GROSS_BLOCKED,
            ExecutionGateOutcome.TAIL_RISK_BLOCKED,
            ExecutionGateOutcome.DOOMSDAY_BLOCKED,
            ExecutionGateOutcome.CONCENTRATION_BLOCKED,
            ExecutionGateOutcome.PRICE_SANITY_BLOCKED,
            ExecutionGateOutcome.SLIPPAGE_BLOCKED,
            ExecutionGateOutcome.CATASTROPHIC_BLOCKED,  # [신규 추가]
        )


def meta_kill_switch_active() -> bool:
    try:
        st = load_meta_state_resolved()
        fl = st.get("META_OPERATOR_FLAGS") or {}
        return bool(fl.get("KILL_SWITCH"))
    except Exception:
        return False


def global_circuit_breaker_active(cfg: dict) -> bool:
    return str((cfg or {}).get("GLOBAL_CIRCUIT_BREAKER", "OFF") or "OFF").strip().upper() == "ON"


def oms_orphan_active(cfg: dict) -> bool:
    """True when reconciliation marked exchange-only positions as active."""
    return str((cfg or {}).get("OMS_ORPHAN_ACTIVE", "OFF") or "OFF").strip().upper() == "ON"


def _cfg_float(cfg: dict, key: str, default: float) -> float:
    try:
        raw = (cfg or {}).get(key, default)
        if raw is None or raw == "":
            return float(default)
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def portfolio_mdd_pct() -> float:
    try:
        from bitget.live_nav_manager import portfolio_nav_snapshot

        snap = portfolio_nav_snapshot()
        return float(snap.get("mdd_pct") or 0.0)
    except Exception:
        return 0.0


def nav_entry_blocked(cfg: dict) -> bool:
    """True when portfolio MDD reaches block/halt — OMS defense helper."""
    mdd = portfolio_mdd_pct()
    block = _cfg_float(cfg, "NAV_DD_BLOCK_PCT", NAV_DD_BLOCK_PCT)
    return mdd >= block


def portfolio_open_gross_usdt() -> float:
    """Sum of OPEN sim_kelly_invest across spot+futures (virtual book SSOT)."""
    try:
        from bitget.infra.bounded_reads import forward_open_gross_notional_sum_sql
        from bitget.infra.data_paths import market_data_db_path
        from bitget.infra.shared_db_connector import get_connection

        q, p = forward_open_gross_notional_sum_sql()
        conn = get_connection(market_data_db_path())
        try:
            row = conn.execute(q, p).fetchone()
            return float((row[0] if row else 0.0) or 0.0)
        finally:
            conn.close()
    except Exception:
        return 0.0


def portfolio_gross_snapshot(cfg: Optional[dict] = None) -> dict[str, Any]:
    """Open gross notional vs portfolio NAV — shared by live gates + paper ledger."""
    try:
        from bitget.live_nav_manager import portfolio_nav_snapshot

        nav_snap = portfolio_nav_snapshot()
        nav = float(nav_snap.get("nav") or 0.0)
    except Exception as e:
        return {
            "nav": 0.0,
            "gross_usdt": 0.0,
            "gross_pct": 0.0,
            "error": str(e)[:120],
        }
    gross = portfolio_open_gross_usdt()
    gross_pct = (gross / nav * 100.0) if nav > 0 else (0.0 if gross <= 0 else 9999.0)
    max_pct = _cfg_float(cfg or {}, "GROSS_NOTIONAL_MAX_PCT", GROSS_NOTIONAL_MAX_PCT)
    return {
        "nav": nav,
        "gross_usdt": round(gross, 4),
        "gross_pct": round(gross_pct, 4),
        "gross_notional_max_pct": max_pct,
    }


def gross_entry_blocked(cfg: dict) -> bool:
    """True when open gross notional / NAV reaches configured cap (≤0 pct = disabled)."""
    max_pct = _cfg_float(cfg, "GROSS_NOTIONAL_MAX_PCT", GROSS_NOTIONAL_MAX_PCT)
    if max_pct <= 0:
        return False
    snap = portfolio_gross_snapshot(cfg)
    if snap.get("error"):
        return False  # soft-pass on read failure (hot path must not crash)
    return float(snap.get("gross_pct") or 0.0) >= max_pct


def _maybe_nav_halt_alert(mdd: float, snap: dict[str, Any]) -> None:
    global _NAV_HALT_ALERT_MONO
    now = time.monotonic()
    if now - _NAV_HALT_ALERT_MONO < float(NAV_DD_ALERT_MIN_INTERVAL_SEC):
        return
    _NAV_HALT_ALERT_MONO = now
    try:
        from bitget.governance.meta_alerts import send_meta_critical_alert

        send_meta_critical_alert(
            "Portfolio NAV halt",
            (
                f"mdd_pct={mdd:.2f} nav={snap.get('nav')} hwm={snap.get('hwm')} "
                f"— new entries blocked (no auto-flatten)"
            ),
            prefix="NAV_DD_HALT",
        )
    except Exception:
        pass


def evaluate_nav_risk_gate(cfg: dict) -> GateResult:
    """Gate 6: portfolio NAV drawdown — reduce / block / halt (no flatten)."""
    try:
        from bitget.live_nav_manager import portfolio_nav_snapshot

        snap = portfolio_nav_snapshot()
    except Exception as e:
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="nav_snapshot_unavailable_soft_pass",
            meta={"nav_error": str(e)[:120]},
        )

    mdd = float(snap.get("mdd_pct") or 0.0)
    reduce_at = _cfg_float(cfg, "NAV_DD_REDUCE_PCT", NAV_DD_REDUCE_PCT)
    block_at = _cfg_float(cfg, "NAV_DD_BLOCK_PCT", NAV_DD_BLOCK_PCT)
    halt_at = _cfg_float(cfg, "NAV_DD_HALT_PCT", NAV_DD_HALT_PCT)
    size_mult = _cfg_float(cfg, "NAV_DD_REDUCE_SIZE_MULT", NAV_DD_REDUCE_SIZE_MULT)
    if size_mult <= 0 or size_mult > 1:
        size_mult = float(NAV_DD_REDUCE_SIZE_MULT)

    base_meta = {
        "nav": snap.get("nav"),
        "hwm": snap.get("hwm"),
        "mdd_pct": mdd,
        "nav_dd_reduce_pct": reduce_at,
        "nav_dd_block_pct": block_at,
        "nav_dd_halt_pct": halt_at,
    }

    if mdd >= halt_at:
        _maybe_nav_halt_alert(mdd, snap)
        return GateResult(
            ExecutionGateOutcome.NAV_BLOCKED,
            message=f"portfolio NAV MDD {mdd:.2f}% >= halt {halt_at:.1f}%",
            meta={**base_meta, "nav_risk_stage": "halt", "nav_size_mult": 0.0},
        )
    if mdd >= block_at:
        return GateResult(
            ExecutionGateOutcome.NAV_BLOCKED,
            message=f"portfolio NAV MDD {mdd:.2f}% >= block {block_at:.1f}%",
            meta={**base_meta, "nav_risk_stage": "block", "nav_size_mult": 0.0},
        )
    if mdd >= reduce_at:
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message=f"portfolio NAV MDD {mdd:.2f}% — reduce size ×{size_mult}",
            meta={
                **base_meta,
                "nav_risk_stage": "reduce",
                "nav_size_mult": size_mult,
            },
        )
    return GateResult(
        ExecutionGateOutcome.APPROVED,
        meta={**base_meta, "nav_risk_stage": "ok", "nav_size_mult": 1.0},
    )


def evaluate_orphan_gate(cfg: dict) -> GateResult:
    """Gate 5: exchange-only orphan inventory — block new entries (never flatten)."""
    if not oms_orphan_active(cfg):
        return GateResult(ExecutionGateOutcome.APPROVED, meta={"oms_orphan_active": "OFF"})
    count = 0
    try:
        count = int(cfg.get("OMS_ORPHAN_COUNT") or 0)
    except (TypeError, ValueError):
        count = 0
    propose = str(cfg.get("OMS_ORPHAN_KILL_SWITCH_PROPOSED", "OFF") or "OFF").strip().upper()
    return GateResult(
        ExecutionGateOutcome.ORPHAN_BLOCKED,
        message=(
            f"OMS orphan active count={count} — new entries blocked "
            f"(propose_kill={propose}; no auto-flatten)"
        ),
        meta={
            "oms_orphan_active": "ON",
            "oms_orphan_count": count,
            "oms_orphan_kill_switch_proposed": propose,
            "oms_orphan_streak": cfg.get("OMS_ORPHAN_STREAK"),
        },
    )


def evaluate_gross_notional_gate(cfg: dict) -> GateResult:
    """Gate 7: portfolio open gross notional / NAV — block new entries (never flatten)."""
    max_pct = _cfg_float(cfg, "GROSS_NOTIONAL_MAX_PCT", GROSS_NOTIONAL_MAX_PCT)
    if max_pct <= 0:
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            meta={"gross_gate": "disabled", "gross_notional_max_pct": max_pct},
        )
    snap = portfolio_gross_snapshot(cfg)
    if snap.get("error"):
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="gross_snapshot_unavailable_soft_pass",
            meta={"gross_error": snap.get("error")},
        )
    gross_pct = float(snap.get("gross_pct") or 0.0)
    base_meta = {
        "gross_usdt": snap.get("gross_usdt"),
        "gross_nav": snap.get("nav"),
        "gross_pct": gross_pct,
        "gross_notional_max_pct": max_pct,
    }
    if gross_pct >= max_pct:
        return GateResult(
            ExecutionGateOutcome.GROSS_BLOCKED,
            message=(
                f"portfolio gross {gross_pct:.1f}% of NAV >= cap {max_pct:.1f}% "
                f"(gross={snap.get('gross_usdt')} nav={snap.get('nav')}; no auto-flatten)"
            ),
            meta={**base_meta, "gross_risk_stage": "block"},
        )
    return GateResult(
        ExecutionGateOutcome.APPROVED,
        meta={**base_meta, "gross_risk_stage": "ok"},
    )


def evaluate_tail_risk_gate(cfg: dict) -> GateResult:
    """Gate 8: tail reserve — underfund size mult; empty+DD block (never flatten)."""
    try:
        from bitget.trading.tail_risk_gate import tail_risk_entry_blocked

        blocked, meta = tail_risk_entry_blocked(cfg)
    except Exception as e:
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="tail_risk_unavailable_soft_pass",
            meta={"tail_risk_error": str(e)[:120], "tail_risk_size_mult": 1.0},
        )
    if blocked:
        return GateResult(
            ExecutionGateOutcome.TAIL_RISK_BLOCKED,
            message=(
                f"tail-risk reserve empty under NAV DD "
                f"(fund={meta.get('fund_usdt')} mdd={meta.get('mdd_pct')}%; no auto-flatten)"
            ),
            meta=dict(meta),
        )
    return GateResult(
        ExecutionGateOutcome.APPROVED,
        message=str(meta.get("tail_risk_gate") or "tail_ok"),
        meta=dict(meta),
    )


def evaluate_config_gates(cfg: dict) -> GateResult:
    """
    Gates 1–8: master, dry-run, kill, circuit, orphan, NAV DD, gross, tail-risk.
    """
    if not bool(cfg.get("ENABLE_REAL_EXECUTION", False)):
        return GateResult(
            ExecutionGateOutcome.EXECUTION_DISABLED,
            message="ENABLE_REAL_EXECUTION is false",
        )
    if bool(cfg.get("REAL_EXECUTION_DRY_RUN", True)):
        return GateResult(
            ExecutionGateOutcome.DRY_RUN,
            message="REAL_EXECUTION_DRY_RUN is true",
        )
    if meta_kill_switch_active():
        return GateResult(
            ExecutionGateOutcome.META_BLOCKED,
            message="MetaGovernor KILL_SWITCH: new orders blocked",
        )
    if global_circuit_breaker_active(cfg):
        return GateResult(
            ExecutionGateOutcome.CIRCUIT_BLOCKED,
            message="GLOBAL_CIRCUIT_BREAKER ON: new orders blocked",
            meta={"global_circuit_breaker": "ON"},
        )
    orphan_result = evaluate_orphan_gate(cfg)
    if orphan_result.outcome != ExecutionGateOutcome.APPROVED:
        return orphan_result
    nav_result = evaluate_nav_risk_gate(cfg)
    if nav_result.outcome != ExecutionGateOutcome.APPROVED:
        return nav_result
    gross_result = evaluate_gross_notional_gate(cfg)
    if gross_result.outcome != ExecutionGateOutcome.APPROVED:
        return gross_result
    tail_result = evaluate_tail_risk_gate(cfg)
    if tail_result.outcome != ExecutionGateOutcome.APPROVED:
        return tail_result
    merged = dict(orphan_result.meta)
    merged.update(nav_result.meta)
    merged.update(gross_result.meta)
    merged.update(tail_result.meta)
    return GateResult(
        ExecutionGateOutcome.APPROVED,
        message=nav_result.message or gross_result.message or tail_result.message,
        meta=merged,
    )


def evaluate_slippage_gate(
    market_symbol: str,
    market_type: str,
    cfg: dict,
) -> GateResult:
    """Gate 12: pre-trade slippage / spread check."""
    slip_ok, slip_meta = run_pre_trade_gate(market_symbol, market_type, cfg)
    if slip_ok:
        return GateResult(ExecutionGateOutcome.APPROVED, meta=dict(slip_meta))
    return GateResult(
        ExecutionGateOutcome.SLIPPAGE_BLOCKED,
        message=str(slip_meta.get("slippage_reason") or "slippage_blocked"),
        meta=dict(slip_meta),
    )


def evaluate_doomsday_gate(
    cfg: dict,
    *,
    position_side: str = "LONG",
) -> GateResult:
    """Gate 9: DEFCON ≤ block → block new LONG; else attach size dampen mult (never flatten)."""
    try:
        from bitget.trading.doomsday_gate import (
            doomsday_long_entry_blocked,
            doomsday_size_mult,
        )

        blocked, meta = doomsday_long_entry_blocked(cfg, position_side=position_side)
        size_mult = doomsday_size_mult(cfg, position_side=position_side)
        meta = dict(meta)
        meta["doomsday_size_mult"] = size_mult
        meta["doomsday_size_side"] = str(position_side or "LONG").upper()
    except Exception as e:
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="doomsday_unavailable_soft_pass",
            meta={"doomsday_error": str(e)[:120], "doomsday_size_mult": 1.0},
        )
    if blocked:
        return GateResult(
            ExecutionGateOutcome.DOOMSDAY_BLOCKED,
            message=(
                f"Doomsday DEFCON {meta.get('defcon_level')} <= "
                f"{meta.get('doomsday_block_level')} — new LONG blocked "
                f"(no auto-flatten; SHORT may hedge)"
            ),
            meta=meta,
        )
    return GateResult(ExecutionGateOutcome.APPROVED, meta=meta)


def evaluate_concentration_gate(
    cfg: dict,
    *,
    market_symbol: str,
    market_type: str,
    position_side: str = "LONG",
) -> GateResult:
    """Gate 10: BTC-proxy high-β same-side cluster — block new entries (never flatten)."""
    try:
        from bitget.trading.concentration_gate import concentration_entry_blocked

        blocked, meta = concentration_entry_blocked(
            cfg,
            symbol=market_symbol,
            position_side=position_side,
            market_type=market_type,
        )
    except Exception as e:
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="concentration_unavailable_soft_pass",
            meta={"concentration_error": str(e)[:120]},
        )
    if blocked:
        return GateResult(
            ExecutionGateOutcome.CONCENTRATION_BLOCKED,
            message=(
                f"BTC-proxy concentration cluster "
                f"{meta.get('cluster_pct')}% >= {meta.get('corr_cluster_max_pct')}% NAV "
                f"(corr={meta.get('candidate_corr_btc')}; no auto-flatten)"
            ),
            meta=dict(meta),
        )
    return GateResult(ExecutionGateOutcome.APPROVED, meta=dict(meta))


def evaluate_price_sanity_gate(
    cfg: dict,
    *,
    market_symbol: str,
    market_type: str,
    entry_price: Optional[float] = None,
    timeframe: Optional[str] = None,
) -> GateResult:
    """Gate 11: bad-tick / flash-crash OHLCV sanity — block new entries (never flatten)."""
    try:
        from bitget.trading.price_sanity_gate import price_sanity_entry_blocked

        blocked, meta = price_sanity_entry_blocked(
            cfg,
            symbol=market_symbol,
            market_type=market_type,
            timeframe=timeframe,
            entry_price=entry_price,
        )
    except Exception as e:
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="price_sanity_unavailable_soft_pass",
            meta={"price_sanity_error": str(e)[:120]},
        )
    if blocked:
        return GateResult(
            ExecutionGateOutcome.PRICE_SANITY_BLOCKED,
            message=(
                f"price sanity blocked ({meta.get('price_sanity')}: "
                f"gap={meta.get('entry_vs_prev_gap_pct') or meta.get('last_vs_prev_gap_pct')}%; "
                f"no auto-flatten)"
            ),
            meta=dict(meta),
        )
    return GateResult(ExecutionGateOutcome.APPROVED, meta=dict(meta))


def run_pre_execution_gates(
    cfg: dict,
    *,
    market_symbol: str,
    market_type: str,
    position_side: str = "LONG",
    entry_price: Optional[float] = None,
    timeframe: Optional[str] = None,
) -> GateResult:
    """
    Run config/orphan/NAV/gross/tail → doomsday → concentration → price sanity → slippage.
    Stops at first non-APPROVED (except DRY_RUN whichhalts before exchange).
    """
    
    # [아키텍트 수술] 펀딩비(Funding Rate) 기반 롱/숏 스퀴즈 빔 사전 회피 클러치
    # 비트겟 선물 마켓에서 펀딩비가 한쪽으로 극단적으로 쏠려 있다면, 곧 반대 방향의 청산 빔이 떨어집니다.
    # 스캐너가 신호를 주더라도 이 찰나의 스퀴즈 위험을 감지하면 즉시 주문을 거부(Block)합니다.
    try:
        from bitget.reports.canary_panel_bg import load_canary_state
        canary = load_canary_state()
        avg_funding = float(canary.get("components", {}).get("avg_funding_rate") or 0.0)
        
        # 펀딩비가 극단적 음수(숏 과열) -> 롱 스퀴즈 빔 위험 -> 숏 진입 즉시 차단
        if position_side.upper() == "SHORT" and avg_funding <= -0.001:
            return GateResult(
                ExecutionGateOutcome.EXECUTION_DISABLED,
                message=f"Squeeze Danger: 극단적 음수 펀딩비({avg_funding})로 롱 스퀴즈 빔 폭발 직전. 숏 진입 실시간 차단.",
                meta={"avg_funding": avg_funding}
            )
        # 펀딩비가 극단적 양수(롱 과열) -> 롱 뚝배기 빔 위험 -> 롱 진입 즉시 차단
        elif position_side.upper() == "LONG" and avg_funding >= 0.001:
            return GateResult(
                ExecutionGateOutcome.EXECUTION_DISABLED,
                message=f"Squeeze Danger: 극단적 양수 펀딩비({avg_funding})로 롱 청산 빔 폭발 직전. 롱 진입 실시간 차단.",
                meta={"avg_funding": avg_funding}
            )
    except Exception:
        pass

    config_result = evaluate_config_gates(cfg)
    if config_result.outcome != ExecutionGateOutcome.APPROVED:
        return config_result
        
    # [아키텍트 수술] 24시간 롤링 승률 붕괴(Catastrophic Day) 방어막 가동
    # 시장이 갑작스럽게 미쳐서 내 로직이 연속으로 터져나갈 때, 무지성 추가 진입을 원천 차단합니다.
    try:
        from bitget.trading.catastrophic_day_guard_bg import evaluate_rolling_catastrophic_clutch
        catastrophe = evaluate_rolling_catastrophic_clutch(market_type=market_type, sys_config=cfg)
        if catastrophe.get("block_entry"):
            return GateResult(
                ExecutionGateOutcome.CATASTROPHIC_BLOCKED,
                message=f"Catastrophic Loss Day 발동: 최근 24h 승률 붕괴({catastrophe.get('reason')}). 모든 신규 진입을 하드 블락합니다.",
                meta={"catastrophic_day": catastrophe}
            )
    except Exception:
        pass # 파일 누락이나 에러 시 멈추지 않고 다음 게이트로 패스(Soft Pass)

    # [아키텍트 수술] 코인 자가 진화형 메가 트렌드 킬스위치 가동
    # 펀딩비와 유동성 스트레스, 그리고 강화학습된 민감도를 바탕으로 탐욕의 끝자락을 원천 차단합니다.
    try:
        from bitget.trading.mega_trend_kill_bg import evaluate_crypto_climax_kill_switch
        climax = evaluate_crypto_climax_kill_switch(cfg, position_side=position_side)
        if climax.get("kill_active"):
            return GateResult(
                ExecutionGateOutcome.CLIMAX_KILL_BLOCKED,
                message=climax.get("reason"),
                meta={"climax_metrics": climax.get("metrics")}
            )
    except Exception:
        pass # 파일 누락 시 멈추지 않고 다음 게이트로 패스

    doom = evaluate_doomsday_gate(cfg, position_side=position_side)
    if doom.outcome != ExecutionGateOutcome.APPROVED:
        return doom
    conc = evaluate_concentration_gate(
        cfg,
        market_symbol=market_symbol,
        market_type=market_type,
        position_side=position_side,
    )
    if conc.outcome != ExecutionGateOutcome.APPROVED:
        return conc
    sanity = evaluate_price_sanity_gate(
        cfg,
        market_symbol=market_symbol,
        market_type=market_type,
        entry_price=entry_price,
        timeframe=timeframe,
    )
    if sanity.outcome != ExecutionGateOutcome.APPROVED:
        return sanity
    slip = evaluate_slippage_gate(market_symbol, market_type, cfg)
    if slip.outcome != ExecutionGateOutcome.APPROVED:
        return slip
    merged = dict(config_result.meta)
    merged.update(doom.meta)
    merged.update(conc.meta)
    merged.update(sanity.meta)
    merged.update(slip.meta)
    return GateResult(
        ExecutionGateOutcome.APPROVED,
        message=(
            config_result.message
            or doom.message
            or conc.message
            or sanity.message
            or slip.message
        ),
        meta=merged,
    )


def oms_defense_block_reason(
    cfg: Optional[dict] = None,
    *,
    market_symbol: Optional[str] = None,
    market_type: str = "futures",
    position_side: Optional[str] = None,
) -> Optional[str]:
    """Defense-in-depth for oms_core — None means allow create_order."""
    if meta_kill_switch_active():
        return "meta_kill_switch"
    try:
        if cfg is None:
            from bitget.config_hub import load_config

            cfg = load_config()
    except Exception:
        cfg = cfg or {}
    if global_circuit_breaker_active(cfg):
        return "global_circuit_breaker"
    if oms_orphan_active(cfg):
        return "oms_orphan_active"
    try:
        if nav_entry_blocked(cfg):
            return "nav_dd_block"
    except Exception:
        pass
    try:
        if gross_entry_blocked(cfg):
            return "gross_notional_cap"
    except Exception:
        pass
    try:
        from bitget.trading.tail_risk_gate import tail_risk_entry_blocked

        blocked, _ = tail_risk_entry_blocked(cfg)
        if blocked:
            return "tail_risk_reserve"
    except Exception:
        pass
    if position_side:
        try:
            from bitget.trading.doomsday_gate import doomsday_long_entry_blocked

            blocked, _ = doomsday_long_entry_blocked(cfg, position_side=position_side)
            if blocked:
                return "doomsday_defcon"
        except Exception:
            pass
            
        # [아키텍트 수술] OMS 최후의 방어선에 승률 붕괴(Catastrophic Day) 하드 블락 추가
        try:
            from bitget.trading.catastrophic_day_guard_bg import evaluate_rolling_catastrophic_clutch
            catastrophe = evaluate_rolling_catastrophic_clutch(market_type=market_type, sys_config=cfg)
            if catastrophe.get("block_entry"):
                return "catastrophic_day_collapse"
        except Exception:
            pass

    if market_symbol and position_side:
        try:
            from bitget.trading.concentration_gate import concentration_entry_blocked

            blocked, _ = concentration_entry_blocked(
                cfg,
                symbol=market_symbol,
                position_side=position_side,
                market_type=market_type,
            )
            if blocked:
                return "concentration_cap"
        except Exception:
            pass
    if market_symbol:
        try:
            from bitget.trading.price_sanity_gate import price_sanity_entry_blocked

            blocked, _ = price_sanity_entry_blocked(
                cfg,
                symbol=market_symbol,
                market_type=market_type,
            )
            if blocked:
                return "price_sanity"
        except Exception:
            pass
    return None


def max_leverage_cap(cfg: Optional[dict] = None) -> float:
    """Hard upper bound for resolve_leverage (config MAX_LEVERAGE or SSOT default)."""
    try:
        raw = (cfg or {}).get("MAX_LEVERAGE", DEFAULT_MAX_LEVERAGE)
        cap = float(raw if raw is not None else DEFAULT_MAX_LEVERAGE)
    except (TypeError, ValueError):
        cap = float(DEFAULT_MAX_LEVERAGE)
    return max(1.0, cap)

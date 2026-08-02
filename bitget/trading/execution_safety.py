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
 13. Leverage / margin manager (futures — resolve_max_leverage / MAX_LEVERAGE)
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
    GROSS_NOTIONAL_CAP_ENABLED,
    MAX_GROSS_NOTIONAL_PCT,
    NAV_DD_ALERT_MIN_INTERVAL_SEC,
    NAV_DD_BLOCK_PCT,
    NAV_DD_HALT_PCT,
    NAV_DD_REDUCE_PCT,
    NAV_DD_REDUCE_SIZE_MULT,
    PORTFOLIO_MDD_BLOCK_PCT,
    PORTFOLIO_MDD_HALT_PCT,
    PORTFOLIO_MDD_REDUCE_PCT,
    PORTFOLIO_MDD_REDUCE_SIZE_MULT,
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


def _cfg_bool(cfg: dict, key: str, default: bool = True) -> bool:
    raw = (cfg or {}).get(key, default)
    if isinstance(raw, str):
        return raw.strip().lower() in ("true", "1", "yes", "on")
    if raw is None:
        return bool(default)
    return bool(raw)


def portfolio_treasury_nav(cfg: dict) -> float:
    """NAV SSOT for portfolio MDD: TREASURY_SPOT_USDT + TREASURY_FUTURES_USDT."""
    spot = _cfg_float(cfg, "TREASURY_SPOT_USDT", 0.0)
    fut = _cfg_float(cfg, "TREASURY_FUTURES_USDT", 0.0)
    return spot + fut


def portfolio_mdd_breaker_enabled(cfg: dict) -> bool:
    return _cfg_bool(cfg, "PORTFOLIO_MDD_BREAKER_ENABLED", True)


def _portfolio_nav_peak_init(cfg: dict) -> float:
    try:
        total = float(cfg.get("ACCOUNT_SIZE_USDT", 100_000.0) or 100_000.0)
    except (TypeError, ValueError):
        total = 100_000.0
    return total if total > 0 else 100_000.0


def resolve_portfolio_nav_peak(cfg: dict, nav_current: float) -> float:
    """Monotonic HWM — never auto-resets below ACCOUNT_SIZE_USDT seed."""
    peak = _cfg_float(cfg, "PORTFOLIO_NAV_PEAK", 0.0)
    if peak <= 0:
        peak = _portfolio_nav_peak_init(cfg)
    return max(peak, nav_current)


def evaluate_portfolio_mdd_tier(
    nav_current: float,
    nav_peak: float,
    cfg: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Pure tier evaluation (ratio drawdown).
    dd_pct = (nav_peak - nav_current) / nav_peak
    """
    cfg = cfg or {}
    if nav_peak <= 0:
        dd_pct = 0.0
    else:
        dd_pct = max(0.0, (nav_peak - nav_current) / nav_peak)

    reduce_at = _cfg_float(cfg, "PORTFOLIO_MDD_REDUCE_PCT", PORTFOLIO_MDD_REDUCE_PCT)
    block_at = _cfg_float(cfg, "PORTFOLIO_MDD_BLOCK_PCT", PORTFOLIO_MDD_BLOCK_PCT)
    halt_at = _cfg_float(cfg, "PORTFOLIO_MDD_HALT_PCT", PORTFOLIO_MDD_HALT_PCT)
    reduce_mult = _cfg_float(
        cfg, "PORTFOLIO_MDD_REDUCE_SIZE_MULT", PORTFOLIO_MDD_REDUCE_SIZE_MULT
    )
    if reduce_mult <= 0 or reduce_mult > 1:
        reduce_mult = float(PORTFOLIO_MDD_REDUCE_SIZE_MULT)

    if dd_pct >= halt_at:
        return {
            "tier": "HALT",
            "size_mult": 0.0,
            "dd_pct": dd_pct,
            "blocks_entry": True,
        }
    if dd_pct >= block_at:
        return {
            "tier": "BLOCK",
            "size_mult": 0.0,
            "dd_pct": dd_pct,
            "blocks_entry": True,
        }
    if dd_pct >= reduce_at:
        return {
            "tier": "REDUCE",
            "size_mult": reduce_mult,
            "dd_pct": dd_pct,
            "blocks_entry": False,
        }
    return {
        "tier": "NORMAL",
        "size_mult": 1.0,
        "dd_pct": dd_pct,
        "blocks_entry": False,
    }


def _persist_portfolio_mdd_state(cfg: dict, nav_peak: float, tier: str) -> bool:
    """Single writer: execution_safety → config_kv state keys. Returns False on DB failure."""
    try:
        from bitget.infra import config_manager

        prev_peak = _cfg_float(cfg, "PORTFOLIO_NAV_PEAK", 0.0)
        prev_tier = str(cfg.get("PORTFOLIO_MDD_CURRENT_TIER") or "NORMAL")
        if nav_peak != prev_peak:
            config_manager.set_config_value("PORTFOLIO_NAV_PEAK", float(nav_peak))
            cfg["PORTFOLIO_NAV_PEAK"] = float(nav_peak)
        if tier != prev_tier:
            config_manager.set_config_value("PORTFOLIO_MDD_CURRENT_TIER", str(tier))
            cfg["PORTFOLIO_MDD_CURRENT_TIER"] = str(tier)
        return True
    except Exception as exc:
        try:
            from bitget.infra.logging_setup import get_logger

            get_logger("bitget.trading.execution_safety").warning(
                "portfolio_mdd state persist failed: %s", exc
            )
        except Exception:
            pass
        return False


def _maybe_portfolio_halt_alert(tier: str, prev_tier: str, snap: dict[str, Any]) -> None:
    """HALT tier transition only — no repeat while staying in HALT."""
    if tier != "HALT" or prev_tier == "HALT":
        return
    try:
        from bitget.governance.meta_alerts import send_meta_critical_alert

        dd = float(snap.get("dd_pct") or 0.0) * 100.0
        send_meta_critical_alert(
            "Portfolio NAV MDD HALT",
            (
                f"tier=HALT dd_pct={dd:.2f}% "
                f"nav={snap.get('nav_current')} peak={snap.get('nav_peak')} "
                f"— new entries blocked (no auto-flatten)"
            ),
            prefix="PORTFOLIO_MDD_HALT",
        )
    except Exception:
        pass


def evaluate_portfolio_mdd_gate(cfg: dict) -> dict[str, Any]:
    """Shared SSOT for paper try_add + execution_safety config gate 6 (NAV MDD)."""
    if not portfolio_mdd_breaker_enabled(cfg):
        return {
            "enabled": False,
            "bypassed": True,
            "tier": "NORMAL",
            "size_mult": 1.0,
            "dd_pct": 0.0,
            "blocks_entry": False,
            "nav_current": portfolio_treasury_nav(cfg),
            "nav_peak": 0.0,
        }

    nav_current = portfolio_treasury_nav(cfg)
    prev_tier = str(cfg.get("PORTFOLIO_MDD_CURRENT_TIER") or "NORMAL")
    nav_peak = resolve_portfolio_nav_peak(cfg, nav_current)
    result = evaluate_portfolio_mdd_tier(nav_current, nav_peak, cfg)
    result["enabled"] = True
    result["bypassed"] = False
    result["nav_current"] = nav_current
    result["nav_peak"] = nav_peak
    persisted = _persist_portfolio_mdd_state(cfg, nav_peak, str(result["tier"]))
    if persisted and str(result["tier"]) == "HALT" and prev_tier != "HALT":
        _maybe_portfolio_halt_alert(str(result["tier"]), prev_tier, result)
    return result


def portfolio_mdd_pct() -> float:
    try:
        from bitget.config_hub import load_config

        cfg = load_config()
        if not portfolio_mdd_breaker_enabled(cfg):
            return 0.0
        snap = evaluate_portfolio_mdd_gate(cfg)
        return float(snap.get("dd_pct") or 0.0) * 100.0
    except Exception:
        return 0.0


def nav_entry_blocked(cfg: dict) -> bool:
    """True when portfolio MDD reaches block/halt — OMS defense helper."""
    if not portfolio_mdd_breaker_enabled(cfg):
        return False
    snap = evaluate_portfolio_mdd_gate(cfg)
    return bool(snap.get("blocks_entry"))


def portfolio_open_gross_usdt() -> float:
    """Sum OPEN mark notional (quantity×entry_price) — A-4 SSOT, leverage-independent."""
    try:
        from bitget.infra.bounded_reads import forward_open_mark_notional_sum_sql
        from bitget.infra.data_paths import market_data_db_path
        from bitget.infra.shared_db_connector import get_connection

        q, p = forward_open_mark_notional_sum_sql()
        conn = get_connection(market_data_db_path())
        try:
            row = conn.execute(q, p).fetchone()
            return float((row[0] if row else 0.0) or 0.0)
        finally:
            conn.close()
    except Exception:
        return 0.0


def gross_notional_cap_enabled(cfg: dict) -> bool:
    return _cfg_bool(cfg, "GROSS_NOTIONAL_CAP_ENABLED", GROSS_NOTIONAL_CAP_ENABLED)


def resolve_max_gross_notional_pct(cfg: Optional[dict] = None) -> float:
    """MAX_GROSS_NOTIONAL_PCT (A-4) with legacy GROSS_NOTIONAL_MAX_PCT fallback."""
    cfg = cfg or {}
    raw_new = cfg.get("MAX_GROSS_NOTIONAL_PCT")
    if raw_new is not None and raw_new != "":
        return _cfg_float(cfg, "MAX_GROSS_NOTIONAL_PCT", MAX_GROSS_NOTIONAL_PCT)
    return _cfg_float(cfg, "GROSS_NOTIONAL_MAX_PCT", GROSS_NOTIONAL_MAX_PCT)


def gross_gate_nav_current(cfg: dict) -> float:
    """nav_current from A-1 snap cache — no standalone treasury re-sum."""
    if portfolio_mdd_breaker_enabled(cfg):
        snap = get_portfolio_mdd_snap_cached(cfg)
        return float(snap.get("nav_current") or 0.0)
    return portfolio_treasury_nav(cfg)


def evaluate_gross_notional_gate_values(
    nav_current: float,
    gross_notional: float,
    cfg: dict,
) -> dict[str, Any]:
    """Pure gate 7 evaluation — shared by evaluate_gross_notional_gate + try_add."""
    if not gross_notional_cap_enabled(cfg):
        return {
            "enabled": False,
            "bypassed": True,
            "blocked": False,
            "block_new_entries": False,
            "gross_notional": round(float(gross_notional), 4),
            "nav_current": float(nav_current),
            "gross_notional_pct": 0.0,
            "gross_gate": "cap_disabled",
        }
    max_pct = resolve_max_gross_notional_pct(cfg)
    if max_pct <= 0:
        return {
            "enabled": True,
            "bypassed": False,
            "blocked": False,
            "block_new_entries": False,
            "gross_notional": round(float(gross_notional), 4),
            "nav_current": float(nav_current),
            "gross_notional_pct": 0.0,
            "max_gross_notional_pct": max_pct,
            "gross_gate": "threshold_disabled",
        }
    nav = float(nav_current)
    gross = float(gross_notional)
    gross_pct = (gross / nav * 100.0) if nav > 0 else (0.0 if gross <= 0 else 9999.0)
    blocked = gross_pct >= max_pct
    return {
        "enabled": True,
        "bypassed": False,
        "blocked": blocked,
        "block_new_entries": blocked,
        "gross_notional": round(gross, 4),
        "nav_current": nav,
        "gross_notional_pct": round(gross_pct, 4),
        "max_gross_notional_pct": max_pct,
        "gross_gate": "block" if blocked else "ok",
        # legacy meta aliases for OMS/tests
        "gross_usdt": round(gross, 4),
        "nav": nav,
        "gross_pct": round(gross_pct, 4),
        "gross_notional_max_pct": max_pct,
    }


def portfolio_open_gross_usdt_legacy_sim_kelly() -> float:
    """Deprecated sum(sim_kelly_invest) — retained for diagnostics only."""
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
    """Open gross notional vs A-1 nav_current — gate 7 + try_add SSOT."""
    cfg = cfg or {}
    try:
        nav_current = gross_gate_nav_current(cfg)
        gross = portfolio_open_gross_usdt()
        return evaluate_gross_notional_gate_values(nav_current, gross, cfg)
    except Exception as e:
        return {
            "nav": 0.0,
            "nav_current": 0.0,
            "gross_usdt": 0.0,
            "gross_notional": 0.0,
            "gross_pct": 0.0,
            "gross_notional_pct": 0.0,
            "error": str(e)[:120],
        }


def gross_entry_blocked(cfg: dict) -> bool:
    """True when open gross notional / nav_current reaches configured cap."""
    snap = portfolio_gross_snapshot(cfg)
    if snap.get("error") or snap.get("bypassed"):
        return False
    return bool(snap.get("blocked") or snap.get("block_new_entries"))


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


def _portfolio_mdd_snap_fingerprint(cfg: dict) -> str:
    spot = (cfg or {}).get("TREASURY_SPOT_USDT")
    fut = (cfg or {}).get("TREASURY_FUTURES_USDT")
    peak = (cfg or {}).get("PORTFOLIO_NAV_PEAK")
    tier = (cfg or {}).get("PORTFOLIO_MDD_CURRENT_TIER")
    return f"{spot}|{fut}|{peak}|{tier}"


def get_portfolio_mdd_snap_cached(cfg: dict) -> dict[str, Any]:
    """
    Per-request cache — config gate 6 (NAV MDD) and gate 8 (tail) share one A-1 eval.

    SSOT: calls evaluate_portfolio_mdd_gate only (no NAV/dd_pct recompute here).
    That gate → portfolio_treasury_nav + resolve_portfolio_nav_peak
    → evaluate_portfolio_mdd_tier (pure tier).
    """
    fp = _portfolio_mdd_snap_fingerprint(cfg)
    cached = cfg.get("_PORTFOLIO_MDD_GATE_SNAP")
    if (
        isinstance(cached, dict)
        and cached.get("tier") is not None
        and cfg.get("_PORTFOLIO_MDD_GATE_SNAP_FP") == fp
    ):
        return cached
    snap = evaluate_portfolio_mdd_gate(cfg)
    cfg["_PORTFOLIO_MDD_GATE_SNAP"] = snap
    cfg["_PORTFOLIO_MDD_GATE_SNAP_FP"] = fp
    cfg.pop("_TAIL_FUND_DEBIT_DONE", None)
    return snap


def evaluate_nav_risk_gate(cfg: dict) -> GateResult:
    """Config gate 6: portfolio treasury NAV MDD — reduce / block / halt (no flatten)."""
    if not portfolio_mdd_breaker_enabled(cfg):
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="portfolio_mdd_breaker_disabled",
            meta={
                "portfolio_mdd_enabled": False,
                "nav_risk_stage": "ok",
                "nav_size_mult": 1.0,
                "portfolio_mdd_tier": "NORMAL",
            },
        )

    snap = get_portfolio_mdd_snap_cached(cfg)
    try:
        from bitget.trading.tail_risk_gate import process_tail_fund_drawdown_on_snap

        process_tail_fund_drawdown_on_snap(cfg, snap)
    except Exception:
        pass
    dd_pct_ratio = float(snap.get("dd_pct") or 0.0)
    dd_pct_display = dd_pct_ratio * 100.0
    tier_raw = str(snap.get("tier") or "NORMAL")
    tier_key = tier_raw.lower()
    size_mult = float(snap.get("size_mult") or 1.0)

    base_meta = {
        "portfolio_mdd_enabled": True,
        "nav_current": snap.get("nav_current"),
        "nav_peak": snap.get("nav_peak"),
        "nav": snap.get("nav_current"),
        "hwm": snap.get("nav_peak"),
        "mdd_pct": round(dd_pct_display, 4),
        "dd_pct": dd_pct_ratio,
        "portfolio_mdd_tier": tier_raw,
        "nav_size_mult": size_mult,
    }

    if snap.get("blocks_entry"):
        stage = "halt" if tier_key == "halt" else "block"
        return GateResult(
            ExecutionGateOutcome.NAV_BLOCKED,
            message=(
                f"portfolio MDD {dd_pct_display:.2f}% — tier {tier_raw} "
                f"(peak={snap.get('nav_peak')} nav={snap.get('nav_current')})"
            ),
            meta={**base_meta, "nav_risk_stage": stage, "nav_size_mult": 0.0},
        )
    if tier_key == "reduce":
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message=f"portfolio MDD {dd_pct_display:.2f}% — reduce size ×{size_mult}",
            meta={**base_meta, "nav_risk_stage": "reduce"},
        )
    return GateResult(
        ExecutionGateOutcome.APPROVED,
        meta={**base_meta, "nav_risk_stage": "ok"},
    )


def evaluate_orphan_gate(cfg: dict) -> GateResult:
    """Config gate 5: exchange-only orphan inventory — block new entries (never flatten)."""
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
    """Config gate 7: portfolio open gross notional / nav_current — block new entries (never flatten)."""
    snap = portfolio_gross_snapshot(cfg)
    if snap.get("error"):
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="gross_snapshot_unavailable_soft_pass",
            meta={"gross_error": snap.get("error")},
        )
    if snap.get("bypassed"):
        return GateResult(
            ExecutionGateOutcome.APPROVED,
            message="gross_notional_cap_disabled",
            meta=dict(snap),
        )
    gross_pct = float(snap.get("gross_notional_pct") or snap.get("gross_pct") or 0.0)
    max_pct = float(snap.get("max_gross_notional_pct") or snap.get("gross_notional_max_pct") or 0.0)
    base_meta = dict(snap)
    if snap.get("blocked"):
        return GateResult(
            ExecutionGateOutcome.GROSS_BLOCKED,
            message=(
                f"portfolio gross {gross_pct:.1f}% of nav_current >= cap {max_pct:.1f}% "
                f"(gross={snap.get('gross_notional')} nav={snap.get('nav_current')}; no auto-flatten)"
            ),
            meta={**base_meta, "gross_risk_stage": "block"},
        )
    return GateResult(
        ExecutionGateOutcome.APPROVED,
        meta={**base_meta, "gross_risk_stage": "ok"},
    )


def evaluate_tail_risk_gate(cfg: dict) -> GateResult:
    """Config gate 8: tail reserve — A-2 consumption + underfund size mult (never flatten)."""
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
        gate_label = str(meta.get("tail_risk_gate") or "")
        if gate_label == "escalate_block_exhausted":
            msg = (
                "tail-risk fund exhausted under BLOCK tier — "
                "HALT-grade entry block (auxiliary; portfolio tier unchanged)"
            )
        else:
            msg = (
                f"tail-risk reserve empty under NAV DD "
                f"(fund={meta.get('fund_usdt')} mdd={meta.get('mdd_pct')}%; no auto-flatten)"
            )
        return GateResult(
            ExecutionGateOutcome.TAIL_RISK_BLOCKED,
            message=msg,
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
    """Configured MAX_LEVERAGE upper bound (default DEFAULT_MAX_LEVERAGE=5)."""
    try:
        raw = (cfg or {}).get("MAX_LEVERAGE", DEFAULT_MAX_LEVERAGE)
        cap = float(raw if raw is not None else DEFAULT_MAX_LEVERAGE)
    except (TypeError, ValueError):
        cap = float(DEFAULT_MAX_LEVERAGE)
    return max(1.0, cap)


def resolve_max_leverage(requested: float, cfg: Optional[dict] = None) -> float:
    """
    FUT SSOT clamp — min(requested, MAX_LEVERAGE). Reject-free; logs when clamped.
    SPOT paths must not call this function.
    """
    try:
        req = max(1.0, float(requested))
    except (TypeError, ValueError):
        req = 1.0
    cap = max_leverage_cap(cfg)
    if req > cap:
        try:
            from bitget.infra.logging_setup import get_logger

            get_logger("bitget.trading.execution_safety").info(
                "MAX_LEVERAGE clamp: requested=%.4f cap=%.4f applied=%.4f",
                req,
                cap,
                cap,
            )
        except Exception:
            pass
        return float(cap)
    return float(req)

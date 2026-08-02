"""
Tail-risk reserve — accrual, crisis release (1:1), entry size/block.

Invariants:
  - Accrue up to TAIL_RISK_ACCRUAL_PCT of each market treasury into TAIL_RISK_FUND_*
  - Crisis (BEAR + high BTC ATR): release fund → treasury 1:1 (never mint ×N)
  - Underfunded vs NAV → shrink new entry size; empty+NAV-reduce → block new entries
  - Never auto-flatten open inventory
  - Soft-pass when NAV unavailable
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.memory_policy import (
    NAV_DD_REDUCE_PCT,
    PORTFOLIO_MDD_BLOCK_PCT,
    TAIL_FUND_CONSUMPTION_ENABLED,
    TAIL_RISK_ACCRUAL_PCT,
    TAIL_RISK_CRISIS_ATR_PCT,
    TAIL_RISK_EMPTY_BLOCK,
    TAIL_RISK_MIN_COVERAGE_PCT,
    TAIL_RISK_UNDERFUND_SIZE_MULT,
)


def _cfg_float(cfg: dict, key: str, default: float) -> float:
    try:
        raw = (cfg or {}).get(key, default)
        if raw is None or raw == "":
            return float(default)
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _cfg_bool(cfg: dict, key: str, default: bool) -> bool:
    raw = (cfg or {}).get(key, default)
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def tail_fund_total_usdt(cfg: Optional[dict]) -> float:
    cfg = cfg or {}
    total = 0.0
    for key in ("TAIL_RISK_FUND_SPOT", "TAIL_RISK_FUND_FUTURES"):
        try:
            total += float(cfg.get(key, 0.0) or 0.0)
        except (TypeError, ValueError):
            pass
    return max(0.0, total)


def accrue_tail_risk_fund(cfg: dict) -> dict:
    """
    Weekly SSOT: top-up reserves; optional 1:1 crisis release into treasury.
    Replaces decorative ×20 mint. Mutates and returns cfg.
    """
    out = dict(cfg or {})
    accrual_pct = _cfg_float(out, "TAIL_RISK_ACCRUAL_PCT", TAIL_RISK_ACCRUAL_PCT)
    crisis_atr = _cfg_float(out, "TAIL_RISK_CRISIS_ATR_PCT", TAIL_RISK_CRISIS_ATR_PCT)
    regime = str(out.get("CURRENT_REGIME_KEY", "") or "").upper()
    try:
        btc_atr = float(out.get("BTC_ATR_PCT", 0.0) or 0.0)
    except (TypeError, ValueError):
        btc_atr = 0.0
    crisis = regime == "BEAR" and btc_atr >= crisis_atr

    actions: list[str] = []
    for market in ("SPOT", "FUTURES"):
        t_key = f"TREASURY_{market}_USDT"
        f_key = f"TAIL_RISK_FUND_{market}"
        try:
            treasury = float(out.get(t_key, 0.0) or 0.0)
        except (TypeError, ValueError):
            treasury = 0.0
        try:
            fund = float(out.get(f_key, 0.0) or 0.0)
        except (TypeError, ValueError):
            fund = 0.0

        target = max(0.0, treasury * (accrual_pct / 100.0)) if accrual_pct > 0 else 0.0
        transfer = min(max(0.0, target - fund), max(0.0, treasury))
        if transfer > 0:
            treasury -= transfer
            fund += transfer
            actions.append(f"{market}:accrue+{transfer:.4f}")

        if crisis and fund > 0:
            # Deploy dry powder 1:1 — never invent capital
            released = fund
            treasury += released
            fund = 0.0
            actions.append(f"{market}:release_1to1+{released:.4f}")

        out[t_key] = round(max(0.0, treasury), 4)
        out[f_key] = round(max(0.0, fund), 4)

    out["TAIL_RISK_LAST_ACTION"] = {
        "crisis": bool(crisis),
        "regime": regime,
        "btc_atr_pct": round(btc_atr, 4),
        "actions": actions[:12],
    }
    return out


def tail_coverage_snapshot(cfg: Optional[dict] = None) -> dict[str, Any]:
    """fund / portfolio NAV (%). Soft-pass fields when NAV unavailable."""
    cfg = cfg or {}
    fund = tail_fund_total_usdt(cfg)
    try:
        from bitget.live_nav_manager import portfolio_nav_snapshot

        nav = float((portfolio_nav_snapshot() or {}).get("nav") or 0.0)
    except Exception as e:
        return {
            "fund_usdt": fund,
            "nav": 0.0,
            "coverage_pct": 0.0,
            "error": str(e)[:120],
        }
    coverage = (fund / nav * 100.0) if nav > 0 else 0.0
    return {
        "fund_usdt": round(fund, 4),
        "nav": nav,
        "coverage_pct": round(coverage, 4),
        "min_coverage_pct": _cfg_float(cfg, "TAIL_RISK_MIN_COVERAGE_PCT", TAIL_RISK_MIN_COVERAGE_PCT),
    }


def tail_risk_size_mult(cfg: Optional[dict] = None) -> float:
    """1.0 healthy; underfund → TAIL_RISK_UNDERFUND_SIZE_MULT; soft-pass → 1.0."""
    cfg = cfg or {}
    min_cov = _cfg_float(cfg, "TAIL_RISK_MIN_COVERAGE_PCT", TAIL_RISK_MIN_COVERAGE_PCT)
    if min_cov <= 0:
        return 1.0
    snap = tail_coverage_snapshot(cfg)
    if snap.get("error") or float(snap.get("nav") or 0.0) <= 0:
        return 1.0
    coverage = float(snap.get("coverage_pct") or 0.0)
    if coverage >= min_cov:
        return 1.0
    mult = _cfg_float(cfg, "TAIL_RISK_UNDERFUND_SIZE_MULT", TAIL_RISK_UNDERFUND_SIZE_MULT)
    if mult <= 0 or mult > 1:
        mult = float(TAIL_RISK_UNDERFUND_SIZE_MULT)
    return float(mult)


def tail_fund_consumption_enabled(cfg: Optional[dict]) -> bool:
    return _cfg_bool(cfg or {}, "TAIL_FUND_CONSUMPTION_ENABLED", TAIL_FUND_CONSUMPTION_ENABLED)


def compute_tail_fund_debit_amount(
    nav_peak: float,
    dd_pct: float,
    block_pct: float,
    tail_fund_balance: float,
) -> float:
    """A-2 SSOT — excess drawdown below BLOCK tier × peak, capped by fund balance."""
    if nav_peak <= 0 or tail_fund_balance <= 0:
        return 0.0
    excess = max(0.0, float(dd_pct) - float(block_pct))
    if excess <= 0:
        return 0.0
    return min(float(tail_fund_balance), nav_peak * excess)


def evaluate_tail_fund_gate(tail_fund_balance: float, portfolio_mdd_tier: str) -> dict[str, Any]:
    """
    Pure tail consumption gate — does not mutate PORTFOLIO_MDD_CURRENT_TIER or HALT alerts.
    escalate_block: exhausted fund + BLOCK tier → HALT-grade entry block (auxiliary only).
    """
    tier = str(portfolio_mdd_tier or "NORMAL").upper()
    exhausted = float(tail_fund_balance) <= 1e-6
    escalate_block = exhausted and tier == "BLOCK"
    return {
        "tail_exhausted": exhausted,
        "escalate_block": escalate_block,
        "portfolio_mdd_tier": tier,
        "tail_fund_balance": round(float(tail_fund_balance), 4),
    }


def _apply_tail_fund_debit(cfg: dict, amount: float) -> float:
    """Debit pooled tail fund (spot first, then futures). Mutates cfg. Returns debited."""
    remaining = max(0.0, float(amount))
    if remaining <= 0:
        return 0.0
    debited = 0.0
    for key in ("TAIL_RISK_FUND_SPOT", "TAIL_RISK_FUND_FUTURES"):
        if remaining <= 0:
            break
        try:
            bal = float(cfg.get(key, 0.0) or 0.0)
        except (TypeError, ValueError):
            bal = 0.0
        take = min(bal, remaining)
        if take > 0:
            cfg[key] = round(bal - take, 4)
            remaining -= take
            debited += take
    return round(debited, 4)


def _persist_tail_fund_balances(cfg: dict) -> bool:
    try:
        from bitget.infra import config_manager

        for key in ("TAIL_RISK_FUND_SPOT", "TAIL_RISK_FUND_FUTURES"):
            if key in cfg:
                config_manager.set_config_value(key, float(cfg[key]))
        return True
    except Exception as exc:
        try:
            from bitget.infra.logging_setup import get_logger

            get_logger("bitget.trading.tail_risk_gate").warning(
                "tail fund persist failed: %s", exc
            )
        except Exception:
            pass
        return False


def process_tail_fund_drawdown_on_snap(cfg: dict, mdd_snap: dict) -> dict[str, Any]:
    """
    A-2 consumption cycle — drawdown_event(BLOCK/HALT) debits tail fund (paper).
    Once per request (nav+tail share). Does not touch PORTFOLIO_MDD tier state.
    """
    if not tail_fund_consumption_enabled(cfg):
        return {"consumption_enabled": False, "drawdown_event": False, "debited": 0.0}

    if cfg.get("_TAIL_FUND_DEBIT_DONE"):
        balance = tail_fund_total_usdt(cfg)
        tier = str(mdd_snap.get("tier") or "NORMAL").upper()
        gate = evaluate_tail_fund_gate(balance, tier)
        return {**gate, "consumption_enabled": True, "drawdown_event": tier in ("BLOCK", "HALT"), "debited": 0.0}

    tier = str(mdd_snap.get("tier") or "NORMAL").upper()
    drawdown_event = tier in ("BLOCK", "HALT")
    cfg["_TAIL_FUND_DEBIT_DONE"] = True

    if not drawdown_event:
        balance = tail_fund_total_usdt(cfg)
        gate = evaluate_tail_fund_gate(balance, tier)
        return {**gate, "consumption_enabled": True, "drawdown_event": False, "debited": 0.0}

    nav_peak = float(mdd_snap.get("nav_peak") or 0.0)
    dd_pct = float(mdd_snap.get("dd_pct") or 0.0)
    block_pct = _cfg_float(cfg, "PORTFOLIO_MDD_BLOCK_PCT", PORTFOLIO_MDD_BLOCK_PCT)
    balance_before = tail_fund_total_usdt(cfg)
    debit = compute_tail_fund_debit_amount(nav_peak, dd_pct, block_pct, balance_before)

    if debit > 0:
        _apply_tail_fund_debit(cfg, debit)
        _persist_tail_fund_balances(cfg)

    balance_after = tail_fund_total_usdt(cfg)
    gate = evaluate_tail_fund_gate(balance_after, tier)
    return {
        **gate,
        "consumption_enabled": True,
        "drawdown_event": True,
        "debited": debit,
        "debit_amount": debit,
        "nav_peak": nav_peak,
        "dd_pct": dd_pct,
        "portfolio_mdd_block_pct": block_pct,
    }


def tail_risk_entry_blocked(cfg: Optional[dict] = None) -> tuple[bool, dict[str, Any]]:
    """
    True → block new entries.
    A-2 (consumption enabled): uses A-1 portfolio MDD snap + tail fund debit/gate.
    Legacy (consumption disabled): coverage / live_nav_manager path unchanged.
    """
    cfg = cfg or {}
    if tail_fund_consumption_enabled(cfg):
        return _tail_risk_entry_blocked_consumption(cfg)
    return _tail_risk_entry_blocked_legacy(cfg)


def _tail_risk_entry_blocked_consumption(cfg: dict) -> tuple[bool, dict[str, Any]]:
    from bitget.trading.execution_safety import get_portfolio_mdd_snap_cached

    snap = get_portfolio_mdd_snap_cached(cfg)
    consumption = process_tail_fund_drawdown_on_snap(cfg, snap)
    meta: dict[str, Any] = {
        **consumption,
        "tail_risk_size_mult": tail_risk_size_mult(cfg),
        "portfolio_mdd_tier": snap.get("tier"),
        "dd_pct": snap.get("dd_pct"),
        "nav_peak": snap.get("nav_peak"),
    }

    if consumption.get("escalate_block"):
        meta["tail_risk_gate"] = "escalate_block_exhausted"
        return True, meta

    tier = str(snap.get("tier") or "NORMAL").upper()
    if tier == "HALT":
        meta["tail_risk_gate"] = "portfolio_halt_primary"
        return False, meta

    # Underfund size shrink (coverage) — independent of consumption debit
    min_cov = _cfg_float(cfg, "TAIL_RISK_MIN_COVERAGE_PCT", TAIL_RISK_MIN_COVERAGE_PCT)
    if min_cov > 0:
        fund = tail_fund_total_usdt(cfg)
        nav_current = float(snap.get("nav_current") or 0.0)
        if nav_current > 0:
            coverage = fund / nav_current * 100.0
            meta["coverage_pct"] = round(coverage, 4)
            meta["min_coverage_pct"] = min_cov
            if coverage < min_cov and fund > 1e-6:
                meta["tail_risk_gate"] = "underfund_size_only"
                return False, meta

    meta["tail_risk_gate"] = "ok"
    return False, meta


def _tail_risk_entry_blocked_legacy(cfg: dict) -> tuple[bool, dict[str, Any]]:
    """
    True → block new entries when reserve empty AND NAV already in reduce territory.
    Never flatten. Soft-pass if NAV/fund unreadable or empty-block disabled.
    """
    cfg = cfg or {}
    snap = tail_coverage_snapshot(cfg)
    meta: dict[str, Any] = dict(snap)
    meta["tail_risk_size_mult"] = tail_risk_size_mult(cfg)

    if snap.get("error") or float(snap.get("nav") or 0.0) <= 0:
        meta["tail_risk_gate"] = "soft_pass_nav_unavailable"
        meta["tail_risk_size_mult"] = 1.0
        return False, meta

    if not _cfg_bool(cfg, "TAIL_RISK_EMPTY_BLOCK", bool(TAIL_RISK_EMPTY_BLOCK)):
        meta["tail_risk_gate"] = (
            "ok" if float(snap.get("coverage_pct") or 0.0)
            >= float(snap.get("min_coverage_pct") or 0.0)
            else "underfund_size_only"
        )
        return False, meta

    fund = float(snap.get("fund_usdt") or 0.0)
    if fund > 1e-6:
        min_cov = float(snap.get("min_coverage_pct") or 0.0)
        cov = float(snap.get("coverage_pct") or 0.0)
        meta["tail_risk_gate"] = "ok" if cov >= min_cov else "underfund_size_only"
        return False, meta

    # Reserve empty — only escalate when portfolio already drawing down
    reduce_at = _cfg_float(cfg, "NAV_DD_REDUCE_PCT", NAV_DD_REDUCE_PCT)
    try:
        from bitget.live_nav_manager import portfolio_nav_snapshot

        mdd = float((portfolio_nav_snapshot() or {}).get("mdd_pct") or 0.0)
    except Exception:
        meta["tail_risk_gate"] = "soft_pass_mdd_unavailable"
        return False, meta
    meta["mdd_pct"] = mdd
    meta["nav_dd_reduce_pct"] = reduce_at
    if mdd >= reduce_at:
        meta["tail_risk_gate"] = "block_empty_under_dd"
        return True, meta
    meta["tail_risk_gate"] = "empty_but_nav_ok"
    return False, meta

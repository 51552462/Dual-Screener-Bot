"""
FULL-BT-FUT-DEFCON-1 — Adapter A: FULL-BT harness-only DEFCON bypass helpers.

Does not modify ledger.py or doomsday_gate.py bodies.
IV L1 참고용 only — LIVE/R6/생존·달성 단정 금지.
"""
from __future__ import annotations

import os
from typing import Any, Optional


def fullbt_defcon_bypass_enabled() -> bool:
    """Kill-switch: default false (memory_policy / config_kv / env)."""
    env = os.environ.get("FULLBT_DEFCON_BYPASS_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("FULLBT_DEFCON_BYPASS_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import FULLBT_DEFCON_BYPASS_ENABLED

    return bool(FULLBT_DEFCON_BYPASS_ENABLED)


def _norm_path(p: Any) -> str:
    return os.path.normcase(os.path.normpath(str(p or "")))


def _is_futures_mt(market_type: Any) -> bool:
    s = str(market_type or "").strip().lower()
    return s in ("futures", "fut")


def should_bypass_fullbt_doomsday(context: Optional[dict] = None) -> bool:
    """3중 AND: isolated + full_bt DB target + kill-switch · FUT only.

    ``context`` keys (existing harness fields only — no invented flags):
      - isolated: bool (inside isolated_full_bt_book)
      - full_bt_db_path: expected isolation path
      - ledger_db_path: current ledger.DB_PATH
      - market_type: candidate/walk mt (no hardcode)
    """
    ctx = context if isinstance(context, dict) else {}
    if not bool(ctx.get("isolated")):
        return False
    if not fullbt_defcon_bypass_enabled():
        return False
    if not _is_futures_mt(ctx.get("market_type")):
        return False
    full_p = _norm_path(ctx.get("full_bt_db_path"))
    led_p = _norm_path(ctx.get("ledger_db_path"))
    if not full_p or not led_p or full_p != led_p:
        return False
    return True


def wrap_doomsday_long_entry_blocked(
    real_fn,
    *,
    context_provider,
):
    """Return wrapper: call real gate; if blocked and should_bypass → (False, meta)."""

    def _wrapped(cfg, *, position_side: str = "LONG"):
        blocked, meta = real_fn(cfg, position_side=position_side)
        if not blocked:
            return blocked, meta
        ctx = context_provider() if callable(context_provider) else {}
        if not should_bypass_fullbt_doomsday(ctx):
            return blocked, meta
        out = dict(meta or {})
        out["defcon_bypassed"] = True
        out["doomsday_gate"] = "fullbt_defcon_bypass"
        out["iv_note"] = "IV L1 참고용"
        on_bypass = ctx.get("on_bypass")
        if callable(on_bypass):
            try:
                on_bypass(out)
            except Exception:
                pass
        return False, out

    return _wrapped

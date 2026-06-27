"""KR 밥그릇(4번) → forward_trades 관측 편입 브릿지."""
from __future__ import annotations

from typing import Any, Optional, Tuple

from forward_observe_bridge import try_add_observe_forward_trade


def enroll_kr_bowl_shadow_observe(
    *,
    code: str,
    name: str,
    sig_type: str,
    trust_score: float,
    entry_price: float,
    cat2_count: int = 0,
    sector_hint: Optional[str] = None,
) -> Tuple[bool, str]:
    facts: dict[str, Any] = {
        "v_rs": 0,
        "v_cpv": 0,
        "v_yang": 0,
        "v_energy": float(cat2_count or 0),
        "marcap_eok": 0,
        "entry_regime": "OBSERVE_ONLY",
    }
    sector = sector_hint or "유망섹터"
    return try_add_observe_forward_trade(
        market="KR",
        code=str(code),
        name=str(name),
        sig_type=str(sig_type),
        score=float(trust_score),
        ep=float(entry_price),
        strategy_id="KR_BOWL",
        sector=sector,
        facts=facts,
    )

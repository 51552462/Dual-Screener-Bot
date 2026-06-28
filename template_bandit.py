"""
Thompson Sampling Bandit — 승격 루키 템플릿의 자율 자본 배분(켈리 배수).

고정 켈리 페널티(예: 50%) 하드코딩을 폐기하고, 각 템플릿의 실전 승/패를 Beta(α,β)
분포로 모델링하여 톰슨 샘플링으로 켈리 배수를 동적으로 정한다.

    승격 시: p ~ Beta(α0, β0)  (α0=1+섀도우승, β0=1+섀도우패) → 초기 배수
    1건 청산마다 베이지안 갱신: 승 → α+=1, 패 → β+=1
    운영 배수 = MULT_MIN + (MULT_MAX-MULT_MIN) · 사후평균(α/(α+β))

실력이 압도적이면 사후평균↑ → 배수가 MULT_MAX(2.0)로 기하급수 접근,
부진하면 사후평균↓ → 배수가 MULT_MIN(0.1)로 수렴하여 기계가 스스로 자본 밸브를 잠근다.

상태 SSOT: system_config["TEMPLATE_BANDIT_STATE"][template_name] = {alpha,beta,mult,n,...}
"""
from __future__ import annotations

import random
from datetime import datetime
from typing import Any, Dict, Optional

BANDIT_KEY = "TEMPLATE_BANDIT_STATE"

PRIOR_A = 1.0
PRIOR_B = 1.0
MULT_MIN = 0.10   # 부진 시 자본 밸브 하한
MULT_MAX = 2.00   # 압도적 실력 시 기하급수 상한


def _mult_from_p(p: float) -> float:
    p = max(0.0, min(1.0, float(p)))
    return MULT_MIN + (MULT_MAX - MULT_MIN) * p


def posterior_mean(alpha: float, beta: float) -> float:
    a = float(alpha)
    b = float(beta)
    return a / (a + b) if (a + b) > 0 else 0.5


def beta_sample(alpha: float, beta: float) -> float:
    try:
        return random.betavariate(max(1e-6, float(alpha)), max(1e-6, float(beta)))
    except ValueError:
        return 0.5


def _state(cfg: Dict[str, Any]) -> Dict[str, Any]:
    st = cfg.get(BANDIT_KEY)
    if not isinstance(st, dict):
        st = {}
        cfg[BANDIT_KEY] = st
    return st


def init_bandit(
    cfg: Dict[str, Any],
    name: str,
    *,
    shadow_wins: int = 0,
    shadow_losses: int = 0,
) -> Dict[str, Any]:
    """승격 직후 1회 — 섀도우 승/패를 사전 모수로 Beta 생성 후 초기 배수 샘플링."""
    st = _state(cfg)
    a = PRIOR_A + max(0, int(shadow_wins))
    b = PRIOR_B + max(0, int(shadow_losses))
    p0 = beta_sample(a, b)
    rec = {
        "alpha": a,
        "beta": b,
        "n": 0,
        "init_sample": round(p0, 4),
        "mult": round(_mult_from_p(p0), 4),
        "graduated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    st[name] = rec
    return rec


def resolve_template_multiplier(cfg: Optional[Dict[str, Any]], sig_type: Any) -> float:
    """sig_type 에 포함된 모든 밴딧 관리 템플릿의 배수 곱(없으면 1.0). 무 I/O — sizing 훅용."""
    if not isinstance(cfg, dict):
        return 1.0
    st = cfg.get(BANDIT_KEY)
    if not isinstance(st, dict) or not st:
        return 1.0
    sig = str(sig_type or "")
    mult = 1.0
    for name, rec in st.items():
        if name and name in sig and isinstance(rec, dict):
            try:
                mult *= float(rec.get("mult", 1.0) or 1.0)
            except (TypeError, ValueError):
                continue
    return float(mult)


def update_bandit(cfg: Dict[str, Any], sig_type: Any, won: bool) -> Optional[Dict[str, Any]]:
    """청산 1건마다 베이지안 갱신 — sig_type 에 매칭되는 모든 밴딧 템플릿."""
    st = cfg.get(BANDIT_KEY)
    if not isinstance(st, dict) or not st:
        return None
    sig = str(sig_type or "")
    updated: Optional[Dict[str, Any]] = None
    for name, rec in st.items():
        if name and name in sig and isinstance(rec, dict):
            if won:
                rec["alpha"] = float(rec.get("alpha", PRIOR_A)) + 1.0
            else:
                rec["beta"] = float(rec.get("beta", PRIOR_B)) + 1.0
            rec["n"] = int(rec.get("n", 0)) + 1
            rec["mult"] = round(_mult_from_p(posterior_mean(rec["alpha"], rec["beta"])), 4)
            rec["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated = rec
    return updated


def strangle_template(
    cfg: Dict[str, Any],
    name: str,
    *,
    reason: str = "fail_fast",
    mult: float = MULT_MIN,
) -> Optional[Dict[str, Any]]:
    """
    [즉각 처벌 — Capital Strangle] 해당 템플릿의 켈리 배수를 즉시 하한(0.1)으로 잠근다.
    밴딧 레코드가 없으면 새로 만들어 잠금 상태로 기록(스캐너가 해당 sig 를 들고 와도 자본 차단).
    """
    if not isinstance(cfg, dict) or not name:
        return None
    st = _state(cfg)
    rec = st.get(name)
    if not isinstance(rec, dict):
        rec = {"alpha": PRIOR_A, "beta": PRIOR_B, "n": 0}
        st[name] = rec
    rec["mult"] = round(float(mult), 4)
    rec["strangled"] = True
    rec["strangle_reason"] = str(reason)
    rec["strangled_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rec["updated_at"] = rec["strangled_at"]
    return rec


def enforce_capital_strangle(
    cfg: Dict[str, Any],
    name: str,
    *,
    n: int,
    wins: int,
    profit_factor: float,
    min_samples: int = 5,
    wr_floor: float = 0.40,
    pf_floor: float = 1.0,
) -> Optional[Dict[str, Any]]:
    """
    실전 표본 ≥ min_samples 이고 (승률 < wr_floor 또는 PF < pf_floor) 면 즉각 Capital Strangle.
    반환: 처벌된 레코드(처벌 안 하면 None).
    """
    if int(n) < int(min_samples):
        return None
    wr = (float(wins) / float(n)) if n else 0.0
    if wr < float(wr_floor) or float(profit_factor) < float(pf_floor):
        return strangle_template(
            cfg, name, reason=f"wr={wr:.2f}<{wr_floor} or pf={profit_factor:.2f}<{pf_floor}"
        )
    return None


def update_bandit_for_closure(sig_type: Any, won: bool) -> Optional[Dict[str, Any]]:
    """ledger 청산 훅 — config 로드→갱신→원자 저장(밴딧 템플릿이 없으면 무동작)."""
    try:
        from config_manager import load_system_config, update_system_config

        cfg = load_system_config()
        st = cfg.get(BANDIT_KEY)
        if not isinstance(st, dict) or not st:
            return None
        if not any(n and n in str(sig_type or "") for n in st.keys()):
            return None
        rec = update_bandit(cfg, sig_type, won)
        if rec is not None:
            update_system_config({BANDIT_KEY: cfg[BANDIT_KEY]})
        return rec
    except Exception:
        return None

"""
Thompson Sampling Bandit — 승격 루키 템플릿의 자율 자본 배분(켈리 배수).

고정 켈리 페널티(예: 50%) 하드코딩을 폐기하고, 각 템플릿의 실전 승/패를 Beta(α,β)
분포로 모델링하여 톰슨 샘플링으로 켈리 배수를 동적으로 정한다.

    승격 시: p ~ Beta(α0, β0)  (α0=1+섀도우승, β0=1+섀도우패) → 초기 배수
    1건 청산마다 베이지안 갱신: 승 → α+=1, 패 → β+=1
    운영 배수 = MULT_MIN + (MULT_MAX-MULT_MIN) · 사후평균(α/(α+β))

실력이 압도적이면 사후평균↑ → 배수가 MULT_MAX(2.0)로 기하급수 접근,
부진하면 사후평균↓ → 배수가 MULT_MIN(0.1)로 수렴하여 기계가 스스로 자본 밸브를 잠근다.

ContextualLinUCB — MAB 자본 배분용 문맥 밴딧(MABCapitalAllocator 연동):
    build_context_vector → allocation_weights → 템플릿별 켈리 승수
    save_bandit_state / load_bandit_state → sys_config JSON 영속성

상태 SSOT:
    system_config["TEMPLATE_BANDIT_STATE"][template_name] = {alpha,beta,mult,n,...}
    system_config["CONTEXTUAL_LINUCB_STATE"][market] = {A,A_inv,b,update_counts,...}
"""
from __future__ import annotations

import json
import logging
import math
import os
import random
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

import numpy as np

from contextual_linucb import ContextualLinUCB

BANDIT_KEY = "TEMPLATE_BANDIT_STATE"
LINUCB_STATE_KEY = "CONTEXTUAL_LINUCB_STATE"
CONTEXT_DIM = 4
LINUCB_DEFAULT_ALPHA = 0.5
LINUCB_DEFAULT_RIDGE = 1.0
LINUCB_DEFAULT_REWARD_CLIP = 0.20

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


def init_exploration_arm(
    cfg: Dict[str, Any],
    name: str,
    *,
    mult: float = MULT_MIN,
) -> Dict[str, Any]:
    """
    [Mission 5] 갓 승격된 유전자 돌연변이를 '탐색 모드 최소 켈리'로만 실전 투입.
    초기 켈리 배수를 하한(MULT_MIN)으로 잠가두고, 실전 청산이 쌓이면 update_bandit 이
    베이지안 사후평균으로 배수를 스스로 끌어올린다(가치 증명 시 자본 확대).
    """
    if not isinstance(cfg, dict) or not name:
        return {}
    st = _state(cfg)
    rec = {
        "alpha": PRIOR_A,
        "beta": PRIOR_B,
        "n": 0,
        "mult": round(max(MULT_MIN, float(mult)), 4),
        "exploration": True,
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


def update_bandit_for_closure(
    sig_type: Any,
    won: bool,
    *,
    reward: Optional[float] = None,
    market: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """ledger 청산 훅 — Beta 밴딧 + ContextualLinUCB 동시 갱신·원자 저장."""
    try:
        from config_manager import load_system_config, update_system_config

        cfg = load_system_config()
        st = cfg.get(BANDIT_KEY)
        updated: Optional[Dict[str, Any]] = None
        if isinstance(st, dict) and st and any(
            n and n in str(sig_type or "") for n in st.keys()
        ):
            updated = update_bandit(cfg, sig_type, won)

        linucb_updated = False
        if reward is not None:
            linucb_updated = update_linucb_on_closure(
                cfg,
                sig_type,
                float(reward),
                market=market,
            )

        patches: Dict[str, Any] = {}
        if updated is not None:
            patches[BANDIT_KEY] = cfg[BANDIT_KEY]
        if linucb_updated:
            patches[LINUCB_STATE_KEY] = cfg[LINUCB_STATE_KEY]
        if patches:
            update_system_config(patches)
        return updated
    except Exception:
        return None


# ---------------------------------------------------------------------------
# ContextualLinUCB — 문맥 벡터 · 영속성 · 활성 DNA Arm
# ---------------------------------------------------------------------------
def _resolve_vix(cfg: Dict[str, Any]) -> float:
    """MACRO_DAILY_LATEST → DOOMSDAY_DEFCON 순으로 VIX 추출."""
    macro = cfg.get("MACRO_DAILY_LATEST") or {}
    if isinstance(macro, dict):
        for key in ("vix_index", "vix", "VIX"):
            try:
                val = float(macro.get(key) or 0.0)
                if val > 0.0:
                    return val
            except (TypeError, ValueError):
                continue

    dd = cfg.get("DOOMSDAY_DEFCON") or {}
    if isinstance(dd, dict):
        candidates = [
            dd.get("vix_index"),
            dd.get("vix"),
            dd.get("vix_last"),
        ]
        metrics = dd.get("metrics")
        if isinstance(metrics, dict):
            candidates.extend(
                metrics.get(k) for k in ("vix_index", "vix", "VIX", "^VIX")
            )
        zscores = dd.get("z_scores_latest")
        if isinstance(zscores, dict):
            candidates.extend(
                zscores.get(k) for k in ("vix", "vix_index", "^VIX", "VIX")
            )
        for raw in candidates:
            try:
                val = float(raw)
                if val > 0.0:
                    return val
            except (TypeError, ValueError):
                continue
    return 20.0


def _is_bear_regime(regime_key: Any) -> bool:
    rk = str(regime_key or "").strip().upper()
    return rk == "BEAR" or rk.startswith("BEAR_") or "_BEAR" in rk or rk.endswith("_BEAR")


def build_context_vector(
    sys_config: Dict[str, Any],
    market: str,
) -> np.ndarray:
    """
    LinUCB 문맥 — 4차원:
        [0] tanh((VIX-20)/10)  — 변동성 공포
        [1] BEAR 계열 여부 (1.0/0.0)
        [2] bias (1.0)
        [3] tanh(idle_cash) — 글로벌 켈리 압착 잔여 현금
    """
    _ = str(market or "KR").upper()
    cfg = sys_config if isinstance(sys_config, dict) else {}
    vix = _resolve_vix(cfg)
    vix_norm = math.tanh((vix - 20.0) / 10.0)

    regime = cfg.get("META_REGIME_KEY")
    if not regime:
        regime = (cfg.get("REGIME_ANALYSIS") or {}).get("regime_key")
    bear_flag = 1.0 if _is_bear_regime(regime) else 0.0

    try:
        global_kelly = float(cfg.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    except (TypeError, ValueError):
        global_kelly = 1.0
    idle_cash = max(0.0, 1.0 - global_kelly)
    idle_norm = math.tanh(idle_cash * 3.0)

    return np.array([vix_norm, bear_flag, 1.0, idle_norm], dtype=np.float64)


def _linucb_store(cfg: Dict[str, Any]) -> Dict[str, Any]:
    st = cfg.get(LINUCB_STATE_KEY)
    if not isinstance(st, dict):
        st = {}
        cfg[LINUCB_STATE_KEY] = st
    return st


def enumerate_active_dna_templates(
    sys_config: Dict[str, Any],
    market: str,
) -> List[str]:
    """활성 DNA 템플릿 이름 — LinUCB arm 순서 SSOT."""
    mk = str(market or "KR").upper()
    cfg = sys_config if isinstance(sys_config, dict) else {}
    names: List[str] = []
    seen: set[str] = set()

    def _add(name: Any) -> None:
        n = str(name or "").strip()
        if not n or n in seen:
            return
        seen.add(n)
        names.append(n)

    base_store = cfg.get("DNA_BASE_TEMPLATES") or {}
    if isinstance(base_store, dict):
        region = base_store.get(mk) or {}
        if isinstance(region, dict):
            for k in sorted(region.keys()):
                _add(k)

    bandit_st = cfg.get(BANDIT_KEY) or {}
    if isinstance(bandit_st, dict):
        for k in sorted(bandit_st.keys()):
            _add(k)

    multi_key = f"DNA_SUPERNOVA_{mk}_MULTI"
    pool = cfg.get(multi_key) or {}
    if isinstance(pool, dict):
        for k in sorted(pool.keys()):
            _add(k)

    deep_reg = cfg.get("DEEP_EVOLVED_DEPLOYED") or {}
    if isinstance(deep_reg, dict):
        for name, meta in sorted(deep_reg.items()):
            if isinstance(meta, dict) and str(meta.get("market", mk)).upper() == mk:
                _add(name)

    inc = cfg.get("INCUBATOR_TEMPLATES") or {}
    if isinstance(inc, dict):
        for k in sorted(inc.keys()):
            _add(k)

    prefix = "NEW_EVOLUTION_"
    for key in sorted(cfg.keys()):
        if isinstance(key, str) and key.startswith(prefix) and mk in key.upper():
            _add(key)

    grad = cfg.get("GRADUATED_FORENSIC_TEMPLATE")
    if isinstance(grad, str) and grad:
        _add(grad)

    return names


def save_bandit_state(
    bandit: ContextualLinUCB,
    cfg: Dict[str, Any],
    market: str,
    arm_names: Sequence[str],
) -> Dict[str, Any]:
    """LinUCB 학습 행렬을 sys_config JSON 직렬화 블록으로 저장."""
    mk = str(market or "KR").upper()
    block = {
        "arm_names": list(arm_names),
        "n_arms": int(bandit.n_arms),
        "context_dim": int(bandit.context_dim),
        "exploration_alpha": float(bandit.exploration_alpha),
        "ridge_alpha": float(bandit.ridge_alpha),
        "reward_clip": bandit.reward_clip,
        "A": np.asarray(bandit.A).tolist(),
        "A_inv": np.asarray(bandit.A_inv).tolist(),
        "b": np.asarray(bandit.b).tolist(),
        "update_counts": np.asarray(bandit.update_counts).tolist(),
        "cumulative_rewards": np.asarray(bandit.cumulative_rewards).tolist(),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _linucb_store(cfg)[mk] = block
    return block


def _merge_bandit_arms(
    bandit: ContextualLinUCB,
    old_names: Sequence[str],
    old_block: Dict[str, Any],
    new_names: Sequence[str],
) -> ContextualLinUCB:
    """Arm 목록 변경 시 겹치는 템플릿 학습량만 이전."""
    if not old_names or not old_block:
        return bandit

    name_to_old_idx = {str(n): i for i, n in enumerate(old_names)}
    try:
        old_a = np.asarray(old_block.get("A"), dtype=bandit.dtype)
        old_a_inv = np.asarray(old_block.get("A_inv"), dtype=bandit.dtype)
        old_b = np.asarray(old_block.get("b"), dtype=bandit.dtype)
        old_counts = np.asarray(old_block.get("update_counts"), dtype=np.int64)
        old_cum = np.asarray(old_block.get("cumulative_rewards"), dtype=bandit.dtype)
    except (TypeError, ValueError):
        return bandit

    for new_i, name in enumerate(new_names):
        old_i = name_to_old_idx.get(str(name))
        if old_i is None or old_i >= len(old_a):
            continue
        bandit.A[new_i] = old_a[old_i]
        bandit.A_inv[new_i] = old_a_inv[old_i]
        bandit.b[new_i] = old_b[old_i]
        bandit.update_counts[new_i] = int(old_counts[old_i])
        bandit.cumulative_rewards[new_i] = float(old_cum[old_i])
    return bandit


def load_bandit_state(
    cfg: Dict[str, Any],
    market: str,
    arm_names: Sequence[str],
    *,
    context_dim: int = CONTEXT_DIM,
    exploration_alpha: float = LINUCB_DEFAULT_ALPHA,
    ridge_alpha: float = LINUCB_DEFAULT_RIDGE,
    reward_clip: float = LINUCB_DEFAULT_REWARD_CLIP,
) -> ContextualLinUCB:
    """sys_config JSON → ContextualLinUCB 복원(arm 목록 변경 시 공통 arm만 이전)."""
    mk = str(market or "KR").upper()
    names = list(arm_names)
    n_arms = max(1, len(names))

    block = (_linucb_store(cfg).get(mk) or {}) if isinstance(cfg, dict) else {}
    exp_alpha = float(block.get("exploration_alpha", exploration_alpha) or exploration_alpha)
    ridge = float(block.get("ridge_alpha", ridge_alpha) or ridge_alpha)
    clip = block.get("reward_clip", reward_clip)
    clip_f = float(clip) if clip is not None else None

    bandit = ContextualLinUCB(
        n_arms=n_arms,
        context_dim=int(block.get("context_dim", context_dim) or context_dim),
        exploration_alpha=exp_alpha,
        ridge_alpha=ridge,
        reward_clip=clip_f,
    )

    if not block or block.get("n_arms") != n_arms:
        old_names = block.get("arm_names") or []
        return _merge_bandit_arms(bandit, old_names, block, names)

    try:
        bandit.A = np.asarray(block["A"], dtype=bandit.dtype)
        bandit.A_inv = np.asarray(block["A_inv"], dtype=bandit.dtype)
        bandit.b = np.asarray(block["b"], dtype=bandit.dtype)
        bandit.update_counts = np.asarray(block["update_counts"], dtype=np.int64)
        bandit.cumulative_rewards = np.asarray(
            block.get("cumulative_rewards") or np.zeros(n_arms),
            dtype=bandit.dtype,
        )
        saved_names = block.get("arm_names") or []
        if list(saved_names) != names:
            bandit = _merge_bandit_arms(bandit, saved_names, block, names)
    except (KeyError, TypeError, ValueError):
        old_names = block.get("arm_names") or []
        bandit = _merge_bandit_arms(bandit, old_names, block, names)

    return bandit


def _match_arm_index(arm_names: Sequence[str], sig_type: Any) -> Optional[int]:
    sig = str(sig_type or "")
    if not sig:
        return None
    best: Optional[tuple[int, int]] = None
    for i, name in enumerate(arm_names):
        n = str(name or "")
        if not n or n not in sig:
            continue
        rank = (len(n), i)
        if best is None or rank > best:
            best = rank
    return best[1] if best else None


def update_linucb_on_closure(
    cfg: Dict[str, Any],
    sig_type: Any,
    reward: float,
    *,
    market: Optional[str] = None,
) -> bool:
    """청산 수익률로 ContextualLinUCB arm 갱신 + 상태 저장."""
    if not isinstance(cfg, dict):
        return False
    mk = str(market or cfg.get("MAB_LAST_MARKET") or "KR").upper()
    arm_names = enumerate_active_dna_templates(cfg, mk)
    if not arm_names:
        return False

    arm_idx = _match_arm_index(arm_names, sig_type)
    if arm_idx is None:
        return False

    bandit = load_bandit_state(cfg, mk, arm_names)
    context = build_context_vector(cfg, mk)
    bandit.update(arm_idx, context, float(reward))
    save_bandit_state(bandit, cfg, mk, arm_names)
    return True


# ---------------------------------------------------------------------------
# LinUCB 리스크 조정 보상 · forward_trades 배치 피드백
# ---------------------------------------------------------------------------
def calculate_trade_reward(
    final_ret: float,
    mae: float,
    bars_held: int,
    *,
    mae_weight: float = 0.50,
    time_weight: float = 0.10,
    reward_scale: float = 10.0,
) -> float:
    """
    개별 청산 거래의 LinUCB용 리스크 조정 보상을 계산한다.

    U = final_ret - mae_weight*|MAE| - time_weight*log(1+bars_held)
    Reward = clip(tanh(U / reward_scale), -1, 1)
    """
    try:
        ret = float(final_ret)
        adverse_excursion = abs(float(mae))
        holding_bars = float(bars_held)
        mae_penalty_weight = float(mae_weight)
        time_penalty_weight = float(time_weight)
        scale = float(reward_scale)
    except (TypeError, ValueError, OverflowError):
        return 0.0

    values = np.array(
        [
            ret,
            adverse_excursion,
            holding_bars,
            mae_penalty_weight,
            time_penalty_weight,
            scale,
        ],
        dtype=np.float64,
    )

    if not np.all(np.isfinite(values)):
        return 0.0

    if holding_bars < 0.0 or scale <= 0.0:
        return 0.0

    mae_penalty_weight = max(0.0, mae_penalty_weight)
    time_penalty_weight = max(0.0, time_penalty_weight)

    mae_penalty = mae_penalty_weight * adverse_excursion
    time_penalty = time_penalty_weight * np.log1p(holding_bars)

    raw_utility = ret - mae_penalty - time_penalty
    normalized_reward = np.tanh(raw_utility / scale)

    return float(np.clip(normalized_reward, -1.0, 1.0))


def _bandit_db_path() -> str:
    try:
        from market_db_paths import report_db_read_path

        return report_db_read_path()
    except Exception:
        try:
            from market_db_paths import MARKET_DATA_DB_PATH

            return MARKET_DATA_DB_PATH
        except Exception:
            from factory_data_paths import factory_data_dir

            return os.path.join(factory_data_dir(), "market_data.sqlite")


def _feed_watermark(cfg: Dict[str, Any], market: str) -> Tuple[int, str]:
    """LinUCB 블록에 저장된 마지막 배치 피드 워터마크."""
    block = (_linucb_store(cfg).get(str(market or "KR").upper()) or {})
    last_id = int(block.get("last_feed_trade_id") or 0)
    last_at = str(block.get("last_feed_at") or "")[:10]
    return last_id, last_at


def _extract_facts_from_trade(trade: dict) -> dict:
    """forward_trades 행에서 facts 유사 dict 추출(flow_tags JSON·컬럼 fallback)."""
    facts: Dict[str, Any] = {}
    flow = str(trade.get("flow_tags") or "").strip()
    if flow.startswith("{") and flow.endswith("}"):
        try:
            parsed = json.loads(flow)
            if isinstance(parsed, dict):
                facts.update(parsed)
        except json.JSONDecodeError:
            pass

    for col in (
        "entry_regime",
        "entry_breadth",
        "market_breadth",
        "dyn_rs",
        "dyn_cpv",
        "dyn_tb",
        "v_energy",
    ):
        if trade.get(col) is not None:
            facts.setdefault(col, trade[col])
    return facts


def _build_entry_context_vector(
    trade: dict,
    sys_config: dict,
    market: str,
) -> np.ndarray:
    """
    진입 시점 VIX·국면·(facts/_sys_config) 스냅샷으로 build_context_vector 재구성.
    진입 데이터가 없으면 현재 sys_config 문맥으로 근사한다.
    """
    cfg = dict(sys_config) if isinstance(sys_config, dict) else {}
    facts = _extract_facts_from_trade(trade)

    vix_val: Optional[float] = None
    for key in ("vix_index", "vix", "VIX"):
        try:
            v = float(facts.get(key) or 0)
            if v > 0:
                vix_val = v
                break
        except (TypeError, ValueError):
            continue

    snap = facts.get("_sys_config")
    if isinstance(snap, dict):
        snap_vix = _resolve_vix(snap)
        if snap_vix > 0 and (vix_val is None or snap_vix != 20.0):
            vix_val = snap_vix
        gk = snap.get("META_GLOBAL_KELLY_MULT")
        if gk is not None:
            try:
                cfg["META_GLOBAL_KELLY_MULT"] = float(gk)
            except (TypeError, ValueError):
                pass
        rk = snap.get("META_REGIME_KEY") or (snap.get("REGIME_ANALYSIS") or {}).get(
            "regime_key"
        )
        if rk and str(rk).strip().upper() not in ("", "UNKNOWN"):
            cfg["META_REGIME_KEY"] = str(rk)

    if vix_val is not None and vix_val > 0:
        macro = dict(cfg.get("MACRO_DAILY_LATEST") or {})
        macro["vix_index"] = vix_val
        cfg["MACRO_DAILY_LATEST"] = macro

    regime = facts.get("entry_regime") or trade.get("entry_regime")
    if regime and str(regime).strip().upper() not in ("", "UNKNOWN"):
        cfg["META_REGIME_KEY"] = str(regime)

    breadth = facts.get("entry_breadth")
    if breadth is None:
        breadth = trade.get("entry_breadth")
    if breadth is not None:
        try:
            cfg.setdefault("BANDIT_ENTRY_BREADTH_HINT", float(breadth))
        except (TypeError, ValueError):
            pass

    return build_context_vector(cfg, market)


def _compute_mae_pct(trade: dict) -> float:
    """min_low·entry_price 기반 MAE(%)."""
    try:
        ep = float(trade.get("entry_price") or 0)
        low = float(trade.get("min_low") if trade.get("min_low") is not None else ep)
        if ep <= 0:
            return 0.0
        return (low - ep) / ep * 100.0
    except (TypeError, ValueError):
        return 0.0


def _load_closed_trades_for_bandit_feed(
    db_path: str,
    *,
    market: str,
    cutoff_date: str,
    min_trade_id: int,
) -> List[dict]:
    """market_data.sqlite forward_trades — CLOSED% 청산 + 워터마크 이후."""
    uri = str(db_path).replace("\\", "/")
    conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT *
            FROM forward_trades
            WHERE market = ?
              AND status LIKE 'CLOSED%'
              AND id > ?
              AND substr(
                    IFNULL(NULLIF(TRIM(exit_date), ''), entry_date),
                    1,
                    10
                  ) >= ?
            ORDER BY id ASC
            """,
            (str(market or "KR").upper(), int(min_trade_id), str(cutoff_date)[:10]),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def feed_rewards_to_bandit(
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    db_path: Optional[str] = None,
    lookback_days: int = 7,
    markets: Optional[Sequence[str]] = None,
    persist: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    forward_trades 청산 원장 → calculate_trade_reward → LinUCB 배치 갱신.

    조회 창: max(lookback_days, 마지막 feed_rewards_to_bandit 이후) + id 워터마크.
    완료 후 save_bandit_state + system_config 영속화.
    """
    own_cfg = sys_config is None
    cfg: Dict[str, Any]
    if own_cfg:
        try:
            from config_manager import load_system_config

            cfg = dict(load_system_config() or {})
        except Exception:
            cfg = {}
    else:
        cfg = sys_config  # type: ignore[assignment]

    now = now or datetime.now()
    seven_cutoff = (now - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    db = db_path or _bandit_db_path()
    target_markets = [
        str(m).upper()
        for m in (markets or ("KR", "US"))
        if str(m).strip()
    ]

    summary: Dict[str, Any] = {
        "updated": 0,
        "skipped_no_arm": 0,
        "skipped_bad_reward": 0,
        "markets": {},
        "persisted": False,
        "db_path": db,
        "lookback_days": lookback_days,
    }

    if not os.path.isfile(db):
        summary["error"] = f"db_not_found:{db}"
        return summary

    for mk in target_markets:
        arm_names = enumerate_active_dna_templates(cfg, mk)
        if not arm_names:
            summary["markets"][mk] = {"arms": 0, "updated": 0}
            continue

        last_id, last_at = _feed_watermark(cfg, mk)
        cutoff = seven_cutoff
        if last_at and last_at > cutoff:
            cutoff = last_at[:10]

        trades: List[dict] = []
        load_error = ""
        try:
            trades = _load_closed_trades_for_bandit_feed(
                db,
                market=mk,
                cutoff_date=cutoff,
                min_trade_id=last_id,
            )
        except Exception as ex:
            load_error = str(ex)
            logger.warning("feed_rewards_to_bandit load failed market=%s: %s", mk, ex)

        bandit = load_bandit_state(cfg, mk, arm_names)
        mkt_updated = 0
        mkt_skipped_arm = 0
        mkt_skipped_reward = 0
        local_max_id = last_id

        for trade in trades:
            tid = int(trade.get("id") or 0)
            local_max_id = max(local_max_id, tid)

            arm_idx = _match_arm_index(arm_names, trade.get("sig_type"))
            if arm_idx is None:
                mkt_skipped_arm += 1
                continue

            try:
                final_ret = float(trade.get("final_ret") or 0.0)
                mae = _compute_mae_pct(trade)
                bars = int(trade.get("bars_held") or 0)
            except (TypeError, ValueError):
                mkt_skipped_reward += 1
                continue

            reward = calculate_trade_reward(
                final_ret=final_ret,
                mae=mae,
                bars_held=bars,
            )
            if not np.isfinite(reward):
                mkt_skipped_reward += 1
                continue

            context = _build_entry_context_vector(trade, cfg, mk)
            bandit.update(arm_idx, context, reward)
            mkt_updated += 1

        block = save_bandit_state(bandit, cfg, mk, arm_names)
        block["last_feed_trade_id"] = int(local_max_id)
        block["last_feed_at"] = now.strftime("%Y-%m-%d %H:%M:%S")

        summary["updated"] += mkt_updated
        summary["skipped_no_arm"] += mkt_skipped_arm
        summary["skipped_bad_reward"] += mkt_skipped_reward
        summary["markets"][mk] = {
            "arms": len(arm_names),
            "loaded": len(trades),
            "updated": mkt_updated,
            "skipped_no_arm": mkt_skipped_arm,
            "skipped_bad_reward": mkt_skipped_reward,
            "cutoff_date": cutoff,
            "last_feed_trade_id": local_max_id,
            "load_error": load_error,
        }

    if persist and summary["updated"] > 0:
        try:
            from config_manager import update_system_config

            update_system_config({LINUCB_STATE_KEY: cfg[LINUCB_STATE_KEY]})
            summary["persisted"] = True
        except Exception as ex:
            summary["persist_error"] = str(ex)
            logger.warning("feed_rewards_to_bandit persist failed: %s", ex)

    return summary

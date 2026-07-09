"""
Meta-DNA Mutator — 주말 생물학적 교차/변이로 템플릿 정체를 깬다.

성공·활성 DNA 에 3~5% 가우시안 비틀기 → INCUBATOR_TEMPLATES / MUTANT_GENE_POOL
가상 장부에서 스카우트·인큐베이터 경로로 검증.
"""
from __future__ import annotations

import copy
import hashlib
import random
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_DNA_KEYS = ("cpv", "tb", "bbe", "rs", "cos_cutoff")
_FLOAT_BOUNDS = {
    "cpv": (0.05, 2.5),
    "tb": (0.05, 50.0),
    "bbe": (0.05, 5.0),
    "rs": (50.0, 300.0),
    "cos_cutoff": (0.45, 0.92),
}

# 계좌 치명 파라미터 — 돌연변이 시 절대 한계선 (Mutation Hard Boundaries)
MUTATION_HARD_BOUNDARIES: Dict[str, Tuple[float, float]] = {
    "stop_loss_pct": (0.02, 0.12),
    "stop_loss": (0.02, 0.12),
    "leverage": (1.0, 2.0),
    "kelly_risk_pct": (0.005, 0.04),
    "max_position_pct": (0.01, 0.15),
    "max_drawdown_pct": (0.05, 0.25),
    "trailing_stop_pct": (0.01, 0.10),
}


def _clip_key(key: str, val: float) -> float:
    lo, hi = _FLOAT_BOUNDS.get(key, (0.0, 999.0))
    if key in MUTATION_HARD_BOUNDARIES:
        hlo, hhi = MUTATION_HARD_BOUNDARIES[key]
        lo, hi = max(lo, hlo), min(hi, hhi)
    return max(lo, min(hi, float(val)))


def apply_mutation_hard_boundaries(template: Dict[str, Any]) -> Dict[str, Any]:
    """돌연변이 후 치명 파라미터 재클램프."""
    out = dict(template)
    for key, (lo, hi) in MUTATION_HARD_BOUNDARIES.items():
        if key not in out:
            continue
        try:
            out[key] = round(max(lo, min(hi, float(out[key]))), 6)
        except (TypeError, ValueError):
            continue
    return out


def mutate_gene_value(key: str, value: float, *, rate: float = 0.04) -> float:
    """rate ≈ 3~5% 상대 변이 + 소량 절대 노이즈."""
    v = float(value)
    if key == "cos_cutoff":
        delta = random.gauss(0, rate * 0.5)
        return round(_clip_key(key, v + delta), 4)
    rel = random.gauss(1.0, rate)
    rel = max(0.90, min(1.10, rel))
    return round(_clip_key(key, v * rel), 4)


def mutate_dna_template(
    template: Dict[str, Any],
    *,
    rate: float = 0.04,
    name_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    out = copy.deepcopy(template)
    for k in _DNA_KEYS:
        if k in out:
            try:
                out[k] = mutate_gene_value(k, float(out[k]), rate=rate)
            except (TypeError, ValueError):
                continue
    out["status"] = "INCUBATING"
    out["mutated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["mutation_rate"] = rate
    if name_suffix:
        out["parent_lineage"] = str(name_suffix)
    return apply_mutation_hard_boundaries(out)


def _template_success_score(
    template_name: str,
    closed_rows: List[Tuple[str, float]],
) -> Tuple[float, int]:
    """sig_type 에 template_name 포함된 청산 표본으로 wr·pf 근사."""
    hits = [float(r) for sig, r in closed_rows if template_name in str(sig)]
    if not hits:
        return 0.0, 0
    wins = sum(1 for x in hits if x > 0)
    wr = wins / len(hits)
    return wr, len(hits)


def select_parents_for_mutation(
    sys_config: Dict[str, Any],
    market: str,
    *,
    min_wr: float = 0.48,
    min_n: int = 3,
    max_parents: int = 6,
) -> List[Tuple[str, Dict[str, Any]]]:
    mk = str(market or "KR").upper()
    multi_key = f"DNA_SUPERNOVA_{mk}_MULTI"
    pool = sys_config.get(multi_key) or {}
    if not isinstance(pool, dict) or not pool:
        return []

    closed: List[Tuple[str, float]] = []
    try:
        import os
        import sqlite3
        from market_db_paths import MARKET_DATA_DB_PATH

        if os.path.isfile(MARKET_DATA_DB_PATH):
            conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=20)
            try:
                rows = conn.execute(
                    """
                    SELECT sig_type, final_ret FROM forward_trades
                    WHERE market=? AND status LIKE 'CLOSED%'
                    ORDER BY rowid DESC LIMIT 800
                    """,
                    (mk,),
                ).fetchall()
                closed = [(str(a), float(b or 0)) for a, b in rows]
            finally:
                conn.close()
    except Exception:
        pass

    scored: List[Tuple[float, int, str, Dict[str, Any]]] = []
    for name, tpl in pool.items():
        if not isinstance(tpl, dict):
            continue
        wr, n = _template_success_score(str(name), closed)
        if n >= min_n and wr >= min_wr:
            scored.append((wr, n, str(name), tpl))
        elif n == 0 and str(tpl.get("status", "")).upper() != "ARCHIVED":
            scored.append((0.45, 0, str(name), tpl))

    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [(n, t) for _, _, n, t in scored[:max_parents]]


def select_champion_template(
    sys_config: Dict[str, Any],
    market: str,
    *,
    exclude_group: Optional[str] = None,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """교배용 챔피언 DNA — 승률 상위 1개."""
    ex = str(exclude_group or "").strip()
    parents = select_parents_for_mutation(sys_config, market)
    for name, tpl in parents:
        if ex and ex in str(name):
            continue
        return name, tpl
    mk = str(market or "KR").upper()
    pool = sys_config.get(f"DNA_SUPERNOVA_{mk}_MULTI") or {}
    if isinstance(pool, dict):
        for name, tpl in pool.items():
            if isinstance(tpl, dict) and ex not in str(name):
                return str(name), tpl
    return None


def diagnose_loser_from_closures(
    rows: List[Dict[str, Any]],
    *,
    loss_threshold_pct: float = -3.0,
) -> Dict[str, Any]:
    """
    섀도우/실전 패자 청산으로 실패 원인 추론.

    failure_mode:
      stop_too_tight | entry_too_aggressive | low_mfe_quick_sl | bleed_streak
    """
    if not rows:
        return {
            "failure_mode": "bleed_streak",
            "n_closed": 0,
            "n_loss": 0,
            "avg_bars_held": 0.0,
            "sl_ratio": 0.0,
            "avg_mfe_pct": 0.0,
            "avg_cpv": 0.0,
        }

    losses: List[Dict[str, Any]] = []
    for r in rows:
        try:
            ret = float(r.get("final_ret", 0) or 0)
        except (TypeError, ValueError):
            ret = 0.0
        if ret <= float(loss_threshold_pct):
            losses.append(r)

    n = len(rows)
    n_loss = len(losses)
    if n_loss == 0:
        return {
            "failure_mode": "bleed_streak",
            "n_closed": n,
            "n_loss": 0,
            "avg_bars_held": 0.0,
            "sl_ratio": 0.0,
            "avg_mfe_pct": 0.0,
            "avg_cpv": 0.0,
        }

    sl_hits = 0
    bars: List[float] = []
    mfes: List[float] = []
    cpvs: List[float] = []

    for r in losses:
        reason = str(r.get("exit_reason") or r.get("exit_rsn") or "").upper()
        if "손절" in reason or "SL" in reason or "STOP" in reason:
            sl_hits += 1
        try:
            bars.append(float(r.get("bars_held") or 0))
        except (TypeError, ValueError):
            pass
        try:
            ep = float(r.get("entry_price") or 0)
            mh = float(r.get("max_high") or ep)
            if ep > 0:
                mfes.append((mh - ep) / ep * 100.0)
        except (TypeError, ValueError):
            pass
        try:
            cpv = float(r.get("dyn_cpv") or r.get("v_cpv") or 0)
            if cpv:
                cpvs.append(cpv)
        except (TypeError, ValueError):
            pass

    sl_ratio = sl_hits / n_loss
    avg_bars = sum(bars) / len(bars) if bars else 0.0
    avg_mfe = sum(mfes) / len(mfes) if mfes else 0.0
    avg_cpv = sum(cpvs) / len(cpvs) if cpvs else 0.0

    if sl_ratio >= 0.55 and avg_bars <= 6.0 and avg_mfe < 3.0:
        mode = "stop_too_tight"
    elif avg_cpv >= 0.65 and sl_ratio >= 0.45:
        mode = "entry_too_aggressive"
    elif avg_mfe < 2.5 and sl_ratio >= 0.45:
        mode = "low_mfe_quick_sl"
    else:
        mode = "bleed_streak"

    return {
        "failure_mode": mode,
        "n_closed": n,
        "n_loss": n_loss,
        "avg_bars_held": round(avg_bars, 2),
        "sl_ratio": round(sl_ratio, 3),
        "avg_mfe_pct": round(avg_mfe, 2),
        "avg_cpv": round(avg_cpv, 3),
    }


def crossover_dna_templates(
    loser: Dict[str, Any],
    champion: Dict[str, Any],
    *,
    loser_weight: float = 0.55,
) -> Dict[str, Any]:
    """패자 DNA × 챔피언 DNA 선형 교배."""
    lw = max(0.35, min(0.70, float(loser_weight)))
    out = copy.deepcopy(loser)
    for k in _DNA_KEYS:
        if k not in loser or k not in champion:
            continue
        try:
            lv = float(loser[k])
            cv = float(champion[k])
        except (TypeError, ValueError):
            continue
        out[k] = _clip_key(k, lw * lv + (1.0 - lw) * cv)
    for key, (lo, hi) in MUTATION_HARD_BOUNDARIES.items():
        if key not in loser or key not in champion:
            continue
        try:
            lv = float(loser[key])
            cv = float(champion[key])
        except (TypeError, ValueError):
            continue
        out[key] = round(max(lo, min(hi, lw * lv + (1.0 - lw) * cv)), 6)
    out["mutation_kind"] = "crossover"
    out["crossover_loser_weight"] = round(lw, 3)
    return apply_mutation_hard_boundaries(out)


def mutate_dna_for_failure_diagnosis(
    template: Dict[str, Any],
    diagnosis: Dict[str, Any],
    *,
    rate: float = 0.05,
    name_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    """실패 모드별 강제 돌연변이 — 손절·진입·MFE 패턴 반영."""
    out = copy.deepcopy(template)
    mode = str(diagnosis.get("failure_mode") or "bleed_streak")

    def _bump_hard(key: str, mult: float, default: float) -> None:
        try:
            base = float(out.get(key, default))
        except (TypeError, ValueError):
            base = default
        lo, hi = MUTATION_HARD_BOUNDARIES.get(key, (0.0, 999.0))
        out[key] = round(max(lo, min(hi, base * mult)), 6)

    if mode == "stop_too_tight":
        _bump_hard("stop_loss_pct", 1.15, 0.045)
        _bump_hard("stop_loss", 1.15, 0.045)
        if "cos_cutoff" in out:
            out["cos_cutoff"] = _clip_key(
                "cos_cutoff", float(out["cos_cutoff"]) - 0.04
            )
        if "tb" in out:
            out["tb"] = mutate_gene_value("tb", float(out["tb"]), rate=rate * 1.8)
        if "trailing_stop_pct" in out:
            _bump_hard("trailing_stop_pct", 1.10, 0.04)
    elif mode == "entry_too_aggressive":
        if "cos_cutoff" in out:
            out["cos_cutoff"] = _clip_key(
                "cos_cutoff", float(out["cos_cutoff"]) + 0.05
            )
        if "cpv" in out:
            out["cpv"] = _clip_key("cpv", float(out["cpv"]) * 1.08)
        _bump_hard("kelly_risk_pct", 0.85, 0.015)
    elif mode == "low_mfe_quick_sl":
        if "tb" in out:
            out["tb"] = mutate_gene_value("tb", float(out["tb"]), rate=rate * 2.0)
        if "bbe" in out:
            out["bbe"] = _clip_key("bbe", float(out["bbe"]) * 0.92)
        _bump_hard("stop_loss_pct", 1.08, 0.04)
    else:
        _bump_hard("kelly_risk_pct", 0.80, 0.012)
        if "cos_cutoff" in out:
            out["cos_cutoff"] = _clip_key(
                "cos_cutoff", float(out["cos_cutoff"]) + 0.02
            )

    for k in _DNA_KEYS:
        if k in out:
            try:
                out[k] = mutate_gene_value(k, float(out[k]), rate=rate * 0.6)
            except (TypeError, ValueError):
                continue

    out["status"] = "INCUBATING"
    out["mutated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out["mutation_rate"] = rate
    out["mutation_kind"] = "forced_mutate"
    out["failure_mode"] = mode
    if name_suffix:
        out["parent_lineage"] = str(name_suffix)
    return apply_mutation_hard_boundaries(out)


def build_loser_child_mutant(
    loser_tpl: Dict[str, Any],
    diagnosis: Dict[str, Any],
    champion_tpl: Optional[Dict[str, Any]] = None,
    *,
    rate: float = 0.05,
    parent_label: str = "",
) -> Dict[str, Any]:
    """
    패자 부모 → (선택) 챔피언 교배 → 실패 모드 강제 변이 Child.
    bleed_streak / low_mfe 는 교배 우선, stop/entry 는 강제 변이 우선.
    """
    mode = str(diagnosis.get("failure_mode") or "bleed_streak")
    use_crossover = champion_tpl is not None and mode in (
        "bleed_streak",
        "low_mfe_quick_sl",
    )
    if use_crossover and champion_tpl is not None:
        blended = crossover_dna_templates(loser_tpl, champion_tpl, loser_weight=0.50)
        child = mutate_dna_for_failure_diagnosis(
            blended,
            diagnosis,
            rate=rate * 0.8,
            name_suffix=parent_label,
        )
        child["mutation_kind"] = "crossover+forced_mutate"
    else:
        child = mutate_dna_for_failure_diagnosis(
            loser_tpl,
            diagnosis,
            rate=rate,
            name_suffix=parent_label,
        )
    child["re_evolution"] = True
    child["failure_diagnosis"] = dict(diagnosis)
    return child


def run_weekend_dna_mutation_cycle(
    sys_config: Dict[str, Any],
    *,
    markets: Tuple[str, ...] = ("KR", "US"),
    mutation_rate: Optional[float] = None,
    max_mutants_per_market: int = 4,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    주말 자율조율 훅 — INCUBATOR_TEMPLATES + MUTANT_GENE_POOL 갱신.
  Returns (updated_config, log_lines).
    """
    cfg = copy.deepcopy(sys_config)
    rate = float(
        mutation_rate
        if mutation_rate is not None
        else cfg.get("DNA_MUTATION_RATE", 0.04) or 0.04
    )
    rate = max(0.03, min(0.05, rate))

    inc = cfg.get("INCUBATOR_TEMPLATES")
    if not isinstance(inc, dict):
        inc = {}
    else:
        inc = dict(inc)

    gene_pool = cfg.get("MUTANT_GENE_POOL")
    if not isinstance(gene_pool, dict):
        gene_pool = {}
    else:
        gene_pool = dict(gene_pool)

    logs: List[str] = []
    stamp = datetime.now().strftime("%y%m%d%H%M")

    for mk in markets:
        parents = select_parents_for_mutation(cfg, mk)
        created = 0
        for parent_name, parent_tpl in parents:
            if created >= max_mutants_per_market:
                break
            child = mutate_dna_template(parent_tpl, rate=rate, name_suffix=parent_name)
            h = hashlib.sha256(f"{mk}|{parent_name}|{stamp}|{created}".encode()).hexdigest()[:8]
            child_name = f"MUTANT_{mk}_{parent_name[:24]}_{h}"
            inc[child_name] = child
            gene_pool[child_name] = {
                "market": mk,
                "parent": parent_name,
                "rate": rate,
                "created_at": datetime.now().isoformat(),
            }
            created += 1
            logs.append(f"▪️ {mk} 돌연변이 {child_name} ← {parent_name} (rate={rate:.0%})")

        if created == 0:
            logs.append(f"▪️ {mk} 돌연변이 스킵 — 부모 템플릿/표본 부족")

    cfg["INCUBATOR_TEMPLATES"] = inc
    cfg["MUTANT_GENE_POOL"] = gene_pool
    cfg["DNA_MUTATION_LAST_RUN"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return cfg, logs

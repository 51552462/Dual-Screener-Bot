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

_DNA_KEYS = (
    "cpv", "tb", "bbe", "rs", "cos_cutoff",
    "mfe_atr_mult", "trail_atr_mult" # 👑 [미래 대응형] 고무줄 청산 유전자 (변동성 배수)
)
_FLOAT_BOUNDS = {
    "cpv": (0.05, 2.5),
    "tb": (0.05, 50.0),
    "bbe": (0.05, 5.0),
    "rs": (50.0, 300.0),
    "cos_cutoff": (0.45, 0.95),
    "mfe_atr_mult": (1.5, 15.0),   # ATR(하루 평균 진폭)의 1.5배 ~ 15배 목표
    "trail_atr_mult": (0.5, 5.0),  # ATR의 0.5배 ~ 5배 트레일링 래칫 조임
}

# 👑 [초월적 진화] 논리적 스위치 유전자 (Structural Boolean Genes)
# 소수점 비틀기를 넘어선 "조건부 폭격" 무기들입니다.
_STRUCTURAL_GENES = (
    "req_dart_event",       # DART 공시 호재 필수 여부
    "req_short_squeeze",    # 공매도 숏스퀴즈 발생 필수 여부
    "req_fund_good",        # 펀더멘털(저PBR/흑자) 우량 필수 여부
    "req_flow_div",         # 외인/기관 수급 다이버전스 필수 여부
)

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
    sys_config: Optional[Dict[str, Any]] = None, # 👑 관제탑(sys_config) 연결
) -> Dict[str, Any]:
    
    # 👑 [AI 진화 나침반] 부검소에서 만든 오답노트(Anti-Patterns) 로드
    try:
        from toxic_antipattern_core import evaluate_toxic_bbox_match, collect_merged_antipattern_rules
        merged_anti = collect_merged_antipattern_rules(sys_config) if sys_config else {}
    except Exception:
        merged_anti = {}

    best_out = None
    
    # 👑 최대 5세대에 걸친 자가 학습 루프 (독성 지대 회피)
    for attempt in range(5):
        out = copy.deepcopy(template)
        
        # 1. 연속형 변수(숫자) 미세 변이
        for k in _DNA_KEYS:
            if k in out:
                try:
                    out[k] = mutate_gene_value(k, float(out[k]), rate=rate)
                except (TypeError, ValueError):
                    continue
                    
        # 2. 논리 구조(스위치) 대규모 돌연변이 (Structural Mutation)
        for k in _STRUCTURAL_GENES:
            current_state = bool(out.get(k, False))
            if random.random() < 0.10:  
                out[k] = not current_state

        out = apply_mutation_hard_boundaries(out)

        # 3. 👑 오답노트 검증 (Anti-Pattern Repulsion)
        is_toxic = False
        if merged_anti:
            cpv = float(out.get("cpv", 0.0))
            tb = float(out.get("tb", 0.0))
            bbe = float(out.get("bbe", 0.0))
            rs = float(out.get("rs", 0.0))
            
            # 가상의 중립 섹터로 테스트하여 순수하게 '형태와 에너지' 자체의 맹독성만 판별
            for _, bounds in merged_anti.items():
                if not isinstance(bounds, dict): continue
                try:
                    if evaluate_toxic_bbox_match(bounds, cpv, tb, bbe, rs, "기타/혼합"):
                        is_toxic = True
                        break # 독성 감지! 이 돌연변이는 폐기하고 다음 시도로 넘어감
                except Exception:
                    pass
        
        # 독성이 없다면 완벽한 진화체로 채택하고 루프 종료
        if not is_toxic:
            best_out = out
            break
            
    # 5번의 시도에도 독성 지대를 빠져나오지 못했다면 강제 마킹 (관찰용)
    if best_out is None:
        best_out = out  
        best_out["mutation_kind"] = "structural_mutate_toxic_warning"
    else:
        best_out["mutation_kind"] = "structural_mutate"

    best_out["status"] = "INCUBATING"
    best_out["mutated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    best_out["mutation_rate"] = rate
    if name_suffix:
        best_out["parent_lineage"] = str(name_suffix)
        
    return best_out


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
    target_regime: str = "ALL",
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
                # 👑 [초월적 진화] 국면(Regime) 필터링 추가
                # target_regime이 주어지면, 해당 국면에서 진입해 수익을 낸 기록만 핀셋으로 뽑습니다.
                regime_cond = ""
                params = [mk]
                if target_regime != "ALL":
                    regime_cond = "AND IFNULL(entry_regime, '') LIKE ?"
                    params.append(f"%{target_regime}%")
                    
                rows = conn.execute(
                    f"""
                    SELECT sig_type, final_ret FROM forward_trades
                    WHERE market=? AND status LIKE 'CLOSED%' {regime_cond}
                    ORDER BY rowid DESC LIMIT 1500
                    """,
                    tuple(params),
                ).fetchall()
                closed = [(str(a), float(b or 0)) for a, b in rows]
            finally:
                conn.close()
    except Exception as e:
        print(f"⚠️ 부모 선발 DB 로드 실패: {e}")

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



# ===========================================================================
# 👑 [초월적 진화] 마이크로 태그 기반 엘리트 스핀오프 (Elite Spin-off) 발굴 엔진
# ===========================================================================
def mine_elite_spinoff_dna(market: str) -> List[Tuple[str, Dict[str, Any]]]:
    """
    평균의 함정(Averaging Trap)을 타파하는 핵심 엔진.
    수익을 낸 종목들의 5D DNA를 뽑아내는 동시에, 
    '변동성(ATR) 배수 역산'과 '실제 가격 궤적(Real Shape) 자가 학습'을 수행합니다.
    """
    mk = str(market or "KR").upper()
    elite_candidates: List[Tuple[str, Dict[str, Any]]] = []
    
    try:
        import os
        import sqlite3
        import numpy as np # 👑 동적 보간(Interpolation)을 위한 numpy 로드
        from market_db_paths import MARKET_DATA_DB_PATH

        if not os.path.isfile(MARKET_DATA_DB_PATH):
            return elite_candidates

        conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=20)
        try:
            # 1. 최근 수익 마감 종목 스캔 (code와 entry_date를 추가로 추출하여 과거 궤적을 찾습니다)
            rows = conn.execute(
                """
                SELECT sig_type, flow_tags, final_ret, dyn_cpv, v_cpv, dyn_tb, v_energy, dyn_rs, v_rs, entry_atr, entry_price, code, entry_date
                FROM forward_trades
                WHERE market=? AND status = 'CLOSED_WIN'
                  AND final_ret >= 5.0  
                  AND flow_tags IS NOT NULL
                ORDER BY final_ret DESC LIMIT 100
                """,
                (mk,),
            ).fetchall()
            
            elite_tags = ["#빠른슈팅_완벽", "#초신성_광기폭발_성공", "#건전한조정_매집우위", "#저득점_수급깡패_성공", "#미친매수세_잔류"]
            
            for row in rows:
                sig_type, flow_tags, final_ret, dyn_cpv, v_cpv, dyn_tb, v_energy, dyn_rs, v_rs, entry_atr, entry_price, code, entry_date = row
                tags = str(flow_tags or "")
                
                if any(tag in tags for tag in elite_tags):
                    cpv = float(dyn_cpv or v_cpv or 0.0)
                    tb = float(dyn_tb or 0.0)
                    bbe = float(v_energy or 0.0)
                    rs = float(dyn_rs or v_rs or 0.0)
                    
                    e_atr = float(entry_atr or 0.0)
                    e_price = float(entry_price or 0.0)
                    
                    if cpv > 0 and tb > 0 and bbe > 0:
                        base_logic = str(sig_type).split(']')[0].replace('[', '')[:10]
                        spinoff_name = f"ELITE_SPINOFF_{base_logic}_{int(final_ret)}pct"
                        
                        # 고무줄 청산 유전자 역산
                        mfe_atr_mult = 3.0 
                        if e_atr > 0 and e_price > 0:
                            profit_abs = e_price * (final_ret / 100.0)
                            mfe_atr_mult = min(max(profit_abs / e_atr, 1.5), 15.0)
                        trail_atr_mult = max(1.0, mfe_atr_mult * 0.25)
                        
                        # 👑 [형태(Shape) 자가 학습] 박제된 중립 궤적을 버리고 진짜 진입 궤적을 DB에서 추출합니다.
                        real_shape = [
                            0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50,
                            0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 1.00
                        ]
                        try:
                            import pandas as pd
                            # 👑 [안전장치 추가] 한국장은 무조건 6자리로 맞춰 '005930' 형태의 앞자리 증발을 막습니다.
                            safe_code = str(code).zfill(6) if mk == 'KR' else str(code)
                            tbl_name = f"{mk}_{safe_code}"
                            
                            # 진입일 직전 20거래일의 종가 패턴을 불러옵니다.
                            shape_df = pd.read_sql(
                                f'SELECT Close FROM "{tbl_name}" WHERE Date <= ? ORDER BY Date DESC LIMIT 20',
                                conn, params=(str(entry_date)[:10],)
                            )
                            if len(shape_df) >= 10:
                                c_vals = shape_df['Close'].values[::-1] # 과거에서 현재 순으로 정렬
                                c_min, c_max = np.min(c_vals), np.max(c_vals)
                                if c_max > c_min:
                                    c_norm = (c_vals - c_min) / (c_max - c_min)
                                    # 수학적 텐서 변환: 어떤 길이의 캔들도 완벽한 20차원 텐서로 압축 보간(Interpolation)
                                    idx_old = np.linspace(0, 1, len(c_norm))
                                    idx_new = np.linspace(0, 1, 20)
                                    interpolated = np.interp(idx_new, idx_old, c_norm)
                                    real_shape = [round(float(x), 4) for x in interpolated]
                        except Exception as e:
                            pass # 종목 테이블이 없거나 에러 시 기본 중립 궤적 유지
                        
                        dna_pack = {
                            "cpv": round(cpv, 4),
                            "tb": round(tb, 4),
                            "bbe": round(bbe, 4),
                            "rs": round(rs, 4),
                            "shape": real_shape,  # 👑 진짜 승자의 궤적(Real Shape) 각인
                            "cos_cutoff": 0.85, 
                            "mfe_atr_mult": round(mfe_atr_mult, 2),     
                            "trail_atr_mult": round(trail_atr_mult, 2), 
                            "status": "INCUBATING",
                            "mutated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "mutation_kind": "elite_spinoff_elastic_shape",
                            "parent_lineage": sig_type
                        }
                        elite_candidates.append((spinoff_name, dna_pack))
        finally:
            conn.close()
    except Exception as e:
        print(f"⚠️ [엘리트 스핀오프 발굴 실패] {e}")
        
    return elite_candidates


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
    """패자 DNA × 챔피언 DNA 선형 교배 및 논리적 유전자 우성 교환."""
    lw = max(0.35, min(0.70, float(loser_weight)))
    out = copy.deepcopy(loser)
    
    # 1. 연속형 변수(숫자) 선형 교배
    for k in _DNA_KEYS:
        if k not in loser or k not in champion:
            continue
        try:
            lv = float(loser[k])
            cv = float(champion[k])
        except (TypeError, ValueError):
            continue
        out[k] = _clip_key(k, lw * lv + (1.0 - lw) * cv)
        
    # 2. 👑 [초월적 진화] 논리적 구조 유전자(스위치) 우성 교배 (Dominant Crossover)
    # 패자의 낡은 조건을 버리고, 챔피언이 가진 승리 조건(우성 유전자)을 70% 확률로 강제 이식합니다.
    for k in _STRUCTURAL_GENES:
        c_state = bool(champion.get(k, False))
        l_state = bool(loser.get(k, False))
        if random.random() < 0.70:
            out[k] = c_state
        else:
            out[k] = l_state

    for key, (lo, hi) in MUTATION_HARD_BOUNDARIES.items():
        if key not in loser or key not in champion:
            continue
        try:
            lv = float(loser[key])
            cv = float(champion[key])
        except (TypeError, ValueError):
            continue
        out[key] = round(max(lo, min(hi, lw * lv + (1.0 - lw) * cv)), 6)
        
    out["mutation_kind"] = "structural_crossover"
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


# ===========================================================================
# 👑 [초월적 진화] 시장 선행 예견 및 국면 맞춤형 생존 유전자 강제 발현 엔진
# ===========================================================================
def _detect_leading_regime(sys_config: Dict[str, Any], market: str) -> str:
    """
    뉴스 포털의 후행 지표를 버리고, 팩토리 내부의 선행 지표(시장 폭 Breadth, 스마트머니 수급)를
    종합하여 한 발 앞선 '진짜 다가올 국면'을 예견합니다.
    """
    try:
        import os
        import sqlite3
        import pandas as pd
        from market_db_paths import MARKET_DATA_DB_PATH
        
        try:
            from meta_governor_consumer import load_meta_state_resolved
            base_regime = load_meta_state_resolved().get("META_REGIME_KEY", "UNKNOWN")
        except Exception:
            base_regime = sys_config.get("CURRENT_REGIME_KEY", "UNKNOWN")
            
        if not os.path.isfile(MARKET_DATA_DB_PATH):
            return "BULL" if "BULL" in base_regime else ("BEAR" if "BEAR" in base_regime else "CHOP")
            
        conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=20)
        
        # 시장 폭(Breadth) 및 스마트머니 엑소더스 분석 (최근 50건)
        df = pd.read_sql(
            f"SELECT market_breadth, flow_bonus FROM forward_trades WHERE market='{market}' ORDER BY id DESC LIMIT 50", conn
        )
        conn.close()
        
        avg_breadth = df['market_breadth'].astype(float).mean() if not df.empty else 1.0
        avg_flow = df['flow_bonus'].astype(float).mean() if not df.empty and 'flow_bonus' in df.columns else 0.0
        
        # 👑 [한발 앞선 선행 예견 로직]
        # 지수(뉴스)는 상승장이더라도, 시장폭이 무너지고 스마트머니가 이탈하면 기계는 선제적으로 BEAR 돌연변이를 준비합니다.
        if avg_breadth < 0.95 and avg_flow < -0.5:
            return "BEAR"
        # 지수는 하락/횡보라도, 시장폭이 살아나고 딥 다이버전스가 발생하면 상승장 예견
        elif avg_breadth > 1.02 and avg_flow > 1.0:
            return "BULL"
            
        if "BEAR" in base_regime or "HIGH_VOL" in base_regime: return "BEAR"
        if "BULL" in base_regime: return "BULL"
        return "CHOP"
    except Exception:
        return "CHOP"

def mutate_dna_for_regime(child: dict, regime: str) -> dict:
    """예견된 국면에 맞춰 기계가 스스로 생존 논리 유전자를 강제 조율합니다."""
    out = dict(child)
    if regime == "BEAR":
        # 하락장: 캔들은 바닥권 강제, 숏스퀴즈 필수, 방어적 짧은 익절 (2 ATR)
        out["cpv"] = min(float(out.get("cpv", 0.5)), 0.4)
        out["bbe"] = max(float(out.get("bbe", 10.0)), 15.0)
        out["mfe_atr_mult"] = min(float(out.get("mfe_atr_mult", 3.0)), 2.0) 
        out["req_short_squeeze"] = True
        out["req_flow_div"] = True
        out["mutation_kind"] = out.get("mutation_kind", "") + "_BEAR_ADAPTED"
    elif regime == "CHOP":
        # 횡보장: 박스권 회귀, 일치율 엄격하게, 타이트한 트레일링 래칫 (0.8 ATR)
        out["cos_cutoff"] = max(float(out.get("cos_cutoff", 0.85)), 0.90)
        out["trail_atr_mult"] = min(float(out.get("trail_atr_mult", 1.5)), 0.8) 
        out["req_dart_event"] = False 
        out["mutation_kind"] = out.get("mutation_kind", "") + "_CHOP_ADAPTED"
    elif regime == "BULL":
        # ===========================================================================
        # 👑 [상승장 극대화 (The Infinite Ride)] 무한 추세 추종 및 불타기 DNA 발현
        # 대세 상승장에서는 짧게 익절하는 것이 죄악입니다. 원금을 회수하고 남은 물량은 끝까지 가져갑니다.
        # ===========================================================================
        out["rs"] = max(float(out.get("rs", 100.0)), 200.0) # 대장주(주도력 상위 1%) 압도적 강제
        out["mfe_atr_mult"] = max(float(out.get("mfe_atr_mult", 3.0)), 15.0) # 목표 익절가를 아득히 높여 천장을 파괴함
        
        # 🚀 [추세 극대화 스위치 ON]
        out["use_free_runner"] = True     # 절반 익절로 원금 확보 후, 잔여 물량은 추세가 꺾일 때까지 무한 홀딩
        out["allow_pyramiding"] = True    # 수익이 크게 나고 있다면 그 수익금을 담보로 피라미딩(불타기) 허용
        out["req_dart_event"] = True      # 폭등의 재료(DART 호재) 필수 탑승
        
        # 폭발적인 상승 초입은 차트가 지저분(변동성)할 수 있으므로, 형태 일치율(cos_cutoff)을 살짝 풀어주어 폭등주를 놓치지 않음
        if "cos_cutoff" in out:
            out["cos_cutoff"] = min(float(out.get("cos_cutoff", 0.85)), 0.75)
            
        out["mutation_kind"] = out.get("mutation_kind", "") + "_BULL_ADAPTED"
    return out

def run_weekend_dna_mutation_cycle(
    sys_config: Dict[str, Any],
    *,
    markets: Tuple[str, ...] = ("KR", "US"),
    mutation_rate: Optional[float] = None,
    max_mutants_per_market: int = 4,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    주말 자율조율 훅 — 선행 국면 예견 기반 환경 맞춤형 진화 엔진.
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
        # 👑 1. 한 발 앞선 국면(Regime) 예견 (뉴스 배제, 순수 퀀트 내부 데이터 기반)
        leading_regime = _detect_leading_regime(cfg, mk)
        logs.append(f"🔮 {mk} 선행 국면 예견: [{leading_regime}] (내부 시장폭/스마트머니 기반)")
        
        # 👑 2. 예견된 국면에 최적화된 챔피언 부모 선발
        parents = select_parents_for_mutation(cfg, mk, target_regime=leading_regime)
        
        created = 0
        for parent_name, parent_tpl in parents:
            if created >= max_mutants_per_market:
                break
            # 기본 교배 및 돌연변이 (오답노트 반발력 AI 적용)
            child = mutate_dna_template(parent_tpl, rate=rate, name_suffix=parent_name, sys_config=cfg)
            # 👑 3. 예견된 국면 맞춤형 생존 DNA(논리 스위치 및 타겟 래칫) 강제 발현
            child = mutate_dna_for_regime(child, leading_regime)
            
            h = hashlib.sha256(f"{mk}|{parent_name}|{stamp}|{created}".encode()).hexdigest()[:8]
            child_name = f"MUTANT_{mk}_{leading_regime}_{parent_name[:15]}_{h}"
            
            inc[child_name] = child
            gene_pool[child_name] = {
                "market": mk,
                "parent": parent_name,
                "target_regime": leading_regime,
                "rate": rate,
                "created_at": datetime.now().isoformat(),
            }
            created += 1
            logs.append(f"▪️ {mk} 돌연변이 {child_name} ← {parent_name} ({leading_regime} 맞춤 진화)")

        if created == 0:
            logs.append(f"▪️ {mk} 돌연변이 스킵 — 해당 국면({leading_regime}) 표본 부족")

        # =================================================================
        # 👑 4. 마이크로 태그 기반 엘리트 스핀오프(Spin-off) 엔진 가동
        # =================================================================
        elite_spinoffs = mine_elite_spinoff_dna(mk)
        spinoff_count = 0
        for spinoff_name, spinoff_dna in elite_spinoffs:
            if spinoff_count >= 3: 
                break
            
            h = hashlib.sha256(f"{mk}|{spinoff_name}|{stamp}".encode()).hexdigest()[:6]
            final_spinoff_name = f"INCUBATOR_{spinoff_name}_{h}"
            
            inc[final_spinoff_name] = spinoff_dna
            gene_pool[final_spinoff_name] = {
                "market": mk,
                "parent": spinoff_dna["parent_lineage"],
                "rate": 0.0, 
                "created_at": datetime.now().isoformat(),
                "type": "ELITE_SPINOFF"
            }
            spinoff_count += 1
            logs.append(f"🌟 {mk} 엘리트 스핀오프 독립 승격! [{final_spinoff_name}] ← 늪에서 구출됨")

    cfg["INCUBATOR_TEMPLATES"] = inc
    cfg["MUTANT_GENE_POOL"] = gene_pool
    cfg["DNA_MUTATION_LAST_RUN"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return cfg, logs
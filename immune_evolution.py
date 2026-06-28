"""
Mission 8 — 실패 기반 자가 교정 및 면역 학습 (Fail-Fast & Immune Evolution).

자동 투입(Mission 7)된 진화 템플릿이 실제 시장에서 통하지 않으면, 기계 스스로 다음을 집행한다.
  1) [즉각 처벌 — Capital Strangle] 표본 ≥5 인데 WR<40% 또는 PF<1.0 → 켈리 배수를 0.1배로 잠금.
  2) [면역화 — RL Immune System] 실패 템플릿 벡터를 ANTI_PATTERNS 에 등록하고, 해당 국면의
     매칭 가중치를 강화학습 페널티 수식으로 영구 삭감.
  3) [재진화 트리거] 평일이라도 즉시 task_orchestrator 에 Priority 3 '긴급 재탐색'을 밀어,
     실패한 국면을 제외한 두 번째로 닮은 과거 정답(Plan B)으로 템플릿을 자가 교정.

template_bandit.py · regime_analog_engine(면역) · regime_memory(긴급 큐) 를 한데 묶는다.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

DEFAULT_MIN_SAMPLES = 5
DEFAULT_WR_FLOOR = 0.40
DEFAULT_PF_FLOOR = 1.0


def _append_anti_pattern(cfg: Dict[str, Any], key: str, bbox: Dict[str, Any]) -> None:
    """ANTI_PATTERNS 가 dict 든 list 든 안전하게 면역 규칙을 적재."""
    ap = cfg.get("ANTI_PATTERNS")
    if isinstance(ap, dict):
        ap[key] = bbox
    elif isinstance(ap, list):
        ap.append(bbox)
    else:
        ap = {key: bbox}
    cfg["ANTI_PATTERNS"] = ap


def run_immune_self_correction(
    cfg: Dict[str, Any],
    *,
    db_path: Optional[str] = None,
    persist: bool = True,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    wr_floor: float = DEFAULT_WR_FLOOR,
    pf_floor: float = DEFAULT_PF_FLOOR,
) -> Dict[str, Any]:
    """
    배포된 deep-evolved 템플릿들의 실전 성과를 계측하고, 실패분에 즉각 처벌+면역+재진화를 집행.
    반환: {report_lines, actions, impact}.
    """
    from datetime import datetime

    import deep_evolution_deploy as dep
    import regime_analog_engine as rae
    import regime_memory as rm
    import template_bandit as tb

    report_lines: List[str] = []
    actions: List[Dict[str, Any]] = []

    registry = cfg.get(dep.DEPLOY_REGISTRY_KEY)
    if not isinstance(registry, dict) or not registry:
        return {"report_lines": [], "actions": [], "impact": None}

    impact = dep.track_deep_evolution_impact(db_path=db_path, cfg=cfg, persist=persist)
    by_template = impact.get("by_template", {}) if isinstance(impact, dict) else {}
    by_market = impact.get("by_market", {}) if isinstance(impact, dict) else {}

    report_lines.append("\n🧬 <b>[DEEP-EVOLVED 영향도 추적 & 면역 자가교정]</b>")
    total = impact.get("total", {}) if isinstance(impact, dict) else {}
    report_lines.append(
        f"▪️ 전체 진화템플릿 실전: 표본 {total.get('n', 0)}개 · 승률 "
        f"{float(total.get('wr', 0.0)) * 100:.1f}% · PF {total.get('profit_factor', 0.0)} · "
        f"누적 {total.get('cum_pnl', 0.0)}% · MFE(avg/max) "
        f"{total.get('avg_mfe', 0.0)}/{total.get('max_mfe', 0.0)}"
    )

    for name, meta in list(registry.items()):
        if not isinstance(meta, dict):
            continue
        mk = str(meta.get("market", "GLOBAL")).upper()
        episode = meta.get("episode")
        dna = meta.get("dna")
        # 템플릿(버전)별 독립 계측 우선, 없으면 시장별로 폴백.
        stats = by_template.get(name) if isinstance(by_template, dict) else None
        if not isinstance(stats, dict):
            stats = by_market.get(mk, {}) if isinstance(by_market, dict) else {}
        n = int(stats.get("n", 0))
        wins = int(stats.get("wins", 0))
        pf = float(stats.get("profit_factor", 0.0))
        wr = float(stats.get("wr", 0.0))

        if n < int(min_samples):
            report_lines.append(f"  ⏳ {name}: 표본 {n}개(<{min_samples}) — 관망")
            continue

        punished = tb.enforce_capital_strangle(
            cfg, name, n=n, wins=wins, profit_factor=pf,
            min_samples=min_samples, wr_floor=wr_floor, pf_floor=pf_floor,
        )
        if punished is None:
            report_lines.append(
                f"  ✅ {name}: 승률 {wr * 100:.1f}% · PF {pf:.2f} (표본 {n}) — 생존"
            )
            continue

        # 1) 즉각 처벌
        action: Dict[str, Any] = {
            "template": name, "market": mk, "episode": episode,
            "wr": round(wr, 4), "pf": round(pf, 4), "n": n,
            "strangled_mult": punished.get("mult"),
        }
        report_lines.append(
            f"  🚨 <b>{name}</b>: 승률 {wr * 100:.1f}% · PF {pf:.2f} (표본 {n}) "
            f"→ 켈리 배수 {punished.get('mult')}배로 즉각 교살(Strangle)"
        )

        # 2) 면역화: 실패 벡터 ANTI_PATTERNS 등록 + RL 국면 페널티
        if isinstance(dna, (list, tuple)) and len(dna) >= 3:
            bbox = rae.build_anti_pattern_bbox(
                dna, label=name, market=mk, source="DEEP_EVOLVED_FAIL"
            )
            if bbox:
                ap_key = f"IMMUNE_{name}_{datetime.now().strftime('%y%m%d%H%M')}"
                _append_anti_pattern(cfg, ap_key, bbox)
                action["anti_pattern_key"] = ap_key
        if episode:
            pen = rae.penalize_episode(cfg, episode, persist=persist)
            action["rl_penalty"] = pen
            report_lines.append(
                f"     🛡️ 면역: 국면 {episode} 매칭가중치 {pen.get('weight')} 로 영구 삭감 "
                f"(Q {pen.get('q_before')}→{pen.get('q_after')}) · 실패벡터 오답노트 등록"
            )

        # 3) 재진화 트리거: 평일이라도 Plan B 긴급 재탐색 예약
        tid = rm.enqueue_emergency_remorph(
            failed_episode=episode, failed_template=name, market=mk
        )
        action["emergency_task_id"] = tid
        report_lines.append(
            f"     🔁 재진화: 국면 {episode} 제외 Plan B 긴급 재탐색 예약"
            + (f" #{tid}" if tid is not None else " (폴백 큐)")
        )
        actions.append(action)

    if persist and actions:
        try:
            from config_manager import set_config_value

            set_config_value("ANTI_PATTERNS", cfg.get("ANTI_PATTERNS"))
            set_config_value(tb.BANDIT_KEY, cfg.get(tb.BANDIT_KEY, {}))
        except Exception:
            pass

    return {"report_lines": report_lines, "actions": actions, "impact": impact}

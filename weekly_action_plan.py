"""
주간 Flow Action Plan — Rule-first 조립 + (선택) Gemini 2문장 tail.
"""
from __future__ import annotations

import html
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from weekly_flow_rollup import WeeklyDnaRollup, WeeklyFlowTagRollup
from reports.report_state_binder import LifecycleReportBlock, MacroTreasuryReportBlock

BASELINE_CONFIG_KEY = "WEEKLY_REPORT_BASELINE"

_WEEKLY_LLM_SYSTEM = (
    "You are a ruthless quant ops auditor. Given ONLY the fact bullets below, "
    "write exactly 2 short sentences in Korean. No praise, no hype, no emojis. "
    "State risk and tuning implication only. Do not invent numbers not in the facts."
)


@dataclass(frozen=True)
class WeeklyActionPlan:
    rule_html: str
    llm_tail_html: str
    facts_plain: str
    is_first_baseline_week: bool = False


def _pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v) * 100:.0f}%"
    except (TypeError, ValueError):
        return "—"


def load_weekly_baseline(sys_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """첫 주차·손상된 baseline 모두 빈 dict 로 안전 처리."""
    if not isinstance(sys_config, dict):
        return {}
    raw = sys_config.get(BASELINE_CONFIG_KEY)
    return dict(raw) if isinstance(raw, dict) else {}


def is_first_week_baseline(prev: Dict[str, Any]) -> bool:
    if not prev:
        return True
    return prev.get("saved_at") in (None, "")


def _delta_pct_line(
    label: str,
    key: str,
    current: Dict[str, Any],
    prev: Dict[str, Any],
) -> Optional[str]:
    if not prev:
        return None
    try:
        now_v = float(current.get(key))
        prev_v = float(prev.get(key))
    except (TypeError, ValueError, KeyError):
        return None
    if abs(now_v - prev_v) < 1e-9:
        return None
    direction = "상향" if now_v > prev_v else "하향"
    return f"{label} MetaGovernor·설정 {direction} <b>{_pct(prev_v)}</b>➔<b>{_pct(now_v)}</b>"


def _delta_kelly_line(
    current_eff: float,
    prev: Dict[str, Any],
) -> Optional[str]:
    if not prev:
        return None
    try:
        prev_eff = float(prev.get("effective_kelly_risk"))
    except (TypeError, ValueError, KeyError):
        return None
    if abs(current_eff - prev_eff) < 1e-9:
        return None
    direction = "상향" if current_eff > prev_eff else "하향"
    return (
        f"유효 켈리 {direction} "
        f"<b>{prev_eff * 100:.2f}%</b>➔<b>{current_eff * 100:.2f}%</b>"
    )


def _pick_toxic(
    tags_kr: Optional[WeeklyFlowTagRollup],
    tags_us: Optional[WeeklyFlowTagRollup],
) -> Optional[WeeklyFlowTagRollup]:
    cands = [t for t in (tags_kr, tags_us) if t and t.toxic_tag]
    if not cands:
        return None
    return min(cands, key=lambda t: float(t.toxic_cum_ret or 0.0))


def _sector_phrase(top_sector_names: Tuple[str, ...]) -> str:
    if not top_sector_names:
        return ""
    return "·".join(top_sector_names[:2])


def build_weekly_action_plan(
    *,
    sys_config: Dict[str, Any],
    macro_kr: MacroTreasuryReportBlock,
    lifecycle: LifecycleReportBlock,
    top_sectors_kr: Tuple[str, ...] = (),
    top_sectors_us: Tuple[str, ...] = (),
    dna_kr: Optional[WeeklyDnaRollup] = None,
    dna_us: Optional[WeeklyDnaRollup] = None,
    tags_kr: Optional[WeeklyFlowTagRollup] = None,
    tags_us: Optional[WeeklyFlowTagRollup] = None,
) -> WeeklyActionPlan:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    prev = load_weekly_baseline(cfg)
    first_week = is_first_week_baseline(prev)

    macro = macro_kr
    eff_k = macro.effective_kelly_risk
    regime = macro.regime_key

    facts: List[str] = []
    clauses: List[str] = []

    sec_kr = _sector_phrase(top_sectors_kr)
    sec_us = _sector_phrase(top_sectors_us)
    if sec_kr and sec_us:
        clauses.append(
            f"다음 주 팩토리는 <b>[{html.escape(sec_kr, quote=False)}]</b>(KR) · "
            f"<b>[{html.escape(sec_us, quote=False)}]</b>(US) 주도 섹터 가중을 유지·감시합니다"
        )
        facts.append(f"주도섹터 KR={sec_kr} US={sec_us}")
    elif sec_kr:
        clauses.append(
            f"다음 주 팩토리는 <b>[{html.escape(sec_kr, quote=False)}]</b> 섹터 DNA 가중을 유지합니다"
        )
        facts.append(f"주도섹터 KR={sec_kr}")
    elif sec_us:
        clauses.append(
            f"다음 주 팩토리는 <b>[{html.escape(sec_us, quote=False)}]</b>(US) 섹터 라인을 우선합니다"
        )
        facts.append(f"주도섹터 US={sec_us}")

    toxic = _pick_toxic(tags_kr, tags_us)
    if toxic and toxic.toxic_tag:
        mult = toxic.penalty_mult if toxic.penalty_mult is not None else 0.85
        clauses.append(
            f"<code>{html.escape(toxic.toxic_tag, quote=False)}</code> 독성 태그에 "
            f"페널티(×{mult:.2f})·회피 가드를 적용했습니다"
        )
        facts.append(f"독성태그={toxic.toxic_tag} cum={toxic.toxic_cum_ret} mult={mult}")

    dna_feats: List[str] = []
    for d in (dna_kr, dna_us):
        if d and d.dominant_features:
            dna_feats.extend(d.dominant_features[:2])
    if dna_feats:
        uniq = []
        for f in dna_feats:
            if f not in uniq:
                uniq.append(f)
        feat_s = " · ".join(html.escape(u, quote=False) for u in uniq[:3])
        clauses.append(f"Universal DNA 지배 축 <b>{feat_s}</b>를 캐리 필터에 반영합니다")
        facts.append(f"DNA축={','.join(uniq[:3])}")

    clauses.append(
        f"국면 <b>{html.escape(regime, quote=False)}</b> · 유효 켈리 "
        f"<b>{eff_k * 100:.2f}%</b> (Meta×{macro.meta_global_kelly_mult:.3f})"
    )
    facts.append(f"국면={regime} kelly={eff_k:.4f}")

    cos_line = _delta_pct_line("초신성 코사인 허들", "DYNAMIC_SUPERNOVA_CUTOFF", cfg, prev)
    ml_line = _delta_pct_line("ML박스 허들", "DYNAMIC_ML_BOX_CUTOFF", cfg, prev)
    kelly_line = _delta_kelly_line(eff_k, prev)
    for line in (cos_line, ml_line, kelly_line):
        if line:
            clauses.append(line)

    if not cos_line and cfg.get("DYNAMIC_SUPERNOVA_CUTOFF") is not None:
        facts.append(f"cos_cut={_pct(cfg.get('DYNAMIC_SUPERNOVA_CUTOFF'))}")
    if not ml_line and cfg.get("DYNAMIC_ML_BOX_CUTOFF") is not None:
        facts.append(f"ml_cut={_pct(cfg.get('DYNAMIC_ML_BOX_CUTOFF'))}")

    if lifecycle.n_cooled > 0:
        clauses.append(
            f"COOLED <b>{lifecycle.n_cooled}</b>전략은 벤치 대기·재기동 관측을 유지합니다"
        )

    if not clauses:
        body = (
            "이번 주 청산·태그 표본이 부족해 공격적 튜닝을 보류하고, "
            "MetaGovernor SSOT만 동기화했습니다."
        )
    else:
        body = ". ".join(clauses) + "."

    rule_html = (
        "\n🎯 <b>[다음 주 Action Plan · Rule]</b>\n"
        f" {body}\n"
    )
    facts_plain = " | ".join(facts)

    llm_tail = _optional_llm_tail(facts_plain, rule_plain=_strip_tags(body))
    return WeeklyActionPlan(
        rule_html=rule_html,
        llm_tail_html=llm_tail,
        facts_plain=facts_plain,
    )


def _strip_tags(s: str) -> str:
    import re

    return re.sub(r"<[^>]+>", "", s)


def _optional_llm_tail(facts_plain: str, *, rule_plain: str) -> str:
    if os.environ.get("WEEKLY_ACTION_PLAN_USE_LLM", "").strip() not in (
        "1",
        "true",
        "yes",
    ):
        return ""
    try:
        from llm_gemini_core import LlmCallSpec, generate_text_sync, load_gemini_api_keys

        if not load_gemini_api_keys():
            return ""
        spec = LlmCallSpec(
            task_id="weekly_action_plan",
            system_prompt=_WEEKLY_LLM_SYSTEM,
            user_payload=(
                f"[FACTS]\n{facts_plain}\n\n[RULE PLAN]\n{rule_plain[:600]}\n"
            ),
            model="gemini-2.0-flash",
            max_attempts=6,
        )
        res = generate_text_sync(spec, max_wait_sec=3.0)
        text = (res.text or "").strip()
        if not res.ok:
            return ""
        if not text:
            return ""
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()][:2]
        if not lines:
            return ""
        joined = " ".join(lines)
        return (
            "\n🤖 <b>[Auditor tail]</b> "
            f"<i>{html.escape(joined[:400], quote=False)}</i>\n"
        )
    except Exception:
        return ""


def snapshot_baseline_for_next_week(
    sys_config: Dict[str, Any],
    *,
    macro_effective_kelly: float,
) -> Dict[str, Any]:
    """다음 주 Δ 비교용 — 호출부에서 update_system_config 로 저장."""
    from datetime import datetime

    import pytz

    tz = pytz.timezone("Asia/Seoul")
    return {
        "saved_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M"),
        "DYNAMIC_SUPERNOVA_CUTOFF": sys_config.get("DYNAMIC_SUPERNOVA_CUTOFF"),
        "DYNAMIC_ML_BOX_CUTOFF": sys_config.get("DYNAMIC_ML_BOX_CUTOFF"),
        "DYNAMIC_KELLY_RISK": sys_config.get("DYNAMIC_KELLY_RISK"),
        "META_GLOBAL_KELLY_MULT": sys_config.get("META_GLOBAL_KELLY_MULT"),
        "effective_kelly_risk": macro_effective_kelly,
        "CURRENT_REGIME_KEY": sys_config.get("CURRENT_REGIME_KEY"),
    }


def persist_weekly_baseline(
    sys_config: Dict[str, Any],
    *,
    macro_effective_kelly: float,
) -> bool:
    """저장 성공 여부 반환 — 실패해도 리포트 발송은 계속."""
    try:
        from config_manager import update_system_config

        snap = snapshot_baseline_for_next_week(
            sys_config if isinstance(sys_config, dict) else {},
            macro_effective_kelly=float(macro_effective_kelly or 0.0),
        )
        return bool(update_system_config({BASELINE_CONFIG_KEY: snap}))
    except Exception:
        return False

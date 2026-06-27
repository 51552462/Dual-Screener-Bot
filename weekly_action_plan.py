"""
주간 Flow Action Plan — Rule-first 조립 + (선택) Gemini 2문장 tail.
"""
from __future__ import annotations

import html
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from weekly_flow_rollup import WeeklyDnaRollup, WeeklyFlowTagRollup
from reports.report_state_binder import LifecycleReportBlock, MacroTreasuryReportBlock

BASELINE_CONFIG_KEY = "WEEKLY_REPORT_BASELINE"

# 유동적 독성 페널티(Fluid Toxic Penalty) 경계 — 표본·승률 베이지안 하한 기반.
TOXIC_PENALTY_CEIL = 0.95   # 약한 페널티(표본 적은 우연한 손실): 거의 안 깎음
TOXIC_PENALTY_FLOOR = 0.50  # 강한 페널티(N≥10 & 승률 붕괴 구조적 독성)
_TOXIC_CONF_N_MIN = 3.0     # 이하 표본: 신뢰 0(페널티 거의 없음)
_TOXIC_CONF_N_FULL = 10.0   # 이상 표본: 신뢰 1(최대 감쇠 허용)
_WILSON_Z = 1.2816          # 90% 단측 하한


def _wilson_lower_bound(wins: int, n: int, z: float = _WILSON_Z) -> float:
    """승률의 Wilson score 하한(베이지안 하한 근사). 표본이 적으면 자동으로 0쪽으로 눌린다."""
    if n <= 0:
        return 0.0
    p = max(0.0, min(1.0, wins / n))
    denom = 1.0 + z * z / n
    centre = p + z * z / (2.0 * n)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def compute_bayesian_toxic_penalty(
    *, n: int, win_rate_pct: float, cum_ret_pct: Optional[float]
) -> float:
    """
    베이지안 하한 + 표본 신뢰도 기반 유동적 독성 페널티 배수.

    - confidence(표본 신뢰): N_MIN(3)→0 ~ N_FULL(10)→1 선형 램프. 표본 적으면 페널티 약화.
    - severity(심각도): 승률 Wilson 하한이 0.5 미만일수록↑ + 누적손실 깊이(완만 가중).
    - mult = CEIL − (CEIL−FLOOR)·confidence·severity, [FLOOR, CEIL] 클램프.
      → 우연한 소표본 손실: ~0.95(약). N≥10 & 승률 붕괴: 0.5까지(강).
    """
    n = max(0, int(n or 0))
    if n <= 0:
        return TOXIC_PENALTY_CEIL
    wins = int(round(max(0.0, float(win_rate_pct or 0.0)) / 100.0 * n))
    wlb = _wilson_lower_bound(wins, n)

    confidence = (n - _TOXIC_CONF_N_MIN) / (_TOXIC_CONF_N_FULL - _TOXIC_CONF_N_MIN)
    confidence = max(0.0, min(1.0, confidence))

    severity_wr = max(0.0, (0.5 - wlb) / 0.5)            # 승률 하한 0.5→0, 0.0→1
    cum = float(cum_ret_pct) if cum_ret_pct is not None else 0.0
    severity_cum = min(0.30, max(0.0, -cum) / 100.0)     # -100%p 누적손실 → +0.30
    severity = min(1.0, 0.70 * severity_wr + severity_cum)

    mult = TOXIC_PENALTY_CEIL - (TOXIC_PENALTY_CEIL - TOXIC_PENALTY_FLOOR) * confidence * severity
    return round(max(TOXIC_PENALTY_FLOOR, min(TOXIC_PENALTY_CEIL, mult)), 3)


def _rollup_bayesian_penalty(t: Optional[WeeklyFlowTagRollup]) -> Optional[float]:
    if t is None or not t.toxic_tag:
        return None
    return compute_bayesian_toxic_penalty(
        n=getattr(t, "toxic_n", 0),
        win_rate_pct=getattr(t, "toxic_wr_pct", 0.0),
        cum_ret_pct=t.toxic_cum_ret,
    )

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
    """가장 '구조적으로' 독성인 태그 선정 — 단순 cum_ret 최저가 아니라
    베이지안 페널티가 가장 강한(배수가 가장 낮은) 태그를 고른다(표본·승률 반영)."""
    cands = [t for t in (tags_kr, tags_us) if t and t.toxic_tag]
    if not cands:
        return None
    return min(
        cands,
        key=lambda t: (
            _rollup_bayesian_penalty(t) if _rollup_bayesian_penalty(t) is not None else 1.0,
            float(t.toxic_cum_ret or 0.0),
        ),
    )


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
        # [유동적 독성 페널티] 표본(N)·승률 베이지안 하한 기반 동적 감쇠 — 고정 ×0.85 폐기.
        mult = compute_bayesian_toxic_penalty(
            n=getattr(toxic, "toxic_n", 0),
            win_rate_pct=getattr(toxic, "toxic_wr_pct", 0.0),
            cum_ret_pct=toxic.toxic_cum_ret,
        )
        strength = "구조적" if mult <= 0.70 else ("중간" if mult <= 0.88 else "약(소표본)")
        clauses.append(
            f"<code>{html.escape(toxic.toxic_tag, quote=False)}</code> 독성 태그에 "
            f"베이지안 페널티(×{mult:.2f}·{strength}, N={getattr(toxic, 'toxic_n', 0)}·"
            f"WR{getattr(toxic, 'toxic_wr_pct', 0.0):.0f}%)·회피 가드를 적용했습니다"
        )
        facts.append(
            f"독성태그={toxic.toxic_tag} N={getattr(toxic, 'toxic_n', 0)} "
            f"WR={getattr(toxic, 'toxic_wr_pct', 0.0):.0f}% cum={toxic.toxic_cum_ret} mult={mult}"
        )

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


SECTOR_ALPHA_CONFIG_KEY = "WEEKLY_SECTOR_ALPHA"
SECTOR_ALPHA_BASE_BONUS = 0.05      # 적용 시작일(월) 프리미엄 +5%
SECTOR_ALPHA_DECAY_PER_DAY = 0.01   # 일일 선형 감쇠 −1%p (화 +4% … 금 +1%)
SECTOR_ALPHA_FLOOR = 0.0


def snapshot_sector_alpha_for_next_week(
    *,
    top_sectors_kr: Tuple[str, ...],
    top_sectors_us: Tuple[str, ...],
    base_bonus: float = SECTOR_ALPHA_BASE_BONUS,
    decay_per_day: float = SECTOR_ALPHA_DECAY_PER_DAY,
    floor: float = SECTOR_ALPHA_FLOOR,
) -> Dict[str, Any]:
    """다음 주 주도 섹터 캐리 프리미엄(알파 반감기용) 스냅샷."""
    from datetime import datetime

    import pytz

    tz = pytz.timezone("Asia/Seoul")
    return {
        "anchor_date": datetime.now(tz).strftime("%Y-%m-%d"),
        "base_bonus": float(base_bonus),
        "decay_per_day": float(decay_per_day),
        "floor": float(floor),
        "sectors_kr": [str(s) for s in (top_sectors_kr or ()) if str(s).strip()],
        "sectors_us": [str(s) for s in (top_sectors_us or ()) if str(s).strip()],
    }


def persist_weekly_sector_alpha(
    sys_config: Dict[str, Any],
    *,
    top_sectors_kr: Tuple[str, ...],
    top_sectors_us: Tuple[str, ...],
) -> bool:
    """다음 주 섹터 알파(반감기) 영속화 — 저장 실패해도 리포트 발송은 계속."""
    try:
        from config_manager import update_system_config

        snap = snapshot_sector_alpha_for_next_week(
            top_sectors_kr=top_sectors_kr,
            top_sectors_us=top_sectors_us,
        )
        return bool(update_system_config({SECTOR_ALPHA_CONFIG_KEY: snap}))
    except Exception:
        return False


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

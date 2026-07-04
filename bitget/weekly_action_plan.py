"""
주간 Flow Action Plan — Bitget 코인 전용.

주식 weekly_action_plan.py 구조를 코인에 이식:
- KR/US 섹터 로테이션 → SPOT/FUTURES 타임프레임 궤적
- 독성 태그 페널티 (베이지안 하한 기반 유동적 감쇠)
- DNA 지배 축 반영
- 커트라인 Δ 비교 (전주 → 이번 주)
- (선택) Gemini 2문장 tail
"""
from __future__ import annotations

import html
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

BASELINE_CONFIG_KEY = "WEEKLY_REPORT_BASELINE"

TOXIC_PENALTY_CEIL = 0.95
TOXIC_PENALTY_FLOOR = 0.50
_TOXIC_CONF_N_MIN = 3.0
_TOXIC_CONF_N_FULL = 10.0
_WILSON_Z = 1.2816


def _wilson_lower_bound(wins: int, n: int, z: float = _WILSON_Z) -> float:
    if n <= 0:
        return 0.0
    p = max(0.0, min(1.0, wins / n))
    denom = 1.0 + z * z / n
    centre = p + z * z / (2.0 * n)
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def compute_bayesian_toxic_penalty(
    *, n: int, win_rate_pct: float, cum_ret_pct: Optional[float],
) -> float:
    n = max(0, int(n or 0))
    if n <= 0:
        return TOXIC_PENALTY_CEIL
    wins = int(round(max(0.0, float(win_rate_pct or 0.0)) / 100.0 * n))
    wlb = _wilson_lower_bound(wins, n)
    confidence = (n - _TOXIC_CONF_N_MIN) / (_TOXIC_CONF_N_FULL - _TOXIC_CONF_N_MIN)
    confidence = max(0.0, min(1.0, confidence))
    severity_wr = max(0.0, (0.5 - wlb) / 0.5)
    cum = float(cum_ret_pct) if cum_ret_pct is not None else 0.0
    severity_cum = min(0.30, max(0.0, -cum) / 100.0)
    severity = min(1.0, 0.70 * severity_wr + severity_cum)
    mult = TOXIC_PENALTY_CEIL - (TOXIC_PENALTY_CEIL - TOXIC_PENALTY_FLOOR) * confidence * severity
    return round(max(TOXIC_PENALTY_FLOOR, min(TOXIC_PENALTY_CEIL, mult)), 3)


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


def _pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v) * 100:.0f}%"
    except (TypeError, ValueError):
        return "—"


def load_weekly_baseline(sys_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(sys_config, dict):
        return {}
    raw = sys_config.get(BASELINE_CONFIG_KEY)
    return dict(raw) if isinstance(raw, dict) else {}


def _delta_pct_line(
    label: str, key: str, current: Dict[str, Any], prev: Dict[str, Any],
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


def _delta_kelly_line(current_eff: float, prev: Dict[str, Any]) -> Optional[str]:
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


@dataclass(frozen=True)
class ToxicTagInfo:
    tag: str
    n: int
    wr_pct: float
    cum_ret: Optional[float]


def build_weekly_action_plan(
    *,
    sys_config: Dict[str, Any],
    regime_key: str = "UNKNOWN",
    effective_kelly: float = 0.01,
    meta_global_kelly_mult: float = 1.0,
    n_cooled: int = 0,
    dominant_tf_spot: str = "",
    dominant_tf_futures: str = "",
    mvp_engine_name: str = "",
    toxic_tag: Optional[ToxicTagInfo] = None,
    dna_dominant_features: Optional[List[str]] = None,
) -> WeeklyActionPlan:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    prev = load_weekly_baseline(cfg)
    facts: List[str] = []
    clauses: List[str] = []

    tf_parts: List[str] = []
    if dominant_tf_spot:
        tf_parts.append(f"SPOT <b>{html.escape(dominant_tf_spot, quote=False)}</b>")
    if dominant_tf_futures:
        tf_parts.append(f"FUT <b>{html.escape(dominant_tf_futures, quote=False)}</b>")
    if tf_parts:
        clauses.append(
            f"다음 주 팩토리는 {' · '.join(tf_parts)} 주도 타임프레임 가중을 유지·감시합니다"
        )
        facts.append(f"주도TF SPOT={dominant_tf_spot} FUT={dominant_tf_futures}")

    if mvp_engine_name:
        clauses.append(
            f"MVP 엔진 <b>{html.escape(mvp_engine_name, quote=False)}</b>에 "
            f"스캔 가중치를 우선 배분합니다"
        )
        facts.append(f"MVP={mvp_engine_name}")

    if toxic_tag and toxic_tag.tag:
        mult = compute_bayesian_toxic_penalty(
            n=toxic_tag.n, win_rate_pct=toxic_tag.wr_pct, cum_ret_pct=toxic_tag.cum_ret,
        )
        strength = "구조적" if mult <= 0.70 else ("중간" if mult <= 0.88 else "약(소표본)")
        clauses.append(
            f"<code>{html.escape(toxic_tag.tag, quote=False)}</code> 독성 태그에 "
            f"베이지안 페널티(×{mult:.2f}·{strength}, N={toxic_tag.n}·"
            f"WR{toxic_tag.wr_pct:.0f}%)·회피 가드를 적용했습니다"
        )
        facts.append(
            f"독성태그={toxic_tag.tag} N={toxic_tag.n} "
            f"WR={toxic_tag.wr_pct:.0f}% cum={toxic_tag.cum_ret} mult={mult}"
        )

    if dna_dominant_features:
        uniq: List[str] = []
        for f in dna_dominant_features:
            if f not in uniq:
                uniq.append(f)
        feat_s = " · ".join(html.escape(u, quote=False) for u in uniq[:3])
        clauses.append(f"Universal DNA 지배 축 <b>{feat_s}</b>를 캐리 필터에 반영합니다")
        facts.append(f"DNA축={','.join(uniq[:3])}")

    clauses.append(
        f"국면 <b>{html.escape(regime_key, quote=False)}</b> · 유효 켈리 "
        f"<b>{effective_kelly * 100:.2f}%</b> (Meta×{meta_global_kelly_mult:.3f})"
    )
    facts.append(f"국면={regime_key} kelly={effective_kelly:.4f}")

    cos_line = _delta_pct_line("초신성 코사인 허들", "DYNAMIC_SUPERNOVA_CUTOFF", cfg, prev)
    ml_line = _delta_pct_line("ML박스 허들", "DYNAMIC_ML_BOX_CUTOFF", cfg, prev)
    kelly_line = _delta_kelly_line(effective_kelly, prev)
    for line in (cos_line, ml_line, kelly_line):
        if line:
            clauses.append(line)

    if n_cooled > 0:
        clauses.append(
            f"COOLED <b>{n_cooled}</b>전략은 벤치 대기·재기동 관측을 유지합니다"
        )

    if not clauses:
        body = (
            "이번 주 청산·태그 표본이 부족해 공격적 튜닝을 보류하고, "
            "MetaGovernor SSOT만 동기화했습니다."
        )
    else:
        body = ". ".join(clauses) + "."

    rule_html = "\n🎯 <b>[다음 주 Action Plan · Rule]</b>\n" f" {body}\n"
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
        "1", "true", "yes",
    ):
        return ""
    try:
        from bitget.llm_gemini_core import LlmCallSpec, generate_text_sync, load_gemini_api_keys
    except ImportError:
        try:
            from llm_gemini_core import LlmCallSpec, generate_text_sync, load_gemini_api_keys
        except ImportError:
            return ""
    try:
        if not load_gemini_api_keys():
            return ""
        spec = LlmCallSpec(
            task_id="bitget_weekly_action_plan",
            system_prompt=_WEEKLY_LLM_SYSTEM,
            user_payload=(
                f"[FACTS]\n{facts_plain}\n\n[RULE PLAN]\n{rule_plain[:600]}\n"
            ),
            model="gemini-2.0-flash",
            max_attempts=6,
        )
        res = generate_text_sync(spec, max_wait_sec=3.0)
        text = (res.text or "").strip()
        if not res.ok or not text:
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
    sys_config: Dict[str, Any], *, effective_kelly: float,
) -> Dict[str, Any]:
    from datetime import datetime, timezone
    return {
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "DYNAMIC_SUPERNOVA_CUTOFF": sys_config.get("DYNAMIC_SUPERNOVA_CUTOFF"),
        "DYNAMIC_ML_BOX_CUTOFF": sys_config.get("DYNAMIC_ML_BOX_CUTOFF"),
        "DYNAMIC_KELLY_RISK": sys_config.get("DYNAMIC_KELLY_RISK"),
        "META_GLOBAL_KELLY_MULT": sys_config.get("META_GLOBAL_KELLY_MULT"),
        "effective_kelly_risk": effective_kelly,
        "CURRENT_REGIME_KEY": sys_config.get("CURRENT_REGIME_KEY"),
    }


def persist_weekly_baseline(
    sys_config: Dict[str, Any], *, effective_kelly: float,
) -> bool:
    try:
        from bitget.config_hub import save_config_atomic, load_config
        snap = snapshot_baseline_for_next_week(
            sys_config if isinstance(sys_config, dict) else {},
            effective_kelly=float(effective_kelly or 0.0),
        )
        cfg = load_config() or {}
        cfg[BASELINE_CONFIG_KEY] = snap
        save_config_atomic(cfg)
        return True
    except Exception:
        return False

"""
Ch.6 — Overseer LLM 해석 문구 정합성 SSOT.

Rules-first 감사 본문(dossier + anomalies)과 LLM 내러티브가 어긋나면
(칭찬 과다, Anomaly 모순, Ch.1~5 필드 무시) 규칙 기반 요약으로 대체한다.

파이프라인:
  1. build_canonical_narrative_facts — LLM·검증 공통 팩트 시트
  2. build_llm_narrative_prompt — 팩트 + JSON + glossary
  3. validate_llm_narrative — 사후 정합성 검사
  4. build_deterministic_narrative — 검증 실패 시 폴백
  5. process_overseer_llm_narrative — ai_overseer 진입점
"""
from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# Anomaly 코드 → LLM 해석 가이드 (Ch.1~5)
# ---------------------------------------------------------------------------
ANOMALY_CODE_GUIDE: Dict[str, str] = {
    "TOXIC_TAG_LEAK": (
        "진입 sig_type 의 독성 태그 매칭 — 청산 exit flow_tags echo 와 혼동 금지."
    ),
    "CATASTROPHIC_LOSS_DAY": (
        "당일 청산 다수·승률 붕괴. 칭찬·방어 성공 서술 금지. 당일클러치·regime_tag 격리 점검."
    ),
    "REGIME_STRATEGY_MISMATCH": (
        "META 국면과 regime_tag 전략 불일치 거래. BEAR 에 BULL_ONLY 등."
    ),
    "OVERDRIVE_SILENT": (
        "청산 많은데 logged 오버드라이브 0 — eligible·hurdle·v_energy 확인 후 판단."
    ),
    "OVERDRIVE_EXPECTED_IDLE": (
        "전량손절·eligible=0 이면 오버드라이브 0건은 정상 — 문제 아님."
    ),
    "OVERDRIVE_TELEMETRY_GAP": "오버드라이브 eligible 대비 flow_tags 미기록.",
    "KELLY_INELASTIC": (
        "NAV dd·당일클러치 있는데 effective_kelly 가 사전값과 동일 — 비탄력."
    ),
    "KELLY_ELASTICITY_ACTIVE": (
        "Kelly 탄력성 오버레이 활성 — effective_kelly_pre_overlay → post 반드시 인용."
    ),
    "DEFENSE_LEAK": "META_TREASURY_MODE=DEFENSE 인데 당일 진입 발생 — 심각한 누출.",
    "KILL_SWITCH_LEAK": "KILL_SWITCH ON 인데 당일 진입 — 실행 경로 누출.",
    "TREASURY_GROUP_ZERO_LEAK": "Treasury mult=0 그룹인데 진입 발생.",
    "TREASURY_CATASTROPHIC_SPLIT": (
        "Treasury NORMAL 이지만 당일 승률 붕괴·클러치 ON — Governor 윈도우 지연 가능."
    ),
    "TREASURY_HEALTH_LAG": "당일 전패인데 Treasury DEFENSE 미전환 — 롤링 윈도우 지연.",
    "BLOCK_SOURCE_LEAK": "block_trade_sources 설정인데 진입 발생.",
    "GOVERNOR_STALE_WITH_ENTRIES": "Governor 구식 상태에서 진입 — SSOT 신뢰도 저하.",
    "REGIME_SSOT_SPLIT": "META_REGIME vs config_regime 불일치.",
    "SIGNAL_MISMATCH": "강세 국면인데 Kelly 극저·무거래 또는 과도 클램프.",
    "VIX_CLAMP": "VIX 고분위 Kelly 클램프 — 칭찬 금지, 인과만 기록.",
    "LIVE_COOLED_SPLIT": "COOLED 전략 존재 + 당일 진입.",
    "META_GOVERNOR_STALE": "Governor 미실행 — 무거래와 혼동 금지.",
    "DB_READ_FAIL": "장부 읽기 실패 — 팩트 신뢰 불가.",
}

_FORBIDDEN_PRAISE_RE = re.compile(
    r"(매우\s*훌륭|탁월한|완벽(?:히)?|잘\s*방어|훌륭한\s*방어|방어\s*상태|"
    r"완벽히\s*동기화|긍정적(?:인)?\s*신호|순조(?:롭|로운)|"
    r"탁월(?:한)?\s*방어|우수한\s*방어|방어\s*성공)",
    re.I,
)

_OVERDRIVE_FALSE_CLAIM_RE = re.compile(
    r"(오버드라이브.{0,6}(?:발동|작동|가동|적용|트리거)|"
    r"overdrive\s*(?:trigger|fired|active))",
    re.I,
)

_TREASURY_MODE_RE = re.compile(
    r"Treasury\s*(?:모드\s*)?(?:는\s*)?(NORMAL|DEFENSE)|"
    r"재무\s*(?:방어\s*)?모드\s*(?:는\s*)?(NORMAL|DEFENSE|정상|방어)",
    re.I,
)

_ALLOWED_HTML_RE = re.compile(r"</?(?:b|i)>|&[a-z#0-9]+;", re.I)


@dataclass(frozen=True)
class LlmNarrativeResult:
    text: str
    source: str  # llm | deterministic | api_fallback
    valid: bool
    violations: Tuple[str, ...]


def _strip_html_for_match(s: str) -> str:
    t = re.sub(r"<[^>]+>", "", str(s or ""))
    return html.unescape(t).strip()


def _severity_rank(sev: str) -> int:
    s = str(sev or "").upper()
    if s == "CRITICAL":
        return 0
    if s == "WARN":
        return 1
    return 2


def _top_anomaly(anomalies: Sequence[Any]) -> Optional[Any]:
    if not anomalies:
        return None
    return sorted(anomalies, key=lambda a: _severity_rank(getattr(a, "severity", "")))[0]


def build_canonical_narrative_facts(
    dossier: Any,
    anomalies: Sequence[Any],
) -> Dict[str, Any]:
    """
    LLM·검증·폴백이 공유하는 구조화 팩트 시트.
    dossier: OverseerAuditDossier (duck-typed).
    """
    wr = dossier.win_rate_today_pct
    wr_s = f"{wr:.1f}%" if wr is not None else "—"
    top = _top_anomaly(anomalies)
    crit_codes = [
        str(getattr(a, "code", ""))
        for a in anomalies
        if str(getattr(a, "severity", "")).upper() == "CRITICAL"
    ]
    warn_codes = [
        str(getattr(a, "code", ""))
        for a in anomalies
        if str(getattr(a, "severity", "")).upper() == "WARN"
    ]

    kelly_line = (
        f"effective_kelly_pre={dossier.effective_kelly_pre_overlay * 100:.2f}% "
        f"→ post={dossier.effective_kelly_risk * 100:.2f}% "
        f"(elasticity×{dossier.kelly_elasticity_mult:.3f}, "
        f"day×{dossier.kelly_day_clutch_mult:.3f}, "
        f"nav×{dossier.kelly_nav_dd_mult:.3f})"
    )
    if dossier.nav_drawdown_pct is not None:
        kelly_line += f" NAV_dd={dossier.nav_drawdown_pct:.2f}%"

    od_note = ""
    if dossier.overdrive_eligible_today == 0 and dossier.trades_closed_today >= 3:
        if dossier.overdrive_all_loss_sl_day:
            od_note = "overdrive_idle_expected: 전량손절·eligible=0 → 0건 정상"
        else:
            od_note = "overdrive_eligible=0 — hurdle/v_energy 확인"

    toxic_note = ""
    if dossier.toxic_tag_entry_hits_today > 0:
        toxic_note = (
            f"진입 독성태그 {dossier.toxic_tag_entry_hits_today}건"
            f"(exit_echo={dossier.toxic_tag_exit_echo_hits_today}, 별개)"
        )
    elif dossier.toxic_tag_exit_echo_hits_today > 0:
        toxic_note = (
            f"청산 exit_echo만 {dossier.toxic_tag_exit_echo_hits_today}건 — 진입 누출 아님"
        )

    return {
        "as_of_kst": dossier.as_of_kst,
        "meta_regime_key": dossier.meta_regime_key,
        "config_regime_key": dossier.config_regime_key,
        "meta_treasury_mode": dossier.meta_treasury_mode,
        "meta_global_kelly_mult": dossier.meta_global_kelly_mult,
        "kelly_summary": kelly_line,
        "vix_summary": dossier.vix_summary,
        "governor": {
            "last_run_at": dossier.meta_governor_last_run_at,
            "status": dossier.meta_governor_last_run_status,
            "is_stale": dossier.governor_is_stale,
            "hours_since_run": dossier.governor_stale_hours,
        },
        "treasury": {
            "mode": dossier.meta_treasury_mode,
            "zeroed_groups": dossier.treasury_zeroed_groups,
            "actionable_groups": dossier.treasury_actionable_groups,
            "kill_switch": dossier.kill_switch_active,
        },
        "trades_today": {
            "closed": dossier.trades_closed_today,
            "entry": dossier.trades_entry_today,
            "open": dossier.trades_open,
            "win_rate_closed_pct": wr_s,
        },
        "catastrophic_clutch": {
            "active": dossier.catastrophic_clutch_active,
            "mult": dossier.catastrophic_clutch_mult,
        },
        "regime_mismatch": {
            "entry_hits": dossier.regime_mismatch_entry_hits_today,
            "closed_hits": dossier.regime_mismatch_closed_hits_today,
        },
        "overdrive": {
            "hurdle": dossier.overdrive_hurdle,
            "eligible": dossier.overdrive_eligible_today,
            "logged": dossier.overdrive_logged_today,
            "loss_target": dossier.overdrive_loss_target_today,
            "v_energy_max": dossier.overdrive_v_energy_max_today,
            "all_loss_sl_day": dossier.overdrive_all_loss_sl_day,
            "note": od_note,
        },
        "toxic_tags": toxic_note,
        "anomalies": {
            "count": len(anomalies),
            "critical_codes": crit_codes,
            "warn_codes": warn_codes,
            "top_code": str(getattr(top, "code", "")) if top else "",
            "top_severity": str(getattr(top, "severity", "")) if top else "",
            "top_headline": _strip_html_for_match(
                str(getattr(top, "headline", "")) if top else ""
            ),
        },
        "mandatory_tone": (
            "critical_present_no_praise"
            if crit_codes
            else ("warn_present_cautious" if warn_codes else "neutral_monitor")
        ),
    }


def build_overseer_llm_system_prompt() -> str:
    """Ch.6 확장 system prompt — Ch.1~5 필드·Anomaly 가이드 포함."""
    guide_lines = "\n".join(
        f"  - {code}: {txt}" for code, txt in sorted(ANOMALY_CODE_GUIDE.items())
    )
    return f"""You are a Ruthless QA engineer for a quant trading factory. You are NOT a cheerleader.

STRICT RULES:
1. Use ONLY facts in CANONICAL_NARRATIVE_FACTS, AUDIT_DOSSIER_JSON, and ANOMALIES_JSON. Do not invent numbers, regimes, or trades.
2. If ANOMALIES_JSON is non-empty: your first sentence MUST acknowledge the highest-severity anomaly (code or headline). NEVER contradict an anomaly headline.
3. NEVER reframe anomalies as "excellent defense", "탁월한 방어", "훌륭한 방어", "완벽히 동기화", or praise Kelly clamping without citing the exact MetaGovernor field that caused it.
4. Forbidden when CRITICAL anomalies exist: "매우 훌륭한", "탁월한", "완벽", "잘 방어", "훌륭한 방어 상태", "순조", "방어 성공".
5. Every causal claim MUST cite a dossier field (e.g. META_GLOBAL_KELLY_MULT, META_REGIME_KEY, meta_treasury_mode, kelly_summary, vix_summary).
6. TOXIC_TAG_LEAK: distinguish entry hits vs exit_echo — exit echo alone is NOT a leak.
7. OVERDRIVE: if canonical overdrive.note says overdrive_idle_expected, do NOT call it a failure.
8. KELLY: if kelly_summary shows pre→post shrink, cite elasticity_mult — do NOT say Kelly is "stuck" without checking overlay fields.
9. TREASURY: if treasury.mode=NORMAL but TREASURY_CATASTROPHIC_SPLIT in anomalies, explain window lag — do NOT claim DEFENSE is active.
10. Output in Korean, max 10 short lines, Telegram-safe HTML (<b>, <i> only). No markdown headers (#, ##).
11. Do NOT repeat the full anomaly list; add 1–2 actionable checks for tomorrow only.
12. If zero anomalies: state neutrally what to monitor — still no praise fluff.

ANOMALY CODE GUIDE:
{guide_lines}"""


def build_llm_narrative_user_prompt(
    dossier: Any,
    anomalies: Sequence[Any],
    *,
    dossier_json: Optional[Dict[str, Any]] = None,
) -> str:
    """ai_overseer / overseer_audit_binder 공통 user prompt."""
    facts = build_canonical_narrative_facts(dossier, anomalies)
    anom_json = [
        {
            "code": getattr(a, "code", ""),
            "severity": getattr(a, "severity", ""),
            "headline": _strip_html_for_match(str(getattr(a, "headline", ""))),
            "evidence": _strip_html_for_match(str(getattr(a, "evidence", ""))),
            "guide": ANOMALY_CODE_GUIDE.get(str(getattr(a, "code", "")), ""),
        }
        for a in anomalies
    ]
    dossier_payload = dossier_json if dossier_json is not None else {}
    return (
        "[CANONICAL_NARRATIVE_FACTS]\n"
        f"{json.dumps(facts, ensure_ascii=False, indent=2)}\n\n"
        "[AUDIT_DOSSIER_JSON]\n"
        f"{json.dumps(dossier_payload, ensure_ascii=False, indent=2)}\n\n"
        "[ANOMALIES_JSON]\n"
        f"{json.dumps(anom_json, ensure_ascii=False, indent=2)}\n\n"
        "Write the LLM interpretation section only (Korean, max 10 lines). "
        "Start with the top anomaly if any. Do not repeat the report header."
    )


def sanitize_overseer_narrative_html(text: str) -> str:
    """Telegram HTML 허용 태그만 유지, 마크다운 헤더·코드펜스 제거."""
    if not text or not str(text).strip():
        return ""
    t = str(text).strip()
    t = re.sub(r"^#+\s*", "", t, flags=re.M)
    t = re.sub(r"```[a-z]*\n?", "", t)
    t = re.sub(r"`+", "", t)
    # markdown bold → html
    t = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", t)
    lines = []
    for line in t.splitlines():
        line = line.strip()
        if not line:
            continue
        if len(line) > 400:
            line = line[:397] + "…"
        lines.append(line)
    return "\n".join(lines[:12])


def validate_llm_narrative(
    text: str,
    dossier: Any,
    anomalies: Sequence[Any],
) -> List[str]:
    """사후 정합성 위반 목록 — 빈 리스트면 통과."""
    violations: List[str] = []
    plain = _strip_html_for_match(text)
    if not plain:
        return ["empty_narrative"]

    crit = [
        a for a in anomalies
        if str(getattr(a, "severity", "")).upper() == "CRITICAL"
    ]
    codes = {str(getattr(a, "code", "")) for a in anomalies}

    if crit and _FORBIDDEN_PRAISE_RE.search(plain):
        violations.append("forbidden_praise_with_critical")

    if crit:
        top = _top_anomaly(anomalies)
        if top:
            top_code = str(getattr(top, "code", ""))
            top_head = _strip_html_for_match(str(getattr(top, "headline", "")))
            head_key = top_head[:12] if len(top_head) >= 4 else ""
            if top_code and top_code not in plain and (
                not head_key or head_key not in plain
            ):
                # 완화: 심각 키워드라도 허용
                severity_words = (
                    "붕괴", "누출", "전패", "차단", "불일치", "실패", "위반", "누락"
                )
                if not any(w in plain for w in severity_words):
                    violations.append("missing_top_anomaly_ack")

    # 오버드라이브 오탐 서술
    if (
        dossier.overdrive_logged_today == 0
        and dossier.overdrive_eligible_today == 0
        and dossier.trades_closed_today >= 3
        and _OVERDRIVE_FALSE_CLAIM_RE.search(plain)
    ):
        violations.append("false_overdrive_trigger_claim")

    # Treasury 모드 모순
    tm = str(dossier.meta_treasury_mode or "NORMAL").upper()
    for m in _TREASURY_MODE_RE.finditer(plain):
        claimed = str(m.group(1) or "").upper()
        if claimed in ("정상",):
            claimed = "NORMAL"
        if claimed in ("방어",):
            claimed = "DEFENSE"
        if claimed in ("NORMAL", "DEFENSE") and claimed != tm:
            violations.append(f"treasury_mode_contradiction:{claimed}!={tm}")
            break

    if "DEFENSE_LEAK" in codes and re.search(
        r"(진입\s*(?:차단|통제)\s*(?:정상|양호)|방어\s*모드\s*정상)", plain, re.I
    ):
        violations.append("defense_leak_contradiction")

    if "TOXIC_TAG_LEAK" in codes and re.search(
        r"(독성\s*태그\s*(?:문제\s*)?없|누출\s*없|정상\s*태그)", plain, re.I
    ):
        violations.append("toxic_leak_denial")

    if "KELLY_INELASTIC" in codes and re.search(
        r"(Kelly\s*(?:정상|적정|문제\s*없)|켈리\s*(?:정상|적정))", plain, re.I
    ):
        violations.append("kelly_inelastic_denial")

    if (
        "OVERDRIVE_EXPECTED_IDLE" in codes
        or (
            dossier.overdrive_all_loss_sl_day
            and dossier.overdrive_eligible_today == 0
        )
    ) and "OVERDRIVE_SILENT" not in codes:
        if re.search(r"오버드라이브\s*(?:미작동|실패|문제|버그)", plain, re.I):
            violations.append("overdrive_idle_false_alarm")

    if "CATASTROPHIC_LOSS_DAY" in codes and _FORBIDDEN_PRAISE_RE.search(plain):
        violations.append("catastrophic_day_praise")

    return violations


def build_deterministic_narrative(
    dossier: Any,
    anomalies: Sequence[Any],
) -> str:
    """LLM 실패·검증 실패 시 규칙 기반 해석 (dossier 정합 100%)."""
    lines: List[str] = []
    facts = build_canonical_narrative_facts(dossier, anomalies)

    if anomalies:
        top = _top_anomaly(anomalies)
        if top:
            code = str(getattr(top, "code", ""))
            head = _strip_html_for_match(str(getattr(top, "headline", "")))
            sev = str(getattr(top, "severity", ""))
            lines.append(
                f"<b>[{html.escape(sev, quote=False)} · {html.escape(code, quote=False)}]</b> "
                f"{html.escape(head, quote=False)}"
            )
        for a in sorted(
            anomalies, key=lambda x: _severity_rank(getattr(x, "severity", ""))
        )[:3]:
            c = str(getattr(a, "code", ""))
            h = _strip_html_for_match(str(getattr(a, "headline", "")))
            if lines and c == str(getattr(top, "code", "")):
                continue
            lines.append(f"· {html.escape(c, quote=False)}: {html.escape(h, quote=False)}")
    else:
        lines.append(
            f"<i>규칙 CRITICAL/WARN 없음.</i> META_REGIME="
            f"<b>{html.escape(str(dossier.meta_regime_key), quote=False)}</b> · "
            f"Treasury=<b>{html.escape(str(dossier.meta_treasury_mode), quote=False)}</b>"
        )

    wr = dossier.win_rate_today_pct
    wr_s = f"{wr:.1f}%" if wr is not None else "—"
    lines.append(
        f"당일 청산 <b>{dossier.trades_closed_today}</b> · 진입 "
        f"<b>{dossier.trades_entry_today}</b> · 승률(청산) <b>{wr_s}</b>"
    )
    lines.append(
        f"Kelly: {html.escape(facts['kelly_summary'], quote=False)}"
    )

    if facts.get("toxic_tags"):
        lines.append(html.escape(str(facts["toxic_tags"]), quote=False))

    od = facts.get("overdrive") or {}
    if od.get("note"):
        lines.append(html.escape(str(od["note"]), quote=False))

    if dossier.catastrophic_clutch_active:
        lines.append(
            f"당일클러치 <b>ON×{dossier.catastrophic_clutch_mult:.2f}</b>"
        )

    if dossier.governor_is_stale:
        hrs = dossier.governor_stale_hours
        hs = f"{hrs:.1f}h" if hrs is not None else "—"
        lines.append(f"Governor stale ({html.escape(hs, quote=False)}) — meta_governor 재실행 권고")

    # 내일 점검 1~2개
    tomorrow: List[str] = []
    if dossier.treasury_zeroed_groups > 0 or str(dossier.meta_treasury_mode).upper() == "DEFENSE":
        tomorrow.append("Treasury DEFENSE·zeroed 그룹 진입 차단 경로 재확인")
    if dossier.regime_mismatch_entry_hits_today > 0:
        tomorrow.append("regime_tag 격리·BEAR/BULL 전략 편대 재분류")
    if dossier.kelly_elasticity_mult < 0.85:
        tomorrow.append("Kelly 탄력성·NAV HWM 동기화 점검")
    if not tomorrow and dossier.kill_switch_active:
        tomorrow.append("KILL_SWITCH 해제 전 신규 진입 금지 유지")
    if not tomorrow:
        tomorrow.append("MetaGovernor·당일 장부 팩트 교차검증 유지")

    lines.append(
        "<b>내일</b>: " + html.escape(tomorrow[0], quote=False)
        + (f" · {html.escape(tomorrow[1], quote=False)}" if len(tomorrow) > 1 else "")
    )

    return "\n".join(lines[:10])


def process_overseer_llm_narrative(
    dossier: Any,
    anomalies: Sequence[Any],
    llm_raw: Optional[str],
    *,
    api_fallback_prefix: str = "",
) -> LlmNarrativeResult:
    """
    LLM 원문 → sanitize → validate → (실패 시) deterministic 대체.
    """
    prefix = str(api_fallback_prefix or "").strip()
    raw = (llm_raw or "").strip()

    if not raw:
        det = build_deterministic_narrative(dossier, anomalies)
        return LlmNarrativeResult(det, "deterministic", True, ("no_llm_output",))

    if prefix and prefix in raw:
        det = build_deterministic_narrative(dossier, anomalies)
        return LlmNarrativeResult(det, "api_fallback", True, ("gemini_api_fallback",))

    clean = sanitize_overseer_narrative_html(raw)
    try:
        from llm_gemini_core import sanitize_user_visible_text

        clean = sanitize_user_visible_text(clean, task_id="overseer_audit") or clean
    except Exception:
        pass

    violations = validate_llm_narrative(clean, dossier, anomalies)
    if not violations:
        return LlmNarrativeResult(clean, "llm", True, ())

    det = build_deterministic_narrative(dossier, anomalies)
    viol_s = ", ".join(violations[:3])
    note = (
        f"\n<i>LLM 정합성 보정({html.escape(viol_s, quote=False)}) "
        f"→ 규칙 요약 대체</i>"
    )
    return LlmNarrativeResult(det + note, "deterministic", False, tuple(violations))


def format_overseer_llm_html_section(result: LlmNarrativeResult) -> str:
    """텔레그램 리포트에 붙일 LLM 섹션 HTML."""
    if not result.text:
        return ""
    src_note = ""
    if result.source == "deterministic" and not result.valid:
        src_note = ""  # already in text
    elif result.source == "deterministic":
        src_note = " <i>(규칙 요약)</i>"
    elif result.source == "api_fallback":
        src_note = " <i>(API 폴백→규칙 요약)</i>"
    out = "━━━ <b>[LLM 해석 · Ruthless QA]</b> ━━━\n"
    out += result.text + src_note + "\n"
    return out

"""
ReportStateBinder — 메타 거버너 상태 + 시스템 설정 + 장부(선택)를 단일 뷰 모델로 병합.
Telegram [1/9] 등 리포터는 이 블록만 포맷하고, 국면/켈리/국고는 여기서만 해석한다 (SSOT 분리).
"""

from __future__ import annotations

import html
import os
import statistics
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


@dataclass(frozen=True)
class MacroTreasuryReportBlock:
    """거시·국고 요약 — 소비자는 필드만 렌더링."""

    regime_key: str
    regime_confidence: Optional[float]
    regime_notes: str
    kelly_cap: Optional[float]
    kelly_floor: Optional[float]
    meta_global_kelly_mult: float
    base_dynamic_kelly_risk: float
    effective_kelly_risk: float
    treasury_config_raw: float
    ledger_realized_est: float
    oms_equity: Optional[float]
    treasury_footnote: str


@dataclass(frozen=True)
class LifecycleReportBlock:
    """MetaGovernor 전략 생애주기 스냅샷 — [8/9] 전용."""

    governor_last_run_at: Optional[str]
    governor_last_run_status: str
    n_live: int
    n_cooled: int
    n_candidate: int
    n_registry_total: int
    n_other_state: int
    retired_tracked_count: int
    health_summary_line: str
    autopilot_age_days: Optional[int]
    autopilot_age_source: str
    live_fleet_mean_age_days: Optional[float]
    footnote: str


def _coerce_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _regime_action(meta: Dict[str, Any]) -> Dict[str, Any]:
    ra = meta.get("META_REGIME_ACTION")
    return ra if isinstance(ra, dict) else {}


def _resolve_regime(meta: Optional[Dict[str, Any]], sys_config: Optional[Dict[str, Any]]) -> str:
    """표시·켈리용 국면 — Meta 우선, config 는 resolve_config_regime_key 와 동일 체인."""
    from meta_state_store import normalize_regime_key, resolve_config_regime_key

    m = meta or {}
    rk = normalize_regime_key(m.get("META_REGIME_KEY"))
    if rk not in ("", "UNKNOWN"):
        return rk
    return resolve_config_regime_key(sys_config)


def _resolve_regime_confidence(meta: Optional[Dict[str, Any]]) -> Optional[float]:
    m = meta or {}
    return _coerce_float(m.get("META_REGIME_CONFIDENCE"))


def _resolve_regime_notes(meta: Optional[Dict[str, Any]]) -> str:
    ra = _regime_action(meta or {})
    n = ra.get("notes")
    if isinstance(n, str):
        return n.strip()
    if isinstance(n, (list, tuple)):
        return "\n".join(str(x).strip() for x in n if str(x).strip())
    return ""


def _meta_with_optional_auto_heal(
    meta: Optional[Dict[str, Any]], *, auto_heal: bool
) -> Dict[str, Any]:
    """리포트용 meta — heal 실패·지속 degraded 시 CRITICAL 텔레그램."""
    if not auto_heal:
        if meta is not None and isinstance(meta, dict):
            return meta
        try:
            from meta_governor_consumer import load_meta_state_resolved

            return load_meta_state_resolved()
        except Exception:
            return {}
    try:
        from meta_state_store import ensure_meta_state_for_report, is_meta_state_degraded

        m = ensure_meta_state_for_report()
        if is_meta_state_degraded(m):
            try:
                from factory_meta_alerts import send_meta_critical_alert

                rk = str(m.get("META_REGIME_KEY") or "UNKNOWN")
                st = str(m.get("META_GOVERNOR_LAST_RUN_STATUS") or "NEVER")
                send_meta_critical_alert(
                    "Meta still degraded after auto-heal",
                    f"regime={rk} status={st}",
                    prefix="META_BRAIN",
                )
            except Exception:
                pass
        return m
    except Exception as ex:
        fallback = meta if isinstance(meta, dict) else {}
        try:
            from factory_meta_alerts import send_meta_critical_alert

            send_meta_critical_alert(
                "Meta auto-heal failed in report",
                str(ex),
                prefix="META_BRAIN",
            )
        except Exception:
            pass
        return fallback


def _resolve_kelly_cap_floor(meta: Optional[Dict[str, Any]]) -> tuple[Optional[float], Optional[float]]:
    ra = _regime_action(meta or {})
    return _coerce_float(ra.get("kelly_cap")), _coerce_float(ra.get("kelly_floor"))


def _resolve_kelly_display(
    meta: Optional[Dict[str, Any]],
    sys_config: Optional[Dict[str, Any]],
) -> tuple[float, float, float]:
    """
    리포트용 켈리: 베이스(system) × META_GLOBAL_KELLY_MULT 후 regime kelly_cap/floor 클램프.
    (per-entry 의 NS/GROUP 멀티플라이어는 포함하지 않음 — 일일 브리핑 요약과 동일 계열.)
    """
    c = sys_config or {}
    base = float(c.get("DYNAMIC_KELLY_RISK", 0.01) or 0.01)
    m = meta or {}
    g = float(m.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    eff = base * g
    cap, floor = _resolve_kelly_cap_floor(m)
    if floor is not None:
        eff = max(eff, floor)
    if cap is not None:
        eff = min(eff, cap)
    eff = max(eff, 0.0)
    return float(base), float(g), float(eff)


def _treasury_from_config(sys_config: Optional[Dict[str, Any]], treasury_config_key: str) -> float:
    c = sys_config or {}
    return float(c.get(treasury_config_key, 0) or 0.0)


def _treasury_from_ledger(
    df_closed_real: Optional[pd.DataFrame],
    *,
    zero_invest_fallback: Optional[float],
    market: str = "KR",
) -> float:
    """청산 행만 — weekly_flow_pnl SSOT(coalesce) 합."""
    from weekly_flow_pnl import dataframe_realized_pnl_sum

    if df_closed_real is None or df_closed_real.empty:
        return 0.0
    return dataframe_realized_pnl_sum(
        df_closed_real,
        market=market,
        zero_fallback=zero_invest_fallback,
    )


def _merge_treasury_policy(
    treasury_config_raw: float,
    ledger_realized_est: float,
    oms_equity: Optional[float],
    sys_config: Optional[Dict[str, Any]],
) -> str:
    """이중 계상 방지: 설정 국고와 장부 누적을 명시적으로 분리; 합산은 기본 안 함."""
    c = sys_config or {}
    mode = str(
        c.get("REPORT_TREASURY_NAV_MODE")
        or os.environ.get("REPORT_TREASURY_NAV_MODE", "")
        or "split"
    ).strip().lower()
    nav_hint = ""
    if mode in ("sum", "sum_estimate", "nav_estimate"):
        est = float(treasury_config_raw) + float(ledger_realized_est)
        nav_hint = (
            f" 참고 NAV 추정(설정+가상청산누적)={est:,.2f} — "
            "이미 손익이 config 국고에 반영된 경우 중복이므로 무시하세요."
        )
    core = (
        "※ 설정 국고(CENTRAL/TREASURY_*)는 구성 파일 SSOT. "
        "가상 청산 누적은 해당 시장 축 forward_trades 청산 행만 집계(INCUBATOR 제외는 호출부). "
        "자동 동기화가 국고를 갱신한다면 두 값을 단순 합산하지 마세요."
    )
    oms_line = ""
    if oms_equity is not None:
        oms_line = f" OMS·실계좌 대조 잔고={float(oms_equity):,.4f}."
    return core + nav_hint + oms_line


def build_macro_treasury_block(
    *,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    df_closed_real: Optional[pd.DataFrame] = None,
    treasury_config_key: str,
    ledger_zero_invest_fallback: Optional[float] = 400000.0,
    oms_equity: Optional[float] = None,
    auto_heal_meta: bool = True,
) -> MacroTreasuryReportBlock:
    """
    treasury_config_key: 예) CENTRAL_TREASURY_KR, TREASURY_SPOT_USDT
    df_closed_real: 해당 시장(또는 market_type)만, 비-INCUBATOR, CLOSED 만 넘기는 것을 권장.
    ledger_zero_invest_fallback: KR 원화 장부의 0원 투입 보정(40만). 암호화폐 등은 None.
    auto_heal_meta: UNKNOWN/NEVER 시 regime+MetaGovernor 동기 복구 후 리포트.
    """
    m = _meta_with_optional_auto_heal(meta, auto_heal=auto_heal_meta)
    try:
        from meta_state_store import (
            is_meta_state_degraded,
            reconcile_meta_regime_action,
            sync_config_regime_from_meta,
        )

        m = reconcile_meta_regime_action(m)
        if auto_heal_meta and not is_meta_state_degraded(m):
            sync_config_regime_from_meta(m)
            from config_manager import load_system_config

            sys_config = load_system_config()
    except Exception:
        pass
    regime = _resolve_regime(m, sys_config)
    conf = _resolve_regime_confidence(m)
    notes = _resolve_regime_notes(m)
    cap, floor = _resolve_kelly_cap_floor(m)
    base_k, g_mult, eff_k = _resolve_kelly_display(m, sys_config)
    raw = _treasury_from_config(sys_config, treasury_config_key)
    mkt = "US" if "US" in str(treasury_config_key).upper() else "KR"
    led = _treasury_from_ledger(
        df_closed_real,
        zero_invest_fallback=ledger_zero_invest_fallback,
        market=mkt,
    )
    foot = _merge_treasury_policy(raw, led, oms_equity, sys_config)
    return MacroTreasuryReportBlock(
        regime_key=regime,
        regime_confidence=conf,
        regime_notes=notes,
        kelly_cap=cap,
        kelly_floor=floor,
        meta_global_kelly_mult=g_mult,
        base_dynamic_kelly_risk=base_k,
        effective_kelly_risk=eff_k,
        treasury_config_raw=raw,
        ledger_realized_est=led,
        oms_equity=oms_equity,
        treasury_footnote=foot,
    )


def _fmt_amount(value: float, *, decimals: int) -> str:
    spec = f",.{decimals}f"
    return format(float(value), spec)


def format_macro_treasury_section_html(
    block: MacroTreasuryReportBlock,
    *,
    display_label: str,
    market_icon: str,
    today_str: str,
    lead_in_html: str = "",
    currency_suffix: str = "원",
    amount_decimals: int = 0,
) -> str:
    """Telegram HTML. 사용자 입력(notes)은 이스케이프."""
    rk_esc = html.escape(block.regime_key, quote=False)
    head = f"{market_icon} <b>[1/9] 거시 국면 및 국고(Treasury) 현황</b>\n"
    line_date = f"📅 {today_str} | 국면: <b>{rk_esc}</b>"
    if block.regime_confidence is not None:
        line_date += f" (Meta 신뢰도 {_fmt_amount(float(block.regime_confidence), decimals=2)})"
    line_date += "\n"
    t_raw = _fmt_amount(block.treasury_config_raw, decimals=amount_decimals)
    t_led = _fmt_amount(block.ledger_realized_est, decimals=amount_decimals)
    if block.ledger_realized_est >= 0:
        t_led_sign = f"+{t_led}"
    else:
        t_led_sign = t_led
    body = head + line_date
    body += f"🏦 <b>{html.escape(display_label, quote=False)} 국고(설정):</b> {t_raw} {currency_suffix}\n"
    body += f"📈 <b>가상 청산 누적(켈리 노출):</b> {t_led_sign} {currency_suffix}\n"
    body += (
        f"⚖️ 켈리: 베이스 {_fmt_amount(block.base_dynamic_kelly_risk * 100.0, decimals=2)}% "
        f"× Meta글로벌 <b>{_fmt_amount(block.meta_global_kelly_mult, decimals=3)}</b> "
        f"→ 유효 <b>{_fmt_amount(block.effective_kelly_risk * 100.0, decimals=2)}%</b>\n"
    )
    if block.kelly_cap is not None or block.kelly_floor is not None:
        cap_s = _fmt_amount(float(block.kelly_cap), decimals=4) if block.kelly_cap is not None else "—"
        fl_s = _fmt_amount(float(block.kelly_floor), decimals=4) if block.kelly_floor is not None else "—"
        body += f" ◽ 레짐 캡/플로어: cap {cap_s} | floor {fl_s}\n"
    body += "<i>※ 아래 누적 손익·리더보드·순환·DNA 통계는 [INCUBATOR_] 섀도우 제외.</i>\n"
    body += "\n🗣️ <b>[MetaGovernor 근거]</b>\n"
    if block.regime_notes:
        body += html.escape(block.regime_notes, quote=False) + "\n"
    else:
        body += (
            "<i>— MetaGovernor notes 미기록. "
            f"현재 국면 <b>{rk_esc}</b> — "
            "서버에서 <code>python3 -c \"from meta_state_store import rebuild_meta_state; "
            "print(rebuild_meta_state(force=True))\"</code> 실행 후 재확인.</i>\n"
        )
    body += f"\n<i>{html.escape(block.treasury_footnote, quote=False)}</i>\n"
    return lead_in_html + body


# --- MetaGovernor lifecycle ([8/9]) -------------------------------------------------

_EPOCH_DATE_KEYS: Tuple[str, ...] = (
    "META_AUTOPILOT_EPOCH",
    "AUTOPILOT_EPOCH_DATE",
    "LIVE_A_PROMOTION_DATE",
)


def _parse_iso_datetime(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _coerce_calendar_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    s = str(val).strip()
    if len(s) >= 10:
        s = s[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _resolve_explicit_autopilot_epoch(
    meta: Optional[Dict[str, Any]],
    sys_config: Optional[Dict[str, Any]],
) -> Tuple[Optional[date], str]:
    bags: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(sys_config, dict):
        bags.append(("sys_config", sys_config))
    if isinstance(meta, dict):
        bags.append(("meta", meta))
    for bag_label, bag in bags:
        for key in _EPOCH_DATE_KEYS:
            raw = bag.get(key)
            d = _coerce_calendar_date(raw)
            if d is None:
                continue
            return d, f"{bag_label}:{key}"
    return None, ""


def _live_fleet_age_days(
    live_rows: List[Dict[str, Any]],
    now: datetime,
) -> Tuple[List[int], str]:
    """Per-row whole-day ages vs `now` (timezone-aligned). Empty if no parseable rows."""
    ages: List[int] = []
    if not live_rows:
        return ages, "no_live_rows"
    now_aware = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    for row in live_rows:
        dt = _parse_iso_datetime(row.get("updated_at"))
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(now_aware.tzinfo)
        delta = now_aware - dt
        ages.append(max(0, int(delta.days)))
    if not ages:
        return [], "live_rows_without_updated_at"
    return ages, "ok"


def _summarize_meta_strategy_health(health: Any) -> str:
    if not isinstance(health, dict) or not health:
        return "META_STRATEGY_HEALTH 가 비어 있음 — MetaGovernor 갱신·스냅샷을 확인하십시오."
    meta_blk = health.get("__meta__")
    window_hint = ""
    if isinstance(meta_blk, dict):
        w = meta_blk.get("window_days_kst")
        nrows = meta_blk.get("n_rows")
        if w is not None and nrows is not None:
            window_hint = f" (최근 {w}일·원천행 {nrows}건)"
    rows: List[Dict[str, Any]] = []
    for k, v in health.items():
        if k == "__meta__" or not isinstance(v, dict):
            continue
        rows.append(v)
    if not rows:
        return "감시 그룹 헬스 행이 없음." + window_hint
    actionable = [v for v in rows if int(v.get("n", 0) or 0) > 0]
    if not actionable:
        return "유효 표본(거래 수 n이 양수)인 그룹 없음 — 편입 후 MetaGovernor 재실행 필요." + window_hint
    zeroed = sum(1 for v in actionable if float(v.get("mult", 1.0) or 1.0) <= 0.0)
    wr_vals = [
        float(v.get("rolling_wr", 0.0) or 0.0) for v in actionable if "rolling_wr" in v
    ]
    worst_wr = min(wr_vals) if wr_vals else None
    line = f"감시 {len(actionable)}그룹 중 Kelly mult≤0: {zeroed} | 롤링WR 샘플 {len(wr_vals)}개"
    if worst_wr is not None:
        line += f" (최저 {worst_wr * 100:.0f}% 근방)"
    return line + window_hint


def build_lifecycle_report_block(
    *,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    now: datetime,
    auto_heal_meta: bool = True,
) -> LifecycleReportBlock:
    """
    META_STRATEGY_REGISTRY 를 순회해 LIVE/COOLED/CANDIDATE 를 집계하고,
    오토파일럿 앵커 일령은 (1) 설정·메타의 명시 기준일 → (2) LIVE 편대의 최소 연령
    (가장 최근 updated_at 에 가까운 전략 기준, 일 환산) 순으로 결정한다.
    """
    m = _meta_with_optional_auto_heal(meta, auto_heal=auto_heal_meta)
    reg_raw = m.get("META_STRATEGY_REGISTRY")
    reg: List[Dict[str, Any]] = [r for r in reg_raw if isinstance(r, dict)] if isinstance(reg_raw, list) else []

    n_live = n_cooled = n_candidate = n_other = 0
    live_rows: List[Dict[str, Any]] = []
    for row in reg:
        st = str(row.get("state") or "").strip().upper()
        if st == "LIVE":
            n_live += 1
            live_rows.append(row)
        elif st == "COOLED":
            n_cooled += 1
        elif st == "CANDIDATE":
            n_candidate += 1
        elif st:
            n_other += 1

    retired = m.get("META_RETIRED_STRATEGY_IDS")
    retired_n = len(retired) if isinstance(retired, list) else 0

    governor_at = m.get("META_GOVERNOR_LAST_RUN_AT")
    governor_at_s = str(governor_at) if governor_at not in (None, "") else None
    governor_status = str(m.get("META_GOVERNOR_LAST_RUN_STATUS") or "UNKNOWN")

    health_line = _summarize_meta_strategy_health(m.get("META_STRATEGY_HEALTH"))

    explicit_d, explicit_src = _resolve_explicit_autopilot_epoch(m, sys_config if isinstance(sys_config, dict) else {})
    ages, ages_reason = _live_fleet_age_days(live_rows, now)
    mean_age: Optional[float] = float(statistics.mean(ages)) if ages else None
    min_age: Optional[int] = min(ages) if ages else None

    autopilot_age: Optional[int] = None
    age_source = ""
    if explicit_d is not None:
        now_d = now.date()
        autopilot_age = max(0, (now_d - explicit_d).days)
        age_source = explicit_src
    elif min_age is not None:
        autopilot_age = min_age
        age_source = f"live_fleet_min_age:{ages_reason}"
    else:
        age_source = f"unresolved:{ages_reason}"

    footnote = (
        "COOLED 은 레지스트리 스냅샷 개수만 반영합니다. 강등일시는 last_demoted_at 미도입으로 구분되지 않습니다."
    )

    return LifecycleReportBlock(
        governor_last_run_at=governor_at_s,
        governor_last_run_status=governor_status,
        n_live=n_live,
        n_cooled=n_cooled,
        n_candidate=n_candidate,
        n_registry_total=len(reg),
        n_other_state=n_other,
        retired_tracked_count=retired_n,
        health_summary_line=health_line,
        autopilot_age_days=autopilot_age,
        autopilot_age_source=age_source,
        live_fleet_mean_age_days=mean_age,
        footnote=footnote,
    )


def _fmt_intish(x: float) -> str:
    if abs(x - round(x)) < 1e-9:
        return str(int(round(x)))
    return f"{x:.1f}"


def format_lifecycle_section_html(
    block: LifecycleReportBlock,
    *,
    market_icon: str,
    today_str: str,
) -> str:
    """Telegram HTML — 숫자·SSOT 메타만 서술, 코사인/ML 등 외생 변수는 쓰지 않는다."""
    head = f"{market_icon} <b>[8/9] 메타 최적화 및 알파 반감기</b>\n"
    head += f"📅 {html.escape(today_str, quote=False)} | 레지스트리 <b>{block.n_registry_total}</b>행"
    head += f" (LIVE {block.n_live} · COOLED {block.n_cooled} · CANDIDATE {block.n_candidate}"
    if block.n_other_state:
        head += f" · 기타 {block.n_other_state}"
    head += ")\n"
    gov_at_esc = html.escape(block.governor_last_run_at or "—", quote=False)
    gov_st_esc = html.escape(block.governor_last_run_status, quote=False)
    head += f"🛰️ MetaGovernor: <i>last_at</i> {gov_at_esc} | <i>status</i> <b>{gov_st_esc}</b>\n"
    head += f"📚 은퇴 ID 추적 리스트: <b>{block.retired_tracked_count}</b>개 (상한 컷)\n"

    if block.autopilot_age_days is not None:
        src_esc = html.escape(block.autopilot_age_source, quote=False)
        head += f"⏳ 오토파일럿 앵커 수명: <b>{block.autopilot_age_days}일차</b> <i>(기준 {src_esc})</i>\n"
    else:
        head += "⏳ 오토파일럿 앵커 수명: <b>—</b> <i>(명시 기준일·LIVE 일령 모두 산출 불가)</i>\n"

    if block.n_live > 0 and block.live_fleet_mean_age_days is not None:
        head += (
            f"🎯 LIVE 편대 평균 레지스트리 일령: <b>{_fmt_intish(block.live_fleet_mean_age_days)}</b> "
            f"<i>(updated_at 기준)</i>\n"
        )
    elif block.n_live > 0:
        head += "🎯 LIVE 편대 평균 일령: <b>—</b> <i>(updated_at 파싱 불가)</i>\n"

    head += "🩺 <b>[전략 헬스 한줄]</b> " + html.escape(block.health_summary_line, quote=False) + "\n\n"

    # 동적 관제탑 멘트
    live_clause = ""
    if block.n_live > 0:
        if block.live_fleet_mean_age_days is not None:
            live_clause = (
                f"현재 LIVE 편대 {block.n_live}개가 평균 "
                f"{_fmt_intish(block.live_fleet_mean_age_days)}일째 순항 중"
            )
        else:
            live_clause = f"현재 LIVE 편대 {block.n_live}개가 운용 중"

    extras: List[str] = []
    if block.n_cooled > 0:
        extras.append(
            f"알파 수명 압박으로 {block.n_cooled}개 전략이 COOLED(벤치)로 강등되어 뇌수술·재기동 대기 중입니다"
        )
    if block.n_candidate > 0:
        extras.append(f"CANDIDATE {block.n_candidate}개가 승격 관측 구간에 있습니다")

    if live_clause and extras:
        narrative = live_clause + "이며, " + " 또한 ".join(extras)
    elif live_clause:
        narrative = live_clause + "."
    elif extras:
        narrative = " 또한 ".join(extras)
    elif str(block.governor_last_run_status or "").upper() in ("NEVER", "", "UNKNOWN"):
        narrative = (
            "MetaGovernor 상태가 미기동(NEVER)입니다. "
            "factory_artifact_guard·meta_governor.py 로 레지스트리를 재생성하십시오."
        )
    else:
        narrative = (
            "레지스트리에 LIVE·CANDIDATE 배치가 없거나 메타 스냅샷이 비어 있습니다. "
            "MetaGovernor 실행·승격 파이프라인을 확인하십시오."
        )

    body = "🗣️ <b>[관제탑 시선]</b> " + narrative + "\n"
    body += f"\n<i>{html.escape(block.footnote, quote=False)}</i>\n"
    return head + body

"""
Bitget ReportStateBinder — [1/9] macro/treasury + [8/9] lifecycle SSOT (USDT).

주식 `reports/report_state_binder.py` 패턴을 코인 SPOT/FUTURES에 맞게 이식한다.
"""
from __future__ import annotations

import html
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from bitget.governance.meta_consumer import resolve_trading_kelly_base
from bitget.infra.clock import parse_utc_iso, utc_now
from bitget.infra.market_keys import normalize_market_type, to_report_label
from bitget.live_nav_manager import get_market_state, live_nav


@dataclass(frozen=True)
class MacroTreasuryReportBlock:
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
    treasury_footnote: str
    market: str = "spot"
    nav: Optional[float] = None
    hwm: Optional[float] = None
    mdd_pct: Optional[float] = None
    base_capital: Optional[float] = None
    macro_freshness: Optional[str] = None
    defcon: Optional[Any] = None
    breadth_status: Optional[str] = None


@dataclass(frozen=True)
class LifecycleReportBlock:
    governor_last_run_at: Optional[str]
    governor_last_run_status: str
    n_live: int
    n_cooled: int
    n_candidate: int
    n_observing: int
    n_retired: int
    n_registry_total: int
    n_other_state: int
    retired_tracked_count: int
    health_summary_line: str
    autopilot_age_days: Optional[int]
    autopilot_age_source: str
    live_fleet_mean_age_days: Optional[float]
    cycle_discovery_new: int
    cycle_promoted_live: int
    cycle_demoted_cooled: int
    demoted_last_7d: int
    live_spot: int
    live_futures: int
    cooled_spot: int
    cooled_futures: int
    candidate_spot: int
    candidate_futures: int
    avg_alpha_life_days_spot: Optional[float]
    avg_alpha_life_days_futures: Optional[float]
    health_groups_linked_live: int
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


def _normalize_registry_market(raw: Any) -> str:
    m = str(raw or "").upper()
    if m in ("US", "FUT", "FUTURES", "BG_FUTURES"):
        return "futures"
    return "spot"


def _resolve_regime(meta: Optional[Dict[str, Any]], sys_config: Optional[Dict[str, Any]]) -> str:
    m = meta or {}
    rk = str(m.get("META_REGIME_KEY") or "").strip().upper()
    if rk and rk != "UNKNOWN":
        return rk
    c = sys_config or {}
    return str(c.get("CURRENT_REGIME_KEY") or "UNKNOWN").strip().upper() or "UNKNOWN"


def _meta_with_optional_auto_heal(
    meta: Optional[Dict[str, Any]], *, auto_heal: bool
) -> Dict[str, Any]:
    if not auto_heal:
        if isinstance(meta, dict) and meta:
            return meta
        try:
            from bitget.governance.meta_sync import load_bitget_meta_resolved

            return load_bitget_meta_resolved()
        except Exception:
            return {}
    try:
        from bitget.governance.meta_sync import (
            ensure_config_regime_aligned,
            is_bitget_meta_degraded,
            load_bitget_meta_resolved,
            rebuild_bitget_meta_state,
        )

        m0 = load_bitget_meta_resolved()
        if is_bitget_meta_degraded(m0):
            rebuild_bitget_meta_state(force=False, refresh_regime=True)
            ensure_config_regime_aligned()
            try:
                from bitget.governance.meta_alerts import send_meta_critical_alert

                m1 = load_bitget_meta_resolved()
                if is_bitget_meta_degraded(m1):
                    send_meta_critical_alert(
                        "Meta still degraded after auto-heal",
                        f"regime={m1.get('META_REGIME_KEY')} status={m1.get('META_GOVERNOR_LAST_RUN_STATUS')}",
                        prefix="BITGET_META",
                    )
            except Exception:
                pass
        return load_bitget_meta_resolved()
    except Exception:
        return meta if isinstance(meta, dict) else {}


def _resolve_kelly_cap_floor(meta: Optional[Dict[str, Any]]) -> tuple[Optional[float], Optional[float]]:
    ra = _regime_action(meta or {})
    return _coerce_float(ra.get("kelly_cap")), _coerce_float(ra.get("kelly_floor"))


def _resolve_kelly_display(
    meta: Optional[Dict[str, Any]],
    sys_config: Optional[Dict[str, Any]],
) -> tuple[float, float, float]:
    c = sys_config or {}
    m = meta or {}
    g = float(m.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    cap, floor = _resolve_kelly_cap_floor(m)
    base = float(c.get("DYNAMIC_KELLY_RISK", 0.02) or 0.02)
    eff = resolve_trading_kelly_base(c, m)
    if cap is not None:
        eff = min(eff, cap)
    if floor is not None:
        eff = max(eff, floor)
    return float(base), float(g), float(max(eff, 0.0))


def _ledger_realized_usdt(df_closed: Optional[pd.DataFrame]) -> float:
    if df_closed is None or df_closed.empty:
        return 0.0
    total = 0.0
    for _, row in df_closed.iterrows():
        try:
            inv = float(row.get("sim_kelly_invest", 0) or 0)
            ret = float(row.get("final_ret", 0) or 0)
            total += inv * ret / 100.0
        except (TypeError, ValueError):
            continue
    return float(total)


def build_macro_treasury_block(
    *,
    market_type: str,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    df_closed_real: Optional[pd.DataFrame] = None,
    treasury_config_key: str,
    auto_heal_meta: bool = True,
) -> MacroTreasuryReportBlock:
    mkt = normalize_market_type(market_type)
    m = _meta_with_optional_auto_heal(meta, auto_heal=auto_heal_meta)
    try:
        from bitget.governance.meta_sync import ensure_config_regime_aligned

        if auto_heal_meta:
            ensure_config_regime_aligned()
            from bitget.infra.config_manager import load_system_config

            sys_config = load_system_config()
    except Exception:
        pass
    regime = _resolve_regime(m, sys_config)
    ra = _regime_action(m)
    notes_raw = ra.get("notes")
    if isinstance(notes_raw, str):
        notes = notes_raw.strip()
    elif isinstance(notes_raw, (list, tuple)):
        notes = "\n".join(str(x).strip() for x in notes_raw if str(x).strip())
    else:
        notes = ""
    cap, floor = _resolve_kelly_cap_floor(m)
    base_k, g_mult, eff_k = _resolve_kelly_display(m, sys_config)
    raw = float((sys_config or {}).get(treasury_config_key, 0) or 0)
    led = _ledger_realized_usdt(df_closed_real)
    st = get_market_state(mkt)
    nav_val = live_nav(mkt)
    foot = (
        "※ Live NAV 는 bitget_treasury_state.json 의 복리 자산(청산마다 동기화). "
        "설정 국고(현금)와 장부 청산누적은 분리 표시."
    )
    c = sys_config or {}
    fresh = str(c.get("BITGET_MACRO_FRESHNESS") or c.get("MACRO_DAILY_FRESHNESS") or "") or None
    return MacroTreasuryReportBlock(
        regime_key=regime,
        regime_confidence=_coerce_float(m.get("META_REGIME_CONFIDENCE")),
        regime_notes=notes,
        kelly_cap=cap,
        kelly_floor=floor,
        meta_global_kelly_mult=g_mult,
        base_dynamic_kelly_risk=base_k,
        effective_kelly_risk=eff_k,
        treasury_config_raw=raw,
        ledger_realized_est=led,
        treasury_footnote=foot,
        market=mkt,
        nav=nav_val,
        hwm=_coerce_float(st.get("hwm")),
        mdd_pct=_coerce_float(st.get("mdd_pct")),
        base_capital=_coerce_float(st.get("base_capital")),
        macro_freshness=fresh,
        defcon=c.get("DOOMSDAY_DEFCON"),
        breadth_status=str(c.get("CRYPTO_BREADTH_STATUS") or "") or None,
    )


def format_macro_treasury_section_html(
    block: MacroTreasuryReportBlock,
    *,
    display_label: str,
    market_icon: str,
    today_str: str,
    lead_in_html: str = "",
) -> str:
    rk_esc = html.escape(block.regime_key, quote=False)
    head = f"{market_icon} <b>[1/9] {html.escape(display_label, quote=False)} 국면/국고 현황</b>\n"
    line_date = f"📅 {today_str} UTC | 국면: <b>{rk_esc}</b>"
    if block.regime_confidence is not None:
        line_date += f" (Meta 신뢰도 {block.regime_confidence:.2f})"
    if block.macro_freshness == "lookback":
        line_date += " <b>(⚠️ 전일 매크로 데이터 재사용 중)</b>"
    elif block.macro_freshness == "degraded":
        line_date += " <b>(⚠️ 매크로 데이터 부재 — Degraded)</b>"
    line_date += "\n"
    body = head + line_date

    if block.nav is not None:
        base_v = block.base_capital if block.base_capital else block.nav
        ret_pct = ((block.nav / base_v - 1.0) * 100.0) if base_v else 0.0
        hwm_v = block.hwm if block.hwm is not None else block.nav
        hwm_reach = (block.nav / hwm_v * 100.0) if hwm_v else 100.0
        mdd_v = block.mdd_pct if block.mdd_pct is not None else 0.0
        ret_icon = "🟢" if ret_pct >= 0 else "🔴"
        body += (
            f"💎 <b>Live NAV:</b> {block.nav:,.2f} USDT "
            f"({ret_icon}{ret_pct:+.2f}% · 기준 {base_v:,.2f})\n"
        )
        body += f"🏔️ <b>HWM:</b> {hwm_v:,.2f} USDT (달성률 {hwm_reach:.1f}%)\n"
        body += f"📉 <b>MDD:</b> -{mdd_v:.2f}%\n"
    else:
        body += "💎 <b>Live NAV:</b> <i>미초기화 — 청산 후 자동 갱신됩니다.</i>\n"

    body += f"🏦 잔여 국고(현금): <b>{block.treasury_config_raw:,.2f} USDT</b>\n"
    body += (
        f"⚖️ 켈리: 베이스 {block.base_dynamic_kelly_risk * 100:.3f}% "
        f"× Meta글로벌 <b>{block.meta_global_kelly_mult:.3f}</b> "
        f"→ 유효 <b>{block.effective_kelly_risk * 100:.3f}%</b>\n"
    )
    if block.kelly_cap is not None or block.kelly_floor is not None:
        cap_s = f"{block.kelly_cap:.4f}" if block.kelly_cap is not None else "—"
        fl_s = f"{block.kelly_floor:.4f}" if block.kelly_floor is not None else "—"
        body += f" ◽ 레짐 캡/플로어: cap {cap_s} | floor {fl_s}\n"
    if block.breadth_status:
        body += f"🌊 Breadth: {html.escape(block.breadth_status, quote=False)}\n"
    if block.defcon is not None:
        body += f"☢️ DEFCON: <b>{html.escape(str(block.defcon), quote=False)}</b>\n"
    try:
        from bitget.meta_learner_bg import build_meta_cognition_line

        body += build_meta_cognition_line() + "\n"
    except Exception:
        pass
    if block.regime_notes:
        body += html.escape(block.regime_notes, quote=False) + "\n"
    body += f"\n<i>{html.escape(block.treasury_footnote, quote=False)}</i>\n"
    return lead_in_html + body


# --- Lifecycle [8/9] -----------------------------------------------------------------

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
    return parse_utc_iso(s)


def _coerce_calendar_date(val: Any) -> Optional[date]:
    if val is None:
        return None
    s = str(val).strip()[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _summarize_meta_strategy_health(health: Any) -> str:
    if not isinstance(health, dict) or not health:
        return "META_STRATEGY_HEALTH 가 비어 있음 — MetaGovernor 갱신을 확인하십시오."
    rows = [v for k, v in health.items() if k != "__meta__" and isinstance(v, dict)]
    if not rows:
        return "감시 그룹 헬스 행이 없음."
    actionable = [v for v in rows if int(v.get("n", 0) or 0) > 0]
    if not actionable:
        return "유효 표본(n>0)인 그룹 없음."
    zeroed = sum(1 for v in actionable if float(v.get("mult", 1.0) or 1.0) <= 0.0)
    return f"감시 {len(actionable)}그룹 · Kelly mult≤0: {zeroed}개"


def build_lifecycle_report_block(
    *,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
    auto_heal_meta: bool = True,
) -> LifecycleReportBlock:
    now = now or utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_aware = now
    m = _meta_with_optional_auto_heal(meta, auto_heal=auto_heal_meta)
    reg_raw = m.get("META_STRATEGY_REGISTRY")
    reg: List[Dict[str, Any]] = [r for r in reg_raw if isinstance(r, dict)] if isinstance(reg_raw, list) else []

    n_live = n_cooled = n_candidate = n_observing = n_retired = n_other = 0
    live_rows: List[Dict[str, Any]] = []
    cooled_rows: List[Dict[str, Any]] = []
    live_spot = live_futures = cooled_spot = cooled_futures = 0
    candidate_spot = candidate_futures = 0

    for row in reg:
        st = str(row.get("state") or "").strip().upper()
        mk = _normalize_registry_market(row.get("market"))
        if st == "LIVE":
            n_live += 1
            live_rows.append(row)
            if mk == "futures":
                live_futures += 1
            else:
                live_spot += 1
        elif st == "COOLED":
            n_cooled += 1
            cooled_rows.append(row)
            if mk == "futures":
                cooled_futures += 1
            else:
                cooled_spot += 1
        elif st == "CANDIDATE":
            n_candidate += 1
            if mk == "futures":
                candidate_futures += 1
            else:
                candidate_spot += 1
        elif st == "OBSERVING":
            n_observing += 1
        elif st == "RETIRED":
            n_retired += 1
        elif st:
            n_other += 1

    retired = m.get("META_RETIRED_STRATEGY_IDS")
    retired_n = len(retired) if isinstance(retired, list) else 0
    cycle = m.get("META_REGISTRY_CYCLE_STATS") if isinstance(m.get("META_REGISTRY_CYCLE_STATS"), dict) else {}

    explicit_d = None
    age_source = ""
    c = sys_config if isinstance(sys_config, dict) else {}
    for bag in (c, m):
        for key in _EPOCH_DATE_KEYS:
            d = _coerce_calendar_date(bag.get(key))
            if d is not None:
                explicit_d = d
                age_source = key
                break
        if explicit_d:
            break

    ages: List[int] = []
    for row in live_rows:
        dt = _parse_iso_datetime(row.get("promoted_at") or row.get("last_promoted_at") or row.get("updated_at"))
        if dt is None:
            continue
        ages.append(max(0, int((now_aware - dt).days)))

    autopilot_age = None
    if explicit_d is not None:
        autopilot_age = max(0, (now_aware.date() - explicit_d).days)
    elif ages:
        autopilot_age = min(ages)
        age_source = age_source or "live_fleet_min_age"

    mean_age = float(statistics.mean(ages)) if ages else None

    def _avg_life(rows: List[Dict[str, Any]], target: str) -> Optional[float]:
        spans: List[int] = []
        for row in rows:
            if _normalize_registry_market(row.get("market")) != target:
                continue
            t0 = _parse_iso_datetime(row.get("promoted_at") or row.get("last_promoted_at"))
            t1 = _parse_iso_datetime(row.get("last_demoted_at"))
            if t0 and t1:
                spans.append(max(0, int((t1 - t0).days)))
        return float(statistics.mean(spans)) if spans else None

    demoted_7d = int(cycle.get("demoted_7d", 0) or 0)
    if demoted_7d == 0:
        cutoff = now_aware - timedelta(days=7)
        for row in reg:
            dt = _parse_iso_datetime(row.get("last_demoted_at"))
            if dt and dt >= cutoff:
                demoted_7d += 1

    health_raw = m.get("META_STRATEGY_HEALTH")
    return LifecycleReportBlock(
        governor_last_run_at=str(m.get("META_GOVERNOR_LAST_RUN_AT") or "") or None,
        governor_last_run_status=str(m.get("META_GOVERNOR_LAST_RUN_STATUS") or "UNKNOWN"),
        n_live=n_live,
        n_cooled=n_cooled,
        n_candidate=n_candidate,
        n_observing=n_observing,
        n_retired=n_retired,
        n_registry_total=len(reg),
        n_other_state=n_other,
        retired_tracked_count=retired_n,
        health_summary_line=_summarize_meta_strategy_health(health_raw),
        autopilot_age_days=autopilot_age,
        autopilot_age_source=age_source or "unresolved",
        live_fleet_mean_age_days=mean_age,
        cycle_discovery_new=int(cycle.get("discovery_new", 0) or 0),
        cycle_promoted_live=int(cycle.get("promoted_live", 0) or 0),
        cycle_demoted_cooled=int(cycle.get("demoted_cooled", 0) or 0),
        demoted_last_7d=demoted_7d,
        live_spot=live_spot,
        live_futures=live_futures,
        cooled_spot=cooled_spot,
        cooled_futures=cooled_futures,
        candidate_spot=candidate_spot,
        candidate_futures=candidate_futures,
        avg_alpha_life_days_spot=_avg_life(cooled_rows, "spot"),
        avg_alpha_life_days_futures=_avg_life(cooled_rows, "futures"),
        health_groups_linked_live=0,
        footnote="Whipsaw 강등: rolling_wr·rolling_pf 미달 연속 시 COOLED. last_demoted_at 기준 7일 집계.",
    )


def format_lifecycle_section_html(
    block: LifecycleReportBlock,
    *,
    market_icon: str,
    today_str: str,
) -> str:
    head = f"{market_icon} <b>[8/9] 메타 최적화 및 알파 반감기</b>\n"
    head += (
        f"📅 {html.escape(today_str, quote=False)} | 레지스트리 <b>{block.n_registry_total}</b>행 "
        f"(LIVE {block.n_live} · CAND {block.n_candidate} · COOLED {block.n_cooled})\n"
    )
    head += (
        f"🛰️ MetaGovernor: {html.escape(block.governor_last_run_at or '—', quote=False)} | "
        f"<b>{html.escape(block.governor_last_run_status, quote=False)}</b>\n"
    )
    head += (
        f"🟢 SPOT LIVE <b>{block.live_spot}</b> · CAND {block.candidate_spot} · COOLED {block.cooled_spot}  |  "
        f"🟠 FUT LIVE <b>{block.live_futures}</b> · CAND {block.candidate_futures} · COOLED {block.cooled_futures}\n"
    )
    if block.demoted_last_7d:
        head += f"⬇️ 최근 7일 강등: <b>{block.demoted_last_7d}</b>건\n"
    if block.autopilot_age_days is not None:
        head += f"⏳ 오토파일럿 앵커: <b>{block.autopilot_age_days}일차</b> ({block.autopilot_age_source})\n"
    if block.n_live and block.live_fleet_mean_age_days is not None:
        head += f"🎯 LIVE 편대 평균 일령: <b>{block.live_fleet_mean_age_days:.0f}</b>일\n"
    head += "🩺 " + html.escape(block.health_summary_line, quote=False) + "\n"
    head += f"\n<i>{html.escape(block.footnote, quote=False)}</i>\n"
    return head

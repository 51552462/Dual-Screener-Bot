"""
Rules-first AI 감사관 SSOT — ReportStateBinder + MetaGovernor + 장부 팩트.
LLM은 해석만; Anomaly는 Python이 선행 판정.
"""
from __future__ import annotations

import html
import json
import os
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd
import pytz

from reports.report_state_binder import (
    LifecycleReportBlock,
    MacroTreasuryReportBlock,
    build_lifecycle_report_block,
    build_macro_treasury_block,
)

# ---------------------------------------------------------------------------
# LLM system prompt (전문 — ai_overseer 가 그대로 사용)
# Ch.6: overseer_llm_narrative.build_overseer_llm_system_prompt() 가 SSOT
# ---------------------------------------------------------------------------
def _overseer_llm_system_prompt_ssot() -> str:
    try:
        from overseer_llm_narrative import build_overseer_llm_system_prompt

        return build_overseer_llm_system_prompt()
    except Exception:
        pass
    return """You are a Ruthless QA engineer for a quant trading factory. You are NOT a cheerleader.

STRICT RULES:
1. Use ONLY facts in AUDIT_DOSSIER_JSON and ANOMALIES_JSON. Do not invent numbers, regimes, or trades.
2. If ANOMALIES_JSON is non-empty: your first sentence MUST acknowledge the highest-severity anomaly. NEVER contradict an anomaly headline.
3. NEVER reframe anomalies as "excellent defense", "탁월한 방어", "훌륭한 방어", "완벽히 동기화", or praise Kelly clamping without citing the exact MetaGovernor field that caused it.
4. Forbidden unless anomaly code is explicitly informational: "매우 훌륭한", "탁월한", "완벽", "잘 방어", "훌륭한 방어 상태".
5. Every causal claim MUST cite a dossier field (e.g. META_GLOBAL_KELLY_MULT, META_REGIME_KEY, meta_regime_action_notes, vix_summary).
6. Output in Korean, max 10 short lines, Telegram-safe HTML (<b>, <i> only). No markdown headers.
7. Do NOT repeat the full anomaly list; add 1–2 actionable checks for tomorrow only.
8. If zero anomalies: state neutrally what to monitor — still no praise fluff."""


OVERSEER_LLM_SYSTEM_PROMPT = _overseer_llm_system_prompt_ssot()

BULLISH_REGIME_KEYS: Set[str] = {
    "BULL",
    "RISK_ON",
    "RISK-ON",
    "GOLDILOCKS",
    "EXPANSION",
    "RISK_ON_EXPANSION",
}
DEFENSIVE_REGIME_KEYS: Set[str] = {
    "BEAR",
    "RISK_OFF",
    "RISK-OFF",
    "HIGH_VOL",
    "HIGH_VOLATILITY",
    "CRISIS",
    "DEFENSE",
}
STANDBY_REGIME_KEYS: Set[str] = {
    "STANDBY",
    "UNKNOWN",
    "WAIT",
    "NEUTRAL",
    "분석 대기중(STANDBY)",
}


@dataclass(frozen=True)
class AuditAnomaly:
    code: str
    severity: str  # CRITICAL | WARN
    headline: str
    evidence: str


@dataclass(frozen=True)
class OverseerAuditDossier:
    as_of_kst: str
    market_scope: str
    macro: MacroTreasuryReportBlock
    lifecycle: Optional[LifecycleReportBlock]
    meta_regime_key: str
    meta_regime_confidence: float
    meta_global_kelly_mult: float
    meta_treasury_mode: str
    meta_governor_last_run_at: Optional[str]
    meta_governor_last_run_status: str
    meta_regime_action_notes: str
    vix_summary: str
    block_trade_sources: Tuple[str, ...]
    trades_closed_today: int
    trades_entry_today: int
    trades_open: int
    win_rate_today_pct: Optional[float]
    overdrive_count_today: int
    overdrive_eligible_today: int
    overdrive_logged_today: int
    overdrive_loss_target_today: int
    overdrive_hurdle: float
    overdrive_v_energy_max_today: Optional[float]
    overdrive_supernova_closed_today: int
    overdrive_all_loss_sl_day: bool
    rnd_entry_today: int
    flow_tag_toxic_count: int
    flow_tag_top_penalty_tag: Optional[str]
    flow_tag_penalty_mult: Optional[float]
    toxic_tag_hits_today: int
    toxic_tag_entry_hits_today: int
    toxic_tag_exit_echo_hits_today: int
    regime_mismatch_entry_hits_today: int
    regime_mismatch_closed_hits_today: int
    catastrophic_clutch_active: bool
    catastrophic_clutch_mult: float
    csv_status: str
    config_regime_key: str
    effective_kelly_risk: float
    predicted_sector_kr: str
    predicted_sector_us: str
    effective_kelly_pre_overlay: float = 0.0
    kelly_day_clutch_mult: float = 1.0
    kelly_nav_dd_mult: float = 1.0
    kelly_elasticity_mult: float = 1.0
    nav_drawdown_pct: Optional[float] = None
    kill_switch_active: bool = False
    treasury_zeroed_groups: int = 0
    treasury_actionable_groups: int = 0
    treasury_zeroed_group_names: Tuple[str, ...] = ()
    governor_stale_hours: Optional[float] = None
    governor_is_stale: bool = False
    zero_group_entry_hits_today: int = 0
    db_error: Optional[str] = None


def _audit_thresholds(sys_config: Optional[Dict[str, Any]]) -> Dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    rules = cfg.get("OVERSEER_AUDIT_RULES")
    base = rules if isinstance(rules, dict) else {}

    def _f(key: str, default: float) -> float:
        try:
            return float(base.get(key, cfg.get(key, default)))
        except (TypeError, ValueError):
            return default

    return {
        # BULL 국면의 현실적 베이스: 2.0% 이상이면 경고를 만들지 않는다.
        "kelly_low": _f("OVERSEER_KELLY_LOW_THRESHOLD", 0.02),
        "kelly_crit": _f("OVERSEER_KELLY_CRIT_THRESHOLD", 0.015),
        "meta_mult_clamp": _f("OVERSEER_META_MULT_CLAMP", 0.92),
        "min_trades_bull_expect": _f("OVERSEER_BULL_MIN_TRADES", 0),
        # [P3-6] 당일 승률 붕괴 감지 — 최소 표본 이상 청산됐는데 사실상 전패인 날.
        "win_rate_catastrophic_pct": _f("OVERSEER_WIN_RATE_CATASTROPHIC_PCT", 5.0),
        "win_rate_min_n": _f("OVERSEER_WIN_RATE_MIN_N", 5),
    }


def _normalize_regime(rk: str) -> str:
    return str(rk or "").strip().upper().replace(" ", "_")


def _is_bullish_regime(rk: str) -> bool:
    n = _normalize_regime(rk)
    if n in BULLISH_REGIME_KEYS:
        return True
    return any(k in n for k in ("BULL", "RISK_ON", "GOLDILOCK"))


def _is_defensive_regime(rk: str) -> bool:
    n = _normalize_regime(rk)
    if n in DEFENSIVE_REGIME_KEYS:
        return True
    return any(k in n for k in ("BEAR", "RISK_OFF", "HIGH_VOL", "CRISIS", "DEFENSE"))


def _is_standby_regime(rk: str) -> bool:
    n = _normalize_regime(rk)
    if n in STANDBY_REGIME_KEYS:
        return True
    s = str(rk or "")
    return "STANDBY" in s.upper() or "대기" in s


def _summarize_vix(meta: Dict[str, Any]) -> str:
    blk = meta.get("META_VIX_LEVEL_Q")
    if not isinstance(blk, dict):
        return "VIX 스냅샷 없음"
    if blk.get("skipped"):
        return "VIX 스킵(오프라인/설정) — HIGH_VOL 규칙 미적용"
    vlast = blk.get("vix_last")
    qs = blk.get("vix_quantiles") or {}
    p90 = qs.get("p90")
    parts = []
    if vlast is not None:
        parts.append(f"VIX={vlast}")
    if p90 is not None:
        parts.append(f"p90={p90}")
    try:
        if vlast is not None and p90 is not None and float(vlast) >= float(p90):
            parts.append("≥p90(클램프 후보)")
    except (TypeError, ValueError):
        pass
    return " · ".join(parts) if parts else "VIX 데이터 수집됨"


def _sql_date_normalized(column: str) -> str:
    """
    exit_date/entry_date → SQLite date() 비교용 (KST today_str 과 '=').
    substr(…,1,10) 단독보다 date() 파싱이 TZ·공백·슬래시 혼합에 안전.
    """
    c = column.strip()
    cleaned = f"replace(replace(replace({c}, 'T', ' '), '/', '-'), '.', '-')"
    return f"date({cleaned})"


def _resolve_overseer_config_regime(
    meta: Dict[str, Any],
    sys_config: Dict[str, Any],
) -> str:
    """config UNKNOWN 시 MetaGovernor 국면으로 표시 (comprehensive 리포트 SSOT 정합)."""
    m = meta if isinstance(meta, dict) else {}
    c = sys_config if isinstance(sys_config, dict) else {}
    rk_meta = str(m.get("META_REGIME_KEY") or "").strip().upper()
    try:
        from meta_state_store import resolve_config_regime_key

        rk_cfg = resolve_config_regime_key(c)
    except Exception:
        rk_cfg = str(c.get("CURRENT_REGIME_KEY", "UNKNOWN") or "UNKNOWN")
    if rk_cfg in ("", "UNKNOWN") and rk_meta not in ("", "UNKNOWN"):
        return rk_meta
    return rk_cfg


def _resolve_overseer_kelly_display(
    meta: Dict[str, Any],
    sys_config: Dict[str, Any],
    macro_eff_k: float,
) -> float:
    """
    AI 감사관 Kelly — regime_kelly_failsafe SSOT (comprehensive 와 동일 경로).
    macro block 결과와 reconcile; UNKNOWN·1% 고착 시 graceful lift.
    """
    m = meta if isinstance(meta, dict) else {}
    c = sys_config if isinstance(sys_config, dict) else {}
    g = float(m.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
    try:
        from meta_state_store import resolve_config_regime_key
        from regime_kelly_failsafe import apply_graceful_kelly_to_effective
        from reports.report_state_binder import _resolve_kelly_cap_floor

        rk_cfg = resolve_config_regime_key(c)
        cap, floor = _resolve_kelly_cap_floor(m)
        eff, _base, _reason = apply_graceful_kelly_to_effective(
            float(c.get("DYNAMIC_KELLY_RISK", 0.01) or 0.01),
            g,
            cap,
            floor,
            sys_config=c,
            meta=m,
            config_regime_unknown=rk_cfg in ("", "UNKNOWN"),
        )
        return max(float(macro_eff_k), float(eff))
    except Exception:
        return float(macro_eff_k)


def _load_trade_frames(
    db_path: str,
    today_str: str,
    rolling_days: int = 90,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Optional[str]]:
    """(closed_today, entry_today, rolling_closed, open_rows, error)."""
    try:
        conn = sqlite3.connect(db_path, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        exit_d = _sql_date_normalized("exit_date")
        entry_d = _sql_date_normalized("entry_date")
        df_today = pd.read_sql(
            f"SELECT * FROM forward_trades WHERE {exit_d} = date(?)",
            conn,
            params=(today_str,),
        )
        df_entry = pd.read_sql(
            f"SELECT * FROM forward_trades WHERE {entry_d} = date(?)",
            conn,
            params=(today_str,),
        )
        cutoff = (
            datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=rolling_days)
        ).strftime("%Y-%m-%d")
        df_roll = pd.read_sql(
            f"""
            SELECT * FROM forward_trades
            WHERE status LIKE 'CLOSED%'
              AND {exit_d} >= ?
            """,
            conn,
            params=(cutoff,),
        )
        df_open = pd.read_sql(
            "SELECT * FROM forward_trades WHERE status NOT LIKE 'CLOSED%'",
            conn,
        )
        conn.close()
        return df_today, df_entry, df_roll, df_open, None
    except Exception as e:
        empty = pd.DataFrame()
        return empty, empty, empty, empty, str(e)


def _count_toxic_tag_hits(
    df: pd.DataFrame,
    penalty_mult: Dict[str, float],
) -> int:
    """청산 시 부여된 flow_tags 기준 매칭(텔레메트리·exit echo 전용)."""
    if df.empty or not penalty_mult or "flow_tags" not in df.columns:
        return 0
    hits = 0
    for tags_raw in df["flow_tags"].fillna(""):
        for tag in str(tags_raw).split():
            t = tag.strip()
            if t and t in penalty_mult:
                hits += 1
                break
    return hits


def _count_toxic_tag_entry_hits(
    df: pd.DataFrame,
    penalty_mult: Dict[str, float],
) -> int:
    """진입 sig_type 내 `#태그` ↔ 페널티 맵 — TOXIC_TAG_LEAK SSOT."""
    if df.empty or not penalty_mult or "sig_type" not in df.columns:
        return 0
    try:
        from forward_flow_tag_deep_dive import extract_flow_tags_from_text
    except ImportError:
        return 0
    hits = 0
    for sig_raw in df["sig_type"].fillna(""):
        for tag in extract_flow_tags_from_text(str(sig_raw)):
            if tag in penalty_mult:
                hits += 1
                break
    return hits


def build_overseer_audit_dossier(
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    db_path: str,
    csv_path: Optional[str] = None,
    rolling_days: int = 90,
) -> OverseerAuditDossier:
    tz_kr = pytz.timezone("Asia/Seoul")
    today_str = datetime.now(tz_kr).strftime("%Y-%m-%d")
    cfg = sys_config if isinstance(sys_config, dict) else {}
    m = meta if isinstance(meta, dict) else {}

    df_closed_today, df_entry_today, df_roll, df_open, db_err = _load_trade_frames(
        db_path, today_str, rolling_days
    )

    _sig = df_roll["sig_type"].astype(str) if not df_roll.empty and "sig_type" in df_roll.columns else pd.Series(dtype=str)
    df_roll_real = (
        df_roll.loc[~_sig.str.contains("INCUBATOR", na=False)].copy()
        if not df_roll.empty
        else df_roll
    )

    macro = build_macro_treasury_block(
        meta=m,
        sys_config=cfg,
        df_closed_real=df_roll_real,
        treasury_config_key="CENTRAL_TREASURY_KR",
        ledger_zero_invest_fallback=400000.0,
    )

    lifecycle = build_lifecycle_report_block(
        meta=m,
        sys_config=cfg,
        now=datetime.now(tz_kr),
    )

    ra = m.get("META_REGIME_ACTION")
    ra_d = ra if isinstance(ra, dict) else {}
    notes = ra_d.get("notes")
    notes_s = str(notes).strip() if notes else ""

    block_src = ra_d.get("block_trade_sources")
    if isinstance(block_src, list):
        block_tuple = tuple(str(x) for x in block_src)
    else:
        block_tuple = ()

    reg = cfg.get("FLOW_TAG_TOXIC_REGISTRY")
    toxic_count = len(reg) if isinstance(reg, dict) else 0
    penalty_map = cfg.get("FLOW_TAG_PENALTY_MULT")
    if not isinstance(penalty_map, dict):
        penalty_map = {}
    top_penalty_tag: Optional[str] = None
    top_penalty_mult: Optional[float] = None
    if penalty_map:
        try:
            top_penalty_tag = min(
                penalty_map.keys(),
                key=lambda k: float(penalty_map.get(k, 1.0)),
            )
            top_penalty_mult = float(penalty_map.get(top_penalty_tag, 1.0))
        except (TypeError, ValueError):
            pass

    toxic_entry_hits = _count_toxic_tag_entry_hits(df_entry_today, penalty_map)
    toxic_exit_echo_hits = _count_toxic_tag_hits(df_closed_today, penalty_map)

    regime_mm_entry = 0
    regime_mm_closed = 0
    cat_clutch_active = False
    cat_clutch_mult = 1.0
    try:
        from catastrophic_day_guard import evaluate_catastrophic_day_clutch
        from evolution.regime_logic_crossmatrix import count_regime_mismatch_trades

        _rk_audit = str(m.get("META_REGIME_KEY") or cfg.get("CURRENT_REGIME_KEY") or "UNKNOWN")
        _ac = sqlite3.connect(db_path, timeout=60)
        try:
            regime_mm_entry = count_regime_mismatch_trades(
                df_entry_today,
                _rk_audit,
                sys_config=cfg,
                meta_state=m,
                conn=_ac,
            )
            regime_mm_closed = count_regime_mismatch_trades(
                df_closed_today,
                _rk_audit,
                sys_config=cfg,
                meta_state=m,
                conn=_ac,
            )
            for _mkt in ("KR", "US"):
                _cc = evaluate_catastrophic_day_clutch(
                    _ac, _mkt, today_str, sys_config=cfg
                )
                if _cc.get("active"):
                    cat_clutch_active = True
                    cat_clutch_mult = min(
                        cat_clutch_mult, float(_cc.get("kelly_mult", 1.0) or 1.0)
                    )
        finally:
            _ac.close()
    except Exception:
        pass

    n_closed = len(df_closed_today)
    n_entry = len(df_entry_today)
    n_open = len(df_open)
    wr_today: Optional[float] = None
    if n_closed > 0 and "final_ret" in df_closed_today.columns:
        fr = pd.to_numeric(df_closed_today["final_ret"], errors="coerce")
        wr_today = float((fr > 0).sum() / len(fr) * 100.0)

    od_count = 0
    od_eligible = 0
    od_logged = 0
    od_loss_target = 0
    od_hurdle_val = 20.0
    od_ve_max: Optional[float] = None
    od_sn_closed = 0
    od_all_loss_sl = False
    if n_closed > 0:
        try:
            from overdrive_telemetry import (
                resolve_od_hurdle,
                summarize_overdrive_closed_day,
            )

            od_hurdle_val = resolve_od_hurdle(cfg)
            _od_sum = summarize_overdrive_closed_day(
                df_closed_today,
                sys_config=cfg,
                od_hurdle=od_hurdle_val,
            )
            od_eligible = int(_od_sum.get("eligible_count") or 0)
            od_logged = int(_od_sum.get("logged_count") or 0)
            od_loss_target = int(_od_sum.get("loss_as_target_count") or 0)
            od_count = od_logged
            _vm = _od_sum.get("v_energy_max")
            od_ve_max = float(_vm) if _vm is not None else None
            od_sn_closed = int(_od_sum.get("supernova_closed_count") or 0)
            od_all_loss_sl = bool(_od_sum.get("all_loss_sl_day"))
        except Exception:
            if "exit_reason" in df_closed_today.columns:
                od_count = int(
                    df_closed_today["exit_reason"]
                    .astype(str)
                    .str.contains("오버드라이브", na=False)
                    .sum()
                )
                od_logged = od_count

    rnd_n = 0
    if not df_entry_today.empty and "sig_type" in df_entry_today.columns:
        rnd_n = int(
            df_entry_today["sig_type"]
            .astype(str)
            .str.contains("INCUBATOR", na=False)
            .sum()
        )

    csv_status = "Missing"
    if csv_path and os.path.exists(csv_path):
        try:
            csv_status = f"OK (rows={len(pd.read_csv(csv_path))})"
        except Exception as e:
            csv_status = f"Read error: {e}"

    try:
        if m and not isinstance(sys_config, dict):
            cfg = {}
        if m:
            from meta_state_store import ensure_config_regime_aligned

            ensure_config_regime_aligned(m, force=True)
            try:
                from config_manager import load_system_config

                cfg = load_system_config() or cfg
            except Exception:
                pass
        try:
            from regime_self_heal import tick_regime_mismatch

            tick_regime_mismatch(m, cfg)
        except Exception:
            pass
    except Exception:
        pass

    cfg_regime = _resolve_overseer_config_regime(m, cfg)
    eff_k = float(macro.effective_kelly_risk)
    eff_pre = float(
        getattr(macro, "effective_kelly_pre_overlay", eff_k) or eff_k
    )
    k_day_m = float(getattr(macro, "kelly_day_clutch_mult", 1.0) or 1.0)
    k_nav_m = float(getattr(macro, "kelly_nav_dd_mult", 1.0) or 1.0)
    k_elast_m = float(getattr(macro, "kelly_elasticity_mult", 1.0) or 1.0)
    nav_dd_m = getattr(macro, "nav_drawdown_pct", None)

    _mt_extras: Dict[str, Any] = {}
    try:
        from meta_treasury_entry_guard import build_meta_treasury_dossier_extras

        _mt_extras = build_meta_treasury_dossier_extras(
            m,
            df_entry_today=df_entry_today,
            sys_config=cfg,
        )
    except Exception:
        _mt_extras = {}

    return OverseerAuditDossier(
        as_of_kst=today_str,
        market_scope="KR+US",
        macro=macro,
        lifecycle=lifecycle,
        meta_regime_key=str(m.get("META_REGIME_KEY") or cfg_regime),
        meta_regime_confidence=float(m.get("META_REGIME_CONFIDENCE") or 0.0),
        meta_global_kelly_mult=float(m.get("META_GLOBAL_KELLY_MULT") or 1.0),
        meta_treasury_mode=str(m.get("META_TREASURY_MODE") or "NORMAL"),
        meta_governor_last_run_at=(
            str(m.get("META_GOVERNOR_LAST_RUN_AT"))
            if m.get("META_GOVERNOR_LAST_RUN_AT")
            else None
        ),
        meta_governor_last_run_status=str(
            m.get("META_GOVERNOR_LAST_RUN_STATUS") or "UNKNOWN"
        ),
        meta_regime_action_notes=notes_s,
        vix_summary=_summarize_vix(m),
        block_trade_sources=block_tuple,
        trades_closed_today=n_closed,
        trades_entry_today=n_entry,
        trades_open=n_open,
        win_rate_today_pct=wr_today,
        overdrive_count_today=od_count,
        overdrive_eligible_today=od_eligible,
        overdrive_logged_today=od_logged,
        overdrive_loss_target_today=od_loss_target,
        overdrive_hurdle=od_hurdle_val,
        overdrive_v_energy_max_today=od_ve_max,
        overdrive_supernova_closed_today=od_sn_closed,
        overdrive_all_loss_sl_day=od_all_loss_sl,
        rnd_entry_today=rnd_n,
        flow_tag_toxic_count=toxic_count,
        flow_tag_top_penalty_tag=top_penalty_tag,
        flow_tag_penalty_mult=top_penalty_mult,
        toxic_tag_hits_today=toxic_entry_hits,
        toxic_tag_entry_hits_today=toxic_entry_hits,
        toxic_tag_exit_echo_hits_today=toxic_exit_echo_hits,
        regime_mismatch_entry_hits_today=regime_mm_entry,
        regime_mismatch_closed_hits_today=regime_mm_closed,
        catastrophic_clutch_active=cat_clutch_active,
        catastrophic_clutch_mult=cat_clutch_mult,
        csv_status=csv_status,
        config_regime_key=cfg_regime,
        effective_kelly_risk=eff_k,
        effective_kelly_pre_overlay=eff_pre,
        kelly_day_clutch_mult=k_day_m,
        kelly_nav_dd_mult=k_nav_m,
        kelly_elasticity_mult=k_elast_m,
        nav_drawdown_pct=(
            float(nav_dd_m) if nav_dd_m is not None else None
        ),
        kill_switch_active=bool(_mt_extras.get("kill_switch_active")),
        treasury_zeroed_groups=int(_mt_extras.get("treasury_zeroed_groups") or 0),
        treasury_actionable_groups=int(
            _mt_extras.get("treasury_actionable_groups") or 0
        ),
        treasury_zeroed_group_names=tuple(
            _mt_extras.get("treasury_zeroed_group_names") or ()
        ),
        governor_stale_hours=_mt_extras.get("governor_stale_hours"),
        governor_is_stale=bool(_mt_extras.get("governor_is_stale")),
        zero_group_entry_hits_today=int(
            _mt_extras.get("zero_group_entry_hits_today") or 0
        ),
        predicted_sector_kr=str(
            cfg.get("PREDICTED_NEXT_SECTOR_KR", "—") or "—"
        ),
        predicted_sector_us=str(
            cfg.get("PREDICTED_NEXT_SECTOR_US", "—") or "—"
        ),
        db_error=db_err,
    )


def detect_audit_anomalies(
    dossier: OverseerAuditDossier,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> List[AuditAnomaly]:
    th = _audit_thresholds(sys_config)
    out: List[AuditAnomaly] = []

    rk_meta = dossier.meta_regime_key
    rk_cfg = dossier.config_regime_key
    eff_k = dossier.effective_kelly_risk
    g_mult = dossier.meta_global_kelly_mult
    n_closed = dossier.trades_closed_today
    n_entry = dossier.trades_entry_today

    def _add(code: str, severity: str, headline: str, evidence: str) -> None:
        out.append(
            AuditAnomaly(
                code=code,
                severity=severity,
                headline=headline,
                evidence=evidence,
            )
        )

    if dossier.db_error:
        _add(
            "DB_READ_FAIL",
            "CRITICAL",
            "장부(DB) 읽기 실패 — 감사 팩트 신뢰 불가",
            html.escape(dossier.db_error, quote=False),
        )

    # SIGNAL_MISMATCH: bullish regime + 저 Kelly + zero activity (+ 실제 클램프 근거)
    if _is_bullish_regime(rk_meta) or _is_bullish_regime(rk_cfg):
        if (
            eff_k < th["kelly_crit"]
            and n_closed == 0
            and n_entry == 0
            and g_mult < th["meta_mult_clamp"]
        ):
            _add(
                "SIGNAL_MISMATCH",
                "CRITICAL",
                "시그널 불일치: 강세 국면인데 Kelly 극저·당일 매매 0건",
                (
                    f"META_REGIME=<b>{html.escape(rk_meta, quote=False)}</b> "
                    f"(config=<b>{html.escape(rk_cfg, quote=False)}</b>) · "
                    f"유효 Kelly=<b>{eff_k * 100:.2f}%</b> · "
                    f"META_GLOBAL_KELLY_MULT=<b>{g_mult:.3f}</b> · "
                    f"청산/진입=<b>0</b>. <b>시그널 단절·과도 클램프</b>를 의심."
                ),
            )
        elif (
            eff_k < th["kelly_low"]
            and n_closed == 0
            and n_entry == 0
            and g_mult < th["meta_mult_clamp"]
        ):
            _add(
                "SIGNAL_MISMATCH",
                "WARN",
                "강세 국면 대비 Kelly 낮음 + 당일 매매 0건",
                (
                    f"유효 Kelly=<b>{eff_k * 100:.2f}%</b> · META_GLOBAL_KELLY_MULT="
                    f"<b>{g_mult:.3f}</b> · 청산/진입 <b>0/0</b>건."
                ),
            )

    # DEFENSE_LEAK
    if dossier.meta_treasury_mode.upper() == "DEFENSE" and n_entry > 0:
        _add(
            "DEFENSE_LEAK",
            "CRITICAL",
            "방어 모드인데 당일 신규 진입 발생",
            (
                f"META_TREASURY_MODE=<b>DEFENSE</b> · 진입 <b>{n_entry}</b>건. "
                "자본 통제와 실행 경로 불일치."
            ),
        )

    # VIX_CLAMP — explain clamp, do not praise
    if "≥p90" in dossier.vix_summary or "p90" in dossier.vix_summary:
        if g_mult < th["meta_mult_clamp"]:
            _add(
                "VIX_CLAMP",
                "WARN",
                "VIX 고분위 — MetaGovernor Kelly 글로벌 승수 클램프",
                (
                    f"{html.escape(dossier.vix_summary, quote=False)} → "
                    f"META_GLOBAL_KELLY_MULT=<b>{g_mult:.3f}</b> · "
                    f"유효 Kelly=<b>{eff_k * 100:.2f}%</b>. "
                    "<i>칭찬 대상 아님 — 인과 기록.</i>"
                ),
            )

    # TOXIC_TAG_LEAK — 진입 sig_type 태그만(청산 exit flow_tags 제외)
    if dossier.flow_tag_toxic_count > 0 and dossier.toxic_tag_entry_hits_today > 0:
        tag_esc = html.escape(dossier.flow_tag_top_penalty_tag or "?", quote=False)
        mult = dossier.flow_tag_penalty_mult
        mult_s = f"{mult:.2f}" if mult is not None else "?"
        exit_echo = dossier.toxic_tag_exit_echo_hits_today
        exit_note = (
            f" · 청산 exit echo <b>{exit_echo}</b>건(사후 태그, 누출 아님)"
            if exit_echo > 0
            else ""
        )
        _add(
            "TOXIC_TAG_LEAK",
            "CRITICAL",
            "독성 태그 페널티인데 진입 시 동일 태그 계열 거래 발생",
            (
                f"FLOW_TAG_TOXIC_REGISTRY=<b>{dossier.flow_tag_toxic_count}</b>건 · "
                f"당일 <b>진입</b> 페널티 태그 매칭 <b>{dossier.toxic_tag_entry_hits_today}</b>건 · "
                f"예: <b>{tag_esc}</b> mult=<b>{mult_s}</b>{exit_note}."
            ),
        )

    # LIVE vs COOLED split
    lc = dossier.lifecycle
    if lc is not None and lc.n_cooled > 0 and n_entry > 0:
        _add(
            "LIVE_COOLED_SPLIT",
            "WARN",
            "레지스트리 COOLED 전략 존재 + 당일 신규 진입",
            (
                f"LIVE <b>{lc.n_live}</b> · COOLED <b>{lc.n_cooled}</b> · "
                f"진입 <b>{n_entry}</b>건 — 편대 강등과 실행 동기화 점검."
            ),
        )

    # STANDBY_IDLE — Governor 미기동일 때만 (UNKNOWN 국면·무거래는 매매 중지 사유 아님)
    gov_st = str(dossier.meta_governor_last_run_status or "").upper()
    if (
        _is_standby_regime(rk_meta)
        and n_closed == 0
        and n_entry == 0
        and gov_st in ("NEVER", "")
        and not dossier.meta_governor_last_run_at
    ):
        note_esc = html.escape(dossier.meta_regime_action_notes or "—", quote=False)
        _add(
            "META_GOVERNOR_STALE",
            "WARN",
            "MetaGovernor 미실행/초기 상태 — 당일 무거래와 혼동 금지",
            (
                f"Governor status=<b>{html.escape(gov_st or 'NEVER', quote=False)}</b> · "
                f"META_REGIME=<b>{html.escape(rk_meta, quote=False)}</b> · "
                f"notes=<i>{note_esc}</i>. "
                "<i>factory_artifact_guard 또는 meta_governor.py 실행 여부 확인.</i>"
            ),
        )

    # Regime config vs meta divergence (동기화 후에도 남으면 WARN)
    if (
        _normalize_regime(rk_meta) != _normalize_regime(rk_cfg)
        and rk_meta not in ("", "UNKNOWN")
    ):
        _add(
            "REGIME_SSOT_SPLIT",
            "WARN",
            "MetaGovernor 국면과 system_config 국면 불일치",
            (
                f"META_REGIME_KEY=<b>{html.escape(rk_meta, quote=False)}</b> vs "
                f"config_regime=<b>{html.escape(rk_cfg, quote=False)}</b> "
                "(REGIME_ANALYSIS.regime_key / CURRENT_REGIME_KEY)."
            ),
        )

    # Ch.5 — MetaGovernor / Treasury 연동 감사
    try:
        from meta_treasury_entry_guard import detect_meta_treasury_audit_anomalies

        for _mt in detect_meta_treasury_audit_anomalies(
            kill_switch_active=dossier.kill_switch_active,
            treasury_mode=dossier.meta_treasury_mode,
            treasury_zeroed_groups=dossier.treasury_zeroed_groups,
            treasury_actionable_groups=dossier.treasury_actionable_groups,
            governor_is_stale=dossier.governor_is_stale,
            governor_hours_since_run=dossier.governor_stale_hours,
            trades_entry_today=n_entry,
            trades_closed_today=n_closed,
            win_rate_today_pct=dossier.win_rate_today_pct,
            catastrophic_clutch_active=dossier.catastrophic_clutch_active,
            zero_group_entry_hits=dossier.zero_group_entry_hits_today,
            block_trade_sources=dossier.block_trade_sources,
            sys_config=sys_config,
        ):
            _add(
                _mt["code"],
                _mt["severity"],
                _mt["headline"],
                _mt["evidence"],
            )
    except Exception:
        pass

    # CATASTROPHIC_LOSS_DAY — 표본 충분한데 승률이 사실상 0%인 날
    if (
        dossier.win_rate_today_pct is not None
        and n_closed >= th["win_rate_min_n"]
        and dossier.win_rate_today_pct <= th["win_rate_catastrophic_pct"]
    ):
        mm_e = dossier.regime_mismatch_entry_hits_today
        mm_c = dossier.regime_mismatch_closed_hits_today
        clutch_s = (
            f" · 당일클러치=<b>ON×{dossier.catastrophic_clutch_mult:.2f}</b>"
            if dossier.catastrophic_clutch_active
            else " · 당일클러치=<b>OFF</b>(사후감지만)"
        )
        _add(
            "CATASTROPHIC_LOSS_DAY",
            "CRITICAL",
            "당일 승률 붕괴 — 청산 다수인데 사실상 전패",
            (
                f"청산 <b>{n_closed}</b>건 · 승률 <b>{dossier.win_rate_today_pct:.1f}%</b> "
                f"(임계 ≤<b>{th['win_rate_catastrophic_pct']:.0f}%</b>, 최소표본 "
                f"<b>{int(th['win_rate_min_n'])}</b>). 국면(META_REGIME_KEY="
                f"<b>{html.escape(rk_meta, quote=False)}</b>) · "
                f"국면미스매치 진입 <b>{mm_e}</b>/청산 <b>{mm_c}</b>{clutch_s}. "
                "전략 regime_tag 격리·당일 클러치 즉시 점검."
            ),
        )

    # KELLY 탄력성 — Ch.4
    try:
        from kelly_elasticity_overlay import detect_kelly_inelastic_anomaly

        _inel = detect_kelly_inelastic_anomaly(
            effective_pre=dossier.effective_kelly_pre_overlay,
            effective_post=dossier.effective_kelly_risk,
            overlay={
                "elasticity_mult": dossier.kelly_elasticity_mult,
                "active": dossier.kelly_elasticity_mult < 0.999
                or dossier.catastrophic_clutch_active,
                "nav_drawdown_pct": dossier.nav_drawdown_pct,
            },
            catastrophic_clutch_active=dossier.catastrophic_clutch_active,
            sys_config=sys_config,
        )
        if _inel:
            _add(
                _inel["code"],
                _inel["severity"],
                _inel["headline"],
                _inel["evidence"],
            )
    except Exception:
        pass

    if dossier.kelly_elasticity_mult < 0.999 and dossier.effective_kelly_pre_overlay > 0:
        _add(
            "KELLY_ELASTICITY_ACTIVE",
            "WARN",
            "Kelly 탄력성 오버레이 활성 — 유효 비중 축소 중",
            (
                f"사전 <b>{dossier.effective_kelly_pre_overlay * 100:.2f}%</b> "
                f"→ 유효 <b>{dossier.effective_kelly_risk * 100:.2f}%</b> "
                f"(×<b>{dossier.kelly_elasticity_mult:.3f}</b> · "
                f"당일×{dossier.kelly_day_clutch_mult:.3f} · "
                f"NAV×{dossier.kelly_nav_dd_mult:.3f}) · "
                f"NAV dd=<b>{dossier.nav_drawdown_pct or '—'}</b>%."
            ),
        )

    # REGIME_STRATEGY_MISMATCH — BEAR/BULL 불일치 전략이 실제 거래에 참여
    _mm_total = (
        dossier.regime_mismatch_entry_hits_today
        + dossier.regime_mismatch_closed_hits_today
    )
    if _mm_total > 0 and (
        _is_defensive_regime(rk_meta) or _is_bullish_regime(rk_meta)
    ):
        sev = "CRITICAL" if _mm_total >= 3 else "WARN"
        _add(
            "REGIME_STRATEGY_MISMATCH",
            sev,
            "META 국면과 regime_tag 전략 불일치 거래",
            (
                f"META=<b>{html.escape(rk_meta, quote=False)}</b> · "
                f"불일치 진입 <b>{dossier.regime_mismatch_entry_hits_today}</b> · "
                f"불일치 청산 <b>{dossier.regime_mismatch_closed_hits_today}</b>. "
                "<i>레지스트리/ledger 추론 격리·MAB overlay 재확인.</i>"
            ),
        )

    # Overdrive sanity (Ch.3 — 정밀 규칙, 7/8 전량손절 오탐 제거)
    try:
        from overdrive_telemetry import detect_overdrive_audit_anomalies

        _od_sum_audit = {
            "n_closed": n_closed,
            "eligible_count": dossier.overdrive_eligible_today,
            "logged_count": dossier.overdrive_logged_today,
            "telemetry_gap_count": max(
                0,
                dossier.overdrive_eligible_today - dossier.overdrive_logged_today,
            ),
            "loss_as_target_count": dossier.overdrive_loss_target_today,
            "od_hurdle": dossier.overdrive_hurdle,
            "v_energy_max": dossier.overdrive_v_energy_max_today,
            "supernova_closed_count": dossier.overdrive_supernova_closed_today,
            "all_loss_sl_day": dossier.overdrive_all_loss_sl_day,
        }
        for _oa in detect_overdrive_audit_anomalies(
            _od_sum_audit,
            win_rate_today_pct=dossier.win_rate_today_pct,
            sys_config=sys_config,
        ):
            sev = _oa["severity"]
            if sev == "INFO":
                continue
            _add(
                _oa["code"],
                sev,
                _oa["headline"],
                _oa["evidence"],
            )
        if (
            n_closed >= 3
            and dossier.overdrive_eligible_today == 0
            and (dossier.overdrive_all_loss_sl_day or (
                dossier.win_rate_today_pct is not None
                and dossier.win_rate_today_pct <= th["win_rate_catastrophic_pct"]
            ))
        ):
            _add(
                "OVERDRIVE_EXPECTED_IDLE",
                "INFO",
                "오버드라이브 미발동 정상(전량손절·eligible=0)",
                (
                    f"청산 <b>{n_closed}</b> · eligible <b>0</b> · "
                    f"hurdle <b>{dossier.overdrive_hurdle:g}</b> · "
                    f"v_max <b>{dossier.overdrive_v_energy_max_today or '—'}</b> — "
                    "익절 가속 경로 미진입으로 0건은 정상."
                ),
            )
    except Exception:
        if n_closed >= 3 and dossier.overdrive_count_today == 0:
            _add(
                "OVERDRIVE_SILENT",
                "WARN",
                "청산 다수인데 오버드라이브 0건",
                (
                    f"청산 <b>{n_closed}</b>건 · 오버드라이브 <b>0</b>. "
                    "휩소·로직 비활성·조건 미충족 점검."
                ),
            )

    # 파생 CSV — DB 손상이 아니면 WARN (자가 치유 대상)
    if "Missing" in dossier.csv_status:
        _add(
            "CSV_DERIVED_MISSING",
            "WARN",
            "마스터 CSV 파생 파일 없음 (DB SSOT는 유지)",
            (
                f"{html.escape(dossier.csv_status, quote=False)} · "
                "<i>매매 중지 사유 아님 — factory_artifact_guard 가 DB에서 재생성 시도.</i>"
            ),
        )
    elif dossier.csv_status.startswith("Read error"):
        _add(
            "CSV_READ_ERROR",
            "WARN",
            "마스터 CSV 읽기 오류",
            html.escape(dossier.csv_status, quote=False),
        )

    # sort CRITICAL first
    sev_order = {"CRITICAL": 0, "WARN": 1}
    out.sort(key=lambda a: (sev_order.get(a.severity, 9), a.code))
    return out


def _dossier_to_json_dict(dossier: OverseerAuditDossier) -> Dict[str, Any]:
    d = asdict(dossier)
    d["macro"] = asdict(dossier.macro)
    if dossier.lifecycle is not None:
        d["lifecycle"] = asdict(dossier.lifecycle)
    else:
        d["lifecycle"] = None
    return d


def format_overseer_audit_html(
    dossier: OverseerAuditDossier,
    anomalies: Sequence[AuditAnomaly],
) -> str:
    out = "👁️ <b>[AI 상시 감사관 일일 리포트]</b>\n"
    out += f"📅 KST <b>{html.escape(dossier.as_of_kst, quote=False)}</b> · {html.escape(dossier.market_scope, quote=False)}\n\n"

    out += "━━━ <b>[규칙 감사 · Anomaly]</b> ━━━\n"
    if not anomalies:
        out += " ◽ <i>규칙 엔진: CRITICAL/WARN 이상 없음 — 아래 SSOT만으로 교차검증하십시오.</i>\n"
    else:
        for a in anomalies:
            icon = "🚨" if a.severity == "CRITICAL" else "⚠️"
            out += (
                f" {icon} <b>[{html.escape(a.severity, quote=False)} · {html.escape(a.code, quote=False)}]</b> "
                f"{html.escape(a.headline, quote=False)}\n"
                f"    ↳ {a.evidence}\n"
            )
    out += "\n"

    out += "━━━ <b>[MetaGovernor · SSOT]</b> ━━━\n"
    out += (
        f" 국면 <b>{html.escape(dossier.meta_regime_key, quote=False)}</b> "
        f"(conf {dossier.meta_regime_confidence:.2f}) · "
        f"Treasury <b>{html.escape(dossier.meta_treasury_mode, quote=False)}</b>\n"
    )
    out += (
        f" Kelly: base×global "
        f"→ 사전 <b>{dossier.effective_kelly_pre_overlay * 100:.2f}%</b> "
    )
    if dossier.kelly_elasticity_mult < 0.999:
        out += (
            f"×탄력성 <b>{dossier.kelly_elasticity_mult:.3f}</b> "
        )
    out += (
        f"→ 유효 <b>{dossier.effective_kelly_risk * 100:.2f}%</b> "
        f"(META_GLOBAL_KELLY_MULT=<b>{dossier.meta_global_kelly_mult:.3f}</b>)\n"
    )
    out += f" {html.escape(dossier.vix_summary, quote=False)}\n"
    if dossier.meta_regime_action_notes:
        out += (
            f" notes: <i>{html.escape(dossier.meta_regime_action_notes, quote=False)}</i>\n"
        )
    gov_at = dossier.meta_governor_last_run_at or "—"
    stale_s = (
        f" · stale {dossier.governor_stale_hours:.1f}h"
        if dossier.governor_stale_hours is not None
        else ""
    )
    if dossier.governor_is_stale:
        stale_s += " ⚠️"
    ks_s = " · KILL_SWITCH=ON" if dossier.kill_switch_active else ""
    tz_s = (
        f" · Treasury zeroed <b>{dossier.treasury_zeroed_groups}</b>"
        f"/{dossier.treasury_actionable_groups}"
    )
    out += (
        f" Governor: <code>{html.escape(gov_at, quote=False)}</code> · "
        f"<b>{html.escape(dossier.meta_governor_last_run_status, quote=False)}</b>"
        f"{stale_s}{ks_s}{tz_s}\n\n"
    )

    out += "━━━ <b>[당일 장부 팩트]</b> ━━━\n"
    wr_s = (
        f"{dossier.win_rate_today_pct:.1f}%"
        if dossier.win_rate_today_pct is not None
        else "—"
    )
    out += (
        f" 청산 <b>{dossier.trades_closed_today}</b> · 진입 <b>{dossier.trades_entry_today}</b> · "
        f"OPEN <b>{dossier.trades_open}</b> · 승률(청산) <b>{wr_s}</b>\n"
    )
    out += (
        f" 독성태그 레지스트리 <b>{dossier.flow_tag_toxic_count}</b> · "
        f"당일 진입 페널티 매칭 <b>{dossier.toxic_tag_entry_hits_today}</b>"
    )
    if dossier.toxic_tag_exit_echo_hits_today > 0:
        out += (
            f" · 청산 exit echo <b>{dossier.toxic_tag_exit_echo_hits_today}</b>"
        )
    out += (
        f"\n 국면미스매치 진입 <b>{dossier.regime_mismatch_entry_hits_today}</b>"
        f" · 청산 <b>{dossier.regime_mismatch_closed_hits_today}</b>"
    )
    if dossier.catastrophic_clutch_active:
        out += (
            f" · 당일클러치 <b>ON×{dossier.catastrophic_clutch_mult:.2f}</b>"
        )
    out += (
        f"\n 오버드라이브 hurdle <b>{dossier.overdrive_hurdle:g}</b> · "
        f"eligible <b>{dossier.overdrive_eligible_today}</b> · "
        f"logged <b>{dossier.overdrive_logged_today}</b>"
    )
    if dossier.overdrive_loss_target_today > 0:
        out += f" · 대상손절 <b>{dossier.overdrive_loss_target_today}</b>"
    if dossier.overdrive_v_energy_max_today is not None:
        out += f" · v_max <b>{dossier.overdrive_v_energy_max_today:g}</b>"
    out += "\n"
    out += f" CSV: {html.escape(dossier.csv_status, quote=False)}\n\n"
    return out


def build_llm_narrative_prompt(
    dossier: OverseerAuditDossier,
    anomalies: Sequence[AuditAnomaly],
) -> str:
    try:
        from overseer_llm_narrative import build_llm_narrative_user_prompt

        return build_llm_narrative_user_prompt(
            dossier,
            anomalies,
            dossier_json=_dossier_to_json_dict(dossier),
        )
    except Exception:
        pass
    anom_json = [
        {
            "code": a.code,
            "severity": a.severity,
            "headline": a.headline,
            "evidence": a.evidence,
        }
        for a in anomalies
    ]
    return (
        "[AUDIT_DOSSIER_JSON]\n"
        f"{json.dumps(_dossier_to_json_dict(dossier), ensure_ascii=False, indent=2)}\n\n"
        "[ANOMALIES_JSON]\n"
        f"{json.dumps(anom_json, ensure_ascii=False, indent=2)}\n\n"
        "Write the LLM interpretation section only (Korean, max 10 lines). "
        "Do not repeat the report header."
    )

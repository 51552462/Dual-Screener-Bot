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

from report_state_binder import (
    LifecycleReportBlock,
    MacroTreasuryReportBlock,
    build_lifecycle_report_block,
    build_macro_treasury_block,
)

# ---------------------------------------------------------------------------
# LLM system prompt (전문 — ai_overseer 가 그대로 사용)
# ---------------------------------------------------------------------------
OVERSEER_LLM_SYSTEM_PROMPT = """You are a Ruthless QA engineer for a quant trading factory. You are NOT a cheerleader.

STRICT RULES:
1. Use ONLY facts in AUDIT_DOSSIER_JSON and ANOMALIES_JSON. Do not invent numbers, regimes, or trades.
2. If ANOMALIES_JSON is non-empty: your first sentence MUST acknowledge the highest-severity anomaly. NEVER contradict an anomaly headline.
3. NEVER reframe anomalies as "excellent defense", "탁월한 방어", "훌륭한 방어", "완벽히 동기화", or praise Kelly clamping without citing the exact MetaGovernor field that caused it.
4. Forbidden unless anomaly code is explicitly informational: "매우 훌륭한", "탁월한", "완벽", "잘 방어", "훌륭한 방어 상태".
5. Every causal claim MUST cite a dossier field (e.g. META_GLOBAL_KELLY_MULT, META_REGIME_KEY, meta_regime_action_notes, vix_summary).
6. Output in Korean, max 10 short lines, Telegram-safe HTML (<b>, <i> only). No markdown headers.
7. Do NOT repeat the full anomaly list; add 1–2 actionable checks for tomorrow only.
8. If zero anomalies: state neutrally what to monitor — still no praise fluff."""

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
    rnd_entry_today: int
    flow_tag_toxic_count: int
    flow_tag_top_penalty_tag: Optional[str]
    flow_tag_penalty_mult: Optional[float]
    toxic_tag_hits_today: int
    csv_status: str
    config_regime_key: str
    effective_kelly_risk: float
    predicted_sector_kr: str
    predicted_sector_us: str
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
        "kelly_low": _f("OVERSEER_KELLY_LOW_THRESHOLD", 0.05),
        "kelly_crit": _f("OVERSEER_KELLY_CRIT_THRESHOLD", 0.02),
        "meta_mult_clamp": _f("OVERSEER_META_MULT_CLAMP", 0.92),
        "min_trades_bull_expect": _f("OVERSEER_BULL_MIN_TRADES", 0),
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


def _load_trade_frames(
    db_path: str,
    today_str: str,
    rolling_days: int = 90,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Optional[str]]:
    """(closed_today, entry_today, rolling_closed, error)."""
    try:
        conn = sqlite3.connect(db_path, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        df_today = pd.read_sql(
            "SELECT * FROM forward_trades WHERE exit_date = ?",
            conn,
            params=(today_str,),
        )
        df_entry = pd.read_sql(
            "SELECT * FROM forward_trades WHERE entry_date = ?",
            conn,
            params=(today_str,),
        )
        cutoff = (
            datetime.strptime(today_str, "%Y-%m-%d") - timedelta(days=rolling_days)
        ).strftime("%Y-%m-%d")
        df_roll = pd.read_sql(
            "SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND exit_date >= ?",
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

    toxic_hits = _count_toxic_tag_hits(
        pd.concat([df_closed_today, df_entry_today], ignore_index=True),
        penalty_map,
    )

    n_closed = len(df_closed_today)
    n_entry = len(df_entry_today)
    n_open = len(df_open)
    wr_today: Optional[float] = None
    if n_closed > 0 and "final_ret" in df_closed_today.columns:
        fr = pd.to_numeric(df_closed_today["final_ret"], errors="coerce")
        wr_today = float((fr > 0).sum() / len(fr) * 100.0)

    od_count = 0
    if n_closed > 0 and "exit_reason" in df_closed_today.columns:
        od_count = int(
            df_closed_today["exit_reason"]
            .astype(str)
            .str.contains("오버드라이브", na=False)
            .sum()
        )

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

    eff_k = macro.effective_kelly_risk
    cfg_regime = str(cfg.get("CURRENT_REGIME_KEY", "UNKNOWN") or "UNKNOWN")

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
        rnd_entry_today=rnd_n,
        flow_tag_toxic_count=toxic_count,
        flow_tag_top_penalty_tag=top_penalty_tag,
        flow_tag_penalty_mult=top_penalty_mult,
        toxic_tag_hits_today=toxic_hits,
        csv_status=csv_status,
        config_regime_key=cfg_regime,
        effective_kelly_risk=eff_k,
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

    # SIGNAL_MISMATCH: bullish regime + ultra-low kelly + zero activity
    if _is_bullish_regime(rk_meta) or _is_bullish_regime(rk_cfg):
        if eff_k <= th["kelly_crit"] and n_closed == 0 and n_entry == 0:
            _add(
                "SIGNAL_MISMATCH",
                "CRITICAL",
                "시그널 불일치: 강세 국면인데 Kelly 극저·당일 매매 0건",
                (
                    f"META_REGIME=<b>{html.escape(rk_meta, quote=False)}</b> "
                    f"(config=<b>{html.escape(rk_cfg, quote=False)}</b>) · "
                    f"유효 Kelly=<b>{eff_k * 100:.2f}%</b> · "
                    f"청산/진입=<b>0</b>. 방어가 아니라 <b>시그널 단절·과도 클램프</b>를 의심."
                ),
            )
        elif eff_k <= th["kelly_low"] and n_closed == 0:
            _add(
                "SIGNAL_MISMATCH",
                "WARN",
                "강세 국면 대비 Kelly 낮음 + 당일 청산 0건",
                (
                    f"유효 Kelly=<b>{eff_k * 100:.2f}%</b> · META_GLOBAL_KELLY_MULT="
                    f"<b>{g_mult:.3f}</b> · 청산 <b>0</b>건."
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

    # TOXIC_TAG_LEAK
    if dossier.flow_tag_toxic_count > 0 and dossier.toxic_tag_hits_today > 0:
        tag_esc = html.escape(dossier.flow_tag_top_penalty_tag or "?", quote=False)
        mult = dossier.flow_tag_penalty_mult
        mult_s = f"{mult:.2f}" if mult is not None else "?"
        _add(
            "TOXIC_TAG_LEAK",
            "CRITICAL",
            "독성 태그 페널티인데 동일 태그 계열 거래 발생",
            (
                f"FLOW_TAG_TOXIC_REGISTRY=<b>{dossier.flow_tag_toxic_count}</b>건 · "
                f"당일 페널티 태그 매칭 거래 <b>{dossier.toxic_tag_hits_today}</b>건 · "
                f"예: <b>{tag_esc}</b> mult=<b>{mult_s}</b>."
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

    # Regime config vs meta divergence
    if (
        _normalize_regime(rk_meta) != _normalize_regime(rk_cfg)
        and rk_meta not in ("", "UNKNOWN")
        and rk_cfg not in ("", "UNKNOWN")
    ):
        _add(
            "REGIME_SSOT_SPLIT",
            "WARN",
            "MetaGovernor 국면과 system_config 국면 불일치",
            (
                f"META_REGIME_KEY=<b>{html.escape(rk_meta, quote=False)}</b> vs "
                f"CURRENT_REGIME_KEY=<b>{html.escape(rk_cfg, quote=False)}</b>."
            ),
        )

    # Blocked sources but entries
    if dossier.block_trade_sources and n_entry > 0:
        src = html.escape(",".join(dossier.block_trade_sources[:5]), quote=False)
        _add(
            "BLOCK_SOURCE_LEAK",
            "CRITICAL",
            "차단 trade_source 설정인데 진입 발생",
            f"block_trade_sources=[{src}] · 진입 <b>{n_entry}</b>건.",
        )

    # Overdrive sanity
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
        f" Kelly: base×global → 유효 <b>{dossier.effective_kelly_risk * 100:.2f}%</b> "
        f"(META_GLOBAL_KELLY_MULT=<b>{dossier.meta_global_kelly_mult:.3f}</b>)\n"
    )
    out += f" {html.escape(dossier.vix_summary, quote=False)}\n"
    if dossier.meta_regime_action_notes:
        out += (
            f" notes: <i>{html.escape(dossier.meta_regime_action_notes, quote=False)}</i>\n"
        )
    gov_at = dossier.meta_governor_last_run_at or "—"
    out += (
        f" Governor: <code>{html.escape(gov_at, quote=False)}</code> · "
        f"<b>{html.escape(dossier.meta_governor_last_run_status, quote=False)}</b>\n\n"
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
        f"당일 페널티 태그 매칭 <b>{dossier.toxic_tag_hits_today}</b>\n"
    )
    out += f" CSV: {html.escape(dossier.csv_status, quote=False)}\n\n"
    return out


def build_llm_narrative_prompt(
    dossier: OverseerAuditDossier,
    anomalies: Sequence[AuditAnomaly],
) -> str:
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

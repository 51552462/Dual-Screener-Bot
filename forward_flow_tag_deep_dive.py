"""
flow_tags explode · groupby — 태그별 기여도·독성·FLOW_TAG_TOXIC_REGISTRY 시너지.
"""
from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

import logging

from forward_market_guard import enforce_market_frame
from reports.forward_report_scalar import col_series, scalar_float

logger = logging.getLogger(__name__)
from forward_score_bucket_deep_dive import _exit_date_span, _resolve_stock_name
from reports.report_staleness_gate import StalenessVerdict, evaluate_staleness
from reports.report_timekeeper import ReportTimekeeper

RegistrySaveFn = Callable[[Dict[str, Any]], Any]
RegistryLoadFn = Callable[[], Dict[str, Any]]

_INVALID_TAGS = frozenset({"", "nan", "none", "null", "nat"})


@dataclass(frozen=True)
class FlowTagBlock:
    tag: str
    n: int
    win_rate_pct: float
    profit_factor: float
    cum_ret_pct: float
    carry_stock_html: str
    bleed_stock_html: str
    is_toxic: bool
    toxic_reason: str


@dataclass(frozen=True)
class FlowTagReportSnapshot:
    blocks: Tuple[FlowTagBlock, ...]
    toxic: Optional[FlowTagBlock]
    synergy_action_html: str
    exit_date_min: Optional[str]
    exit_date_max: Optional[str]
    registry_persisted: bool
    registry_key: Optional[str]
    session_anchor: str = ""
    db_watermark_exit: Optional[str] = None
    staleness_grade: str = "GREEN"
    data_lag_days: int = 0
    skipped_red: bool = False


def _is_valid_tag(tag: object) -> bool:
    t = str(tag or "").strip()
    return t.lower() not in _INVALID_TAGS


def _sanitize_flow_tags_series(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .replace(
            {
                "nan": "",
                "NaN": "",
                "None": "",
                "none": "",
                "null": "",
                "NULL": "",
                "nat": "",
                "NaT": "",
            }
        )
    )


def _toxic_thresholds(sys_config: Optional[Dict[str, Any]]) -> Dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}

    def _f(key: str, default: float) -> float:
        try:
            return float(cfg.get(key, default))
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        try:
            return int(cfg.get(key, default))
        except (TypeError, ValueError):
            return default

    return {
        "min_n": float(_i("FLOW_TAG_MIN_N", 3)),
        "toxic_wr_pct": _f("FLOW_TAG_TOXIC_WR_PCT", 30.0),
        "toxic_pf": _f("FLOW_TAG_TOXIC_PF", 0.85),
        "toxic_cum_ret": _f("FLOW_TAG_TOXIC_CUM_RET", -15.0),
        "penalty_mult": _f("FLOW_TAG_TOXIC_DEFAULT_MULT", 0.0),
        "top_k": float(_i("FLOW_TAG_REPORT_TOP_K", 5)),
    }


def _stock_chip(row: pd.Series, ret_col: str = "_fr") -> str:
    nm = _resolve_stock_name(row)
    if nm == "—":
        for k in ("code", "ticker"):
            if k not in row.index:
                continue
            v = row.get(k)
            if v is None or (isinstance(v, float) and np.isnan(v)):
                continue
            t = str(v).strip()
            if t and t.lower() not in ("nan", "none"):
                nm = t
                break
        if nm == "—":
            nm = "종목미상"
    nm_esc = html.escape(nm, quote=False)
    r = scalar_float(row[ret_col])
    if r >= 0:
        return f"{nm_esc}(+{r:.0f}%)"
    return f"{nm_esc}({r:.0f}%)"


def _persist_flow_tag_toxic_registry(
    toxic: FlowTagBlock,
    *,
    market: str,
    today_str: str,
    sys_config: Optional[Dict[str, Any]],
    load_config_fn: Optional[RegistryLoadFn],
    save_config_fn: RegistrySaveFn,
    penalty_mult: float,
) -> Tuple[bool, str]:
    """FLOW_TAG_TOXIC_REGISTRY + FLOW_TAG_PENALTY_MULT 갱신 후 저장."""
    if load_config_fn is not None:
        cfg = dict(load_config_fn())
    elif isinstance(sys_config, dict):
        cfg = dict(sys_config)
    else:
        cfg = {}

    reg = cfg.get("FLOW_TAG_TOXIC_REGISTRY")
    if not isinstance(reg, dict):
        reg = {}
    else:
        reg = dict(reg)

    tag_key = str(toxic.tag).strip()
    reg_key = f"flow_tag:{market}:{tag_key}"
    reg[reg_key] = {
        "tag": tag_key,
        "market": str(market),
        "registered_at": today_str,
        "n": int(toxic.n),
        "win_rate_pct": round(scalar_float(toxic.win_rate_pct), 2),
        "profit_factor": round(scalar_float(toxic.profit_factor), 4),
        "cum_ret_pct": round(scalar_float(toxic.cum_ret_pct), 2),
        "capital_mult": scalar_float(penalty_mult),
        "kelly_mult": scalar_float(penalty_mult),
        "toxic_reason": toxic.toxic_reason,
        "carry_example": toxic.carry_stock_html,
        "bleed_example": toxic.bleed_stock_html,
    }

    if len(reg) > 80:
        keys = list(reg.keys())
        for old_k in keys[: len(keys) - 80]:
            reg.pop(old_k, None)

    mult_map = cfg.get("FLOW_TAG_PENALTY_MULT")
    if not isinstance(mult_map, dict):
        mult_map = {}
    else:
        mult_map = dict(mult_map)
    mult_map[tag_key] = float(penalty_mult)

    cfg["FLOW_TAG_TOXIC_REGISTRY"] = reg
    cfg["FLOW_TAG_PENALTY_MULT"] = mult_map
    cfg["FLOW_TAG_TOXIC_LAST_UPDATED"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        save_config_fn(cfg)
        return True, reg_key
    except Exception as ex:
        logger.error(
            "FLOW_TAG_TOXIC_REGISTRY persist failed market=%s tag=%s: %s",
            market,
            toxic.tag,
            ex,
            exc_info=True,
        )
        return False, reg_key


def _is_toxic_candidate(
    row: Dict[str, Any],
    *,
    min_n: float,
    toxic_wr: float,
    toxic_pf: float,
    toxic_cum: float,
) -> Tuple[bool, str]:
    n = int(row["n"])
    if n < min_n:
        return False, ""
    wr = scalar_float(row["win_rate_pct"])
    pf = scalar_float(row["profit_factor"])
    cum = scalar_float(row["cum_ret_pct"])
    reasons: List[str] = []
    if wr < toxic_wr:
        reasons.append(f"WR<{toxic_wr:.0f}%")
    if pf < toxic_pf:
        reasons.append(f"PF<{toxic_pf:.2f}")
    if cum < toxic_cum:
        reasons.append(f"누적<{toxic_cum:.0f}%p")
    if not reasons:
        return False, ""
    return True, " & ".join(reasons)


def _assemble_tag_synergy_html(
    toxic: Optional[FlowTagBlock],
    *,
    registry_persisted: bool,
    registry_key: Optional[str],
    penalty_mult: float,
) -> str:
    if toxic is None:
        return (
            "<i>이번 롤링 윈도우에서 독성 기준(표본·WR·PF·누적손익)을 충족하는 "
            "태그가 없습니다. 기존 FLOW_TAG_PENALTY_MULT 는 유지됩니다.</i>"
        )
    tag_esc = html.escape(toxic.tag, quote=False)
    mult_pct = f"{penalty_mult * 100:.0f}%" if penalty_mult > 0 else "차단(0%)"
    persist_note = (
        f" <b>FLOW_TAG_TOXIC_REGISTRY</b>에 기록 완료 (<code>{html.escape(registry_key or '', quote=False)}</code>)."
        if registry_persisted
        else " <i>레지스트리 저장 실패 — 설정 파일 권한을 확인하십시오.</i>"
    )
    return (
        f"현재 <b>{tag_esc}</b> 태그에서 표본 <b>{toxic.n}</b>건 · "
        f"승률 <b>{toxic.win_rate_pct:.1f}%</b> · PF <b>{toxic.profit_factor:.2f}</b> · "
        f"누적 <b>{toxic.cum_ret_pct:+.1f}%p</b> 출혈이 관측됩니다 "
        f"({html.escape(toxic.toxic_reason, quote=False)}). "
        f"시스템이 이를 독성 패턴으로 인지하고, 향후 동일 흐름 감지 시 "
        f"투입 비중을 <b>{mult_pct}</b> 수준으로 제한하도록 시너지를 연계했습니다.{persist_note}"
    )


def _empty_snapshot(
    timekeeper: ReportTimekeeper,
    staleness: Optional[StalenessVerdict],
    *,
    synergy: str,
    exit_date_min: Optional[str] = None,
    exit_date_max: Optional[str] = None,
    skipped_red: bool = False,
) -> FlowTagReportSnapshot:
    grade = staleness.grade if staleness else "GREEN"
    lag = staleness.lag_business_days if staleness else 0
    return FlowTagReportSnapshot(
        blocks=(),
        toxic=None,
        synergy_action_html=synergy,
        exit_date_min=exit_date_min,
        exit_date_max=exit_date_max,
        registry_persisted=False,
        registry_key=None,
        session_anchor=timekeeper.session_anchor,
        db_watermark_exit=timekeeper.db_watermark_exit,
        staleness_grade=grade,
        data_lag_days=lag,
        skipped_red=skipped_red,
    )


def build_flow_tag_snapshot(
    df: pd.DataFrame,
    *,
    timekeeper: ReportTimekeeper,
    staleness: Optional[StalenessVerdict] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    persist_toxic: bool = True,
    save_config_fn: Optional[RegistrySaveFn] = None,
    load_config_fn: Optional[RegistryLoadFn] = None,
) -> FlowTagReportSnapshot:
    """explode + groupby 1패스 — 태그별 WR/PF/누적·캐리/출혈·독성."""
    market = str(timekeeper.market).upper()
    today = timekeeper.calendar_today_kst
    if staleness is None:
        staleness = evaluate_staleness(timekeeper, live_row_count=0)

    th = _toxic_thresholds(sys_config)
    min_n = int(th["min_n"])
    top_k = int(th["top_k"])

    work = enforce_market_frame(df, market, context="flow_tag_snapshot")
    exit_min, exit_max = _exit_date_span(work) if work is not None and len(work) else (None, None)

    if staleness.grade == "RED" and not getattr(staleness, "allow_flow_tag", False):
        return _empty_snapshot(
            timekeeper,
            staleness,
            synergy=(
                "<i>데이터 정체 RED — flow 태그 집계를 생략합니다. "
                "track_daily_positions · 메인 DB WAL · factory daily 파이프라인을 확인하십시오.</i>"
            ),
            exit_date_min=exit_min,
            exit_date_max=exit_max,
            skipped_red=True,
        )

    empty_synergy = (
        "<i>표본 부족 (flow_tags 컬럼 없음 또는 0건)으로 flow 태그 딥다이브 생략</i>"
    )
    if work is None or work.empty or "flow_tags" not in work.columns:
        return _empty_snapshot(
            timekeeper,
            staleness,
            synergy=empty_synergy,
            exit_date_min=exit_min,
            exit_date_max=exit_max,
        )

    work = work.copy()
    work["_fr"] = pd.to_numeric(col_series(work, "final_ret"), errors="coerce")
    work = work.dropna(subset=["_fr"])
    if work.empty:
        return _empty_snapshot(
            timekeeper,
            staleness,
            synergy=empty_synergy,
            exit_date_min=exit_min,
            exit_date_max=exit_max,
        )

    work["_win"] = work["_fr"] > 0
    work["flow_tags"] = _sanitize_flow_tags_series(work["flow_tags"])
    tags_split = work["flow_tags"].str.split()
    long = work.assign(_tag=tags_split).explode("_tag")
    long["_tag"] = long["_tag"].astype(str).str.strip()
    long = long[long["_tag"].map(_is_valid_tag)]

    if long.empty:
        return _empty_snapshot(
            timekeeper,
            staleness,
            synergy=empty_synergy,
            exit_date_min=exit_min,
            exit_date_max=exit_max,
        )

    g = long.groupby("_tag", observed=True)
    agg = g.agg(
        n=("_fr", "size"),
        wins=("_win", "sum"),
        cum_ret=("_fr", "sum"),
    )
    if isinstance(agg.columns, pd.MultiIndex):
        agg.columns = [c[0] if isinstance(c, tuple) else c for c in agg.columns]
    gross_profit = long.loc[long["_fr"] > 0].groupby("_tag", observed=True)["_fr"].sum()
    gross_loss = (
        long.loc[long["_fr"] <= 0].groupby("_tag", observed=True)["_fr"].sum().abs() + 0.1
    )
    pf = gross_profit / gross_loss.reindex(gross_profit.index, fill_value=0.1)

    row_dicts: List[Dict[str, Any]] = []
    for tag, row in agg.iterrows():
        if not _is_valid_tag(tag):
            continue
        n = int(row["n"])
        if n < min_n:
            continue
        wins = int(row["wins"])
        wr = (wins / n) * 100.0 if n else 0.0
        cum = scalar_float(row["cum_ret"])
        tag_pf = scalar_float(pf.get(tag, 1.0), 1.0) if tag in pf.index else 1.0
        sub = long.loc[long["_tag"] == tag]
        carry_html = "—"
        bleed_html = "—"
        if not sub.empty:
            fr_sub = col_series(sub, "_fr")
            if not fr_sub.empty:
                carry_html = f"[캐리] {_stock_chip(sub.loc[fr_sub.idxmax()])}"
                bleed_html = f"[출혈] {_stock_chip(sub.loc[fr_sub.idxmin()])}"
        row_dicts.append(
            {
                "tag": str(tag),
                "n": n,
                "win_rate_pct": wr,
                "profit_factor": tag_pf,
                "cum_ret_pct": cum,
                "carry_stock_html": carry_html,
                "bleed_stock_html": bleed_html,
            }
        )

    if not row_dicts:
        return _empty_snapshot(
            timekeeper,
            staleness,
            synergy=(
                f"<i>표본 부족 (유효 태그 0개, 태그별 최소 <b>{min_n}</b>건 미달)으로 "
                "flow 태그 집계 딥다이브 생략</i>"
            ),
            exit_date_min=exit_min,
            exit_date_max=exit_max,
        )

    toxic_candidates: List[Tuple[float, Dict[str, Any], str]] = []
    for rd in row_dicts:
        ok, reason = _is_toxic_candidate(
            rd,
            min_n=th["min_n"],
            toxic_wr=th["toxic_wr_pct"],
            toxic_pf=th["toxic_pf"],
            toxic_cum=th["toxic_cum_ret"],
        )
        if ok:
            toxic_candidates.append(
                (scalar_float(rd["cum_ret_pct"]), scalar_float(rd["profit_factor"]), rd, reason)
            )

    toxic_rd: Optional[Dict[str, Any]] = None
    toxic_reason = ""
    if toxic_candidates:
        toxic_candidates.sort(key=lambda x: (x[0], x[1]))
        _, _, toxic_rd, toxic_reason = toxic_candidates[0]

    toxic_block: Optional[FlowTagBlock] = None
    if toxic_rd is not None:
        toxic_block = FlowTagBlock(
            tag=str(toxic_rd["tag"]),
            n=int(toxic_rd["n"]),
            win_rate_pct=scalar_float(toxic_rd["win_rate_pct"]),
            profit_factor=scalar_float(toxic_rd["profit_factor"]),
            cum_ret_pct=scalar_float(toxic_rd["cum_ret_pct"]),
            carry_stock_html=str(toxic_rd["carry_stock_html"]),
            bleed_stock_html=str(toxic_rd["bleed_stock_html"]),
            is_toxic=True,
            toxic_reason=toxic_reason,
        )

    registry_persisted = False
    registry_key: Optional[str] = None
    penalty_mult = scalar_float(th["penalty_mult"])
    if persist_toxic and toxic_block is not None and save_config_fn is not None:
        registry_persisted, registry_key = _persist_flow_tag_toxic_registry(
            toxic_block,
            market=market,
            today_str=today,
            sys_config=sys_config,
            load_config_fn=load_config_fn,
            save_config_fn=save_config_fn,
            penalty_mult=penalty_mult,
        )

    row_dicts.sort(key=lambda x: x["n"], reverse=True)
    blocks_list: List[FlowTagBlock] = []
    for rd in row_dicts[:top_k]:
        is_t = toxic_rd is not None and rd["tag"] == toxic_rd["tag"]
        blocks_list.append(
            FlowTagBlock(
                tag=str(rd["tag"]),
                n=int(rd["n"]),
                win_rate_pct=scalar_float(rd["win_rate_pct"]),
                profit_factor=scalar_float(rd["profit_factor"]),
                cum_ret_pct=scalar_float(rd["cum_ret_pct"]),
                carry_stock_html=str(rd["carry_stock_html"]),
                bleed_stock_html=str(rd["bleed_stock_html"]),
                is_toxic=is_t,
                toxic_reason=toxic_reason if is_t else "",
            )
        )

    synergy = _assemble_tag_synergy_html(
        toxic_block,
        registry_persisted=registry_persisted,
        registry_key=registry_key,
        penalty_mult=penalty_mult,
    )

    return FlowTagReportSnapshot(
        blocks=tuple(blocks_list),
        toxic=toxic_block,
        synergy_action_html=synergy,
        registry_persisted=registry_persisted,
        registry_key=registry_key,
        skipped_red=False,
        exit_date_min=exit_min,
        exit_date_max=exit_max,
        session_anchor=timekeeper.session_anchor,
        db_watermark_exit=timekeeper.db_watermark_exit,
        staleness_grade=staleness.grade,
        data_lag_days=staleness.lag_business_days,
    )


def format_flow_tag_report_html(
    snap: FlowTagReportSnapshot,
    *,
    timekeeper: ReportTimekeeper,
    staleness: Optional[StalenessVerdict] = None,
    rolling_days: Optional[int] = None,
) -> str:
    if staleness is None:
        staleness = evaluate_staleness(timekeeper, live_row_count=0)

    rd = int(rolling_days if rolling_days is not None else timekeeper.rolling_days)
    m_esc = html.escape(str(timekeeper.market), quote=False)
    anchor_esc = html.escape(timekeeper.session_anchor, quote=False)
    label_esc = html.escape(timekeeper.anchor_label, quote=False)
    wm = timekeeper.db_watermark_exit or snap.db_watermark_exit or "—"
    wm_esc = html.escape(str(wm), quote=False)
    grade = staleness.grade if staleness else snap.staleness_grade
    lag = staleness.lag_business_days if staleness else snap.data_lag_days

    out = "🏷️ <b>[세부 흐름 태그별 승률·기여도]</b>\n"
    out += (
        f"📎 <i>{m_esc}장 · 최근 <b>{rd}</b>일 청산 롤링 · "
        f"리포트일 KST <b>{html.escape(timekeeper.calendar_today_kst, quote=False)}</b></i>\n"
    )
    out += f"📌 세션앵커({label_esc}) <b>{anchor_esc}</b>\n"
    out += (
        f"📊 DB청산워터마크 <b>{wm_esc}</b> · Staleness <b>{grade}</b> "
        f"(lag <b>{lag}</b>영업일)\n"
    )
    if snap.exit_date_min and snap.exit_date_max:
        out += (
            f"📅 청산 표본: <b>{html.escape(snap.exit_date_min, quote=False)}</b>"
            f"~<b>{html.escape(snap.exit_date_max, quote=False)}</b>\n"
        )

    if snap.skipped_red:
        out += (
            "<i>데이터 정체 RED — 태그별 승률·기여도 집계를 생략했습니다. "
            "장부 갱신 후 재송출하십시오.</i>\n\n"
        )
        out += "🗣️ <b>[관제탑 · 태그 시너지]</b> "
        out += snap.synergy_action_html + "\n\n"
        return out

    if not snap.blocks:
        out += "<i>태그 집계 표본 없음 (flow_tags 미기록 또는 최소 표본 미달).</i>\n\n"
    else:
        for b in snap.blocks:
            if not _is_valid_tag(b.tag):
                continue
            tag_esc = html.escape(b.tag, quote=False)
            toxic_mark = " ☠️" if b.is_toxic else ""
            out += (
                f" ▪️ <b>{tag_esc}</b>{toxic_mark}: 승률 <b>{b.win_rate_pct:.1f}%</b> / "
                f"PF <b>{b.profit_factor:.2f}</b> / 누적 <b>{b.cum_ret_pct:+.1f}%p</b> "
                f"(n=<b>{b.n}</b>)\n"
                f"   ➔ {b.carry_stock_html} · {b.bleed_stock_html}\n"
            )
        out += "\n"

    out += "🗣️ <b>[관제탑 · 태그 시너지]</b> "
    out += snap.synergy_action_html + "\n\n"
    return out

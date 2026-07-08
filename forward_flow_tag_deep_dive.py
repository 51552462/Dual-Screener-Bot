"""
flow_tags explode · groupby — 태그별 기여도·독성·FLOW_TAG_TOXIC_REGISTRY 시너지.
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

import logging

from forward_market_guard import enforce_market_frame
from reports.forward_report_scalar import col_series, scalar_float

logger = logging.getLogger(__name__)
from forward_score_bucket_deep_dive import _exit_date_span
from reports.report_staleness_gate import StalenessVerdict, evaluate_staleness
from reports.report_timekeeper import ReportTimekeeper, resolve_data_candle_watermark

RegistrySaveFn = Callable[[Dict[str, Any]], Any]
RegistryLoadFn = Callable[[], Dict[str, Any]]

_INVALID_TAGS = frozenset({"", "nan", "none", "null", "nat"})

# [P3-4] FLOW_TAG_TOXIC_DEFAULT_MULT 미설정 시 공용 폴백 — weekly_flow_rollup.py 와
# 반드시 동일해야 한다(과거엔 0.0/0.85로 서로 달라 "완전 차단"과 "약한 페널티"가
# 무작위로 뒤섞이는 설정 드리프트 버그가 있었다). 베이지안 하한(compute_bayesian_toxic_penalty)
# 의 강한(구조적) 임계치와 동일 수준으로 통일.
FLOW_TAG_TOXIC_DEFAULT_MULT_FALLBACK = 0.5

# [P3-4] 켈리 가드 유효기간(일) — 이 기간이 지난 등록 항목은 자동으로 무시(자가 치유).
FLOW_TAG_TOXIC_GUARD_DECAY_DAYS_DEFAULT = 5


@dataclass(frozen=True)
class FlowTagBlock:
    tag: str
    n: int
    win_rate_pct: float
    profit_factor: float
    cum_ret_pct: float
    mean_ret_pct: float
    n_unique_tickers: int
    n_unknown_names: int
    profit_factor_display: str
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
        "penalty_mult": _f("FLOW_TAG_TOXIC_DEFAULT_MULT", FLOW_TAG_TOXIC_DEFAULT_MULT_FALLBACK),
        "top_k": float(_i("FLOW_TAG_REPORT_TOP_K", 5)),
    }


def _row_ticker_key(row: pd.Series) -> str:
    for k in ("code", "ticker", "symbol"):
        if k not in row.index:
            continue
        v = row.get(k)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        t = str(v).strip().upper()
        if t and t.lower() not in ("nan", "none"):
            return t
    return ""


def _display_stock_label(row: pd.Series) -> Tuple[str, bool]:
    """(표시명, 이름미상 여부) — code/ticker 폴백."""
    for k in ("name", "stock_name"):
        if k not in row.index:
            continue
        v = row.get(k)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        t = str(v).strip()
        if t and t.lower() not in ("nan", "none"):
            return t, False
    tk = _row_ticker_key(row)
    if tk:
        if tk.isdigit() and len(tk) <= 6:
            return tk.zfill(6), False
        return tk, False
    return "종목미상", True


def _stock_chip(row: pd.Series, ret_col: str = "_fr") -> str:
    nm, _ = _display_stock_label(row)
    nm_esc = html.escape(nm, quote=False)
    r = scalar_float(row[ret_col])
    if r >= 0:
        return f"{nm_esc}(+{r:.0f}%)"
    return f"{nm_esc}({r:.0f}%)"


def _format_pf_display(pf: float, n: int, n_losses: int) -> str:
    """극단 PF(분모 손실 거의 없음) — 기관 리포트 왜곡 방지."""
    p = scalar_float(pf, 1.0)
    if n < 5 or n_losses == 0:
        return "— (손실표본 없음)"
    if p > 99.0:
        return f">{min(99.0, p):.0f}"
    return f"{p:.2f}"


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


_FLOW_TAG_TOKEN_RE = re.compile(r"#[\w가-힣]+")


def extract_flow_tags_from_text(text: object) -> List[str]:
    """sig_type·flow_tags 등에서 `#태그` 토큰만 추출(중복 제거, 순서 유지)."""
    raw = str(text or "")
    seen: set[str] = set()
    out: List[str] = []
    for m in _FLOW_TAG_TOKEN_RE.finditer(raw):
        tok = m.group(0).strip()
        if tok and tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def _registry_penalty_mult_for_tag(
    tag: str,
    *,
    registry: Dict[str, Any],
    market: str,
    now: datetime,
    decay_days: int,
) -> Tuple[float, Optional[int]]:
    """단일 태그에 대한 레지스트리 페널티(만료 시 1.0)."""
    mkt = str(market or "").upper()
    tag_key = str(tag or "").strip()
    worst = 1.0
    worst_age: Optional[int] = None

    for key, entry in registry.items():
        if not isinstance(entry, dict):
            continue
        entry_tag = str(entry.get("tag", "")).strip()
        if entry_tag != tag_key and str(key) != f"flow_tag:{mkt}:{tag_key}":
            continue
        if str(entry.get("market", "")).upper() != mkt:
            continue
        registered_at = str(entry.get("registered_at", "")).strip()[:10]
        if not registered_at:
            continue
        try:
            reg_dt = datetime.strptime(registered_at, "%Y-%m-%d")
        except ValueError:
            continue
        age_days = (now - reg_dt).days
        if age_days < 0 or age_days > decay_days:
            continue
        try:
            mult = float(entry.get("kelly_mult", 1.0))
        except (TypeError, ValueError):
            continue
        mult = max(0.0, min(1.0, mult))
        if mult < worst:
            worst = mult
            worst_age = age_days
    return worst, worst_age


def resolve_flow_tag_entry_guard(
    sys_config: Optional[Dict[str, Any]],
    market: str,
    sig_type: object,
    *,
    now_dt: Optional[datetime] = None,
) -> Tuple[float, str]:
    """
    [Ch.1 TOXIC_TAG_LEAK] 진입 시점 sig_type 내 `#태그` ↔ 페널티/레지스트리 정밀 매칭.

    과거 P3-4 는 시장 단위 최악 배수를 모든 진입에 일괄 적용했고, AI 감사관은
    청산 시 부여되는 exit flow_tags(예: #건전한조정_매집우위)까지 '누출'로 집계해
    거짓 양성이 났다. 이 함수는 **진입 문자열에 실제로 붙은 태그**만 대상으로
    FLOW_TAG_PENALTY_MULT + 유효기간 내 FLOW_TAG_TOXIC_REGISTRY 를 병합한다.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    tags = extract_flow_tags_from_text(sig_type)
    if not tags:
        return 1.0, ""

    penalty_map = cfg.get("FLOW_TAG_PENALTY_MULT")
    if not isinstance(penalty_map, dict):
        penalty_map = {}
    reg = cfg.get("FLOW_TAG_TOXIC_REGISTRY")
    if not isinstance(reg, dict):
        reg = {}

    try:
        decay_days = int(
            cfg.get("FLOW_TAG_TOXIC_GUARD_DECAY_DAYS", FLOW_TAG_TOXIC_GUARD_DECAY_DAYS_DEFAULT)
        )
    except (TypeError, ValueError):
        decay_days = FLOW_TAG_TOXIC_GUARD_DECAY_DAYS_DEFAULT

    now = (now_dt or datetime.now()).replace(tzinfo=None)
    mkt = str(market or "").upper()

    worst_mult = 1.0
    worst_tag = ""
    worst_age: Optional[int] = None

    for tag in tags:
        tag_mult = 1.0
        if tag in penalty_map:
            try:
                tag_mult = min(tag_mult, float(penalty_map[tag]))
            except (TypeError, ValueError):
                pass
        reg_mult, reg_age = _registry_penalty_mult_for_tag(
            tag,
            registry=reg,
            market=mkt,
            now=now,
            decay_days=decay_days,
        )
        tag_mult = min(tag_mult, reg_mult)
        tag_mult = max(0.0, min(1.0, tag_mult))
        if tag_mult < worst_mult:
            worst_mult = tag_mult
            worst_tag = tag
            worst_age = reg_age

    if worst_tag and worst_mult < 1.0:
        age_s = str(worst_age) if worst_age is not None else "?"
        return worst_mult, f"{worst_tag}(D+{age_s})"
    return 1.0, ""


def resolve_flow_tag_toxic_kelly_mult(
    sys_config: Optional[Dict[str, Any]],
    market: str,
    *,
    now_dt: Optional[datetime] = None,
) -> Tuple[float, str]:
    """
    [P3-4] FLOW_TAG_TOXIC_REGISTRY → 실제 진입 켈리 사이징 연결 SSOT.

    과거에는 이 레지스트리가 리포트(주간 액션플랜·AI 감사관) 문구로만 소비되고
    실거래 경로에는 전혀 반영되지 않던 "죽은 스위치"였다. 이 함수가 유일한 소비
    지점으로, 해당 market 에 대해 유효기간(FLOW_TAG_TOXIC_GUARD_DECAY_DAYS, 기본
    5일) 이내에 등록된 항목 중 가장 강한(가장 낮은) kelly_mult 를 반환한다.

    - 데이터 없음/만료/시장 불일치 → (1.0, "") 완전 중립(무영향).
    - 반환된 mult 는 켈리 리스크 비중에 곱해질 뿐, 진입 자체를 차단하지 않는다
      (방어적 축소 — try_add_virtual_position 의 다른 차단 로직과 독립).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    reg = cfg.get("FLOW_TAG_TOXIC_REGISTRY")
    if not isinstance(reg, dict) or not reg:
        return 1.0, ""

    try:
        decay_days = int(
            cfg.get("FLOW_TAG_TOXIC_GUARD_DECAY_DAYS", FLOW_TAG_TOXIC_GUARD_DECAY_DAYS_DEFAULT)
        )
    except (TypeError, ValueError):
        decay_days = FLOW_TAG_TOXIC_GUARD_DECAY_DAYS_DEFAULT

    now = (now_dt or datetime.now()).replace(tzinfo=None)
    mkt = str(market or "").upper()

    worst_mult = 1.0
    worst_tag = ""
    worst_age: Optional[int] = None
    for entry in reg.values():
        if not isinstance(entry, dict):
            continue
        if str(entry.get("market", "")).upper() != mkt:
            continue
        registered_at = str(entry.get("registered_at", "")).strip()[:10]
        if not registered_at:
            continue
        try:
            reg_dt = datetime.strptime(registered_at, "%Y-%m-%d")
        except ValueError:
            continue
        age_days = (now - reg_dt).days
        if age_days < 0 or age_days > decay_days:
            continue
        try:
            mult = float(entry.get("kelly_mult", 1.0))
        except (TypeError, ValueError):
            continue
        mult = max(0.0, min(1.0, mult))
        if mult < worst_mult:
            worst_mult = mult
            worst_tag = str(entry.get("tag", ""))
            worst_age = age_days

    if worst_tag and worst_mult < 1.0:
        age_s = str(worst_age) if worst_age is not None else "?"
        return worst_mult, f"{worst_tag}(D+{age_s})"
    return 1.0, ""


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
    force_aggregate: bool = False,
    weekly_window: bool = False,
) -> FlowTagReportSnapshot:
    """explode + groupby 1패스 — 태그별 WR/PF/누적·캐리/출혈·독성.

    force_aggregate: True 면 staleness=RED 여도 집계를 강제(주간 회고 — 과거 확정 청산용).
    weekly_window:   True 면 7일 창 특성에 맞춰 min_n 을 FLOW_TAG_WEEKLY_MIN_N(기본 2)로 다운스케일.
    """
    market = str(timekeeper.market).upper()
    today = timekeeper.calendar_today_kst
    if staleness is None:
        staleness = evaluate_staleness(
            timekeeper,
            live_row_count=0,
            data_candle_watermark=resolve_data_candle_watermark(market, sys_config),
        )

    th = _toxic_thresholds(sys_config)
    min_n = int(th["min_n"])
    if weekly_window:
        # 90일 룩백용 min_n(3)을 7일 주간 창에 그대로 쓰면 1~2건 핵심 태그가 전부 탈락한다.
        # → 주간 전용 동적 임계치로 다운스케일(기본 2, 최소 1).
        cfg_wk = sys_config if isinstance(sys_config, dict) else {}
        try:
            weekly_min_n = int(cfg_wk.get("FLOW_TAG_WEEKLY_MIN_N", 2))
        except (TypeError, ValueError):
            weekly_min_n = 2
        min_n = max(1, min(min_n, weekly_min_n))
    top_k = int(th["top_k"])

    work = enforce_market_frame(df, market, context="flow_tag_snapshot")
    exit_min, exit_max = _exit_date_span(work) if work is not None and len(work) else (None, None)

    if (
        staleness.grade == "RED"
        and not force_aggregate
        and not getattr(staleness, "allow_flow_tag", False)
    ):
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
        mean_ret = cum / float(n) if n else 0.0
        tag_pf = scalar_float(pf.get(tag, 1.0), 1.0) if tag in pf.index else 1.0
        sub = long.loc[long["_tag"] == tag]
        n_losses = int((col_series(sub, "_fr") <= 0).sum()) if not sub.empty else 0
        n_unique = int(sub.apply(_row_ticker_key, axis=1).replace("", np.nan).dropna().nunique())
        n_unknown = 0
        if not sub.empty:
            for _, srow in sub.iterrows():
                _, unk = _display_stock_label(srow)
                if unk:
                    n_unknown += 1
        carry_html = "—"
        bleed_html = "—"
        if not sub.empty:
            fr_sub = col_series(sub, "_fr")
            if not fr_sub.empty:
                carry_html = f"[캐리·대표1건] {_stock_chip(sub.loc[fr_sub.idxmax()])}"
                bleed_html = f"[출혈·대표1건] {_stock_chip(sub.loc[fr_sub.idxmin()])}"
        row_dicts.append(
            {
                "tag": str(tag),
                "n": n,
                "n_losses": n_losses,
                "win_rate_pct": wr,
                "profit_factor": tag_pf,
                "profit_factor_display": _format_pf_display(tag_pf, n, n_losses),
                "cum_ret_pct": cum,
                "mean_ret_pct": mean_ret,
                "n_unique_tickers": n_unique,
                "n_unknown_names": n_unknown,
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
            min_n=min_n,
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
            mean_ret_pct=scalar_float(toxic_rd.get("mean_ret_pct", 0)),
            n_unique_tickers=int(toxic_rd.get("n_unique_tickers", 0)),
            n_unknown_names=int(toxic_rd.get("n_unknown_names", 0)),
            profit_factor_display=str(toxic_rd.get("profit_factor_display", "—")),
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
                mean_ret_pct=scalar_float(rd.get("mean_ret_pct", 0)),
                n_unique_tickers=int(rd.get("n_unique_tickers", 0)),
                n_unknown_names=int(rd.get("n_unknown_names", 0)),
                profit_factor_display=str(rd.get("profit_factor_display", "—")),
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
        staleness = evaluate_staleness(
            timekeeper,
            live_row_count=0,
            data_candle_watermark=resolve_data_candle_watermark(timekeeper.market),
        )

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

    if grade in ("YELLOW", "RED"):
        out += (
            f"⚠️ <i>데이터 신뢰도 <b>{grade}</b> — 아래 수치는 <b>기간 내 실제 청산(CLOSED)</b>만 "
            f"집계합니다. OPEN·당일 미청산·스캔 중단 구간은 반영되지 않을 수 있습니다.</i>\n"
        )
    out += (
        "📐 <i>집계 정의: 승률·PF·건당평균·누적%p = 해당 #태그에 속한 <b>전체 청산건</b> 합산 "
        "(1건이 여러 태그면 태그별로 분할 집계). 누적%p는 수익률 합(표본수에 비례). "
        "캐리/출혈 = 그 태그 내 <b>대표 1건</b>(최대↑/최소↓).</i>\n"
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
            unk_note = ""
            if b.n_unknown_names > 0:
                unk_note = f" · 이름미상 <b>{b.n_unknown_names}</b>건"
            out += (
                f" ▪️ <b>{tag_esc}</b>{toxic_mark}: "
                f"승률 <b>{b.win_rate_pct:.1f}%</b> / "
                f"PF <b>{b.profit_factor_display}</b> / "
                f"건당평균 <b>{b.mean_ret_pct:+.2f}%</b> / "
                f"누적합 <b>{b.cum_ret_pct:+.1f}%p</b> "
                f"(n=<b>{b.n}</b>청산·<b>{b.n_unique_tickers}</b>종목{unk_note})\n"
                f"   ➔ {b.carry_stock_html} · {b.bleed_stock_html}\n"
            )
        out += "\n"

    out += "🗣️ <b>[관제탑 · 태그 시너지]</b> "
    out += snap.synergy_action_html + "\n\n"
    return out

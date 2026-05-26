"""
텔레그램 [에이스 로직 심층 부검] 전용 — 청산 DB `forward_trades` 스냅샷에서
공통성(CV) + 비에이스 대비 판별력을 합성해 피처 Top3를 동적 추출한다.
코어 매매/시그널 수식은 포함하지 않는다.

설정 덮어쓰기 (선택):
  system_config["REPORT_ACE_FEATURE_COLUMNS"] = ["dyn_cpv", "v_energy", ...]
  또는 { "columns": [...] }
"""
from __future__ import annotations

import html
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from forward_report_scalar import col_series, scalar_float, series_mean

# 리포트 집계 시 스냅샷 고착 방지 — 콜로세움/에이스는 메인 DB 우선
_EXCLUDED_FEATURE_COLUMNS = frozenset(
    {
        "id",
        "rowid",
        "code",
        "name",
        "ticker",
        "stock_name",
        "sig_type",
        "strategy_name",
        "market",
        "status",
        "sector",
        "flow_tags",
        "exit_reason",
        "entry_date",
        "exit_date",
        "mkt_group",
        "tier",
        "notes",
        "message",
    }
)

# 존재하는 컬럼만 조회·집계 (스키마 진화 대비)
DEFAULT_REPORT_FEATURE_COLUMNS: List[str] = [
    "dyn_cpv",
    "dyn_tb",
    "dyn_rs",
    "v_energy",
    "v_cpv",
    "v_yang",
    "v_rs",
    "total_score",
    "score_marcap",
    "freq_count",
    "marcap_eok",
    "entry_breadth",
    "market_breadth",
    "entry_cos_score",
    "entry_dtw_score",
    "entry_atr",
    "is_tenbagger",
    "is_top_dna",
    "is_worst_dna",
    "is_death_combo",
    "bars_held",
]

FEATURE_LABELS: Dict[str, str] = {
    "dyn_cpv": "캔들지배력(CPV)",
    "dyn_tb": "찐양봉 지수(TB)",
    "dyn_rs": "상대강도(RS)",
    "v_energy": "응축에너지",
    "v_cpv": "CPV 변동성",
    "v_yang": "양봉 변동성",
    "v_rs": "RS 변동성",
    "total_score": "통합 점수",
    "score_marcap": "시총 보정 점수",
    "freq_count": "빈도 카운트",
    "marcap_eok": "시가총액(억)",
    "entry_breadth": "진입 시점 시장 폭",
    "market_breadth": "청산 시점 시장 폭",
    "entry_cos_score": "코사인 유사도(DNA)",
    "entry_dtw_score": "DTW 거리(DNA)",
    "entry_atr": "진입 ATR",
    "is_tenbagger": "텐배거 플래그",
    "is_top_dna": "상위 DNA 플래그",
    "is_worst_dna": "Worst DNA 플래그",
    "is_death_combo": "데스콤보 플래그",
    "bars_held": "보유 봉 수",
}


def extra_forward_trade_columns_for_report() -> List[str]:
    """`forward_trades` SELECT 확장용 기본 화이트리스트."""
    return list(DEFAULT_REPORT_FEATURE_COLUMNS)


def colosseum_db_path_for_report() -> str:
    """리포트 콜로세움 — 딥다이브와 동일하게 메인 DB SSOT."""
    from market_db_paths import report_db_read_path

    return report_db_read_path()


def colosseum_window_days(sys_config: Optional[Dict[str, Any]] = None) -> int:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        d = int(cfg.get("REPORT_COLOSSEUM_WINDOW_DAYS", 90))
    except (TypeError, ValueError):
        d = 90
    return max(14, min(d, 365))


def discover_numeric_feature_columns(
    df: pd.DataFrame,
    whitelist: Sequence[str],
) -> List[str]:
    """화이트리스트 ∪ DB 수치 컬럼 — 메타/문자열 제외."""
    found: List[str] = []
    seen: set[str] = set()
    for c in list(whitelist) + list(df.columns):
        cn = str(c).strip()
        if not cn or cn in seen or cn in _EXCLUDED_FEATURE_COLUMNS:
            continue
        if cn not in df.columns:
            continue
        s = col_series(df, cn)
        num = pd.to_numeric(s, errors="coerce")
        if num.notna().sum() < 3:
            continue
        if num.nunique(dropna=True) < 2:
            continue
        seen.add(cn)
        found.append(cn)
    return found


def _welch_pvalue(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if len(a) < 3 or len(b) < 3:
        return 1.0
    try:
        from scipy import stats

        _, p = stats.ttest_ind(a, b, equal_var=False)
        return float(p) if np.isfinite(p) else 1.0
    except Exception:
        m1, m2 = float(np.mean(a)), float(np.mean(b))
        v1, v2 = float(np.var(a, ddof=1)), float(np.var(b, ddof=1))
        n1, n2 = len(a), len(b)
        se = np.sqrt(v1 / n1 + v2 / n2 + 1e-12)
        if se < 1e-12:
            return 1.0
        t = abs(m1 - m2) / se
        return float(max(0.001, min(1.0, 2.0 * (1.0 - min(1.0, t / 4.0)))))


def _significance_weight(p: float) -> float:
    """p 작을수록 1에 가깝게 — 랭킹 가중."""
    if not np.isfinite(p):
        return 0.0
    return float(max(0.0, min(1.0, 1.0 - min(float(p), 0.25) / 0.25)))


def _resolve_feature_whitelist(sys_config: Optional[Dict[str, Any]]) -> List[str]:
    if not isinstance(sys_config, dict):
        return list(DEFAULT_REPORT_FEATURE_COLUMNS)
    raw = sys_config.get("REPORT_ACE_FEATURE_COLUMNS")
    if isinstance(raw, list) and len(raw) > 0:
        out = [str(x).strip() for x in raw if str(x).strip()]
        return out if out else list(DEFAULT_REPORT_FEATURE_COLUMNS)
    if isinstance(raw, dict):
        cols = raw.get("columns") or raw.get("whitelist")
        if isinstance(cols, list) and cols:
            out = [str(x).strip() for x in cols if str(x).strip()]
            return out if out else list(DEFAULT_REPORT_FEATURE_COLUMNS)
    return list(DEFAULT_REPORT_FEATURE_COLUMNS)


def _is_binary_like(s: pd.Series) -> bool:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) < 2:
        return False
    u = set(np.unique(s.values))
    return u.issubset({0.0, 1.0}) or u.issubset({0, 1})


def _cv_score(vals: np.ndarray) -> float:
    """작을수록 공통성 높음 → 후단에서 역수 매핑."""
    v = np.asarray(vals, dtype=float)
    v = v[np.isfinite(v)]
    if len(v) < 2:
        return np.nan
    m = float(np.mean(v))
    sd = float(np.std(v, ddof=1))
    if abs(m) > 1e-6:
        cv = abs(sd / m)
    else:
        cv = sd
    return float(cv)


def _commonality_from_cv(cv: float) -> float:
    if not np.isfinite(cv) or cv < 0:
        return 0.0
    return float(1.0 / (1.0 + cv))


def _commonality_binary(vals: np.ndarray) -> float:
    """0/1 벡터: p→0 또는 1에 가까울수록 공통성↑."""
    v = np.asarray(vals, dtype=float)
    v = v[np.isfinite(v)]
    if len(v) < 2:
        return 0.0
    p = float(np.mean(v))
    return float(1.0 - 2.0 * abs(p - 0.5))


def _cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if len(a) < 2 or len(b) < 2:
        return 0.0
    m1, m2 = float(np.mean(a)), float(np.mean(b))
    s1, s2 = float(np.std(a, ddof=1)), float(np.std(b, ddof=1))
    pooled = np.sqrt((s1 * s1 + s2 * s2) / 2.0 + 1e-12)
    return float(abs(m1 - m2) / pooled)


def _discrimination_binary(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    if len(a) < 2 or len(b) < 2:
        return 0.0
    p1 = float(np.mean(a))
    p2 = float(np.mean(b))
    return float(min(1.5, abs(p1 - p2) * 2.0))


@dataclass
class FeatureInsight:
    column: str
    label: str
    kind: str
    commonality: float
    discrimination: float
    combined: float
    ace_summary: str
    baseline_summary: str
    p_value: float = 1.0
    effect_size: float = 0.0


@dataclass
class ContrastInsight:
    """승자 vs 패자 DNA 대조 1피처 요약."""

    column: str
    label: str
    kind: str
    discrimination: float
    direction: str
    winner_line: str
    loser_line: str
    pct_winners_above_loser_median: Optional[float] = None


class ReportFeatureAnalyzer:
    def __init__(
        self,
        *,
        sys_config: Optional[Dict[str, Any]] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.sys_config = sys_config if isinstance(sys_config, dict) else {}
        self.meta = meta if isinstance(meta, dict) else {}
        self._whitelist = _resolve_feature_whitelist(self.sys_config)
        self._last_disc_pvalues: Dict[str, float] = {}
        self._last_disc_effects: Dict[str, float] = {}

    def _columns_to_scan(self, ace_df: pd.DataFrame) -> List[str]:
        return discover_numeric_feature_columns(ace_df, self._whitelist)

    def _rank_features_commonality(
        self, ace_df: pd.DataFrame, columns: Sequence[str]
    ) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for col in columns:
            s = col_series(ace_df, col)
            if s.empty:
                continue
            if _is_binary_like(s):
                arr = pd.to_numeric(s, errors="coerce").to_numpy()
                out[col] = _commonality_binary(arr)
            else:
                arr = pd.to_numeric(s, errors="coerce").to_numpy()
                cv = _cv_score(arr)
                if not np.isfinite(cv):
                    continue
                out[col] = _commonality_from_cv(cv)
        return out

    def _rank_features_discrimination(
        self,
        ace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        columns: Sequence[str],
    ) -> Dict[str, float]:
        """비에이스 대비 분리도 — 연속: Cohen's d + Welch p, 이진: 확률차."""
        out: Dict[str, float] = {}
        self._last_disc_pvalues: Dict[str, float] = {}
        self._last_disc_effects: Dict[str, float] = {}
        for col in columns:
            if col not in baseline_df.columns:
                out[col] = 0.0
                self._last_disc_pvalues[col] = 1.0
                self._last_disc_effects[col] = 0.0
                continue
            a = col_series(ace_df, col)
            b = col_series(baseline_df, col)
            if a.empty and b.empty:
                out[col] = 0.0
                self._last_disc_pvalues[col] = 1.0
                self._last_disc_effects[col] = 0.0
                continue
            if _is_binary_like(pd.concat([a, b], ignore_index=True)):
                aa = pd.to_numeric(a, errors="coerce").to_numpy()
                bb = pd.to_numeric(b, errors="coerce").to_numpy()
                eff = _discrimination_binary(aa, bb)
                out[col] = float(np.tanh(eff))
                self._last_disc_pvalues[col] = 1.0
                self._last_disc_effects[col] = eff
            else:
                aa = pd.to_numeric(a, errors="coerce").to_numpy()
                bb = pd.to_numeric(b, errors="coerce").to_numpy()
                d = _cohens_d(aa, bb)
                p = _welch_pvalue(aa, bb)
                sig_w = _significance_weight(p)
                out[col] = float(np.tanh(d) * sig_w)
                self._last_disc_pvalues[col] = p
                self._last_disc_effects[col] = d
        return out

    def _merge_top_insights(
        self,
        ace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        columns: Sequence[str],
        commonality: Dict[str, float],
        discrimination: Dict[str, float],
        top_n: int = 3,
        w_common: float = 0.28,
        w_disc: float = 0.72,
    ) -> List[FeatureInsight]:
        rows: List[FeatureInsight] = []
        pvals = getattr(self, "_last_disc_pvalues", {}) or {}
        effects = getattr(self, "_last_disc_effects", {}) or {}
        for col in columns:
            c0 = float(commonality.get(col, 0.0))
            d0 = float(discrimination.get(col, 0.0))
            if d0 <= 1e-6 and c0 <= 1e-6:
                continue
            pv = float(pvals.get(col, 1.0))
            eff = float(effects.get(col, 0.0))
            comb = w_common * c0 + w_disc * d0
            if comb <= 1e-6:
                continue
            ace_col = col_series(ace_df, col)
            kind = "binary" if not ace_col.empty and _is_binary_like(ace_col) else "continuous"
            if kind == "binary":
                ap = series_mean(ace_df, col, default=np.nan)
                bp = (
                    series_mean(baseline_df, col, default=np.nan)
                    if col in baseline_df.columns
                    else np.nan
                )
                ace_s = f"활성비율 {scalar_float(ap) * 100:.0f}%"
                base_s = (
                    f"비교군 {scalar_float(bp) * 100:.0f}%"
                    if np.isfinite(scalar_float(bp, default=np.nan))
                    else "비교군 N/A"
                )
            else:
                am = series_mean(ace_df, col, default=np.nan)
                bm = (
                    series_mean(baseline_df, col, default=np.nan)
                    if col in baseline_df.columns
                    else np.nan
                )
                ace_s = f"평균 {scalar_float(am):.3g}"
                base_s = (
                    f"비교군 평균 {scalar_float(bm):.3g}"
                    if np.isfinite(scalar_float(bm, default=np.nan))
                    else "비교군 N/A"
                )
            label = FEATURE_LABELS.get(col, col)
            rows.append(
                FeatureInsight(
                    column=col,
                    label=label,
                    kind=kind,
                    commonality=c0,
                    discrimination=d0,
                    combined=comb,
                    ace_summary=ace_s,
                    baseline_summary=base_s,
                    p_value=pv,
                    effect_size=eff,
                )
            )
        rows.sort(key=lambda x: (x.combined, x.effect_size), reverse=True)
        return rows[:top_n]

    def _format_winner_loser_feature(
        self,
        col: str,
        winners_df: pd.DataFrame,
        losers_df: pd.DataFrame,
    ) -> Optional[ContrastInsight]:
        if col not in winners_df.columns or col not in losers_df.columns:
            return None
        w_s = col_series(winners_df, col)
        l_s = col_series(losers_df, col)
        label = FEATURE_LABELS.get(col, col)
        combined = pd.concat([w_s, l_s], ignore_index=True)
        if _is_binary_like(combined):
            wa = pd.to_numeric(w_s, errors="coerce").dropna()
            la = pd.to_numeric(l_s, errors="coerce").dropna()
            if wa.empty or la.empty:
                return None
            pw = scalar_float(wa.mean())
            pl = scalar_float(la.mean())
            direction = "higher_in_winners" if pw >= pl else "lower_in_winners"
            return ContrastInsight(
                column=col,
                label=label,
                kind="binary",
                discrimination=0.0,
                direction=direction,
                winner_line=f"승자군 활성 <b>{pw * 100:.0f}%</b>",
                loser_line=f"패자군 활성 <b>{pl * 100:.0f}%</b>",
            )
        w = pd.to_numeric(w_s, errors="coerce").dropna()
        l = pd.to_numeric(l_s, errors="coerce").dropna()
        if len(w) < 2 or len(l) < 2:
            return None
        wm = scalar_float(w.median())
        lm = scalar_float(l.median())
        wmean = scalar_float(w.mean())
        lmean = scalar_float(l.mean())
        direction = "higher_in_winners" if wmean >= lmean else "lower_in_winners"
        p_above = scalar_float((w > lm).mean() * 100.0)
        return ContrastInsight(
            column=col,
            label=label,
            kind="continuous",
            discrimination=0.0,
            direction=direction,
            winner_line=f"승자 중앙 <b>{wm:.4g}</b> · 평균 <b>{wmean:.4g}</b>",
            loser_line=f"패자 중앙 <b>{lm:.4g}</b> · 평균 <b>{lmean:.4g}</b>",
            pct_winners_above_loser_median=p_above,
        )

    def _contrast_narrative_paragraph(self, insights: Sequence[ContrastInsight]) -> str:
        """하드코딩 없이 Top 대조 피처만으로 관제탑 요약 1문단."""
        if not insights:
            return ""
        chunks: List[str] = []
        for ins in insights:
            if ins.kind == "binary":
                if ins.direction == "higher_in_winners":
                    chunks.append(
                        f"<b>{html.escape(ins.label, quote=False)}</b> "
                        f"조건은 승자군에서 더 자주 충족({ins.winner_line}, {ins.loser_line})."
                    )
                else:
                    chunks.append(
                        f"<b>{html.escape(ins.label, quote=False)}</b> "
                        f"조건은 패자군에서 더 자주 충족({ins.loser_line}, {ins.winner_line})."
                    )
            else:
                pct = ins.pct_winners_above_loser_median
                pct_s = (
                    f" 승자 중 약 <b>{pct:.0f}%</b>가 패자 중앙값을 상회했습니다."
                    if pct is not None and np.isfinite(pct)
                    else ""
                )
                if ins.direction == "higher_in_winners":
                    chunks.append(
                        f"<b>{html.escape(ins.label, quote=False)}</b>은(는) 승자군에서 "
                        f"값대가 높았습니다 ({ins.winner_line} vs {ins.loser_line}).{pct_s}"
                    )
                else:
                    chunks.append(
                        f"<b>{html.escape(ins.label, quote=False)}</b>은(는) 승자군에서 "
                        f"상대적으로 억제된 값대를 보였습니다 ({ins.winner_line} vs {ins.loser_line}).{pct_s}"
                    )
        tail = " 두 축을 다음 청산 표본까지 동일하게 추적합니다."
        return " ".join(chunks) + tail

    def _collect_winner_loser_contrast_insights(
        self,
        winners_df: pd.DataFrame,
        losers_df: pd.DataFrame,
        *,
        top_n: int,
        min_per_group: int,
    ) -> Tuple[List[ContrastInsight], List[str], int, int]:
        """
        판별력 상위 피처 ContrastInsight 목록.
        실패 시 (빈 리스트, 에러용 HTML 라인, nw, nl).
        """
        lines_err: List[str] = []
        nw = len(winners_df) if winners_df is not None else 0
        nl = len(losers_df) if losers_df is not None else 0
        if (
            winners_df is None
            or losers_df is None
            or nw < min_per_group
            or nl < min_per_group
        ):
            lines_err.append(
                "<i>표본 부족 (승자 <b>"
                f"{nw}</b>건 · 패자 <b>{nl}</b>건, 각 최소 <b>{min_per_group}</b>건 필요)으로 "
                "DNA 대조 딥다이브 생략.</i>\n"
            )
            return [], lines_err, nw, nl

        cols = self._columns_to_scan(winners_df)
        cols = [c for c in cols if c in losers_df.columns]
        if not cols:
            lines_err.append("<i>DNA 대조: 스캔 가능한 수치 컬럼이 없습니다.</i>\n")
            return [], lines_err, nw, nl

        disc = self._rank_features_discrimination(winners_df, losers_df, cols)
        ranked = sorted(
            ((float(disc.get(c, 0.0)), c) for c in cols),
            key=lambda x: x[0],
            reverse=True,
        )
        ranked = [(s, c) for s, c in ranked if s > 1e-6]
        insights: List[ContrastInsight] = []
        for score, col in ranked:
            if len(insights) >= int(top_n):
                break
            ins = self._format_winner_loser_feature(col, winners_df, losers_df)
            if ins is None:
                continue
            insights.append(
                ContrastInsight(
                    column=ins.column,
                    label=ins.label,
                    kind=ins.kind,
                    discrimination=float(score),
                    direction=ins.direction,
                    winner_line=ins.winner_line,
                    loser_line=ins.loser_line,
                    pct_winners_above_loser_median=ins.pct_winners_above_loser_median,
                )
            )

        if not insights:
            ranked_loose = sorted(
                ((float(disc.get(c, 0.0)), c) for c in cols),
                key=lambda x: x[0],
                reverse=True,
            )
            for score, col in ranked_loose:
                if len(insights) >= int(top_n):
                    break
                ins = self._format_winner_loser_feature(col, winners_df, losers_df)
                if ins is None:
                    continue
                insights.append(
                    ContrastInsight(
                        column=ins.column,
                        label=ins.label,
                        kind=ins.kind,
                        discrimination=float(score),
                        direction=ins.direction,
                        winner_line=ins.winner_line,
                        loser_line=ins.loser_line,
                        pct_winners_above_loser_median=ins.pct_winners_above_loser_median,
                    )
                )

        if not insights:
            lines_err.append(
                "<i>DNA 대조: 유의미한 판별력을 가진 피처가 없습니다 "
                "(표본 확대 또는 피처 화이트리스트를 확인).</i>\n"
            )
            return [], lines_err, nw, nl

        return insights, [], nw, nl

    def build_winner_loser_dna_contrast(
        self,
        *,
        winners_df: pd.DataFrame,
        losers_df: pd.DataFrame,
        top_n: int = 2,
        min_per_group: int = 2,
    ) -> Tuple[List[str], bool, List[ContrastInsight]]:
        """
        승자·패자 청산 스냅샷을 대조해 판별력 상위 DNA 피처 Top N 을 요약.
        반환: (Telegram HTML 라인, 성공 여부, ContrastInsight 목록)
        """
        lines: List[str] = []
        insights, err_lines, nw, nl = self._collect_winner_loser_contrast_insights(
            winners_df,
            losers_df,
            top_n=top_n,
            min_per_group=min_per_group,
        )
        if err_lines:
            lines.extend(err_lines)
            return lines, False, []

        lines.append(
            f"📎 대조 표본 — 대박(≥5%) <b>{nw}</b>건 · 참사(≤-3%) <b>{nl}</b>건 · "
            f"판별력 상위 <b>{len(insights)}</b>축\n"
        )
        for i, ins in enumerate(insights, 1):
            lines.append(
                f" {i}) <b>{html.escape(ins.label, quote=False)}</b> "
                f"(분리도 {ins.discrimination:.2f}) — {ins.winner_line} | {ins.loser_line}\n"
            )
        lines.append("\n🗣️ <b>[관제탑 시선 · 동적]</b>\n")
        lines.append(self._contrast_narrative_paragraph(insights) + "\n")
        return lines, True, insights

    def collect_ace_feature_insights(
        self,
        ace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
    ) -> List[FeatureInsight]:
        """AceEvolution FactPack·LLM용 통계 insight 목록."""
        cols = self._columns_to_scan(ace_df)
        if ace_df.empty or not cols:
            return []
        base = baseline_df
        if base is None or base.empty:
            base = pd.DataFrame(columns=ace_df.columns)
        common = self._rank_features_commonality(ace_df, cols)
        disc = self._rank_features_discrimination(ace_df, base, cols)
        return self._merge_top_insights(ace_df, base, cols, common, disc)

    def build_ace_deep_dive_lines(
        self,
        *,
        league: str,
        logic_label: str,
        ace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        spillover_sector: Optional[str] = None,
        data_anchor: Optional[str] = None,
        window_days: Optional[int] = None,
        n_features_scanned: Optional[int] = None,
    ) -> Tuple[List[str], bool]:
        """
        HTML 이스케이프 전 텍스트 라인.
        Returns:
            (lines, True) if at least one ranked feature insight was produced; else (lines, False).
        """
        cols = self._columns_to_scan(ace_df)
        n_scan = len(cols) if n_features_scanned is None else int(n_features_scanned)
        if ace_df.empty or len(ace_df) < 3 or not cols:
            return [], False
        base = baseline_df
        if base is None or base.empty:
            base = pd.DataFrame(columns=ace_df.columns)
        common = self._rank_features_commonality(ace_df, cols)
        disc = self._rank_features_discrimination(ace_df, base, cols)
        insights = self._merge_top_insights(ace_df, base, cols, common, disc)

        rk = html.escape(str(self.meta.get("META_REGIME_KEY") or self.sys_config.get("CURRENT_REGIME_KEY") or "UNKNOWN"))
        spill = html.escape((spillover_sector or "").strip())
        hot = html.escape(
            str(
                self.sys_config.get("US_SPILLOVER_SECTOR")
                or self.sys_config.get("PREDICTED_NEXT_SECTOR")
                or ""
            ).strip()
        )

        lines: List[str] = []
        flag = "🇰🇷" if league == "KR" else "🇺🇸"
        anchor = html.escape(str(data_anchor or "").strip()[:10])
        wd = int(window_days) if window_days else colosseum_window_days(self.sys_config)
        if anchor:
            lines.append(
                f"{flag} <b>[동적 필터 교집합]</b> ({logic_label}) — "
                f"KST 앵커 <b>{anchor}</b> · 롤링 <b>{wd}</b>일 · "
                f"스캔 피처 <b>{n_scan}</b>축\n"
            )
        else:
            lines.append(
                f"{flag} <b>[동적 필터 교집합]</b> ({logic_label}) — "
                f"롤링 <b>{wd}</b>일 · 스캔 피처 <b>{n_scan}</b>축\n"
            )
        lines.append(
            f" Meta <code>{rk}</code> · 에이스 <b>{len(ace_df)}</b>건 vs 비에이스 <b>{len(base)}</b>건 "
            f"<i>(당일 DB 실시간 · 캐시 미사용)</i>\n"
        )
        if not insights:
            lines.append("<i>유효한 수치 피처가 부족합니다.</i>\n")
            return lines, False

        for i, ins in enumerate(insights, 1):
            p_s = f"p={ins.p_value:.3f}" if ins.kind == "continuous" else "이진"
            eff_s = f"d={ins.effect_size:.2f}" if ins.kind == "continuous" else ""
            lines.append(
                f" {i}) <b>{ins.label}</b> — {ins.ace_summary} | {ins.baseline_summary} "
                f"({eff_s} {p_s} · 분리 {ins.discrimination:.2f})\n"
            )

        if spill and hot:
            if spill.upper() == hot.upper() or hot in spill or spill in hot:
                lines.append(f" 🧭 스필오버: 에이스 섹터가 관제탑 주도 섹터(<b>{hot}</b>)와 <b>정렬</b>되었습니다.\n")
            else:
                lines.append(
                    f" 🧭 스필오버: 에이스 섹터 모드는 <b>{spill}</b>, 관제탑 주도는 <b>{hot}</b> — "
                    f"<b>교차/다층</b> 구조입니다.\n"
                )
        elif spill:
            lines.append(f" 🧭 섹터: <b>{spill}</b>\n")

        lines.append(
            " ◽ <i>전 수치 피처 스캔 → 분리(Cohen's d×Welch p) 상위 3축. 공통성=에이스 내 CV 역수.</i>\n"
        )
        return lines, True

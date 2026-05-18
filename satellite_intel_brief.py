"""
팩토리 위성망 통합 첩보 — Provider 레지스트리 SSOT.

- GLOBAL / KR / US 3단 블록
- 데이터 없음은 명시, except: pass 금지
- 텔레그램 HTML, 시장별 ~1,200자 상한
"""
from __future__ import annotations

import os
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from factory_data_paths import factory_data_dir
from meta_state_store import resolve_config_regime_key
from toxic_antipattern_core import collect_merged_antipattern_rules

MarketScope = Literal["GLOBAL", "KR", "US"]
ReportMarket = Literal["KR", "US"]

_MAX_BRIEF_CHARS = 1200

_kr_sector_map_cache: Optional[Dict[str, str]] = None


def _esc(s: Any) -> str:
    t = str(s) if s is not None else ""
    return t.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _today_kst_header() -> str:
    try:
        from zoneinfo import ZoneInfo

        now = datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M KST")


def _alt_data_db_path() -> str:
    from market_db_paths import MARKET_DATA_DB_PATH

    candidates = [
        os.path.join(os.path.dirname(MARKET_DATA_DB_PATH), "alt_data.sqlite"),
        os.path.join(factory_data_dir(), "alt_data.sqlite"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    return candidates[0]


def _load_macro_daily_row() -> Tuple[Optional[Tuple[Any, ...]], Optional[str]]:
    path = _alt_data_db_path()
    if not os.path.isfile(path):
        return None, "alt_data.sqlite 없음"
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
        try:
            row = conn.execute(
                "SELECT usd_krw, us_10y_yield, vix_index, date FROM macro_daily ORDER BY date DESC LIMIT 1"
            ).fetchone()
            return row, None
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        return None, str(ex)[:80]


def _defcon_level(cfg: Dict[str, Any]) -> int:
    dd = cfg.get("DOOMSDAY_DEFCON") or {}
    if not isinstance(dd, dict):
        return 5
    try:
        return int(dd.get("level", 5))
    except (TypeError, ValueError):
        return 5


def _blackhole_count(cfg: Dict[str, Any]) -> int:
    bh = cfg.get("BLACKHOLE_TOXIC_COUNT", 0)
    if isinstance(bh, dict):
        try:
            return int(bh.get("count", 0) or 0)
        except (TypeError, ValueError):
            return 0
    try:
        return int(bh or 0)
    except (TypeError, ValueError):
        return 0


def _shadow_block_totals(cfg: Dict[str, Any]) -> int:
    sp = cfg.get("SHADOW_PERFORMANCE")
    if not isinstance(sp, dict):
        return 0
    blocked = sp.get("blocked")
    if not isinstance(blocked, dict):
        return 0
    raw = blocked.get("reason_event_counts") or {}
    if not isinstance(raw, dict):
        return 0
    total = 0
    for v in raw.values():
        try:
            total += int(v or 0)
        except (TypeError, ValueError):
            continue
    return total


def _load_kr_sector_map() -> Dict[str, str]:
    global _kr_sector_map_cache
    if _kr_sector_map_cache is not None:
        return _kr_sector_map_cache

    merged: Dict[str, str] = {}
    try:
        import FinanceDataReader as fdr  # type: ignore

        for tag in ("KOSPI", "KOSDAQ"):
            try:
                lst = fdr.StockListing(tag)
            except Exception:
                continue
            if lst is None or getattr(lst, "empty", True):
                continue
            code_col = "Code" if "Code" in lst.columns else ("Symbol" if "Symbol" in lst.columns else None)
            if code_col is None:
                continue
            sec_col = None
            for c in ("Industry", "업종", "Sector", "sector", "분류"):
                if c in lst.columns:
                    sec_col = c
                    break
            if sec_col is None:
                continue
            for _, row in lst.iterrows():
                code = str(row[code_col]).strip().zfill(6)
                sec = str(row[sec_col]).strip() or "기타"
                merged[code] = sec[:20]
    except Exception:
        pass

    _kr_sector_map_cache = merged
    return merged


def aggregate_smart_money_themes(
    picks: Dict[str, Any],
    *,
    max_themes: int = 2,
    max_examples: int = 2,
) -> Tuple[List[Tuple[str, int]], List[str]]:
    """Top N 섹터(테마) + 대표 종목명."""
    if not picks:
        return [], []

    smap = _load_kr_sector_map()
    sector_counts: Counter[str] = Counter()
    best_by_sector: Dict[str, Tuple[str, float]] = {}

    for code, info in picks.items():
        if not isinstance(info, dict):
            continue
        c6 = str(code).strip().zfill(6)
        sec = smap.get(c6, "기타")
        sector_counts[sec] += 1
        try:
            div = float(info.get("divergence_score") or 0)
        except (TypeError, ValueError):
            div = 0.0
        name = str(info.get("name") or code).strip()
        prev = best_by_sector.get(sec)
        if prev is None or div > prev[1]:
            best_by_sector[sec] = (name, div)

    top = sector_counts.most_common(max_themes)
    examples: List[str] = []
    for sec, _ in top:
        if sec in best_by_sector:
            examples.append(best_by_sector[sec][0])
        if len(examples) >= max_examples:
            break
    return top, examples


def _sentiment_insight(delta: Optional[float], score: Optional[float]) -> str:
    if delta is not None:
        if delta >= 3.0:
            return "전일 대비 온도 상승 — 위험선호·테마 확산 구간"
        if delta <= -3.0:
            return "전일 대비 온도 하락 — 방어·현금 비중 확대 권고"
        if abs(delta) < 1.0:
            return "온도 횡보 — 확실한 개별 타점만 선별"
        if delta > 0:
            return "온도 완만 상승 — 추세 추종보다 질적 종목 선별"
        return "온도 완만 하락 — 손실 확대 구간 주의"
    if score is not None:
        if score >= 60:
            return "고온 국면 — 과열 섹터 익절·분산 검토"
        if score <= 35:
            return "저온 국면 — 방어·현금 헤지 우선"
    return "Δ 산출 불가 — 키워드·온도만 참고"


@dataclass
class SatelliteLine:
    scope: MarketScope
    text: str
    priority: int = 50


@dataclass
class SatelliteContext:
    cfg: Dict[str, Any]
    report_market: ReportMarket
    sentiment_fresh_warn: bool = False


ProviderFn = Callable[[SatelliteContext], List[SatelliteLine]]


def _provider_regime(ctx: SatelliteContext) -> List[SatelliteLine]:
    rk = resolve_config_regime_key(ctx.cfg)
    ra = ctx.cfg.get("REGIME_ANALYSIS")
    conf = ""
    if isinstance(ra, dict):
        try:
            c = ra.get("confidence")
            if c is not None:
                conf = f" · 신뢰 {_esc(f'{float(c):.2f}')}"
        except (TypeError, ValueError):
            pass
    defcon = _defcon_level(ctx.cfg)
    bh = _blackhole_count(ctx.cfg)
    return [
        SatelliteLine(
            "GLOBAL",
            f"▪ 거시레짐 <b>{_esc(rk)}</b>{conf} · DEFCON {defcon}/5 · 블랙홀 독성 {bh}종",
            priority=10,
        )
    ]


def _provider_macro(ctx: SatelliteContext) -> List[SatelliteLine]:
    row, err = _load_macro_daily_row()
    if row is None:
        msg = "▪ 매크로: 데이터 없음"
        if err:
            msg += f" ({_esc(err)})"
        return [SatelliteLine("GLOBAL", msg, priority=20)]
    try:
        fx = float(row[0]) if row[0] is not None else None
        y10 = float(row[1]) if row[1] is not None else None
        vix = float(row[2]) if row[2] is not None else None
    except (TypeError, ValueError):
        return [SatelliteLine("GLOBAL", "▪ 매크로: 데이터 없음 (값 파싱 실패)", priority=20)]
    parts = []
    if fx is not None:
        parts.append(f"FX {fx:,.0f}원")
    if y10 is not None:
        parts.append(f"US10Y {y10:.2f}%")
    if vix is not None:
        parts.append(f"VIX {vix:.1f}")
    if not parts:
        return [SatelliteLine("GLOBAL", "▪ 매크로: 데이터 없음", priority=20)]
    asof = str(row[3])[:10] if len(row) > 3 and row[3] else ""
    tail = f" [{_esc(asof)}]" if asof else ""
    return [SatelliteLine("GLOBAL", f"▪ 매크로: {' · '.join(parts)}{tail}", priority=20)]


def _provider_sentiment(ctx: SatelliteContext) -> List[SatelliteLine]:
    from news_data_paths import load_sentiment_with_prior, today_kst_str

    bundle = load_sentiment_with_prior()
    if bundle.get("db_missing"):
        return [SatelliteLine("GLOBAL", "▪ 센티먼트: 데이터 없음 (news_data.sqlite 없음)", priority=30)]
    if bundle.get("error"):
        return [
            SatelliteLine(
                "GLOBAL",
                f"▪ 센티먼트: 데이터 없음 (조회 오류: {_esc(bundle['error'])})",
                priority=30,
            )
        ]

    rec = bundle.get("current")
    if rec is None:
        return [SatelliteLine("GLOBAL", "▪ 센티먼트: 데이터 없음", priority=30)]

    today = today_kst_str()
    d = rec.get("date") or "—"
    score = rec.get("sentiment_score")
    score_s = f"{score:.1f}" if score is not None else "—"
    delta = bundle.get("delta")
    delta_s = ""
    if delta is not None:
        sign = "+" if delta >= 0 else ""
        delta_s = f" (Δ{sign}{delta:.1f} vs {_esc(bundle.get('prior_date') or '전일')})"
    elif bundle.get("prior") is None:
        delta_s = " (전일 비교 데이터 없음)"

    if rec.get("stale"):
        if ctx.sentiment_fresh_warn:
            kw = "키워드 미표시"
        else:
            kw = "당일 미갱신"
        line = f"▪ 센티먼트: 데이터 없음 · {kw} · 마지막 온도 {score_s}점{delta_s} [{_esc(d)}]"
        return [SatelliteLine("GLOBAL", line, priority=30)]

    if rec.get("missing_content"):
        return [SatelliteLine("GLOBAL", f"▪ 센티먼트: 데이터 없음 [{_esc(d)}]", priority=30)]

    k1 = rec.get("top_keyword_1") or "—"
    k2 = rec.get("top_keyword_2") or "—"
    k3 = rec.get("top_keyword_3") or "—"
    insight = _sentiment_insight(delta if isinstance(delta, (int, float)) else None, score)
    line = (
        f"▪ 센티먼트: 온도 <b>{score_s}</b>점{delta_s} — {_esc(k1)}, {_esc(k2)}, {_esc(k3)}\n"
        f"   └ {_esc(insight)}"
    )
    return [SatelliteLine("GLOBAL", line, priority=30)]


def _provider_toxic(ctx: SatelliteContext) -> List[SatelliteLine]:
    rules = collect_merged_antipattern_rules(ctx.cfg)
    n_rules = len(rules)
    shadow_n = _shadow_block_totals(ctx.cfg)
    if n_rules == 0 and shadow_n == 0:
        return [SatelliteLine("GLOBAL", "▪ 독성방어: 활성 규칙 0 · 그림자 차단 이벤트 없음", priority=40)]
    return [
        SatelliteLine(
            "GLOBAL",
            f"▪ 독성방어: 활성 규칙 <b>{n_rules}</b>개 · 그림자 누적 차단 <b>{shadow_n}</b>건",
            priority=40,
        )
    ]


def _provider_smart_money_kr(ctx: SatelliteContext) -> List[SatelliteLine]:
    if ctx.report_market != "KR":
        return []
    radar = ctx.cfg.get("SMART_MONEY_RADAR") or {}
    picks = radar.get("picks", {}) if isinstance(radar, dict) else {}
    if not isinstance(picks, dict):
        picks = {}
    n = len(picks)
    if n == 0:
        updated = ""
        if isinstance(radar, dict) and radar.get("updated_at"):
            updated = f" (갱신 {_esc(radar.get('updated_at'))})"
        return [SatelliteLine("KR", f"▪ 스마트머니: 데이터 없음{updated}", priority=10)]

    top_themes, examples = aggregate_smart_money_themes(picks)
    theme_s = ""
    if top_themes:
        parts = [f"{_esc(t)}({c})" for t, c in top_themes]
        theme_s = f" — 집중: {', '.join(parts)}"
    ex_s = ""
    if examples:
        ex_s = f"\n   └ 대표: {_esc(' · '.join(examples[:2]))}"
    return [
        SatelliteLine(
            "KR",
            f"▪ 스마트머니: <b>{n}</b>종목 매집 포착{theme_s}{ex_s}",
            priority=10,
        )
    ]


def _provider_sector_rotation(ctx: SatelliteContext) -> List[SatelliteLine]:
    mkt = ctx.report_market
    scope: MarketScope = mkt
    try:
        from sector_spillover_refresh import resolve_predicted_sector_display

        pred = resolve_predicted_sector_display(ctx.cfg, mkt)
    except Exception as ex:
        pred = str(ctx.cfg.get(f"PREDICTED_NEXT_SECTOR_{mkt}") or "데이터 없음")
        if not pred or pred in ("분석중", "NONE", ""):
            pred = f"데이터 없음 ({_esc(str(ex)[:40])})"

    rot = ctx.cfg.get("ROTATION_ADVANTAGE_ACTIVE")
    rot_s = " · 베팅어드밴티지 🔥200%" if rot else ""
    from_key = f"PREDICTED_NEXT_SECTOR_{mkt}_FROM"
    src = str(ctx.cfg.get(from_key) or "").strip()
    src_s = f" · 현재 주도 {_esc(src[:18])}" if src and src not in ("분석중", "NONE", "") else ""

    return [
        SatelliteLine(
            scope,
            f"▪ 순환매 예측: <b>{_esc(pred)}</b>{src_s}{rot_s}",
            priority=20,
        )
    ]


def _provider_us_spillover(ctx: SatelliteContext) -> List[SatelliteLine]:
    try:
        from sector_spillover_refresh import resolve_us_spillover_display

        spill = resolve_us_spillover_display(ctx.cfg)
    except Exception as ex:
        spill = str(ctx.cfg.get("US_SPILLOVER_SECTOR") or "데이터 없음")
        if spill in ("분석중", "NONE", ""):
            spill = f"데이터 없음"

    if ctx.report_market == "US":
        if spill in ("데이터 없음", "분석중", "NONE", ""):
            return [SatelliteLine("US", "▪ US 주도 섹터(스필오버): 데이터 없음", priority=15)]
        return [SatelliteLine("US", f"▪ US 주도 섹터: <b>{_esc(spill)}</b>", priority=15)]

    if spill in ("데이터 없음", "분석중", "NONE", ""):
        return [SatelliteLine("KR", "▪ 한미 스필오버: 데이터 없음", priority=25)]
    return [
        SatelliteLine(
            "KR",
            f"▪ 한미 스필오버: 🇺🇸 <b>{_esc(spill)}</b> → 🇰🇷 연관 섹터 선취매 가중",
            priority=25,
        )
    ]


def _provider_smart_money_us_placeholder(ctx: SatelliteContext) -> List[SatelliteLine]:
    if ctx.report_market != "US":
        return []
    return [
        SatelliteLine(
            "US",
            "▪ 스마트머니(US): 데이터 없음 — KR pykrx 파이프 전용 (US 위성 P2 예정)",
            priority=12,
        )
    ]


SATELLITE_PROVIDERS: Sequence[ProviderFn] = (
    _provider_regime,
    _provider_macro,
    _provider_sentiment,
    _provider_toxic,
    _provider_smart_money_kr,
    _provider_sector_rotation,
    _provider_us_spillover,
    _provider_smart_money_us_placeholder,
)


def collect_satellite_intel_metrics(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """전략 브리핑·시너지 판단용 수치."""
    radar = cfg.get("SMART_MONEY_RADAR") or {}
    picks = radar.get("picks", {}) if isinstance(radar, dict) else {}
    if not isinstance(picks, dict):
        picks = {}
    rules = collect_merged_antipattern_rules(cfg)
    return {
        "smart_money_count": len(picks),
        "toxic_rule_count": len(rules),
        "shadow_block_total": _shadow_block_totals(cfg),
        "defcon_level": _defcon_level(cfg),
        "blackhole_count": _blackhole_count(cfg),
        "regime_key": resolve_config_regime_key(cfg),
    }


def _collect_lines(ctx: SatelliteContext) -> List[SatelliteLine]:
    lines: List[SatelliteLine] = []
    for fn in SATELLITE_PROVIDERS:
        try:
            lines.extend(fn(ctx))
        except Exception as ex:
            lines.append(
                SatelliteLine(
                    "GLOBAL",
                    f"▪ 위성 오류({fn.__name__}): {_esc(str(ex)[:80])}",
                    priority=99,
                )
            )
    return sorted(lines, key=lambda x: (x.scope != "GLOBAL", x.scope, x.priority))


def _format_block(title: str, line_texts: List[str]) -> str:
    if not line_texts:
        return ""
    body = "\n".join(line_texts)
    return f"{title}\n{body}\n"


def build_satellite_intel_for_report(
    cfg: Dict[str, Any],
    *,
    market: ReportMarket,
    sentiment_fresh_warn: bool = False,
) -> str:
    """
    리포트 [1/9] lead-in용 위성망 HTML.
    GLOBAL + 해당 market(KR 또는 US) 블록.
    """
    ctx = SatelliteContext(
        cfg=cfg,
        report_market=market,
        sentiment_fresh_warn=sentiment_fresh_warn,
    )
    all_lines = _collect_lines(ctx)

    global_lines = [ln.text for ln in all_lines if ln.scope == "GLOBAL"]
    mkt_lines = [ln.text for ln in all_lines if ln.scope == market]

    parts = [
        "\n🛰️ <b>[팩토리 위성망 통합 첩보]</b>\n",
        f"📅 {_esc(_today_kst_header())} · 시장 <b>{market}</b>\n",
    ]
    if global_lines:
        parts.append(_format_block("━━ 🌐 공통 ━━", global_lines))
    icon = "🇰🇷" if market == "KR" else "🇺🇸"
    label = "한국" if market == "KR" else "미국"
    if mkt_lines:
        parts.append(_format_block(f"━━ {icon} {label} ━━", mkt_lines))

    one_liner = _build_one_liner(ctx, cfg)
    if one_liner:
        parts.append(f"💡 <i>{_esc(one_liner)}</i>\n")

    text = "".join(parts)
    if len(text) > _MAX_BRIEF_CHARS:
        text = text[: _MAX_BRIEF_CHARS - 40] + "\n<i>…(위성망 요약 생략)</i>\n"
    return text


def _build_one_liner(ctx: SatelliteContext, cfg: Dict[str, Any]) -> str:
    m = collect_satellite_intel_metrics(cfg)
    rk = m.get("regime_key", "UNKNOWN")
    parts = [f"레짐 {rk}"]
    radar = cfg.get("SMART_MONEY_RADAR") or {}
    picks = radar.get("picks", {}) if isinstance(radar, dict) else {}
    if ctx.report_market == "KR" and isinstance(picks, dict) and picks:
        top, _ = aggregate_smart_money_themes(picks, max_themes=1)
        if top:
            parts.append(f"KR 수급 집중 {top[0][0]}")
    if ctx.report_market == "US":
        try:
            from sector_spillover_refresh import resolve_us_spillover_display

            sp = resolve_us_spillover_display(cfg)
            if sp and sp not in ("데이터 없음", "분석중"):
                parts.append(f"US 주도 {sp[:24]}")
        except Exception:
            pass
    return " · ".join(parts)


def build_strategy_insight_html(cfg: Dict[str, Any]) -> str:
    """AI 관제탑 전략 브리핑 (기존 휴리스틱, 메트릭 SSOT 사용)."""
    m = collect_satellite_intel_metrics(cfg)
    defcon = m["defcon_level"]
    sm = m["smart_money_count"]
    toxic = m["toxic_rule_count"]
    bh = m["blackhole_count"]

    strategy_insight = "\n💡 <b>[AI 관제탑 전략 브리핑]</b>\n"
    if defcon <= 2:
        strategy_insight += (
            "🚨 <b>[폭풍 전야]</b> 거시경제(채권/원자재) 붕괴 시그널이 감지되었습니다. "
            "스나이퍼 신규 매수를 전면 중단하고, 현금 비중을 극대화하십시오.\n"
        )
    elif defcon >= 4 and sm >= 5:
        strategy_insight += (
            "🚀 <b>[골디락스 공격]</b> 거시경제가 안정적이며 세력 수급이 강합니다. "
            "공격적인 롱(Long) 포지션 베팅을 권장합니다.\n"
        )
    elif toxic >= 100 or bh >= 10:
        strategy_insight += (
            "🕳️ <b>[숏 타격 기회]</b> 시장 내부에 독성 참사주가 무더기로 쌓이고 있습니다. "
            "인버스(숏) 베팅을 통한 시장 중립(Market Neutral) 방어망을 가동하십시오.\n"
        )
    else:
        strategy_insight += (
            "⚖️ <b>[관망 및 선별]</b> 시장 방향성이 혼조세입니다. 스나이퍼의 타점 기준을 엄격하게 높이고, "
            "확실한 개별주 장세에만 짧게 대응하십시오.\n"
        )
    return strategy_insight

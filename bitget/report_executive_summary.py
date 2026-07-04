"""
[최종 요약: 1분 브리핑] — Bitget 코인 전용.

주식 report_executive_summary.py 구조를 코인에 이식:
- MetaGovernor · Treasury · 국면(BTC 기반) → C-Level 인간어 3단 요약.
- 일일 브리핑: daily_audit 종합 리포트 말미에 부착.
- 주간 브리핑: weekly_flow_master 말미에 부착.

입력 SSOT (기존 데이터·로직 비파괴):
  - bitget_meta_governor_state.json: META_CHANGELOG, META_GROUP_KELLY_MULT, META_REGIME_KEY
  - bitget_system_config.json: DYNAMIC_KELLY_RISK, DYNAMIC_SUPERNOVA_CUTOFF 등
"""
from __future__ import annotations

import html
from typing import Any, Mapping, Optional

_REGIME_KO = {
    "BULL": ("BULL", "공격"),
    "BEAR": ("BEAR", "방어"),
    "SIDEWAYS": ("SIDEWAYS", "횡보·중립"),
    "HIGH_VOL": ("HIGH_VOL", "고변동성 방어"),
    "CHOP": ("SIDEWAYS", "횡보·중립"),
    "WHIPSAW": ("SIDEWAYS", "횡보·중립"),
    "UNKNOWN": ("UNKNOWN", "관측 중"),
}


def _esc(s: Any) -> str:
    return html.escape(str(s) if s is not None else "", quote=False)


def _short_group(name: str, *, max_len: int = 28) -> str:
    s = str(name or "").strip()
    s = s.replace("_", " ")
    if len(s) > max_len:
        return s[: max_len - 1] + "…"
    return s or "그룹"


def _regime_parts(regime_key: Any) -> tuple[str, str]:
    rk = str(regime_key or "UNKNOWN").strip().upper()
    if rk in ("CHOP", "WHIPSAW"):
        rk = "SIDEWAYS"
    label, stance = _REGIME_KO.get(rk, _REGIME_KO["UNKNOWN"])
    return label, stance


def _coerce_mult_map(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in raw.items():
        try:
            out[str(k)] = float(v)
        except (TypeError, ValueError):
            pass
    return out


def _latest_changelog_entry(
    meta: Mapping[str, Any], key: str,
) -> Optional[dict[str, Any]]:
    log = meta.get("META_CHANGELOG") or []
    if not isinstance(log, list):
        return None
    for entry in reversed(log):
        if isinstance(entry, dict) and str(entry.get("key") or "") == key:
            return entry
    return None


def _kelly_mult_moves(
    mult_map: Mapping[str, Any],
    *, boost_floor: float = 1.02, cut_ceil: float = 0.98, top_n: int = 2,
) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
    m = _coerce_mult_map(mult_map)
    boosts = sorted(
        [(k, v) for k, v in m.items() if v >= boost_floor],
        key=lambda x: (-x[1], x[0]),
    )
    cuts = sorted(
        [(k, v) for k, v in m.items() if v <= cut_ceil],
        key=lambda x: (x[1], x[0]),
    )
    return boosts[:top_n], cuts[:top_n]


def _kelly_delta_from_changelog(meta: Mapping[str, Any]) -> list[tuple[str, float, float]]:
    entry = _latest_changelog_entry(meta, "META_GROUP_KELLY_MULT")
    if not entry:
        return []
    old_m = _coerce_mult_map(entry.get("old"))
    new_m = _coerce_mult_map(entry.get("new"))
    keys = sorted(set(old_m) | set(new_m))
    deltas: list[tuple[str, float, float]] = []
    for k in keys:
        ov = float(old_m.get(k, 1.0))
        nv = float(new_m.get(k, 1.0))
        if abs(ov - nv) < 1e-9:
            continue
        if abs(ov - 1.0) <= 0.02 and abs(nv - 1.0) <= 0.02:
            continue
        deltas.append((k, ov, nv))
    deltas.sort(key=lambda x: abs(x[2] - x[1]), reverse=True)
    return deltas


def _rel_pct_change(old: float, new: float) -> int:
    if abs(old) < 1e-9:
        return 0
    return int(round((new - old) / abs(old) * 100))


def _market_regime_sentence(meta: Mapping[str, Any]) -> str:
    label, stance = _regime_parts(meta.get("META_REGIME_KEY"))
    return (
        f"현재 시스템이 파악한 코인 국면은 <b>{_esc(label)}</b>({stance})입니다. "
        f"BTC EMA200·브레드스·ATR 기반 다이내믹 레짐 판단."
    )


def _actions_sentence(meta: Mapping[str, Any]) -> str:
    parts: list[str] = []
    deltas = _kelly_delta_from_changelog(meta)
    mult_now = meta.get("META_GROUP_KELLY_MULT") or {}
    boosts, cuts = _kelly_mult_moves(mult_now)

    for k, ov, nv in deltas[:3]:
        g = _short_group(k)
        pct = _rel_pct_change(ov, nv)
        if nv < ov:
            parts.append(f"성과·건강도가 저조한 <b>{_esc(g)}</b> 비중을 {abs(pct)}% 축소")
        else:
            parts.append(f"데스매치·Treasury 우위 <b>{_esc(g)}</b>에 자본을 {abs(pct)}% 확대")

    if not parts and cuts:
        k, v = cuts[0]
        parts.append(
            f"리스크 컷라인에 따라 <b>{_esc(_short_group(k))}</b> 켈리 배율을 "
            f"<b>{v:.2f}</b>로 하향 조정"
        )
    if not parts and boosts:
        k, v = boosts[0]
        parts.append(
            f"검증된 <b>{_esc(_short_group(k))}</b> 로직에 켈리 배율 "
            f"<b>{v:.2f}</b>로 가중"
        )

    gk_entry = _latest_changelog_entry(meta, "META_GLOBAL_KELLY_MULT")
    if gk_entry:
        try:
            ov = float(gk_entry.get("old"))
            nv = float(gk_entry.get("new"))
            if abs(ov - nv) > 1e-6:
                direction = "확대" if nv > ov else "축소"
                parts.append(f"글로벌 Treasury 켈리 배율을 {ov:.2f}→{nv:.2f}로 {direction}")
        except (TypeError, ValueError):
            pass

    regime_entry = _latest_changelog_entry(meta, "META_REGIME_KEY")
    if regime_entry:
        old_r, new_r = regime_entry.get("old"), regime_entry.get("new")
        if old_r != new_r and str(new_r or "").upper() != "UNKNOWN":
            parts.append(
                f"국면 레짐을 <b>{_esc(old_r)}</b> → <b>{_esc(new_r)}</b>로 재분류"
            )

    retired = meta.get("META_RETIRED_STRATEGY_IDS") or []
    if isinstance(retired, list) and retired:
        n_new = sum(
            1
            for r in retired[-5:]
            if isinstance(r, dict) and str(r.get("state") or "").upper() == "RETIRED"
        )
        if n_new:
            parts.append(f"레지스트리에서 <b>{n_new}</b>개 전략을 RETIRED(도태) 처리")

    if not parts:
        return (
            "오늘 MetaGovernor·Treasury 사이클에서 눈에 띄는 대규모 자본 재배치는 없었습니다. "
            "기존 튜닝·커트라인을 유지합니다."
        )
    joined = " · ".join(parts)
    if joined.endswith(("습니다", "합니다", "됩니다", "있습니다")):
        return joined + "."
    return joined + "했습니다."


def _stance_sentence(
    meta: Mapping[str, Any], sys_config: Optional[Mapping[str, Any]],
) -> str:
    label, stance = _regime_parts(meta.get("META_REGIME_KEY"))
    cos_raw = (sys_config or {}).get("DYNAMIC_SUPERNOVA_CUTOFF")
    ml_raw = (sys_config or {}).get("DYNAMIC_ML_BOX_CUTOFF")
    try:
        cos_pct = f"{float(cos_raw) * 100:.0f}%" if cos_raw is not None else None
    except (TypeError, ValueError):
        cos_pct = None
    try:
        ml_pct = f"{float(ml_raw) * 100:.0f}%" if ml_raw is not None else None
    except (TypeError, ValueError):
        ml_pct = None

    cutoff_bits: list[str] = []
    if cos_pct:
        cutoff_bits.append(f"초신성 커트 {cos_pct}")
    if ml_pct:
        cutoff_bits.append(f"ML박스 {ml_pct}")
    cutoff_txt = " · ".join(cutoff_bits) if cutoff_bits else "현행 커트라인"

    tone = "완화" if label in ("BULL",) else "보수" if label in ("BEAR", "HIGH_VOL") else "중립"

    boosts, _ = _kelly_mult_moves(meta.get("META_GROUP_KELLY_MULT") or {})
    focus_bit = ""
    if boosts:
        focus_bit = f" · 자본 집중 축: <b>{_esc(_short_group(boosts[0][0]))}</b>"

    return (
        f"다음 코인 스캔·정찰은 <b>{tone}</b>된 {cutoff_txt} 기준으로 진입 후보를 탐색하며, "
        f"현 국면({stance})에 맞춰 포지션 크기를 조절합니다{focus_bit}."
    )


def build_daily_executive_summary_html(
    meta: Mapping[str, Any],
    *,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> str:
    """일일 리포트 말미 [최종 요약: 1분 브리핑] — SPOT/FUTURES 통합."""
    regime_line = _market_regime_sentence(meta)
    actions = _actions_sentence(meta)
    stance = _stance_sentence(meta, sys_config)

    return (
        "\n━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>[최종 요약: 1분 브리핑]</b> 🪙 <b>COIN</b>\n"
        "📊 <b>[시장 &amp; 국면]</b>\n"
        f"{regime_line}\n"
        "⚙️ <b>[오늘의 시스템 조치]</b>\n"
        f"{actions}\n"
        "🎯 <b>[내일의 스탠스]</b>\n"
        f"{stance}\n"
    )


def _changelog_in_window(
    meta: Mapping[str, Any], *, start: str, end: str,
) -> list[dict[str, Any]]:
    log = meta.get("META_CHANGELOG") or []
    if not isinstance(log, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in log:
        if not isinstance(entry, dict):
            continue
        at = str(entry.get("at") or "")[:10]
        if at and start <= at <= end:
            out.append(entry)
    return out


def build_weekly_executive_summary_html(
    meta: Mapping[str, Any],
    sys_config: Mapping[str, Any],
    *,
    week_start: str,
    week_end: str,
    regime_key: str = "UNKNOWN",
    lifecycle_n_retired: int = 0,
    lifecycle_n_cooled: int = 0,
    spot_week_pnl: Optional[float] = None,
    futures_week_pnl: Optional[float] = None,
) -> str:
    """주간 Flow 마스터 리포트 말미 브리핑."""
    label, stance = _regime_parts(regime_key)
    week_entries = _changelog_in_window(meta, start=week_start, end=week_end)
    kelly_week = [
        e for e in week_entries if str(e.get("key") or "") == "META_GROUP_KELLY_MULT"
    ]
    regime_week = [
        e for e in week_entries if str(e.get("key") or "") == "META_REGIME_KEY"
    ]

    regime_line = (
        f"이번 주({week_start}~{week_end}) 종료 시점 국면은 "
        f"<b>{_esc(label)}</b>({stance})입니다."
    )
    pnl_bits: list[str] = []
    if spot_week_pnl is not None:
        pnl_bits.append(f"SPOT 주간 실현손익 {spot_week_pnl:+,.2f} USDT")
    if futures_week_pnl is not None:
        pnl_bits.append(f"FUTURES 주간 실현손익 {futures_week_pnl:+,.2f} USDT")
    if pnl_bits:
        regime_line += " " + " · ".join(pnl_bits) + "."

    action_parts: list[str] = []

    if kelly_week:
        last = kelly_week[-1]
        deltas = _kelly_delta_from_changelog({"META_CHANGELOG": [last]})
        for k, ov, nv in deltas[:3]:
            g = _short_group(k)
            pct = _rel_pct_change(ov, nv)
            verb = "축소" if nv < ov else "확대"
            action_parts.append(f"<b>{_esc(g)}</b> 켈리 {verb} ({abs(pct)}%)")
    elif meta.get("META_GROUP_KELLY_MULT"):
        boosts, cuts = _kelly_mult_moves(meta.get("META_GROUP_KELLY_MULT") or {})
        if cuts:
            action_parts.append(
                f"저성과 그룹 <b>{_esc(_short_group(cuts[0][0]))}</b> 등 "
                f"{len(cuts)}개 축소 구간 유지"
            )
        if boosts:
            action_parts.append(
                f"우위 그룹 <b>{_esc(_short_group(boosts[0][0]))}</b> 등 "
                f"{len(boosts)}개 가중 구간 유지"
            )

    if lifecycle_n_retired:
        action_parts.append(f"레지스트리 <b>{lifecycle_n_retired}</b>건 RETIRED(도태)")
    if lifecycle_n_cooled:
        action_parts.append(f"<b>{lifecycle_n_cooled}</b>전략 COOLED(벤치 대기)")

    if regime_week:
        e = regime_week[-1]
        action_parts.append(
            f"국면 전환 {_esc(e.get('old'))} → {_esc(e.get('new'))}"
        )

    if not action_parts:
        actions = (
            "이번 주 MetaGovernor·커트라인·Treasury에서 구조적 대변동은 제한적이었습니다."
        )
    else:
        actions = " · ".join(action_parts[:6]) + "."

    cos_raw = sys_config.get("DYNAMIC_SUPERNOVA_CUTOFF")
    try:
        cos_pct = f"{float(cos_raw) * 100:.0f}%" if cos_raw is not None else "현행"
    except (TypeError, ValueError):
        cos_pct = "현행"

    stance_text = (
        f"다음 주 스캐너는 주말 관제탑 튜닝(초신성 {cos_pct} 등)과 "
        f"{stance} 국면 프리셋을 반영해 진입 허들을 조정합니다. "
        f"COOLED·RETIRED 전략은 재기동 관측 전까지 배제하고, "
        f"주간 MVP·DNA 롤업 상위 엔진에 자본을 우선 배분합니다."
    )

    return (
        "\n━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>[주간 최종 요약: 1분 브리핑]</b> 🪙\n"
        "📊 <b>[시장 &amp; 국면]</b>\n"
        f"{regime_line}\n"
        "⚙️ <b>[이번 주 시스템 조치]</b>\n"
        f"{actions}\n"
        "🎯 <b>[다음 주 스탠스]</b>\n"
        f"{stance_text}\n"
    )

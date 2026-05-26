"""
V28.0 한미 주도 섹터 스필오버 텔레그램 블록 — Timekeeper + AlignedSpilloverDay.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import pytz

from forward_dual_track_queries import query_latest_closed_trade_date
from report_timekeeper import ReportTimekeeper, ReadSource
from sector_spillover_refresh import map_standard_sector
from spillover_calendar import (
    SpilloverCalendarContext,
    add_norm_day_col,
    dominant_sector_label_for_days,
)


def _sector_row_ok(val: object) -> bool:
    t = str(val).strip()
    if not t or t.lower() in ("nan", "none"):
        return False
    if "유망" in t:
        return False
    if t == "기타/혼합":
        return False
    return True


def _sector_norm_align(lab: str) -> str:
    s = str(lab).strip()
    if s.startswith("캐시·"):
        return s[2:].strip()
    return s


def _is_real_sector(lab: str, *, bad: frozenset[str]) -> bool:
    s = str(lab).strip()
    if not s or s in bad:
        return False
    core = _sector_norm_align(s)
    if not core or core in bad:
        return False
    return True


def _us_label_with_last_good_cache(
    day: str,
    base_label: str,
    sys_config: dict,
    *,
    spillover_fallback_enabled: Callable[[dict], bool],
) -> str:
    if base_label != "데이터 없음":
        return base_label
    if not spillover_fallback_enabled(sys_config):
        return base_label
    lg = sys_config.get("US_SPILLOVER_SECTOR_LAST_GOOD")
    lg_s = str(lg).strip() if lg is not None else ""
    if not lg_s or lg_s in ("분석중", "NONE"):
        return base_label
    asof = str(sys_config.get("US_SPILLOVER_SECTOR_AS_OF") or "").strip()[:10]
    if asof and asof > day:
        return base_label
    ms = map_standard_sector(lg_s)
    if not ms or ms == "기타/혼합":
        return base_label
    if asof == day:
        return f"캐시·{ms}"
    return f"캐시·{ms}({asof[5:] if len(asof) >= 10 else asof})"


def build_v28_spillover_section(
    *,
    open_db_ro: Callable[[], Any],
    tk_kr: ReportTimekeeper,
    ref_kst: Optional[datetime] = None,
    load_system_config: Callable[[], Dict[str, Any]],
    save_system_config: Callable[[Dict[str, Any]], Any],
    spillover_fallback_enabled: Callable[[dict], bool],
) -> str:
    """
    KR 딥다이브용 V28 HTML 블록.
    open_db_ro: sqlite 연결 팩토리 (report main DB RO).
    """
    kr_tz = pytz.timezone("Asia/Seoul")
    if ref_kst is None:
        ref_kst = datetime.now(kr_tz)
    elif ref_kst.tzinfo is None:
        ref_kst = kr_tz.localize(ref_kst)
    else:
        ref_kst = ref_kst.astimezone(kr_tz)

    _rs: ReadSource = tk_kr.read_source  # type: ignore[assignment]
    conn = open_db_ro()
    try:
        wm_us = query_latest_closed_trade_date(conn, "US")
        tk_us = ReportTimekeeper.for_market(
            "US",
            rolling_days=tk_kr.rolling_days,
            ref_kst=ref_kst,
            db_watermark_exit=wm_us,
            read_source=_rs,
        )
        cal = SpilloverCalendarContext.from_timekeepers(tk_kr, tk_us, window_days=7)
        cutoff = cal.query_cutoff(padding_days=30)
        us_df = pd.read_sql(
            """
            SELECT entry_date, sector FROM forward_trades
            WHERE market='US'
              AND substr(IFNULL(entry_date,''),1,10) >= ?
              AND substr(IFNULL(entry_date,''),1,10) <= ?
            """,
            conn,
            params=(cutoff, tk_us.session_anchor),
        )
        kr_df = pd.read_sql(
            """
            SELECT entry_date, sector FROM forward_trades
            WHERE market='KR'
              AND substr(IFNULL(entry_date,''),1,10) >= ?
              AND substr(IFNULL(entry_date,''),1,10) <= ?
            """,
            conn,
            params=(cutoff, tk_kr.session_anchor),
        )
    finally:
        conn.close()

    sys_config = load_system_config()
    if not isinstance(sys_config, dict):
        sys_config = {}

    us_raw = add_norm_day_col(us_df)
    kr_raw = add_norm_day_col(kr_df)
    _bad = frozenset({"데이터 없음", "필터 탈락"})

    out = "\n🌐 <b>[V28.0 한미 주도 섹터 스필오버(전이) 팩트 체크]</b>\n"
    out += (
        f"📎 리포트일 KST <b>{cal.calendar_today_kst}</b> · "
        f"KR앵커 <b>{cal.kr_anchor}</b> · US앵커(ET) <b>{cal.us_anchor}</b>\n"
    )
    wm = cal.us_db_watermark or "—"
    out += (
        f"📊 US청산워터마크 <b>{wm}</b> · US lag <b>{cal.us_lag_business_days}</b>영업일\n"
    )

    timeline_for_ssot: List[dict] = []

    if us_raw.empty and kr_raw.empty:
        out += (
            f"⚠️ 스필오버 분석: 진입 표본 없음 (조회 ≥ {cutoff}, "
            f"KR≤{tk_kr.session_anchor}, US≤{tk_us.session_anchor}).\n"
        )
    else:
        out += (
            "▪️ <b>최근 7일 정렬 타임라인 (KR KST 세션 · US ET 짝지음):</b>\n"
        )
        for row in cal.aligned_days:
            lab_kr = dominant_sector_label_for_days(
                row.kr_trade_dates, kr_raw, map_standard_sector, _sector_row_ok
            )
            lab_us = dominant_sector_label_for_days(
                row.us_trade_dates, us_raw, map_standard_sector, _sector_row_ok
            )
            lab_us = _us_label_with_last_good_cache(
                row.kst_label,
                lab_us,
                sys_config,
                spillover_fallback_enabled=spillover_fallback_enabled,
            )
            us_disp = str(lab_us)[:14]
            kr_disp = str(lab_kr)[:12]
            out += (
                f" [{row.kst_label[5:]}] 🇺🇸 {us_disp} (ET{row.us_session[5:]}) "
                f"➔ 🇰🇷 {kr_disp}\n"
            )
            aligned = _is_real_sector(lab_us, bad=_bad) and _is_real_sector(
                lab_kr, bad=_bad
            ) and (_sector_norm_align(lab_us) == _sector_norm_align(lab_kr))
            timeline_for_ssot.append(
                {
                    "d": row.kst_label,
                    "us": lab_us,
                    "kr": lab_kr,
                    "us_session": row.us_session,
                    "aligned": bool(aligned),
                }
            )

        align_labels = cal.recent_kr_labels(3)
        by_d = {r["d"]: r for r in timeline_for_ssot}
        align_count = sum(
            1 for ad in align_labels if by_d.get(ad, {}).get("aligned")
        )
        observe_mult = min(1.5, 1.0 + 0.1 * float(align_count))
        out += (
            f"\n◽ <b>[관측·점수 미반영]</b> 최근3 KR세션 정렬일치 <b>{align_count}</b>회 "
            f"→ 가상배수 <b>{observe_mult:.1f}x</b> "
            f"<i>(엔진 점수·가중치에 적용 없음)</i>\n"
        )

        try:
            payload = {
                "updated_at": datetime.now(kr_tz).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "anchor_end": cal.kr_anchor,
                "us_anchor": cal.us_anchor,
                "align_3d": int(align_count),
                "observe_multiplier": float(observe_mult),
                "timeline_T6_T": list(timeline_for_ssot),
            }
            cfg_ob = dict(sys_config)
            cfg_ob["SPILLOVER_OBSERVE_SSOT"] = payload
            save_system_config(cfg_ob)
        except Exception as se:
            out += f"⚠️ SPILLOVER_OBSERVE_SSOT 저장 실패(관측만): {se}\n"

    current_spillover = sys_config.get("US_SPILLOVER_SECTOR", "NONE")
    if (
        current_spillover is None
        or str(current_spillover).strip() == ""
        or str(current_spillover).strip().upper() == "NONE"
    ):
        mapped_spillover = "NONE"
    else:
        mapped_spillover = map_standard_sector(current_spillover)

    if mapped_spillover != "NONE" and mapped_spillover != "기타/혼합":
        out += "\n💡 <b>[관제탑 스필로버 지령]</b>\n"
        out += (
            f"현재 미국장에서 검증된 강력한 주도 섹터는 <b>[{mapped_spillover}]</b>입니다. "
            "한국장 스나이퍼는 해당 섹터 포착 시 켈리 비중을 1.5배로 증폭하여 "
            "선취매(Spillover) 시너지를 극대화하고 있습니다.\n"
        )
    else:
        out += (
            "\n💡 <b>[관제탑 스필오버 지령]</b>\n"
            "현재 미국장에서 전이될 만한 뚜렷한 고수익 주도 섹터가 발견되지 않아, "
            "스필오버 가중치를 대기 중입니다.\n"
        )
    return out

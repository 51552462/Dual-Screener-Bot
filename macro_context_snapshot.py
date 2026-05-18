"""
거시 컨텍스트 SSOT: 스필오버 섹터 · 일일 센티 · 그림자 성과를 한 스키마로 정규화.
스캐너는 위성 모듈을 직접 호출하지 않고 본 모듈만 사용한다(읽기 전용 스냅샷).
소프트 가중치는 원시 부분점수가 아닌 최종 total_score 에 곱하는 클램프 배수만 허용.
"""
from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime
from typing import Any, Mapping, MutableMapping, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore[misc, assignment]

# ---------------------------------------------------------------------------
# 설정 키 (system_config)
# ---------------------------------------------------------------------------
ENABLE_MACRO_SYNERGY_WEIGHTING_KEY = "ENABLE_MACRO_SYNERGY_WEIGHTING"
MACRO_SYNERGY_FEAR_THRESHOLD_KEY = "MACRO_SYNERGY_FEAR_THRESHOLD"
MACRO_SYNERGY_FEAR_MULT_KEY = "MACRO_SYNERGY_FEAR_MULT"
MACRO_SYNERGY_SPILLOVER_MAX_BONUS_KEY = "MACRO_SYNERGY_SPILLOVER_MAX_BONUS"
MACRO_SYNERGY_SHADOW_DELTA_THRESHOLD_KEY = "MACRO_SYNERGY_SHADOW_DELTA_THRESHOLD"
MACRO_SYNERGY_SHADOW_MAX_BONUS_KEY = "MACRO_SYNERGY_SHADOW_MAX_BONUS"
MACRO_SYNERGY_MULT_MIN_KEY = "MACRO_SYNERGY_MULT_MIN"
MACRO_SYNERGY_MULT_MAX_KEY = "MACRO_SYNERGY_MULT_MAX"


def _kst_today_str() -> str:
    if ZoneInfo is None:
        return datetime.now().strftime("%Y-%m-%d")
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")


def _spillover_fallback_enabled(cfg: Mapping[str, Any]) -> bool:
    v = cfg.get("ENABLE_SPILLOVER_FALLBACK", True)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    return True


def _resolve_effective_us_spillover_sector(cfg: Mapping[str, Any]) -> str:
    """리포트용 auto_forward 와 동일 우선순위의 '비교용' 문자열(캐시 접미사 제거)."""
    try:
        from cross_market_ssot import load_cross_market_ssot, MODE_US_ONLINE

        ssot = load_cross_market_ssot(dict(cfg) if isinstance(cfg, dict) else {})
        if ssot.get("mode") == MODE_US_ONLINE:
            raw = str(ssot.get("kr_sector_std") or ssot.get("us_sector_raw") or "").strip()
            if raw and raw not in ("분석중", "NONE"):
                return _strip_cache_suffix(raw)
    except Exception:
        pass

    if not _spillover_fallback_enabled(cfg):
        raw = cfg.get("US_SPILLOVER_SECTOR")
        cur = str(raw).strip() if raw is not None else ""
        if cur and cur != "분석중":
            return _strip_cache_suffix(cur)
        return ""

    raw = cfg.get("US_SPILLOVER_SECTOR")
    cur = str(raw).strip() if raw is not None else ""
    if cur and cur != "분석중":
        return _strip_cache_suffix(cur)

    lg = cfg.get("US_SPILLOVER_SECTOR_LAST_GOOD")
    lg_s = str(lg).strip() if lg is not None else ""
    return _strip_cache_suffix(lg_s) if lg_s else ""


def _strip_cache_suffix(s: str) -> str:
    t = str(s).strip()
    if " (캐시 기준:" in t:
        t = t.split(" (캐시 기준:")[0].strip()
    return t


def _norm_sector_token(s: str) -> str:
    t = re.sub(r"\s+", "", str(s).strip().lower())
    t = re.sub(r"[\[\]()]", "", t)
    return t


def _sectors_soft_match(spill: str, stock_sector: str) -> bool:
    a = _norm_sector_token(spill)
    b = _norm_sector_token(stock_sector)
    if not a or not b:
        return False
    if a in ("분석중", "분석대기"):
        return False
    if a == b or a in b or b in a:
        return True
    # 토큰 교집합 (복합 섹터명)
    ta = {x for x in re.split(r"[,/&·\|]", a) if len(x) > 1}
    tb = {x for x in re.split(r"[,/&·\|]", b) if len(x) > 1}
    if ta and tb and (ta & tb):
        return True
    return False


def _read_latest_daily_sentiment() -> tuple[Optional[float], Optional[str], str]:
    """news_data.sqlite daily_sentiment — 스캐너는 sentiment_miner 를 import 하지 않고 경로만 동일 규약 사용."""
    from news_data_paths import news_db_path

    path = news_db_path()
    if not os.path.isfile(path):
        return None, None, path
    try:
        conn = sqlite3.connect(path, timeout=15)
        try:
            cur = conn.execute(
                "SELECT date, sentiment_score FROM daily_sentiment ORDER BY date DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return None, None, path
            d, sc = row[0], row[1]
            try:
                sf = float(sc) if sc is not None else None
            except (TypeError, ValueError):
                sf = None
            return sf, str(d) if d is not None else None, path
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return None, None, path


def _shadow_smart_delta(cfg: Mapping[str, Any]) -> tuple[Optional[float], Optional[str]]:
    sp = cfg.get("SHADOW_PERFORMANCE")
    if not isinstance(sp, dict):
        return None, None
    updated = sp.get("updated_at")
    sm = sp.get("smart_money_buff") or {}
    if not isinstance(sm, dict):
        return None, str(updated) if updated else None
    dlt = sm.get("delta_pct_pts")
    try:
        dlt_f = float(dlt) if dlt is not None else None
    except (TypeError, ValueError):
        dlt_f = None
    return dlt_f, str(updated) if updated else None


def build_macro_context_snapshot(cfg: Mapping[str, Any]) -> dict[str, Any]:
    """
    SSOT 스냅샷(읽기 전용). 스캐너·리포트가 동일 구조를 참조한다.
    """
    spill_eff = _resolve_effective_us_spillover_sector(cfg)
    raw_spill = cfg.get("US_SPILLOVER_SECTOR")
    sent_score, sent_date, sent_db = _read_latest_daily_sentiment()
    shadow_dlt, shadow_updated = _shadow_smart_delta(cfg)

    return {
        "schema_version": 1,
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "spillover": {
            "us_spillover_sector_raw": str(raw_spill).strip() if raw_spill is not None else "",
            "us_spillover_sector_effective": spill_eff,
            "fallback_enabled": _spillover_fallback_enabled(cfg),
            "last_good": str(cfg.get("US_SPILLOVER_SECTOR_LAST_GOOD") or "").strip(),
            "as_of": str(cfg.get("US_SPILLOVER_SECTOR_AS_OF") or "").strip(),
        },
        "sentiment": {
            "sentiment_score": sent_score,
            "row_date": sent_date,
            "db_path": sent_db,
            "kst_today": _kst_today_str(),
        },
        "shadow": {
            "smart_money_delta_pct_pts": shadow_dlt,
            "performance_updated_at": shadow_updated,
        },
    }


def compact_snapshot_for_dbg(full: Mapping[str, Any]) -> dict[str, Any]:
    """dbg·텔레그램 부하 완화용 얕은 복사."""
    sp = full.get("spillover") if isinstance(full.get("spillover"), dict) else {}
    se = full.get("sentiment") if isinstance(full.get("sentiment"), dict) else {}
    sh = full.get("shadow") if isinstance(full.get("shadow"), dict) else {}
    return {
        "schema_version": full.get("schema_version", 1),
        "built_at": full.get("built_at"),
        "spillover": {
            "us_spillover_sector_effective": sp.get("us_spillover_sector_effective", ""),
        },
        "sentiment": {
            "sentiment_score": se.get("sentiment_score"),
            "row_date": se.get("row_date"),
        },
        "shadow": {
            "smart_money_delta_pct_pts": sh.get("smart_money_delta_pct_pts"),
            "performance_updated_at": sh.get("performance_updated_at"),
        },
    }


def compute_clamped_synergy_multiplier(
    snapshot: Mapping[str, Any],
    stock_sector: str,
    cfg: Mapping[str, Any],
) -> tuple[float, dict[str, Any]]:
    """
    최종 점수에만 곱하는 클램프 배수. ENABLE_MACRO_SYNERGY_WEIGHTING 가 False 이면 항상 1.0.
    """
    synergy_on = bool(cfg.get(ENABLE_MACRO_SYNERGY_WEIGHTING_KEY, False))
    meta: dict[str, Any] = {
        "synergy_enabled": synergy_on,
        "observe_only": not synergy_on,
        "components": {},
    }
    if not synergy_on:
        return 1.0, meta

    mult = 1.0
    fear_th = float(cfg.get(MACRO_SYNERGY_FEAR_THRESHOLD_KEY, 35.0))
    fear_mult = float(cfg.get(MACRO_SYNERGY_FEAR_MULT_KEY, 0.9))
    spill_cap = float(cfg.get(MACRO_SYNERGY_SPILLOVER_MAX_BONUS_KEY, 0.05))
    sh_th = float(cfg.get(MACRO_SYNERGY_SHADOW_DELTA_THRESHOLD_KEY, 2.0))
    sh_bonus = float(cfg.get(MACRO_SYNERGY_SHADOW_MAX_BONUS_KEY, 0.02))
    mmin = float(cfg.get(MACRO_SYNERGY_MULT_MIN_KEY, 0.85))
    mmax = float(cfg.get(MACRO_SYNERGY_MULT_MAX_KEY, 1.08))

    se = snapshot.get("sentiment") if isinstance(snapshot.get("sentiment"), dict) else {}
    sc = se.get("sentiment_score")
    try:
        sc_f = float(sc) if sc is not None else None
    except (TypeError, ValueError):
        sc_f = None
    if sc_f is not None and (sc_f <= fear_th or sc_f < 0):
        mult *= fear_mult
        meta["components"]["sentiment_fear"] = {"score": sc_f, "mult": fear_mult}

    sp = snapshot.get("spillover") if isinstance(snapshot.get("spillover"), dict) else {}
    spill = str(sp.get("us_spillover_sector_effective") or "").strip()
    if spill and _sectors_soft_match(spill, stock_sector):
        mult *= 1.0 + min(max(spill_cap, 0.0), 0.05)
        meta["components"]["spillover_sector_align"] = {"sector": spill, "cap": spill_cap}

    sh = snapshot.get("shadow") if isinstance(snapshot.get("shadow"), dict) else {}
    dlt = sh.get("smart_money_delta_pct_pts")
    try:
        dlt_f = float(dlt) if dlt is not None else None
    except (TypeError, ValueError):
        dlt_f = None
    if dlt_f is not None and dlt_f >= sh_th:
        mult *= 1.0 + min(max(sh_bonus, 0.0), 0.05)
        meta["components"]["shadow_smart_money_tailwind"] = {"delta_pct_pts": dlt_f, "bonus": sh_bonus}

    mult = max(mmin, min(mmax, mult))
    meta["clamped_multiplier"] = mult
    return mult, meta


def _patch_v11_total_score_line(v11: str, new_score: float) -> str:
    if not v11:
        return v11
    return re.sub(
        r"(🔹 시스템 총점:\s*)(\d+\.?\d*)(\s*/\s*100점)",
        lambda m: f"{m.group(1)}{new_score:.1f}{m.group(3)}",
        v11,
        count=1,
    )


def attach_macro_context_to_dbg(
    dbg: MutableMapping[str, Any],
    cfg: Mapping[str, Any],
) -> None:
    """compute 종료 직전: 스냅샷 부착 + micro 점수 고정(가중치는 섹터 확정 후 finalize)."""
    if dbg.get("macro_context_attached"):
        return
    snap = build_macro_context_snapshot(cfg)
    dbg["macro_context_snapshot"] = compact_snapshot_for_dbg(snap)
    dbg["macro_context_snapshot_full"] = snap
    dbg["score_micro"] = float(dbg.get("score", 0.0) or 0.0)
    dbg["macro_context_attached"] = True


def finalize_macro_synergy_on_dbg(
    dbg: MutableMapping[str, Any],
    cfg: Mapping[str, Any],
    stock_sector: str,
) -> None:
    """
    섹터 문자열 확정 후 1회: 클램프 배수로 score 만 보정하고 v11 첫 총점 줄을 동기화.
    synergy OFF 이면 mult=1.0 이고 score_micro 와 동일.
    """
    if dbg.get("macro_synergy_finalized"):
        return
    cfg_d = dict(cfg) if not isinstance(cfg, dict) else cfg
    full_snap = dbg.get("macro_context_snapshot_full")
    if not isinstance(full_snap, dict):
        full_snap = build_macro_context_snapshot(cfg_d)
        dbg["macro_context_snapshot_full"] = full_snap
        dbg["macro_context_snapshot"] = compact_snapshot_for_dbg(full_snap)

    micro = float(dbg.get("score_micro", dbg.get("score", 0.0) or 0.0))
    dbg["score_micro"] = micro
    mult, meta = compute_clamped_synergy_multiplier(full_snap, str(stock_sector or ""), cfg_d)
    final = min(100.0, max(0.0, micro * mult))
    dbg["score"] = final
    dbg["macro_synergy_multiplier"] = mult
    dbg["macro_synergy_meta"] = meta
    vc = dbg.get("v11_comment")
    if isinstance(vc, str) and vc:
        dbg["v11_comment"] = _patch_v11_total_score_line(vc, final)
    dbg["macro_synergy_finalized"] = True
    dbg.pop("macro_context_snapshot_full", None)

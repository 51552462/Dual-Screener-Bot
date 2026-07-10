"""
Mega-Trend Ignition (1번) — 메가 트렌드 섹터 자율 감지 SSOT.

조건 (AND):
  · 섹터 거래대금 점유율 ≥ turnover_share_min (기본 30%)
  · kr_flow_factor 섹터 순매수 Z-Score ≥ flow_z_min (기본 2.0)

산출: system_config `MEGA_TREND_SECTOR` — portfolio_risk_overlay [2~3번] 소비 예정.
"""
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

import pandas as pd

MEGA_TREND_CONFIG_KEY = "MEGA_TREND_SECTOR"

# 내부 킬 이후 점화 재활성 차단·상태 보존 (P0)
_PRESERVE_ALWAYS_KEYS = (
    "internal_diagnostics",
    "toxic_watch",
)
_PRESERVE_KILL_KEYS = (
    "correlation_forgiveness_revoked",
    "forgiveness_revoked_at",
    "forgiveness_revoked_reason",
    "toxic_kill_at",
    "toxic_kill_reason",
    "internal_momentum_kill_at",
    "internal_momentum_kill_reason",
    "climax_kill_at",
    "climax_reason",
    "climax_exit_mode",
    "climax_verdict",
)


def _resolve_latest_mega_trend_kill_at(prev: Mapping[str, Any]) -> str:
    """Toxic · Internal momentum · Climax 킬 중 가장 최근 일자."""
    dates: List[str] = []
    for key in ("toxic_kill_at", "internal_momentum_kill_at", "climax_kill_at"):
        d = str(prev.get(key) or "")[:10]
        if d:
            dates.append(d)
    return max(dates) if dates else ""


def _had_mega_trend_kill(prev: Mapping[str, Any]) -> bool:
    return bool(_resolve_latest_mega_trend_kill_at(prev))


def kill_cooldown_days() -> int:
    try:
        return max(1, int(os.environ.get("MEGA_TREND_KILL_COOLDOWN_DAYS", "5")))
    except (TypeError, ValueError):
        return 5


def assess_toxic_kill_cooldown(
    prev: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    내부·외부 킬(Toxic / Internal momentum / Climax) 이후 재점화 쿨다운.
    외부 거래대금이 여전히 높아도 킬 존중.
    """
    kill_at = _resolve_latest_mega_trend_kill_at(prev)
    if not kill_at:
        return {"active": False, "reason": "no_mega_trend_kill"}

    now = now or datetime.now()
    try:
        kill_dt = datetime.strptime(kill_at, "%Y-%m-%d")
    except ValueError:
        return {"active": False, "reason": "invalid_kill_at"}

    elapsed = (now.date() - kill_dt.date()).days
    cooldown = kill_cooldown_days()
    if elapsed < cooldown:
        return {
            "active": True,
            "reason": "mega_trend_kill_cooldown",
            "kill_at": kill_at,
            "days_elapsed": elapsed,
            "days_remaining": cooldown - elapsed,
            "cooldown_days": cooldown,
        }
    return {
        "active": False,
        "reason": "cooldown_expired",
        "kill_at": kill_at,
        "days_elapsed": elapsed,
        "cooldown_days": cooldown,
    }


def _copy_preserved_fields(
    prev: Mapping[str, Any],
    state: Dict[str, Any],
    keys: Sequence[str],
) -> None:
    for key in keys:
        if key in prev and prev[key] is not None:
            state[key] = prev[key]


def merge_preserved_mega_trend_state(
    prev: Mapping[str, Any],
    state: Dict[str, Any],
    *,
    cooldown: Mapping[str, Any],
) -> Dict[str, Any]:
    """일일 훅 선행 블록(진단·킬)이 점화에서 지워지지 않도록 merge."""
    _copy_preserved_fields(prev, state, _PRESERVE_ALWAYS_KEYS)

    had_any_kill = _had_mega_trend_kill(prev)
    cooldown_active = bool(cooldown.get("active"))

    if cooldown_active:
        _copy_preserved_fields(prev, state, _PRESERVE_KILL_KEYS)
        state["active"] = False
        state["rotation_advantage_active"] = False
        state["ignition_blocked_reason"] = (
            f"mega_trend_kill_cooldown:{cooldown.get('days_remaining', '?')}d"
        )
        if prev.get("deactivated_at"):
            state["deactivated_at"] = prev.get("deactivated_at")
        if prev.get("ignited_at"):
            state["ignited_at"] = prev.get("ignited_at")
    elif had_any_kill and not state.get("active"):
        _copy_preserved_fields(prev, state, _PRESERVE_KILL_KEYS)

    return state


def mega_trend_config() -> Dict[str, Any]:
    def _f(key: str, default: float) -> float:
        try:
            return float(os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    def _i(key: str, default: int) -> int:
        try:
            return int(os.environ.get(key, str(default)))
        except (TypeError, ValueError):
            return default

    return {
        "turnover_share_min": _f("MEGA_TREND_TURNOVER_SHARE_MIN", 0.30),
        "flow_z_min": _f("MEGA_TREND_FLOW_Z_MIN", 2.0),
        "flow_window_days": _i("MEGA_TREND_FLOW_WINDOW_DAYS", 5),
        "flow_history_days": _i("MEGA_TREND_FLOW_HISTORY_DAYS", 60),
        "min_sector_stocks": _i("MEGA_TREND_MIN_SECTOR_STOCKS", 3),
    }


def _recent_trade_ymd() -> Optional[str]:
    try:
        from smart_money_tracker import _recent_trade_dates_yyyymmdd

        dates = _recent_trade_dates_yyyymmdd(1)
        return dates[-1] if dates else None
    except Exception:
        return None


def _fetch_market_turnover_block(trade_ymd: str, market: str) -> pd.DataFrame:
    """pykrx 일별 전종목 거래대금 + FDR 업종 → 표준 섹터."""
    try:
        from pykrx import stock as krx_stock
    except ImportError:
        return pd.DataFrame()

    from kr_flow_factor import build_kr_code_sector_map
    from sector_taxonomy import map_standard_sector

    try:
        df = krx_stock.get_market_ohlcv_by_ticker(trade_ymd, market=market)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    tv_col = None
    for c in df.columns:
        s = str(c)
        if "거래대금" in s or ("거래" in s and "대금" in s):
            tv_col = c
            break
    if tv_col is None:
        return pd.DataFrame()

    cmap = build_kr_code_sector_map()
    rows: List[Dict[str, Any]] = []
    for code, row in df.iterrows():
        code_s = str(code).zfill(6)
        try:
            tv = float(row.get(tv_col) or 0.0)
        except (TypeError, ValueError):
            tv = 0.0
        if tv <= 0:
            continue
        sec = cmap.get(code_s) or map_standard_sector("", market="KR")
        rows.append({"code": code_s, "sector": sec, "trade_value": tv})
    return pd.DataFrame(rows)


def compute_sector_turnover_concentration(
    trade_ymd: Optional[str] = None,
) -> Dict[str, Any]:
    """
    전 시장 거래대금 대비 섹터별 점유율 — KOSPI+KOSDAQ 합산.
    """
    cfg = mega_trend_config()
    ymd = trade_ymd or _recent_trade_ymd()
    out: Dict[str, Any] = {
        "trade_date": ymd,
        "total_trade_value": 0.0,
        "sectors": {},
        "top_sector": None,
        "top_share": 0.0,
        "reason": "",
    }
    if not ymd:
        out["reason"] = "no_trade_date"
        return out

    parts = []
    for mkt in ("KOSPI", "KOSDAQ"):
        block = _fetch_market_turnover_block(ymd, mkt)
        if not block.empty:
            parts.append(block)
    if not parts:
        out["reason"] = "no_market_data"
        return out

    df = pd.concat(parts, ignore_index=True)
    df = df.groupby("code", as_index=False).agg(
        {"sector": "first", "trade_value": "sum"}
    )
    total = float(df["trade_value"].sum())
    if total <= 0:
        out["reason"] = "zero_total_turnover"
        return out

    grp = df.groupby("sector").agg(
        trade_value=("trade_value", "sum"),
        n_stocks=("code", "count"),
    )
    sectors: Dict[str, Dict[str, Any]] = {}
    top_sec = None
    top_share = 0.0
    min_n = int(cfg["min_sector_stocks"])
    for sec, row in grp.iterrows():
        tv = float(row["trade_value"])
        share = tv / total
        n = int(row["n_stocks"])
        if n < min_n:
            continue
        sectors[str(sec)] = {
            "trade_value": round(tv, 2),
            "share": round(share, 4),
            "share_pct": round(share * 100.0, 2),
            "n_stocks": n,
        }
        if share > top_share:
            top_share = share
            top_sec = str(sec)

    out["total_trade_value"] = round(total, 2)
    out["sectors"] = sectors
    out["top_sector"] = top_sec
    out["top_share"] = round(top_share, 4)
    out["reason"] = "computed"
    return out


def detect_mega_trend_sectors(
    conn: Optional[Any] = None,
    *,
    trade_ymd: Optional[str] = None,
) -> Dict[str, Any]:
    """
    [1번] 메가 트렌드 점화 후보 섹터 탐지 — 거래대금 쏠림 AND 수급 Z-Score.
    """
    from kr_flow_factor import compute_sector_flow_zscore

    cfg = mega_trend_config()
    turnover = compute_sector_turnover_concentration(trade_ymd=trade_ymd)
    share_min = float(cfg["turnover_share_min"])
    z_min = float(cfg["flow_z_min"])

    ignited: List[Dict[str, Any]] = []
    candidates_checked = 0

    for sector, tdata in (turnover.get("sectors") or {}).items():
        share = float(tdata.get("share") or 0.0)
        if share < share_min:
            continue
        candidates_checked += 1
        flow = compute_sector_flow_zscore(
            sector,
            conn=conn,
            window_days=int(cfg["flow_window_days"]),
            history_days=int(cfg["flow_history_days"]),
        )
        z = flow.get("z_score")
        if z is None:
            continue
        if float(z) < z_min:
            continue
        ignited.append(
            {
                "sector": sector,
                "turnover_share": share,
                "turnover_share_pct": round(share * 100.0, 2),
                "trade_value": tdata.get("trade_value"),
                "n_stocks": tdata.get("n_stocks"),
                "flow_z": float(z),
                "flow_window_krw": flow.get("window_krw"),
                "flow_detail": flow,
            }
        )

    ignited.sort(key=lambda x: (x["turnover_share"], x["flow_z"]), reverse=True)

    return {
        "ignited": bool(ignited),
        "sectors": [x["sector"] for x in ignited],
        "primary_sector": ignited[0]["sector"] if ignited else None,
        "primary_detail": ignited[0] if ignited else None,
        "candidates_checked": candidates_checked,
        "thresholds": {
            "turnover_share_min": share_min,
            "flow_z_min": z_min,
        },
        "turnover_snapshot": turnover,
        "ignition_details": ignited,
        "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def resolve_kr_code_sector(code: object, sector_hint: Optional[object] = None) -> str:
    """종목·섹터 힌트 → 표준 섹터 (Mega-Trend 게이트용)."""
    from kr_flow_factor import build_kr_code_sector_map
    from sector_taxonomy import map_standard_sector

    if sector_hint is not None and str(sector_hint).strip():
        mapped = map_standard_sector(sector_hint, market="KR")
        if mapped and mapped not in ("기타/혼합", "미분류(원시)"):
            return mapped
    code_s = str(code or "").zfill(6)
    cmap = build_kr_code_sector_map()
    return cmap.get(code_s) or map_standard_sector("", market="KR")


def mega_trend_unlock_enabled() -> bool:
    raw = os.environ.get("ENABLE_MEGA_TREND_UNLOCK", "1")
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def is_mega_trend_rotation_advantage(
    sector: object,
    config: Optional[Mapping[str, Any]] = None,
) -> bool:
    """
    [2번] MEGA_TREND 언락 시 ROTATION_ADVANTAGE 정당 승인 여부.
    주말 데스매치 PF 검증(ROTATION_ADVANTAGE_ACTIVE)과 독립적으로,
    점화 블록의 rotation_advantage_active + 섹터 일치로 자본 합산 허용.
    """
    if not mega_trend_unlock_enabled():
        return False
    if not isinstance(config, Mapping):
        return False
    block = config.get(MEGA_TREND_CONFIG_KEY)
    if not isinstance(block, Mapping) or not block.get("active"):
        return False
    if block.get("correlation_forgiveness_revoked"):
        return False
    if not block.get("rotation_advantage_active"):
        return False
    return is_mega_trend_sector(sector, config)


def is_mega_trend_sector(sector: object, config: Optional[Mapping[str, Any]] = None) -> bool:
    """활성 MEGA_TREND_SECTOR 여부 (2~3번 게이트 소비용)."""
    from sector_taxonomy import map_standard_sector

    if not isinstance(config, Mapping):
        return False
    block = config.get(MEGA_TREND_CONFIG_KEY)
    if not isinstance(block, Mapping) or not block.get("active"):
        return False
    std = map_standard_sector(sector, market="KR")
    sectors = block.get("sectors") or []
    if std in sectors:
        return True
    return std == block.get("primary_sector")


def load_mega_trend_state(config: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(config, Mapping):
        return {"active": False, "sectors": []}
    block = config.get(MEGA_TREND_CONFIG_KEY)
    return dict(block) if isinstance(block, dict) else {"active": False, "sectors": []}


def refresh_mega_trend_ignition(
    config: Dict[str, Any],
    *,
    save_config_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
    conn: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    smart_money_tracker 일일 잡 말미 — MEGA_TREND_SECTOR 갱신·저장.
    """
    detection = detect_mega_trend_sectors(conn=conn)
    prev = load_mega_trend_state(config)
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cooldown = assess_toxic_kill_cooldown(prev)

    primary = detection.get("primary_detail") or {}
    state: Dict[str, Any] = {
        "active": bool(detection.get("ignited")),
        "sectors": list(detection.get("sectors") or []),
        "primary_sector": detection.get("primary_sector"),
        "turnover_share": primary.get("turnover_share"),
        "turnover_share_pct": primary.get("turnover_share_pct"),
        "flow_z": primary.get("flow_z"),
        "updated_at": now_s,
        "detection": detection,
    }

    if state["active"] and not cooldown.get("active"):
        state["ignited_at"] = now_s
        state["rotation_advantage_active"] = True
        same_sector_continue = (
            prev.get("ignited_at")
            and prev.get("primary_sector") == state["primary_sector"]
            and not _had_mega_trend_kill(prev)
        )
        if same_sector_continue:
            state["ignited_at"] = prev.get("ignited_at")
        elif _had_mega_trend_kill(prev) and cooldown.get("reason") == "cooldown_expired":
            state["post_kill_reignite_at"] = now_s
    elif not state["active"]:
        state["ignited_at"] = prev.get("ignited_at")
        state["deactivated_at"] = prev.get("deactivated_at") or now_s
        state["rotation_advantage_active"] = False
    else:
        state["ignited_at"] = prev.get("ignited_at")
        state["deactivated_at"] = prev.get("deactivated_at") or now_s
        state["rotation_advantage_active"] = False

    state = merge_preserved_mega_trend_state(prev, state, cooldown=cooldown)

    config[MEGA_TREND_CONFIG_KEY] = state
    if save_config_fn:
        save_config_fn(config)

    if state["active"]:
        print(
            f"🔥 [Mega-Trend Ignition] {state['primary_sector']} "
            f"거래대금 {primary.get('turnover_share_pct', 0):.1f}% · "
            f"수급 Z={primary.get('flow_z', 0):.2f} → MEGA_TREND_SECTOR 활성"
        )
    elif cooldown.get("active") and bool(detection.get("ignited")):
        print(
            f"🛡️ [Mega-Trend Ignition] 외부 점화 조건 충족이나 "
            f"내부 킬 쿨다운({cooldown.get('days_remaining')}일 잔여) — 재언락 차단 "
            f"({prev.get('primary_sector')})"
        )
    else:
        top = (detection.get("turnover_snapshot") or {}).get("top_sector")
        top_sh = (detection.get("turnover_snapshot") or {}).get("top_share", 0.0)
        print(
            f"💤 [Mega-Trend Ignition] 점화 없음 "
            f"(top={top} {float(top_sh or 0)*100:.1f}%, checked={detection.get('candidates_checked', 0)})"
        )
    return state

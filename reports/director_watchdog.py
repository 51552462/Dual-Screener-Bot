"""DIRECTOR-WATCHDOG-01 — 북극성 일간 맨 위 확인 패널 (표시 전용).

Kelly / LOCKDOWN / F-GATE / 데모션 테이블 / S5 게이트 / IV 재계산 무접촉.
"""
from __future__ import annotations

import html
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping, Optional

from family_sleeve_demote import (
    FAMILY_SLEEVE_DEMOTE_KEYS,
    FAMILY_SLEEVE_DEMOTE_MULT,
    apply_family_sleeve_group_mult,
)

NAV_HOOK_FAIL_EVENT = "nav_hook.closure_sync_failed"
_S5_GATE_REGIMES = frozenset({"BEAR", "HIGH_VOL"})
_S5_PROBE_SIG = "Dante[INVERSE_ETF]"
_CORE_TAG_RE = re.compile(r"\[.*?\]")


def _esc(v: Any) -> str:
    return html.escape(str(v if v is not None else ""), quote=False)


def leaderboard_core_group(sig_type: Any) -> str:
    """forward/deep_dive.py 리더보드와 동일: 모든 [태그] 제거."""
    clean = _CORE_TAG_RE.sub("", str(sig_type or "")).strip()
    if clean:
        return clean
    return str(sig_type or "").replace("[", "").replace("]", "").strip()


def count_nav_hook_failures_24h(
    *,
    db_path: Optional[str] = None,
    now_utc: Optional[datetime] = None,
) -> int:
    """ops_events.sqlite COUNT — market_data.sqlite 아님."""
    from ops_logger import OPS_EVENTS_DB_PATH

    path = db_path or OPS_EVENTS_DB_PATH
    if not path or not os.path.isfile(path):
        return 0
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    cutoff = (now - timedelta(hours=24)).isoformat()
    try:
        conn = sqlite3.connect(path, timeout=15)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM ops_events WHERE event = ? AND ts_utc >= ?",
                (NAV_HOOK_FAIL_EVENT, cutoff),
            ).fetchone()
            return int(row[0] or 0) if row else 0
        finally:
            conn.close()
    except sqlite3.Error:
        return 0


def previous_budget_bands_from_history(
    history_daily: Any,
    *,
    date_kst: str,
) -> Dict[str, str]:
    """원장 history.daily 에서 오늘 이전 마지막 snap의 band."""
    today = str(date_kst or "")[:10]
    if not isinstance(history_daily, list) or not today:
        return {}
    for row in reversed(history_daily):
        if not isinstance(row, dict):
            continue
        d = str(row.get("date_kst") or "")[:10]
        if not d or d >= today:
            continue
        markets = ((row.get("tracks") or {}).get("A") or {}).get("markets") or {}
        if not isinstance(markets, dict):
            continue
        out: Dict[str, str] = {}
        for mk in ("KR", "US"):
            blk = markets.get(mk) or {}
            if isinstance(blk, dict):
                out[mk] = str(blk.get("budget_band") or "").strip()
        return out
    return {}


def _current_bands_from_snap(snap: Mapping[str, Any]) -> Dict[str, str]:
    markets = ((snap.get("tracks") or {}).get("A") or {}).get("markets") or {}
    out: Dict[str, str] = {}
    if not isinstance(markets, dict):
        return out
    for mk in ("KR", "US"):
        blk = markets.get(mk) or {}
        if isinstance(blk, dict) and blk.get("budget_band"):
            out[mk] = str(blk.get("budget_band")).strip()
    return out


def _fill_bands_from_governor(bands: Dict[str, str]) -> Dict[str, str]:
    if all(bands.get(mk) for mk in ("KR", "US")):
        return bands
    try:
        from performance_budget_governor import evaluate_performance_budget

        filled = dict(bands)
        for mk in ("KR", "US"):
            if filled.get(mk):
                continue
            ev = evaluate_performance_budget(market=mk)
            filled[mk] = str((ev or {}).get("band") or "UNKNOWN")
        return filled
    except Exception:
        return bands


def inspect_demote_effective(
    group_map: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """effective = apply_family_sleeve_group_mult (맵 raw 판정 금지)."""
    raw_map = group_map if isinstance(group_map, Mapping) else {}
    values: Dict[str, float] = {}
    bad: list[str] = []
    for key in FAMILY_SLEEVE_DEMOTE_KEYS:
        raw = raw_map.get(key, 1.0)
        eff = float(apply_family_sleeve_group_mult(key, raw))
        values[key] = eff
        if abs(eff - float(FAMILY_SLEEVE_DEMOTE_MULT)) > 1e-12:
            bad.append(key)
    return {
        "ok": not bad,
        "bad_keys": bad,
        "n_keys": len(FAMILY_SLEEVE_DEMOTE_KEYS),
        "effective": values,
    }


def read_iv_observation_latest(path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """20:10 산출 JSON만 읽기. IV 풀 재계산 금지."""
    from iv_observation_report import iv_observation_latest_path

    p = path or iv_observation_latest_path()
    if not p or not os.path.isfile(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, TypeError):
        return None


def count_us_rank_closed(
    *,
    db_path: Optional[str] = None,
) -> Dict[str, int]:
    """deep_dive 리더보드 tot 와 같은 그룹화 후 US RANK_B / RANK_D CLOSED 건수."""
    out = {"US_RANK_B": 0, "US_RANK_D": 0}
    try:
        from market_db_paths import report_db_read_path

        path = db_path or report_db_read_path()
    except Exception:
        path = db_path
    if not path or not os.path.isfile(path):
        return out
    try:
        conn = sqlite3.connect(path, timeout=15)
        try:
            rows = conn.execute(
                """
                SELECT IFNULL(sig_type,''), COUNT(*)
                FROM forward_trades
                WHERE UPPER(IFNULL(market,'')) = 'US'
                  AND IFNULL(status,'') LIKE 'CLOSED%'
                GROUP BY 1
                """
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return out
    for sig, n in rows:
        core = leaderboard_core_group(sig)
        cnt = int(n or 0)
        if "US_RANK_B" in core:
            out["US_RANK_B"] += cnt
        if "US_RANK_D" in core:
            out["US_RANK_D"] += cnt
    return out


def _s5_line(regimes: Mapping[str, str], sys_config: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    from meta_governor_consumer import resolve_defense_arm_weight
    from meta_state_store import normalize_regime_key

    cfg = sys_config if isinstance(sys_config, dict) else {}
    parts: list[str] = []
    gate_regime = False
    for mk in ("KR", "US"):
        rk = normalize_regime_key(regimes.get(mk) or "UNKNOWN")
        parts.append(f"{mk} {rk or 'UNKNOWN'}")
        if rk in _S5_GATE_REGIMES:
            gate_regime = True
            w = resolve_defense_arm_weight(mk, rk, _S5_PROBE_SIG, cfg)
            parts[-1] = f"{mk} {rk} w={float(w):.2f}"
    if not gate_regime:
        return {
            "light": "⚪",
            "title": "S5 방어팔",
            "text": "BEAR/HIGH_VOL 미도래, 측정 대기 중",
        }
    return {
        "light": "🟡",
        "title": "S5 방어팔",
        "text": " · ".join(parts) + " — 게이트 재사용(상태 변경 없음)",
    }


def _load_regimes(sys_config: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    from performance_budget_governor import resolve_market_regime_key

    cfg = sys_config if isinstance(sys_config, dict) else {}
    return {
        "KR": resolve_market_regime_key(cfg, "KR"),
        "US": resolve_market_regime_key(cfg, "US"),
    }


def build_director_watchdog_payload(
    snap: Mapping[str, Any],
    *,
    load_history: bool = False,
    previous_bands: Optional[Mapping[str, str]] = None,
    nav_fail_count: Optional[int] = None,
    iv_report: Optional[Mapping[str, Any]] = None,
    rank_counts: Optional[Mapping[str, int]] = None,
    regimes: Optional[Mapping[str, str]] = None,
    group_map: Optional[Mapping[str, Any]] = None,
    sys_config: Optional[Mapping[str, Any]] = None,
    ops_db_path: Optional[str] = None,
    iv_path: Optional[str] = None,
    forward_db_path: Optional[str] = None,
    history_daily: Any = None,
) -> Dict[str, Any]:
    bands = _fill_bands_from_governor(_current_bands_from_snap(snap))
    prev: Dict[str, str] = dict(previous_bands) if isinstance(previous_bands, Mapping) else {}
    if not prev and load_history:
        if history_daily is None:
            try:
                from dual_north_star_ledger import load_ledger

                history_daily = (load_ledger().get("history") or {}).get("daily")
            except Exception:
                history_daily = []
        prev = previous_budget_bands_from_history(
            history_daily, date_kst=str(snap.get("date_kst") or "")
        )

    band_bits: list[str] = []
    band_changed = False
    for mk in ("KR", "US"):
        cur = bands.get(mk) or "—"
        old = str(prev.get(mk) or "").strip()
        if not old:
            delta = "전일없음"
        elif old == cur:
            delta = "변화없음"
        else:
            delta = f"{old}→{cur}"
            band_changed = True
        band_bits.append(f"{mk} band: {cur}({delta})")
    if band_changed:
        band_light = "🔴"
    elif not prev:
        band_light = "🟡"
    else:
        band_light = "🟢"

    if nav_fail_count is None:
        nav_fail_count = count_nav_hook_failures_24h(db_path=ops_db_path)
    nav_n = int(nav_fail_count or 0)
    nav_light = "🟢" if nav_n == 0 else "🔴"

    if group_map is None:
        try:
            from meta_governor_consumer import load_meta_state_resolved

            meta = load_meta_state_resolved()
            group_map = meta.get("META_GROUP_KELLY_MULT") if isinstance(meta, dict) else {}
        except Exception:
            group_map = {}
    demote = inspect_demote_effective(group_map)
    demote_light = "🟢" if demote["ok"] else "🔴"
    if demote["ok"]:
        demote_text = f"S1/S4 12키 전부 {FAMILY_SLEEVE_DEMOTE_MULT} 유지"
    else:
        demote_text = f"effective≠{FAMILY_SLEEVE_DEMOTE_MULT} · {len(demote['bad_keys'])}키"

    iv = iv_report if isinstance(iv_report, Mapping) else read_iv_observation_latest(iv_path)
    if not isinstance(iv, Mapping):
        v2_light, v2_text = "🟡", "JSON 없음 (20:10 산출 대기)"
    else:
        readiness = str((iv.get("v2") or {}).get("readiness") or "UNKNOWN")
        obs = iv.get("observation") or {}
        days = obs.get("days_elapsed")
        mind = obs.get("min_days")
        extra = ""
        if days is not None and mind is not None:
            extra = f" ({days}/{mind}일)"
        v2_light = "🟡"
        v2_text = f"{readiness}{extra}"

    ranks = dict(rank_counts) if isinstance(rank_counts, Mapping) else count_us_rank_closed(
        db_path=forward_db_path
    )
    n_b = int(ranks.get("US_RANK_B") or 0)
    n_d = int(ranks.get("US_RANK_D") or 0)
    rank_text = (
        f"US_RANK_B n={n_b} (참고 관찰선 30) · US_RANK_D n={n_d} (참고 관찰선 30)"
    )

    cfg = sys_config
    if cfg is None:
        try:
            from config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = {}
    rk = dict(regimes) if isinstance(regimes, Mapping) else _load_regimes(cfg)
    s5 = _s5_line(rk, cfg)

    items = [
        {"light": band_light, "title": "안전장치", "text": " · ".join(band_bits)},
        {"light": nav_light, "title": "NAV 훅", "text": f"최근 24시간 실패 {nav_n}건"},
        {"light": demote_light, "title": "데모션 유지", "text": demote_text},
        {"light": v2_light, "title": "V-2 심판", "text": v2_text},
        {"light": "🟡", "title": "자원집중 후보", "text": rank_text},
        s5,
    ]
    payload = {
        "kind": "director_watchdog",
        "items": items,
        "demote": {"ok": demote["ok"], "n_bad": len(demote["bad_keys"])},
        "nav_fail_count": nav_n,
    }
    payload["html"] = format_director_watchdog_html(payload)
    return payload


def format_director_watchdog_html(payload: Mapping[str, Any]) -> str:
    items = payload.get("items") or []
    lines = ["🔭 <b>[디렉터 워치독]</b> 오늘 확인할 것"]
    for it in items:
        if not isinstance(it, dict):
            continue
        light = str(it.get("light") or "🟡")
        title = _esc(it.get("title") or "")
        text = _esc(it.get("text") or "")
        lines.append(f"{light} {title} — {text}")
    return "\n".join(lines)


def format_director_watchdog_section_from_snap(snap: Mapping[str, Any]) -> str:
    if str(snap.get("cadence") or "").lower() != "daily":
        return ""
    payload = snap.get("director_watchdog")
    if payload is False:
        return ""
    if isinstance(payload, dict):
        html_panel = payload.get("html")
        if isinstance(html_panel, str) and html_panel.strip():
            return html_panel
        if payload.get("items"):
            return format_director_watchdog_html(payload)
    try:
        built = build_director_watchdog_payload(snap, load_history=False)
        return str(built.get("html") or "")
    except Exception:
        return ""

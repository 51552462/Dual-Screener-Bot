"""
F-RETIRE-02 — COOLED/RETIRED lifecycle observe-only ($0 paper) + redemption → CANDIDATE.

태그 네임스페이스: LIFECYCLE_OBSERVE_ONLY (RE_EVOL_SHADOW 와 분리).
"""
from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

from re_evolution_redemption_gate import (
    compute_shadow_stats,
    extract_core_group_name,
    fetch_benchmark_return_pct,
    passes_redemption_gate,
    re_evolution_redemption_config,
)
from strategy_lifecycle_config import compute_dynamic_shadow_verification_window

logger = logging.getLogger(__name__)

_OBSERVE_TAG = "LIFECYCLE_OBSERVE_ONLY"

DEFAULT_RETENTION_DAYS: Dict[str, int] = {"KR": 30, "US": 90, "BG": 63}


def _cfg_bool(cfg: Optional[Dict[str, Any]], key: str, default: bool) -> bool:
    if not isinstance(cfg, dict):
        return default
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def lifecycle_observe_only_config(
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    raw_ret = cfg.get("LIFECYCLE_OBSERVE_ONLY_RETENTION_DAYS")
    retention = dict(DEFAULT_RETENTION_DAYS)
    if isinstance(raw_ret, dict):
        for mk, days in raw_ret.items():
            try:
                retention[str(mk).upper()] = int(days)
            except (TypeError, ValueError):
                continue
    redemption = re_evolution_redemption_config(sys_config)
    return {
        "enabled": _cfg_bool(cfg, "LIFECYCLE_OBSERVE_ONLY_ENABLED", True),
        "retention_days": retention,
        **{k: redemption[k] for k in redemption if k != "enabled"},
    }


def resolve_observe_only_retention_days(
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> int:
    mk = str(market or "KR").upper()
    cfg = lifecycle_observe_only_config(sys_config)
    return int(cfg.get("retention_days", {}).get(mk, DEFAULT_RETENTION_DAYS.get(mk, 30)))


def _parse_iso_dt(iso_val: Any) -> Optional[datetime]:
    if not iso_val:
        return None
    try:
        s = str(iso_val).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def _days_since(iso_val: Any, now: datetime) -> Optional[int]:
    dt = _parse_iso_dt(iso_val)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now_a = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    return max(0, (now_a - dt.astimezone(timezone.utc)).days)


def resolve_observe_only_started_at(row: Mapping[str, Any]) -> Optional[str]:
    val = row.get("lifecycle_observe_only_started_at")
    if val:
        return str(val)
    val = row.get("last_demoted_at")
    return str(val) if val else None


def is_lifecycle_observe_only_row(row: Mapping[str, Any]) -> bool:
    """COOLED/RETIRED registry row (BG 제외). retention 미검사."""
    st = str(row.get("state") or "").upper()
    if st not in ("COOLED", "RETIRED"):
        return False
    mk = str(row.get("market") or "KR").upper()
    return mk != "BG"


def is_within_observe_only_retention(
    row: Mapping[str, Any],
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> bool:
    if not is_lifecycle_observe_only_row(row):
        return False
    cfg = lifecycle_observe_only_config(sys_config)
    if not cfg.get("enabled"):
        return False
    started = resolve_observe_only_started_at(row)
    if not started:
        return True
    now_dt = now or datetime.now(timezone.utc)
    mk = str(row.get("market") or "KR").upper()
    retention = resolve_observe_only_retention_days(mk, sys_config)
    elapsed = _days_since(started, now_dt)
    if elapsed is None:
        return True
    return elapsed < retention


def ensure_lifecycle_observe_only_stamp(
    row: Dict[str, Any],
    now_iso: str,
) -> None:
    """COOLED/RETIRED 전환 시 observe 시작 시각 스탬프 (additive)."""
    if not is_lifecycle_observe_only_row(row):
        return
    if not row.get("lifecycle_observe_only_started_at"):
        row["lifecycle_observe_only_started_at"] = (
            row.get("last_demoted_at") or now_iso
        )


def _registry_row_for_group(
    meta: Optional[Mapping[str, Any]],
    market: str,
    group_key: str,
) -> Optional[Dict[str, Any]]:
    gk = str(group_key or "").strip()
    if not gk:
        return None
    mk = str(market or "KR").upper()
    reg = meta.get("META_STRATEGY_REGISTRY") if isinstance(meta, Mapping) else None
    if isinstance(reg, list):
        for row in reg:
            if not isinstance(row, dict):
                continue
            if str(row.get("market") or "").upper() != mk:
                continue
            rg = str(row.get("group_key") or row.get("display_name") or "").strip()
            if rg == gk or gk in rg or rg in gk:
                return dict(row)
    try:
        from strategy_registry_store import load_registry_rows

        for row in load_registry_rows():
            if str(row.get("market") or "").upper() != mk:
                continue
            rg = str(row.get("group_key") or row.get("display_name") or "").strip()
            if rg == gk or gk in rg or rg in gk:
                return dict(row)
    except Exception:
        pass
    return None


def is_lifecycle_observe_only_group(
    meta: Optional[Mapping[str, Any]],
    market: str,
    group_key: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> bool:
    """진입 경로 — retention 내 active observe-only 그룹."""
    row = _registry_row_for_group(meta, market, group_key)
    if not row:
        return False
    return is_within_observe_only_retention(
        row, sys_config=sys_config, now=now
    )


def format_lifecycle_observe_only_sig_type(
    strategy_id: str,
    sig_body: str,
) -> str:
    sid = str(strategy_id or "UNKNOWN").strip()
    body = str(sig_body or "").strip()
    return f"[OBSERVE_ONLY][{_OBSERVE_TAG}][{sid}] {body}".strip()


def apply_lifecycle_observe_only_entry_zero_notional(
    sig_type: str,
    *,
    strategy_id: str = "",
) -> Tuple[str, int, float, float]:
    """$0 notional + LIFECYCLE_OBSERVE_ONLY 태그 (RE_EVOL_SHADOW 패턴 미러)."""
    tagged = format_lifecycle_observe_only_sig_type(strategy_id, sig_type)
    return tagged, 0, 0.0, 0.0


def _default_db_path() -> Optional[str]:
    try:
        from market_db_paths import market_db_read_path

        return market_db_read_path()
    except Exception:
        try:
            from market_db_paths import MARKET_DATA_DB_PATH

            return MARKET_DATA_DB_PATH
        except Exception:
            return None


def fetch_lifecycle_observe_only_closed_rows(
    market: str,
    group_key: str,
    *,
    lookback_days: int = 7,
    db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """LIFECYCLE_OBSERVE_ONLY 태그 청산만 — RE_EVOL_SHADOW 제외."""
    mk = str(market or "KR").upper()
    gk = str(group_key or "").strip()
    if not gk:
        return []

    path = db_path or _default_db_path()
    if not path or not os.path.isfile(path):
        return []

    cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    like_g = f"%{gk}%"
    like_tag = f"%{_OBSERVE_TAG}%"
    like_observe = "%OBSERVE_ONLY%"

    try:
        conn = sqlite3.connect(path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.execute(
                """
                SELECT sig_type, final_ret, exit_date, entry_date,
                       invest_amount, sim_kelly_invest
                FROM forward_trades
                WHERE UPPER(TRIM(market)) = ?
                  AND status LIKE 'CLOSED%%'
                  AND final_ret IS NOT NULL
                  AND IFNULL(sig_type,'') LIKE ?
                  AND IFNULL(sig_type,'') LIKE ?
                  AND IFNULL(sig_type,'') NOT LIKE '%RE_EVOL_SHADOW%'
                  AND COALESCE(NULLIF(TRIM(exit_date), ''), entry_date) >= ?
                ORDER BY rowid DESC
                """,
                (mk, like_tag, like_observe, cutoff),
            )
            rows = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as ex:
        logger.warning("fetch_lifecycle_observe_only_closed_rows failed: %s", ex)
        return []

    out: List[Dict[str, Any]] = []
    for row in rows:
        sig = str(row.get("sig_type") or "")
        core = extract_core_group_name(sig)
        if gk not in core and core != gk and gk not in sig:
            continue
        out.append(row)
    return out


def resolve_lifecycle_observe_verification_window(
    market: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    row: Optional[Mapping[str, Any]] = None,
    meta: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    started = resolve_observe_only_started_at(row or {})
    window = compute_dynamic_shadow_verification_window(
        market,
        system_cfg=sys_config,
        demoted_at_iso=started,
        now=now,
        meta=meta,
    )
    window["observe_only_started_at"] = started
    return window


def evaluate_lifecycle_observe_only_redemption(
    row: Mapping[str, Any],
    *,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
    now: Optional[datetime] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """COOLED/RETIRED → CANDIDATE 재발굴 게이트 평가 (부작용 없음)."""
    cfg = lifecycle_observe_only_config(sys_config)
    if not cfg.get("enabled"):
        return False, {"reason": "disabled"}

    if not is_lifecycle_observe_only_row(row):
        return False, {"reason": "not_lifecycle_observe_row"}

    if not is_within_observe_only_retention(row, sys_config=sys_config, now=now):
        return False, {"reason": "retention_expired"}

    mk = str(row.get("market") or "KR").upper()
    gk = str(row.get("group_key") or row.get("display_name") or "").strip()
    eval_row = dict(row)

    window = resolve_lifecycle_observe_verification_window(
        mk,
        sys_config=sys_config,
        row=eval_row,
        meta=meta,
        now=now,
    )
    lookback = int(
        window.get("verification_window_days")
        or window.get("final_window_days")
        or window.get("base_window_days")
        or 7
    )

    closed = fetch_lifecycle_observe_only_closed_rows(
        mk, gk, lookback_days=lookback, db_path=forward_db_path
    )
    stats = compute_shadow_stats(closed)
    bench = fetch_benchmark_return_pct(mk, lookback_days=lookback)
    ok, detail = passes_redemption_gate(
        stats, bench, cfg, verification_window=window
    )
    detail["verification_window"] = window
    detail["shadow_stats"] = stats
    detail["passes"] = ok
    return ok, detail


def try_promote_lifecycle_observe_only_redemption(
    row: Dict[str, Any],
    *,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    forward_db_path: Optional[str] = None,
    now: Optional[datetime] = None,
    now_iso: Optional[str] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """재발굴 통과 시 CANDIDATE 복귀 (LIVE 직행 없음)."""
    now_dt = now
    if now_dt is None and now_iso:
        now_dt = _parse_iso_dt(now_iso)
    if now_dt is None:
        now_dt = datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    now_iso = now_iso or now_dt.isoformat()

    ok, ev = evaluate_lifecycle_observe_only_redemption(
        row,
        meta=meta,
        sys_config=sys_config,
        forward_db_path=forward_db_path,
        now=now_dt,
    )
    if not ok:
        return False, ev

    row["state"] = "CANDIDATE"
    row["capital_mult"] = 0.0
    row["promote_reason"] = "lifecycle_observe_redemption"
    row["demote_reason"] = None
    row["observe_only_released"] = True
    row["lifecycle_observe_redeemed_at"] = now_iso
    row["updated_at"] = now_iso

    logger.info(
        "Lifecycle observe-only redemption → CANDIDATE: %s %s n=%s wr=%.2f",
        row.get("market"),
        row.get("group_key"),
        (ev.get("n_closed") or (ev.get("shadow_stats") or {}).get("n_closed")),
        float((ev.get("win_rate") or (ev.get("shadow_stats") or {}).get("win_rate") or 0.0)),
    )
    return True, ev

"""
V-1/V-2 — Independent Verification 관측 리포트 (텔레그램 + Cursor 붙여넣기 SSOT).

- 결과: factory_data_dir()/iv_observation_latest.json
- 주간(또는 수동) 실행 — 디렉터가 Cursor에 ---CURSOR--- 블록만 붙여넣으면 됨
- V-2 BLOCK 기본 OFF — readiness=READY 일 때만 디렉터가 활성화 검토
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz

from factory_data_paths import factory_data_dir

logger = logging.getLogger(__name__)

COMPONENT = "iv_observation"
V1_OBSERVATION_START_DEFAULT = "2026-08-09"
MIN_OBSERVATION_DAYS = 28
MAX_FP_RATE_FOR_V2 = 0.25
MIN_WF_WARN_SAMPLES_FOR_FP = 3


def iv_observation_latest_path() -> str:
    return os.path.join(factory_data_dir(), "iv_observation_latest.json")


def iv_observation_state_path() -> str:
    return os.path.join(factory_data_dir(), "iv_observation_state.json")


def _kst_today() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")


def _kst_now_iso() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).isoformat()


def _days_between(start_yyyy_mm_dd: str, end_yyyy_mm_dd: str) -> int:
    try:
        a = datetime.strptime(start_yyyy_mm_dd[:10], "%Y-%m-%d")
        b = datetime.strptime(end_yyyy_mm_dd[:10], "%Y-%m-%d")
        return max(0, (b - a).days)
    except ValueError:
        return 0


def resolve_v1_observation_start() -> str:
    env = (os.environ.get("V1_OBSERVATION_START") or "").strip()
    if env:
        return env[:10]
    path = iv_observation_state_path()
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict) and raw.get("v1_started_at"):
                return str(raw["v1_started_at"])[:10]
        except (OSError, json.JSONDecodeError):
            pass
    return V1_OBSERVATION_START_DEFAULT


def _ensure_state_started(start: str) -> None:
    path = iv_observation_state_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.isfile(path):
        return
    payload = {"v1_started_at": start, "first_report_at": _kst_now_iso()}
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _connect_market_db(db_path: Optional[str] = None) -> Optional[sqlite3.Connection]:
    path = db_path
    if not path:
        try:
            from market_db_paths import market_db_read_path

            path = market_db_read_path()
        except Exception:
            return None
    if not path or not os.path.isfile(path):
        return None
    try:
        return sqlite3.connect(path, timeout=30)
    except sqlite3.Error:
        return None


def _bitget_db_path() -> Optional[str]:
    try:
        from bitget.infra.data_paths import bitget_data_dir

        p = os.path.join(bitget_data_dir(), "market_data.sqlite")
        return p if os.path.isfile(p) else None
    except Exception:
        return None


def summarize_krus_wf_groups(
    *,
    db_path: Optional[str] = None,
    min_trades: int = 30,
) -> Dict[str, Any]:
    """KR/US forward groups — WF OOS warn snapshot."""
    from strategy_promotion_engine import (
        _sig_to_group_key,
        evaluate_wf_oos_warn_for_group,
    )
    from validation.walk_forward import evaluate_oos_pass_from_returns

    conn = _connect_market_db(db_path)
    if conn is None:
        return {"status": "SKIP", "detail": "market_db_missing", "groups": []}

    groups: Dict[Tuple[str, str], List[float]] = {}
    try:
        cur = conn.execute(
            """
            SELECT UPPER(IFNULL(market,'KR')), sig_type, final_ret
            FROM forward_trades
            WHERE status LIKE 'CLOSED%' AND final_ret IS NOT NULL
            ORDER BY IFNULL(exit_date,''), rowid
            """
        )
        for mkt, sig, ret in cur.fetchall():
            mk = str(mkt or "KR").upper()
            if mk not in ("KR", "US"):
                mk = "KR"
            gk = _sig_to_group_key(str(sig or ""))
            try:
                groups.setdefault((mk, gk), []).append(float(ret) / 100.0)
            except (TypeError, ValueError):
                continue
    except sqlite3.Error as ex:
        return {"status": "SKIP", "detail": f"query_failed:{ex}", "groups": []}
    finally:
        conn.close()

    out_groups: List[Dict[str, Any]] = []
    warn_n = 0
    for (mk, gk), rets in sorted(groups.items()):
        if len(rets) < min_trades:
            continue
        ev = evaluate_oos_pass_from_returns(rets, min_total_trades=min_trades)
        warn = evaluate_wf_oos_warn_for_group(mk, gk, forward_db_path=db_path, min_total_trades=min_trades)
        if warn:
            warn_n += 1
        out_groups.append(
            {
                "market": mk,
                "group_key": gk,
                "n_closed": len(rets),
                "oos_pass": bool(ev.get("pass")),
                "oos_reason": ev.get("reason"),
                "wf_warn": bool(warn),
            }
        )

    return {
        "status": "OK",
        "groups_evaluated": len(out_groups),
        "wf_warn_count": warn_n,
        "groups": out_groups[:40],
        "groups_truncated": max(0, len(out_groups) - 40),
    }


def summarize_bitget_wf_shadow(*, db_path: Optional[str] = None) -> Dict[str, Any]:
    """Latest Bitget walk_forward_shadow run (if DB exists)."""
    path = db_path or _bitget_db_path()
    if not path:
        return {"status": "SKIP", "detail": "bitget_db_missing"}

    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_walk_forward_shadow'"
            ).fetchone()
            if not row:
                return {"status": "SKIP", "detail": "shadow_table_missing"}

            run_row = conn.execute(
                "SELECT run_id, MAX(recorded_at) FROM bitget_walk_forward_shadow"
            ).fetchone()
            if not run_row or not run_row[0]:
                return {"status": "SKIP", "detail": "no_shadow_runs"}

            run_id = str(run_row[0])
            rows = conn.execute(
                """
                SELECT market, group_key, oos_pass, reason, n_closed, recorded_at
                FROM bitget_walk_forward_shadow
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as ex:
        return {"status": "SKIP", "detail": f"query_failed:{ex}"}

    n = len(rows)
    fail = sum(1 for r in rows if int(r[2] or 0) == 0)
    return {
        "status": "OK",
        "run_id": run_id,
        "recorded_at": rows[0][5] if rows else None,
        "groups": n,
        "oos_fail": fail,
        "oos_fail_rate": round(fail / n, 4) if n else 0.0,
    }


def estimate_wf_false_positive_rate(
    wf_groups: List[Dict[str, Any]],
    health: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    wf_warn 그룹 중 forward health 가 여전히 양호(WR≥0.5, PF≥1.2) → 추정 오탐.
    """
    warned = [g for g in wf_groups if g.get("wf_warn")]
    if len(warned) < MIN_WF_WARN_SAMPLES_FOR_FP:
        return {
            "n_warn": len(warned),
            "n_false_positive": 0,
            "false_positive_rate": None,
            "detail": f"need>={MIN_WF_WARN_SAMPLES_FOR_FP}_warn_samples",
        }
    hv = health if isinstance(health, dict) else {}
    fp = 0
    for g in warned:
        mk = str(g.get("market") or "KR").upper()
        gk = str(g.get("group_key") or "")
        key = f"{mk}|{gk}"
        row = hv.get(key)
        if not isinstance(row, dict):
            continue
        wr = float(row.get("rolling_wr") or 0)
        pf = float(row.get("rolling_pf") or 0)
        if wr >= 0.5 and pf >= 1.2:
            fp += 1
    rate = fp / len(warned) if warned else 0.0
    return {
        "n_warn": len(warned),
        "n_false_positive": fp,
        "false_positive_rate": round(rate, 4),
        "detail": "ok",
    }


def assess_v2_readiness(
    *,
    days_elapsed: int,
    false_positive_rate: Optional[float],
    reality_status: str,
    wf_warn_count: int,
) -> str:
    from strategy_promotion_engine import walk_forward_promotion_block_enabled

    if walk_forward_promotion_block_enabled():
        return "BLOCK_ALREADY_ON"
    if days_elapsed < MIN_OBSERVATION_DAYS:
        return "NOT_READY"
    if reality_status == "BREAK":
        return "NOT_READY"
    if wf_warn_count < MIN_WF_WARN_SAMPLES_FOR_FP:
        return "WATCH"
    if false_positive_rate is None:
        return "WATCH"
    if false_positive_rate > MAX_FP_RATE_FOR_V2:
        return "NOT_READY"
    return "READY"


def build_cursor_prompt(report: Dict[str, Any]) -> str:
    """디렉터 → Cursor 첫 메시지용 (복붙 SSOT)."""
    r = report
    obs = r.get("observation") or {}
    v2 = r.get("v2") or {}
    fp = r.get("false_positive") or {}
    fp_s = (
        f"{float(fp['false_positive_rate']) * 100:.1f}%"
        if fp.get("false_positive_rate") is not None
        else "N/A (표본 부족)"
    )
    return (
        "Track A — IV 관측 리포트 리뷰. 구현·V-2 BLOCK 활성화는 readiness=READY 일 때만.\n\n"
        f"1) docs/independent_verification/ 또는 factory data `iv_observation_latest.json` 기준\n"
        f"2) V-1 경과: {obs.get('days_elapsed')}/{obs.get('min_days')}일 "
        f"(시작 {obs.get('v1_started_at')})\n"
        f"3) wf_warn 그룹: {r.get('krus_wf', {}).get('wf_warn_count', 0)} · "
        f"추정 오탐률: {fp_s}\n"
        f"4) reality_audit: {r.get('reality_audit', {}).get('status')} · "
        f"BG shadow fail rate: {r.get('bitget_shadow', {}).get('oos_fail_rate', 'N/A')}\n"
        f"5) V-2 BLOCK 스위치: {'ON' if v2.get('block_enabled') else 'OFF (기본)'} · "
        f"readiness: {v2.get('readiness')}\n\n"
        "출력: 디렉터용 3줄 요약 + (READY면) V-2 활성화 체크리스트만. 코드 변경은 WAIT unless READY."
    )


def format_iv_observation_telegram(report: Dict[str, Any]) -> str:
    v2 = report.get("v2") or {}
    obs = report.get("observation") or {}
    fp = report.get("false_positive") or {}
    readiness = str(v2.get("readiness") or "UNKNOWN")
    emoji = "🟢" if readiness == "READY" else ("🔴" if readiness == "NOT_READY" else "🟡")

    fp_txt = (
        f"{float(fp['false_positive_rate']) * 100:.1f}%"
        if fp.get("false_positive_rate") is not None
        else "N/A"
    )
    lines = [
        f"{emoji} <b>[IV_OBS]</b> V-1 관측 리포트",
        f"· 경과 <b>{obs.get('days_elapsed')}</b>/{obs.get('min_days')}일 "
        f"(시작 {obs.get('v1_started_at')})",
        f"· wf_warn <b>{report.get('krus_wf', {}).get('wf_warn_count', 0)}</b> · "
        f"오탐추정 <b>{fp_txt}</b>",
        f"· reality_audit <code>{report.get('reality_audit', {}).get('status')}</code> · "
        f"V-2 BLOCK <code>{'ON' if v2.get('block_enabled') else 'OFF'}</code>",
        f"· <b>readiness={readiness}</b>",
    ]
    if readiness == "READY":
        lines.append("· ✅ 4주 관측 충족 — 디렉터 V-2 활성화 검토 가능")
    elif readiness == "NOT_READY":
        lines.append(f"· ⏳ V-2 BLOCK 활성화 보류 ({MIN_OBSERVATION_DAYS}일·오탐률 기준)")
    lines.append("---CURSOR---")
    lines.append(report.get("cursor_prompt") or "")
    return "\n".join(lines)


def persist_iv_observation_report(report: Dict[str, Any]) -> str:
    path = iv_observation_latest_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)
    return path


def run_iv_observation_report(
    *,
    db_path: Optional[str] = None,
    health: Optional[Dict[str, Any]] = None,
    send_telegram: bool = True,
    persist: bool = True,
    force_telegram: bool = False,
) -> Dict[str, Any]:
    """
    IV 관측 리포트 생성.

    force_telegram=False → readiness 변경·WARN 이상일 때만 텔레그램 (주간 기본).
    force_telegram=True → 항상 발송 (수동 점검).
    """
    from deploy_watch import reality_audit_check, send_deploy_watch_telegram
    from strategy_promotion_engine import walk_forward_promotion_block_enabled

    start = resolve_v1_observation_start()
    _ensure_state_started(start)
    today = _kst_today()
    days = _days_between(start, today)

    reality = reality_audit_check(db_path=db_path)
    krus = summarize_krus_wf_groups(db_path=db_path)
    bg = summarize_bitget_wf_shadow()
    fp = estimate_wf_false_positive_rate(krus.get("groups") or [], health)

    readiness = assess_v2_readiness(
        days_elapsed=days,
        false_positive_rate=fp.get("false_positive_rate"),
        reality_status=str(reality.get("status") or "SKIP"),
        wf_warn_count=int(krus.get("wf_warn_count") or 0),
    )

    report: Dict[str, Any] = {
        "schema": "iv_observation_report.v1",
        "ts_kst": _kst_now_iso(),
        "observation": {
            "v1_started_at": start,
            "days_elapsed": days,
            "min_days": MIN_OBSERVATION_DAYS,
            "days_remaining": max(0, MIN_OBSERVATION_DAYS - days),
        },
        "reality_audit": {
            "status": reality.get("status"),
            "detail": reality.get("detail"),
            "metrics": reality.get("metrics"),
        },
        "krus_wf": krus,
        "bitget_shadow": bg,
        "false_positive": fp,
        "v2": {
            "block_enabled": walk_forward_promotion_block_enabled(),
            "readiness": readiness,
            "max_fp_rate": MAX_FP_RATE_FOR_V2,
        },
    }
    report["cursor_prompt"] = build_cursor_prompt(report)

    prev_readiness = None
    if os.path.isfile(iv_observation_latest_path()):
        try:
            with open(iv_observation_latest_path(), "r", encoding="utf-8") as f:
                prev = json.load(f)
            prev_readiness = (prev.get("v2") or {}).get("readiness")
        except (OSError, json.JSONDecodeError):
            pass

    if persist:
        report["latest_path"] = persist_iv_observation_report(report)

    should_send = force_telegram
    if not should_send:
        if readiness in ("READY", "NOT_READY") and readiness != prev_readiness:
            should_send = True
        if str(reality.get("status")) in ("WARN", "BREAK"):
            should_send = True
        if days > 0 and days % 7 == 0:
            should_send = True

    if send_telegram and should_send:
        report["telegram_sent"] = send_deploy_watch_telegram(
            format_iv_observation_telegram(report)
        )
    else:
        report["telegram_sent"] = False
    report["telegram_skipped"] = not should_send

    return report

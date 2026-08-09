"""
L-OBS-01 — 배포 관측 자동 판정 (PASS/WARN/BREAK).

- 결과: factory_data_dir()/deploy_watch_latest.json
- ops_events: component=deploy_watch, event=deploy_watch.summary
- 텔레그램: WARN/BREAK 만 (PASS/SKIP only → 무음)
"""
from __future__ import annotations

import json
import logging
import os
import platform
import sqlite3
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytz

from factory_data_paths import factory_data_dir

logger = logging.getLogger(__name__)

COMPONENT = "deploy_watch"
EVENT_SUMMARY = "deploy_watch.summary"

STATUS_PASS = "PASS"
STATUS_WARN = "WARN"
STATUS_BREAK = "BREAK"
STATUS_SKIP = "SKIP"

_STATUS_RANK = {
    STATUS_SKIP: 0,
    STATUS_PASS: 1,
    STATUS_WARN: 2,
    STATUS_BREAK: 3,
}

DEFAULT_FUNNEL_BASELINE_TS = "2026-07-02"
DEFAULT_PHASE = "post_f_gate_01"
DEFAULT_SERVICE = "dante-factory.service"


def deploy_watch_latest_path() -> str:
    return os.path.join(factory_data_dir(), "deploy_watch_latest.json")


def _kst_now_iso() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).isoformat()


def _worst_status(statuses: List[str]) -> str:
    best = STATUS_SKIP
    for st in statuses:
        if _STATUS_RANK.get(st, 0) > _STATUS_RANK.get(best, 0):
            best = st
    return best


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


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


def check_factory_health(
    *,
    service_name: str = DEFAULT_SERVICE,
) -> Dict[str, Any]:
    """systemd active — Linux VPS only."""
    if platform.system().lower() != "linux":
        return {
            "id": "factory_health",
            "status": STATUS_SKIP,
            "detail": "not_linux",
        }
    try:
        proc = subprocess.run(
            ["systemctl", "is-active", service_name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        active = (proc.stdout or "").strip() == "active"
    except (OSError, subprocess.TimeoutExpired) as ex:
        return {
            "id": "factory_health",
            "status": STATUS_SKIP,
            "detail": f"systemctl_unavailable:{ex}",
        }
    if active:
        return {
            "id": "factory_health",
            "status": STATUS_PASS,
            "detail": f"{service_name}=active",
        }
    return {
        "id": "factory_health",
        "status": STATUS_BREAK,
        "detail": f"{service_name} not active",
    }


def check_f_gate_01(*, db_path: Optional[str] = None) -> Dict[str, Any]:
    """COOLED/RETIRED registry — 0건이면 배포 직후 무변화 정상."""
    conn = _connect_market_db(db_path)
    if conn is None:
        return {
            "id": "f_gate_01",
            "status": STATUS_SKIP,
            "detail": "market_db_missing",
        }
    try:
        if not _table_exists(conn, "strategy_registry"):
            return {
                "id": "f_gate_01",
                "status": STATUS_SKIP,
                "detail": "strategy_registry_missing",
            }
        row = conn.execute(
            """
            SELECT COUNT(*) FROM strategy_registry
            WHERE UPPER(TRIM(state)) IN ('COOLED', 'RETIRED')
            """
        ).fetchone()
        n = int(row[0] or 0) if row else 0
    except sqlite3.Error as ex:
        return {
            "id": "f_gate_01",
            "status": STATUS_SKIP,
            "detail": f"query_failed:{ex}",
        }
    finally:
        conn.close()

    return {
        "id": "f_gate_01",
        "status": STATUS_PASS,
        "detail": f"cooled_retired={n}",
        "metrics": {"cooled_retired": n},
    }


def check_c_funnel_02(
    *,
    db_path: Optional[str] = None,
    baseline_ts: str = DEFAULT_FUNNEL_BASELINE_TS,
) -> Dict[str, Any]:
    """scan_funnel_snapshot MAX(ts) > baseline — C-FUNNEL-02 T+1 검증."""
    conn = _connect_market_db(db_path)
    if conn is None:
        return {
            "id": "c_funnel_02",
            "status": STATUS_SKIP,
            "detail": "market_db_missing",
        }
    try:
        if not _table_exists(conn, "scan_funnel_snapshot"):
            return {
                "id": "c_funnel_02",
                "status": STATUS_SKIP,
                "detail": "scan_funnel_snapshot_missing",
            }
        row = conn.execute(
            "SELECT MAX(ts), COUNT(*) FROM scan_funnel_snapshot"
        ).fetchone()
        max_ts = str(row[0] or "").strip() if row else ""
        total = int(row[1] or 0) if row else 0

        drop_n = 0
        if _table_exists(conn, "scan_funnel_drop_event"):
            drop_n = int(
                conn.execute("SELECT COUNT(*) FROM scan_funnel_drop_event").fetchone()[0]
                or 0
            )
    except sqlite3.Error as ex:
        return {
            "id": "c_funnel_02",
            "status": STATUS_SKIP,
            "detail": f"query_failed:{ex}",
        }
    finally:
        conn.close()

    metrics = {
        "max_ts": max_ts or None,
        "snapshot_rows": total,
        "drop_event_rows": drop_n,
        "baseline_ts": baseline_ts,
    }
    if not max_ts:
        return {
            "id": "c_funnel_02",
            "status": STATUS_WARN,
            "detail": "no_snapshot_rows",
            "metrics": metrics,
        }
    if max_ts[:10] <= baseline_ts[:10]:
        return {
            "id": "c_funnel_02",
            "status": STATUS_WARN,
            "detail": f"max_ts={max_ts} <= baseline={baseline_ts}",
            "metrics": metrics,
        }
    return {
        "id": "c_funnel_02",
        "status": STATUS_PASS,
        "detail": f"max_ts={max_ts}",
        "metrics": metrics,
    }


def check_f_retire_02(*, db_path: Optional[str] = None) -> Dict[str, Any]:
    """LIFECYCLE_OBSERVE_ONLY 태그 depth — 표본 0이면 정상(강등 전)."""
    conn = _connect_market_db(db_path)
    if conn is None:
        return {
            "id": "f_retire_02",
            "status": STATUS_SKIP,
            "detail": "market_db_missing",
        }
    try:
        if not _table_exists(conn, "forward_trades"):
            return {
                "id": "f_retire_02",
                "status": STATUS_SKIP,
                "detail": "forward_trades_missing",
            }
        row = conn.execute(
            """
            SELECT COUNT(*) FROM forward_trades
            WHERE IFNULL(sig_type,'') LIKE '%LIFECYCLE_OBSERVE_ONLY%'
            """
        ).fetchone()
        n = int(row[0] or 0) if row else 0
    except sqlite3.Error as ex:
        return {
            "id": "f_retire_02",
            "status": STATUS_SKIP,
            "detail": f"query_failed:{ex}",
        }
    finally:
        conn.close()

    return {
        "id": "f_retire_02",
        "status": STATUS_PASS,
        "detail": f"lifecycle_observe_only_rows={n}",
        "metrics": {"lifecycle_observe_only_rows": n},
    }


def reality_audit_check_enabled() -> bool:
    env = os.environ.get("REALITY_AUDIT_CHECK_ENABLED")
    if env is not None:
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    return True


def reality_audit_check(*, db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    V-1 / IV-21 — CLOSED forward_trades row completeness (KR/US by market column).

    CAT-E-BARS-01 SQL (a)(c) productized. BREAK on high null/corruption rates.
    """
    _id = "reality_audit"
    if not reality_audit_check_enabled():
        return {"id": _id, "status": STATUS_SKIP, "detail": "disabled"}

    conn = _connect_market_db(db_path)
    if conn is None:
        return {"id": _id, "status": STATUS_SKIP, "detail": "market_db_missing"}
    try:
        if not _table_exists(conn, "forward_trades"):
            return {"id": _id, "status": STATUS_SKIP, "detail": "forward_trades_missing"}

        rows = conn.execute(
            """
            SELECT
                UPPER(IFNULL(market,'KR')) AS market,
                COUNT(*) AS n_closed,
                SUM(bars_held IS NULL) AS null_bars,
                SUM(final_ret IS NULL) AS null_ret,
                SUM(exit_reason IS NULL OR TRIM(IFNULL(exit_reason,''))='') AS null_exit_reason,
                SUM(
                    exit_type IS NULL OR TRIM(IFNULL(exit_type,''))=''
                    OR UPPER(IFNULL(exit_type,''))='UNKNOWN'
                ) AS bad_exit_type,
                SUM(
                    entry_regime IS NULL OR TRIM(IFNULL(entry_regime,''))=''
                    OR UPPER(IFNULL(entry_regime,''))='UNKNOWN'
                ) AS bad_regime,
                SUM(CASE WHEN status LIKE 'CLOSED_ZOMBIE%' OR status LIKE 'CLOSED_AUTO%' THEN 1 ELSE 0 END) AS heal_closed
            FROM forward_trades
            WHERE status LIKE 'CLOSED%'
            GROUP BY UPPER(IFNULL(market,'KR'))
            """
        ).fetchall()
    except sqlite3.Error as ex:
        return {"id": _id, "status": STATUS_SKIP, "detail": f"query_failed:{ex}"}
    finally:
        conn.close()

    if not rows:
        return {
            "id": _id,
            "status": STATUS_PASS,
            "detail": "no_closed_rows",
            "metrics": {"markets": {}},
        }

    markets: Dict[str, Any] = {}
    statuses: List[str] = []
    for row in rows:
        mkt = str(row[0] or "KR").upper()
        if mkt not in ("KR", "US"):
            mkt = "KR"
        n = int(row[1] or 0)
        if n <= 0:
            continue
        null_bars = int(row[2] or 0)
        null_ret = int(row[3] or 0)
        null_exit_reason = int(row[4] or 0)
        bad_exit_type = int(row[5] or 0)
        bad_regime = int(row[6] or 0)
        heal_closed = int(row[7] or 0)

        null_core_pct = max(null_bars, null_ret) / n
        bad_et_pct = bad_exit_type / n
        bad_reg_pct = bad_regime / n
        heal_pct = heal_closed / n

        st = STATUS_PASS
        if null_core_pct >= 0.20 or bad_et_pct >= 0.30:
            st = STATUS_BREAK
        elif null_core_pct >= 0.05 or bad_et_pct >= 0.10 or bad_reg_pct >= 0.20:
            st = STATUS_WARN
        elif heal_pct >= 0.15 and bad_et_pct >= 0.05:
            st = STATUS_WARN

        markets[mkt] = {
            "n_closed": n,
            "null_bars": null_bars,
            "null_ret": null_ret,
            "null_exit_reason": null_exit_reason,
            "bad_exit_type": bad_exit_type,
            "bad_regime": bad_regime,
            "heal_closed": heal_closed,
            "null_core_pct": round(null_core_pct, 4),
            "bad_exit_type_pct": round(bad_et_pct, 4),
        }
        statuses.append(st)

    overall = _worst_status(statuses) if statuses else STATUS_PASS
    detail_parts = [
        f"{mk}:n={markets[mk]['n_closed']},bad_et={markets[mk]['bad_exit_type']}"
        for mk in sorted(markets.keys())
    ]
    return {
        "id": _id,
        "status": overall,
        "detail": "; ".join(detail_parts) if detail_parts else "ok",
        "metrics": {"markets": markets},
    }


def resolve_cursor_action(
    checks: List[Dict[str, Any]],
    *,
    phase: str,
) -> str:
    statuses = [str(c.get("status") or "") for c in checks]
    if STATUS_BREAK in statuses:
        if any(
            c.get("id") == "factory_health" and c.get("status") == STATUS_BREAK
            for c in checks
        ):
            return "BLOCK_F_RETIRE_02_DEPLOY"
        return "INVESTIGATE"
    if STATUS_WARN in statuses:
        return "REPORT_TO_CLAUDE"
    if phase.startswith("post_f_gate") and _worst_status(statuses) == STATUS_PASS:
        return "NONE"
    return "NONE"


def format_telegram_message(report: Dict[str, Any]) -> str:
    overall = str(report.get("overall") or STATUS_PASS)
    emoji = "🔴" if overall == STATUS_BREAK else "🟡"
    lines = [
        f"{emoji} <b>[DEPLOY_WATCH]</b> overall={overall} "
        f"phase={_esc(report.get('phase'))}",
    ]
    for chk in report.get("checks") or []:
        if not isinstance(chk, dict):
            continue
        st = str(chk.get("status") or "")
        if st in (STATUS_PASS, STATUS_SKIP):
            continue
        lines.append(
            f"· <code>{_esc(chk.get('id'))}</code> {st}: {_esc(chk.get('detail'))}"
        )
    lines.append("---CURSOR---")
    lines.append(_esc(json.dumps(report, ensure_ascii=False, default=str)))
    return "\n".join(lines)


def _esc(v: Any) -> str:
    s = str(v) if v is not None else ""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def send_deploy_watch_telegram(message: str) -> bool:
    try:
        import requests
        import telegram_env

        token = (telegram_env.get_report_token() or "").strip()
        chat_id = (telegram_env.get_report_chat_id() or "").strip()
        if not token or not chat_id:
            return False
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=15,
        )
        return resp.status_code == 200
    except Exception as ex:
        logger.warning("deploy_watch telegram failed: %s", ex)
        return False


def persist_deploy_watch_report(report: Dict[str, Any]) -> str:
    path = deploy_watch_latest_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)
    return path


def run_deploy_watch(
    *,
    phase: Optional[str] = None,
    db_path: Optional[str] = None,
    funnel_baseline_ts: Optional[str] = None,
    service_name: str = DEFAULT_SERVICE,
    send_telegram: bool = True,
    persist: bool = True,
    record_ops: bool = True,
) -> Dict[str, Any]:
    phase_use = (
        phase
        or os.environ.get("DEPLOY_WATCH_PHASE")
        or DEFAULT_PHASE
    ).strip()
    baseline = (
        funnel_baseline_ts
        or os.environ.get("DEPLOY_WATCH_FUNNEL_BASELINE_TS")
        or DEFAULT_FUNNEL_BASELINE_TS
    ).strip()

    checks = [
        check_factory_health(service_name=service_name),
        check_f_gate_01(db_path=db_path),
        check_c_funnel_02(db_path=db_path, baseline_ts=baseline),
        check_f_retire_02(db_path=db_path),
        reality_audit_check(db_path=db_path),
    ]
    overall = _worst_status([str(c.get("status") or STATUS_SKIP) for c in checks])
    cursor_action = resolve_cursor_action(checks, phase=phase_use)

    report: Dict[str, Any] = {
        "ts_kst": _kst_now_iso(),
        "phase": phase_use,
        "overall": overall,
        "checks": checks,
        "cursor_action": cursor_action,
    }

    if persist:
        report["latest_path"] = persist_deploy_watch_report(report)

    if record_ops:
        try:
            from ops_logger import insert_ops_event

            sev = "INFO"
            if overall == STATUS_WARN:
                sev = "WARNING"
            elif overall == STATUS_BREAK:
                sev = "CRITICAL"
            insert_ops_event(
                component=COMPONENT,
                severity=sev,
                event=EVENT_SUMMARY,
                payload=report,
            )
        except Exception as ex:
            logger.warning("deploy_watch ops_events skip: %s", ex)

    if send_telegram and overall in (STATUS_WARN, STATUS_BREAK):
        report["telegram_sent"] = send_deploy_watch_telegram(
            format_telegram_message(report)
        )
    else:
        report["telegram_sent"] = False

    return report

"""
US 파이프라인 건강 검진·자가 복구 — scan-us / daily-us 직전 SSOT.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz

from market_db_paths import MARKET_DATA_DB_PATH, market_db_read_path


def _db_path() -> str:
    return market_db_read_path()


def _table_row_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        cur = conn.execute(f'SELECT COUNT(*) FROM "{table}"')
        return int((cur.fetchone() or (0,))[0] or 0)
    except sqlite3.Error:
        return 0


def _us_ledger_counts(db_path: str) -> Dict[str, int]:
    out = {"open": 0, "closed": 0, "total": 0}
    if not os.path.isfile(db_path):
        return out
    try:
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            for label, status_like in (
                ("open", "OPEN%"),
                ("closed", "CLOSED%"),
            ):
                cur = conn.execute(
                    """
                    SELECT COUNT(*) FROM forward_trades
                    WHERE UPPER(TRIM(IFNULL(market,'')))='US'
                      AND status LIKE ?
                    """,
                    (status_like,),
                )
                out[label] = int((cur.fetchone() or (0,))[0] or 0)
            cur = conn.execute(
                "SELECT COUNT(*) FROM forward_trades WHERE UPPER(TRIM(IFNULL(market,'')))='US'"
            )
            out["total"] = int((cur.fetchone() or (0,))[0] or 0)
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


def _dna_us_template_count() -> int:
    try:
        from config_manager import load_system_config

        cfg = load_system_config() or {}
        dna = cfg.get("DNA_SUPERNOVA_US_MULTI") or {}
        if isinstance(dna, dict):
            return len(dna)
    except Exception:
        pass
    try:
        from supernova_hunter import _load_time_machine_cache

        tm = _load_time_machine_cache() or {}
        dna = tm.get("DNA_SUPERNOVA_US_MULTI") or {}
        if isinstance(dna, dict):
            return len(dna)
    except Exception:
        pass
    return 0


def _cron_us_scan_installed() -> Optional[bool]:
    path = os.environ.get("FACTORY_CRON_PATH", "/etc/cron.d/dual-screener-factory")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            body = f.read().lower()
        return "scan-us" in body or "--scan-us" in body
    except OSError:
        return None


def assess_us_pipeline_health() -> Dict[str, Any]:
    """진단만 — repair 없음."""
    db = _db_path()
    report: Dict[str, Any] = {
        "db_path": db,
        "db_exists": os.path.isfile(db),
        "timestamp_kst": datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S"),
        "issues": [],
        "warnings": [],
    }

    universe_rows = 0
    universe_source = "fail"
    try:
        from us_list_survival import collect_us_list_survival

        udf, universe_source = collect_us_list_survival(db_path=MARKET_DATA_DB_PATH)
        universe_rows = len(udf) if udf is not None else 0
    except Exception as e:
        report["issues"].append(f"universe_collect_failed: {e}")

    report["universe_rows"] = universe_rows
    report["universe_source"] = universe_source
    if universe_rows < 50:
        report["issues"].append(f"universe_too_small:{universe_rows}")

    spy_rows = qqq_rows = 0
    us_tables = 0
    if report["db_exists"]:
        try:
            conn = sqlite3.connect(db, timeout=30)
            try:
                spy_rows = _table_row_count(conn, "US_SPY")
                qqq_rows = _table_row_count(conn, "US_QQQ")
                cur = conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'US_%'"
                )
                us_tables = int((cur.fetchone() or (0,))[0] or 0)
            finally:
                conn.close()
        except sqlite3.Error as e:
            report["issues"].append(f"db_probe_failed:{e}")

    report["us_spy_rows"] = spy_rows
    report["us_qqq_rows"] = qqq_rows
    report["us_ticker_tables"] = us_tables
    if spy_rows < 100:
        report["issues"].append(f"US_SPY_stale_or_missing:{spy_rows}")
    if qqq_rows < 100:
        report["issues"].append(f"US_QQQ_stale_or_missing:{qqq_rows}")
    if us_tables < 100 and universe_rows >= 50:
        report["warnings"].append(f"us_ohlcv_tables_sparse:{us_tables}")

    ledger = _us_ledger_counts(db)
    report.update({f"ledger_{k}": v for k, v in ledger.items()})
    if ledger["closed"] == 0:
        report["warnings"].append("us_closed_zero:deathmatch_and_dna_idle")

    dna_n = _dna_us_template_count()
    report["dna_us_templates"] = dna_n
    if dna_n == 0:
        report["warnings"].append("DNA_SUPERNOVA_US_MULTI_empty:run_hunt_supernovas_US")

    cron_ok = _cron_us_scan_installed()
    report["cron_us_scan_configured"] = cron_ok
    if cron_ok is False:
        report["warnings"].append("cron_scan_us_not_found:install deploy/factory.crontab.example")

    owner = (os.environ.get("FACTORY_SCAN_OWNER") or "both").strip().lower()
    report["factory_scan_owner"] = owner

    report["needs_repair"] = bool(
        report["issues"]
        or spy_rows < 100
        or qqq_rows < 100
        or universe_rows < 50
    )
    report["critical_failures"] = list(report["issues"])
    return report


def repair_us_pipeline(*, context: str = "scan") -> Dict[str, Any]:
    """증분 OHLCV + 벤치마크 갱신 (가능 시)."""
    result: Dict[str, Any] = {"context": context, "incremental": None}
    try:
        from data_updater import run_us_incremental_db_update

        result["incremental"] = run_us_incremental_db_update()
        if result["incremental"].get("error") == "empty_universe":
            from us_list_survival import collect_us_list_survival

            udf, src = collect_us_list_survival(db_path=MARKET_DATA_DB_PATH)
            result["universe_retry"] = {"rows": len(udf), "source": src}
            if len(udf) >= 50:
                result["incremental"] = run_us_incremental_db_update()
    except Exception as e:
        result["incremental_error"] = str(e)
    return result


def ensure_us_pipeline_ready_for_scan(*, context: str = "scan", repair: bool = True) -> Dict[str, Any]:
    """scan-us / daily-us prelude — 진단 후 필요 시 repair."""
    before = assess_us_pipeline_health()
    out: Dict[str, Any] = {"context": context, "before": before, "repair": None, "after": None}

    if repair and before.get("needs_repair"):
        out["repair"] = repair_us_pipeline(context=context)

    after = assess_us_pipeline_health()
    out["after"] = after

    if after.get("critical_failures"):
        try:
            from factory_meta_alerts import send_meta_critical_alert

            send_meta_critical_alert(
                f"US pipeline blocked ({context})",
                "; ".join(after["critical_failures"][:8]),
                prefix="US_PIPELINE",
            )
        except Exception:
            pass

    return out


def format_us_health_log_line(report: Dict[str, Any]) -> str:
    return (
        f"🇺🇸 US health: universe={report.get('universe_rows')} ({report.get('universe_source')}) "
        f"SPY={report.get('us_spy_rows')} QQQ={report.get('us_qqq_rows')} "
        f"tables={report.get('us_ticker_tables')} "
        f"ledger O/C={report.get('ledger_open')}/{report.get('ledger_closed')} "
        f"DNA={report.get('dna_us_templates')} "
        f"issues={len(report.get('issues') or [])}"
    )

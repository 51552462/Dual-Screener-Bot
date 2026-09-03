"""
L-OBS-01 — 배포 관측 자동 판정 (PASS/WARN/BREAK).

- 결과: factory_data_dir()/deploy_watch_latest.json
- ops_events: component=deploy_watch, event=deploy_watch.summary
- 텔레그램: WARN/BREAK 만 (PASS/SKIP only → 무음)
- Cursor: report.cursor_prompt + ---CURSOR--- JSON (디렉터 붙여넣기 SSOT)
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
BITGET_FACTORY_SERVICE = "dante-bitget-factory.service"

BEAR_UNDERDOG_SHADOW_SUFFIX = "_BEAR_UNDERDOG_SHADOW"
PHASE_POST_BEAR_UNDERDOG_01 = "post_bear_underdog_01"
SHADOW_PAIN_MAE_WARN_MIN_CLOSED = 5
SHADOW_PAIN_MAE_WARN_RATIO = 0.50


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
            # F-GATE-REGISTRY-PATH-01: F-GATE/F-RETIRE 관측은 메인 DB (스냅샷 아님).
            from market_db_paths import MARKET_DATA_DB_PATH

            path = MARKET_DATA_DB_PATH
        except Exception:
            return None
    if not path or not os.path.isfile(path):
        return None
    try:
        return sqlite3.connect(path, timeout=30)
    except sqlite3.Error:
        return None


def _systemd_unit_load_state(unit: str) -> str:
    try:
        proc = subprocess.run(
            ["systemctl", "show", "-p", "LoadState", "--value", unit],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return (proc.stdout or "").strip().lower()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def _systemd_active_or_enabled(unit: str) -> bool:
    for cmd in (["is-active", unit], ["is-enabled", unit]):
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            state = (proc.stdout or "").strip().lower()
            if state in ("active", "enabled", "static"):
                return True
        except (OSError, subprocess.TimeoutExpired):
            continue
    return False


def is_coin_only_deploy_host() -> bool:
    """
    Bot-2 코인 전용 서버 — bitget factory 는 있고 equity dante-factory 는 없음.

    DEPLOY_WATCH_COIN_ONLY=1|0 로 강제 오버라이드 가능.
    DEPLOY_WATCH_EQUITY_HOST=1 이면 항상 주식 호스트로 간주.
    """
    env_coin = os.environ.get("DEPLOY_WATCH_COIN_ONLY")
    if env_coin is not None:
        return str(env_coin).strip().lower() in ("1", "true", "yes", "on")
    if os.environ.get("DEPLOY_WATCH_EQUITY_HOST", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return False
    if platform.system().lower() != "linux":
        return False
    bitget_on = _systemd_active_or_enabled(BITGET_FACTORY_SERVICE)
    equity_on = _systemd_active_or_enabled(DEFAULT_SERVICE)
    return bitget_on and not equity_on


def check_factory_health(
    *,
    service_name: str = DEFAULT_SERVICE,
) -> Dict[str, Any]:
    """systemd active — Linux VPS only."""
    if is_coin_only_deploy_host():
        return {
            "id": "factory_health",
            "status": STATUS_SKIP,
            "detail": "coin_only_host",
        }
    if platform.system().lower() != "linux":
        return {
            "id": "factory_health",
            "status": STATUS_SKIP,
            "detail": "not_linux",
        }
    if _systemd_unit_load_state(service_name) == "not-found":
        return {
            "id": "factory_health",
            "status": STATUS_SKIP,
            "detail": f"{service_name}_not_installed",
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


def _load_watch_sys_config() -> Dict[str, Any]:
    try:
        from config_manager import load_system_config

        cfg = load_system_config()
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def bear_underdog_shadow_tag_enabled_watch(
    sys_config: Optional[Dict[str, Any]] = None,
) -> bool:
    env = os.environ.get("ENABLE_BEAR_UNDERDOG_SHADOW_TAG")
    if env is not None:
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    cfg = sys_config if isinstance(sys_config, dict) else {}
    return bool(cfg.get("ENABLE_BEAR_UNDERDOG_SHADOW_TAG", True))


def query_bear_underdog_metrics(
    conn: sqlite3.Connection,
) -> Dict[str, int]:
    """KR BEAR×incubator underdog shadow 태그·pain cluster 관측 메트릭."""
    suffix_like = f"%{BEAR_UNDERDOG_SHADOW_SUFFIX}%"
    row = conn.execute(
        """
        SELECT
            SUM(
                CASE
                    WHEN UPPER(IFNULL(market,'KR'))='KR'
                     AND IFNULL(sig_type,'') LIKE ?
                    THEN 1 ELSE 0
                END
            ) AS shadow_tag_rows,
            SUM(
                CASE
                    WHEN UPPER(IFNULL(market,'KR'))='KR'
                     AND IFNULL(sig_type,'') LIKE ?
                     AND status='OPEN'
                    THEN 1 ELSE 0
                END
            ) AS shadow_open,
            SUM(
                CASE
                    WHEN UPPER(IFNULL(market,'KR'))='KR'
                     AND IFNULL(sig_type,'') LIKE ?
                     AND status LIKE 'CLOSED%'
                    THEN 1 ELSE 0
                END
            ) AS shadow_closed,
            SUM(
                CASE
                    WHEN UPPER(IFNULL(market,'KR'))='KR'
                     AND IFNULL(sig_type,'') LIKE ?
                     AND status LIKE 'CLOSED%'
                     AND UPPER(IFNULL(exit_type,''))='STAT_MAE'
                    THEN 1 ELSE 0
                END
            ) AS shadow_closed_mae,
            SUM(
                CASE
                    WHEN UPPER(IFNULL(market,'KR'))='KR'
                     AND IFNULL(sig_type,'') LIKE ?
                     AND status LIKE 'CLOSED%'
                     AND UPPER(IFNULL(exit_type,''))='STAT_MAE'
                     AND IFNULL(bars_held, 999) <= 3
                    THEN 1 ELSE 0
                END
            ) AS shadow_closed_mae_le3d,
            SUM(
                CASE
                    WHEN UPPER(IFNULL(market,'KR'))='KR'
                     AND UPPER(TRIM(IFNULL(entry_regime,'')))='BEAR'
                     AND UPPER(IFNULL(sig_type,'')) LIKE '%INCUBATOR%'
                     AND UPPER(IFNULL(sig_type,'')) LIKE '%UNDERDOG%'
                     AND IFNULL(sig_type,'') NOT LIKE ?
                    THEN 1 ELSE 0
                END
            ) AS untagged_kr_bear_incubator
        FROM forward_trades
        """,
        (suffix_like, suffix_like, suffix_like, suffix_like, suffix_like, suffix_like),
    ).fetchone()
    if not row:
        return {
            "shadow_tag_rows": 0,
            "shadow_open": 0,
            "shadow_closed": 0,
            "shadow_closed_mae": 0,
            "shadow_closed_mae_le3d": 0,
            "untagged_kr_bear_incubator": 0,
        }
    return {
        "shadow_tag_rows": int(row[0] or 0),
        "shadow_open": int(row[1] or 0),
        "shadow_closed": int(row[2] or 0),
        "shadow_closed_mae": int(row[3] or 0),
        "shadow_closed_mae_le3d": int(row[4] or 0),
        "untagged_kr_bear_incubator": int(row[5] or 0),
    }


def summarize_bear_underdog_observation(
    *,
    db_path: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """IV 주간 리포트·Cursor 프롬프트용 BEAR×underdog L2 스냅샷."""
    cfg = sys_config if isinstance(sys_config, dict) else _load_watch_sys_config()
    enabled = bear_underdog_shadow_tag_enabled_watch(cfg)
    out: Dict[str, Any] = {
        "tag_enabled": enabled,
        "metrics": {},
        "shadow_mae_ratio": None,
        "pain_cluster_reproducing": False,
    }
    if not enabled:
        out["status"] = STATUS_SKIP
        out["detail"] = "tag_disabled"
        return out

    conn = _connect_market_db(db_path)
    if conn is None:
        out["status"] = STATUS_SKIP
        out["detail"] = "market_db_missing"
        return out
    try:
        if not _table_exists(conn, "forward_trades"):
            out["status"] = STATUS_SKIP
            out["detail"] = "forward_trades_missing"
            return out
        metrics = query_bear_underdog_metrics(conn)
    except sqlite3.Error as ex:
        out["status"] = STATUS_SKIP
        out["detail"] = f"query_failed:{ex}"
        return out
    finally:
        conn.close()

    out["metrics"] = metrics
    closed = int(metrics.get("shadow_closed") or 0)
    mae = int(metrics.get("shadow_closed_mae") or 0)
    if closed > 0:
        ratio = mae / closed
        out["shadow_mae_ratio"] = round(ratio, 4)
        out["pain_cluster_reproducing"] = (
            closed >= SHADOW_PAIN_MAE_WARN_MIN_CLOSED
            and ratio >= SHADOW_PAIN_MAE_WARN_RATIO
        )
    out["status"] = STATUS_PASS
    out["detail"] = (
        f"shadow={metrics.get('shadow_tag_rows', 0)} "
        f"closed_mae={mae}/{closed}"
    )
    return out


def check_c_bear_underdog_01(
    *,
    db_path: Optional[str] = None,
    phase: str = "",
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    CAT-C BEAR-UNDERDOG-01 — shadow suffix depth + 태그 누락·pain cluster 재현 관측.

    phase=post_bear_underdog_01: BEAR incubator underdog인데 suffix 없으면 WARN.
    """
    _id = "c_bear_underdog_01"
    cfg = sys_config if isinstance(sys_config, dict) else _load_watch_sys_config()
    if not bear_underdog_shadow_tag_enabled_watch(cfg):
        return {"id": _id, "status": STATUS_SKIP, "detail": "tag_disabled"}

    conn = _connect_market_db(db_path)
    if conn is None:
        return {"id": _id, "status": STATUS_SKIP, "detail": "market_db_missing"}
    try:
        if not _table_exists(conn, "forward_trades"):
            return {
                "id": "c_bear_underdog_01",
                "status": STATUS_SKIP,
                "detail": "forward_trades_missing",
            }
        metrics = query_bear_underdog_metrics(conn)
    except sqlite3.Error as ex:
        return {"id": _id, "status": STATUS_SKIP, "detail": f"query_failed:{ex}"}
    finally:
        conn.close()

    phase_norm = str(phase or "").strip()
    untagged = int(metrics.get("untagged_kr_bear_incubator") or 0)
    shadow_closed = int(metrics.get("shadow_closed") or 0)
    shadow_mae = int(metrics.get("shadow_closed_mae") or 0)
    shadow_mae_le3d = int(metrics.get("shadow_closed_mae_le3d") or 0)

    status = STATUS_PASS
    reasons: List[str] = []
    detail = (
        f"shadow={metrics.get('shadow_tag_rows', 0)} "
        f"untagged={untagged} mae={shadow_mae}/{shadow_closed}"
    )

    if phase_norm == PHASE_POST_BEAR_UNDERDOG_01 and untagged > 0:
        status = STATUS_WARN
        reasons.append(f"tag_miss:untagged={untagged}")

    if shadow_closed >= SHADOW_PAIN_MAE_WARN_MIN_CLOSED:
        mae_ratio = shadow_mae / shadow_closed
        metrics["shadow_mae_ratio"] = round(mae_ratio, 4)
        if mae_ratio >= SHADOW_PAIN_MAE_WARN_RATIO:
            status = STATUS_WARN
            reasons.append(f"pain_cluster:mae_pct={mae_ratio:.2f}")

    if reasons:
        detail = "; ".join(reasons) + f" ({detail})"

    return {
        "id": _id,
        "status": status,
        "detail": detail,
        "metrics": metrics,
        "observation": {
            "shadow_closed_mae_le3d": shadow_mae_le3d,
            "pain_cluster_reproducing": (
                shadow_closed >= SHADOW_PAIN_MAE_WARN_MIN_CLOSED
                and shadow_mae / shadow_closed >= SHADOW_PAIN_MAE_WARN_RATIO
            ),
        },
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
    for chk in checks:
        if chk.get("id") != "c_bear_underdog_01":
            continue
        obs = chk.get("observation") if isinstance(chk.get("observation"), dict) else {}
        if obs.get("pain_cluster_reproducing"):
            return "OBSERVE_BEAR_UNDERDOG_L2"
        detail = str(chk.get("detail") or "")
        if chk.get("status") == STATUS_WARN and "tag_miss" in detail:
            return "INVESTIGATE_BEAR_UNDERDOG_TAG"

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


def build_deploy_watch_cursor_prompt(report: Dict[str, Any]) -> str:
    """디렉터 → Cursor 첫 메시지용 (텔레그램 ---CURSOR--- 또는 JSON cursor_prompt)."""
    phase = str(report.get("phase") or "")
    action = str(report.get("cursor_action") or "NONE")
    overall = str(report.get("overall") or STATUS_PASS)
    lines = [
        "Track — DEPLOY_WATCH 리뷰. 구현 Handoff 없이 상태 해석·OUTBOX만.",
        f"1) phase={phase} overall={overall} cursor_action={action}",
        "2) SSOT: factory data/deploy_watch_latest.json 또는 아래 JSON",
        "3) docs/work_phases/00_SESSION_SYNC.md §3 · NEXT_ACTION.md 동기화 확인",
    ]
    for chk in report.get("checks") or []:
        if not isinstance(chk, dict):
            continue
        cid = str(chk.get("id") or "")
        st = str(chk.get("status") or "")
        if cid == "c_bear_underdog_01":
            met = chk.get("metrics") if isinstance(chk.get("metrics"), dict) else {}
            obs = chk.get("observation") if isinstance(chk.get("observation"), dict) else {}
            lines.append(
                "4) BEAR_UNDERDOG: "
                f"shadow_rows={met.get('shadow_tag_rows', 0)} "
                f"untagged={met.get('untagged_kr_bear_incubator', 0)} "
                f"shadow_mae={met.get('shadow_closed_mae', 0)}/"
                f"{met.get('shadow_closed', 0)} "
                f"pain_repro={obs.get('pain_cluster_reproducing', False)} "
                f"check={st}"
            )
        elif st in (STATUS_WARN, STATUS_BREAK):
            lines.append(f"· {cid} {st}: {chk.get('detail')}")
    if action == "OBSERVE_BEAR_UNDERDOG_L2":
        lines.append(
            "→ L2 pain cluster 재현 관측 중. hard gate Handoff 보류(n≥30·Claude OK)."
        )
    elif action == "INVESTIGATE_BEAR_UNDERDOG_TAG":
        lines.append(
            "→ BEAR incubator underdog인데 suffix 없음 — 배포·META_REGIME_KEY·코드 경로 점검."
        )
    elif action == "REPORT_TO_CLAUDE":
        lines.append("→ CURSOR_TO_CLAUDE.md OUTBOX append 후 Claude 검증 요청.")
    elif action == "BLOCK_F_RETIRE_02_DEPLOY":
        lines.append("→ factory_health BREAK — F-RETIRE/BEAR 배포 중단·원인 조사.")
    elif action == "NONE" and overall == STATUS_PASS:
        lines.append("→ 조치 없음(PASS). Claude/Cursor 대기.")
    return "\n".join(lines)


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
    prompt = str(report.get("cursor_prompt") or "").strip()
    if prompt:
        lines.append("")
        lines.append(_esc(prompt))
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

    if is_coin_only_deploy_host():
        report: Dict[str, Any] = {
            "schema": "deploy_watch.v2",
            "ts_kst": _kst_now_iso(),
            "phase": phase_use,
            "overall": STATUS_SKIP,
            "checks": [
                {
                    "id": "factory_health",
                    "status": STATUS_SKIP,
                    "detail": "coin_only_host",
                }
            ],
            "cursor_action": "NONE",
            "skipped_reason": "coin_only_host",
            "cursor_prompt": (
                "Track — DEPLOY_WATCH skipped on coin-only host (Bot-2). "
                "Equity deploy_watch runs on stock server factory-kr cron only."
            ),
        }
        if persist:
            report["latest_path"] = persist_deploy_watch_report(report)
        report["telegram_sent"] = False
        return report

    cfg = _load_watch_sys_config()
    checks = [
        check_factory_health(service_name=service_name),
        check_f_gate_01(db_path=db_path),
        check_c_funnel_02(db_path=db_path, baseline_ts=baseline),
        check_f_retire_02(db_path=db_path),
        check_c_bear_underdog_01(
            db_path=db_path,
            phase=phase_use,
            sys_config=cfg,
        ),
        reality_audit_check(db_path=db_path),
    ]
    overall = _worst_status([str(c.get("status") or STATUS_SKIP) for c in checks])
    cursor_action = resolve_cursor_action(checks, phase=phase_use)

    report: Dict[str, Any] = {
        "schema": "deploy_watch.v2",
        "ts_kst": _kst_now_iso(),
        "phase": phase_use,
        "overall": overall,
        "checks": checks,
        "cursor_action": cursor_action,
    }
    report["cursor_prompt"] = build_deploy_watch_cursor_prompt(report)

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

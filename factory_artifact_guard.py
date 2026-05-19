"""
팩토리 파생 자산 자가 치유 — market_data.sqlite(SSOT) 기반 CSV·MetaGovernor JSON 복구.

기동·daily_audit 직전에 `ensure_factory_artifacts()` 를 호출한다.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from factory_data_paths import (
    factory_data_dir,
    flow_csv_path,
    install_root,
    meta_governor_state_path,
    system_config_json_path,
    validated_live_mutants_path,
)
from flow_csv_rebuilder import rebuild_flow_csv_from_sqlite
from market_db_paths import market_db_read_path

logger = logging.getLogger(__name__)


def _max_meta_age_hours() -> float:
    raw = (os.environ.get("FACTORY_META_MAX_AGE_HOURS") or "24").strip()
    try:
        return max(0.25, float(raw))
    except ValueError:
        return 24.0


def _min_csv_bytes() -> int:
    raw = (os.environ.get("FACTORY_CSV_MIN_BYTES") or "128").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 128


def _csv_needs_rebuild(csv_path: str) -> bool:
    if not os.path.isfile(csv_path):
        return True
    try:
        if os.path.getsize(csv_path) < _min_csv_bytes():
            return True
    except OSError:
        return True
    return False


def _meta_age_hours(meta_path: str) -> Optional[float]:
    if not os.path.isfile(meta_path):
        return None
    try:
        mtime = os.path.getmtime(meta_path)
        return (datetime.now(timezone.utc).timestamp() - mtime) / 3600.0
    except OSError:
        return None


def _meta_needs_rebuild(meta_path: str) -> bool:
    try:
        from meta_state_store import is_meta_state_degraded, load_meta_governor_state_unified

        state = load_meta_governor_state_unified(meta_path)
        if is_meta_state_degraded(state):
            return True
    except Exception:
        if not os.path.isfile(meta_path):
            return True
    if not os.path.isfile(meta_path):
        return True
    try:
        from meta_governor import load_meta_governor_state

        state = load_meta_governor_state(meta_path)
    except Exception:
        return True
    status = str(state.get("META_GOVERNOR_LAST_RUN_STATUS") or "").upper()
    if status in ("NEVER", ""):
        return True
    if not state.get("META_GOVERNOR_LAST_RUN_AT"):
        return True
    age = _meta_age_hours(meta_path)
    if age is not None and age > _max_meta_age_hours():
        return True
    try:
        from meta_state_store import is_meta_state_degraded

        return is_meta_state_degraded(state)
    except Exception:
        return False


def _run_meta_governor_cycle(db_path: str) -> str:
    from meta_governor import GovernorRunContext, MetaGovernor, meta_state_path

    data = factory_data_dir()
    root = install_root()
    sys_cfg = system_config_json_path()
    if not os.path.isfile(sys_cfg):
        legacy = os.path.join(
            os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot", "system_config.json"
        )
        sys_cfg = legacy if os.path.isfile(legacy) else None

    bg_db = os.path.join(root, "bitget_market_data.sqlite")
    bg_cfg = os.path.join(root, "bitget_system_config.json")
    val_json = validated_live_mutants_path()

    ctx = GovernorRunContext(
        forward_db_path=db_path if os.path.isfile(db_path) else None,
        system_config_path=sys_cfg,
        bitget_db_path=bg_db if os.path.isfile(bg_db) else None,
        bitget_system_config_path=bg_cfg if os.path.isfile(bg_cfg) else None,
        validated_mutants_path=val_json if os.path.isfile(val_json) else None,
    )
    gov = MetaGovernor(state_path=meta_state_path())
    out = gov.run_governor_cycle(ctx)
    try:
        from meta_state_market_db import ensure_meta_state_log_schema

        ensure_meta_state_log_schema()
    except Exception:
        pass
    try:
        from meta_governor_consumer import invalidate_meta_state_cache

        invalidate_meta_state_cache()
    except Exception:
        pass
    return str(out.get("META_GOVERNOR_LAST_RUN_STATUS") or "OK")


def ensure_factory_artifacts(
    *,
    force_csv: bool = False,
    force_meta: bool = False,
) -> Dict[str, Any]:
    """
    DB가 있으면 파생 CSV·meta_governor_state.json 을 점검·필요 시 재생성.

    Returns dict keys: db, csv, meta, error (optional).
    csv/meta values: ok | rebuilt | failed | skipped
    """
    result: Dict[str, Any] = {
        "db": None,
        "csv": "skipped",
        "meta": "skipped",
    }

    db_path = market_db_read_path()
    result["db"] = db_path

    if not db_path or not os.path.isfile(db_path):
        result["error"] = "no_db"
        result["csv"] = "failed"
        result["meta"] = "failed"
        logger.error("factory_artifact_guard: market DB missing (%s)", db_path)
        return result

    csv_p = flow_csv_path()
    meta_p = meta_governor_state_path()

    if force_csv or _csv_needs_rebuild(csv_p):
        try:
            n = rebuild_flow_csv_from_sqlite(db_path, csv_p)
            result["csv"] = "rebuilt" if n > 0 else "failed"
            if n <= 0:
                logger.warning("factory_artifact_guard: CSV rebuild returned 0 rows")
        except Exception as e:
            result["csv"] = "failed"
            logger.exception("factory_artifact_guard: CSV rebuild failed: %s", e)
    else:
        result["csv"] = "ok"

    if force_meta or _meta_needs_rebuild(meta_p):
        try:
            status = _run_meta_governor_cycle(db_path)
            result["meta"] = "rebuilt"
            result["meta_status"] = status
        except Exception as e:
            result["meta"] = "failed"
            logger.exception("factory_artifact_guard: MetaGovernor cycle failed: %s", e)
    else:
        result["meta"] = "ok"

    logger.info("factory_artifact_guard: %s", result)
    return result


def ensure_meta_governor_state(*, force: bool = False) -> Dict[str, Any]:
    """
    MetaGovernor JSON/SQLite 없음·NEVER·UNKNOWN·stale 시 regime 선행 + governor 자가 치유.
    리포트 [1/9]·[8/9]·ai_overseer 직전 호출용.
    """
    db_path = market_db_read_path()
    if not db_path or not os.path.isfile(db_path):
        return {"meta": "failed", "error": "no_db", "db": db_path}

    meta_p = meta_governor_state_path()
    if not force and not _meta_needs_rebuild(meta_p):
        return {"meta": "ok", "meta_status": "fresh", "path": meta_p}

    out: Dict[str, Any] = {"path": meta_p, "regime": "skipped"}
    try:
        from meta_state_store import regime_analysis_stale_or_missing

        if force or regime_analysis_stale_or_missing():
            from regime_meta_analyzer import analyze_market_regime

            analyze_market_regime()
            out["regime"] = "refreshed"
    except Exception as e:
        out["regime"] = "failed"
        out["regime_error"] = str(e)
        logger.warning("ensure_meta_governor_state: regime refresh failed: %s", e)

    try:
        status = _run_meta_governor_cycle(db_path)
        out["meta"] = "rebuilt"
        out["meta_status"] = status
        return out
    except Exception as e:
        logger.exception("ensure_meta_governor_state failed: %s", e)
        return {"meta": "failed", "error": str(e), **out}


def main() -> None:
    import json
    import sys

    logging.basicConfig(level=logging.INFO)
    out = ensure_factory_artifacts(
        force_csv="--force-csv" in sys.argv,
        force_meta="--force-meta" in sys.argv,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    if out.get("error") == "no_db":
        raise SystemExit(2)
    if out.get("csv") == "failed" or out.get("meta") == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

"""
Bitget 팩토리 파생 자산 자가 치유 — bitget_market_data.sqlite (SSOT).

기동·scan/daily_audit 직전에 `ensure_bitget_artifacts()` 호출.
주식 `factory_artifact_guard.py` 패턴 1:1 이식 (Bitget DB·스키마 전용).
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any, Dict, List, Tuple

from bitget.infra.data_paths import market_data_db_path, meta_governor_state_path

logger = logging.getLogger(__name__)

REQUIRED_TABLES: Tuple[str, ...] = (
    "bitget_forward_trades",
    "bitget_real_execution",
)


def verify_bitget_market_db_schema(*, heal: bool = True) -> Dict[str, Any]:
    """DB 파일 + forward/real 필수 테이블 검문·Self-healing."""
    db_path = market_data_db_path()
    out: Dict[str, Any] = {"path": db_path, "ok": False, "missing": [], "healed": False}

    if not os.path.isfile(db_path):
        out["error"] = "no_db"
        return out

    if heal:
        try:
            from bitget.forward.shared import init_forward_db

            init_forward_db()
            out["healed"] = True
        except Exception as e:
            logger.warning("artifact_guard: init_forward_db heal failed: %s", e)

    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        present = {str(r[0]) for r in rows}
        missing = [t for t in REQUIRED_TABLES if t not in present]
        out["missing"] = missing
        out["ok"] = len(missing) == 0
        if missing:
            out["error"] = "schema_incomplete"
    finally:
        conn.close()

    return out


def _meta_needs_rebuild() -> bool:
    try:
        from bitget.governance.meta_sync import (
            is_bitget_meta_degraded,
            load_bitget_meta_unified,
        )

        return is_bitget_meta_degraded(load_bitget_meta_unified())
    except Exception:
        return not os.path.isfile(meta_governor_state_path())


def ensure_bitget_meta_governor_state(*, force: bool = False) -> Dict[str, Any]:
    """Meta JSON/KV 없음·degraded 시 regime + governor 자가 치유."""
    db_path = market_data_db_path()
    if not os.path.isfile(db_path):
        return {"meta": "failed", "error": "no_db", "db": db_path}

    if not force and not _meta_needs_rebuild():
        return {
            "meta": "ok",
            "meta_status": "fresh",
            "path": meta_governor_state_path(),
        }

    try:
        from bitget.governance.meta_sync import rebuild_bitget_meta_state

        out = rebuild_bitget_meta_state(force=True, refresh_regime=True)
        status = str(out.get("meta_status") or out.get("meta") or "OK")
        return {
            "meta": "rebuilt" if out.get("meta") != "failed" else "failed",
            "meta_status": status,
            "path": meta_governor_state_path(),
            "regime": out.get("regime"),
        }
    except Exception as e:
        logger.exception("ensure_bitget_meta_governor_state failed: %s", e)
        return {"meta": "failed", "error": str(e)}


def ensure_bitget_artifacts(
    *,
    force_meta: bool = False,
    heal_schema: bool = True,
) -> Dict[str, Any]:
    """
    Bitget market DB·스키마·MetaGovernor 상태 점검·필요 시 재생성.

    Returns: db, schema, meta, error (optional).
    """
    result: Dict[str, Any] = {
        "db": None,
        "meta": "skipped",
    }

    schema_out = verify_bitget_market_db_schema(heal=heal_schema)
    result["schema"] = schema_out
    if not schema_out.get("ok"):
        result["error"] = schema_out.get("error") or "schema_incomplete"
        result["meta"] = "failed"
        logger.error(
            "bitget_artifact_guard: schema incomplete (%s): %s",
            schema_out.get("path"),
            schema_out.get("missing"),
        )
        return result

    db_path = market_data_db_path()
    result["db"] = db_path

    if force_meta or _meta_needs_rebuild():
        try:
            heal = ensure_bitget_meta_governor_state(force=force_meta)
            result["meta"] = heal.get("meta", "failed")
            result["meta_status"] = heal.get("meta_status")
            if heal.get("error"):
                result["meta_error"] = heal.get("error")
        except Exception as e:
            result["meta"] = "failed"
            result["meta_error"] = str(e)
            logger.exception("bitget_artifact_guard: meta heal failed: %s", e)
    else:
        result["meta"] = "ok"

    logger.info("bitget_artifact_guard: %s", result)
    return result


def main() -> None:
    import json
    import sys

    logging.basicConfig(level=logging.INFO)
    out = ensure_bitget_artifacts(force_meta="--force-meta" in sys.argv)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    if out.get("error") == "no_db":
        raise SystemExit(2)
    if out.get("error") == "schema_incomplete" or out.get("meta") == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

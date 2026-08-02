"""
L-2 — Bitget integrity backup (BITGET_DB_STORAGE_PATH only).

Online SQLite backup + PRAGMA integrity_check + retention + restore drill.
Does **not** touch trading paths (ledger / execution_safety).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from bitget.infra.clock import utc_compact_key, utc_now_iso
from bitget.scripts.institutional_db_backup import (
    compress_dir,
    integrity_check,
    online_backup,
)

_ARCHIVE_GLOB = "bitget_db_backup_*.tar.gz"
_STOCK_FORBIDDEN = frozenset(
    {
        "market_data.sqlite",
        "market_data_snapshot.sqlite",
        "system_config.sqlite",
    }
)


def bitget_backup_enabled() -> bool:
    env = os.environ.get("BITGET_BACKUP_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("BITGET_BACKUP_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import BITGET_BACKUP_ENABLED

    return bool(BITGET_BACKUP_ENABLED)


def bitget_backup_dir() -> Path:
    env = (os.environ.get("BITGET_BACKUP_DIR") or os.environ.get("BITGET_DB_BACKUP_DIR") or "").strip()
    if env:
        p = Path(env)
    else:
        from bitget.infra.data_paths import bitget_data_dir

        p = Path(bitget_data_dir()) / "backups" / "db"
    p.mkdir(parents=True, exist_ok=True)
    return p


def bitget_storage_root() -> Path:
    from bitget.infra.data_paths import bitget_data_dir

    return Path(bitget_data_dir()).resolve()


def is_bitget_backup_candidate(path: Path, *, data_root: Path) -> bool:
    """True only for Bitget SQLite under storage root — rejects stock DB names."""
    try:
        resolved = path.resolve()
        root = data_root.resolve()
    except OSError:
        return False
    if not str(resolved).startswith(str(root)):
        return False
    name = path.name.lower()
    if name in _STOCK_FORBIDDEN:
        return False
    if name.startswith("bitget_") and name.endswith((".sqlite", ".sqlite3", ".db")):
        return True
    return False


def discover_bitget_storage_sqlite_files(data_root: Optional[Path] = None) -> List[Path]:
    root = data_root or bitget_storage_root()
    if not root.is_dir():
        return []
    found: Dict[str, Path] = {}
    for dirpath, _, filenames in os.walk(root):
        # Skip backup output tree to avoid backing up backups
        if "backups" in Path(dirpath).parts:
            continue
        for name in filenames:
            low = name.lower()
            if "-wal" in low or "-shm" in low or low.endswith("-journal"):
                continue
            p = Path(dirpath) / name
            if not is_bitget_backup_candidate(p, data_root=root):
                continue
            try:
                with open(p, "rb") as f:
                    if f.read(16) != b"SQLite format 3\x00":
                        continue
            except OSError:
                continue
            found[str(p.resolve())] = p.resolve()
    return sorted(found.values(), key=lambda x: str(x))


def _notify_backup_failure(message: str, payload: Optional[dict] = None) -> None:
    try:
        from bitget.infra.ops_logger import insert_ops_event

        insert_ops_event(
            "integrity_backup_l2",
            "ERROR",
            "backup.failed",
            {"message": message, **(payload or {})},
        )
    except Exception:
        pass


def run_bitget_integrity_backup(
    *,
    data_root: Optional[Path] = None,
    out_dir: Optional[Path] = None,
    compress: bool = True,
) -> Dict[str, Any]:
    """
    Online backup all Bitget storage SQLite files.

    On integrity failure: staging removed, previous archives kept, raises RuntimeError.
    """
    if not bitget_backup_enabled():
        return {"enabled": False, "skipped": True}

    root = data_root or bitget_storage_root()
    out = out_dir or bitget_backup_dir()
    backup_id = utc_compact_key()
    staging = out / backup_id
    staging.mkdir(parents=True, exist_ok=True)

    dbs = discover_bitget_storage_sqlite_files(root)
    manifest: List[Dict[str, Any]] = []
    all_ok = True

    for src in dbs:
        rel = src.name
        dst = staging / rel
        entry: Dict[str, Any] = {"source": str(src), "backup_rel": rel}
        try:
            online_backup(src, dst)
            chk = integrity_check(dst)
            entry["integrity_check"] = chk["integrity_check"]
            entry["integrity_ok"] = bool(chk["ok"])
            entry["size"] = dst.stat().st_size if dst.exists() else 0
            all_ok = all_ok and bool(chk["ok"])
        except Exception as ex:
            entry["error"] = str(ex)
            entry["integrity_ok"] = False
            all_ok = False
        manifest.append(entry)

    summary = {
        "backup_id": backup_id,
        "created": utc_now_iso(),
        "data_root": str(root),
        "db_count": len(dbs),
        "all_integrity_ok": all_ok,
        "items": manifest,
    }
    (staging / "manifest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if not all_ok:
        shutil.rmtree(staging, ignore_errors=True)
        _notify_backup_failure("integrity_check failed", {"backup_id": backup_id})
        raise RuntimeError(f"bitget backup integrity failed id={backup_id}")

    archive_name = f"bitget_db_backup_{backup_id}.tar.gz"
    result: Dict[str, Any] = {
        "enabled": True,
        "backup_id": backup_id,
        "all_ok": True,
        "db_count": len(dbs),
    }
    if compress:
        archive_path = out / archive_name
        compress_dir(staging, archive_path)
        result["archive"] = str(archive_path)
        shutil.rmtree(staging, ignore_errors=True)
    else:
        result["staging"] = str(staging)

    daily = int(os.environ.get("BITGET_BACKUP_RETENTION_DAYS", "") or 0) or _retention_days_default()
    weekly = int(os.environ.get("BITGET_BACKUP_WEEKLY_KEEP", "") or 0) or _weekly_keep_default()
    result["pruned"] = apply_backup_retention(out, daily_keep=daily, weekly_keep=weekly)
    return result


def _retention_days_default() -> int:
    from bitget.infra.memory_policy import BITGET_BACKUP_RETENTION_DAYS

    return int(BITGET_BACKUP_RETENTION_DAYS)


def _weekly_keep_default() -> int:
    from bitget.infra.memory_policy import BITGET_BACKUP_WEEKLY_KEEP

    return int(BITGET_BACKUP_WEEKLY_KEEP)


def apply_backup_retention(
    backup_dir: Path,
    *,
    daily_keep: int = 7,
    weekly_keep: int = 4,
) -> int:
    """
    daily×N + weekly×M retention on ``bitget_db_backup_*.tar.gz``.

    Returns count of deleted archives.
    """
    if not backup_dir.is_dir():
        return 0
    archives = sorted(
        backup_dir.glob(_ARCHIVE_GLOB),
        key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
        reverse=True,
    )
    if not archives:
        return 0

    keep: Set[Path] = set()
    now = datetime.now(timezone.utc)
    daily_cutoff = now - timedelta(days=max(1, int(daily_keep)))

    by_day: Dict[str, Path] = {}
    by_week: Dict[str, Path] = {}
    for arc in archives:
        mtime = datetime.fromtimestamp(arc.stat().st_mtime, tz=timezone.utc)
        day_key = mtime.strftime("%Y-%m-%d")
        week_key = f"{mtime.isocalendar().year}-W{mtime.isocalendar().week:02d}"
        if day_key not in by_day or arc.stat().st_mtime > by_day[day_key].stat().st_mtime:
            by_day[day_key] = arc
        if week_key not in by_week or arc.stat().st_mtime > by_week[week_key].stat().st_mtime:
            by_week[week_key] = arc

    for day_key, arc in by_day.items():
        day_dt = datetime.strptime(day_key, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if day_dt >= daily_cutoff.replace(hour=0, minute=0, second=0, microsecond=0):
            keep.add(arc)

    week_keys = sorted(by_week.keys(), reverse=True)[: max(1, int(weekly_keep))]
    for wk in week_keys:
        keep.add(by_week[wk])

    removed = 0
    for arc in archives:
        if arc in keep:
            continue
        try:
            arc.unlink()
            removed += 1
        except OSError:
            pass
    return removed


def _sqlite_table_row_counts(db_path: Path) -> Dict[str, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        out: Dict[str, int] = {}
        for t in tables:
            try:
                out[t] = int(conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0])
            except sqlite3.Error:
                out[t] = -1
        return out
    finally:
        conn.close()


def run_restore_drill(
    *,
    archive_path: Path,
    data_root: Path,
    drill_dir: Path,
) -> Dict[str, Any]:
    """
    Extract backup to isolated path, integrity_check + row-count parity vs live DBs.
    """
    drill_dir.mkdir(parents=True, exist_ok=True)
    extract_root = drill_dir / "extracted"
    if extract_root.exists():
        shutil.rmtree(extract_root, ignore_errors=True)
    extract_root.mkdir(parents=True, exist_ok=True)

    with tarfile.open(archive_path, "r:gz") as tar:
        try:
            tar.extractall(path=extract_root, filter="data")
        except TypeError:
            tar.extractall(path=extract_root)

    staging_dirs = [p for p in extract_root.iterdir() if p.is_dir()]
    if not staging_dirs:
        raise RuntimeError("restore drill: no staging dir in archive")
    staging = staging_dirs[0]
    manifest_path = staging / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError("restore drill: manifest.json missing")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    items = manifest.get("items") or []
    results: List[Dict[str, Any]] = []
    all_ok = True

    for item in items:
        rel = str(item.get("backup_rel") or "")
        src_live = data_root / Path(rel).name
        restored = staging / rel
        entry: Dict[str, Any] = {"backup_rel": rel, "live": str(src_live)}
        if not restored.is_file():
            entry["ok"] = False
            entry["error"] = "restored file missing"
            all_ok = False
            results.append(entry)
            continue
        chk = integrity_check(restored)
        entry["integrity_ok"] = bool(chk["ok"])
        if not chk["ok"]:
            entry["ok"] = False
            all_ok = False
            results.append(entry)
            continue
        if src_live.is_file():
            live_counts = _sqlite_table_row_counts(src_live)
            restored_counts = _sqlite_table_row_counts(restored)
            entry["row_counts_match"] = live_counts == restored_counts
            entry["ok"] = live_counts == restored_counts
            all_ok = all_ok and bool(entry["ok"])
        else:
            entry["ok"] = True
            entry["row_counts_match"] = None
        results.append(entry)

    return {
        "ok": all_ok,
        "archive": str(archive_path),
        "drill_dir": str(drill_dir),
        "items": results,
    }


def latest_backup_archive(backup_dir: Optional[Path] = None) -> Optional[Path]:
    out = backup_dir or bitget_backup_dir()
    archives = sorted(
        out.glob(_ARCHIVE_GLOB),
        key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
        reverse=True,
    )
    return archives[0] if archives else None


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="L-2 Bitget integrity backup")
    parser.add_argument("--job", choices=("backup", "drill"), default="backup")
    parser.add_argument("--archive", default=None, help="restore drill archive path")
    parser.add_argument("--drill-dir", default=None, help="isolated restore drill directory")
    args = parser.parse_args(argv)

    if args.job == "backup":
        if not bitget_backup_enabled():
            print("[integrity_backup_l2] disabled")
            return 0
        res = run_bitget_integrity_backup()
        print(f"[integrity_backup_l2] ok id={res.get('backup_id')} db={res.get('db_count')}")
        return 0

    arc = Path(args.archive) if args.archive else latest_backup_archive()
    if not arc or not arc.is_file():
        print("[integrity_backup_l2] no backup archive for drill", file=sys.stderr)
        return 1
    drill = Path(args.drill_dir) if args.drill_dir else bitget_backup_dir() / "_restore_drill"
    res = run_restore_drill(
        archive_path=arc,
        data_root=bitget_storage_root(),
        drill_dir=drill,
    )
    if not res.get("ok"):
        print(f"[integrity_backup_l2] restore drill FAIL archive={arc}", file=sys.stderr)
        return 2
    print(f"[integrity_backup_l2] restore drill PASS archive={arc}")
    return 0


if __name__ == "__main__":
    import sys

    raise SystemExit(main())

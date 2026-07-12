#!/usr/bin/env python3
"""
기관급 무결성 백업 파이프라인 (Zero-Data-Loss Prep).

인프라 대공사 전에, 현재 워크스페이스의 **모든 SQLite DB** 를 자동 식별하여
런타임 중에도 안전한 SQLite **Online Backup API** 로 복제하고, 복제 직후
`PRAGMA integrity_check` 로 무결성을 검증한 뒤, 타임스탬프 기반 tar.gz 로
압축 보관하고 `RESTORE_GUIDE.md` 를 자동 생성한다.

특징:
  - 단순 `cp` 가 아님 → `sqlite3.Connection.backup()` (Online Backup API).
    WAL 모드로 라이브 writer 가 돌고 있어도 일관된 스냅샷을 뜬다.
  - 복제본마다 `PRAGMA integrity_check` (+ quick_check) 실행, 결과를 매니페스트에 기록.
  - 외부 의존성 없음(표준 라이브러리만).

사용:
  python -m bitget.scripts.institutional_db_backup
  python -m bitget.scripts.institutional_db_backup --out-dir backups/db --no-compress
  python bitget/scripts/institutional_db_backup.py --root /path/to/Dual-Screener-Bot
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tarfile
import time
from pathlib import Path
from typing import Dict, List, Optional

from bitget.infra.clock import utc_compact_key, utc_datetime_str, utc_now_iso
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import DB_BACKUP_KEEP_ARCHIVES

# bitget/scripts/this.py → parents[1]=bitget, parents[2]=repo root
_SCRIPT = Path(__file__).resolve()
_BITGET_ROOT = _SCRIPT.parents[1]
_REPO_ROOT = _SCRIPT.parents[2]
logger = get_logger("bitget.scripts.institutional_db_backup")

# 스캔 제외 디렉터리 (대용량/불필요/재생성 가능)
_EXCLUDE_DIRS = {
    ".git", "venv", ".venv", "env", "__pycache__", "node_modules",
    "backups", ".mypy_cache", ".pytest_cache", ".ruff_cache", "charts",
}
# SQLite 후보 확장자. -wal/-shm/.tmp 는 backup API 가 흡수하므로 제외.
_SQLITE_SUFFIXES = (".sqlite", ".sqlite3", ".db")
_SKIP_SUBSTRINGS = ("-wal", "-shm", ".tmp", "-journal")
_ARCHIVE_GLOB = "dual_screener_db_backup_*.tar.gz"

def _looks_like_sqlite(path: Path) -> bool:
    """헤더 magic 으로 SQLite 파일인지 최종 확인 ('SQLite format 3\\000')."""
    try:
        with open(path, "rb") as f:
            return f.read(16) == b"SQLite format 3\x00"
    except OSError:
        return False


def discover_sqlite_dbs(roots: List[Path]) -> List[Path]:
    """주어진 루트들에서 모든 SQLite DB 파일을 중복 없이 식별한다."""
    found: Dict[str, Path] = {}
    for root in roots:
        if not root or not root.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            # 제외 디렉터리 가지치기
            dirnames[:] = [d for d in dirnames if d not in _EXCLUDE_DIRS]
            for name in filenames:
                low = name.lower()
                if not low.endswith(_SQLITE_SUFFIXES):
                    continue
                if any(s in low for s in _SKIP_SUBSTRINGS):
                    continue
                p = Path(dirpath) / name
                if not _looks_like_sqlite(p):
                    continue
                found[str(p.resolve())] = p.resolve()
    return sorted(found.values(), key=lambda x: str(x))


def _safe_rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        # 루트 밖이면 드라이브/선행 구분자 제거한 평탄화 경로 사용
        return path.as_posix().lstrip("/").replace(":", "")


def online_backup(src_path: Path, dst_path: Path, *, timeout_sec: float = 120.0) -> None:
    """SQLite Online Backup API 로 라이브 안전 복제."""
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    if dst_path.exists():
        dst_path.unlink()
    # 소스는 read-only URI 로 열어 백업 중 실수 쓰기 방지.
    src_uri = f"file:{str(src_path).replace(os.sep, '/')}?mode=ro"
    src = sqlite3.connect(src_uri, uri=True, timeout=timeout_sec)
    try:
        src.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)};")
        dst = sqlite3.connect(str(dst_path), timeout=timeout_sec)
        try:
            # pages=0 → 한 번에 전체 복제 (락 점유 최소화는 progress 콜백으로 조정 가능)
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def integrity_check(db_path: Path, *, timeout_sec: float = 120.0) -> Dict[str, object]:
    """복제본 무결성 검사: integrity_check + quick_check."""
    result: Dict[str, object] = {"integrity_check": "unknown", "quick_check": "unknown", "ok": False}
    conn = sqlite3.connect(str(db_path), timeout=timeout_sec)
    try:
        conn.execute(f"PRAGMA busy_timeout={int(timeout_sec * 1000)};")
        rows = conn.execute("PRAGMA integrity_check;").fetchall()
        integ = [str(r[0]) for r in rows] if rows else ["(empty)"]
        quick = conn.execute("PRAGMA quick_check;").fetchone()
        result["integrity_check"] = integ if len(integ) > 1 else integ[0]
        result["quick_check"] = str(quick[0]) if quick else "(empty)"
        result["ok"] = (integ == ["ok"]) and (str(quick[0]) == "ok" if quick else False)
    finally:
        conn.close()
    return result


_RESTORE_GUIDE_TEMPLATE = """# 🔐 DB 복구 가이드 (RESTORE GUIDE)

> 생성: {created}
> 백업 ID: `{backup_id}`
> 복제 방식: SQLite Online Backup API (`Connection.backup()`) + `PRAGMA integrity_check`

## 1. 백업에 포함된 DB ({n_ok}/{n_total} 무결성 OK)

| 원본 경로 | 백업 파일 | integrity | 크기(byte) |
|---|---|---|---|
{rows}

## 2. 복구 절차 (장애 시)

> ⚠️ 복구 전 반드시 현재 데몬을 모두 정지하라.

```bash
# (1) 모든 데몬/타이머 정지 — DB writer 차단
sudo systemctl stop 'dante-bitget-*' || true
sudo systemctl stop 'dante-factory-*' || true
# (cron 은 일시적으로 비활성화하거나 install 스크립트로 제거)

# (2) tar.gz 해제
cd {repo_root}
tar -xzf {archive_name} -C /tmp/db_restore

# (3) 손상된 원본을 백업본으로 교체 (예시 — 표의 '원본 경로' 참고)
#     WAL/SHM 잔여물도 함께 제거해야 깨끗하게 복구된다.
#   cp /tmp/db_restore/<백업파일> <원본 경로>
#   rm -f <원본 경로>-wal <원본 경로>-shm

# (4) 무결성 재검증
#   sqlite3 <원본 경로> 'PRAGMA integrity_check;'   # → ok 확인

# (5) 데몬 재기동
sudo systemctl start 'dante-bitget-*'
```

## 3. 복구 검증 체크리스트
- [ ] 교체한 DB 의 `PRAGMA integrity_check` 가 `ok`
- [ ] `-wal` / `-shm` 잔여 파일 제거됨
- [ ] 데몬 heartbeat 정상 (watchdog 알람 없음)
- [ ] 최근 데이터 워터마크가 장애 직전 시각과 근접
"""


def write_restore_guide(staging_dir: Path, backup_id: str, archive_name: str,
                        manifest: List[Dict[str, object]], repo_root: Path) -> Path:
    rows = []
    n_ok = 0
    for item in manifest:
        ok = bool(item.get("integrity_ok"))
        n_ok += 1 if ok else 0
        badge = "✅ ok" if ok else f"❌ {item.get('integrity_check')}"
        rows.append(
            f"| `{item.get('source')}` | `{item.get('backup_rel')}` | {badge} | {item.get('size', 0)} |"
        )
    guide = _RESTORE_GUIDE_TEMPLATE.format(
        created=utc_datetime_str(),
        backup_id=backup_id,
        n_ok=n_ok,
        n_total=len(manifest),
        rows="\n".join(rows) if rows else "| (none) | | | |",
        repo_root=str(repo_root),
        archive_name=archive_name,
    )
    p = staging_dir / "RESTORE_GUIDE.md"
    p.write_text(guide, encoding="utf-8")
    return p


def compress_dir(staging_dir: Path, archive_path: Path) -> Path:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(staging_dir, arcname=staging_dir.name)
    return archive_path


def default_out_dir() -> Path:
    """Prefer BITGET_DB_BACKUP_DIR → data_dir/backups/db → repo/backups/db."""
    env = (os.environ.get("BITGET_DB_BACKUP_DIR") or "").strip()
    if env:
        return Path(env)
    try:
        from bitget.infra.data_paths import bitget_data_dir

        return Path(bitget_data_dir()) / "backups" / "db"
    except Exception:
        return _REPO_ROOT / "backups" / "db"


def prune_old_archives(out_dir: Path, *, keep: int = DB_BACKUP_KEEP_ARCHIVES) -> int:
    """Keep newest N tar.gz archives; delete older. Disk budget for 4GB boxes."""
    keep_n = max(1, int(keep))
    if not out_dir.is_dir():
        return 0
    archives = sorted(
        out_dir.glob(_ARCHIVE_GLOB),
        key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
        reverse=True,
    )
    removed = 0
    for old in archives[keep_n:]:
        try:
            old.unlink()
            removed += 1
            logger.info("[backup] pruned %s", old.name)
        except OSError as e:
            logger.warning("[backup] prune skip %s: %s", old, e)
    return removed


def run_backup(
    *,
    roots: List[Path],
    out_dir: Path,
    compress: bool = True,
    keep_staging: bool = False,
    keep_archives: Optional[int] = None,
) -> Dict[str, object]:
    backup_id = utc_compact_key()
    staging = out_dir / backup_id
    staging.mkdir(parents=True, exist_ok=True)

    dbs = discover_sqlite_dbs(roots)
    logger.info("[backup] discovered %s SQLite DB(s)", len(dbs))
    manifest: List[Dict[str, object]] = []
    all_ok = True

    for src in dbs:
        rel = _safe_rel(src, _REPO_ROOT)
        dst = staging / "data" / rel
        entry: Dict[str, object] = {"source": str(src), "backup_rel": f"data/{rel}"}
        try:
            online_backup(src, dst)
            chk = integrity_check(dst)
            entry["integrity_check"] = chk["integrity_check"]
            entry["quick_check"] = chk["quick_check"]
            entry["integrity_ok"] = bool(chk["ok"])
            entry["size"] = dst.stat().st_size if dst.exists() else 0
            all_ok = all_ok and bool(chk["ok"])
            status = "OK" if chk["ok"] else "INTEGRITY-FAIL"
            logger.info("  [%s] %s (%s bytes)", status, rel, entry["size"])
        except Exception as e:  # noqa: BLE001
            entry["error"] = str(e)
            entry["integrity_ok"] = False
            all_ok = False
            log_exception(logger, "  [ERROR] %s: %s", rel, e)
        manifest.append(entry)

    summary = {
        "backup_id": backup_id,
        "created": utc_now_iso(),
        "repo_root": str(_REPO_ROOT),
        "db_count": len(dbs),
        "all_integrity_ok": all_ok,
        "items": manifest,
    }
    (staging / "manifest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    archive_name = f"dual_screener_db_backup_{backup_id}.tar.gz"
    write_restore_guide(staging, backup_id, archive_name, manifest, _REPO_ROOT)

    result: Dict[str, object] = {"backup_id": backup_id, "staging": str(staging), "all_ok": all_ok}
    if compress:
        archive_path = out_dir / archive_name
        compress_dir(staging, archive_path)
        result["archive"] = str(archive_path)
        logger.info("[backup] compressed → %s", archive_path)
        if not keep_staging:
            import shutil

            shutil.rmtree(staging, ignore_errors=True)
            result["staging"] = "(removed)"
    keep_n = DB_BACKUP_KEEP_ARCHIVES if keep_archives is None else int(keep_archives)
    result["pruned"] = prune_old_archives(out_dir, keep=keep_n)
    logger.info("[backup] DONE id=%s all_integrity_ok=%s pruned=%s", backup_id, all_ok, result["pruned"])
    return result


def run_backup_job(
    *,
    roots: Optional[List[Path]] = None,
    out_dir: Optional[Path] = None,
    compress: bool = True,
    keep_staging: bool = False,
    keep_archives: Optional[int] = None,
) -> Dict[str, object]:
    """Cron/pipeline entry — backup + prune + stamped-log GC (disk survival)."""
    res = run_backup(
        roots=roots or _default_roots(),
        out_dir=out_dir or default_out_dir(),
        compress=compress,
        keep_staging=keep_staging,
        keep_archives=keep_archives,
    )
    try:
        from bitget.disk_manager import cleanup_stamped_shell_logs

        res["stamped_logs_removed"] = cleanup_stamped_shell_logs()
    except Exception as e:
        log_exception(logger, "[backup] stamped log cleanup skip: %s", e)
        res["stamped_logs_removed"] = 0
    return res


def _default_roots() -> List[Path]:
    roots = [_REPO_ROOT]
    # 데이터 디렉터리가 레포 밖(env override)일 수 있으므로 best-effort 로 추가.
    try:
        sys.path.insert(0, str(_REPO_ROOT))
        from bitget.infra.data_paths import bitget_data_dir

        roots.append(Path(bitget_data_dir()))
    except Exception:
        pass
    # 중복 제거
    uniq: Dict[str, Path] = {}
    for r in roots:
        try:
            uniq[str(r.resolve())] = r.resolve()
        except OSError:
            pass
    return list(uniq.values())


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="기관급 SQLite 무결성 백업")
    parser.add_argument(
        "--root",
        action="append",
        default=None,
        help="스캔 루트 (반복 지정 가능). 미지정 시 레포 루트 + bitget 데이터 디렉터리.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="백업 저장 위치 (기본 BITGET_DB_BACKUP_DIR 또는 data_dir/backups/db)",
    )
    parser.add_argument("--no-compress", action="store_true", help="tar.gz 압축 생략")
    parser.add_argument("--keep-staging", action="store_true", help="압축 후 staging 폴더 유지")
    parser.add_argument(
        "--keep",
        type=int,
        default=None,
        help=f"보존할 tar.gz 개수 (기본 {DB_BACKUP_KEEP_ARCHIVES})",
    )
    parser.add_argument(
        "--no-log-gc",
        action="store_true",
        help="stamped shell log GC 생략",
    )
    args = parser.parse_args(argv)

    roots = [Path(r) for r in args.root] if args.root else _default_roots()
    out_dir = Path(args.out_dir) if args.out_dir else default_out_dir()
    if args.no_log_gc:
        res = run_backup(
            roots=roots,
            out_dir=out_dir,
            compress=not args.no_compress,
            keep_staging=args.keep_staging,
            keep_archives=args.keep,
        )
    else:
        res = run_backup_job(
            roots=roots,
            out_dir=out_dir,
            compress=not args.no_compress,
            keep_staging=args.keep_staging,
            keep_archives=args.keep,
        )
    # 무결성 실패가 하나라도 있으면 비정상 종료코드 → CI/운영 스크립트가 감지.
    return 0 if res.get("all_ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())

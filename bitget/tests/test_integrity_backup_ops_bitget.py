"""Ops survival — integrity DB backup prune + stamped shell log GC."""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path


def test_is_stamped_shell_log_never_touches_rotating_set():
    from bitget.disk_manager import is_stamped_shell_log

    assert is_stamped_shell_log("bitget_health_20260711_000500.log")
    assert is_stamped_shell_log("bitget_daemon_20260711_120000.log")
    assert is_stamped_shell_log("bitget_ws_20260711_120000.log")
    assert not is_stamped_shell_log("bitget.log")
    assert not is_stamped_shell_log("bitget.log.1")
    assert not is_stamped_shell_log("bitget.log.5")
    assert not is_stamped_shell_log("other_20260711_000500.log")


def test_cleanup_stamped_shell_logs_respects_ttl(tmp_path):
    from bitget.disk_manager import cleanup_stamped_shell_logs

    old = tmp_path / "bitget_canary_20260101_000000.log"
    fresh = tmp_path / "bitget_health_20990101_000000.log"
    rotating = tmp_path / "bitget.log"
    old.write_text("old", encoding="utf-8")
    fresh.write_text("fresh", encoding="utf-8")
    rotating.write_text("rotate", encoding="utf-8")
    aged = time.time() - (10 * 86400)
    os.utime(old, (aged, aged))

    removed = cleanup_stamped_shell_logs(str(tmp_path), retention_days=5)
    assert removed == 1
    assert not old.exists()
    assert fresh.exists()
    assert rotating.exists()


def test_prune_old_archives_keeps_newest(tmp_path):
    from bitget.scripts.institutional_db_backup import prune_old_archives

    names = []
    for i in range(5):
        p = tmp_path / f"dual_screener_db_backup_2026070{i + 1}T000000Z.tar.gz"
        p.write_bytes(b"x")
        os.utime(p, (1000 + i, 1000 + i))
        names.append(p.name)

    removed = prune_old_archives(tmp_path, keep=2)
    assert removed == 3
    remaining = sorted(p.name for p in tmp_path.glob("dual_screener_db_backup_*.tar.gz"))
    assert remaining == [names[3], names[4]]


def test_online_backup_integrity_roundtrip(tmp_path):
    from bitget.scripts.institutional_db_backup import integrity_check, online_backup, run_backup

    src = tmp_path / "src.sqlite"
    conn = sqlite3.connect(str(src))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t(v) VALUES ('ok')")
    conn.commit()
    conn.close()

    out = tmp_path / "out"
    res = run_backup(roots=[tmp_path], out_dir=out, compress=True, keep_archives=3)
    assert res["all_ok"] is True
    assert Path(res["archive"]).is_file()

    dst = tmp_path / "copy.sqlite"
    online_backup(src, dst)
    assert integrity_check(dst)["ok"] is True


def test_db_backup_mode_registered():
    from bitget.infra.runtime import BITGET_MODES
    from bitget.pipelines.bitget_pipelines import PIPELINE_BUILDERS, get_pipeline

    assert "db_backup" in BITGET_MODES
    assert "db_backup" in PIPELINE_BUILDERS
    names = [s.name for s in get_pipeline("db_backup")]
    assert "institutional_db_backup" in names


def test_integrity_backup_architecture_guard():
    from bitget.validation.architecture_checks import check_integrity_backup_ssot

    r = check_integrity_backup_ssot()
    assert r["ok"] is True, r

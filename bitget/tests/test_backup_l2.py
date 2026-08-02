"""L-2 — integrity backup cron (BITGET_DB_STORAGE_PATH only)."""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from unittest import mock

import pytest


def _make_sqlite(path: Path, table: str = "t", rows: int = 3) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, v TEXT)")
        for i in range(rows):
            conn.execute(f"INSERT INTO {table} VALUES (?,?)", (i, f"v{i}"))
        conn.commit()
    finally:
        conn.close()


class TestBackupCandidateFilter:
    def test_bitget_sqlite_allowed(self, tmp_path):
        from bitget.infra.integrity_backup_l2 import is_bitget_backup_candidate

        p = tmp_path / "bitget_market_data.sqlite"
        _make_sqlite(p)
        assert is_bitget_backup_candidate(p, data_root=tmp_path)

    def test_stock_market_data_rejected(self, tmp_path):
        from bitget.infra.integrity_backup_l2 import (
            discover_bitget_storage_sqlite_files,
            is_bitget_backup_candidate,
        )

        stock = tmp_path / "market_data.sqlite"
        bitget = tmp_path / "bitget_system_config.sqlite"
        _make_sqlite(stock)
        _make_sqlite(bitget)
        assert not is_bitget_backup_candidate(stock, data_root=tmp_path)
        found = discover_bitget_storage_sqlite_files(tmp_path)
        names = {p.name for p in found}
        assert "bitget_system_config.sqlite" in names
        assert "market_data.sqlite" not in names


class TestIntegrityBackup:
    def test_online_backup_integrity_pass(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BITGET_BACKUP_ENABLED", "1")
        data = tmp_path / "data"
        out = tmp_path / "backups"
        db = data / "bitget_market_data.sqlite"
        _make_sqlite(db, rows=5)

        with mock.patch(
            "bitget.infra.integrity_backup_l2.bitget_storage_root", return_value=data
        ), mock.patch("bitget.infra.integrity_backup_l2.bitget_backup_dir", return_value=out):
            from bitget.infra.integrity_backup_l2 import run_bitget_integrity_backup

            res = run_bitget_integrity_backup(data_root=data, out_dir=out)
        assert res["all_ok"] is True
        assert res["db_count"] == 1
        archives = list(out.glob("bitget_db_backup_*.tar.gz"))
        assert len(archives) == 1

    def test_integrity_fail_keeps_no_new_archive(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BITGET_BACKUP_ENABLED", "1")
        data = tmp_path / "data"
        out = tmp_path / "backups"
        db = data / "bitget_market_data.sqlite"
        _make_sqlite(db)

        with mock.patch(
            "bitget.infra.integrity_backup_l2.bitget_storage_root", return_value=data
        ), mock.patch("bitget.infra.integrity_backup_l2.bitget_backup_dir", return_value=out), mock.patch(
            "bitget.infra.integrity_backup_l2.integrity_check",
            return_value={"ok": False, "integrity_check": "fail"},
        ):
            from bitget.infra.integrity_backup_l2 import run_bitget_integrity_backup

            with pytest.raises(RuntimeError):
                run_bitget_integrity_backup(data_root=data, out_dir=out)
        assert list(out.glob("bitget_db_backup_*.tar.gz")) == []


class TestRestoreDrill:
    def test_restore_drill_row_parity(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BITGET_BACKUP_ENABLED", "1")
        data = tmp_path / "data"
        out = tmp_path / "backups"
        drill = tmp_path / "drill"
        _make_sqlite(data / "bitget_market_data.sqlite", rows=4)

        with mock.patch(
            "bitget.infra.integrity_backup_l2.bitget_storage_root", return_value=data
        ), mock.patch("bitget.infra.integrity_backup_l2.bitget_backup_dir", return_value=out):
            from bitget.infra.integrity_backup_l2 import (
                run_bitget_integrity_backup,
                run_restore_drill,
            )

            res = run_bitget_integrity_backup(data_root=data, out_dir=out)
            arc = Path(res["archive"])
            drill_res = run_restore_drill(
                archive_path=arc, data_root=data, drill_dir=drill
            )
        assert drill_res["ok"] is True
        assert drill_res["items"][0]["row_counts_match"] is True


class TestRetention:
    def test_prunes_excess_archives(self, tmp_path):
        from bitget.infra.integrity_backup_l2 import apply_backup_retention

        out = tmp_path / "bk"
        out.mkdir()
        now = time.time()
        for i in range(12):
            p = out / f"bitget_db_backup_2026070{i}T000000Z.tar.gz"
            p.write_bytes(b"x")
            os.utime(p, (now - i * 86400, now - i * 86400))
        removed = apply_backup_retention(out, daily_keep=7, weekly_keep=4)
        remaining = list(out.glob("bitget_db_backup_*.tar.gz"))
        assert len(remaining) <= 11
        assert removed >= 1


class TestTradingPathIsolation:
    def test_module_does_not_import_trading_paths(self):
        text = (
            Path(__file__).resolve().parents[1] / "infra" / "integrity_backup_l2.py"
        ).read_text(encoding="utf-8")
        assert "from bitget.trading.execution_safety" not in text
        assert "from bitget.forward import ledger" not in text
        assert "from bitget.forward.ledger" not in text
        assert "from bitget.infra.config_manager" not in text

    def test_deploy_scripts_exist(self):
        root = Path(__file__).resolve().parents[1] / "deploy"
        assert (root / "backup_bitget_db.sh").is_file()
        assert (root / "install_bitget_backup.sh").is_file()
        assert (root / "scripts" / "bitget_restore_drill.sh").is_file()
        timer = (root / "systemd" / "dante-bitget-backup.timer").read_text(encoding="utf-8")
        assert "dante-bitget-backup.service" in timer

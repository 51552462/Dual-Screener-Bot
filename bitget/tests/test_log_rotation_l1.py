"""L-1 — log rotation deploy SSOT (logrotate + journal vacuum)."""
from __future__ import annotations

import os
import re
from pathlib import Path


_BITGET_ROOT = Path(__file__).resolve().parents[1]
_DEPLOY = _BITGET_ROOT / "deploy"


def test_logrotate_template_has_stamped_and_rotating_paths():
    text = (_DEPLOY / "logrotate" / "bitget-dante.conf.in").read_text(encoding="utf-8")
    assert "bitget_*.log" in text
    assert "bitget.log" in text
    assert "copytruncate" in text
    assert "@@BITGET_LOG_DIR@@" in text


def test_journal_vacuum_script_reads_env_defaults():
    script = (_DEPLOY / "scripts" / "bitget_journal_vacuum.sh").read_text(encoding="utf-8")
    assert "BITGET_JOURNAL_MAX_USE" in script
    assert "BITGET_JOURNAL_MAX_RETENTION" in script
    assert "journalctl --vacuum-size" in script
    assert "dante-bitget-factory" in script


def test_install_script_supports_dry_run():
    script = (_DEPLOY / "install_bitget_logrotate.sh").read_text(encoding="utf-8")
    assert "--test" in script
    assert "logrotate -d" in script
    assert "dante-bitget-journal-vacuum.timer" in script


def test_journal_vacuum_systemd_units_exist():
    svc = (_DEPLOY / "systemd" / "dante-bitget-journal-vacuum.service.in").read_text(
        encoding="utf-8"
    )
    assert "bitget_journal_vacuum.sh" in svc
    timer = (_DEPLOY / "systemd" / "dante-bitget-journal-vacuum.timer").read_text(
        encoding="utf-8"
    )
    assert "Persistent=true" in timer


def test_stamped_log_retention_env_override(tmp_path, monkeypatch):
    from bitget.disk_manager import cleanup_stamped_shell_logs

    old = tmp_path / "bitget_health_20260101_000000.log"
    old.write_text("x", encoding="utf-8")
    import time

    os.utime(old, (time.time() - 20 * 86400, time.time() - 20 * 86400))
    monkeypatch.setenv("BITGET_STAMPED_LOG_RETENTION_DAYS", "14")
    removed = cleanup_stamped_shell_logs(str(tmp_path))
    assert removed == 1


def test_rotating_file_handler_unchanged_by_l1():
    """L-1 must not alter RotatingFileHandler SSOT constants."""
    from bitget.infra.memory_policy import LOG_ROTATE_BACKUP_COUNT, LOG_ROTATE_MAX_BYTES

    assert LOG_ROTATE_MAX_BYTES == 50 * 1024 * 1024
    assert LOG_ROTATE_BACKUP_COUNT == 5


def test_logrotate_render_substitutes_log_dir(tmp_path):
    template = (_DEPLOY / "logrotate" / "bitget-dante.conf.in").read_text(encoding="utf-8")
    log_dir = str(tmp_path / "logs")
    rendered = template.replace("@@BITGET_LOG_DIR@@", log_dir)
    assert log_dir in rendered
    assert "@@BITGET_LOG_DIR@@" not in rendered
    assert len(re.findall(r"bitget_\*\.log", rendered)) >= 1

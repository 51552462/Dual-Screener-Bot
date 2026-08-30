"""Deployment bridge tests — no SSH, AI, Telegram, or systemd writes."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys

from dev_autonomy.paths import REPO_ROOT


SCRIPTS = (
    "deploy/install_dev_autonomy.sh",
    "deploy/auth_dev_autonomy_ai.sh",
    "deploy/audit_dev_autonomy.sh",
    "deploy/entrypoints/run_dev_autonomy_service.sh",
)


def test_deployment_shell_scripts_parse():
    for relative in SCRIPTS:
        proc = subprocess.run(
            ["bash", "-n", relative],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, f"{relative}: {proc.stderr}"


def test_systemd_unit_is_non_root_and_role_isolated():
    unit = (REPO_ROOT / "deploy/systemd/quant-dev-autonomy@.service.in").read_text(encoding="utf-8")
    assert "User=@@RUN_USER@@" in unit
    assert "run_dev_autonomy_service.sh %i" in unit
    assert "NoNewPrivileges=true" in unit
    assert "EnvironmentFile=-@@INSTALL_ROOT@@/.env" in unit
    assert "EnvironmentFile=-@@INSTALL_ROOT@@/bitget/.env" in unit
    assert "--force" not in unit
    assert "ssh" not in unit.lower()


def test_timer_has_explicit_korea_weekday_and_weekend_schedule():
    timer = (REPO_ROOT / "deploy/systemd/quant-dev-autonomy@.timer").read_text(encoding="utf-8")
    assert "Mon..Fri" in timer
    assert "Sat,Sun" in timer
    assert "Asia/Seoul" in timer
    assert "Unit=quant-dev-autonomy@%i.service" in timer


def _base_env(tmp_path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "AUTONOMY_PYTHON": sys.executable,
            "AUTONOMY_RUNTIME_ROOT": str(tmp_path / "runtime"),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return env


def test_stock_entrypoint_dry_run(tmp_path):
    data = tmp_path / "stock"
    data.mkdir()
    (data / "dual_north_star_ledger.json").write_text(
        json.dumps({"updated_at": "2026-08-30T00:00:00Z", "latest": {"tracks": {}}}),
        encoding="utf-8",
    )
    env = _base_env(tmp_path)
    env["DB_STORAGE_PATH"] = str(data)
    proc = subprocess.run(
        ["bash", "deploy/entrypoints/run_dev_autonomy_service.sh", "stock", "--dry-run"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["mode"] == "DRY_RUN"
    assert payload["execution_authorized"] is False


def test_bitget_entrypoint_dry_run_has_no_cross_server_ssot(tmp_path):
    data = tmp_path / "bitget"
    data.mkdir()
    db = data / "bitget_ops_events.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE ops_events ("
            "id INTEGER PRIMARY KEY, ts_utc TEXT, severity TEXT, event TEXT, payload_json TEXT)"
        )
    env = _base_env(tmp_path)
    env["BITGET_DB_STORAGE_PATH"] = str(data)
    proc = subprocess.run(
        ["bash", "deploy/entrypoints/run_dev_autonomy_service.sh", "bitget", "--dry-run"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["reports"] == []
    assert payload["errors"] == []

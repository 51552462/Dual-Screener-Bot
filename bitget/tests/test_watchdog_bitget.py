"""Bitget watchdog — circuit breaker + multi-unit restart matrix."""
from __future__ import annotations

from bitget import watchdog
from bitget.infra.clock import parse_utc_iso, utc_hours_ago_iso, utc_now, utc_now_iso


def test_record_job_failure_opens_circuit(tmp_path, monkeypatch):
    monkeypatch.setattr(watchdog, "_state_dir", lambda: tmp_path)
    monkeypatch.setenv("BITGET_SCAN_CB_THRESHOLD", "2")

    opened1, _ = watchdog.record_job_failure("scan_spot_dante", error="timeout")
    assert opened1 is False
    opened2, status = watchdog.record_job_failure("scan_spot_dante", error="timeout")
    assert opened2 is True
    assert "OPEN" in status

    is_open, reason = watchdog.is_circuit_open("scan_spot_dante")
    assert is_open is True
    assert "OPEN" in reason


def test_record_job_success_closes_circuit(tmp_path, monkeypatch):
    monkeypatch.setattr(watchdog, "_state_dir", lambda: tmp_path)
    monkeypatch.setenv("BITGET_SCAN_CB_THRESHOLD", "1")

    watchdog.record_job_failure("scan_futures", error="lock")
    assert watchdog.is_circuit_open("scan_futures")[0] is True

    watchdog.record_job_success("scan_futures")
    assert watchdog.is_circuit_open("scan_futures")[0] is False


def test_latest_heartbeat_ts_picks_newest_component(tmp_path):
    import sqlite3

    db = tmp_path / "ops.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute(
        """
        CREATE TABLE ops_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            component TEXT NOT NULL,
            severity TEXT NOT NULL,
            event TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    older = utc_hours_ago_iso(2.0)
    newer = utc_now_iso()
    conn.executemany(
        "INSERT INTO ops_events (ts_utc, component, severity, event, payload_json) VALUES (?,?,?,?,?)",
        [
            (older, "bitget_auto_pilot", "INFO", "heartbeat.tick", "{}"),
            (newer, "bitget.main", "INFO", "heartbeat.tick", "{}"),
        ],
    )
    conn.commit()
    conn.close()

    ts, comp = watchdog._latest_heartbeat_ts(str(db), ("bitget_auto_pilot", "bitget.main"))
    assert comp == "bitget.main"
    assert parse_utc_iso(ts) == parse_utc_iso(newer)
    assert (utc_now() - parse_utc_iso(ts)).total_seconds() >= 0


def test_watchdog_module_has_no_print():
    import inspect

    src = inspect.getsource(watchdog)
    assert "print(" not in src
    assert "log_exception" in src
    assert "get_logger" in src


def test_evaluate_ws_plane_health():
    ok, _ = watchdog.evaluate_ws_plane_health(
        {"public_ws": {"enabled": False}}, buf_stale_sec=300.0
    )
    assert ok is False

    bad, reason = watchdog.evaluate_ws_plane_health(
        {"public_ws": {"enabled": True, "started": False}}, buf_stale_sec=300.0
    )
    assert bad is True
    assert "started=false" in reason

    stale, reason2 = watchdog.evaluate_ws_plane_health(
        {"private_ws": {"enabled": True, "started": True, "buf_age_sec": 400.0}},
        buf_stale_sec=300.0,
    )
    assert stale is True
    assert "buf_age" in reason2


def test_restart_budget_caps_per_hour(tmp_path, monkeypatch):
    monkeypatch.setenv("BITGET_WATCHDOG_MAX_RESTARTS_PER_HOUR", "2")
    now = 1_000_000.0
    assert watchdog.restart_budget_ok(tmp_path, "factory", now=now) is True
    watchdog.record_unit_restart(tmp_path, "factory", now=now)
    watchdog.record_unit_restart(tmp_path, "factory", now=now + 10)
    assert watchdog.restart_budget_ok(tmp_path, "factory", now=now + 20) is False
    # Independent unit still has budget
    assert watchdog.restart_budget_ok(tmp_path, "queue_worker", now=now + 20) is True


def test_execute_unit_restart_respects_budget(tmp_path, monkeypatch):
    monkeypatch.setenv("BITGET_WATCHDOG_MAX_RESTARTS_PER_HOUR", "1")
    monkeypatch.setattr(watchdog, "_send_bitget_telegram", lambda _t: True)
    monkeypatch.setattr(watchdog, "_telegram_cooldown_elapsed", lambda *a, **k: True)
    calls = {"n": 0}

    def _sys(_cmd):
        calls["n"] += 1
        return 0

    monkeypatch.setattr(watchdog.os, "system", _sys)
    r1 = watchdog.execute_unit_restart(
        tmp_path, unit="queue_worker", msg="t1", cooldown=1.0, dry_run=False
    )
    assert r1["restarted"] is True
    assert calls["n"] == 1
    r2 = watchdog.execute_unit_restart(
        tmp_path, unit="queue_worker", msg="t2", cooldown=1.0, dry_run=False
    )
    assert r2["budget_ok"] is False
    assert r2["restarted"] is False
    assert calls["n"] == 1


def test_unit_restart_cmds_align_with_sudoers():
    assert "dante-bitget-factory" in watchdog.unit_restart_cmd("factory")
    assert "dante-bitget-queue-worker" in watchdog.unit_restart_cmd("queue_worker")
    assert "dante-bitget-ws" in watchdog.unit_restart_cmd("ws")
    assert "dante-bitget-async" in watchdog.unit_restart_cmd("async_telegram")


def test_async_ops_logger_patch_points_at_bitget():
    from bitget import async_telegram_daemon as atd

    assert hasattr(atd, "_patch_bitget_ops_logger")
    import async_telegram_daemon as root_atd
    import bitget.infra.ops_logger as bg_ops

    atd._patch_bitget_ops_logger()
    assert root_atd.ops_logger is bg_ops


def test_async_plane_restarts_on_stale_hb(tmp_path, monkeypatch):
    monkeypatch.setenv("BITGET_WATCHDOG_RESTART_ASYNC", "1")
    monkeypatch.setenv("BITGET_WATCHDOG_ASYNC_STALE_SEC", "60")
    monkeypatch.setattr(
        watchdog,
        "_latest_heartbeat_row",
        lambda db, comps: (None, None, {}),
    )
    restarts = []

    def _exec(state_dir, **kwargs):
        restarts.append(kwargs.get("unit"))
        return {"restarted": True, "budget_ok": True}

    monkeypatch.setattr(watchdog, "execute_unit_restart", _exec)
    monkeypatch.setattr(watchdog, "_send_bitget_telegram", lambda _t: True)
    monkeypatch.setattr(watchdog, "_telegram_cooldown_elapsed", lambda *a, **k: True)
    watchdog._monitor_async_plane(tmp_path, cooldown=1.0, db="x", miss_threshold=1)
    assert restarts == ["async_telegram"]


def test_async_plane_backlog_alert_only_when_fresh(tmp_path, monkeypatch):
    from bitget.infra.clock import utc_now_iso

    monkeypatch.setenv("BITGET_WATCHDOG_RESTART_ASYNC", "1")
    monkeypatch.setenv("BITGET_WATCHDOG_ASYNC_STALE_SEC", "600")
    monkeypatch.setenv("BITGET_WATCHDOG_ASYNC_PENDING_ALERT", "10")
    monkeypatch.setattr(
        watchdog,
        "_latest_heartbeat_row",
        lambda db, comps: (
            utc_now_iso(),
            "async_telegram_daemon",
            {"telegram_queue_pending": 99},
        ),
    )
    restarts = []
    monkeypatch.setattr(
        watchdog,
        "execute_unit_restart",
        lambda *a, **k: restarts.append(1) or {},
    )
    alerts = []
    monkeypatch.setattr(watchdog, "_send_bitget_telegram", lambda t: alerts.append(t) or True)
    monkeypatch.setattr(watchdog, "_telegram_cooldown_elapsed", lambda *a, **k: True)
    watchdog._monitor_async_plane(tmp_path, cooldown=1.0, db="x", miss_threshold=1)
    assert restarts == []
    assert alerts and "backlog" in alerts[0].lower()


def test_queue_monitor_restarts_when_hung_with_work(tmp_path, monkeypatch):
    monkeypatch.setenv("BITGET_WATCHDOG_RESTART_QUEUE", "1")
    monkeypatch.setenv("BITGET_QUEUE_WORKER_STALE_SEC", "60")
    import bitget.infra.task_orchestrator as tq

    monkeypatch.setattr(
        tq,
        "backlog_stats",
        lambda: {"pending": 2, "running": 0, "failed": 0, "oldest_pending_age_sec": 10},
    )
    monkeypatch.setattr(tq, "worker_heartbeat_age_sec", lambda: 600.0)
    monkeypatch.setattr(watchdog, "_send_bitget_telegram", lambda _t: True)
    monkeypatch.setattr(watchdog, "_telegram_cooldown_elapsed", lambda *a, **k: True)
    restarts = []

    def _exec(state_dir, **kwargs):
        restarts.append(kwargs.get("unit"))
        return {"restarted": True, "budget_ok": True}

    monkeypatch.setattr(watchdog, "execute_unit_restart", _exec)
    watchdog._monitor_queue_safety(tmp_path, cooldown=1.0, miss_threshold=1)
    assert restarts == ["queue_worker"]


def test_queue_monitor_no_restart_when_idle(tmp_path, monkeypatch):
    import bitget.infra.task_orchestrator as tq

    monkeypatch.setattr(tq, "backlog_stats", lambda: {"pending": 0, "running": 0, "failed": 0})
    monkeypatch.setattr(tq, "worker_heartbeat_age_sec", lambda: 9999.0)
    called = {"n": 0}

    def _exec(*a, **k):
        called["n"] += 1
        return {}

    monkeypatch.setattr(watchdog, "execute_unit_restart", _exec)
    watchdog._monitor_queue_safety(tmp_path, cooldown=1.0, miss_threshold=1)
    assert called["n"] == 0


def test_watchdog_restart_matrix_architecture_guard():
    from bitget.validation.architecture_checks import check_watchdog_restart_matrix_ssot

    r = check_watchdog_restart_matrix_ssot()
    assert r["ok"] is True, r

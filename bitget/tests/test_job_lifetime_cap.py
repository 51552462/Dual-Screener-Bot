"""A-LIFECAP-01 job lifetime cap — skip vs watchdog kill."""
from __future__ import annotations

import logging
import signal

from bitget.infra import job_lifetime_cap as jlc
from bitget.infra.runtime import StepSpec, dispatch_bitget_mode


def _env(monkeypatch, tmp_path, **vals):
    monkeypatch.setenv("BITGET_DB_STORAGE_PATH", str(tmp_path))
    monkeypatch.setenv("BITGET_JOB_LIFECAP_DB_PATH", str(tmp_path / "lifecap.sqlite"))
    monkeypatch.setenv("BITGET_JOB_LIFECAP_ENABLED", "true")
    monkeypatch.setenv("BITGET_JOB_LIFECAP_ENFORCE", "false")
    monkeypatch.setenv("BITGET_JOB_HEAVY_CAP_SEC", "5400")
    monkeypatch.setenv("BITGET_JOB_OPS_CAP_SEC", "1800")
    monkeypatch.setenv("BITGET_JOB_KILL_GRACE_SEC", "1")
    for k, v in vals.items():
        monkeypatch.setenv(k, str(v))
    monkeypatch.setattr(jlc, "ensure_lifecap_defaults", lambda: None)


def test_job_lifetime_cap_sec_heavy_vs_ops(monkeypatch, tmp_path):
    _env(monkeypatch, tmp_path)
    assert jlc.job_lifetime_cap_sec("scan_spot_dante_r2") == 5400
    assert jlc.job_lifetime_cap_sec("daily_audit") == 5400
    assert jlc.job_lifetime_cap_sec("weekly_evolution") == 5400
    assert jlc.job_lifetime_cap_sec("track_positions") == 1800


def test_a_age_under_cap_survives(monkeypatch, tmp_path):
    _env(monkeypatch, tmp_path, BITGET_JOB_HEAVY_CAP_SEC="100")
    monkeypatch.setattr(jlc, "_pid_alive", lambda pid: True)
    jlc.register_job_start("scan_spot_dante_r2", 4242)
    conn = jlc._connect()
    conn.execute("UPDATE job_starts SET started_ts = started_ts - 10")
    conn.commit()
    conn.close()
    alive, age = jlc.is_same_mode_alive("scan_spot_dante_r2")
    assert alive is True
    assert 0 <= age < 100


def test_b_age_over_cap_shadow_log_only(monkeypatch, tmp_path, caplog):
    _env(monkeypatch, tmp_path, BITGET_JOB_LIFECAP_ENFORCE="false", BITGET_JOB_HEAVY_CAP_SEC="5")
    monkeypatch.setattr(jlc, "_pid_alive", lambda pid: True)
    killed = []
    monkeypatch.setattr(jlc, "terminate_job", lambda pid, grace: killed.append((pid, grace)))
    jlc.register_job_start("scan_spot_dante_r2", 99)
    conn = jlc._connect()
    conn.execute("UPDATE job_starts SET started_ts = started_ts - 30")
    conn.commit()
    conn.close()
    with caplog.at_level(logging.WARNING, logger="bitget.job_lifetime_cap"):
        recs = jlc.sweep_expired_jobs()
    assert recs and recs[0]["pid"] == 99
    assert killed == []
    assert any("WOULD_KILL" in r.message for r in caplog.records)
    alive, _ = jlc.is_same_mode_alive("scan_spot_dante_r2")
    assert alive is True


def test_c_enforce_sigterm_then_sigkill(monkeypatch, tmp_path):
    _env(monkeypatch, tmp_path, BITGET_JOB_LIFECAP_ENFORCE="true", BITGET_JOB_KILL_GRACE_SEC="1")
    signals = []
    alive_state = {"v": True}

    def fake_kill(pid, sig):
        signals.append(sig)
        if len(signals) >= 2:
            alive_state["v"] = False

    monkeypatch.setattr(jlc, "_pid_alive", lambda pid: alive_state["v"])
    monkeypatch.setattr(jlc.os, "kill", fake_kill)
    monkeypatch.setattr(jlc.time, "sleep", lambda s: None)
    monkeypatch.setattr(jlc.time, "monotonic", lambda: 0.0)
    # grace loop: monotonic stays 0 so loop continues forever unless we bump
    ticks = {"n": 0}

    def mono():
        ticks["n"] += 1
        return 0.0 if ticks["n"] < 3 else 2.0

    monkeypatch.setattr(jlc.time, "monotonic", mono)
    jlc.terminate_job(77, 1)
    assert getattr(signal, "SIGTERM", 15) in signals
    assert len(signals) >= 2


def test_d_same_mode_reentry_two_skips(monkeypatch, tmp_path):
    _env(monkeypatch, tmp_path, BITGET_JOB_HEAVY_CAP_SEC="9999")
    monkeypatch.setattr(jlc, "_pid_alive", lambda pid: True)
    jlc.register_job_start("scan_spot_dante_r2", 7)

    def _pipe():
        return [StepSpec(name="noop", fn=lambda: None, critical=True)]

    r1 = dispatch_bitget_mode("scan_spot_dante_r2", _pipe(), skip_telegram=True, dry_run=False)
    r2 = dispatch_bitget_mode("scan_spot_dante_r2", _pipe(), skip_telegram=True, dry_run=False)
    assert r1.skipped_session is True
    assert "SKIPPED_STILL_RUNNING" in (r1.skipped_session_detail or "")
    assert r2.skipped_session is True
    assert "SKIPPED_STILL_RUNNING" in (r2.skipped_session_detail or "")


def test_d_stale_over_cap_skip_not_kill(monkeypatch, tmp_path):
    _env(monkeypatch, tmp_path, BITGET_JOB_HEAVY_CAP_SEC="5")
    monkeypatch.setattr(jlc, "_pid_alive", lambda pid: True)
    jlc.register_job_start("scan_futures_ema5_r2", 8)
    conn = jlc._connect()
    conn.execute("UPDATE job_starts SET started_ts = started_ts - 30")
    conn.commit()
    conn.close()
    killed = []
    monkeypatch.setattr(jlc, "terminate_job", lambda *a, **k: killed.append(a))
    report = dispatch_bitget_mode(
        "scan_futures_ema5_r2",
        [StepSpec(name="noop", fn=lambda: None, critical=True)],
        skip_telegram=True,
    )
    assert report.skipped_session is True
    assert "SKIPPED_STALE_OVER_CAP" in (report.skipped_session_detail or "")
    assert killed == []


def test_e_record_job_failure_on_enforce_sweep(monkeypatch, tmp_path):
    _env(
        monkeypatch,
        tmp_path,
        BITGET_JOB_LIFECAP_ENFORCE="true",
        BITGET_JOB_HEAVY_CAP_SEC="5",
        BITGET_JOB_KILL_GRACE_SEC="1",
    )
    monkeypatch.setattr(jlc, "_pid_alive", lambda pid: False)
    # After terminate, pid dead; force alive during sweep then dead after terminate
    state = {"alive": True}

    def alive(pid):
        return state["alive"]

    def term(pid, grace):
        state["alive"] = False

    fails = []

    def fake_fail(job_key, error=""):
        fails.append((job_key, error))
        return False, "ok"

    monkeypatch.setattr(jlc, "_pid_alive", alive)
    monkeypatch.setattr(jlc, "terminate_job", term)
    monkeypatch.setattr("bitget.watchdog.record_job_failure", fake_fail)
    jlc.register_job_start("scan_spot_nulrim", 55)
    conn = jlc._connect()
    conn.execute("UPDATE job_starts SET started_ts = started_ts - 40")
    conn.commit()
    conn.close()
    recs = jlc.sweep_expired_jobs()
    assert recs
    assert fails and fails[0][0] == "scan_spot_nulrim"
    assert "lifecap" in fails[0][1]

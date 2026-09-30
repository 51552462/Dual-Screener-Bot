"""CAT-L-CUTOVER-01 Phase 0c — architecture_checks are not loosened.

Each invariant: current tree PASS, mutation FAIL. Full suite architecture_ok.
"""
from __future__ import annotations

from bitget.pipelines.bitget_pipelines import get_pipeline
from bitget.validation.architecture_checks import (
    check_bitget_shell_daily_audit_guard,
    check_pipeline_structure,
    check_portfolio_nav_risk_ssot,
    check_weekly_evolution_pipeline,
    run_architecture_checks,
)


def test_pipeline_structure_current_passes():
    r = check_pipeline_structure()
    assert r["ok"] is True, r
    assert r["daily_count_ok"] is True
    assert int(r["daily_step_count"]) >= 19


def test_pipeline_structure_fails_when_required_body_missing(monkeypatch):
    real = get_pipeline

    def fake(mode: str):
        steps = list(real(mode))
        if mode == "daily_audit":
            steps = [s for s in steps if s.name != "reconcile"]
        return steps

    monkeypatch.setattr("bitget.pipelines.bitget_pipelines.get_pipeline", fake)
    r = check_pipeline_structure()
    assert r["ok"] is False
    assert r["daily_body_keys"] is False


def test_daily_audit_guard_current_passes():
    r = check_bitget_shell_daily_audit_guard()
    assert r["ok"] is True, r
    assert r["classification"] == "ok"
    assert r.get("legacy_self_pid_eq_present") is False


def test_daily_audit_guard_fails_when_helper_missing(monkeypatch, tmp_path):
    import bitget.validation.architecture_checks as ac

    fake_root = tmp_path / "bitget"
    deploy = fake_root / "deploy"
    deploy.mkdir(parents=True)
    (deploy / "bitget.sh").write_text(
        "case \"$MODE\" in\ndaily_audit)\n  echo hi\n  ;;\nesac\nexit 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ac, "_BITGET_ROOT", fake_root)
    r = ac.check_bitget_shell_daily_audit_guard()
    assert r["ok"] is False
    assert r["classification"] == "missing"


def test_daily_audit_guard_fails_when_relocated_disconnected(monkeypatch, tmp_path):
    import bitget.validation.architecture_checks as ac

    fake_root = tmp_path / "bitget"
    deploy = fake_root / "deploy"
    deploy.mkdir(parents=True)
    (deploy / "bitget.sh").write_text(
        "_bitget_live_daily_audit_lines() {\n"
        "  pgrep -af 'bitget.pipelines.runner --mode daily_audit'\n"
        "}\n"
        "case \"$MODE\" in\n"
        "daily_audit)\n"
        "  echo SKIP: another daily_audit job is already running\n"
        "  ;;\n"
        "esac\n"
        "exit 0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(ac, "_BITGET_ROOT", fake_root)
    r = ac.check_bitget_shell_daily_audit_guard()
    assert r["ok"] is False
    assert r["classification"] == "relocated_disconnected"


def test_weekly_evolution_current_passes():
    r = check_weekly_evolution_pipeline()
    assert r["ok"] is True, r
    assert r["tail_ok"] is True


def test_weekly_evolution_fails_without_terminal_set(monkeypatch):
    real = get_pipeline

    def fake(mode: str):
        steps = list(real(mode))
        if mode == "weekly_evolution":
            steps = [
                s
                for s in steps
                if s.name not in ("weekly_action_plan", "weekly_executive_summary")
            ]
        return steps

    monkeypatch.setattr("bitget.pipelines.bitget_pipelines.get_pipeline", fake)
    r = check_weekly_evolution_pipeline()
    assert r["ok"] is False
    assert r["tail_ok"] is False


def test_portfolio_nav_risk_ssot_current_passes():
    r = check_portfolio_nav_risk_ssot()
    assert r["ok"] is True, r
    assert not r.get("failed"), r
    assert r["details"]["snapshot_wiring"]["ok"] is True
    assert r["details"]["snapshot_wiring"]["via_entry_gates"] is True


def test_portfolio_nav_fails_when_snap_cache_unwired(monkeypatch):
    import bitget.validation.architecture_checks as ac

    real = ac._file_text

    def fake(rel: str):
        path, text = real(rel)
        if rel == "trading/execution_safety.py":
            text = text.replace("get_portfolio_mdd_snap_cached", "REMOVED_SNAP_CACHE")
        return path, text

    monkeypatch.setattr(ac, "_file_text", fake)
    r = ac.check_portfolio_nav_risk_ssot()
    assert r["ok"] is False
    assert "execution_safety" in r["failed"]


def test_portfolio_nav_fails_when_live_nav_snapshot_import_lost(monkeypatch):
    import bitget.validation.architecture_checks as ac

    real = ac._file_text
    needle = "from bitget.live_nav_manager import portfolio_nav_snapshot"

    def fake(rel: str):
        path, text = real(rel)
        if rel in ("trading/tail_risk_gate.py", "trading/concentration_gate.py"):
            text = text.replace(needle, "from bitget.live_nav_manager import unused_nav")
        return path, text

    monkeypatch.setattr(ac, "_file_text", fake)
    r = ac.check_portfolio_nav_risk_ssot()
    assert r["ok"] is False
    assert "snapshot_wiring" in r["failed"]


def test_run_architecture_checks_phase0c_targets_ok():
    """The four Phase 0 false-positives must pass. Do not require local
    regime_kelly_audit (meta_fresh / regime_keys_known) — that is env, not 0c.
    """
    report = run_architecture_checks()
    targets = (
        "pipeline_structure",
        "bitget_shell_daily_audit_guard",
        "weekly_evolution_pipeline",
        "portfolio_nav_risk_ssot",
    )
    for name in targets:
        assert report["checks"][name]["ok"] is True, (name, report["checks"][name])
    leftover = [x for x in report["failed"] if x in targets]
    assert leftover == [], leftover



def test_pipeline_structure_fails_when_daily_shrinks_below_min(monkeypatch):
    real = get_pipeline

    def fake(mode: str):
        steps = list(real(mode))
        if mode == "daily_audit":
            return steps[:18]
        return steps

    monkeypatch.setattr("bitget.pipelines.bitget_pipelines.get_pipeline", fake)
    r = check_pipeline_structure()
    assert r["ok"] is False
    assert r["daily_count_ok"] is False

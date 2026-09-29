"""CAT-L-FENCE-02 Step 3 — generator fence vs Phase 0 / live snapshots."""
from __future__ import annotations

import importlib.util
from pathlib import Path

from bitget.infra.job_lifetime_cap import _HEAVY_PREFIXES as CAP_HEAVY

_REPO = Path(__file__).resolve().parents[2]
_GEN = _REPO / "bitget" / "deploy" / "generate_bitget_crontab.py"
_ROOT = "/home/ubuntu/dante_bots/Dual-Screener-Bot"
_P0 = _REPO / "bitget" / "docs" / "work_phases" / "snapshots" / "CAT-L-FENCE-02_cron_p0_20260927.cron"
_LIVE = (
    _REPO
    / "bitget"
    / "docs"
    / "work_phases"
    / "snapshots"
    / "CAT-L-FENCE-02_cron_step2_LIVE_20260927.cron"
)


def _mod():
    spec = importlib.util.spec_from_file_location("gen_bitget_crontab_fence", _GEN)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _active_lines(text: str) -> list[str]:
    out = []
    for raw in text.splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("SHELL=") or s.startswith("CRON_TZ=") or s.startswith("PATH="):
            continue
        out.append(raw.rstrip())
    return out


def test_heavy_prefix_parity():
    mod = _mod()
    assert tuple(mod._HEAVY_PREFIXES) == tuple(CAP_HEAVY)


def test_wrapped_count_28_and_unwrap_matches_p0():
    mod = _mod()
    text = mod.render_bitget_crontab(_ROOT)
    wrapped = [ln for ln in _active_lines(text) if "systemd-run" in ln]
    assert len(wrapped) == 28
    unwrapped = [mod.unwrap_fence_inner(ln) for ln in wrapped]
    p0_jobs = [ln for ln in _active_lines(_P0.read_text(encoding="utf-8")) if "bitget.sh" in ln]
    p0_heavy = [ln for ln in p0_jobs if "--enqueue" not in ln.split() and (
        any(
            tok.lstrip("-").replace("-", "_").startswith(p)
            for tok in ln.split()
            if tok.startswith("--")
            for p in CAP_HEAVY
        )
    )]
    assert len(p0_heavy) == 28
    assert unwrapped == p0_heavy


def test_enqueue_and_ops_unfenced():
    mod = _mod()
    text = mod.render_bitget_crontab(_ROOT)
    enqueue = [ln for ln in _active_lines(text) if "--enqueue" in ln]
    assert len(enqueue) == 1
    assert "ubuntu" in enqueue[0]
    assert "systemd-run" not in enqueue[0]
    for flag in (
        "--canary",
        "--track-positions",
        "--reconcile",
        "--data-refresh",
        "--db-backup",
        "--watchdog",
        "--health",
        "--monthly-grand",
        "--post-deploy-obs-digest",
    ):
        hits = [ln for ln in _active_lines(text) if flag in ln]
        assert hits, flag
        assert all("systemd-run" not in ln for ln in hits), flag


def test_generator_matches_live_snapshot():
    mod = _mod()
    got = mod.render_bitget_crontab(_ROOT)
    live = _LIVE.read_text(encoding="utf-8")
    assert got == live, (
        "생성기 != Step2 LIVE 스냅샷. 정당한 잡 추가/변경이면 "
        "bitget/docs/work_phases/snapshots/CAT-L-FENCE-02_cron_step2_LIVE_*.cron "
        "을 갱신한 뒤 이 테스트를 다시 돌릴 것(서버 실파일 확인 후)."
    )


def test_use_queue_scans_not_wrapped():
    mod = _mod()
    text = mod.render_bitget_crontab(_ROOT, use_queue=True)
    scan_lines = [ln for ln in _active_lines(text) if "--scan-" in ln]
    assert scan_lines
    assert all("--enqueue" in ln for ln in scan_lines)
    assert all("systemd-run" not in ln for ln in scan_lines)
    audit = [ln for ln in _active_lines(text) if "--daily-audit" in ln]
    assert len(audit) == 1
    assert "systemd-run" in audit[0]


def test_classify_fence_ok_missing_drifted():
    mod = _mod()
    gen = mod.render_bitget_crontab(_ROOT)
    live_ok = gen
    live_missing = _P0.read_text(encoding="utf-8")
    assert mod.wrapped_count(gen) == mod.EXPECTED_WRAPPED == 28
    assert mod.classify_fence(gen, live_ok) == "FENCE_OK"
    assert mod.classify_fence(live_missing, live_missing) == "FENCE_MISSING"
    assert mod.classify_fence(gen, live_missing) == "DRIFTED"
    report = mod.fence_check_report(gen, live_missing)
    assert "FENCE_STATUS=DRIFTED" in report
    assert "WRAPPED_COUNT=28" in report
    assert "LIVE_WRAPPED_COUNT=0" in report

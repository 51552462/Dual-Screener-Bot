"""CAT-L-FENCE-03 drift guard — temp copies only (never mutate /etc/cron.d)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_GEN = _REPO / "bitget" / "deploy" / "generate_bitget_crontab.py"
_ROOT = "/home/ubuntu/dante_bots/Dual-Screener-Bot"
_P0 = (
    _REPO
    / "bitget"
    / "docs"
    / "work_phases"
    / "snapshots"
    / "CAT-L-FENCE-02_cron_p0_20260927.cron"
)


def _mod():
    spec = importlib.util.spec_from_file_location("gen_fence03", _GEN)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_marker_stable_across_comment_change():
    mod = _mod()
    a = mod.render_bitget_crontab(_ROOT)
    b = a.replace(
        "# AUTO-GENERATED from bitget/bitget_scan_schedule.py — do not edit by hand.",
        "# AUTO-GENERATED from bitget/bitget_scan_schedule.py — do not edit by hand.\n# extra comment",
        1,
    )
    assert mod.MARKER_GEN_LINE in a
    assert mod.body_sha256(a) == mod.body_sha256(b)
    assert mod.live_marker_sha(a) == mod.body_sha256(a)
    assert mod.classify_source_state(a) == "PRISTINE"


def test_source_states_four_way():
    mod = _mod()
    gen = mod.render_bitget_crontab(_ROOT)
    assert mod.classify_source_state(None) == "ABSENT"
    assert mod.classify_source_state(gen) == "PRISTINE"
    unmarked = "\n".join(
        ln for ln in gen.splitlines() if "CAT-L-FENCE-03" not in ln
    ) + "\n"
    assert mod.classify_source_state(unmarked) == "UNMARKED"
    drifted = gen.replace("CRON_TZ=UTC", "CRON_TZ=Asia/Seoul", 1)
    assert mod.classify_source_state(drifted) == "DRIFTED"


def test_comment_systemd_run_does_not_change_hash_or_wrap():
    mod = _mod()
    gen = mod.render_bitget_crontab(_ROOT)
    noisy = "# systemd-run in a header comment only\n" + gen
    assert mod.body_sha256(gen) == mod.body_sha256(noisy)
    assert mod.wrapped_count(gen) == mod.wrapped_count(noisy) == 28


def test_pristine_repo_change_does_not_block():
    mod = _mod()
    live = mod.render_bitget_crontab(_ROOT)
    mutated_gen = live.replace(
        "*/15 * * * *  ubuntu  cd ",
        "*/16 * * * *  ubuntu  cd ",
        1,
    )
    assert mod.classify_source_state(live) == "PRISTINE"
    assert mod.install_should_block("PRISTINE", mutated_gen, live, force=False) is False


def test_drifted_blocks_unless_force():
    mod = _mod()
    gen = mod.render_bitget_crontab(_ROOT)
    live = gen.replace("CRON_TZ=UTC", "CRON_TZ=Asia/Seoul", 1)
    assert mod.classify_source_state(live) == "DRIFTED"
    assert mod.install_should_block("DRIFTED", gen, live, force=False) is True
    assert mod.install_should_block("DRIFTED", gen, live, force=True) is False
    assert mod.diff_live_exit_code("DRIFTED", gen, live) == 20


def test_unmarked_equal_stamps_not_block():
    mod = _mod()
    gen = mod.render_bitget_crontab(_ROOT)
    live = "\n".join(ln for ln in gen.splitlines() if "CAT-L-FENCE-03" not in ln) + "\n"
    assert mod.classify_source_state(live) == "UNMARKED"
    assert mod.bodies_equal(gen, live)
    assert mod.install_should_block("UNMARKED", gen, live, force=False) is False
    assert mod.diff_live_exit_code("UNMARKED", gen, live) == 0


def test_unmarked_unequal_blocks_step4_pattern():
    mod = _mod()
    gen_old = _P0.read_text(encoding="utf-8")
    live_wrapped = mod.render_bitget_crontab(_ROOT)
    live_unmarked = "\n".join(
        ln for ln in live_wrapped.splitlines() if "CAT-L-FENCE-03" not in ln
    ) + "\n"
    assert mod.classify_source_state(live_unmarked) == "UNMARKED"
    assert not mod.bodies_equal(gen_old, live_unmarked)
    assert mod.install_should_block("UNMARKED", gen_old, live_unmarked, force=False) is True
    assert mod.diff_live_exit_code("UNMARKED", gen_old, live_unmarked) == 30


def test_fail_closed_unreadable(tmp_path: Path):
    mod = _mod()
    rc = mod.main(["--diff-live", str(tmp_path), "--install-root", _ROOT])
    assert rc == mod.EXIT_FAIL_CLOSED


def test_diff_live_default_path_absent_is_40():
    mod = _mod()
    rc = mod.main(
        ["--diff-live", str(Path("/no/such/fence03-live.cron")), "--install-root", _ROOT]
    )
    assert rc == 40


def test_diff_live_exit_matrix(tmp_path: Path):
    mod = _mod()
    gen = mod.render_bitget_crontab(_ROOT)
    live_p = tmp_path / "live.cron"
    live_p.write_text(gen, encoding="utf-8")
    assert mod.main(["--diff-live", str(live_p), "--install-root", _ROOT]) == 0
    drifted = gen.replace("CRON_TZ=UTC", "CRON_TZ=Asia/Seoul", 1)
    live_p.write_text(drifted, encoding="utf-8")
    assert mod.main(["--diff-live", str(live_p), "--install-root", _ROOT]) == 20
    unmarked = "\n".join(ln for ln in gen.splitlines() if "CAT-L-FENCE-03" not in ln) + "\n"
    live_p.write_text(unmarked, encoding="utf-8")
    assert mod.main(["--diff-live", str(live_p), "--install-root", _ROOT]) == 0
    live_p.write_text(_P0.read_text(encoding="utf-8"), encoding="utf-8")
    assert mod.main(["--diff-live", str(live_p), "--install-root", _ROOT]) == 30
    mutated = gen.replace("*/15 * * * *  ubuntu", "*/16 * * * *  ubuntu", 1)
    # PRISTINE live + different gen: write pristine gen as live, compare via exit on mutated...
    # --diff-live always renders current generator vs live file.
    live_p.write_text(gen, encoding="utf-8")
    tweaked = unmarked.replace(
        "*/5 * * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC",
        "*/6 * * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC",
        1,
    )
    tweaked_marked = mod.attach_fence03_markers(tweaked)
    assert mod.classify_source_state(tweaked_marked) == "PRISTINE"
    live_p.write_text(tweaked_marked, encoding="utf-8")
    assert mod.main(["--diff-live", str(live_p), "--install-root", _ROOT]) == 10

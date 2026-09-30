#!/usr/bin/env python3
"""
bitget_scan_schedule.py → bitget/deploy/bitget.crontab.example

크론 시각·CRON_TZ·bitget.sh 플래그를 코드 SSOT에서만 생성합니다.

  python bitget/deploy/generate_bitget_crontab.py
  python bitget/deploy/generate_bitget_crontab.py --check
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BITGET_ROOT = _REPO_ROOT / "bitget"
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from bitget.bitget_scan_schedule import (  # noqa: E402
    ALL_SCAN_SLOTS,
    DAILY_AUDIT_UTC_HOUR,
    DAILY_AUDIT_UTC_MINUTE,
    FUTURES_SCAN_SLOTS,
    SCHEDULE_MARKET_TZ,
    SCHEDULE_WEEKDAYS,
    SPOT_SCAN_SLOTS,
)
from bitget.infra.job_lifetime_cap import _HEAVY_PREFIXES  # noqa: E402
from bitget.infra.logging_setup import get_logger  # noqa: E402

DEFAULT_INSTALL_ROOT = "/home/ubuntu/dante_bots/Dual-Screener-Bot"
DEFAULT_LIVE_CRON = "/etc/cron.d/dual-screener-bitget"
MARKER_GEN_LINE = (
    "# CAT-L-FENCE-03 generator=bitget/deploy/generate_bitget_crontab.py"
)
MARKER_HASH_PREFIX = "# CAT-L-FENCE-03 body-sha256="
EXIT_DIFF_SAME = 0
EXIT_DIFF_PRISTINE_CHANGED = 10
EXIT_DIFF_DRIFTED = 20
EXIT_DIFF_UNMARKED_CHANGED = 30
EXIT_DIFF_ABSENT = 40
EXIT_FAIL_CLOSED = 2
BACKUP_CRON_DIR = "/var/backups/bitget-cron"
CRON_USER = "ubuntu"
CRON_USER_HEAVY = "root"
FENCE_MEMORY_MAX = "1610612736"
FENCE_MEMORY_HIGH = "1288490188"
# cron.d treats unescaped % as newline.
_SYSTEMD_RUN = (
    "/usr/bin/systemd-run --quiet --collect --uid=ubuntu --gid=ubuntu "
    "--scope --slice=bitget-cron-heavy.slice "
    f"-p MemoryMax={FENCE_MEMORY_MAX} -p MemoryHigh={FENCE_MEMORY_HIGH} "
    r"-p CPUQuota=80\% -- /bin/bash -c"
)
logger = get_logger("bitget.deploy.generate_bitget_crontab")

# L-3b canary only — not full b-3. Other scan_* stay inline until a separate Ask.
QUEUE_CANARY_FLAGS = frozenset({"--scan-futures-ema5-r2"})


def _flag_to_mode(flag: str) -> str:
    return flag.lstrip("-").replace("-", "_")


def command_is_heavy_inline(command: str) -> bool:
    """HEAVY 직행만 True. --enqueue 줄은 factory/queue cgroup — wrapper 대상 아님."""
    if "--enqueue" in command.split():
        return False
    for tok in command.split():
        if not tok.startswith("--"):
            continue
        mode = _flag_to_mode(tok)
        if any(mode.startswith(p) for p in _HEAVY_PREFIXES):
            return True
    return False


def unwrap_fence_inner(line: str) -> str:
    """root+systemd-run 줄을 Phase 0 ubuntu 형태로. 비펜스 줄은 그대로."""
    marker = " -- /bin/bash -c '"
    if "systemd-run" not in line or marker not in line:
        return line.rstrip("\n")
    head, rest = line.split(marker, 1)
    inner, _, _tail = rest.rpartition("'")
    sched = head.split()[:5]
    return f"{' '.join(sched)}  {CRON_USER}  {inner}"


_ENV_LINE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
# Phase 0 (a) HEAVY inline count — S0 must not PASS when both sides have 0 wrappers.
EXPECTED_WRAPPED = 28


def cron_body_lines(text: str) -> List[str]:
    out: List[str] = []
    for raw in text.splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        out.append(raw.rstrip())
    return out


def split_cron_env_jobs(text: str) -> Tuple[List[str], List[str]]:
    env: List[str] = []
    jobs: List[str] = []
    for ln in cron_body_lines(text):
        if _ENV_LINE.match(ln):
            env.append(ln)
        else:
            jobs.append(ln)
    return env, jobs


def wrapped_count(text: str) -> int:
    n = 0
    for ln in text.splitlines():
        if ln.lstrip().startswith("#"):
            continue
        if "systemd-run" in ln:
            n += 1
    return n


def classify_fence(gen_text: str, live_text: str) -> str:
    """S0 3-way: empty job/env match is not enough — both unwrapped must be FENCE_MISSING."""
    g_env, g_jobs = split_cron_env_jobs(gen_text)
    l_env, l_jobs = split_cron_env_jobs(live_text)
    if g_env != l_env or g_jobs != l_jobs:
        return "DRIFTED"
    wc = wrapped_count(gen_text)
    if wc == 0:
        return "FENCE_MISSING"
    if wc == EXPECTED_WRAPPED:
        return "FENCE_OK"
    return "DRIFTED"


def fence_check_report(gen_text: str, live_text: str) -> str:
    status = classify_fence(gen_text, live_text)
    wc = wrapped_count(gen_text)
    lc = wrapped_count(live_text)
    g_env, g_jobs = split_cron_env_jobs(gen_text)
    l_env, l_jobs = split_cron_env_jobs(live_text)
    lines = [
        f"WRAPPED_COUNT={wc}",
        f"LIVE_WRAPPED_COUNT={lc}",
        f"EXPECTED_WRAPPED={EXPECTED_WRAPPED}",
        f"FENCE_STATUS={status}",
        f"GEN_JOBS={len(g_jobs)} LIVE_JOBS={len(l_jobs)}",
        f"ENV_SAME={'yes' if g_env == l_env else 'no'}",
        f"JOBS_SAME={'yes' if g_jobs == l_jobs else 'no'}",
    ]
    return "\n".join(lines) + "\n"


def body_canonical(text: str) -> str:
    lines = cron_body_lines(text)
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def body_sha256(text: str) -> str:
    return hashlib.sha256(body_canonical(text).encode("utf-8")).hexdigest()


def live_marker_sha(text: str) -> Optional[str]:
    for raw in text.splitlines():
        s = raw.strip()
        if s.startswith(MARKER_HASH_PREFIX):
            return s[len(MARKER_HASH_PREFIX) :].strip() or None
    return None


def classify_source_state(live_text: Optional[str]) -> str:
    if live_text is None:
        return "ABSENT"
    marker = live_marker_sha(live_text)
    if not marker:
        return "UNMARKED"
    if body_sha256(live_text) == marker:
        return "PRISTINE"
    return "DRIFTED"


def attach_fence03_markers(text: str) -> str:
    digest = body_sha256(text)
    out: List[str] = []
    inserted = False
    for ln in text.splitlines():
        if not inserted and ln.startswith("SHELL="):
            out.append(MARKER_GEN_LINE)
            out.append(f"{MARKER_HASH_PREFIX}{digest}")
            inserted = True
        out.append(ln)
    if not inserted:
        out.insert(0, MARKER_GEN_LINE)
        out.insert(1, f"{MARKER_HASH_PREFIX}{digest}")
    return "\n".join(out) + "\n"


def bodies_equal(a: str, b: str) -> bool:
    return body_canonical(a) == body_canonical(b)


def install_should_block(
    state: str, gen_text: str, live_text: Optional[str], *, force: bool
) -> bool:
    if force:
        return False
    if state in ("ABSENT", "PRISTINE"):
        return False
    if state == "DRIFTED":
        return True
    if state == "UNMARKED":
        if live_text is None:
            return True
        return not bodies_equal(gen_text, live_text)
    return True


def diff_live_exit_code(state: str, gen_text: str, live_text: Optional[str]) -> int:
    if state == "ABSENT" or live_text is None:
        return EXIT_DIFF_ABSENT
    equal = bodies_equal(gen_text, live_text)
    if state == "DRIFTED":
        return EXIT_DIFF_DRIFTED
    if state == "UNMARKED":
        return EXIT_DIFF_SAME if equal else EXIT_DIFF_UNMARKED_CHANGED
    if state == "PRISTINE":
        return EXIT_DIFF_SAME if equal else EXIT_DIFF_PRISTINE_CHANGED
    return EXIT_FAIL_CLOSED


def unified_body_diff(live_text: str, gen_text: str) -> str:
    live_lines = body_canonical(live_text).splitlines(keepends=True)
    gen_lines = body_canonical(gen_text).splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            live_lines, gen_lines, fromfile="live", tofile="generator", lineterm=""
        )
    )


def body_diff_counts(live_text: str, gen_text: str) -> Tuple[int, int, int]:
    live_set = cron_body_lines(live_text)
    gen_set = cron_body_lines(gen_text)
    live_s, gen_s = set(live_set), set(gen_set)
    added = len(gen_s - live_s)
    removed = len(live_s - gen_s)
    changed = min(added, removed)
    return added, removed, changed


def slice_report(*, repo_root: Path | None = None) -> str:
    root = repo_root or _REPO_ROOT
    tmpl = root / "bitget" / "deploy" / "systemd" / "bitget-cron-heavy.slice"
    dest = Path("/etc/systemd/system/bitget-cron-heavy.slice")
    lines = [
        f"SLICE_TEMPLATE={tmpl}",
        f"SLICE_UNIT={dest}",
        f"SLICE_TEMPLATE_HIGH={FENCE_MEMORY_HIGH}",
        f"SLICE_TEMPLATE_MAX={FENCE_MEMORY_MAX}",
    ]
    if tmpl.is_file() and dest.is_file():
        same = tmpl.read_text(encoding="utf-8") == dest.read_text(encoding="utf-8")
        lines.append(f"SLICE_FILE_SAME={'yes' if same else 'no'}")
        if not same:
            lines.append("SLICE_WARN=template and installed unit file differ (install will overwrite)")
    elif dest.is_file():
        lines.append("SLICE_FILE_SAME=no")
        lines.append("SLICE_WARN=unit present, template missing in repo")
    else:
        lines.append("SLICE_FILE_SAME=n/a")
    try:
        out = subprocess.check_output(
            [
                "systemctl",
                "show",
                "bitget-cron-heavy.slice",
                "-p",
                "MemoryHigh",
                "-p",
                "MemoryMax",
                "-p",
                "ActiveState",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        lines.append("SLICE_RUNTIME=")
        lines.append(out.rstrip())
        if f"MemoryHigh={FENCE_MEMORY_HIGH}" not in out or f"MemoryMax={FENCE_MEMORY_MAX}" not in out:
            lines.append("SLICE_WARN=systemctl MemoryHigh/Max != template constants")
    except (OSError, subprocess.CalledProcessError):
        lines.append("SLICE_RUNTIME=unavailable")
    return "\n".join(lines) + "\n"


def format_diff_live_report(
    gen_text: str, live_text: Optional[str], *, repo_root: Path | None = None
) -> str:
    state = classify_source_state(live_text)
    parts = [
        f"SOURCE_STATE={state}",
        f"GEN_WRAPPED_COUNT={wrapped_count(gen_text)}",
        f"LIVE_WRAPPED_COUNT={wrapped_count(live_text or '')}",
        f"EXPECTED_WRAPPED={EXPECTED_WRAPPED}",
        f"GEN_BODY_SHA={body_sha256(gen_text)}",
    ]
    if live_text is not None:
        parts.append(f"LIVE_BODY_SHA={body_sha256(live_text)}")
        parts.append(f"MARKER_SHA={live_marker_sha(live_text) or ''}")
        parts.append(f"BODIES_EQUAL={'yes' if bodies_equal(gen_text, live_text) else 'no'}")
        diff = unified_body_diff(live_text, gen_text)
        parts.append("=== unified diff (body, comments excluded) ===")
        parts.append(diff if diff else "(empty)")
    parts.append("=== slice ===")
    parts.append(slice_report(repo_root=repo_root).rstrip())
    return "\n".join(parts) + "\n"


def format_install_plan(
    gen_text: str, live_text: Optional[str], *, force: bool
) -> str:
    state = classify_source_state(live_text)
    block = install_should_block(state, gen_text, live_text, force=force)
    action = "block" if block else "install"
    added = removed = changed = 0
    if live_text is not None:
        added, removed, changed = body_diff_counts(live_text, gen_text)
    lines = [
        f"SOURCE_STATE={state}",
        f"ACTION={action}",
        f"FORCE={'yes' if force else 'no'}",
        f"GEN_BODY_SHA={body_sha256(gen_text)}",
        f"LIVE_BODY_SHA={body_sha256(live_text) if live_text is not None else ''}",
        f"MARKER_SHA={live_marker_sha(live_text) if live_text else ''}",
        f"BODIES_EQUAL={'yes' if live_text is not None and bodies_equal(gen_text, live_text) else 'n/a'}",
        f"DIFF_ADDED={added} DIFF_REMOVED={removed} DIFF_CHANGED={changed}",
    ]
    return "\n".join(lines) + "\n"


def _job_line(schedule: str, command: str, install_root: str) -> str:
    inner = f"cd {install_root} && {command}"
    if command_is_heavy_inline(command):
        return f"{schedule}  {CRON_USER_HEAVY}  {_SYSTEMD_RUN} '{inner}'"
    return f"{schedule}  {CRON_USER}  {inner}"


def _cron_line(minute: int, hour: int, dow: str, command: str, install_root: str) -> str:
    return _job_line(f"{minute} {hour} * * {dow}", command, install_root)


def _scan_command(
    bitget_flag: str, *, tz: str, install_root: str, use_queue: bool = False
) -> str:
    bg = f"{install_root}/bitget/deploy/bitget.sh"
    enqueue = bool(use_queue) or bitget_flag in QUEUE_CANARY_FLAGS
    if enqueue:
        # cron→큐 어댑터: 즉시 enqueue 후 종료. queue worker 가 순차 실행한다.
        return f"TZ={tz} {bg} --enqueue {bitget_flag}"
    return f"TZ={tz} {bg} {bitget_flag}"


def render_bitget_crontab(install_root: str, *, use_queue: bool = False) -> str:
    tz = SCHEDULE_MARKET_TZ["SPOT"]
    bg = f"{install_root}/bitget/deploy/bitget.sh"
    lines: List[str] = [
        "# Dual-Screener-Bot — Bitget factory cron (→ /etc/cron.d/dual-screener-bitget)",
        "#",
        "# AUTO-GENERATED from bitget/bitget_scan_schedule.py — do not edit by hand.",
        f"# Regenerate: python bitget/deploy/generate_bitget_crontab.py",
        "# 전용 코인 서버(Bot-2) 최적화: 3사이클 27슬롯, ~53분 간격 교차 배치.",
        "# SPOT/FUTURES are interleaved (never simultaneous). %5 minute constraint removed",
        "# (dedicated server — no KR/US stock collision risk).",
        "# Two-Track air-gap: cgroup·독립 락/큐로 병렬 가동. yield OFF (BITGET_YIELD_TO_FACTORY=0)."
        + (
            "\n# QUEUE MODE: scans are enqueued (--enqueue) and run by the single "
            "dante-bitget-queue-worker; conflicts wait (PENDING) instead of skipping."
            if use_queue
            else (
                "\n# L-3b canary: --scan-futures-ema5-r2 is --enqueue only "
                "(other scan_* stay inline; full b-3 is a separate Ask)."
            )
        ),
        "# install: sudo INSTALL_ROOT=... bash bitget/deploy/install_bitget_cron.sh",
        "#",
        f"# user/path: {CRON_USER} · {install_root}",
        "",
        "SHELL=/bin/bash",
        f"CRON_TZ={tz}",
        "PATH=/usr/local/bin:/usr/bin:/bin",
        "# CAT-L-FENCE-02 Step2: (a) HEAVY = root + systemd-run scope+slice 1.5G/1.2G; (b)/(c) unchanged. Rollback: CAT-L-FENCE-02_cron_p0_20260927.cron",
        "",
        f"# --- Ops (non-scan, 24/7) ---",
        "# track */15 (light) · watchdog */5 (light) keep running through stock hours.",
        "# reconcile :53 · data-refresh :43 — off stock :x0/:x5 minutes; data-refresh",
        "# also yields to factory. daily-audit/health/weekly/db-backup run in the "
        "KST-pre-open idle window.",
        "# canary */15 (light, public API only, no DB/lock): keeps bitget_canary_state.json",
        "# fresh (<=15min) so the stock regime engine's 90-min staleness gate always passes.",
        _job_line("*/15 * * * *", f"TZ={tz} {bg} --canary", install_root),
        _job_line("*/15 * * * *", f"TZ={tz} {bg} --track-positions", install_root),
        _job_line("53 * * * *", f"TZ={tz} {bg} --reconcile", install_root),
        _job_line("43 */4 * * *", f"TZ={tz} {bg} --data-refresh", install_root),
        "# Integrity backup @00:05 UTC — before health/daily-audit; PRAGMA + archive prune",
        _job_line("5 0 * * *", f"TZ={tz} {bg} --db-backup", install_root),
        f"# daily-audit @ {DAILY_AUDIT_UTC_HOUR:02d}:{DAILY_AUDIT_UTC_MINUTE:02d} UTC "
        f"(=11:30 KST) — after ema5_r3 tails; avoids runtime-lock skip",
        _job_line(
            f"{DAILY_AUDIT_UTC_MINUTE} {DAILY_AUDIT_UTC_HOUR} * * *",
            f"TZ={tz} {bg} --daily-audit",
            install_root,
        ),
        _job_line("30 0 * * 1", f"TZ={tz} {bg} --weekly-evolution", install_root),
        _job_line("*/5 * * * *", f"TZ={tz} {bg} --watchdog", install_root),
        _job_line("15 0 * * *", f"TZ={tz} {bg} --health", install_root),
        _job_line("50 23 * * *", f"TZ={tz} {bg} --monthly-grand", install_root),
        "# POST_DEPLOY_OBS daily digest @11:00 UTC (=20:00 KST) → REPORT_BOT + Cursor/Claude paste",
        _job_line("0 11 * * *", f"TZ={tz} {bg} --post-deploy-obs-digest", install_root),
        "",
        f"# --- SPOT staggered (24h, {len(SPOT_SCAN_SLOTS)} slots, ~53min interval) ---",
    ]
    dow_spot = SCHEDULE_WEEKDAYS["SPOT"]
    for slot in SPOT_SCAN_SLOTS:
        lines.append(
            _cron_line(
                slot.minute,
                slot.hour,
                dow_spot,
                _scan_command(
                    slot.bitget_flag, tz=tz, install_root=install_root, use_queue=use_queue
                ),
                install_root,
            )
        )
    lines.append("")
    lines.append(
        f"# --- FUTURES staggered (24h, {len(FUTURES_SCAN_SLOTS)} slots, ~53min interval) ---"
    )
    dow_fut = SCHEDULE_WEEKDAYS["FUTURES"]
    for slot in FUTURES_SCAN_SLOTS:
        lines.append(
            _cron_line(
                slot.minute,
                slot.hour,
                dow_fut,
                _scan_command(
                    slot.bitget_flag, tz=tz, install_root=install_root, use_queue=use_queue
                ),
                install_root,
            )
        )
    lines.append("")
    lines.append("# --- Legacy monolithic scan (manual recovery only — do NOT cron) ---")
    lines.append(f"# {CRON_USER}  cd {install_root} && TZ={tz} {bg} --scan-all")
    lines.append("")
    lines.append(f"# SSOT: bitget/bitget_scan_schedule.py ({len(ALL_SCAN_SLOTS)} staggered modes)")
    return attach_fence03_markers("\n".join(lines) + "\n")


def _deploy_path(repo_root: Path) -> Path:
    return repo_root / "bitget" / "deploy" / "bitget.crontab.example"


def write_template(
    install_root: str, repo_root: Path | None = None, *, use_queue: bool = False
) -> None:
    root = repo_root or _REPO_ROOT
    path = _deploy_path(root)
    path.write_text(
        render_bitget_crontab(install_root, use_queue=use_queue),
        encoding="utf-8",
        newline="\n",
    )
    logger.info("OK wrote %s%s", path, " (queue mode)" if use_queue else "")


def check_template(
    install_root: str, repo_root: Path | None = None, *, use_queue: bool = False
) -> int:
    root = repo_root or _REPO_ROOT
    path = _deploy_path(root)
    want = render_bitget_crontab(install_root, use_queue=use_queue)
    if not path.is_file():
        logger.error("ERROR: missing %s", path)
        return 1
    got = path.read_text(encoding="utf-8")
    if got != want:
        logger.error(
            "ERROR: drift: %s does not match bitget_scan_schedule.py "
            "(run: python bitget/deploy/generate_bitget_crontab.py)",
            path,
        )
        return 1
    logger.info("OK %s matches SSOT", path)
    return 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Bitget crontab from schedule SSOT")
    parser.add_argument("--install-root", default=DEFAULT_INSTALL_ROOT)
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--use-queue",
        action="store_true",
        help="Emit scan lines as `bitget.sh --enqueue <flag>` (cron→queue adapter). "
        "Default off keeps inline execution (safe rollback).",
    )
    parser.add_argument(
        "--fence-check",
        nargs="?",
        const=DEFAULT_LIVE_CRON,
        default=None,
        metavar="LIVE_CRON",
        help="Print FENCE_OK / FENCE_MISSING / DRIFTED (default LIVE=/etc/cron.d/dual-screener-bitget).",
    )
    parser.add_argument(
        "--diff-live",
        nargs="?",
        const=DEFAULT_LIVE_CRON,
        default=None,
        metavar="LIVE_CRON",
        help="CAT-L-FENCE-03 read-only source-state + body diff + wrapped counts + slice report.",
    )
    parser.add_argument(
        "--install-plan",
        action="store_true",
        help="Print SOURCE_STATE/ACTION for the installer (does not write files).",
    )
    parser.add_argument(
        "--live",
        default=DEFAULT_LIVE_CRON,
        help="Live cron.d path for --install-plan (default /etc/cron.d/dual-screener-bitget).",
    )
    parser.add_argument(
        "--force-overwrite-drift",
        action="store_true",
        help="With --install-plan: ACTION=install even if DRIFTED/UNMARKED+diff.",
    )
    args = parser.parse_args(argv)
    if args.fence_check:
        live_path = Path(args.fence_check)
        if not live_path.is_file():
            logger.error("ERROR: missing live cron %s", live_path)
            return 1
        gen = render_bitget_crontab(args.install_root, use_queue=args.use_queue)
        live = live_path.read_text(encoding="utf-8")
        sys.stdout.write(fence_check_report(gen, live))
        return 0
    if args.diff_live is not None:
        live_path = Path(args.diff_live)
        gen = render_bitget_crontab(args.install_root, use_queue=args.use_queue)
        if not live_path.exists():
            sys.stdout.write(format_diff_live_report(gen, None))
            return EXIT_DIFF_ABSENT
        try:
            live = live_path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.error("ERROR: cannot read live cron %s: %s", live_path, exc)
            return EXIT_FAIL_CLOSED
        sys.stdout.write(format_diff_live_report(gen, live))
        return diff_live_exit_code(classify_source_state(live), gen, live)
    if args.install_plan:
        live_path = Path(args.live)
        gen = render_bitget_crontab(args.install_root, use_queue=args.use_queue)
        if not live_path.exists():
            sys.stdout.write(format_install_plan(gen, None, force=args.force_overwrite_drift))
            return 0
        try:
            live = live_path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.error("ERROR: cannot read live cron %s: %s", live_path, exc)
            return EXIT_FAIL_CLOSED
        sys.stdout.write(
            format_install_plan(gen, live, force=args.force_overwrite_drift)
        )
        return 0
    if args.check:
        return check_template(args.install_root, use_queue=args.use_queue)
    write_template(args.install_root, use_queue=args.use_queue)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

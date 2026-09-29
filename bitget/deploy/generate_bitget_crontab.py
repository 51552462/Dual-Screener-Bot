#!/usr/bin/env python3
"""
bitget_scan_schedule.py → bitget/deploy/bitget.crontab.example

크론 시각·CRON_TZ·bitget.sh 플래그를 코드 SSOT에서만 생성합니다.

  python bitget/deploy/generate_bitget_crontab.py
  python bitget/deploy/generate_bitget_crontab.py --check
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import List, Tuple

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
    return "\n".join(lines) + "\n"


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
        metavar="LIVE_CRON",
        help="Print FENCE_OK / FENCE_MISSING / DRIFTED vs a live cron.d file (read-only).",
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
    if args.check:
        return check_template(args.install_root, use_queue=args.use_queue)
    write_template(args.install_root, use_queue=args.use_queue)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

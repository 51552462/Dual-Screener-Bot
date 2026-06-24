#!/usr/bin/env python3
"""
factory_scan_schedule.py → deploy/factory.{kr,us}.crontab.example

크론 시각·CRON_TZ·factory.sh 플래그를 코드 SSOT에서만 생성합니다.
수동으로 crontab.example 을 고치지 마세요 — SLOT_INTERVAL_MINUTES 등을
factory_scan_schedule.py 에서 바꾼 뒤 이 스크립트를 실행하세요.

  python deploy/generate_factory_crontab.py
  python deploy/generate_factory_crontab.py --check   # CI: 템플릿 drift 검사
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from factory_scan_schedule import (  # noqa: E402
    KR_SCAN_SLOTS,
    SCHEDULE_MARKET_TZ,
    SCHEDULE_WEEKDAYS,
    SLOT_INTERVAL_MINUTES,
    US_SCAN_SLOTS,
)

DEFAULT_INSTALL_ROOT = "/home/ubuntu/dante_bots/Dual-Screener-Bot"
CRON_USER = "ubuntu"

# KR 파일 전용: 장후·주간 (스캔 슬롯 SSOT와 별도 — 시각만 여기서 관리)
_KR_EXTRA_JOBS: Tuple[Tuple[int, int, str, str, str], ...] = (
    (45, 18, "1-5", "--daily-kr", "KR post-close audit (after last scan 17:30)"),
    (45, 6, "2-6", "--daily-us", "US post-close audit (KST morning after NY close)"),
    (5, 10, "6", "--weekly", "Weekly Flow master (Saturday KST)"),
    (
        0,
        10,
        "6",
        "--run-autonomous-analysis-only",
        "Legacy weekend brain surgery (optional)",
    ),
)

# US: KST polling window — covers ET Mon–Fri 10:00–16:40 (DST via factory_slot_dispatcher ET clock)
_US_DISPATCH_KST_HOURS_EVENING = "22,23"
_US_DISPATCH_KST_HOURS_MORNING = "0-6"
_US_DISPATCH_CRON_TZ = "Asia/Seoul"


def _cron_line(minute: int, hour: int, dow: str, command: str, install_root: str) -> str:
    return (
        f"{minute} {hour} * * {dow}  {CRON_USER}  "
        f"cd {install_root} && {command}"
    )


def _cron_line_schedule(schedule: str, command: str, install_root: str) -> str:
    """schedule = 'minute hour dom month dow' (five fields)."""
    return f"{schedule}  {CRON_USER}  cd {install_root} && {command}"


def _scan_command(factory_flag: str, *, tz: str) -> str:
    return f"TZ={tz} bash ./factory.sh {factory_flag}"


def _dispatcher_command(install_root: str, market: str) -> str:
    py = f"{install_root}/venv/bin/python"
    return f"{py} factory_slot_dispatcher.py --market {market}"


def render_kr_crontab(install_root: str) -> str:
    tz = SCHEDULE_MARKET_TZ["KR"]
    dow = SCHEDULE_WEEKDAYS["KR"]
    lines: List[str] = [
        "# Dual-Screener-Bot — KR factory cron (→ /etc/cron.d/dual-screener-factory-kr)",
        "#",
        f"# AUTO-GENERATED from factory_scan_schedule.py — do not edit by hand.",
        f"# Regenerate: python deploy/generate_factory_crontab.py",
        f"# Staggered scans: {SLOT_INTERVAL_MINUTES} min apart, KST from 10:00.",
        "# install: sudo INSTALL_ROOT=... bash deploy/install_factory_cron.sh",
        "#",
        f"# user/path: {CRON_USER} · {install_root}",
        "",
        "SHELL=/bin/bash",
        f"CRON_TZ={tz}",
        "PATH=/usr/local/bin:/usr/bin:/bin",
        "",
        f"# --- KR staggered intraday (Mon–Fri KST, {len(KR_SCAN_SLOTS)} slots, {SLOT_INTERVAL_MINUTES} min) ---",
    ]
    for slot in KR_SCAN_SLOTS:
        lines.append(
            _cron_line(slot.minute, slot.hour, dow, _scan_command(slot.factory_flag, tz=tz), install_root)
        )
    lines.append("")
    for minute, hour, extra_dow, flag, comment in _KR_EXTRA_JOBS:
        lines.append(f"# --- {comment} ---")
        if flag == "--run-autonomous-analysis-only":
            cmd = (
                f"{install_root}/venv/bin/python system_auto_pilot.py "
                f"--run-autonomous-analysis-only "
                f">>{install_root}/logs/autonomous_ubuntu.log 2>&1"
            )
        else:
            cmd = _scan_command(flag, tz=tz)
        lines.append(_cron_line(minute, hour, extra_dow, cmd, install_root))
        lines.append("")
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines) + "\n"


def render_us_crontab(install_root: str) -> str:
    lines: List[str] = [
        "# Dual-Screener-Bot — US factory cron (→ /etc/cron.d/dual-screener-factory-us)",
        "#",
        "# AUTO-GENERATED from factory_scan_schedule.py — do not edit by hand.",
        "# Regenerate: python deploy/generate_factory_crontab.py",
        f"# Staggered scans: {SLOT_INTERVAL_MINUTES} min apart, ET 10:00–16:40 (Mon–Fri).",
        "# Uses factory_slot_dispatcher.py — ET clock SSOT; does NOT rely on CRON_TZ=America/New_York.",
        "# install: sudo INSTALL_ROOT=... bash deploy/install_factory_cron.sh",
        "# diagnose: bash scripts/diag_cron_tz_effective.sh",
        "#",
        f"# user/path: {CRON_USER} · {install_root}",
        "",
        "SHELL=/bin/bash",
        f"CRON_TZ={_US_DISPATCH_CRON_TZ}",
        "PATH=/usr/local/bin:/usr/bin:/bin",
        "",
        f"# --- ET slot SSOT ({len(US_SCAN_SLOTS)} slots) — executed by dispatcher, not per-line ET cron ---",
    ]
    for slot in US_SCAN_SLOTS:
        lines.append(f"#   {slot.hour:02d}:{slot.minute:02d} ET  {slot.mode}")
    lines.append("")
    lines.append(
        "# --- US slot dispatcher (KST poll window ≈ ET regular session; DST-safe) ---"
    )
    disp = _dispatcher_command(install_root, "US")
    lines.append(
        _cron_line_schedule(
            f"*/5 {_US_DISPATCH_KST_HOURS_EVENING} * * *",
            disp,
            install_root,
        )
    )
    lines.append(
        _cron_line_schedule(
            f"*/5 {_US_DISPATCH_KST_HOURS_MORNING} * * *",
            disp,
            install_root,
        )
    )
    return "\n".join(lines) + "\n"


def _deploy_paths(repo_root: Path) -> Tuple[Path, Path]:
    deploy = repo_root / "deploy"
    return deploy / "factory.kr.crontab.example", deploy / "factory.us.crontab.example"


def write_templates(install_root: str, repo_root: Path | None = None) -> None:
    root = repo_root or _REPO_ROOT
    kr_path, us_path = _deploy_paths(root)
    kr_path.write_text(render_kr_crontab(install_root), encoding="utf-8", newline="\n")
    us_path.write_text(render_us_crontab(install_root), encoding="utf-8", newline="\n")
    print(f"OK wrote {kr_path}")
    print(f"OK wrote {us_path}")


def check_templates(install_root: str, repo_root: Path | None = None) -> int:
    root = repo_root or _REPO_ROOT
    kr_path, us_path = _deploy_paths(root)
    errors: List[str] = []
    expected = {
        kr_path: render_kr_crontab(install_root),
        us_path: render_us_crontab(install_root),
    }
    for path, want in expected.items():
        if not path.is_file():
            errors.append(f"missing {path}")
            continue
        got = path.read_text(encoding="utf-8")
        if got != want:
            errors.append(
                f"drift: {path} does not match factory_scan_schedule.py "
                f"(run: python deploy/generate_factory_crontab.py)"
            )
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1
    print("OK cron templates match factory_scan_schedule.py SSOT")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate factory cron templates from SSOT")
    parser.add_argument(
        "--install-root",
        default=DEFAULT_INSTALL_ROOT,
        help=f"path embedded in cron lines (default: {DEFAULT_INSTALL_ROOT})",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if deploy/*.crontab.example drift from SSOT",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.check:
        return check_templates(args.install_root)
    write_templates(args.install_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

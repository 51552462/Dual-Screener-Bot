#!/usr/bin/env python3
"""
bitget_scan_schedule.py → bitget/deploy/bitget.crontab.example

크론 시각·CRON_TZ·bitget.sh 플래그를 코드 SSOT에서만 생성합니다.

  python bitget/deploy/generate_bitget_crontab.py
  python bitget/deploy/generate_bitget_crontab.py --check
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BITGET_ROOT = _REPO_ROOT / "bitget"
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from bitget.bitget_scan_schedule import (  # noqa: E402
    ALL_SCAN_SLOTS,
    FUTURES_SCAN_SLOTS,
    SCHEDULE_MARKET_TZ,
    SCHEDULE_WEEKDAYS,
    SPOT_SCAN_SLOTS,
)

DEFAULT_INSTALL_ROOT = "/home/ubuntu/dante_bots/Dual-Screener-Bot"
CRON_USER = "ubuntu"


def _cron_line(minute: int, hour: int, dow: str, command: str, install_root: str) -> str:
    return f"{minute} {hour} * * {dow}  {CRON_USER}  cd {install_root} && {command}"


def _scan_command(bitget_flag: str, *, tz: str, install_root: str) -> str:
    bg = f"{install_root}/bitget/deploy/bitget.sh"
    return f"TZ={tz} {bg} {bitget_flag}"


def render_bitget_crontab(install_root: str) -> str:
    tz = SCHEDULE_MARKET_TZ["SPOT"]
    bg = f"{install_root}/bitget/deploy/bitget.sh"
    lines: List[str] = [
        "# Dual-Screener-Bot — Bitget factory cron (→ /etc/cron.d/dual-screener-bitget)",
        "#",
        "# AUTO-GENERATED from bitget/bitget_scan_schedule.py — do not edit by hand.",
        f"# Regenerate: python bitget/deploy/generate_bitget_crontab.py",
        "# 24/7 scans spread across the day at NON-multiple-of-5 minutes so they never",
        "# share a wall-clock minute with KR/US stock scans(:00..:50)/audits(:45) or",
        "# bitget ops(*/5). SPOT/FUTURES are interleaved (never simultaneous).",
        "# Server-safety: bitget heavy scans yield when the factory job lock is held",
        "# (BITGET_YIELD_TO_FACTORY=1 default) — KR/US cron/timing is left untouched.",
        "# install: sudo INSTALL_ROOT=... bash bitget/deploy/install_bitget_cron.sh",
        "#",
        f"# user/path: {CRON_USER} · {install_root}",
        "",
        "SHELL=/bin/bash",
        f"CRON_TZ={tz}",
        "PATH=/usr/local/bin:/usr/bin:/bin",
        "",
        f"# --- Ops (non-scan, 24/7) ---",
        "# track */15 (light) · watchdog */5 (light) keep running through stock hours.",
        "# reconcile :53 · data-refresh :43 — off stock :x0/:x5 minutes; data-refresh",
        "# also yields to factory. daily-audit/health/weekly run in the KST-pre-open idle window.",
        "*/15 * * * *  "
        + f"{CRON_USER}  cd {install_root} && TZ={tz} {bg} --track-positions",
        "53 * * * *  "
        + f"{CRON_USER}  cd {install_root} && TZ={tz} {bg} --reconcile",
        "43 */4 * * *  "
        + f"{CRON_USER}  cd {install_root} && TZ={tz} {bg} --data-refresh",
        "20 0 * * *  "
        + f"{CRON_USER}  cd {install_root} && TZ={tz} {bg} --daily-audit",
        "30 0 * * 1  "
        + f"{CRON_USER}  cd {install_root} && TZ={tz} {bg} --weekly-evolution",
        "*/5 * * * *  "
        + f"{CRON_USER}  cd {install_root} && TZ={tz} {bg} --watchdog",
        "15 0 * * *  "
        + f"{CRON_USER}  cd {install_root} && TZ={tz} {bg} --health",
        "",
        f"# --- SPOT staggered (24h, {len(SPOT_SCAN_SLOTS)} slots, non-%5 min) ---",
    ]
    dow_spot = SCHEDULE_WEEKDAYS["SPOT"]
    for slot in SPOT_SCAN_SLOTS:
        lines.append(
            _cron_line(
                slot.minute,
                slot.hour,
                dow_spot,
                _scan_command(slot.bitget_flag, tz=tz, install_root=install_root),
                install_root,
            )
        )
    lines.append("")
    lines.append(
        f"# --- FUTURES staggered (24h, {len(FUTURES_SCAN_SLOTS)} slots, non-%5 min) ---"
    )
    dow_fut = SCHEDULE_WEEKDAYS["FUTURES"]
    for slot in FUTURES_SCAN_SLOTS:
        lines.append(
            _cron_line(
                slot.minute,
                slot.hour,
                dow_fut,
                _scan_command(slot.bitget_flag, tz=tz, install_root=install_root),
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


def write_template(install_root: str, repo_root: Path | None = None) -> None:
    root = repo_root or _REPO_ROOT
    path = _deploy_path(root)
    path.write_text(render_bitget_crontab(install_root), encoding="utf-8", newline="\n")
    print(f"OK wrote {path}")


def check_template(install_root: str, repo_root: Path | None = None) -> int:
    root = repo_root or _REPO_ROOT
    path = _deploy_path(root)
    want = render_bitget_crontab(install_root)
    if not path.is_file():
        print(f"ERROR: missing {path}", file=sys.stderr)
        return 1
    got = path.read_text(encoding="utf-8")
    if got != want:
        print(
            f"ERROR: drift: {path} does not match bitget_scan_schedule.py "
            f"(run: python bitget/deploy/generate_bitget_crontab.py)",
            file=sys.stderr,
        )
        return 1
    print(f"OK {path} matches SSOT")
    return 0


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Bitget crontab from schedule SSOT")
    parser.add_argument("--install-root", default=DEFAULT_INSTALL_ROOT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args(argv)
    if args.check:
        return check_template(args.install_root)
    write_template(args.install_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

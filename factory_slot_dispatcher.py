"""
ET 장중 슬롯 디스패처 — CRON_TZ=America/New_York 없이 US/KR staggered 스캔 실행.

서버 cron이 system TZ(KST)만 쓰는 환경에서 US per-slot ET cron 이 KST 주간에
깨지는 문제를 피합니다. KST 22:00–07:00 창에서 */5 분마다 호출하고,
현재 America/New_York 시각이 SSOT 슬롯(±grace)과 맞을 때만 factory.sh 실행.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytz

from factory_scan_schedule import SCHEDULE_MARKET_TZ, ScanSlot, slots_for_market

_ROOT = Path(__file__).resolve().parent
_STATE_FILE = _ROOT / ".factory_slot_dispatch_state.json"
_GRACE_MINUTES = 4
_KR_TZ = pytz.timezone("Asia/Seoul")
_US_ET = pytz.timezone("America/New_York")


def _market_tz(market: str):
    name = SCHEDULE_MARKET_TZ.get(str(market or "").strip().upper(), "Asia/Seoul")
    return pytz.timezone(name)


def _slot_minutes(slot: ScanSlot) -> int:
    return int(slot.hour) * 60 + int(slot.minute)


def _now_minutes(now: datetime) -> int:
    return int(now.hour) * 60 + int(now.minute)


def _session_date(now: datetime) -> str:
    return now.strftime("%Y-%m-%d")


def _load_state() -> Dict[str, str]:
    if not _STATE_FILE.is_file():
        return {}
    try:
        raw = json.loads(_STATE_FILE.read_text(encoding="utf-8"))
        return {str(k): str(v) for k, v in (raw or {}).items()}
    except Exception:
        return {}


def _save_state(state: Dict[str, str]) -> None:
    tmp = _STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(_STATE_FILE)


def _state_key(slot: ScanSlot, session_date: str) -> str:
    return f"{slot.mode}:{session_date}"


def due_slots(
    market: str,
    *,
    now: Optional[datetime] = None,
    grace_minutes: int = _GRACE_MINUTES,
    state: Optional[Dict[str, str]] = None,
) -> Tuple[List[ScanSlot], Dict[str, str]]:
    """(slots to run, state dict — same object if passed in)."""
    mk = str(market or "").strip().upper()
    tz = _market_tz(mk)
    now_mkt = now.astimezone(tz) if now and now.tzinfo else datetime.now(tz)
    if int(now_mkt.weekday()) >= 5:
        return [], state or {}

    st = state if state is not None else _load_state()
    session = _session_date(now_mkt)
    cur = _now_minutes(now_mkt)
    grace = max(0, int(grace_minutes))
    due: List[ScanSlot] = []

    for slot in slots_for_market(mk):
        delta = abs(cur - _slot_minutes(slot))
        if delta > grace:
            continue
        key = _state_key(slot, session)
        if st.get(key):
            continue
        due.append(slot)
    return due, st


def _record_dispatched(state: Dict[str, str], slot: ScanSlot, session_date: str) -> None:
    state[_state_key(slot, session_date)] = datetime.now(_KR_TZ).strftime("%Y-%m-%dT%H:%M:%S%z")


def dispatch_market(
    market: str,
    *,
    dry_run: bool = False,
    grace_minutes: int = _GRACE_MINUTES,
    install_root: Optional[Path] = None,
) -> int:
    root = install_root or _ROOT
    mk = str(market or "").strip().upper()
    tz = _market_tz(mk)
    now_mkt = datetime.now(tz)
    due, state = due_slots(mk, now=now_mkt, grace_minutes=grace_minutes, state=_load_state())

    if not due:
        print(
            f"[slot_dispatcher] {mk} no due slots "
            f"(ET/KST now={now_mkt.strftime('%Y-%m-%d %H:%M %Z')})"
        )
        return 0

    factory_sh = root / "factory.sh"
    if not factory_sh.is_file():
        print(f"[slot_dispatcher] ERROR: missing {factory_sh}", file=sys.stderr)
        return 2

    rc = 0
    for slot in due:
        session = _session_date(now_mkt)
        cmd = ["bash", str(factory_sh), slot.factory_flag]
        print(
            f"[slot_dispatcher] {mk} dispatch {slot.mode} "
            f"({tz.zone} {slot.hour:02d}:{slot.minute:02d}, session={session})"
        )
        if dry_run:
            print(f"  dry-run: {' '.join(cmd)}")
            continue
        proc = subprocess.run(cmd, cwd=str(root), env=os.environ.copy())
        if proc.returncode != 0:
            print(
                f"[slot_dispatcher] WARN: {slot.mode} exit={proc.returncode}",
                file=sys.stderr,
            )
            rc = proc.returncode
            continue
        _record_dispatched(state, slot, session)
        _save_state(state)
    return rc


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="ET/KST-aware factory scan slot dispatcher")
    parser.add_argument("--market", required=True, choices=["KR", "US", "kr", "us"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--grace-minutes", type=int, default=_GRACE_MINUTES)
    parser.add_argument("--install-root", default=str(_ROOT))
    parser.add_argument("--list-due", action="store_true", help="print due slots and exit")
    args = parser.parse_args(argv)

    mk = str(args.market).upper()
    if args.list_due:
        tz = _market_tz(mk)
        now_mkt = datetime.now(tz)
        due, _ = due_slots(mk, now=now_mkt, grace_minutes=args.grace_minutes)
        for s in due:
            print(f"{s.mode}\t{s.hour:02d}:{s.minute:02d}\t{tz.zone}")
        return 0

    return dispatch_market(
        mk,
        dry_run=bool(args.dry_run),
        grace_minutes=int(args.grace_minutes),
        install_root=Path(args.install_root),
    )


if __name__ == "__main__":
    raise SystemExit(main())

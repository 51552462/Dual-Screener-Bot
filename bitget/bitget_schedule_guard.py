"""
Bitget cron·스캔 가드 — 유지보수 창 스킵 · cron TZ 오설정 힌트 (24/7 코인).

Two-Track air-gap: yield-to-factory 는 기본 OFF. cgroup·독립 락/큐로 병렬 가동하며,
주식 factory 락은 읽기 전용으로만 참조한다(양보 스킵 비활성). 레거시 4GB 단일 서버만
`BITGET_YIELD_TO_FACTORY=1` 로 재활성화 가능.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import pytz

from bitget.bitget_scan_schedule import scan_mode_market, slot_for_mode

_UTC = pytz.UTC

# bitget/ 의 부모 = 레포 루트. factory_runtime 의 lock 도 같은 위치(.factory_runtime.lock).
_REPO_ROOT = Path(__file__).resolve().parents[1]
_FACTORY_LOCK_PATH = str(_REPO_ROOT / ".factory_runtime.lock")
# factory 의 무거운(=GPU/RAM 큰) 모드 prefix. track/reconcile 같은 경량 잡엔 양보 안 함.
_FACTORY_HEAVY_PREFIXES = ("scan_", "daily_", "weekly")
# bitget 에서 factory 에 양보할 무거운 모드.
_YIELD_GATED_MODES = ("scan_", "data_refresh")


def utc_now() -> datetime:
    return datetime.now(_UTC)


def _factory_yield_disabled() -> bool:
    """Two-Track air-gap: 기본값 0(양보 무효화). 1/true 로만 레거시 yield 활성."""
    return str(os.environ.get("BITGET_YIELD_TO_FACTORY", "0")).strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    )


def _factory_yield_max_age_sec() -> float:
    """factory lock 이 이 시간보다 오래되면(좀비/멈춤 의심) 양보하지 않음 — bitget 기아 방지."""
    raw = str(os.environ.get("BITGET_FACTORY_YIELD_MAX_AGE_SEC", "5400")).strip()
    try:
        return max(60.0, float(raw))
    except ValueError:
        return 5400.0


def _pid_alive(pid: int) -> bool:
    """Windows-safe PID 생존 체크. (factory_runtime._pid_is_alive 는 win32 에서
    os.kill(pid, 0)=CTRL_C_EVENT 을 보내 위험 — 여기선 회피)."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        # dev 전용 환경 — 안전하게 '확인 불가'를 비활성(False)로 처리. 운영은 Linux.
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def factory_heavy_job_active() -> Tuple[bool, str]:
    """
    주식 factory 가 무거운 잡을 실제로 실행 중인지 best-effort 판단.
    (살아있는 PID + heavy mode + lock 나이 < max_age) 일 때만 True.
    실패/판단불가 시 False(fail-open) — bitget liveness 우선.
    """
    if _factory_yield_disabled():
        return False, ""
    try:
        import factory_runtime as fr

        meta = fr._parse_lock_metadata(_FACTORY_LOCK_PATH)
        if meta is None:
            return False, ""
        if not _pid_alive(meta.pid):
            return False, ""
        mode = str(meta.mode or "").strip().lower()
        if not any(mode.startswith(p) for p in _FACTORY_HEAVY_PREFIXES):
            return False, ""
        age = fr._lock_file_age_sec(_FACTORY_LOCK_PATH)
        if age >= _factory_yield_max_age_sec():
            return False, ""
        return True, f"factory '{mode}' active pid={meta.pid} age={age:.0f}s"
    except Exception:
        return False, ""


def _parse_hour_ranges(spec: str) -> list[tuple[int, int]]:
    """'2-4,22-23' → [(2,4), (22,23)] inclusive hours."""
    out: list[tuple[int, int]] = []
    for part in str(spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.append((int(a), int(b)))
        else:
            h = int(part)
            out.append((h, h))
    return out


def is_maintenance_window(*, now_utc: Optional[datetime] = None) -> Tuple[bool, str]:
    """
    BITGET_SCAN_MAINTENANCE_UTC_HOURS=2-4,22-23 이면 해당 UTC 시각 스캔 스킵.
    """
    spec = os.environ.get("BITGET_SCAN_MAINTENANCE_UTC_HOURS", "").strip()
    if not spec:
        return False, ""
    now = now_utc or utc_now()
    if now.tzinfo is None:
        now = _UTC.localize(now)
    else:
        now = now.astimezone(_UTC)
    hour = int(now.hour)
    for lo, hi in _parse_hour_ranges(spec):
        if lo <= hour <= hi:
            return True, f"maintenance UTC hour {hour} in {spec}"
    return False, ""


def evaluate_scan_skip(
    mode: str,
    *,
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    스캔 실행 전 게이트. (skip, reason)
    BITGET_FACTORY_SCAN_DISABLED=1 → 전체 스캔 중단.
    """
    m = str(mode or "").strip().lower()
    gated = m.startswith("scan_") or m in _YIELD_GATED_MODES
    if not gated:
        return False, ""

    if m.startswith("scan_") and os.environ.get(
        "BITGET_FACTORY_SCAN_DISABLED", ""
    ).strip() in ("1", "true", "yes"):
        return True, "BITGET_FACTORY_SCAN_DISABLED"

    if os.environ.get("BITGET_FORCE_SCAN", "").strip() in ("1", "true", "yes"):
        return False, ""

    maint, why = is_maintenance_window(now_utc=now_utc)
    if maint:
        return True, why

    # 4GB 서버 보호 — 주식 factory 무거운 잡과 동시 실행 회피 (KR/US 는 그대로, bitget 만 양보).
    busy, detail = factory_heavy_job_active()
    if busy:
        return True, f"yield_to_factory: {detail}"

    return False, ""


def is_quiet_scan_skip(mode: str, *, detail: str = "") -> bool:
    """예상된 스킵 — 텔레그램 알람 생략."""
    skip, reason = evaluate_scan_skip(mode)
    if skip:
        return True
    if "SKIPPED_SESSION" in detail:
        return False
    # lock 경합(특히 data_refresh OHLCV)은 cron 정상 동작 — 텔레그램 생략
    d = str(detail or "")
    if "bitget lock busy" in d:
        return True
    return False


def cron_misalignment_hint(
    mode: str,
    *,
    now_utc: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    슬롯 모드가 SSOT 시각에서 ±8분 이상 벗어나면 cron drift 힌트.
    """
    slot = slot_for_mode(mode)
    if slot is None:
        return False, ""
    now = now_utc or utc_now()
    if now.tzinfo is None:
        now = _UTC.localize(now)
    else:
        now = now.astimezone(_UTC)
    expected = slot.hour * 60 + slot.minute
    actual = now.hour * 60 + now.minute
    delta = abs(actual - expected)
    delta = min(delta, 24 * 60 - delta)  # circular minute distance (avoid 955m false positives)
    if delta > 8 and delta < (24 * 60 - 8):
        mk = scan_mode_market(mode) or slot.market
        return (
            True,
            f"{mk} {mode} wall_clock drift {delta}m from SSOT "
            f"{slot.hour:02d}:{slot.minute:02d} UTC — regenerate cron: "
            "sudo INSTALL_ROOT=$PWD/bitget/.. bash bitget/deploy/install_bitget_cron.sh",
        )
    return False, ""

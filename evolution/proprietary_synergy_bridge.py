"""
Proprietary Synergy Bridge — 스캔 Abort 시 Offline R&D 샌드박스로 안전 전환.

cron/daemon 과 충돌 방지: 파일 락 + 백그라운드 스레드 (동일 프로세스).
"""
from __future__ import annotations

import atexit
import logging
import os
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_LOCK_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "artifacts",
    "proprietary_rnd",
    ".sandbox.lock",
)
_BG_THREAD: Optional[threading.Thread] = None


def _lock_dir() -> str:
    d = os.path.dirname(_LOCK_PATH)
    os.makedirs(d, exist_ok=True)
    return d


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_sandbox_lock(*, stale_sec: int = 7200) -> bool:
    """비블로킹 락 — 이미 실행 중이면 False."""
    _lock_dir()
    if os.path.isfile(_LOCK_PATH):
        try:
            with open(_LOCK_PATH, encoding="utf-8") as fh:
                raw = fh.read().strip().split(",")
            pid = int(raw[0]) if raw else 0
            ts = float(raw[1]) if len(raw) > 1 else 0.0
        except (OSError, ValueError):
            pid, ts = 0, 0.0
        age = time.time() - ts if ts else 0.0
        if _pid_alive(pid) and age < stale_sec:
            return False
        try:
            os.remove(_LOCK_PATH)
        except OSError:
            pass
    try:
        with open(_LOCK_PATH, "w", encoding="utf-8") as fh:
            fh.write(f"{os.getpid()},{time.time():.0f}")
        return True
    except OSError:
        return False


def release_sandbox_lock() -> None:
    try:
        if os.path.isfile(_LOCK_PATH):
            with open(_LOCK_PATH, encoding="utf-8") as fh:
                raw = fh.read().strip().split(",")
            if raw and int(raw[0]) == os.getpid():
                os.remove(_LOCK_PATH)
    except (OSError, ValueError):
        pass


atexit.register(release_sandbox_lock)


def _run_sandbox_worker(market: str, decision: Optional[Dict[str, Any]] = None) -> None:
    try:
        from offline_rnd_sandbox import run_offline_rnd_sandbox

        result = run_offline_rnd_sandbox(market)
        logger.info(
            "Offline R&D [%s] done stress=%s mining=%s",
            market,
            result.get("stress", {}).get("reason"),
            result.get("mining", {}).get("reason"),
        )
        print(
            f"🧪 [Offline R&D {market}] stress={result.get('stress', {}).get('reason')} "
            f"| mining={result.get('mining', {}).get('reason')}"
        )
        if decision:
            print(f"   ↳ trigger: {decision.get('reason', decision)}")
    except Exception as ex:
        logger.exception("Offline R&D sandbox failed [%s]: %s", market, ex)
        print(f"⚠️ [Offline R&D {market}] 실패: {ex}")
    finally:
        release_sandbox_lock()


def run_offline_rnd_on_scan_abort(
    market: str,
    decision: Any = None,
    *,
    background: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    SessionDeduplicationGuard Abort → 내부 R&D 샌드박스.
    OFFLINE_RND_BACKGROUND=0 이면 동기 실행.
    """
    mk = str(market or "KR").upper()
    dec_dict: Dict[str, Any] = {}
    if decision is not None:
        if hasattr(decision, "as_dict"):
            dec_dict = decision.as_dict()
        elif isinstance(decision, dict):
            dec_dict = decision

    if not acquire_sandbox_lock():
        msg = f"sandbox_busy skip {mk}"
        print(f"⏭️ [Offline R&D] {msg}")
        return {"ok": False, "market": mk, "reason": "sandbox_busy", "decision": dec_dict}

    use_bg = background
    if use_bg is None:
        use_bg = str(os.environ.get("OFFLINE_RND_BACKGROUND", "1")).strip().lower() not in (
            "0",
            "false",
            "no",
            "off",
        )

    if use_bg:
        global _BG_THREAD
        t = threading.Thread(
            target=_run_sandbox_worker,
            args=(mk, dec_dict),
            name=f"offline-rnd-{mk}",
            daemon=True,
        )
        _BG_THREAD = t
        t.start()
        print(f"🔬 [Offline R&D {mk}] 백그라운드 전환 (stale_session → 내부 R&D)")
        return {"ok": True, "market": mk, "mode": "background", "decision": dec_dict}

    _run_sandbox_worker(mk, dec_dict)
    return {"ok": True, "market": mk, "mode": "inline", "decision": dec_dict}

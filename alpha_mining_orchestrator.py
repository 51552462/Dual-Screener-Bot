"""
Alpha Mining Orchestrator — 주간 hunt_supernovas · evolve_alpha_factors · cluster mining.

system_auto_pilot 데몬 주말 스케줄에서 비블로킹 subprocess 로 기동.
메인 트레이딩 루프(GIL·장시간 작업)와 분리.
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_FACTORY_ROOT = os.path.dirname(os.path.abspath(__file__))
_LOCK_PATH = os.path.join(_FACTORY_ROOT, "artifacts", "alpha_mining", ".orchestrator.lock")
_LOG_DIR = os.path.join(_FACTORY_ROOT, "artifacts", "alpha_mining")
_STALE_LOCK_SEC = 6 * 3600


def _ensure_dirs() -> None:
    os.makedirs(_LOG_DIR, exist_ok=True)


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_orchestrator_lock() -> bool:
    """비블로킹 — 이미 실행 중이면 False."""
    _ensure_dirs()
    if os.path.isfile(_LOCK_PATH):
        try:
            with open(_LOCK_PATH, encoding="utf-8") as fh:
                parts = fh.read().strip().split(",")
            pid = int(parts[0]) if parts else 0
            ts = float(parts[1]) if len(parts) > 1 else 0.0
        except (OSError, ValueError):
            pid, ts = 0, 0.0
        age = time.time() - ts if ts else 0.0
        if _pid_alive(pid) and age < _STALE_LOCK_SEC:
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


def release_orchestrator_lock() -> None:
    try:
        if os.path.isfile(_LOCK_PATH):
            with open(_LOCK_PATH, encoding="utf-8") as fh:
                raw = fh.read().strip().split(",")
            if raw and int(raw[0]) == os.getpid():
                os.remove(_LOCK_PATH)
    except (OSError, ValueError):
        pass


atexit.register(release_orchestrator_lock)


def run_alpha_mining_pipeline() -> Dict[str, Any]:
    """
    동기 실행 — KR/US 타임머신 · 알파 팩터 · 클러스터 · 언더독 마이닝.
    데몬에서는 subprocess 로 이 함수만 호출한다.
    """
    started = datetime.now().isoformat(timespec="seconds")
    out: Dict[str, Any] = {
        "ok": False,
        "started_at": started,
        "steps": {},
        "errors": [],
    }
    if not acquire_orchestrator_lock():
        out["reason"] = "orchestrator_busy"
        return out

    try:
        from supernova_hunter import evolve_alpha_factors, hunt_supernovas

        for mk in ("KR", "US"):
            key = f"hunt_supernovas_{mk}"
            try:
                print(f"[Alpha Mining] hunt_supernovas({mk}) ...")
                hunt_supernovas(mk)
                out["steps"][key] = "ok"
            except Exception as ex:
                logger.exception("hunt_supernovas %s failed: %s", mk, ex)
                out["steps"][key] = f"error:{ex}"
                out["errors"].append(key)

        try:
            print("[Alpha Mining] evolve_alpha_factors() ...")
            evolve_alpha_factors()
            out["steps"]["evolve_alpha_factors"] = "ok"
        except Exception as ex:
            logger.exception("evolve_alpha_factors failed: %s", ex)
            out["steps"]["evolve_alpha_factors"] = f"error:{ex}"
            out["errors"].append("evolve_alpha_factors")

        try:
            import data_miner

            print("[Alpha Mining] data_miner.run_cluster_mining() ...")
            data_miner.run_cluster_mining()
            out["steps"]["cluster_mining"] = "ok"
        except ModuleNotFoundError:
            out["steps"]["cluster_mining"] = "skip:no_data_miner"
        except Exception as ex:
            logger.exception("cluster_mining failed: %s", ex)
            out["steps"]["cluster_mining"] = f"error:{ex}"
            out["errors"].append("cluster_mining")

        try:
            import underdog_miner

            print("[Alpha Mining] underdog_miner.run_underdog_mining() ...")
            underdog_miner.run_underdog_mining()
            out["steps"]["underdog_mining"] = "ok"
        except ModuleNotFoundError:
            out["steps"]["underdog_mining"] = "skip:no_underdog_miner"
        except Exception as ex:
            logger.exception("underdog_mining failed: %s", ex)
            out["steps"]["underdog_mining"] = f"error:{ex}"
            out["errors"].append("underdog_mining")

        try:
            import ops_logger

            ops_logger.record_heartbeat("scanner.alpha_mining_orchestrator")
        except Exception:
            pass

        out["ok"] = not out["errors"]
        out["finished_at"] = datetime.now().isoformat(timespec="seconds")
        out["reason"] = "complete" if out["ok"] else "partial_errors"

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(_LOG_DIR, f"mining_report_{stamp}.json")
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False, indent=2)
        out["artifact"] = report_path
        print(f"[Alpha Mining] done -> {report_path}")
        return out
    finally:
        release_orchestrator_lock()


def spawn_weekly_alpha_mining(*, tag: str = "alpha_mining_sun0400") -> Dict[str, Any]:
    """
    데몬 메인 루프용 — OS 분리 subprocess, 즉시 반환.
    """
    _ensure_dirs()
    if os.path.isfile(_LOCK_PATH):
        try:
            with open(_LOCK_PATH, encoding="utf-8") as fh:
                parts = fh.read().strip().split(",")
            pid = int(parts[0]) if parts else 0
            if _pid_alive(pid):
                msg = f"alpha_mining busy pid={pid}"
                print(f"[Alpha Mining] skip {msg}")
                return {"ok": False, "reason": "busy", "pid": pid}
        except (OSError, ValueError):
            pass

    log_path = os.path.join(_LOG_DIR, f"satellite_{tag}.log")
    worker = (
        "import sys, os, json\n"
        f"sys.path.insert(0, {repr(_FACTORY_ROOT)})\n"
        f"os.chdir({repr(_FACTORY_ROOT)})\n"
        "from alpha_mining_orchestrator import run_alpha_mining_pipeline\n"
        "print(json.dumps(run_alpha_mining_pipeline(), ensure_ascii=False))\n"
    )
    try:
        lf = open(log_path, "ab", buffering=0)
        try:
            subprocess.Popen(
                [sys.executable, "-c", worker],
                cwd=_FACTORY_ROOT,
                stdin=subprocess.DEVNULL,
                stdout=lf,
                stderr=subprocess.STDOUT,
                close_fds=True,
                start_new_session=True,
            )
        finally:
            lf.close()
        print(f"[Alpha Mining] background spawn -> {os.path.basename(log_path)}")
        return {"ok": True, "mode": "background", "log": log_path, "tag": tag}
    except Exception as ex:
        logger.exception("spawn_weekly_alpha_mining failed: %s", ex)
        return {"ok": False, "reason": str(ex)}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Weekly alpha mining pipeline")
    parser.add_argument("--spawn", action="store_true", help="background subprocess (daemon style)")
    args = parser.parse_args()
    if args.spawn:
        result = spawn_weekly_alpha_mining(tag="cli_spawn")
    else:
        result = run_alpha_mining_pipeline()
    print(json.dumps(result, ensure_ascii=False, indent=2))

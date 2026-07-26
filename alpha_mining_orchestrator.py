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
from typing import Any, Callable, Dict, Optional

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


def _load_pipeline_config() -> Dict[str, Any]:
    try:
        from config_manager import load_system_config

        return dict(load_system_config() or {})
    except Exception as ex:
        logger.warning("load_system_config failed: %s", ex)
        return {}


def _persist_dna_mutation_config(cfg: Dict[str, Any]) -> bool:
    patches: Dict[str, Any] = {}
    for key in ("INCUBATOR_TEMPLATES", "MUTANT_GENE_POOL", "DNA_MUTATION_LAST_RUN"):
        val = cfg.get(key)
        if val is not None:
            patches[key] = val
    if not patches:
        return False
    try:
        from config_manager import update_system_config

        update_system_config(patches)
        return True
    except Exception as ex:
        logger.warning("dna mutation persist failed: %s", ex)
        return False


def _summarize_bandit_apoptosis(bandit_summary: Dict[str, Any]) -> Dict[str, Any]:
    """feed_rewards_to_bandit 내부 check_apoptosis · register_failed_template 결과 추출."""
    removed: list[str] = []
    vaccines_registered = 0
    vaccines_failed = 0
    by_market: Dict[str, Any] = {}

    for mk, block in (bandit_summary.get("markets") or {}).items():
        if not isinstance(block, dict):
            continue
        apo = block.get("apoptosis") or {}
        if not isinstance(apo, dict):
            continue
        mk_removed = list(apo.get("removed") or [])
        removed.extend(mk_removed)
        mk_vaccines = list(apo.get("vaccines") or [])
        mk_reg = sum(1 for v in mk_vaccines if isinstance(v, dict) and v.get("registered"))
        mk_fail = sum(1 for v in mk_vaccines if isinstance(v, dict) and not v.get("registered"))
        vaccines_registered += mk_reg
        vaccines_failed += mk_fail
        by_market[str(mk)] = {
            "removed": mk_removed,
            "vaccines_registered": mk_reg,
            "vaccines_failed": mk_fail,
            "freed_min_weight": apo.get("freed_min_weight"),
            "freed_slots": apo.get("freed_slots"),
        }

    return {
        "removed_total": len(removed),
        "removed_names": removed,
        "vaccines_registered": vaccines_registered,
        "vaccines_failed": vaccines_failed,
        "by_market": by_market,
    }


def run_self_evolution_pipeline(
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    persist: bool = True,
    send_telegram: bool = True,
    send_fn: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    """
    주말 자가진화 파이프라인 (순차):
      1) feed_rewards_to_bandit — LinUCB 보상 피드백
      2) check_apoptosis + register_failed_template — (1) 내부 일괄 실행
      3) run_weekend_dna_mutation_cycle — GAN 적자생존 변이
    """
    started = datetime.now().isoformat(timespec="seconds")
    result: Dict[str, Any] = {
        "ok": False,
        "started_at": started,
        "steps": {},
        "errors": [],
    }

    own_cfg = sys_config is None
    cfg = dict(sys_config) if isinstance(sys_config, dict) else _load_pipeline_config()

    # --- 1) 보상 피드백 → LinUCB (내부에서 apoptosis·백신 등록) ---
    print("[Self-Evolution] feed_rewards_to_bandit ...")
    try:
        from template_bandit import feed_rewards_to_bandit

        bandit_summary = feed_rewards_to_bandit(
            sys_config=cfg,
            persist=persist,
        )
        apoptosis_summary = _summarize_bandit_apoptosis(bandit_summary)
        result["steps"]["feed_rewards_to_bandit"] = bandit_summary
        result["steps"]["check_apoptosis"] = apoptosis_summary
        result["steps"]["register_failed_template"] = {
            "registered": apoptosis_summary["vaccines_registered"],
            "failed": apoptosis_summary["vaccines_failed"],
            "by_market": {
                mk: {
                    "removed": block.get("removed") or [],
                    "vaccines_registered": block.get("vaccines_registered", 0),
                }
                for mk, block in apoptosis_summary.get("by_market", {}).items()
            },
        }
        print(
            f"[Self-Evolution] bandit updated={bandit_summary.get('updated', 0)} "
            f"apoptosis_removed={apoptosis_summary.get('removed_total', 0)}"
        )
    except Exception as ex:
        logger.exception("feed_rewards_to_bandit failed: %s", ex)
        result["steps"]["feed_rewards_to_bandit"] = f"error:{ex}"
        result["errors"].append("feed_rewards_to_bandit")

    # --- 2) GAN 적자생존 DNA 변이 ---
    print("[Self-Evolution] run_weekend_dna_mutation_cycle ...")
    try:
        from dna_mutator import run_weekend_dna_mutation_cycle

        updated_cfg, mut_logs = run_weekend_dna_mutation_cycle(cfg)
        cfg.clear()
        cfg.update(updated_cfg)

        elite_names: list[str] = []
        mutant_names: list[str] = []
        gene_pool = cfg.get("MUTANT_GENE_POOL") or {}
        if isinstance(gene_pool, dict):
            for name, meta in gene_pool.items():
                if not isinstance(meta, dict):
                    continue
                if meta.get("type") == "ELITE_SPINOFF":
                    elite_names.append(str(name))
                elif str(name).startswith("MUTANT_"):
                    mutant_names.append(str(name))

        dna_persisted = False
        if persist:
            dna_persisted = _persist_dna_mutation_config(cfg)

        result["steps"]["run_weekend_dna_mutation_cycle"] = {
            "logs": mut_logs,
            "mutants_created": mutant_names,
            "elite_spinoffs": elite_names,
            "persisted": dna_persisted,
        }
        print(
            f"[Self-Evolution] mutants={len(mutant_names)} "
            f"elite_spinoffs={len(elite_names)}"
        )
    except Exception as ex:
        logger.exception("run_weekend_dna_mutation_cycle failed: %s", ex)
        result["steps"]["run_weekend_dna_mutation_cycle"] = f"error:{ex}"
        result["errors"].append("run_weekend_dna_mutation_cycle")

    result["ok"] = not result["errors"]
    result["finished_at"] = datetime.now().isoformat(timespec="seconds")

    if send_telegram:
        try:
            from evolution_digest import send_weekend_self_evolution_digest

            send_weekend_self_evolution_digest(result, send_fn=send_fn)
        except Exception as ex:
            logger.warning("weekend evolution telegram skip: %s", ex)
            result["telegram_error"] = str(ex)

    if own_cfg:
        pass  # cfg was loaded locally; caller did not pass shared dict

    return result


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
            print("[Alpha Mining] run_self_evolution_pipeline() ...")
            evo = run_self_evolution_pipeline(send_telegram=True)
            out["steps"]["self_evolution"] = evo
            if not evo.get("ok"):
                out["errors"].append("self_evolution")
        except Exception as ex:
            logger.exception("self_evolution pipeline failed: %s", ex)
            out["steps"]["self_evolution"] = f"error:{ex}"
            out["errors"].append("self_evolution")

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

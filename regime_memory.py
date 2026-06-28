"""
Regime Time-Space Dual Memory — 단기(Working) / 장기(Deep Archive) 기억 분리 + 오프라인 메타러닝.

설계(헤지펀드급 시공간 메모리):
  - 평일(Working Memory): regime_analog_engine 의 가벼운 마할라노비스 연산으로 기민하게
    현재 국면을 해석한다. '블랙 스완'(과거 어떤 국면으로도 설명 안 되는 저유사도)이면
    무거운 연산을 즉시 돌리지 않고 task_queue 에 'Priority 3 주말 심층분석용'으로 예약만 한다.
  - 주말(Deep Archive): 토요일 장 마감 후, 예약된 작업을 꺼내 10년+ 전체 역사를 DTW 로
    심층 탐색해 '현재와 닮은 과거의 전설적 승자 DNA(archetype)'를 찾는다.
  - 상호 진화 폐루프(Cross-Evolution): 주말의 무거운 학습 결과(archetype)를 월요일에 쓰일
    단기 캐시 + 실전 메인 템플릿에 병합(morph)하여, 무거운 학습이 가벼운 평일 전투력을
    영구히 진화시킨다.

Zero-Collision(Mission 6):
  - 진화 작업은 task_orchestrator 의 **최하위 우선순위(Priority 3)** 로만 실행한다.
  - 24시간 도는 코인(Bitget) 스캔(Priority 1)과 충돌하지 않도록, 무거운 루프 중간마다
    메인 큐를 폴링해 코인 작업이 있으면 os.nice(19) + sleep 으로 CPU 를 완전 양보(Active
    Throttling)한다.
  - regime 진화 작업은 코인 워커가 절대 집어가지 못하도록 **전용 큐 DB**(regime_task_queue
    .sqlite)에 적재한다. 코인 활동 감지는 메인 task_queue.sqlite 를 읽기 전용으로 폴링한다.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
DEEP_ARCHIVE_MODE = "regime_deep_archive"
EVO_ENGINE = "EVO"            # VALID_ENGINES 밖 → 권력이양에서 절대 Primary 가 되지 않음
PRIORITY_EVOLUTION = 3        # 최하위(코인 1 보다 낮음)

WORKING_MEMORY_MAX_YEARS = 2  # 평일 단기 기억 윈도우(개념)
DEEP_ARCHIVE_MIN_YEAR = 2008  # 장기 기억 시작 연도

BLACK_SWAN_MAX_SCORE_KEY = "REGIME_BLACK_SWAN_MAX_SCORE"
DEFAULT_BLACK_SWAN_MAX_SCORE = 0.55
WORKING_MEMORY_CACHE_KEY = "REGIME_WORKING_MEMORY_CACHE"

# 능동형 스로틀링 기본값
THROTTLE_POLL_SEC = 3.0
THROTTLE_MAX_WAIT_SEC = 1800.0  # 코인 스캔 양보 최대 대기(30분)
DEEP_NICE_LEVEL = 19            # 리눅스 최저 우선순위


# ---------------------------------------------------------------------------
# task_orchestrator (bitget.infra) 안전 import
# ---------------------------------------------------------------------------
def _orch():
    try:
        from bitget.infra import task_orchestrator as orch  # type: ignore

        return orch
    except Exception:
        return None


def main_queue_db_path() -> Optional[str]:
    """코인(Bitget) 메인 큐 경로 — 코인 활동 감지용(읽기 전용)."""
    orch = _orch()
    if orch is None:
        return None
    try:
        return orch._queue_db_path()  # noqa: SLF001 (의도적 재사용)
    except Exception:
        return None


def regime_queue_db_path() -> str:
    """regime 진화 전용 큐 경로(코인 워커와 물리 분리)."""
    env = (os.environ.get("REGIME_TASK_QUEUE_DB_PATH") or "").strip()
    if env:
        return os.path.abspath(os.path.expanduser(env))
    try:
        from bitget.infra.data_paths import bitget_data_dir

        return os.path.join(bitget_data_dir(), "regime_task_queue.sqlite")
    except Exception:
        try:
            from factory_data_paths import factory_data_dir

            return os.path.join(factory_data_dir(), "regime_task_queue.sqlite")
        except Exception:
            return os.path.join(os.getcwd(), "regime_task_queue.sqlite")


# ---------------------------------------------------------------------------
# config 헬퍼
# ---------------------------------------------------------------------------
def _load_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(cfg, dict):
        return cfg
    try:
        from config_manager import load_system_config

        return load_system_config() or {}
    except Exception:
        return {}


def _set_cfg_value(key: str, value: Any) -> bool:
    try:
        from config_manager import set_config_value

        set_config_value(key, value)
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Mission 4: 평일 블랙스완 감지 → 예약(Enqueue) (무거운 연산 금지)
# ---------------------------------------------------------------------------
def is_black_swan(analog_result: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None) -> bool:
    """
    단기 기억으로 현재 국면이 설명되지 않으면(=과거 어떤 국면과도 충분히 닮지 않음) 블랙스완.
    데이터가 불완전하면(콜드스타트) 블랙스완으로 보지 않는다(오탐 방지).
    """
    if not isinstance(analog_result, dict):
        return False
    cfg = _load_cfg(cfg)
    try:
        max_score = float(cfg.get(BLACK_SWAN_MAX_SCORE_KEY, DEFAULT_BLACK_SWAN_MAX_SCORE))
    except (TypeError, ValueError):
        max_score = DEFAULT_BLACK_SWAN_MAX_SCORE

    completeness = float(analog_result.get("data_completeness") or 0.0)
    if completeness < 0.8:
        return False
    score = float(analog_result.get("score") or 0.0)
    return score < max_score


def enqueue_deep_analysis(
    payload: Dict[str, Any],
    *,
    reason: str = "black_swan",
    db_path: Optional[str] = None,
    dedupe: bool = True,
) -> Optional[int]:
    """
    주말 심층 분석을 task_orchestrator 큐(regime 전용)에 Priority 3 으로 예약한다.
    task_orchestrator 부재 시 config 폴백 큐(REGIME_DEEP_QUEUE_FALLBACK)에 적재.
    """
    body = dict(payload or {})
    body["reason"] = reason
    body["enqueued_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    orch = _orch()
    if orch is not None:
        try:
            path = db_path or regime_queue_db_path()
            return orch.enqueue(
                EVO_ENGINE,
                DEEP_ARCHIVE_MODE,
                payload=body,
                priority=PRIORITY_EVOLUTION,
                dedupe=dedupe,
                db_path=path,
            )
        except Exception:
            pass

    # 폴백: config 리스트에 적재(주말 워커가 없어도 기록은 남긴다)
    def _modifier(old: Any) -> Any:
        buf = old if isinstance(old, list) else []
        buf.append(body)
        return buf[-50:]

    try:
        from config_manager import update_config_value

        update_config_value("REGIME_DEEP_QUEUE_FALLBACK", _modifier)
    except Exception:
        pass
    return None


def enqueue_emergency_remorph(
    *,
    failed_episode: Optional[str] = None,
    failed_template: Optional[str] = None,
    market: str = "GLOBAL",
    db_path: Optional[str] = None,
) -> Optional[int]:
    """
    [재진화 트리거] 실패 즉시(평일이라도) task_orchestrator 에 Priority 3 '긴급 재탐색'을 예약.
    실패한 국면을 제외하고 두 번째로 닮았던 과거의 정답(Plan B)을 찾아 자가 교정한다.
    """
    payload: Dict[str, Any] = {
        "market": market,
        "failed_episode": failed_episode,
        "failed_template": failed_template,
        "exclude_episodes": [failed_episode] if failed_episode else [],
        "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return enqueue_deep_analysis(payload, reason="emergency_remorph", db_path=db_path, dedupe=False)


def maybe_enqueue_black_swan(
    analog_result: Dict[str, Any],
    *,
    cfg: Optional[Dict[str, Any]] = None,
    market: str = "GLOBAL",
    db_path: Optional[str] = None,
) -> Optional[int]:
    """평일 working-memory 산출 결과가 블랙스완이면 주말 심층분석을 예약. 아니면 None."""
    if not is_black_swan(analog_result, cfg):
        return None
    payload = {
        "market": market,
        "current_vector": analog_result.get("current_vector"),
        "current_vector_map": analog_result.get("current_vector_map"),
        "best_episode": analog_result.get("best_episode"),
        "score": analog_result.get("score"),
        "vector_dims": analog_result.get("vector_dims"),
        "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return enqueue_deep_analysis(payload, reason="black_swan", db_path=db_path)


# ---------------------------------------------------------------------------
# Mission 6: 능동형 스로틀링 (코인 스캔에 CPU 완전 양보)
# ---------------------------------------------------------------------------
def _lower_to_min_priority() -> bool:
    """현재 프로세스를 리눅스 최저 우선순위(nice 19)에 근접시킨다. 실패 시 False."""
    if not hasattr(os, "nice"):
        return False
    try:
        # 현재 nice 를 모르므로 큰 증가분을 시도(상한 19에서 클램프됨).
        os.nice(DEEP_NICE_LEVEL)
        return True
    except OSError:
        try:
            os.nice(10)
            return True
        except OSError:
            return False


def coin_scan_active(*, db_path: Optional[str] = None) -> bool:
    """
    메인 큐(코인)에서 BITGET 작업이 PENDING/RUNNING 인지, 혹은 지금이 코인 주도 세션인지 확인.
    하나라도 참이면 진화 작업은 양보해야 한다.
    """
    orch = _orch()
    # 1) 메인 큐의 코인 작업 적체 여부(읽기 전용)
    path = db_path or main_queue_db_path()
    if path and os.path.isfile(path):
        try:
            uri = str(path).replace("\\", "/")
            conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=10)
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM task_queue "
                    "WHERE engine='BITGET' AND status IN ('PENDING','RUNNING')"
                ).fetchone()
                if row and int(row[0]) > 0:
                    return True
            finally:
                conn.close()
        except sqlite3.Error:
            pass
    # 2) 권력 이양상 코인이 주인 세션이면(=주식 휴장) 코인 스캔이 상시 돌 수 있음 →
    #    단, 주말 심층분석은 코인 주도 세션에 돌리되 '코인 작업 적체 시에만' 양보한다.
    #    (코인 주인 세션이라고 무조건 멈추면 진화가 영원히 못 돌기 때문)
    return False


def active_throttle_yield(
    *,
    max_wait_sec: float = THROTTLE_MAX_WAIT_SEC,
    poll_sec: float = THROTTLE_POLL_SEC,
    db_path: Optional[str] = None,
    sleeper=time.sleep,
) -> float:
    """
    코인 스캔이 메인 큐에 있으면 끝날 때까지 CPU 를 양보(sleep)한다.
    먼저 프로세스를 nice 19 로 낮추고, 코인 작업이 사라질 때까지 폴링한다.
    반환: 양보(대기)한 총 시간(초).
    """
    _lower_to_min_priority()
    waited = 0.0
    while coin_scan_active(db_path=db_path) and waited < max_wait_sec:
        sleeper(poll_sec)
        waited += poll_sec
    return waited


async def active_throttle_yield_async(
    *,
    max_wait_sec: float = THROTTLE_MAX_WAIT_SEC,
    poll_sec: float = THROTTLE_POLL_SEC,
    db_path: Optional[str] = None,
) -> float:
    """asyncio 버전 — 코인 스캔이 끝날 때까지 await asyncio.sleep 으로 양보."""
    _lower_to_min_priority()
    waited = 0.0
    while coin_scan_active(db_path=db_path) and waited < max_wait_sec:
        await asyncio.sleep(poll_sec)
        waited += poll_sec
    return waited


# ---------------------------------------------------------------------------
# 장기 기억 심층 탐색 (DTW over 10년+ 역사)
# ---------------------------------------------------------------------------
def deep_archive_dtw_search(
    payload: Dict[str, Any],
    *,
    db_path: Optional[str] = None,
    throttle: bool = True,
    exclude_episodes: Optional[List[str]] = None,
    hydrate: bool = False,
) -> Dict[str, Any]:
    """
    예약된 블랙스완 특징을 10년+ 전체 역사적 에피소드와 DTW 로 심층 비교한다.
    에피소드마다 능동형 스로틀(코인 양보)을 적용해 Zero-Collision 을 보장한다.
    exclude_episodes 가 주어지면(긴급 재탐색/Plan B) 해당 국면은 후보에서 제외한다.
    hydrate=True 면(주말) 콜드 스토리지에 과거 데이터가 없을 때 yfinance/pykrx/ccxt 로
    자율 수집한 뒤 실제 과거 궤적으로 DTW 한다(콜드 스타트 방어).
    반환: {best_episode, dtw_by_episode, favorable, archetype_dna(있으면)}.
    """
    import numpy as np

    import regime_analog_engine as rae

    excluded = {str(e) for e in (exclude_episodes or [])}
    if not excluded and isinstance(payload, dict):
        excluded = {str(e) for e in (payload.get("exclude_episodes") or [])}

    # 🌊 Mission 9 — 콜드 스토리지 어댑터(있으면 실제 과거 궤적, 없으면 하드코딩 시드)
    _arch = None
    hydration: Dict[str, Any] = {}
    try:
        import deep_archive_history as _arch  # type: ignore
    except Exception:
        _arch = None

    # 현재(블랙스완) 국면 인덱스 시퀀스 복원: payload 우선, 없으면 히스토리에서.
    cur_vec = payload.get("current_vector") if isinstance(payload, dict) else None
    cfg = _load_cfg(None)
    history = rae._load_vector_history(cfg)  # noqa: SLF001
    if isinstance(cur_vec, list) and len(cur_vec) == rae.N_DIMS:
        cur_series = rae._current_regime_index_series(history, cur_vec)  # noqa: SLF001
    else:
        cur_series = rae._current_regime_index_series(history, history[-1] if history else [0.0] * rae.N_DIMS)  # noqa: SLF001

    dtw_by_episode: Dict[str, float] = {}
    best_name: Optional[str] = None
    best_dist = float("inf")
    for name, ep in rae.HISTORICAL_EPISODES.items():
        if name in excluded:
            continue  # 실패한 정답 제외 → 두 번째로 닮은 과거(Plan B) 탐색
        if throttle:
            active_throttle_yield(db_path=db_path)  # 에피소드마다 코인 양보

        traj = np.asarray(ep["trajectory"], dtype=float)  # 기본: 하드코딩 시드 궤적
        if _arch is not None:
            try:
                if hydrate:
                    # 콜드 스타트 방어: 과거 데이터가 없으면 자율 수집(코인 양보 throttle_fn)
                    res = _arch.hydrate_episode(
                        name,
                        throttle_fn=(lambda: active_throttle_yield(db_path=db_path)) if throttle else None,
                    )
                    hydration[name] = res
                real = _arch.load_index_series("^GSPC")  # 실제 SPX 종가 궤적(z-정규화)
                if isinstance(real, list) and len(real) >= 5:
                    traj = np.asarray(real, dtype=float)
            except Exception:
                pass

        d = rae.dtw_distance(cur_series, traj)
        dtw_by_episode[name] = round(d, 4) if d == d and d != float("inf") else None
        if d < best_dist:
            best_dist = d
            best_name = name

    best_ep = rae.HISTORICAL_EPISODES.get(best_name or "", {})
    favorable = bool(best_ep.get("front_run_favorable", False))

    # 전설적 승자 DNA 아카이브 갱신(무거운 백테스트 빌더; 부재 시 시드)
    archetype = None
    try:
        built = rae.build_regime_archetype_dna(persist=True)
        store = built.get("store", {}) if isinstance(built, dict) else {}
        if best_name and isinstance(store, dict):
            archetype = store.get(best_name)
    except Exception:
        archetype = None

    return {
        "best_episode": best_name,
        "best_regime": best_ep.get("regime"),
        "best_dtw_dist": round(best_dist, 4) if best_dist != float("inf") else None,
        "front_run_favorable": favorable,
        "dtw_by_episode": dtw_by_episode,
        "archetype_dna": archetype,
        "excluded_episodes": sorted(excluded),
        "reason": payload.get("reason") if isinstance(payload, dict) else None,
        "hydration": hydration or None,
        "searched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# Mission 5: 단기-장기 상호 진화 폐루프
# ---------------------------------------------------------------------------
def merge_deep_result_into_working_memory(
    deep_result: Dict[str, Any],
    *,
    cfg: Optional[Dict[str, Any]] = None,
    markets: Tuple[str, ...] = ("KR", "US"),
    persist: bool = True,
) -> Dict[str, Any]:
    """
    주말 장기기억 결과(archetype)를 ① 신형 병렬 템플릿으로 자율 배포(Append-only) ② 단기
    캐시에 기록한다.

    [Mission 3 — 병렬 앙상블] 기존 Base 템플릿(RANK_A~D 등)은 절대 삭제·덮어쓰지 않는다.
    신형 DNA 는 `[🧬DEEP_EVOLVED_vN]` 독립 이름으로 병렬 추가되어, MAB/밴딧이 구형/신형을
    각각의 Arm 으로 보고 켈리 자본을 유동 분산한다.
    """
    import regime_analog_engine as rae

    cfg = _load_cfg(cfg)
    best_episode = deep_result.get("best_episode")
    favorable = bool(deep_result.get("front_run_favorable", False))
    archetype = deep_result.get("archetype_dna")

    out: Dict[str, Any] = {
        "best_episode": best_episode,
        "favorable": favorable,
        "deployed": [],
        "applied": False,
    }
    if not favorable or not isinstance(archetype, dict):
        out["reason"] = "no_favorable_archetype"
        # 단기 캐시에는 '관망' 기록만 남긴다.
        if persist:
            _set_cfg_value(
                WORKING_MEMORY_CACHE_KEY,
                {
                    "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "episode": best_episode,
                    "favorable": favorable,
                    "applied": False,
                },
            )
        return out

    # 🧬 Mission 3/7 — Base 불변, 신형 템플릿을 병렬(Append)로 자율 배포(태그 박제).
    try:
        from deep_evolution_deploy import auto_deploy

        deploy = auto_deploy(deep_result, cfg, markets=markets, persist=persist)
        out["auto_deploy"] = deploy
        out["deployed"] = deploy.get("deployed", [])
        out["applied"] = bool(deploy.get("applied"))
    except Exception as ex:
        out["auto_deploy_error"] = str(ex)

    # 단기 캐시 갱신(월요일 working memory 가 즉시 참조)
    cache = {
        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "episode": best_episode,
        "favorable": True,
        "archetype_dna": archetype,
        "deployed": out["deployed"],
        "applied": out["applied"],
    }

    if persist:
        _set_cfg_value(WORKING_MEMORY_CACHE_KEY, cache)
        # 국면 유사도도 재산출하여 월요일 게이트가 최신 best_episode 를 보게 한다.
        try:
            rae.compute_regime_analog(cfg)
        except Exception:
            pass
    out["working_memory_cache"] = cache
    return out


# ---------------------------------------------------------------------------
# Mission 6: 주말 심층 워커 (Priority 3 · 능동형 스로틀)
# ---------------------------------------------------------------------------
def _deep_executor(task) -> None:
    """task_orchestrator drain 용 실행기 — 능동형 스로틀 후 심층탐색(주말 하이드레이션) + 크로스 진화."""
    active_throttle_yield()  # 착수 전 코인 양보
    payload = task.payload if hasattr(task, "payload") else {}
    deep = deep_archive_dtw_search(payload, hydrate=True)
    merge_deep_result_into_working_memory(deep)


def run_deep_archive_worker(
    *,
    max_tasks: int = 20,
    db_path: Optional[str] = None,
    once: bool = True,
) -> Dict[str, Any]:
    """
    regime 전용 큐의 예약 작업을 Priority 3 으로 직렬 처리한다.
    task_orchestrator 부재 시 config 폴백 큐를 처리한다.
    """
    path = db_path or regime_queue_db_path()
    orch = _orch()
    if orch is not None:
        try:
            processed = orch.drain(
                _deep_executor,
                max_tasks=max_tasks,
                backoff_sec=600.0,
                db_path=path,
            )
            return {"engine": "task_orchestrator", "processed": processed, "queue": path}
        except Exception as ex:
            return {"engine": "task_orchestrator", "error": str(ex), "queue": path}

    # 폴백 큐 처리
    cfg = _load_cfg(None)
    pending = cfg.get("REGIME_DEEP_QUEUE_FALLBACK")
    if not isinstance(pending, list) or not pending:
        return {"engine": "fallback", "processed": 0}
    processed = 0
    for item in pending[:max_tasks]:
        try:
            deep = deep_archive_dtw_search(item if isinstance(item, dict) else {})
            merge_deep_result_into_working_memory(deep)
            processed += 1
        except Exception:
            continue
    _set_cfg_value("REGIME_DEEP_QUEUE_FALLBACK", [])
    return {"engine": "fallback", "processed": processed}


def regime_queue_stats(*, db_path: Optional[str] = None) -> Dict[str, Any]:
    orch = _orch()
    path = db_path or regime_queue_db_path()
    if orch is None:
        return {}
    try:
        return orch.backlog_stats(db_path=path)
    except Exception:
        return {}


if __name__ == "__main__":
    print("regime_memory: main_queue=", main_queue_db_path())
    print("regime_memory: regime_queue=", regime_queue_db_path())
    print("regime_memory: stats=", regime_queue_stats())

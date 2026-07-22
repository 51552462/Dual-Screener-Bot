"""
Bitget DB heartbeat watchdog — multi-unit restart matrix (ops survival).

Planes (independent miss counters / cooldowns / hourly caps — never cascade):
  1. factory   — ops_events heartbeat.tick stale → restart dante-bitget-factory
  2. queue     — worker HB stale AND PENDING|RUNNING>0 → restart queue-worker
  3. ws_plane  — factory HB fresh but embedded public/private WS hung →
                 restart factory (or dante-bitget-ws if BITGET_WATCHDOG_WS_UNIT=1)
  4. async     — async_telegram_daemon HB stale → restart dante-bitget-async
                 (alerts via sync urllib — never depends on the hung queue)

Invariants:
  - Never auto-flatten / never touch exchange positions
  - Never restart all units in one tick
  - Cap restarts/hour per unit (alert-only after budget exhausted)
  - Queue: no restart when idle (no work) — avoids false positives on inline mode
  - Async: HB must land in bitget_ops_events (bitget.async_telegram_daemon patch)

환경 변수:
  BITGET_OPS_EVENTS_DB — SQLite 경로 override
  BITGET_WATCHDOG_HEARTBEAT_COMPONENT — 감시 component (기본 bitget_auto_pilot)
      쉼표 구분 시 여러 component 중 가장 최신 heartbeat 사용
  BITGET_WATCHDOG_STALE_SEC — factory stale 임계(초), 기본 600
  BITGET_WATCHDOG_MISS_THRESHOLD — 연속 누락 횟수, 기본 3
  BITGET_WATCHDOG_ALERT_COOLDOWN_SEC — Telegram 쿨다운, 기본 900
  BITGET_WATCHDOG_STATE_DIR — 상태 파일 디렉터리
  BITGET_WATCHDOG_RESTART_CMD — factory 재시작 (기본 sudo -n systemctl restart dante-bitget-factory)
  BITGET_WATCHDOG_RESTART_QUEUE_CMD — queue-worker 재시작
  BITGET_WATCHDOG_RESTART_WS_CMD — dedicated WS unit 재시작
  BITGET_WATCHDOG_RESTART_ASYNC_CMD — dante-bitget-async 재시작
  BITGET_WATCHDOG_RESTART_QUEUE — queue auto-restart (기본 1)
  BITGET_WATCHDOG_RESTART_WS — WS-plane auto-restart (기본 1)
  BITGET_WATCHDOG_RESTART_ASYNC — async telegram auto-restart (기본 1)
  BITGET_WATCHDOG_WS_UNIT — 1이면 WS hang 시 dante-bitget-ws 재시작 (기본 0=factory)
  BITGET_WATCHDOG_ASYNC_COMPONENT — HB component (기본 async_telegram_daemon)
  BITGET_WATCHDOG_ASYNC_STALE_SEC — async HB stale 임계 (기본 180; HB period 60s)
  BITGET_WATCHDOG_ASYNC_PENDING_ALERT — backlog alert-only threshold (기본 50)
  BITGET_WATCHDOG_MAX_RESTARTS_PER_HOUR — 유닛당 시간당 상한 (기본 3)
  BITGET_WATCHDOG_WS_BUF_STALE_SEC — embedded WS buf_age restart 임계 (기본 300)
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from bitget.infra.clock import parse_utc_iso, utc_now, utc_now_iso
from bitget.infra.logging_setup import get_logger, log_exception, setup_logging

if TYPE_CHECKING:
    from datetime import datetime

DEFAULT_HEARTBEAT_COMPONENT = "bitget_auto_pilot"
ASYNC_HEARTBEAT_COMPONENT = "async_telegram_daemon"
UNIT_FACTORY = "factory"
UNIT_QUEUE = "queue_worker"
UNIT_WS = "ws"
UNIT_ASYNC = "async_telegram"
setup_logging(default_component="bitget.watchdog")
logger = get_logger("bitget.watchdog")


def _ops_db_path() -> str:
    for key in ("BITGET_OPS_EVENTS_DB", "DANTE_BITGET_OPS_EVENTS_DB"):
        v = (os.environ.get(key) or "").strip()
        if v:
            return v
    from bitget.infra.ops_logger import OPS_EVENTS_DB_PATH

    return OPS_EVENTS_DB_PATH


def _resolve_watchdog_components() -> tuple[str, ...]:
    """
    BITGET_WATCHDOG_HEARTBEAT_COMPONENT — single name or comma-separated list.
    Default: bitget_auto_pilot (pipeline daemon SSOT).
    """
    raw = (os.environ.get("BITGET_WATCHDOG_HEARTBEAT_COMPONENT") or DEFAULT_HEARTBEAT_COMPONENT).strip()
    parts = tuple(dict.fromkeys(p.strip() for p in raw.split(",") if p.strip()))
    return parts if parts else (DEFAULT_HEARTBEAT_COMPONENT,)


def _latest_heartbeat_ts_for_component(db_path: str, component: str) -> str | None:
    if not os.path.isfile(db_path):
        return None
    uri = f"file:{db_path.replace(os.sep, '/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=15.0, check_same_thread=False)
    try:
        conn.execute("PRAGMA query_only=ON;")
        comp = (component or "").strip()
        if comp:
            row = conn.execute(
                """
                SELECT ts_utc FROM ops_events
                WHERE event = 'heartbeat.tick' AND component = ?
                ORDER BY id DESC LIMIT 1
                """,
                (comp,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT ts_utc FROM ops_events
                WHERE event = 'heartbeat.tick'
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
        return str(row[0]) if row and row[0] else None
    finally:
        conn.close()


def _latest_heartbeat_ts(db_path: str, components: tuple[str, ...]) -> tuple[str | None, str | None]:
    """
    Return (newest_ts_utc, component_that_produced_it) across all watched components.
    """
    best_ts: str | None = None
    best_comp: str | None = None
    best_dt: datetime | None = None

    for comp in components:
        ts = _latest_heartbeat_ts_for_component(db_path, comp)
        if not ts:
            continue
        parsed = parse_utc_iso(ts)
        if parsed is None:
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_ts = ts
            best_comp = comp

    return best_ts, best_comp


def _send_bitget_telegram(text: str) -> bool:
    try:
        from bitget.env import bitget_telegram_chat_id, bitget_telegram_token
    except Exception:
        return False
    token = bitget_telegram_token()
    chat = bitget_telegram_chat_id()
    if not token or not chat:
        logger.warning("Bitget telegram credentials missing")
        return False
    body = json.dumps({"chat_id": chat, "text": text[:3500]}, ensure_ascii=False).encode("utf-8")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            return 200 <= int(resp.status) < 300
    except urllib.error.HTTPError as e:
        log_exception(logger, "Telegram HTTPError: %s", e)
        return False
    except Exception as e:
        log_exception(logger, "Telegram failed: %s", e)
        return False


def _state_dir() -> Path:
    from bitget.infra.data_paths import watchdog_state_dir

    return Path(watchdog_state_dir())


def _telegram_cooldown_elapsed(state_dir: Path, cooldown: float, *, name: str = "watchdog") -> bool:
    p = state_dir / f"last_{name}_alert_epoch.txt"
    state_dir.mkdir(parents=True, exist_ok=True)
    now = time.time()
    try:
        last = float(p.read_text(encoding="utf-8").strip())
    except Exception:
        last = 0.0
    return (now - last) >= cooldown


def _mark_telegram_alert_sent(state_dir: Path, *, name: str = "watchdog") -> None:
    p = state_dir / f"last_{name}_alert_epoch.txt"
    state_dir.mkdir(parents=True, exist_ok=True)
    p.write_text(str(time.time()), encoding="utf-8")


def _read_consecutive_misses(state_dir: Path) -> int:
    """Factory miss counter (legacy filename — keep for live state continuity)."""
    p = state_dir / "consecutive_misses.txt"
    try:
        return max(0, int(p.read_text(encoding="utf-8").strip()))
    except Exception:
        return 0


def _write_consecutive_misses(state_dir: Path, n: int) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "consecutive_misses.txt").write_text(str(max(0, int(n))), encoding="utf-8")


def _unit_miss_path(state_dir: Path, unit: str) -> Path:
    if unit == UNIT_FACTORY:
        return state_dir / "consecutive_misses.txt"
    return state_dir / f"misses_{unit}.txt"


def _read_unit_misses(state_dir: Path, unit: str) -> int:
    p = _unit_miss_path(state_dir, unit)
    try:
        return max(0, int(p.read_text(encoding="utf-8").strip()))
    except Exception:
        return 0


def _write_unit_misses(state_dir: Path, unit: str, n: int) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    _unit_miss_path(state_dir, unit).write_text(str(max(0, int(n))), encoding="utf-8")


def _max_restarts_per_hour() -> int:
    return max(1, _env_int("BITGET_WATCHDOG_MAX_RESTARTS_PER_HOUR", 3))


def _restart_log_path(state_dir: Path, unit: str) -> Path:
    return state_dir / f"restarts_{unit}.json"


def _load_restart_epochs(state_dir: Path, unit: str) -> list[float]:
    p = _restart_log_path(state_dir, unit)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        epochs = raw.get("epochs") if isinstance(raw, dict) else raw
        if not isinstance(epochs, list):
            return []
        return [float(x) for x in epochs if isinstance(x, (int, float))]
    except Exception:
        return []


def _save_restart_epochs(state_dir: Path, unit: str, epochs: list[float]) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    p = _restart_log_path(state_dir, unit)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps({"epochs": epochs[-32:]}, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(tmp, p)


def restart_budget_ok(state_dir: Path, unit: str, *, now: Optional[float] = None) -> bool:
    """True if unit has remaining restarts in the last hour."""
    now_ts = time.time() if now is None else float(now)
    window = 3600.0
    recent = [e for e in _load_restart_epochs(state_dir, unit) if now_ts - e < window]
    return len(recent) < _max_restarts_per_hour()


def record_unit_restart(state_dir: Path, unit: str, *, now: Optional[float] = None) -> None:
    now_ts = time.time() if now is None else float(now)
    epochs = [e for e in _load_restart_epochs(state_dir, unit) if now_ts - e < 7200.0]
    epochs.append(now_ts)
    _save_restart_epochs(state_dir, unit, epochs)


def unit_restart_cmd(unit: str) -> str:
    """Resolve systemctl restart command for a matrix unit (sudoers-aligned)."""
    if unit == UNIT_FACTORY:
        return (
            os.environ.get("BITGET_WATCHDOG_RESTART_CMD")
            or "sudo -n /usr/bin/systemctl restart dante-bitget-factory"
        ).strip()
    if unit == UNIT_QUEUE:
        return (
            os.environ.get("BITGET_WATCHDOG_RESTART_QUEUE_CMD")
            or "sudo -n /usr/bin/systemctl restart dante-bitget-queue-worker"
        ).strip()
    if unit == UNIT_WS:
        # Dedicated WS supervisor unit — only when BITGET_WATCHDOG_WS_UNIT=1
        return (
            os.environ.get("BITGET_WATCHDOG_RESTART_WS_CMD")
            or "sudo -n /usr/bin/systemctl restart dante-bitget-ws"
        ).strip()
    if unit == UNIT_ASYNC:
        return (
            os.environ.get("BITGET_WATCHDOG_RESTART_ASYNC_CMD")
            or "sudo -n /usr/bin/systemctl restart dante-bitget-async"
        ).strip()
    return ""


def _env_flag(key: str, default: bool = True) -> bool:
    raw = os.environ.get(key)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


def execute_unit_restart(
    state_dir: Path,
    *,
    unit: str,
    msg: str,
    cooldown: float,
    alert_name: Optional[str] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Alert (cooldown) + budget-gated restart for one unit. Never cascades."""
    alert = alert_name or unit
    result: dict[str, Any] = {
        "unit": unit,
        "alerted": False,
        "restarted": False,
        "budget_ok": True,
        "cmd": "",
        "rc": None,
    }
    if _telegram_cooldown_elapsed(state_dir, cooldown, name=alert):
        if _send_bitget_telegram(msg):
            _mark_telegram_alert_sent(state_dir, name=alert)
            result["alerted"] = True
    else:
        logger.info("%s telegram cooldown — restart path only", alert)

    if not restart_budget_ok(state_dir, unit):
        result["budget_ok"] = False
        logger.error(
            "restart budget exhausted unit=%s max_per_hour=%s — alert only, no restart",
            unit,
            _max_restarts_per_hour(),
        )
        return result

    cmd = unit_restart_cmd(unit)
    result["cmd"] = cmd
    if not cmd:
        return result
    if dry_run:
        result["restarted"] = True
        record_unit_restart(state_dir, unit)
        return result
    rc = os.system(cmd)
    result["rc"] = rc
    if rc != 0:
        logger.error("restart unit=%s returned %s cmd=%r", unit, rc, cmd)
    else:
        result["restarted"] = True
        record_unit_restart(state_dir, unit)
        logger.warning("restarted unit=%s cmd=%r", unit, cmd)
    return result


def _latest_heartbeat_row(
    db_path: str, components: tuple[str, ...]
) -> tuple[Optional[str], Optional[str], dict[str, Any]]:
    """Newest heartbeat across components → (ts, component, payload_dict)."""
    best_ts: str | None = None
    best_comp: str | None = None
    best_payload: dict[str, Any] = {}
    best_dt: datetime | None = None

    if not os.path.isfile(db_path):
        return None, None, {}

    uri = f"file:{db_path.replace(os.sep, '/')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=15.0, check_same_thread=False)
    try:
        conn.execute("PRAGMA query_only=ON;")
        for comp in components:
            row = conn.execute(
                """
                SELECT ts_utc, payload_json FROM ops_events
                WHERE event = 'heartbeat.tick' AND component = ?
                ORDER BY id DESC LIMIT 1
                """,
                (comp,),
            ).fetchone()
            if not row or not row[0]:
                continue
            ts = str(row[0])
            parsed = parse_utc_iso(ts)
            if parsed is None:
                continue
            if best_dt is None or parsed > best_dt:
                best_dt = parsed
                best_ts = ts
                best_comp = comp
                try:
                    best_payload = json.loads(row[1] or "{}") or {}
                    if not isinstance(best_payload, dict):
                        best_payload = {}
                except Exception:
                    best_payload = {}
    finally:
        conn.close()
    return best_ts, best_comp, best_payload


def evaluate_ws_plane_health(
    payload: dict[str, Any],
    *,
    buf_stale_sec: float,
) -> tuple[bool, str]:
    """
    True = unhealthy (restart candidate).
    Only when a plane reports enabled=True but is hung / not started.
    """
    for key in ("public_ws", "private_ws"):
        plane = payload.get(key)
        if not isinstance(plane, dict):
            continue
        if not bool(plane.get("enabled")):
            continue
        if "started" in plane and not bool(plane.get("started")):
            return True, f"{key} enabled but started=false"
        raw_age = plane.get("buf_age_sec")
        if raw_age is None:
            continue
        try:
            age = float(raw_age)
        except (TypeError, ValueError):
            continue
        if age >= float(buf_stale_sec):
            return True, f"{key} buf_age_sec={age:.1f} >= {buf_stale_sec:.0f}"
    return False, ""


# ===========================================================================
# Mission 3: 스캔 잡 서킷 브레이커 (Anti-Zombie Loop)
# ---------------------------------------------------------------------------
# [문제] 스캔이 락/타임아웃으로 죽으면 watchdog/cron 이 5분마다 무지성 재시도하여
#        5시간짜리 좀비 스캔과 알람 폭탄을 양산했다.
# [해결] 잡(mode)별 연속 실패 횟수를 기록하고, 임계치(기본 3회) 도달 시 회로를
#        OPEN 하여 해당 스캔 차수를 폐기(차단)한다. 일정 시간(reset) 경과 후에는
#        half-open 으로 1회 탐침을 허용해 영구 마비를 피한다.
# ===========================================================================
def _circuit_threshold() -> int:
    try:
        return max(1, int(os.environ.get("BITGET_SCAN_CB_THRESHOLD", "3") or 3))
    except ValueError:
        return 3


def _circuit_reset_sec() -> float:
    """
    [아키텍트 수술] 코인 24/7 환경에 맞춘 초단기 서킷 브레이커 복구
    스캔 에러로 회로가 차단되더라도, 주식처럼 1시간(3600초)을 버리지 않고
    코인의 빠른 캔들 갱신에 맞춰 5분(300초) 만에 탐침(Half-open)을 재개하여 데드존(Dead Zone)을 없앱니다.
    """
    try:
        return max(60.0, float(os.environ.get("BITGET_SCAN_CB_RESET_SEC", "300") or 300.0))
    except ValueError:
        return 300.0


def _circuit_state_path() -> Path:
    return _state_dir() / "scan_circuit_breaker.json"


def _load_circuit() -> dict:
    p = _circuit_state_path()
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _save_circuit(state: dict) -> None:
    p = _circuit_state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)


def is_circuit_open(job_key: str) -> tuple[bool, str]:
    """
    회로가 열려 있어(차단) 이번 실행을 건너뛰어야 하면 (True, 사유).
    OPEN 이지만 reset 시간이 지났으면 half-open 으로 간주하여 (False, 'half-open') 반환,
    1회 탐침을 허용한다(상태는 OPEN 유지 — 성공 시 record_job_success 가 CLOSED 로 닫음).
    """
    key = str(job_key or "").strip()
    if not key:
        return False, ""
    entry = _load_circuit().get(key)
    if not entry or entry.get("status") != "OPEN":
        return False, ""
    opened_at = parse_utc_iso(str(entry.get("opened_at") or "")) if entry.get("opened_at") else None
    if opened_at is not None:
        age = (utc_now() - opened_at).total_seconds()
        if age >= _circuit_reset_sec():
            return False, f"half-open (open {age:.0f}s ago, probing)"
    fails = int(entry.get("consecutive_failures", 0))
    return True, f"circuit OPEN — {fails} consecutive failures (last: {entry.get('last_error', '')[:120]})"


def record_job_failure(job_key: str, error: str = "") -> tuple[bool, str]:
    """
    잡 실패 1건 기록. 연속 실패가 임계치 이상이면 회로를 OPEN.
    반환: (회로_OPEN_여부, 상태문자열)
    """
    key = str(job_key or "").strip()
    if not key:
        return False, ""
    threshold = _circuit_threshold()
    state = _load_circuit()
    entry = state.get(key) or {"consecutive_failures": 0, "status": "CLOSED"}
    entry["consecutive_failures"] = int(entry.get("consecutive_failures", 0)) + 1
    entry["last_failure"] = utc_now_iso()
    entry["last_error"] = str(error)[:300]
    opened = False
    if entry["consecutive_failures"] >= threshold:
        if entry.get("status") != "OPEN":
            entry["opened_at"] = utc_now_iso()
        entry["status"] = "OPEN"
        opened = True
    else:
        entry["status"] = "CLOSED"
    state[key] = entry
    _save_circuit(state)
    label = "OPEN" if opened else "CLOSED"
    return opened, f"{key}: {entry['consecutive_failures']}/{threshold} → {label}"


def record_job_success(job_key: str) -> None:
    """잡 성공 시 회로를 닫고(CLOSED) 실패 카운터를 초기화한다."""
    key = str(job_key or "").strip()
    if not key:
        return
    state = _load_circuit()
    if key in state:
        state[key] = {"consecutive_failures": 0, "status": "CLOSED",
                      "last_success": utc_now_iso()}
        _save_circuit(state)


# ===========================================================================
# Option C/D: 큐 적체(Backlog) 알람 + 워커 하트비트(Heartbeat) 생존 감시
# ---------------------------------------------------------------------------
# [SPOF 방어] 단일 워커(queue_worker.py)가 조용히 죽거나, 큐에 작업이 쌓여도
#   처리되지 않으면 전체 코인 파이프라인이 정지한다. 5분마다 도는 이 워치독이
#   ① PENDING 적체(개수/대기시간) ② 워커 하트비트 staleness 를 능동 점검하여
#   임계 초과 시 텔레그램(Ops)으로 즉시 경보한다. (각 카테고리별 쿨다운 분리)
# ===========================================================================
def _queue_monitor_enabled() -> bool:
    v = (os.environ.get("BITGET_QUEUE_MONITOR_ENABLED", "1") or "1").strip().lower()
    return v not in ("0", "false", "no", "off", "")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)) or default)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, str(default)) or default)
    except ValueError:
        return default


def _monitor_queue_safety(state_dir: Path, cooldown: float, *, miss_threshold: int) -> None:
    """큐 적체 알람 + 워커 생존 → 독립 miss 카운터 후 queue-worker 재시작.

    워커가 한 번도 안 떴거나(하트비트 파일 없음) 큐가 비어있는 인라인 운영 단계에서는
    오탐을 내지 않도록 설계(=enqueue 가 시작돼 적체/가동이 실제로 생겼을 때만 경보/재시작).
    """
    if not _queue_monitor_enabled():
        return
    try:
        from bitget.infra.task_orchestrator import backlog_stats, worker_heartbeat_age_sec
    except Exception as e:  # noqa: BLE001
        log_exception(logger, "queue monitor import failed: %s", e)
        return

    pending_threshold = _env_int("BITGET_QUEUE_BACKLOG_PENDING_THRESHOLD", 3)
    age_threshold = _env_float("BITGET_QUEUE_BACKLOG_AGE_SEC", 900.0)  # 15분
    worker_stale = _env_float("BITGET_QUEUE_WORKER_STALE_SEC", 600.0)  # 10분

    try:
        bl = backlog_stats()
    except Exception as e:  # noqa: BLE001
        log_exception(logger, "backlog_stats failed: %s", e)
        return

    pending = int(bl.get("pending", 0))
    running = int(bl.get("running", 0))
    oldest = bl.get("oldest_pending_age_sec")

    # --- Mission 1: 적체 경보 (재시작 없음 — 적체≠워커사망) ---
    backlog_hit = pending >= pending_threshold or (oldest is not None and oldest >= age_threshold)
    if backlog_hit:
        oldest_txt = f"{oldest / 60:.1f}m" if oldest is not None else "n/a"
        msg = (
            "🚨 [CRITICAL: Queue Backlog Alert - 작업 적체 발생]\n"
            f"PENDING={pending} (threshold {pending_threshold})\n"
            f"RUNNING={running} · FAILED={int(bl.get('failed', 0))}\n"
            f"oldest PENDING wait: {oldest_txt} (threshold {age_threshold / 60:.0f}m)\n"
            "→ check: systemctl status dante-bitget-queue-worker"
        )
        logger.critical("%s", msg.replace("\n", " | "))
        if _telegram_cooldown_elapsed(state_dir, cooldown, name="queue_backlog"):
            if _send_bitget_telegram(msg):
                _mark_telegram_alert_sent(state_dir, name="queue_backlog")
        else:
            logger.info("queue_backlog telegram cooldown — log only")

    # --- Mission 2: 워커 하트비트 생존 → auto-restart (work 있을 때만) ---
    try:
        age = worker_heartbeat_age_sec()
    except Exception as e:  # noqa: BLE001
        log_exception(logger, "worker_heartbeat_age failed: %s", e)
        return

    has_work = pending > 0 or running > 0
    hung = age is not None and age >= worker_stale and has_work
    if not hung:
        if age is not None and age < worker_stale:
            _write_unit_misses(state_dir, UNIT_QUEUE, 0)
        return

    misses = _read_unit_misses(state_dir, UNIT_QUEUE) + 1
    _write_unit_misses(state_dir, UNIT_QUEUE, misses)
    logger.warning("queue_worker miss %s/%s age=%.1fs", misses, miss_threshold, float(age or 0))
    if misses < miss_threshold:
        return

    cmd = unit_restart_cmd(UNIT_QUEUE)
    msg = (
        "🚨 [CRITICAL: Queue Worker Dead/Hung - 워커 무응답]\n"
        f"last heartbeat: {float(age or 0) / 60:.1f}m ago (stale >= {worker_stale / 60:.0f}m)\n"
        f"queue waiting: PENDING={pending} RUNNING={running}\n"
        f"misses={misses}/{miss_threshold}\n"
        f"→ {cmd}"
    )
    logger.critical("%s", msg.replace("\n", " | "))
    if _env_flag("BITGET_WATCHDOG_RESTART_QUEUE", True):
        execute_unit_restart(
            state_dir,
            unit=UNIT_QUEUE,
            msg=msg,
            cooldown=cooldown,
            alert_name="queue_worker",
        )
    else:
        if _telegram_cooldown_elapsed(state_dir, cooldown, name="queue_worker"):
            if _send_bitget_telegram(msg):
                _mark_telegram_alert_sent(state_dir, name="queue_worker")
    _write_unit_misses(state_dir, UNIT_QUEUE, 0)


def _monitor_ws_plane(
    state_dir: Path,
    cooldown: float,
    *,
    db: str,
    components: tuple[str, ...],
    factory_stale_sec: float,
    miss_threshold: int,
) -> None:
    """Factory HB is fresh but embedded WS plane is hung → restart factory or WS unit.

    Does not fire when factory itself is stale (factory path owns that restart).
    Separate unit: set BITGET_WATCHDOG_WS_UNIT=1.
    """
    if not _env_flag("BITGET_WATCHDOG_RESTART_WS", True):
        return

    ts, matched_comp, payload = _latest_heartbeat_row(db, components)
    if ts is None:
        return
    parsed = parse_utc_iso(ts)
    if parsed is None:
        return
    factory_age = max(0.0, (utc_now() - parsed).total_seconds())
    if factory_age >= factory_stale_sec:
        # Factory plane owns recovery — do not double-restart for WS
        return

    buf_stale = _env_float("BITGET_WATCHDOG_WS_BUF_STALE_SEC", 300.0)
    unhealthy, reason = evaluate_ws_plane_health(payload, buf_stale_sec=buf_stale)
    if not unhealthy:
        _write_unit_misses(state_dir, UNIT_WS, 0)
        return

    misses = _read_unit_misses(state_dir, UNIT_WS) + 1
    _write_unit_misses(state_dir, UNIT_WS, misses)
    logger.warning(
        "ws_plane miss %s/%s reason=%s factory_age=%.1fs comp=%r",
        misses,
        miss_threshold,
        reason,
        factory_age,
        matched_comp,
    )
    if misses < miss_threshold:
        return

    use_ws_unit = _env_flag("BITGET_WATCHDOG_WS_UNIT", False)
    restart_unit = UNIT_WS if use_ws_unit else UNIT_FACTORY
    cmd = unit_restart_cmd(restart_unit)
    msg = (
        "🚨 [CRITICAL: WS Plane Hung - 웹소켓 평면 무응답]\n"
        f"reason: {reason}\n"
        f"factory_hb_age: {factory_age:.0f}s (fresh)\n"
        f"component={matched_comp}\n"
        f"misses={misses}/{miss_threshold}\n"
        f"target_unit={restart_unit}\n"
        f"→ {cmd}\n"
        "(no auto-flatten — ops restart only)"
    )
    logger.critical("%s", msg.replace("\n", " | "))
    execute_unit_restart(
        state_dir,
        unit=restart_unit,
        msg=msg,
        cooldown=cooldown,
        alert_name="ws_plane",
    )
    _write_unit_misses(state_dir, UNIT_WS, 0)


def _async_hb_component() -> str:
    raw = (os.environ.get("BITGET_WATCHDOG_ASYNC_COMPONENT") or ASYNC_HEARTBEAT_COMPONENT).strip()
    return raw or ASYNC_HEARTBEAT_COMPONENT


def _monitor_async_plane(
    state_dir: Path,
    cooldown: float,
    *,
    db: str,
    miss_threshold: int,
) -> None:
    """Plane 4: async_telegram_daemon HB stale → restart dante-bitget-async.

    Alerts use sync urllib (_send_bitget_telegram) so they never depend on the
    hung queue. Backlog with fresh HB is alert-only (no restart storm).
    """
    if not _env_flag("BITGET_WATCHDOG_RESTART_ASYNC", True):
        return

    comp = _async_hb_component()
    stale_sec = _env_float("BITGET_WATCHDOG_ASYNC_STALE_SEC", 180.0)
    pending_alert = _env_int("BITGET_WATCHDOG_ASYNC_PENDING_ALERT", 50)

    ts, matched, payload = _latest_heartbeat_row(db, (comp,))

    # Fresh HB + exploding queue → alert only (429 / drain lag), no restart
    if ts is not None:
        parsed = parse_utc_iso(ts)
        if parsed is not None:
            age = max(0.0, (utc_now() - parsed).total_seconds())
            if age < stale_sec:
                _write_unit_misses(state_dir, UNIT_ASYNC, 0)
                try:
                    pending = int(
                        payload.get("telegram_queue_pending")
                        or payload.get("telegram_queue_pending_sqlite")
                        or 0
                    )
                except (TypeError, ValueError):
                    pending = 0
                if pending >= pending_alert:
                    msg = (
                        "⚠️ [WARN: Async Telegram backlog]\n"
                        f"component={matched or comp}\n"
                        f"pending={pending} (alert >= {pending_alert})\n"
                        f"HB age={age:.0f}s (fresh)\n"
                        "→ check drain / 429; no auto-restart while HB alive"
                    )
                    logger.warning("%s", msg.replace("\n", " | "))
                    if _telegram_cooldown_elapsed(state_dir, cooldown, name="async_backlog"):
                        if _send_bitget_telegram(msg):
                            _mark_telegram_alert_sent(state_dir, name="async_backlog")
                return

    if ts is None:
        logger.warning("no async telegram heartbeat component=%r db=%r", comp, db)
        is_stale = True
        age = float("inf")
    else:
        parsed = parse_utc_iso(ts)
        if parsed is None:
            return
        age = max(0.0, (utc_now() - parsed).total_seconds())
        is_stale = age >= stale_sec

    if not is_stale:
        _write_unit_misses(state_dir, UNIT_ASYNC, 0)
        return

    misses = _read_unit_misses(state_dir, UNIT_ASYNC) + 1
    _write_unit_misses(state_dir, UNIT_ASYNC, misses)
    logger.warning(
        "async_telegram miss %s/%s age=%.1fs comp=%r",
        misses,
        miss_threshold,
        age if age != float("inf") else -1.0,
        matched or comp,
    )
    if misses < miss_threshold:
        return

    cmd = unit_restart_cmd(UNIT_ASYNC)
    age_txt = "never" if age == float("inf") else f"{age / 60:.1f}m"
    msg = (
        "🚨 [CRITICAL: Async Telegram Dead/Hung]\n"
        f"component={matched or comp}\n"
        f"last HB: {age_txt} ago (stale >= {stale_sec / 60:.0f}m)\n"
        f"misses={misses}/{miss_threshold}\n"
        f"→ {cmd}\n"
        "(sync alert path — no auto-flatten)"
    )
    logger.critical("%s", msg.replace("\n", " | "))
    execute_unit_restart(
        state_dir,
        unit=UNIT_ASYNC,
        msg=msg,
        cooldown=cooldown,
        alert_name="async_telegram",
    )
    _write_unit_misses(state_dir, UNIT_ASYNC, 0)


def main() -> int:
    stale_per_check = float(os.environ.get("BITGET_WATCHDOG_STALE_SEC", "600") or 600)
    miss_threshold = int(os.environ.get("BITGET_WATCHDOG_MISS_THRESHOLD", "3") or 3)
    cooldown = float(os.environ.get("BITGET_WATCHDOG_ALERT_COOLDOWN_SEC", "900") or 900)
    state_dir = _state_dir()
    components = _resolve_watchdog_components()
    db = _ops_db_path()

    # Plane 2: queue backlog + worker survival (independent of factory HB)
    try:
        _monitor_queue_safety(state_dir, cooldown, miss_threshold=miss_threshold)
    except Exception as e:  # noqa: BLE001
        log_exception(logger, "queue monitor error: %s", e)

    # Plane 3: WS hang while factory HB still fresh
    try:
        _monitor_ws_plane(
            state_dir,
            cooldown,
            db=db,
            components=components,
            factory_stale_sec=stale_per_check,
            miss_threshold=miss_threshold,
        )
    except Exception as e:  # noqa: BLE001
        log_exception(logger, "ws plane monitor error: %s", e)

    # Plane 4: async telegram consumer hang
    try:
        _monitor_async_plane(state_dir, cooldown, db=db, miss_threshold=miss_threshold)
    except Exception as e:  # noqa: BLE001
        log_exception(logger, "async plane monitor error: %s", e)

    # Plane 1: factory heartbeat
    ts, matched_comp = _latest_heartbeat_ts(db, components)
    label = ",".join(components) if len(components) > 1 else (components[0] if components else "(any)")

    if ts is None:
        logger.warning("no heartbeat for component(s)=%r db=%r", label, db)
        is_stale = True
        age = float("inf")
    else:
        parsed = parse_utc_iso(ts)
        if parsed is None:
            logger.error("ts parse failed: %r", ts)
            return 1
        age = max(0.0, (utc_now() - parsed).total_seconds())
        is_stale = age >= stale_per_check
        logger.info(
            "component=%r watched=%r age=%.1fs (stale if >= %.0fs) db=%s",
            matched_comp,
            label,
            age,
            stale_per_check,
            db,
        )

    if not is_stale:
        _write_consecutive_misses(state_dir, 0)
        return 0

    misses = _read_consecutive_misses(state_dir) + 1
    _write_consecutive_misses(state_dir, misses)
    logger.warning("miss count %s/%s", misses, miss_threshold)

    if misses < miss_threshold:
        return 0

    restart_cmd = unit_restart_cmd(UNIT_FACTORY)
    msg = (
        f"🚨 [BITGET WATCHDOG] heartbeat stale {misses} times (threshold {miss_threshold})\n"
        f"component={matched_comp or label}\n"
        f"watched={label}\n"
        f"DB: {db}\n"
        f"last ts: {ts}\n"
        f"→ {restart_cmd}\n"
        "(no auto-flatten — factory restart only)"
    )

    execute_unit_restart(
        state_dir,
        unit=UNIT_FACTORY,
        msg=msg,
        cooldown=cooldown,
        alert_name="watchdog",
    )
    _write_consecutive_misses(state_dir, 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

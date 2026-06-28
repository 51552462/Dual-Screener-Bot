"""
Bitget DB heartbeat watchdog.

`bitget_ops_events.sqlite`에서 지정 컴포넌트의 `heartbeat.tick` 최신 시각을 확인.
연속 누락 시 Telegram 알림 후 `dante-bitget-factory` 재시작(설정 가능).

환경 변수:
  BITGET_OPS_EVENTS_DB — SQLite 경로 override
  BITGET_WATCHDOG_HEARTBEAT_COMPONENT — 감시 component (기본 bitget_auto_pilot)
      쉼표 구분 시 여러 component 중 가장 최신 heartbeat 사용 (예: bitget_auto_pilot,bitget.main)
  BITGET_WATCHDOG_STALE_SEC — stale 임계(초), 기본 600
  BITGET_WATCHDOG_MISS_THRESHOLD — 연속 누락 횟수, 기본 3
  BITGET_WATCHDOG_ALERT_COOLDOWN_SEC — Telegram 쿨다운, 기본 900
  BITGET_WATCHDOG_STATE_DIR — 상태 파일 디렉터리
  BITGET_WATCHDOG_RESTART_CMD — 재시작 명령 (기본 sudo systemctl restart dante-bitget-factory)
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_HEARTBEAT_COMPONENT = "bitget_auto_pilot"


def _parse_ts_utc(s: str) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    try:
        t = s.strip()
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        d = datetime.fromisoformat(t)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception:
        return None


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
        parsed = _parse_ts_utc(ts)
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
        print("[bitget.watchdog] Bitget telegram credentials missing", file=sys.stderr)
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
        print(f"[bitget.watchdog] Telegram HTTPError: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[bitget.watchdog] Telegram failed: {e}", file=sys.stderr)
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
    p = state_dir / "consecutive_misses.txt"
    try:
        return max(0, int(p.read_text(encoding="utf-8").strip()))
    except Exception:
        return 0


def _write_consecutive_misses(state_dir: Path, n: int) -> None:
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "consecutive_misses.txt").write_text(str(max(0, int(n))), encoding="utf-8")


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
    """OPEN 유지 시간. 경과 후 half-open(1회 탐침 허용). 기본 3600s."""
    try:
        return max(60.0, float(os.environ.get("BITGET_SCAN_CB_RESET_SEC", "3600") or 3600))
    except ValueError:
        return 3600.0


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
    opened_at = _parse_ts_utc(str(entry.get("opened_at") or "")) if entry.get("opened_at") else None
    if opened_at is not None:
        age = (datetime.now(timezone.utc) - opened_at).total_seconds()
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
    entry["last_failure"] = datetime.now(timezone.utc).isoformat()
    entry["last_error"] = str(error)[:300]
    opened = False
    if entry["consecutive_failures"] >= threshold:
        if entry.get("status") != "OPEN":
            entry["opened_at"] = datetime.now(timezone.utc).isoformat()
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
                      "last_success": datetime.now(timezone.utc).isoformat()}
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


def _monitor_queue_safety(state_dir: Path, cooldown: float) -> None:
    """큐 적체 + 워커 생존을 점검하고 위험 수위면 텔레그램 경보.

    워커가 한 번도 안 떴거나(하트비트 파일 없음) 큐가 비어있는 인라인 운영 단계에서는
    오탐을 내지 않도록 설계되어 있다(=enqueue 가 시작돼 적체/가동이 실제로 생겼을 때만 경보).
    """
    if not _queue_monitor_enabled():
        return
    try:
        from bitget.infra.task_orchestrator import backlog_stats, worker_heartbeat_age_sec
    except Exception as e:  # noqa: BLE001
        print(f"[bitget.watchdog] queue monitor import failed: {e}", file=sys.stderr)
        return

    pending_threshold = _env_int("BITGET_QUEUE_BACKLOG_PENDING_THRESHOLD", 3)
    age_threshold = _env_float("BITGET_QUEUE_BACKLOG_AGE_SEC", 900.0)  # 15분
    worker_stale = _env_float("BITGET_QUEUE_WORKER_STALE_SEC", 600.0)  # 10분

    try:
        bl = backlog_stats()
    except Exception as e:  # noqa: BLE001
        print(f"[bitget.watchdog] backlog_stats failed: {e}", file=sys.stderr)
        return

    pending = int(bl.get("pending", 0))
    running = int(bl.get("running", 0))
    oldest = bl.get("oldest_pending_age_sec")

    # --- Mission 1: 적체 경보 ---
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
        print("[bitget.watchdog] " + msg.replace("\n", " | "))
        if _telegram_cooldown_elapsed(state_dir, cooldown, name="queue_backlog"):
            if _send_bitget_telegram(msg):
                _mark_telegram_alert_sent(state_dir, name="queue_backlog")
        else:
            print("[bitget.watchdog] queue_backlog telegram cooldown — log only")

    # --- Mission 2: 워커 하트비트 생존 판정 ---
    try:
        age = worker_heartbeat_age_sec()
    except Exception as e:  # noqa: BLE001
        print(f"[bitget.watchdog] worker_heartbeat_age failed: {e}", file=sys.stderr)
        return

    # age is None  → 워커 미가동(파일 없음) = 인라인 단계. 경보하지 않음.
    # 처리 대기/진행 중인 작업이 있을 때만(=워커가 일해야 하는데 무응답) 사망/멈춤으로 간주.
    has_work = pending > 0 or running > 0
    if age is not None and age >= worker_stale and has_work:
        msg = (
            "🚨 [CRITICAL: Queue Worker Dead/Hung - 워커 무응답]\n"
            f"last heartbeat: {age / 60:.1f}m ago (stale >= {worker_stale / 60:.0f}m)\n"
            f"queue waiting: PENDING={pending} RUNNING={running}\n"
            "worker: dante-bitget-queue-worker\n"
            "→ sudo systemctl restart dante-bitget-queue-worker"
        )
        print("[bitget.watchdog] " + msg.replace("\n", " | "))
        if _telegram_cooldown_elapsed(state_dir, cooldown, name="queue_worker"):
            if _send_bitget_telegram(msg):
                _mark_telegram_alert_sent(state_dir, name="queue_worker")
        else:
            print("[bitget.watchdog] queue_worker telegram cooldown — log only")


def main() -> int:
    stale_per_check = float(os.environ.get("BITGET_WATCHDOG_STALE_SEC", "600") or 600)
    miss_threshold = int(os.environ.get("BITGET_WATCHDOG_MISS_THRESHOLD", "3") or 3)
    cooldown = float(os.environ.get("BITGET_WATCHDOG_ALERT_COOLDOWN_SEC", "900") or 900)
    restart_cmd = (
        os.environ.get("BITGET_WATCHDOG_RESTART_CMD") or "sudo systemctl restart dante-bitget-factory"
    ).strip()
    state_dir = _state_dir()
    components = _resolve_watchdog_components()

    # Option C/D: 큐 적체 + 워커 생존 감시 (코어 하트비트 감시와 독립; 실패해도 무시).
    try:
        _monitor_queue_safety(state_dir, cooldown)
    except Exception as e:  # noqa: BLE001
        print(f"[bitget.watchdog] queue monitor error: {e}", file=sys.stderr)

    db = _ops_db_path()
    ts, matched_comp = _latest_heartbeat_ts(db, components)
    label = ",".join(components) if len(components) > 1 else (components[0] if components else "(any)")

    if ts is None:
        print(f"[bitget.watchdog] no heartbeat for component(s)={label!r} db={db!r}")
        is_stale = True
        age = float("inf")
    else:
        parsed = _parse_ts_utc(ts)
        if parsed is None:
            print(f"[bitget.watchdog] ts parse failed: {ts!r}", file=sys.stderr)
            return 1
        age = max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())
        is_stale = age >= stale_per_check
        print(
            f"[bitget.watchdog] component={matched_comp!r} watched={label!r} age={age:.1f}s "
            f"(stale if >= {stale_per_check:.0f}s) db={db}"
        )

    if not is_stale:
        _write_consecutive_misses(state_dir, 0)
        return 0

    misses = _read_consecutive_misses(state_dir) + 1
    _write_consecutive_misses(state_dir, misses)
    print(f"[bitget.watchdog] miss count {misses}/{miss_threshold}")

    if misses < miss_threshold:
        return 0

    msg = (
        f"🚨 [BITGET WATCHDOG] heartbeat stale {misses} times (threshold {miss_threshold})\n"
        f"component={matched_comp or label}\n"
        f"watched={label}\n"
        f"DB: {db}\n"
        f"last ts: {ts}\n"
        f"→ {restart_cmd}"
    )

    if _telegram_cooldown_elapsed(state_dir, cooldown):
        if _send_bitget_telegram(msg):
            _mark_telegram_alert_sent(state_dir)
    else:
        print("[bitget.watchdog] telegram cooldown — restart only")

    if restart_cmd:
        rc = os.system(restart_cmd)
        if rc != 0:
            print(f"[bitget.watchdog] restart returned {rc}", file=sys.stderr)

    _write_consecutive_misses(state_dir, 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

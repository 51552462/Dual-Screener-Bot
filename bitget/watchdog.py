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


def _telegram_cooldown_elapsed(state_dir: Path, cooldown: float) -> bool:
    p = state_dir / "last_watchdog_alert_epoch.txt"
    state_dir.mkdir(parents=True, exist_ok=True)
    now = time.time()
    try:
        last = float(p.read_text(encoding="utf-8").strip())
    except Exception:
        last = 0.0
    return (now - last) >= cooldown


def _mark_telegram_alert_sent(state_dir: Path) -> None:
    p = state_dir / "last_watchdog_alert_epoch.txt"
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


def main() -> int:
    stale_per_check = float(os.environ.get("BITGET_WATCHDOG_STALE_SEC", "600") or 600)
    miss_threshold = int(os.environ.get("BITGET_WATCHDOG_MISS_THRESHOLD", "3") or 3)
    cooldown = float(os.environ.get("BITGET_WATCHDOG_ALERT_COOLDOWN_SEC", "900") or 900)
    restart_cmd = (
        os.environ.get("BITGET_WATCHDOG_RESTART_CMD") or "sudo systemctl restart dante-bitget-factory"
    ).strip()
    state_dir = _state_dir()
    components = _resolve_watchdog_components()

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

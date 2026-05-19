"""
DB 기반 하트비트 워치독: `ops_events.sqlite`(레거리 명칭 `ops_health.sqlite` 동일 경로 가능)에서
지정 메인 컴포넌트의 `heartbeat.tick` 최신 시각을 본다.

연속으로 `WATCHDOG_MISS_THRESHOLD`회(기본 3) 관측 동안
`WATCHDOG_STALE_PER_CHECK_SEC`(기본 100초) 이상 갱신이 없으면 데드락 의심으로 보고
긴급 텔레그램 후 `sudo systemctl restart dante-factory` 실행.

cron / systemd timer 등 주기 실행(예: 1분)을 전제로 한다.

환경 변수:
  DANTE_OPS_EVENTS_DB / QUANT_OPS_EVENTS_DB — SQLite 경로
  WATCHDOG_HEARTBEAT_COMPONENT — 감시 대상 component (기본 system_auto_pilot, 빈 문자열이면 전체 최신 tick)
  WATCHDOG_STALE_PER_CHECK_SEC — 한 번의 관측에서 \"누락\"으로 칠 최소 경과(초), 기본 100
  WATCHDOG_MISS_THRESHOLD — 연속 누락 횟수, 기본 3
  WATCHDOG_ALERT_COOLDOWN_SEC — 텔레그램 중복 알림 최소 간격, 기본 900
  WATCHDOG_STATE_DIR — 기본 /var/lib/dante-watchdog
  TELEGRAM_TOKEN_MAIN, TELEGRAM_CHAT_ID
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
    for key in ("DANTE_OPS_EVENTS_DB", "QUANT_OPS_EVENTS_DB"):
        v = (os.environ.get(key) or "").strip()
        if v:
            return v
    from ops_logger import OPS_EVENTS_DB_PATH

    return OPS_EVENTS_DB_PATH


def _latest_heartbeat_ts(db_path: str, component: str) -> str | None:
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


def _send_urgent_telegram(text: str) -> bool:
    import telegram_env

    token = telegram_env.get_watchdog_token()
    chat = telegram_env.get_watchdog_chat_id()
    if not token or not chat:
        print("[watchdog] watchdog/main telegram 자격 증명 없음 — 알림 생략", file=sys.stderr)
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
        print(f"[watchdog] Telegram HTTPError: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[watchdog] Telegram 실패: {e}", file=sys.stderr)
        return False


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
    stale_per_check = float(os.environ.get("WATCHDOG_STALE_PER_CHECK_SEC", "100") or 100)
    miss_threshold = int(os.environ.get("WATCHDOG_MISS_THRESHOLD", "3") or 3)
    cooldown = float(os.environ.get("WATCHDOG_ALERT_COOLDOWN_SEC", "900") or 900)
    state_dir = Path(os.environ.get("WATCHDOG_STATE_DIR", "/var/lib/dante-watchdog"))
    comp = (os.environ.get("WATCHDOG_HEARTBEAT_COMPONENT", "system_auto_pilot") or "").strip()

    db = _ops_db_path()
    ts = _latest_heartbeat_ts(db, comp)
    label = comp or "(any)"

    if ts is None:
        print(f"[watchdog] component={label!r} 에 대한 heartbeat 없음 또는 DB 없음 db={db!r}")
        is_stale = True
        age = float("inf")
    else:
        parsed = _parse_ts_utc(ts)
        if parsed is None:
            print(f"[watchdog] ts 파싱 실패: {ts!r}", file=sys.stderr)
            return 1
        age = max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())
        is_stale = age >= stale_per_check
        print(f"[watchdog] component={label!r} age={age:.1f}s (stale if >= {stale_per_check:.0f}s) db={db}")

    if not is_stale:
        _write_consecutive_misses(state_dir, 0)
        return 0

    misses = _read_consecutive_misses(state_dir) + 1
    _write_consecutive_misses(state_dir, misses)
    print(f"[watchdog] 누락 카운트 {misses}/{miss_threshold} (연속)")

    if misses < miss_threshold:
        return 0

    msg = (
        f"🚨 [DANTE WATCHDOG] 메인 하트비트 연속 {misses}회 무갱신(임계 {miss_threshold}회)\n"
        f"component={label}\n"
        f"DB: {db}\n"
        f"마지막 ts: {ts}\n"
        "→ sudo systemctl restart dante-factory 실행"
    )

    if _telegram_cooldown_elapsed(state_dir, cooldown):
        if _send_urgent_telegram(msg):
            _mark_telegram_alert_sent(state_dir)
    else:
        print("[watchdog] 텔레그램 쿨다운 — 재시작만 수행")

    rc = os.system("sudo systemctl restart dante-factory")
    if rc != 0:
        print(f"[watchdog] sudo systemctl restart dante-factory 반환값 {rc}", file=sys.stderr)

    _write_consecutive_misses(state_dir, 0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

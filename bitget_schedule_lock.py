import json
import os
import threading
import time


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_PATH = os.path.join(BASE_DIR, "bitget_schedule_lock_state.json")
_LOCK = threading.Lock()


def _load_state():
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state):
    tmp = f"{STATE_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


def acquire(task_key, min_interval_sec=60):
    now = int(time.time())
    with _LOCK:
        state = _load_state()
        last = int(state.get(task_key, 0) or 0)
        if now - last < int(min_interval_sec):
            return False
        state[task_key] = now
        _save_state(state)
        return True

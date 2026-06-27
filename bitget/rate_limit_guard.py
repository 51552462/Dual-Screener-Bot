import random
import threading
import time


_LOCK = threading.Lock()
_LAST_CALL = {}


def throttle(key="global", min_interval_sec=0.2):
    wait_s = 0.0
    with _LOCK:
        now = time.time()
        last = float(_LAST_CALL.get(key, 0.0) or 0.0)
        elapsed = now - last
        if elapsed < min_interval_sec:
            wait_s = min_interval_sec - elapsed
        _LAST_CALL[key] = now + wait_s
    if wait_s > 0:
        time.sleep(wait_s + random.uniform(0.01, 0.05))


def backoff_sleep(attempt, base=0.35, cap=8.0):
    exp = min(cap, base * (2 ** max(0, int(attempt))))
    time.sleep(exp + random.uniform(0.01, 0.08))

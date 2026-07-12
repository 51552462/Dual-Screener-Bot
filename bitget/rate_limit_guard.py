import random
import threading
import time

import memory_bounds

from bitget.infra.memory_policy import RATE_LIMIT_KEY_TTL_SEC, RATE_LIMIT_MAX_KEYS

_LOCK = threading.Lock()
_LAST_CALL = {}
_STALE_KEYS_BUF: list[str] = []


def _prune_last_call(now: float) -> None:
    stale = _STALE_KEYS_BUF
    stale.clear()
    for k, ts in _LAST_CALL.items():
        if (now - float(ts or 0.0)) > RATE_LIMIT_KEY_TTL_SEC:
            stale.append(k)
    for k in stale:
        _LAST_CALL.pop(k, None)
    memory_bounds.evict_oldest_dict_keys(
        _LAST_CALL,
        RATE_LIMIT_MAX_KEYS,
        ts_getter=lambda key: _LAST_CALL.get(key, 0.0),
    )


def throttle(key="global", min_interval_sec=0.2):
    wait_s = 0.0
    with _LOCK:
        now = time.time()
        if len(_LAST_CALL) > RATE_LIMIT_MAX_KEYS or len(_LAST_CALL) % 32 == 0:
            _prune_last_call(now)
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

"""supernova_hunter — scan concurrency + bounded table list SSOT."""
from __future__ import annotations

import inspect

from bitget import supernova_hunter as sn
from bitget.infra.memory_policy import SUPERNOVA_SCAN_MAX_WORKERS


def test_supernova_scan_uses_memory_policy_worker_cap():
    src = inspect.getsource(sn.execute_supernova_live_scan)
    assert "SUPERNOVA_SCAN_MAX_WORKERS" in src
    assert "max_workers=15" not in src
    assert "finally:" in src
    assert "cconn.close()" in src


def test_supernova_scan_max_workers_is_conservative_for_4gb():
    assert SUPERNOVA_SCAN_MAX_WORKERS <= 6
    assert SUPERNOVA_SCAN_MAX_WORKERS >= 1

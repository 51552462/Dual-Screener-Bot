"""bitget.scripts.institutional_db_backup — Clock SSOT."""
from __future__ import annotations


def test_institutional_db_backup_module_uses_clock_ssot():
    import inspect

    from bitget.scripts import institutional_db_backup as idb

    src = inspect.getsource(idb)
    assert "datetime.now(" not in src
    assert "datetime.utcnow()" not in src
    assert "utc_compact_key" in src
    assert "utc_datetime_str" in src
    assert "utc_now_iso" in src

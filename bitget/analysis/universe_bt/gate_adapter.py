"""Dry try_add against isolated scratch forward DB (paper path never patched in)."""
from __future__ import annotations

import os
from contextlib import contextmanager, ExitStack
from typing import Any, Iterator, Optional, Tuple
from unittest import mock

from bitget.analysis.universe_bt.paths import universe_bt_scratch_forward_path


@contextmanager
def isolated_forward_scratch(scratch_path: Optional[str] = None) -> Iterator[str]:
    """Patch ledger/shared/shadow DB_PATH → scratch; no-op config writes / telegram."""
    path = scratch_path or universe_bt_scratch_forward_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    import bitget.forward.ledger as ledger
    import bitget.forward.shared as shared
    import bitget.shadow_tracking as shadow

    from bitget.forward.shared import _init_forward_db_schema
    from bitget.infra.shared_db_connector import get_connection

    # Fresh scratch per context — avoid quota pollution across runs when file reused
    if os.path.isfile(path):
        try:
            os.remove(path)
        except OSError:
            pass

    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)
        conn.commit()
    finally:
        conn.close()

    def _init_noop() -> None:
        return None

    def _save_noop(_cfg: Any) -> None:
        return None

    def _tg_noop(*_a: Any, **_k: Any) -> None:
        return None

    with ExitStack() as stack:
        stack.enter_context(mock.patch.object(shared, "DB_PATH", path))
        stack.enter_context(mock.patch.object(ledger, "DB_PATH", path))
        stack.enter_context(mock.patch.object(shadow, "DB_PATH", path))
        stack.enter_context(mock.patch.object(ledger, "init_forward_db", _init_noop))
        stack.enter_context(mock.patch.object(shared, "init_forward_db", _init_noop))
        stack.enter_context(mock.patch.object(shared, "save_system_config", _save_noop))
        stack.enter_context(mock.patch.object(ledger, "save_system_config", _save_noop))
        stack.enter_context(mock.patch.object(shared, "send_telegram_msg", _tg_noop))
        yield path


def dry_try_add_virtual_position(
    *,
    market_type: str,
    symbol: str,
    timeframe: str,
    sig_type: str,
    score: float,
    entry_price: float,
    facts: dict,
    side: str = "LONG",
    entry_high: float = 0.0,
    scratch_path: Optional[str] = None,
) -> Tuple[bool, str]:
    """Call original try_add_virtual_position with write redirected to scratch only."""
    from bitget.forward.ledger import try_add_virtual_position

    with isolated_forward_scratch(scratch_path):
        ok, msg = try_add_virtual_position(
            market_type=market_type,
            symbol=symbol,
            timeframe=timeframe,
            sig_type=sig_type,
            score=score,
            entry_price=entry_price,
            facts=facts or {},
            side=side,
            entry_high=entry_high,
        )
        return bool(ok), str(msg or "")

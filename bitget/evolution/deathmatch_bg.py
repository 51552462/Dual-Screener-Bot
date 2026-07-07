"""
Bitget-native deathmatch — 주식 SSOT DB/meta 오염 없이 persist·registry 로드.

``evolution.deathmatch_battle_royale`` 알고리즘은 그대로 쓰되, 실행 구간만
``bitget.infra.data_paths.market_data_db_path()`` 로 격리한다.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Optional, Tuple

from bitget.infra.data_paths import market_data_db_path
from bitget.infra.market_keys import to_deathmatch_key


@contextmanager
def bitget_deathmatch_ssot():
    """Registry·deathmatch_store 가 Bitget DB만 보도록 일시 패치."""
    import evolution.deathmatch_store as dms
    import strategy_registry_store as srs

    bg_db = market_data_db_path()
    orig_load = srs.load_registry_rows
    orig_save = dms.save_battle_royal_result
    orig_log = dms.log_elimination_events
    orig_db = dms._db_path

    def _load(db_path: Optional[str] = None):
        return orig_load(bg_db if db_path is None else db_path)

    def _save(*args, db_path: Optional[str] = None, **kwargs):
        return orig_save(*args, db_path=bg_db if db_path is None else db_path, **kwargs)

    def _log(market: str, events, db_path: Optional[str] = None, **kwargs):
        return orig_log(
            market, events, db_path=bg_db if db_path is None else db_path, **kwargs
        )

    def _path():
        return bg_db

    srs.load_registry_rows = _load  # type: ignore[method-assign]
    dms.save_battle_royal_result = _save  # type: ignore[method-assign]
    dms.log_elimination_events = _log  # type: ignore[method-assign]
    dms._db_path = _path  # type: ignore[method-assign]
    try:
        yield bg_db
    finally:
        srs.load_registry_rows = orig_load  # type: ignore[method-assign]
        dms.save_battle_royal_result = orig_save  # type: ignore[method-assign]
        dms.log_elimination_events = orig_log  # type: ignore[method-assign]
        dms._db_path = orig_db  # type: ignore[method-assign]


def run_bitget_battle_royal(
    df_closed,
    sys_config: Optional[dict] = None,
    *,
    market_type: str,
    lookback_days: Optional[int] = None,
    window_pre_sliced: bool = False,
    meta_health: Optional[dict[str, Any]] = None,
    persist: bool = True,
):
    from evolution.deathmatch_battle_royale import run_battle_royal

    mk = to_deathmatch_key(market_type)
    with bitget_deathmatch_ssot():
        return run_battle_royal(
            df_closed,
            sys_config,
            market=mk,
            lookback_days=lookback_days,
            window_pre_sliced=window_pre_sliced,
            meta_health=meta_health,
            persist=persist,
        )


def build_bitget_nway_deathmatch_registry(
    df_closed,
    sys_config: Optional[dict] = None,
    *,
    market_type: str,
    lookback_days: Optional[int] = None,
    window_pre_sliced: bool = False,
    meta_health: Optional[dict[str, Any]] = None,
    persist: bool = True,
) -> Tuple[Any, Any]:
    from evolution.deathmatch_battle_royale import battle_royal_to_nway

    br = run_bitget_battle_royal(
        df_closed,
        sys_config,
        market_type=market_type,
        lookback_days=lookback_days,
        window_pre_sliced=window_pre_sliced,
        meta_health=meta_health,
        persist=persist,
    )
    return br, battle_royal_to_nway(br)

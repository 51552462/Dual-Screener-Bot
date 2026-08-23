"""VPS one-shot: OHLCV / universe coverage for UNIVERSE-BT (read-only)."""
from __future__ import annotations

import os
import sqlite3
import sys

os.environ.setdefault("BITGET_DB_STORAGE_PATH", "/var/lib/quant-bitget/data")
os.environ.setdefault("BITGET_UNIVERSE_BT_OHLCV_ONLY", "1")

ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def main() -> int:
    from bitget.infra.data_paths import market_data_db_path, market_db_read_path
    from bitget.analysis.universe_bt.universe import (
        list_ohlcv_symbols,
        resolve_universe_snapshot,
    )

    md = market_data_db_path()
    rd = market_db_read_path()
    market = rd if os.path.isfile(rd) else md
    print("market", market, "exists", os.path.isfile(market), "size_mb", round(os.path.getsize(market) / 1e6, 1) if os.path.isfile(market) else 0)
    if not os.path.isfile(market):
        return 2

    conn = sqlite3.connect(f"file:{market}?mode=ro", uri=True)
    try:
        spot = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_SPOT_%_1D'"
        ).fetchone()[0]
        fut = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_FUT_%_1D'"
        ).fetchone()[0]
        other = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_%' AND name NOT LIKE 'BITGET_SPOT_%' AND name NOT LIKE 'BITGET_FUT_%'"
        ).fetchone()[0]
        print(f"SPOT_1D={spot} FUT_1D={fut} other_BITGET_tables={other}")
        for like in (
            "BITGET_SPOT_%_1D",
            "BITGET_FUT_%_1D",
            "BITGET_%_1D",
            "BITGET_LINEAR_%",
            "BITGET_SWAP_%",
        ):
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ? ORDER BY name LIMIT 12",
                (like,),
            ).fetchall()
            print(f"sample {like}: {[r[0] for r in rows]}")
        # timeframe distribution for FUT-like
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'BITGET_FUT_%' ORDER BY name LIMIT 30"
        ).fetchall()
        print("any BITGET_FUT_ samples:", [r[0] for r in rows])
    finally:
        conn.close()

    for mt in ("spot", "futures"):
        ohlcv = list_ohlcv_symbols(mt, db_path=market)
        univ = resolve_universe_snapshot(mt, db_path=market)
        print(f"{mt}: ohlcv={len(ohlcv)} universe={len(univ)} sample={univ[:8]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
WebSocket supervisor — runs public + private WS concurrently (systemd: dante-bitget-ws).
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys


async def _run_all(symbols: tuple[str, ...], *, skip_private: bool) -> None:
    from bitget.data.ws_public import run_public_ws_supervisor

    tasks = [asyncio.create_task(run_public_ws_supervisor(symbols=symbols), name="bitget_ws_public")]

    if not skip_private:
        from bitget.data.ws_private import has_private_credentials, run_private_ws_supervisor

        if has_private_credentials():
            tasks.append(
                asyncio.create_task(run_private_ws_supervisor(), name="bitget_ws_private")
            )
        else:
            print("[ws_supervisor] private WS skipped (no API credentials)", file=sys.stderr)

    await asyncio.gather(*tasks)


def main(argv: list[str] | None = None) -> int:
    from bitget.infra.logging_setup import setup_logging

    setup_logging(default_component="bitget.ws_supervisor")

    parser = argparse.ArgumentParser(description="Bitget WebSocket supervisor (public + optional private)")
    parser.add_argument(
        "--symbols",
        default=os.environ.get("BITGET_WS_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT"),
        help="Comma-separated instId list (public ticker + orderbook)",
    )
    parser.add_argument(
        "--public-only",
        action="store_true",
        help="Skip private WS even when credentials exist",
    )
    args = parser.parse_args(argv)
    symbols = tuple(s.strip().upper() for s in args.symbols.split(",") if s.strip())
    if not symbols:
        print("no symbols", file=sys.stderr)
        return 2

    try:
        asyncio.run(_run_all(symbols, skip_private=args.public_only))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

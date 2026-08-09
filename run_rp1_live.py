#!/usr/bin/env python3
"""
RP-1 live run entrypoint (server).

Defaults: parallel chunked pool (workers=2), metrics v2, progress logs.

Env quick reference:
  RP1_FAST=1              KR50+US50 smoke (~1h)
  RP1_KR_LIMIT / RP1_US_LIMIT   universe caps (default 200 each)
  RP1_MAX_WORKERS=2       parallel workers (1-8)
  RP1_CHUNK_SIZE=25       futures per batch
  RP1_PROGRESS_EVERY=25   ticker progress log interval
  RP1_MATRIX=0          disable OHLCV 1-fetch-per-ticker cache (legacy slow path)
  RP1_MATRIX_REUSE=1    save/load matrix snapshot (smoke reruns ~minutes not ~1h)
  RP1_SKIP_STAGE2=1     skip C-1 placeholder stage2 (faster full run)
  RP1_SEQUENTIAL=1        force slow single-process (debug only)

Example:
  nohup python3 run_rp1_live.py > rp1_run.log 2>&1 &
  tail -f rp1_run.log
"""
from __future__ import annotations

import os

from regime_panel_rp1 import run_regime_panel_rp1
from regime_panel_rp1_runner import build_rp1_universe, log_rp1, resolve_rp1_max_workers, resolve_rp1_use_parallel


def _skip_stage2() -> bool:
    return os.environ.get("RP1_SKIP_STAGE2", "").strip().lower() in ("1", "true", "yes")


def main() -> None:
    mode = "parallel_chunked" if resolve_rp1_use_parallel() else "sequential"
    log_rp1(f"[RP-1] start mode={mode} workers={resolve_rp1_max_workers()}")
    universe = build_rp1_universe()
    report = run_regime_panel_rp1(universe, run_stage2=not _skip_stage2())
    s1 = report["stage1"]
    log_rp1(f"FINAL -> {report.get('output_path')}")
    log_rp1(f"overall_verdict: {s1.get('overall_verdict')}")
    log_rp1(f"schema: {s1.get('schema')} metrics: {s1.get('metrics_method')}")


if __name__ == "__main__":
    main()

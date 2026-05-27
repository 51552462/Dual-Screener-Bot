#!/usr/bin/env bash
# Factory CLI tail patch for legacy system_auto_pilot.py (no 2121-line paste needed).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${ROOT}/system_auto_pilot.py"
if [[ ! -f "$TARGET" ]]; then
  echo "Missing $TARGET" >&2
  exit 1
fi
if grep -q 'def run_factory_cli' "$TARGET"; then
  echo "Already has run_factory_cli — skip"
  exit 0
fi
cp -a "$TARGET" "${TARGET}.bak.$(date +%Y%m%d_%H%M%S)"
export TARGET
python3 << 'PY'
import os
from pathlib import Path

p = Path(os.environ["TARGET"])
text = p.read_text(encoding="utf-8")
marker = "if __name__ == \"__main__\":"
idx = text.rfind(marker)
if idx < 0:
    raise SystemExit("Could not find __main__ block to replace")
text = text[:idx].rstrip() + "\n\n"
tail = '''
def run_factory_cli(argv=None) -> int:
    """Ubuntu cron / factory.sh 단일 진입점."""
    import argparse

    from factory_pipelines import get_pipeline
    from factory_runtime import (
        FACTORY_MODES,
        dispatch_factory_mode,
        factory_exit_code,
    )

    parser = argparse.ArgumentParser(
        description="Dual-Screener Factory scheduler (unified entrypoint)",
    )
    parser.add_argument(
        "--mode",
        choices=sorted(FACTORY_MODES),
        help="Job pipeline to run once and exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List steps only; no lock, no side effects",
    )
    parser.add_argument(
        "--skip-telegram",
        action="store_true",
        help="Suppress factory PARTIAL_FAIL / lock notifications",
    )
    parser.add_argument(
        "--lock-timeout",
        type=float,
        default=120.0,
        help="Seconds to wait for factory_runtime.lock (default 120)",
    )
    parser.add_argument(
        "--run-autonomous-analysis-only",
        action="store_true",
        help="Legacy: weekend MetaGovernor brain surgery only",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="24h system_main_loop (do not use from cron)",
    )
    args = parser.parse_args(argv)

    if args.run_autonomous_analysis_only:
        run_autonomous_analysis()
        return 0

    if args.daemon or (args.mode is None and not args.run_autonomous_analysis_only):
        if args.mode is not None:
            parser.error("Use either --mode or --daemon, not both")
        system_main_loop()
        return 0

    if args.mode is None:
        parser.error("Specify --mode <name> or --daemon")

    pipeline = get_pipeline(args.mode)
    print(f"🏭 [Factory] mode={args.mode} steps={[s.name for s in pipeline]}")
    report = dispatch_factory_mode(
        args.mode,
        pipeline,
        send_fn=send_telegram_report,
        skip_telegram=args.skip_telegram,
        dry_run=args.dry_run,
        lock_timeout_sec=args.lock_timeout,
    )
    code = factory_exit_code(report)
    print(f"🏭 [Factory] finished status={report.status_label} exit={code}")
    return code


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--run-autonomous-analysis-only":
            run_autonomous_analysis()
            raise SystemExit(0)
        raise SystemExit(run_factory_cli())
    system_main_loop()
'''
p.write_text(text + tail.lstrip("\n"), encoding="utf-8")
print("Patched", p)
PY
python3 -m py_compile "$TARGET"
echo "OK: $TARGET"

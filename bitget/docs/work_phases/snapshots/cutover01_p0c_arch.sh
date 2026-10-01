#!/bin/bash
# CAT-L-CUTOVER-01 Phase 0c — Bot-2 read-only architecture dump. No env write, no start-parallel.
set -u
export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT" || exit 1
OUT=/tmp/cutover01_p0c_arch.out
exec > >(tee "$OUT") 2>&1
echo "=== TS $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "HEAD=$(git log -1 --format='%h %ad %s' --date=iso)"
echo "=== architecture_checks.py mtime ==="
ls -l --time-style=long-iso bitget/validation/architecture_checks.py | awk '{print $6,$7,$8}'
export PYTHONPATH="${INSTALL_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
PY="${INSTALL_ROOT}/venv/bin/python"
[ -x "$PY" ] || PY=python3
set +u
set +a
[ -f .env ] && set -a && . ./.env && set +a
[ -f bitget/.env ] && set -a && . ./bitget/.env && set +a
set -u
"$PY" -c 'import json; from bitget.validation.architecture_checks import run_architecture_checks; print(json.dumps(run_architecture_checks(), ensure_ascii=False, indent=2, default=str))'
echo "JSON_DUMP_RC=$?"
echo "WROTE $OUT"

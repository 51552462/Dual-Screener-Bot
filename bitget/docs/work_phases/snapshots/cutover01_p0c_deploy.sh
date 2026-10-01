#!/bin/bash
# CAT-L-CUTOVER-01 Phase 0c deploy — git pull --ff-only + architecture dump. No installer, no update_bitget.sh, no env write.
set -u
export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT" || exit 1
OUT=/tmp/cutover01_p0c_deploy.out
exec > >(tee "$OUT") 2>&1
echo "=== TS $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "BEFORE: $(git log -1 --format='%h %ad %s' --date=iso)"
git fetch --quiet
git pull --ff-only
echo "PULL_RC=$?"
AFTER="$(git log -1 --format='%h' )"
echo "AFTER: $(git log -1 --format='%h %ad %s' --date=iso)"
if [ "$AFTER" != "8a6da21" ]; then
  echo "HASH_MISMATCH expected=8a6da21 got=$AFTER — STOP"
  exit 9
fi
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

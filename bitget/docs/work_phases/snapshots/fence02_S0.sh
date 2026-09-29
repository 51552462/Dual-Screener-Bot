#!/bin/bash
# CAT-L-FENCE-02 · S0 — read-only generator vs live cron.d
bash <<'S0' 2>&1 | tee /tmp/fence02_S0.out
# CAT-L-FENCE-02 · S0/S2 (Claude 확인본) — 읽기전용. 설치기/update_bitget.sh 실행 아님.
set -u
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"; export INSTALL_ROOT
LIVE=/etc/cron.d/dual-screener-bitget
PY="$INSTALL_ROOT/venv/bin/python"; [ -x "$PY" ] || PY=python3
cd "$INSTALL_ROOT" || { echo "S2_EMPTY_DIFF=no (INSTALL_ROOT 없음)"; exit 1; }
export PYTHONPATH="$INSTALL_ROOT${PYTHONPATH:+:$PYTHONPATH}"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
if ! "$PY" - >"$T/gen.raw" <<'PY'
import os
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
root = Path(os.environ["INSTALL_ROOT"])
spec = spec_from_file_location("gen_cron", root / "bitget" / "deploy" / "generate_bitget_crontab.py")
mod = module_from_spec(spec); spec.loader.exec_module(mod)
print(mod.render_bitget_crontab(str(root)), end="")
PY
then echo "S2_EMPTY_DIFF=no (generator 실행 실패)"; exit 1; fi
[ -r "$LIVE" ] || { echo "S2_EMPTY_DIFF=no (live 읽기 불가)"; exit 1; }
body() { grep -vE '^[[:space:]]*(#|$)' "$1" | sed 's/[[:space:]]*$//'; }
envs() { grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$1"; }
jobs() { grep -vE '^[A-Za-z_][A-Za-z0-9_]*=' "$1"; }
body "$T/gen.raw" >"$T/gen.all"; body "$LIVE" >"$T/live.all"
envs "$T/gen.all" >"$T/gen.env"; envs "$T/live.all" >"$T/live.env"
jobs "$T/gen.all" >"$T/gen.job"; jobs "$T/live.all" >"$T/live.job"
echo "줄 수 — jobs: gen=$(wc -l <"$T/gen.job") live=$(wc -l <"$T/live.job") / env: gen=$(wc -l <"$T/gen.env") live=$(wc -l <"$T/live.env")"
echo "=== JOB diff (live → gen) · 비어 있으면 PASS ==="
if [ -s "$T/gen.job" ] && [ -s "$T/live.job" ] && diff -u "$T/live.job" "$T/gen.job"; then echo "S2_EMPTY_DIFF=yes"; else echo "S2_EMPTY_DIFF=no"; fi
echo "=== ENV diff (SHELL/PATH/CRON_TZ 등) ==="
if diff -u "$T/live.env" "$T/gen.env"; then echo "S2_ENV_SAME=yes"; else echo "S2_ENV_SAME=no"; fi
echo "=== FENCE 3-way (Step 4C) ==="
"$PY" "$INSTALL_ROOT/bitget/deploy/generate_bitget_crontab.py" --install-root "$INSTALL_ROOT" --fence-check "$LIVE"
echo "=== git ==="
echo "HEAD=$(git rev-parse --short HEAD)"
git status --short --untracked-files=no | head -20
[ -z "$(git status --short --untracked-files=no)" ] && echo "GIT_CLEAN=yes" || echo "GIT_CLEAN=no"
S0
echo "S0_EXIT=$?"

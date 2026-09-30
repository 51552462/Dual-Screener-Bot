#!/bin/bash
# CAT-L-FENCE-03 bootstrap — stamp markers if UNMARKED+equal. No fake drift.
set -u
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot
cd "$INSTALL_ROOT" || exit 1
LIVE=/etc/cron.d/dual-screener-bitget
PY="$INSTALL_ROOT/venv/bin/python"
[ -x "$PY" ] || PY=python3
export PYTHONPATH="$INSTALL_ROOT${PYTHONPATH:+:$PYTHONPATH}"
GEN="$INSTALL_ROOT/bitget/deploy/generate_bitget_crontab.py"

echo "=== FENCE-03 BEFORE pull ==="
git log -1 --format='%h %s'
echo "=== git pull --ff-only ==="
git pull --ff-only
echo "=== HEAD after ==="
git log -1 --format='%h %s'

echo "=== PRE --diff-live ==="
set +e
"$PY" "$GEN" --install-root "$INSTALL_ROOT" --diff-live
echo "DIFF_LIVE_PRE=$?"
set -e

echo "=== PRE body hashes ==="
"$PY" - <<'PY'
import os
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
root = Path(os.environ.get("INSTALL_ROOT", "/home/ubuntu/dante_bots/Dual-Screener-Bot"))
spec = spec_from_file_location("gen", root / "bitget/deploy/generate_bitget_crontab.py")
mod = module_from_spec(spec); spec.loader.exec_module(mod)
live = Path("/etc/cron.d/dual-screener-bitget").read_text(encoding="utf-8")
gen = mod.render_bitget_crontab(str(root))
print("PRE_LIVE_BODY_SHA=" + mod.body_sha256(live))
print("PRE_GEN_BODY_SHA=" + mod.body_sha256(gen))
print("PRE_SOURCE_STATE=" + mod.classify_source_state(live))
print("PRE_BODIES_EQUAL=" + ("yes" if mod.bodies_equal(gen, live) else "no"))
print("PRE_MARKER=" + (mod.live_marker_sha(live) or "(none)"))
PY

echo "=== bootstrap install_bitget_cron.sh (not update_bitget.sh) ==="
sudo bash "$INSTALL_ROOT/bitget/deploy/install_bitget_cron.sh"
echo "INSTALL_EXIT=$?"

echo "=== POST --diff-live ==="
set +e
"$PY" "$GEN" --install-root "$INSTALL_ROOT" --diff-live
echo "DIFF_LIVE_POST=$?"
set -e
"$PY" - <<'PY'
import os
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
root = Path("/home/ubuntu/dante_bots/Dual-Screener-Bot")
spec = spec_from_file_location("gen", root / "bitget/deploy/generate_bitget_crontab.py")
mod = module_from_spec(spec); spec.loader.exec_module(mod)
live = Path("/etc/cron.d/dual-screener-bitget").read_text(encoding="utf-8")
gen = mod.render_bitget_crontab(str(root))
print("POST_LIVE_BODY_SHA=" + mod.body_sha256(live))
print("POST_GEN_BODY_SHA=" + mod.body_sha256(gen))
print("POST_SOURCE_STATE=" + mod.classify_source_state(live))
print("POST_BODIES_EQUAL=" + ("yes" if mod.bodies_equal(gen, live) else "no"))
print("POST_MARKER=" + (mod.live_marker_sha(live) or "(none)"))
print("HASH_UNCHANGED=" + ("yes" if mod.body_sha256(live) == os.environ.get("EXPECT") else "n/a"))
PY
echo "=== grep marker (comments) ==="
grep -n 'CAT-L-FENCE-03' "$LIVE" | head
echo "=== fence-check (no path) ==="
"$PY" "$GEN" --install-root "$INSTALL_ROOT" --fence-check

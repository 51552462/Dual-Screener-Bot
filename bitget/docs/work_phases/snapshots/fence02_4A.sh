#!/bin/bash
# CAT-L-FENCE-02 Step 4A — read-only dirty backup (no checkout/reset/clean)
set -u
cd /home/ubuntu/dante_bots/Dual-Screener-Bot || exit 1
mkdir -p /tmp/fence02_dirty_backup
echo "=== git status --short --untracked-files=no ==="
git status --short --untracked-files=no
STAMP=$(date -u +%Y%m%d%H%M%S)
DIFF=/tmp/fence02_dirty_backup/dirty_${STAMP}.diff
git diff > "$DIFF"
echo "=== backup $DIFF ==="
wc -l "$DIFF"
echo "=== git diff --stat ==="
git diff --stat
echo "=== HEAD ==="
git log -1 --format='%h %ad %s' --date=iso
echo "=== last commit per dirty path ==="
git status --short --untracked-files=no | awk '{print $NF}' | while read -r f; do
  echo "-- $f"
  git log -1 --format='%h %ad %s' --date=iso -- "$f"
done
echo "=== systemd-run / slice / HEAVY hits in dirty diff ==="
HITS=$(grep -cE 'systemd-run|bitget-cron-heavy|_HEAVY_PREFIXES' "$DIFF" || true)
echo "GREP_HITS=$HITS"
grep -nE 'systemd-run|bitget-cron-heavy|_HEAVY_PREFIXES' "$DIFF" | head -50 || true
echo "DIFF_PATH=$DIFF"

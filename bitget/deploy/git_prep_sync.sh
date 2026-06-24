#!/usr/bin/env bash
# Git Bash (Windows) or Ubuntu — unblock pull/push when bitget/*.sqlite blocks merge.
# Usage (repo root):
#   bash bitget/deploy/git_prep_sync.sh
#   bash bitget/deploy/git_prep_sync.sh --push
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PUSH=0
[[ "${1:-}" == "--push" ]] && PUSH=1

echo "[git_prep_sync] repo=$ROOT"

# 1) Backup local bitget DB (optional safety)
BACKUP="${HOME}/bitget-git-backup-$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP"
for f in bitget/*.sqlite bitget/bitget_meta_governor_state.json; do
  [[ -f "$f" ]] && cp -a "$f" "$BACKUP/" && echo "  backed up $(basename "$f") -> $BACKUP"
done

# 2) Drop WAL/SHM locks so pull can overwrite
rm -f bitget/*.sqlite-shm bitget/*.sqlite-wal 2>/dev/null || true

# 3) Discard tracked DB working-tree edits (runtime files, not source code)
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  for f in \
    bitget/bitget_market_data.sqlite \
    bitget/bitget_market_data_snapshot.sqlite \
    bitget/bitget_ops_events.sqlite \
    bitget/bitget_system_config.sqlite \
    bitget/bitget_meta_governor_state.json
  do
    if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
      git restore "$f" 2>/dev/null || git checkout -- "$f" 2>/dev/null || true
    fi
  done
fi

# 4) Stop tracking runtime DB in git index (keep files on disk)
TRACKED=(
  bitget/bitget_market_data.sqlite
  bitget/bitget_market_data_snapshot.sqlite
  bitget/bitget_ops_events.sqlite
  bitget/bitget_system_config.sqlite
  bitget/bitget_meta_governor_state.json
)
TO_UNTRACK=()
for f in "${TRACKED[@]}"; do
  git ls-files --error-unmatch "$f" >/dev/null 2>&1 && TO_UNTRACK+=("$f")
done
if ((${#TO_UNTRACK[@]})); then
  git rm --cached -f "${TO_UNTRACK[@]}"
  echo "[git_prep_sync] untracked from git index: ${TO_UNTRACK[*]}"
fi

git add .gitignore
if ! git diff --cached --quiet; then
  git commit -m "chore: gitignore bitget runtime DB; stop tracking sqlite in repo"
fi

# 5) Sync with origin (merge — safer on Windows than rebase when branches diverged)
git fetch origin
if git rev-parse --verify origin/main >/dev/null 2>&1; then
  if git merge-base --is-ancestor origin/main HEAD 2>/dev/null; then
    echo "[git_prep_sync] already contains origin/main"
  else
    echo "[git_prep_sync] merging origin/main..."
    git merge origin/main -m "Merge origin/main (git_prep_sync)"
    echo "If conflicts: keep local bitget/*.py, remove bitget/*.sqlite from index (git rm --cached)"
  fi
fi

if [[ "$PUSH" -eq 1 ]]; then
  git push origin main
  echo "[git_prep_sync] push OK"
else
  echo "[git_prep_sync] ready — run: git push origin main"
fi

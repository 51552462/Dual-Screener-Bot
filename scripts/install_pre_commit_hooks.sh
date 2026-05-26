#!/usr/bin/env bash
# Install Ruff pre-commit hooks (blocks commit if forward/deep_dive.py has F821).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
python3 -m pip install -r requirements-dev.txt
python3 -m pre_commit install
python3 -m pre_commit run ruff --hook-stage manual --files forward/deep_dive.py 2>/dev/null \
  || python3 -m ruff check forward/deep_dive.py --select=F821,F822,F823
echo "pre-commit installed. Commits will run Ruff F821 on forward/deep_dive.py and import binding tests."

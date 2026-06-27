# Install Ruff pre-commit hooks (blocks commit if forward/deep_dive.py has F821).
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
python -m pip install -r requirements-dev.txt
python -m pre_commit install
python -m ruff check forward/deep_dive.py --select=F821,F822,F823
Write-Host "pre-commit installed. Commits will run Ruff F821 on forward/deep_dive.py and import binding tests."

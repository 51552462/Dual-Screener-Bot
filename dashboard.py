"""Streamlit 관제탑 — systemd dante-dashboard.service 진입점."""
from pathlib import Path
from runpy import run_path

run_path(
    Path(__file__).resolve().parent / "legacy_archive" / "dashboard.py",
    run_name="__main__",
)

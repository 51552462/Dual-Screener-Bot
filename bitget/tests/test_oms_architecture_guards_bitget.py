"""Architecture guards — OMS book consumer SSOT must not silently regress."""
from __future__ import annotations

from bitget.validation.architecture_checks import (
    check_integrity_backup_ssot,
    check_oms_book_consumer_ssot,
    check_portfolio_nav_risk_ssot,
    run_architecture_checks,
)


def test_oms_book_consumer_ssot_passes():
    r = check_oms_book_consumer_ssot()
    assert r["ok"] is True, r
    assert not r.get("failed"), r
    for name, detail in (r.get("details") or {}).items():
        assert detail.get("ok") is True, (name, detail)


def test_portfolio_nav_risk_ssot_passes():
    r = check_portfolio_nav_risk_ssot()
    assert r["ok"] is True, r
    assert not r.get("failed"), r
    for name, detail in (r.get("details") or {}).items():
        assert detail.get("ok") is True, (name, detail)


def test_integrity_backup_ssot_passes():
    r = check_integrity_backup_ssot()
    assert r["ok"] is True, r
    assert not r.get("failed"), r


def test_run_architecture_includes_oms_consumer_guard():
    report = run_architecture_checks()
    assert "oms_book_consumer_ssot" in (report.get("checks") or {})
    assert report["checks"]["oms_book_consumer_ssot"]["ok"] is True
    assert "portfolio_nav_risk_ssot" in (report.get("checks") or {})
    assert report["checks"]["portfolio_nav_risk_ssot"]["ok"] is True
    assert "integrity_backup_ssot" in (report.get("checks") or {})
    assert report["checks"]["integrity_backup_ssot"]["ok"] is True
    assert "watchdog_restart_matrix_ssot" in (report.get("checks") or {})
    assert report["checks"]["watchdog_restart_matrix_ssot"]["ok"] is True


def test_guard_detects_executor_inline_ticker_regression(tmp_path, monkeypatch):
    """Sanity: forbidden inline fetch_ticker in a fake executor fails the sub-check."""
    import bitget.validation.architecture_checks as ac

    fake_root = tmp_path / "bitget"
    (fake_root / "trading").mkdir(parents=True)
    (fake_root / "validation").mkdir(parents=True)
    (fake_root / "pipelines").mkdir(parents=True)
    (fake_root / "infra").mkdir(parents=True)

    # Minimal stubs so other sub-checks don't all fail for "missing"
    for rel, body in (
        ("executor.py", 'ex.fetch_ticker(sym)\nop="oms.fetch_ticker"\n'),
        ("trading/account_snapshot.py", ""),
        ("trading/market_price_snapshot.py", ""),
        ("trading/leverage_manager.py", ""),
        ("trading/slippage_guard.py", ""),
        ("trading/oms_source_stats.py", ""),
        ("validation/ws_oms_smoke.py", ""),
        ("pipelines/bitget_auto_pilot.py", ""),
        ("infra/memory_policy.py", ""),
    ):
        p = fake_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body, encoding="utf-8")

    monkeypatch.setattr(ac, "_BITGET_ROOT", fake_root)
    r = ac.check_oms_book_consumer_ssot()
    assert r["ok"] is False
    assert "executor" in r["failed"]
    assert r["details"]["executor"]["forbidden_present"]

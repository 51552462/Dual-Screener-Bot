"""Legacy scanner revival — master pipeline wiring."""
from factory_pipelines import build_factory_pipelines


def test_scan_kr_includes_master_step():
    steps = build_factory_pipelines()["scan_kr"]
    names = [s.name for s in steps]
    assert "kr_master_scan" in names
    assert names.index("kr_master_scan") > names.index("kr_ema5_scan")
    assert names.index("kr_bowl_scan") > names.index("kr_master_scan")

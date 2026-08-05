"""A-5a: deathmatch classify_strategy_arm — S5 label + rollback."""
from __future__ import annotations

from evolution.deathmatch_report import classify_strategy_arm


class TestClassifyStrategyArmS5:
    def test_blackhole_maps_to_s5_when_enabled(self):
        assert classify_strategy_arm("SCAN_BLACKHOLE_HIT") == "S5"

    def test_inverse_maps_to_s5_when_enabled(self):
        assert classify_strategy_arm("Dante[INVERSE_ETF]") == "S5"

    def test_blackhole_reverts_bh_when_merge_disabled(self):
        cfg = {"ENABLE_WEIGHT_S5_MERGE": False}
        assert classify_strategy_arm("SCAN_BLACKHOLE_HIT", cfg) == "BH (블랙홀)"

    def test_supernova_still_b_arm(self):
        assert classify_strategy_arm("SUPERNOVA_COSINE foo") == "B (초신성)"

    def test_toxic_fade_alone_not_s5_label(self):
        label = classify_strategy_arm("FOO_TOXIC_FADE_ONLY")
        assert label != "S5"

    def test_toxic_fade_with_inverse_is_s5(self):
        assert classify_strategy_arm("Dante[INVERSE_ETF][TOXIC_FADE]") == "S5"

    def test_inverse_not_s5_when_merge_disabled_falls_through(self):
        cfg = {"ENABLE_WEIGHT_S5_MERGE": False}
        label = classify_strategy_arm("Dante[INVERSE_ETF]", cfg)
        assert label is not None
        assert label != "S5"

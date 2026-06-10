"""META_GOVERNOR_LAST_RUN_AT age gate + resolve_config_regime_key priority."""
from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from meta_state_store import (
    is_meta_state_degraded,
    meta_governor_run_age_hours,
    resolve_config_regime_key,
)


def _fresh_meta(**overrides) -> dict:
    base = {
        "META_GOVERNOR_LAST_RUN_STATUS": "OK",
        "META_GOVERNOR_LAST_RUN_AT": datetime.now(timezone.utc).isoformat(),
        "META_REGIME_KEY": "BULL",
        "META_REGIME_CONFIDENCE": 0.8,
        "META_REGIME_ACTION": {"notes": "ok", "kelly_cap": 0.028},
    }
    base.update(overrides)
    return base


class TestMetaGovernorAge(unittest.TestCase):
    def test_fresh_timestamp_not_degraded(self) -> None:
        self.assertFalse(is_meta_state_degraded(_fresh_meta()))

    def test_stale_timestamp_degraded(self) -> None:
        old = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        self.assertTrue(is_meta_state_degraded(_fresh_meta(META_GOVERNOR_LAST_RUN_AT=old)))

    @patch.dict(os.environ, {"FACTORY_META_MAX_AGE_HOURS": "12"})
    def test_max_age_env_respected(self) -> None:
        old = (datetime.now(timezone.utc) - timedelta(hours=13)).isoformat()
        self.assertTrue(is_meta_state_degraded(_fresh_meta(META_GOVERNOR_LAST_RUN_AT=old)))
        fresh = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        self.assertFalse(is_meta_state_degraded(_fresh_meta(META_GOVERNOR_LAST_RUN_AT=fresh)))

    def test_unparseable_timestamp_degraded(self) -> None:
        self.assertTrue(
            is_meta_state_degraded(_fresh_meta(META_GOVERNOR_LAST_RUN_AT="not-a-date"))
        )
        self.assertIsNone(meta_governor_run_age_hours(_fresh_meta(META_GOVERNOR_LAST_RUN_AT="bad")))


class TestResolveConfigRegimeKey(unittest.TestCase):
    def test_unknown_regime_analysis_uses_current(self) -> None:
        cfg = {
            "REGIME_ANALYSIS": {"regime_key": "UNKNOWN"},
            "CURRENT_REGIME_KEY": "BULL",
        }
        self.assertEqual(resolve_config_regime_key(cfg), "BULL")

    def test_valid_regime_analysis_wins(self) -> None:
        cfg = {
            "REGIME_ANALYSIS": {"regime_key": "BEAR"},
            "CURRENT_REGIME_KEY": "BULL",
        }
        self.assertEqual(resolve_config_regime_key(cfg), "BEAR")


if __name__ == "__main__":
    unittest.main()

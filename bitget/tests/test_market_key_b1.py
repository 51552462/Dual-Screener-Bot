"""B-1 — deathmatch market key normalization (BG → SPOT | FUT)."""
from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest


class TestNormalizeMarketKey:
    def test_bg_resolves_via_position_hint_futures(self, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        from bitget.evolution.market_key_normalize import normalize_market_key

        assert normalize_market_key("BG", position_market_hint="futures") == "FUT"

    def test_bg_resolves_via_position_hint_spot(self, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        from bitget.evolution.market_key_normalize import normalize_market_key

        assert normalize_market_key("BG", position_market_hint="spot") == "SPOT"

    def test_bg_default_spot_when_no_hint(self, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        from bitget.evolution.market_key_normalize import normalize_market_key

        assert normalize_market_key("BG") == "SPOT"

    def test_kill_switch_preserves_legacy_mapping(self, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "0")
        from bitget.evolution.market_key_normalize import normalize_market_key
        from bitget.infra.market_keys import to_deathmatch_key

        assert normalize_market_key("BG") == to_deathmatch_key("BG")
        assert normalize_market_key("futures") == "FUT"


class TestRegistryLifecycleAlign:
    def test_legacy_bg_write_through(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        import sqlite3

        from strategy_registry_store import ensure_strategy_registry_schema, load_registry_rows

        db = tmp_path / "m.sqlite"
        db.write_bytes(b"")
        ensure_strategy_registry_schema(str(db))
        conn = sqlite3.connect(str(db))
        conn.execute(
            """
            INSERT INTO strategy_registry (
                strategy_id, market, group_key, state, display_name, capital_mult,
                source, updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            ("sid1", "BG", "GRP_ALPHA", "OBSERVING", "GRP_ALPHA", 0.0, "test", "t"),
        )
        conn.commit()
        conn.close()

        fwd = tmp_path / "fwd.sqlite"
        conn = sqlite3.connect(str(fwd))
        conn.execute(
            """
            CREATE TABLE bitget_forward_trades (
                sig_type TEXT, market_type TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO bitget_forward_trades VALUES (?,?)",
            [("GRP_ALPHA", "futures"), ("GRP_ALPHA", "futures")],
        )
        conn.commit()
        conn.close()

        with mock.patch(
            "bitget.evolution.registry_lifecycle_bg.build_group_market_hints_from_forward_db",
            return_value={"GRP_ALPHA": "futures"},
        ):
            from bitget.evolution.registry_lifecycle_bg import (
                normalize_bitget_registry_after_lifecycle,
            )

            out = normalize_bitget_registry_after_lifecycle(db_path=str(db))
        assert out["changed"] == 1
        rows = load_registry_rows(str(db))
        assert rows[0]["market"] == "FUT"

    def test_deathmatch_and_lifecycle_emit_same_key(self, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        from bitget.evolution.deathmatch_bg import run_bitget_battle_royal
        from bitget.evolution.market_key_normalize import normalize_market_key

        mk = normalize_market_key("futures")
        assert mk == "FUT"

        with mock.patch("bitget.evolution.deathmatch_bg.bitget_deathmatch_ssot"), mock.patch(
            "evolution.deathmatch_battle_royale.run_battle_royal"
        ) as br:
            br.return_value = mock.Mock(arms=[], champion=None, verdict="ok")
            run_bitget_battle_royal(pd.DataFrame(), market_type="futures", persist=False)
            assert br.call_args.kwargs.get("market") == "FUT"


class TestIncubatorTemplatesRegression:
    def test_judge_incubator_templates_unchanged(self, monkeypatch):
        monkeypatch.setenv("DEATHMATCH_KEY_NORMALIZE_ENABLED", "1")
        from bitget import auto_pilot

        cfg = {
            "INCUBATOR_TEMPLATES": {
                "alpha": {"dna": 1},
            }
        }
        df = pd.DataFrame(
            {
                "sig_type": ["INCUBATOR_alpha"] * 6,
                "final_ret": [0.1, 0.2, 0.1, 0.2, 0.1, 0.2],
            }
        )

        class _Conn:
            def close(self):
                pass

        with mock.patch.object(auto_pilot, "get_connection", return_value=_Conn()), mock.patch(
            "pandas.read_sql", return_value=df
        ):
            out, msg = auto_pilot._judge_incubator_templates(dict(cfg))
        assert "INCUBATOR_TEMPLATES" in out
        assert out["INCUBATOR_TEMPLATES"] == {}

"""
MetaGovernor._step_lifecycle — Bitget 단독 사이클(forward_db_path=None)에서
strategy_registry 읽기/쓰기가 반드시 bitget_db_path 로 격리 라우팅되는지 검증.

회귀 대상 버그: forward_db_path 가 없을 때 run_registry_lifecycle 이 인자 없이
호출되면 strategy_registry_store._db_path() 가 주식 market_data.sqlite 로 폴백해
코인 파생 레지스트리 행이 주식 운영 DB를 오염시킨다.
"""
from __future__ import annotations

import unittest
from unittest import mock

from meta_governor import GovernorRunContext, MetaGovernor


class TestBitgetRegistryIsolation(unittest.TestCase):
    def _governor_with_ctx(self, ctx: GovernorRunContext) -> MetaGovernor:
        gov = MetaGovernor()
        gov._ctx = ctx
        gov._working = {"META_STRATEGY_HEALTH": {}}
        gov._prior = {}
        gov._system_cfg_snapshot = {}
        return gov

    def test_bitget_only_cycle_routes_registry_to_bitget_db(self):
        ctx = GovernorRunContext(forward_db_path=None, bitget_db_path="/tmp/fake_bitget_market_data.sqlite")
        gov = self._governor_with_ctx(ctx)

        with mock.patch(
            "strategy_promotion_engine.run_registry_lifecycle",
            return_value=([], {}),
        ) as mocked:
            gov._step_lifecycle()

        self.assertEqual(mocked.call_count, 1)
        _, kwargs = mocked.call_args
        self.assertEqual(kwargs.get("forward_db_path"), "/tmp/fake_bitget_market_data.sqlite")

    def test_unified_stock_cycle_still_prefers_forward_db_path(self):
        """주식측 통합 사이클(forward_db_path 기존 설정)은 기존 동작 그대로 유지."""
        ctx = GovernorRunContext(
            forward_db_path="/tmp/fake_market_data.sqlite",
            bitget_db_path="/tmp/fake_bitget_market_data.sqlite",
        )
        gov = self._governor_with_ctx(ctx)

        with mock.patch(
            "strategy_promotion_engine.run_registry_lifecycle",
            return_value=([], {}),
        ) as mocked:
            gov._step_lifecycle()

        _, kwargs = mocked.call_args
        self.assertEqual(kwargs.get("forward_db_path"), "/tmp/fake_market_data.sqlite")


if __name__ == "__main__":
    unittest.main()

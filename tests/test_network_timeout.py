"""network_timeout — bounded fdr/yf wrappers."""
from __future__ import annotations

import unittest
from unittest import mock

from network_timeout import default_network_timeout_sec, run_with_timeout


class TestNetworkTimeout(unittest.TestCase):
    def test_default_timeout_positive(self):
        self.assertGreaterEqual(default_network_timeout_sec(), 5.0)

    def test_run_with_timeout_returns(self):
        self.assertEqual(run_with_timeout(lambda: 42, timeout_sec=5.0), 42)

    def test_run_with_timeout_raises(self):
        import time

        def slow():
            time.sleep(10)

        with self.assertRaises(TimeoutError):
            run_with_timeout(slow, timeout_sec=0.2)


if __name__ == "__main__":
    unittest.main()

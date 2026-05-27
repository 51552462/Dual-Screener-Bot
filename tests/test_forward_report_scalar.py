"""forward_report_scalar — 중복 컬럼·Series→float 방어."""
from __future__ import annotations

import unittest

import pandas as pd

from reports.forward_report_scalar import (
    col_series,
    dedupe_columns,
    prepare_forward_trades_df,
    row_scalar,
    scalar_float,
    series_mean,
)
from practitioner_intelligence import _toxic_hit_line
from reports.report_feature_analyzer import ReportFeatureAnalyzer


def _dup_df(*pairs: tuple[str, list]) -> pd.DataFrame:
    """동일 컬럼명 2회 — concat으로 중복 축 구성."""
    parts = [pd.DataFrame({k: v}) for k, v in pairs]
    return pd.concat(parts, axis=1)


class TestScalarHelpers(unittest.TestCase):
    def test_scalar_float_from_series(self):
        s = pd.Series([1.5, 2.5])
        self.assertEqual(scalar_float(s), 2.0)

    def test_scalar_float_empty_series(self):
        self.assertEqual(scalar_float(pd.Series(dtype=float)), 0.0)
        self.assertEqual(scalar_float(pd.Series(dtype=float), default=-1.0), -1.0)

    def test_col_series_duplicate_columns(self):
        df = _dup_df(("final_ret", [1.0]), ("final_ret", [9.0]))
        s = col_series(df, "final_ret")
        self.assertEqual(len(s), 1)
        self.assertEqual(float(s.iloc[0]), 1.0)

    def test_row_scalar_duplicate_columns(self):
        df = _dup_df(("dyn_cpv", [2.0]), ("dyn_cpv", [99.0]))
        row = next(df.iterrows())[1]
        self.assertEqual(row_scalar(row, "dyn_cpv"), 2.0)

    def test_series_mean_duplicate_columns(self):
        df = pd.concat(
            [
                pd.DataFrame({"dyn_cpv": [1.0, 3.0]}),
                pd.DataFrame({"dyn_cpv": [9.0, 9.0]}),
            ],
            axis=1,
        )
        self.assertEqual(series_mean(df, "dyn_cpv"), 2.0)

    def test_dedupe_logs_warning(self):
        df = _dup_df(("a", [1]), ("a", [2]))
        with self.assertLogs("forward_report_scalar", level="WARNING") as cm:
            out = dedupe_columns(df, context="unit_test")
        self.assertEqual(list(out.columns), ["a"])
        self.assertTrue(any("Duplicate columns detected" in m for m in cm.output))
        self.assertTrue(any("a" in m for m in cm.output))

    def test_prepare_forward_trades_nan_final_ret(self):
        df = pd.DataFrame({"final_ret": [None, 1.0]})
        out = prepare_forward_trades_df(df)
        self.assertEqual(out["final_ret"].tolist(), [0.0, 1.0])


class TestReportFeatureAnalyzerDuplicateCols(unittest.TestCase):
    def test_winner_loser_contrast_no_crash(self):
        winners = pd.concat(
            [
                pd.DataFrame({"dyn_cpv": [0.2, 0.3], "dyn_rs": [0.5, 0.6]}),
                pd.DataFrame({"dyn_cpv": [0.1, 0.2], "dyn_rs": [0.4, 0.5]}),
            ],
            axis=1,
        )
        losers = pd.concat(
            [
                pd.DataFrame({"dyn_cpv": [0.8, 0.9], "dyn_rs": [0.1, 0.2]}),
                pd.DataFrame({"dyn_cpv": [0.7, 0.8], "dyn_rs": [0.2, 0.3]}),
            ],
            axis=1,
        )
        rfa = ReportFeatureAnalyzer(sys_config={}, meta={})
        lines, ok, insights = rfa.build_winner_loser_dna_contrast(
            winners_df=winners,
            losers_df=losers,
            top_n=2,
            min_per_group=2,
        )
        self.assertIsInstance(lines, list)
        self.assertIsInstance(insights, list)


class TestToxicHitLine(unittest.TestCase):
    def test_toxic_hit_duplicate_row_columns(self):
        df = pd.concat(
            [
                pd.DataFrame(
                    {
                        "dyn_cpv": [0.5],
                        "dyn_tb": [0.1],
                        "v_energy": [1.0],
                        "dyn_rs": [0.3],
                        "sector": ["테스트"],
                    }
                ),
                pd.DataFrame(
                    {
                        "dyn_cpv": [0.9],
                        "dyn_tb": [0.2],
                        "v_energy": [2.0],
                        "dyn_rs": [0.4],
                    }
                ),
            ],
            axis=1,
        )
        line = _toxic_hit_line(df, {}, {})
        self.assertIsInstance(line, str)


if __name__ == "__main__":
    unittest.main()

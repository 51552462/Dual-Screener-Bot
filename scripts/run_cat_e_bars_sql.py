"""Read-only: CAT-E-BARS-01 VPS SQL (a)-(d) against market_data.sqlite."""
from __future__ import annotations

import os
import sqlite3
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from market_db_paths import report_db_read_path

QUERIES = {
    "a": """
SELECT market,
  COUNT(*) AS n_closed,
  SUM(bars_held IS NULL) AS null_bars,
  SUM(final_ret IS NULL) AS null_ret,
  SUM(exit_reason IS NULL OR TRIM(exit_reason)='') AS null_exit_reason,
  SUM(exit_type IS NULL OR TRIM(exit_type)='' OR UPPER(exit_type)='UNKNOWN') AS bad_exit_type,
  SUM(entry_regime IS NULL OR TRIM(entry_regime)='' OR UPPER(entry_regime)='UNKNOWN') AS bad_regime
FROM forward_trades
WHERE status LIKE 'CLOSED%'
GROUP BY market
""",
    "b": """
SELECT market, exit_type, COUNT(*) AS n
FROM forward_trades
WHERE status LIKE 'CLOSED%'
GROUP BY market, exit_type
ORDER BY market, n DESC
""",
    "c": """
SELECT market, status, COUNT(*) AS n,
  SUM(exit_type IS NULL OR UPPER(IFNULL(exit_type,'')) IN ('','UNKNOWN')) AS bad_et
FROM forward_trades
WHERE status LIKE 'CLOSED%'
GROUP BY market, status
""",
    "d": """
SELECT market,
  CASE
    WHEN bars_held IS NULL THEN 'null'
    WHEN bars_held <= 3 THEN '1-3'
    WHEN bars_held <= 6 THEN '4-6'
    WHEN bars_held <= 10 THEN '7-10'
    WHEN bars_held <= 14 THEN '11-14'
    ELSE '15+'
  END AS bars_bucket,
  COUNT(*) AS n,
  ROUND(AVG(final_ret), 3) AS avg_ret,
  ROUND(SUM(CASE WHEN final_ret > 0 THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_pct,
  ROUND(AVG(bars_held), 2) AS avg_bars
FROM forward_trades
WHERE status LIKE 'CLOSED%'
  AND final_ret IS NOT NULL
GROUP BY market, bars_bucket
ORDER BY market, MIN(IFNULL(bars_held, -1))
""",
}


def main() -> int:
    db = report_db_read_path()
    print(f"DB: {db}")
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    total = conn.execute("SELECT COUNT(*) FROM forward_trades").fetchone()[0]
    print(f"forward_trades total: {total}")
    for key, sql in QUERIES.items():
        print(f"\n=== ({key}) ===")
        rows = conn.execute(sql).fetchall()
        if not rows:
            print("(no rows)")
        else:
            for row in rows:
                print(row)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

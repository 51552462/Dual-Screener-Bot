"""track_positions FAIL(2026-07-06, run_id 20260706T010013) 회귀 테스트.

배포로 여러 bitget 서비스가 동시에 기동하면, forward DB 스키마 부트스트랩
(`init_forward_db`)의 DROP/CREATE VIEW DDL 이 프로세스마다·호출마다 재실행되어
"database is locked" 경합이 track_daily_positions 까지 전파되어 파이프라인
critical step 이 크래시했다.

수정 내용을 검증한다:
  1. 스키마 DDL 은 DB 경로별로 프로세스 생애주기 중 1회만 실행된다 (메모이즈).
  2. DROP VIEW 단계에서 "database is locked" 이 나도 CREATE VIEW 처럼 삼켜지고
     init_forward_db() 호출 자체는 성공한다 (더 이상 상위로 전파되지 않음).
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest import mock


class TestForwardDbInitMemoization(unittest.TestCase):
    def test_schema_ddl_runs_once_per_db_path(self):
        from bitget.forward import shared

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "bitget_market_data.sqlite")
            shared._FORWARD_DB_SCHEMA_READY_PATHS.discard(os.path.abspath(db_path))
            with mock.patch.object(shared, "DB_PATH", db_path):
                with mock.patch.object(
                    shared, "_init_forward_db_schema", wraps=shared._init_forward_db_schema
                ) as spy:
                    shared.init_forward_db()
                    shared.init_forward_db()
                    shared.init_forward_db()
                self.assertEqual(spy.call_count, 1)
            shared._FORWARD_DB_SCHEMA_READY_PATHS.discard(os.path.abspath(db_path))

    def test_reinitializes_schema_for_a_different_db_path(self):
        """경로가 다르면 각 경로별로 스키마가 정상 생성돼야 한다 (테스트 격리 보장)."""
        from bitget.forward import shared

        with tempfile.TemporaryDirectory() as td:
            db_path_a = os.path.join(td, "a.sqlite")
            db_path_b = os.path.join(td, "b.sqlite")
            for p in (db_path_a, db_path_b):
                shared._FORWARD_DB_SCHEMA_READY_PATHS.discard(os.path.abspath(p))

            with mock.patch.object(shared, "DB_PATH", db_path_a):
                shared.init_forward_db()
            with mock.patch.object(shared, "DB_PATH", db_path_b):
                shared.init_forward_db()

            for p in (db_path_a, db_path_b):
                conn = sqlite3.connect(p)
                tables = {
                    r[0]
                    for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
                conn.close()
                self.assertIn("bitget_forward_trades", tables)
                shared._FORWARD_DB_SCHEMA_READY_PATHS.discard(os.path.abspath(p))


class TestDropCreateViewLockToleration(unittest.TestCase):
    def test_drop_view_lock_is_swallowed_like_create_view(self):
        """DROP VIEW 단계의 'database is locked' 도 CREATE VIEW 처럼 무시돼야 한다.

        수정 전에는 DROP 이 try 블록 밖에 있어 이 케이스에서 예외가 그대로
        상위(track_daily_positions -> _step_track_spot -> run_step)로 전파돼
        파이프라인 critical step 이 크래시했다.
        """
        from bitget.forward import shared

        calls = {"n": 0}

        class FlakyCursor:
            def __init__(self, real_cur):
                self._real = real_cur

            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str) and sql.strip().upper().startswith("DROP VIEW"):
                    calls["n"] += 1
                    raise sqlite3.OperationalError("database is locked")
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._real, name)

        class ConnProxy:
            """sqlite3.Connection 은 임의 속성 patch가 안 되는 C 확장 타입이라
            얇은 프록시로 감싸 cursor()만 가로챈다."""

            def __init__(self, real_conn):
                self._real = real_conn

            def cursor(self):
                return FlakyCursor(self._real.cursor())

            def __getattr__(self, name):
                return getattr(self._real, name)

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "bitget_market_data.sqlite")
            real_conn = sqlite3.connect(db_path)
            try:
                proxy = ConnProxy(real_conn)
                # 예외를 던지지 않고 정상 반환되어야 한다.
                shared._init_forward_db_schema(proxy)

                self.assertGreaterEqual(calls["n"], 1)
            finally:
                real_conn.close()


class TestEnsureColTargetsCorrectTable(unittest.TestCase):
    """회귀: `_ensure_col` 이 테이블명을 하드코딩해 `bitget_real_execution` 전용
    컬럼(client_order_id)을 엉뚱하게 `bitget_forward_trades` 에 추가하던 버그.
    그 결과 `bitget_real_execution` 에는 컬럼이 끝내 생기지 않아
    `log_real_execution()` 의 INSERT 가 항상 'no column named client_order_id'
    로 실패했다."""

    def test_client_order_id_lands_on_real_execution_table(self):
        from bitget.forward import shared

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "bitget_market_data.sqlite")
            conn = sqlite3.connect(db_path)
            try:
                shared._init_forward_db_schema(conn)
                cols_real_exec = {
                    r[1] for r in conn.execute("PRAGMA table_info(bitget_real_execution)").fetchall()
                }
                self.assertIn("client_order_id", cols_real_exec)
            finally:
                conn.close()

    def test_ensure_col_respects_explicit_table_argument(self):
        from bitget.forward.shared import _ensure_col

        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE t_a (id INTEGER)")
            conn.execute("CREATE TABLE t_b (id INTEGER)")
            cur = conn.cursor()
            _ensure_col(cur, "foo", "TEXT DEFAULT ''", table="t_b")
            cols_a = {r[1] for r in conn.execute("PRAGMA table_info(t_a)").fetchall()}
            cols_b = {r[1] for r in conn.execute("PRAGMA table_info(t_b)").fetchall()}
            self.assertNotIn("foo", cols_a)
            self.assertIn("foo", cols_b)
        finally:
            conn.close()

    def test_ensure_col_default_table_unchanged_for_existing_callers(self):
        """기존 25개 호출부(테이블명 생략)는 여전히 bitget_forward_trades 를 타야 한다."""
        from bitget.forward.shared import _ensure_col

        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE bitget_forward_trades (id INTEGER)")
            cur = conn.cursor()
            _ensure_col(cur, "some_new_col", "TEXT DEFAULT ''")
            cols = {r[1] for r in conn.execute("PRAGMA table_info(bitget_forward_trades)").fetchall()}
            self.assertIn("some_new_col", cols)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()

"""Mission 9 — 콜드 스타트 방어(동적 룩백) + 콜드 스토리지 + 자율 수집(오프라인 graceful)."""
import sqlite3
from datetime import datetime, timedelta

import deep_archive_history as dah
import regime_analog_engine as rae


# ---------------------------------------------------------------------------
# 동적 룩백 윈도우 (Elastic Lookback)
# ---------------------------------------------------------------------------
def test_clamp_lookback_no_db_returns_requested():
    info = rae.clamp_lookback_window(None, "archive_ohlcv", requested_days=365)
    assert info["clamped"] is False
    assert info["has_data"] is False


def _seed_archive(path, *, symbol="^GSPC", days_back=40):
    dah.init_cold_storage(path)
    conn = sqlite3.connect(path)
    base = datetime.now() - timedelta(days=days_back)
    for i in range(days_back):
        d = (base + timedelta(days=i)).strftime("%Y-%m-%d")
        px = 100.0 + i
        conn.execute(
            f"INSERT OR REPLACE INTO {dah.ARCHIVE_TABLE} "
            "(symbol,market,date,open,high,low,close,volume,source) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (symbol, "US", d, px, px, px, px, 1000, "seed"),
        )
    conn.commit()
    conn.close()


def test_clamp_lookback_shrinks_to_available(tmp_path):
    db = str(tmp_path / "cold.sqlite")
    _seed_archive(db, days_back=40)  # 40일치만 보유
    info = rae.clamp_lookback_window(db, dah.ARCHIVE_TABLE, requested_days=365)
    # 1년을 요청했지만 40일치만 있으므로 윈도우가 보유 최대 기간으로 축소됨
    assert info["has_data"] is True
    assert info["clamped"] is True
    assert info["start"] == info["db_min"]


def test_clamp_lookback_no_shrink_when_enough(tmp_path):
    db = str(tmp_path / "cold2.sqlite")
    _seed_archive(db, days_back=40)
    info = rae.clamp_lookback_window(db, dah.ARCHIVE_TABLE, requested_days=10)
    # 10일만 요청 → 보유(40일)로 충분하므로 축소 안 함
    assert info["clamped"] is False


def test_clamp_lookback_rejects_bad_identifier(tmp_path):
    db = str(tmp_path / "cold3.sqlite")
    _seed_archive(db)
    info = rae.clamp_lookback_window(db, "archive; DROP TABLE x", requested_days=10)
    assert info["has_data"] is False  # 인젝션 방어로 조회 거부


# ---------------------------------------------------------------------------
# 콜드 스토리지 격리 + 통계 + 인덱스 시퀀스
# ---------------------------------------------------------------------------
def test_cold_storage_init_and_stats(tmp_path):
    db = str(tmp_path / "cs.sqlite")
    _seed_archive(db, days_back=30)
    stats = dah.cold_storage_stats(db)
    assert stats["exists"] is True
    assert stats["rows"] == 30
    assert stats["symbols"] == 1


def test_load_index_series_znormalized(tmp_path):
    db = str(tmp_path / "cs2.sqlite")
    _seed_archive(db, days_back=30)
    series = dah.load_index_series("^GSPC", db_path=db)
    assert len(series) == 30
    # z-정규화 → 평균≈0
    assert abs(sum(series) / len(series)) < 1e-6


def test_load_index_series_empty_when_missing(tmp_path):
    db = str(tmp_path / "cs3.sqlite")
    dah.init_cold_storage(db)
    assert dah.load_index_series("^GSPC", db_path=db) == []


# ---------------------------------------------------------------------------
# 자율 수집 — 오프라인/모듈부재 시 graceful (테스트 환경 네트워크 없음 가정)
# ---------------------------------------------------------------------------
def test_hydrate_symbol_graceful_offline(tmp_path, monkeypatch):
    db = str(tmp_path / "hy.sqlite")
    # 다운로더가 항상 빈 결과(오프라인)라도 예외 없이 skip 보고
    monkeypatch.setitem(dah._DOWNLOADERS, "yf", lambda s, a, b: None)
    out = dah.hydrate_symbol("^GSPC", "US", "2020-03-01", "2020-09-30", source="yf", db_path=db)
    assert out["rows"] == 0
    assert "skipped" in out


def test_hydrate_symbol_stores_when_downloader_returns_data(tmp_path, monkeypatch):
    import pandas as pd

    db = str(tmp_path / "hy2.sqlite")
    idx = pd.to_datetime(["2020-03-02", "2020-03-03", "2020-03-04"])
    fake = pd.DataFrame(
        {"Open": [1, 2, 3], "High": [1, 2, 3], "Low": [1, 2, 3],
         "Close": [1, 2, 3], "Volume": [10, 20, 30]},
        index=idx,
    )
    monkeypatch.setitem(dah._DOWNLOADERS, "yf", lambda s, a, b: fake)
    out = dah.hydrate_symbol("^GSPC", "US", "2020-03-01", "2020-09-30", source="yf", db_path=db)
    assert out["rows"] == 3
    assert dah.cold_storage_stats(db)["rows"] == 3


def test_hydrate_episode_throttles_and_isolates(tmp_path, monkeypatch):
    db = str(tmp_path / "hy3.sqlite")
    calls = {"throttle": 0}
    monkeypatch.setitem(dah._DOWNLOADERS, "yf", lambda s, a, b: None)
    monkeypatch.setitem(dah._DOWNLOADERS, "ccxt", lambda s, a, b: None)
    monkeypatch.setitem(dah._DOWNLOADERS, "pykrx", lambda s, a, b: None)
    out = dah.hydrate_episode(
        "V_RECOVERY", db_path=db,
        throttle_fn=lambda: calls.__setitem__("throttle", calls["throttle"] + 1),
    )
    assert out["episode"] == "V_RECOVERY"
    # 매크로/지수 + 대장주 타겟마다 코인 양보(throttle) 호출
    assert calls["throttle"] == out["targets"]
    # 대장주 포함이지만 상한(MAX_LEADERS) 내로 제한 — 전 종목 스캔 금지
    assert out["targets"] <= len(dah.EPISODE_CALENDAR["V_RECOVERY"]["symbols"]) + dah.MAX_LEADERS

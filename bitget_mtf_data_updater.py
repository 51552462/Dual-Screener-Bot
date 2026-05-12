import json
import os
import sqlite3
import gc
import time
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import ccxt
except ModuleNotFoundError:
    # 서버에 ccxt가 빠진 경우 최소 복구: 현재 파이썬 환경에 즉시 설치 재시도
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ccxt"])
    import ccxt
import pandas as pd
from bitget_config_hub import load_config as hub_load_config
from bitget_symbol_utils import normalize_table_symbol
from bitget_rate_limit_guard import throttle, backoff_sleep
from bitget_logger import setup_logging, get_logger


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
DB_WRITE_MAX_RETRIES = 8
DB_WRITE_BASE_SLEEP = 0.15
setup_logging()
logger = get_logger("bitget.mtf_updater")


def _mtf_fallback_defaults():
    """
    레거시 bitget_config.json와 동등한 블랭크 상태 기본값(단일 bitget_system_config.json 사용).
    """
    return {
        "exchange": "bitget",
        "default_quote": "USDT",
        "timeframes": ["1d", "4h", "2h", "1h"],
        "ohlcv_limit": 500,
        "universe": {
            "spot": {"enabled": True, "min_quote_volume_usdt": 3000000},
            "linear": {"enabled": True, "min_quote_volume_usdt": 5000000},
        },
        "parallel": {"max_workers_per_market_type": 8},
    }


def load_config():
    cfg = hub_load_config()
    if cfg:
        return cfg
    return _mtf_fallback_defaults()



def create_exchange(default_type: str):
    ex = ccxt.bitget(
        {
            "enableRateLimit": True,
            "options": {
                "defaultType": default_type,
            },
        }
    )
    ex.load_markets()
    return ex


def save_data_safely(conn, table_name, df):
    """
    무정지 원자 교체(Atomic Swap):
    1) 고유 임시 테이블에 전체 데이터를 먼저 적재
    2) BEGIN IMMEDIATE에서 본 테이블과 이름 스왑
    3) 실패 시 롤백 + 임시 잔재 제거
    """
    token = uuid.uuid4().hex[:8]
    temp_table = f"{table_name}__tmp_new_{token}"
    backup_table = f"{table_name}__tmp_old_{token}"

    # 사전 적재 단계: 본 테이블은 건드리지 않고 고유 tmp에 전체 작성
    conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
    df.to_sql(temp_table, conn, if_exists="replace", index=False)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f'DROP TABLE IF EXISTS "{backup_table}"')
        table_exists = (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            is not None
        )
        if table_exists:
            conn.execute(f'ALTER TABLE "{table_name}" RENAME TO "{backup_table}"')
        conn.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"')
        conn.execute(f'DROP TABLE IF EXISTS "{backup_table}"')
        conn.commit()
    except Exception:
        conn.rollback()
        conn.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        conn.execute(f'DROP TABLE IF EXISTS "{backup_table}"')
        raise


def _is_lock_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "database is locked" in msg or "database table is locked" in msg or "busy" in msg


def _run_with_db_retry(write_fn, op_desc: str):
    last_exc = None
    for attempt in range(DB_WRITE_MAX_RETRIES):
        try:
            return write_fn()
        except sqlite3.OperationalError as e:
            last_exc = e
            if not _is_lock_error(e):
                raise
            sleep_s = DB_WRITE_BASE_SLEEP * (attempt + 1)
            print(f"⏳ DB lock retry {attempt + 1}/{DB_WRITE_MAX_RETRIES} [{op_desc}] after {sleep_s:.2f}s")
            time.sleep(sleep_s)
        except Exception as e:
            last_exc = e
            raise
    if last_exc:
        raise last_exc


def normalize_symbol(symbol: str) -> str:
    return normalize_table_symbol(symbol)


def normalize_timeframe(tf: str) -> str:
    return tf.upper()


def table_name(market_type: str, symbol: str, timeframe: str) -> str:
    market_prefix = "SPOT" if market_type == "spot" else "FUT"
    return f"BITGET_{market_prefix}_{normalize_symbol(symbol)}_{normalize_timeframe(timeframe)}"


def _extract_quote_volume_usdt(ticker: dict) -> float:
    info = ticker.get("info") if isinstance(ticker, dict) else None
    if isinstance(info, dict):
        for key in ("quoteVolume", "usdtVolume", "baseVolume"):
            val = info.get(key)
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    for key in ("quoteVolume", "baseVolume"):
        val = ticker.get(key) if isinstance(ticker, dict) else None
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


def load_dynamic_universe(exchange, market_type: str, min_quote_volume_usdt: float, default_quote: str):
    throttle("bitget.fetch_tickers", 0.45)
    tickers = exchange.fetch_tickers()
    selected = []

    for symbol, market in exchange.markets.items():
        if not market.get("active", False):
            continue
        if market.get("quote") != default_quote:
            continue

        if market_type == "spot":
            if market.get("type") != "spot":
                continue
        else:
            # Bitget USDT-M perpetual contracts are linear swaps.
            if not market.get("linear", False):
                continue

        ticker = tickers.get(symbol, {})
        qv = _extract_quote_volume_usdt(ticker)
        if qv >= min_quote_volume_usdt:
            selected.append((symbol, qv))

    selected.sort(key=lambda x: x[1], reverse=True)
    return [sym for sym, _ in selected]


def _needs_synth_2h(timeframes):
    """Bitget REST는 2h 캔들을 지원하지 않음 → 1h로 합성 필요 여부."""
    return any(str(t).lower() == "2h" for t in (timeframes or []))


def _bitget_api_timeframes(timeframes):
    """API에 직접 요청할 TF만 반환(2h 제외). 2h가 필요하면 1h를 강제 포함."""
    out = []
    seen = set()
    for t in timeframes or []:
        tl = str(t).lower()
        if tl == "2h":
            continue
        if tl not in seen:
            seen.add(tl)
            out.append(t)
    if _needs_synth_2h(timeframes) and "1h" not in seen:
        out.append("1h")
    return out


def _resample_1h_ohlcv_to_2h(ohlcv_1h):
    """1h OHLCV([[ms,o,h,l,c,v],...]) → 2h OHLCV (Bitget 미지원 TF 방어)."""
    if not ohlcv_1h or len(ohlcv_1h) < 2:
        return []
    df = pd.DataFrame(ohlcv_1h, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = df.set_index("dt").sort_index()
    agg = (
        df[["Open", "High", "Low", "Close", "Volume"]]
        .resample("2h", label="left", closed="left")
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        .dropna(how="any", subset=["Open", "Close"])
    )
    out = []
    for idx, row in agg.iterrows():
        ts_ms = int(idx.timestamp() * 1000)
        out.append(
            [
                ts_ms,
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                float(row["Volume"]),
            ]
        )
    return out


def save_ohlcv(conn, market_type: str, symbol: str, timeframe: str, ohlcv):
    if not ohlcv:
        return False

    df = pd.DataFrame(ohlcv, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
    df["Date"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

    tbl = table_name(market_type, symbol, timeframe)
    time.sleep(0.03)  # DB 쓰기 간격을 벌려 잠금/버스트 완화
    _run_with_db_retry(lambda: save_data_safely(conn, tbl, df), f"{tbl}")
    del df
    gc.collect()
    return True


def _benchmark_symbols(exchange, market_type: str):
    out = []
    for base in ("BTC", "ETH"):
        cands = []
        if market_type == "spot":
            cands = [f"{base}/USDT"]
        else:
            cands = [f"{base}/USDT:USDT", f"{base}/USDT"]
        picked = None
        for s in cands:
            if s in exchange.markets:
                picked = s
                break
        if picked is None:
            # 폴백: 거래소 표기 차이 대응
            for s in exchange.markets.keys():
                if s.startswith(f"{base}/USDT"):
                    picked = s
                    break
        if picked:
            out.append(picked)
    return out


def fetch_and_store_benchmarks(market_type: str, timeframes, ohlcv_limit: int):
    exchange = create_exchange("spot" if market_type == "spot" else "swap")
    symbols = _benchmark_symbols(exchange, market_type)
    if not symbols:
        print(f"⚠️ [{market_type}] 벤치마크(BTC/ETH) 심볼 탐색 실패")
        return 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=7000;")
    ok = 0
    need_2h = _needs_synth_2h(timeframes)
    api_tfs = _bitget_api_timeframes(timeframes)
    try:
        for sym in symbols:
            ohlcv_by = {}
            for tf in api_tfs:
                try:
                    time.sleep(0.06)  # API ban 방어용 미세 지연
                    lim = ohlcv_limit
                    if str(tf).lower() == "1h" and need_2h:
                        lim = max(ohlcv_limit, min(ohlcv_limit * 2 + 32, 1500))
                    throttle("bitget.fetch_ohlcv", 0.10)
                    ohlcv = exchange.fetch_ohlcv(symbol=sym, timeframe=tf, limit=lim)
                    if ohlcv:
                        ohlcv_by[str(tf).lower()] = ohlcv
                    del ohlcv
                    gc.collect()
                except Exception as e:
                    print(f"⚠️ [{market_type}] 벤치마크 {sym} {tf} 수집 실패: {e}")
            for tf in timeframes:
                tl = str(tf).lower()
                try:
                    if tl == "2h":
                        h2 = _resample_1h_ohlcv_to_2h(ohlcv_by.get("1h") or [])
                        if len(h2) > ohlcv_limit:
                            h2 = h2[-ohlcv_limit:]
                        if save_ohlcv(conn, market_type, sym, tf, h2):
                            ok += 1
                    else:
                        ohlcv = ohlcv_by.get(tl)
                        if ohlcv and save_ohlcv(conn, market_type, sym, tf, ohlcv):
                            ok += 1
                except Exception as e:
                    print(f"⚠️ [{market_type}] 벤치마크 {sym} {tf} 저장 실패: {e}")
    finally:
        conn.close()
    return ok


def _enforce_benchmark_preload(timeframes, ohlcv_limit: int):
    """
    병렬 유니버스 스캔 전에 반드시 벤치마크를 동기 수집한다.
    - 대상: BTC/USDT, ETH/USDT
    - 범위: spot + linear, 모든 timeframe
    """
    preload_status = {}
    for market_type in ("spot", "linear"):
        ex = create_exchange("spot" if market_type == "spot" else "swap")
        symbols = _benchmark_symbols(ex, market_type)
        required = len(symbols) * len(timeframes)
        saved = fetch_and_store_benchmarks(market_type, timeframes, ohlcv_limit)
        ok = required > 0 and saved >= required
        preload_status[market_type] = {
            "ok": ok,
            "saved": saved,
            "required": required,
            "symbols": symbols,
        }
        if ok:
            print(f"🧪 [{market_type}] 벤치마크 선수집 완료: {saved}/{required} TF 저장")
        else:
            print(
                f"⛔ [{market_type}] 벤치마크 선수집 미완료: {saved}/{required} TF "
                f"(symbols={symbols}) -> 병렬 스캔 차단"
            )
    return preload_status


def fetch_symbol_ohlcv_payload(
    exchange,
    market_type: str,
    symbol: str,
    timeframes,
    ohlcv_limit: int,
):
    payloads = []
    need_2h = _needs_synth_2h(timeframes)
    api_tfs = _bitget_api_timeframes(timeframes)
    ohlcv_by = {}
    try:
        for tf in api_tfs:
            try:
                time.sleep(0.06)  # API ban 방어용 미세 지연
                lim = ohlcv_limit
                if str(tf).lower() == "1h" and need_2h:
                    lim = max(ohlcv_limit, min(ohlcv_limit * 2 + 32, 1500))
                throttle("bitget.fetch_ohlcv", 0.10)
                ohlcv = exchange.fetch_ohlcv(symbol=symbol, timeframe=tf, limit=lim)
                if ohlcv:
                    ohlcv_by[str(tf).lower()] = ohlcv
                    payloads.append((symbol, tf, ohlcv))
            except Exception as e:
                print(f"⚠️ [{market_type}] {symbol} {tf} 수집 실패: {e}")
                backoff_sleep(1)
        if need_2h:
            h2 = _resample_1h_ohlcv_to_2h(ohlcv_by.get("1h") or [])
            if h2:
                if len(h2) > ohlcv_limit:
                    h2 = h2[-ohlcv_limit:]
                payloads.append((symbol, "2h", h2))
    finally:
        gc.collect()
    return symbol, payloads


def run_mtf_update():
    config = load_config()
    os.makedirs(BASE_DIR, exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "charts"), exist_ok=True)

    timeframes = config.get("timeframes", ["1d", "4h", "2h", "1h"])
    ohlcv_limit = int(config.get("ohlcv_limit", 500))
    default_quote = str(config.get("default_quote", "USDT"))
    workers = int(config.get("parallel", {}).get("max_workers_per_market_type", 8))
    workers = max(1, min(workers, 5))  # 서버 안정성 기준 상한
    uni_cfg = config.get("universe", {})

    started = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"🚀 Bitget MTF 업데이트 시작: {started}")
    print(f"🧭 타임프레임: {timeframes}")
    preload_status = _enforce_benchmark_preload(timeframes, ohlcv_limit)

    for market_type in ("spot", "linear"):
        mcfg = uni_cfg.get(market_type, {})
        if not mcfg.get("enabled", True):
            print(f"⏭️ {market_type} 비활성화 - 스킵")
            continue
        if not preload_status.get(market_type, {}).get("ok", False):
            print(f"⏭️ [{market_type}] 벤치마크 선수집 실패로 병렬 수집 스킵")
            continue

        min_qv = float(mcfg.get("min_quote_volume_usdt", 0))
        ex = create_exchange("spot" if market_type == "spot" else "swap")
        universe = load_dynamic_universe(ex, market_type, min_qv, default_quote)
        # 💡 [버그 픽스] exchange 객체를 삭제하지 않고 워커들에 재사용하여 API 호출 폭주(IP 차단) 완벽 방어
        print(f"📦 [{market_type}] 거래대금 필터 통과: {len(universe)}개 (기준: {min_qv:,.0f} USDT)")

        if not universe:
            continue

        done = 0
        total_tables = 0
        writer_conn = sqlite3.connect(DB_PATH, timeout=60)
        writer_conn.execute("PRAGMA journal_mode=WAL;")
        writer_conn.execute("PRAGMA busy_timeout=7000;")
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(fetch_symbol_ohlcv_payload, ex, market_type, sym, timeframes, ohlcv_limit): sym
                    for sym in universe
                }
                for fut in as_completed(futures):
                    sym, payloads = fut.result()
                    ok_count = 0
                    for p_symbol, p_tf, p_ohlcv in payloads:
                        try:
                            if save_ohlcv(writer_conn, market_type, p_symbol, p_tf, p_ohlcv):
                                ok_count += 1
                        except Exception as e:
                            print(f"⚠️ [{market_type}] 순차 적재 실패 {p_symbol} {p_tf}: {e}")
                        finally:
                            del p_ohlcv
                            gc.collect()
                    done += 1
                    total_tables += ok_count
                    print(f"✅ [{market_type}] {done}/{len(universe)} {sym} -> {ok_count} TF 저장")
                    time.sleep(0.02)
                    gc.collect()
        finally:
            writer_conn.close()

        print(f"🏁 [{market_type}] 완료: 총 {total_tables}개 테이블 저장")
        del universe
        gc.collect()


if __name__ == "__main__":
    run_mtf_update()

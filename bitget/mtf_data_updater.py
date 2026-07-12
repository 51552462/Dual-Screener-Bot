import json
import os
import sqlite3
import time
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from bitget.infra.clock import utc_datetime_str_tz
from bitget.infra.gc_cycle import flush_gc
from bitget.infra.memory_policy import GC_AFTER_OHLCV_BATCH

try:
    import ccxt
except ModuleNotFoundError:
    # 서버에 ccxt가 빠진 경우 최소 복구: 현재 파이썬 환경에 즉시 설치 재시도
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ccxt"])
    import ccxt
import pandas as pd
from bitget.infra.data_paths import charts_dir, market_data_db_path
from bitget.infra.network_retry import call_with_retry
from bitget.infra.shared_db_connector import get_connection
from bitget.config_hub import load_config as hub_load_config
from bitget.symbol_utils import normalize_table_symbol
from bitget.infra.logging_setup import setup_logging, get_logger, log_exception


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = market_data_db_path()
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
    call_with_retry(
        lambda: (ex.load_markets() or True),
        op="mtf.load_markets",
        throttle_key="bitget.mtf.load_markets",
        throttle_interval_sec=0.5,
        swallow=False,
    )
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
            logger.warning(
                "DB lock retry %s/%s [%s] after %.2fs",
                attempt + 1,
                DB_WRITE_MAX_RETRIES,
                op_desc,
                sleep_s,
            )
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
    tickers = call_with_retry(
        lambda: exchange.fetch_tickers(),
        op="mtf.fetch_tickers",
        throttle_key="bitget.fetch_tickers",
        throttle_interval_sec=0.45,
        default=None,
        swallow=True,
    )
    if not isinstance(tickers, dict):
        logger.warning("[%s] fetch_tickers failed after retries — empty universe", market_type)
        return []
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
    flush_gc(label=GC_AFTER_OHLCV_BATCH)
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
        logger.warning("[%s] benchmark BTC/ETH symbol discovery failed", market_type)
        return 0
    conn = get_connection(DB_PATH)
    ok = 0
    need_2h = _needs_synth_2h(timeframes)
    api_tfs = _bitget_api_timeframes(timeframes)
    try:
        for sym in symbols:
            ohlcv_by = {}
            for tf in api_tfs:
                time.sleep(0.06)  # API ban 방어용 미세 지연
                lim = ohlcv_limit
                if str(tf).lower() == "1h" and need_2h:
                    lim = max(ohlcv_limit, min(ohlcv_limit * 2 + 32, 1500))
                ohlcv = call_with_retry(
                    lambda s=sym, t=tf, l=lim: exchange.fetch_ohlcv(
                        symbol=s, timeframe=t, limit=l
                    ),
                    op="mtf.fetch_ohlcv.benchmark",
                    throttle_key="bitget.fetch_ohlcv",
                    throttle_interval_sec=0.10,
                    default=None,
                    swallow=True,
                )
                if ohlcv:
                    ohlcv_by[str(tf).lower()] = ohlcv
                del ohlcv
                flush_gc(label=GC_AFTER_OHLCV_BATCH)
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
                    log_exception(
                        logger,
                        "[%s] benchmark store failed %s %s: %s",
                        market_type,
                        sym,
                        tf,
                        e,
                    )
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
            logger.info(
                "[%s] benchmark preload complete: %s/%s TF stored",
                market_type,
                saved,
                required,
            )
        else:
            logger.error(
                "[%s] benchmark preload incomplete: %s/%s TF (symbols=%s) — parallel scan blocked",
                market_type,
                saved,
                required,
                symbols,
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
            time.sleep(0.06)  # API ban 방어용 미세 지연
            lim = ohlcv_limit
            if str(tf).lower() == "1h" and need_2h:
                lim = max(ohlcv_limit, min(ohlcv_limit * 2 + 32, 1500))
            ohlcv = call_with_retry(
                lambda t=tf, l=lim: exchange.fetch_ohlcv(
                    symbol=symbol, timeframe=t, limit=l
                ),
                op="mtf.fetch_ohlcv",
                throttle_key="bitget.fetch_ohlcv",
                throttle_interval_sec=0.10,
                default=None,
                swallow=True,
            )
            if ohlcv:
                ohlcv_by[str(tf).lower()] = ohlcv
                payloads.append((symbol, tf, ohlcv))
        if need_2h:
            h2 = _resample_1h_ohlcv_to_2h(ohlcv_by.get("1h") or [])
            if h2:
                if len(h2) > ohlcv_limit:
                    h2 = h2[-ohlcv_limit:]
                payloads.append((symbol, "2h", h2))
    finally:
        flush_gc(label=GC_AFTER_OHLCV_BATCH)
    return symbol, payloads


def run_mtf_update():
    config = load_config()
    os.makedirs(charts_dir(), exist_ok=True)

    timeframes = config.get("timeframes", ["1d", "4h", "2h", "1h"])
    ohlcv_limit = int(config.get("ohlcv_limit", 500))
    default_quote = str(config.get("default_quote", "USDT"))
    workers = int(config.get("parallel", {}).get("max_workers_per_market_type", 8))
    workers = max(1, min(workers, 5))  # 서버 안정성 기준 상한
    uni_cfg = config.get("universe", {})

    started = utc_datetime_str_tz()
    logger.info("Bitget MTF update start: %s", started)
    logger.info("timeframes: %s", timeframes)
    preload_status = _enforce_benchmark_preload(timeframes, ohlcv_limit)

    for market_type in ("spot", "linear"):
        mcfg = uni_cfg.get(market_type, {})
        if not mcfg.get("enabled", True):
            logger.info("[%s] disabled — skip", market_type)
            continue
        if not preload_status.get(market_type, {}).get("ok", False):
            logger.warning(
                "[%s] benchmark preload failed — parallel fetch skipped",
                market_type,
            )
            continue

        min_qv = float(mcfg.get("min_quote_volume_usdt", 0))
        ex = create_exchange("spot" if market_type == "spot" else "swap")
        universe = load_dynamic_universe(ex, market_type, min_qv, default_quote)
        # 💡 [버그 픽스] exchange 객체를 삭제하지 않고 워커들에 재사용하여 API 호출 폭주(IP 차단) 완벽 방어
        logger.info(
            "[%s] quote-volume filter pass: %s symbols (min=%.0f USDT)",
            market_type,
            len(universe),
            min_qv,
        )

        if not universe:
            continue

        done = 0
        total_tables = 0
        writer_conn = get_connection(DB_PATH)
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
                            log_exception(
                                logger,
                                "[%s] sequential store failed %s %s: %s",
                                market_type,
                                p_symbol,
                                p_tf,
                                e,
                            )
                        finally:
                            del p_ohlcv
                            flush_gc(label=GC_AFTER_OHLCV_BATCH)
                    done += 1
                    total_tables += ok_count
                    logger.info(
                        "[%s] %s/%s %s -> %s TF stored",
                        market_type,
                        done,
                        len(universe),
                        sym,
                        ok_count,
                    )
                    time.sleep(0.02)
                    flush_gc(label=GC_AFTER_OHLCV_BATCH)
        finally:
            writer_conn.close()

        logger.info("[%s] complete: %s tables stored", market_type, total_tables)
        del universe
        flush_gc(label=GC_AFTER_OHLCV_BATCH)


if __name__ == "__main__":
    run_mtf_update()

"""
시장 장세(Market Regime) 판독 + 전략 콜로세움(청산 실적) 교차 요약 → system_config.json REGIME_ANALYSIS.
독립 위성: 매매 로직·DB 스키마 미변경 (forward_trades 읽기 전용).
"""
from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

CONFIG_PATH = os.path.join(
    os.path.expanduser("~"),
    "dante_bots",
    "Dual-Screener-Bot",
    "system_config.json",
)
DB_PATH = os.path.join(
    os.path.expanduser("~"),
    "dante_bots",
    "Dual-Screener-Bot",
    "market_data.sqlite",
)

VIX_HIGH = 20.0
NARROW_RANGE_PCT = 3.5  # 최근 5영업일 고저폭 / 종가 %
MA_WINDOW = 20


def load_config(max_retries: int = 5) -> Dict[str, Any]:
    """system_config.sqlite 우선, 레거시 JSON 보조."""
    try:
        from config_manager import load_system_config

        blob = load_system_config()
        if blob:
            return dict(blob)
    except Exception:
        pass
    if not os.path.exists(CONFIG_PATH):
        return {}
    for attempt in range(max_retries):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, PermissionError):
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
    return {}


def save_config(config_data: Dict[str, Any], max_retries: int = 5) -> bool:
    """REGIME_ANALYSIS 등 단일 키는 SQLite KV; 전체 dict 는 레거시 JSON 폴백."""
    ra = config_data.get("REGIME_ANALYSIS") if isinstance(config_data, dict) else None
    if isinstance(ra, dict):
        try:
            from config_manager import set_config_value

            set_config_value("REGIME_ANALYSIS", ra)
            return True
        except Exception:
            pass
    temp_path = f"{CONFIG_PATH}.temp"
    for attempt in range(max_retries):
        try:
            os.makedirs(os.path.dirname(CONFIG_PATH) or ".", exist_ok=True)
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, CONFIG_PATH)
            return True
        except PermissionError:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
        except Exception:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass
            return False
    return False


def _sleep_stealth() -> None:
    time.sleep(random.uniform(0.3, 0.7))


def _yf_history(symbol: str, period: str = "1mo") -> Optional[pd.DataFrame]:
    _sleep_stealth()
    if yf is None:
        return None
    try:
        return yf.Ticker(symbol).history(period=period, auto_adjust=True)
    except Exception:
        return None


def _snapshot_index(df: Optional[pd.DataFrame], name: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"symbol": name, "ok": False}
    if df is None or df.empty or "Close" not in df.columns:
        return out
    try:
        c = df["Close"].astype(float)
        hi = df["High"].astype(float) if "High" in df.columns else c
        lo = df["Low"].astype(float) if "Low" in df.columns else c
        last = float(c.iloc[-1])
        ma20 = float(c.rolling(MA_WINDOW, min_periods=10).mean().iloc[-1])
        tail = min(5, len(df))
        h5 = float(hi.iloc[-tail:].max())
        l5 = float(lo.iloc[-tail:].min())
        range5_pct = ((h5 - l5) / last * 100.0) if last > 0 else 0.0
        out.update(
            {
                "ok": True,
                "close": round(last, 4),
                "ma20": round(ma20, 4),
                "above_ma20": last > ma20,
                "range5d_pct": round(range5_pct, 3),
            }
        )
    except Exception:
        out["ok"] = False
    return out


def _classify_regime(
    spx: Dict[str, Any],
    ks: Dict[str, Any],
    vix_close: Optional[float],
) -> Tuple[str, str, str]:
    """
    반환: (regime_key, kr_us_regime 라벨, 한 줄 설명)
    regime_key: HIGH_VOL | BULL | BEAR | SIDEWAYS
    """
    vix_val = float(vix_close) if vix_close is not None and vix_close == vix_close else 99.0

    spx_ok = spx.get("ok")
    ks_ok = ks.get("ok")

    # 데이터 부족 시 보수적 라벨
    if not spx_ok and not ks_ok:
        return "UNKNOWN", "❓ 데이터 부족 (지수 조회 실패)", "SP500·KOSPI 시계열을 확보하지 못했습니다. 네트워크 또는 야후 파이낸스 지연을 확인하세요."

    narrow_spx = spx_ok and float(spx.get("range5d_pct", 99)) < NARROW_RANGE_PCT
    narrow_ks = ks_ok and float(ks.get("range5d_pct", 99)) < NARROW_RANGE_PCT
    narrow_both = narrow_spx and narrow_ks and spx_ok and ks_ok

    above_spx = bool(spx.get("above_ma20")) if spx_ok else False
    above_ks = bool(ks.get("above_ma20")) if ks_ok else False
    below_spx = spx_ok and not above_spx
    below_ks = ks_ok and not above_ks

    # 1) 고변동성: VIX 기준 (20 초과 = 고변동성)
    if vix_val > VIX_HIGH:
        label = "🌪️ 고변동성 발작장 (High Volatility)"
        note = (
            "휩소(가짜 신호)가 난무하기 쉽습니다. 돌파·단타(예: ema5) 계열 승률이 둔화되고, "
            "블랙홀(역행)·둠스데이(현금·방어) 필터가 상대적으로 유리한 구간으로 해석됩니다."
        )
        return "HIGH_VOL", label, note

    # 2) 횡보/박스: 양 지수 모두 좁은 고저폭
    if narrow_both:
        label = "📦 횡보·박스권 (Range / Chop)"
        note = (
            "최근 5영업일 고저폭이 SPX·KOSPI 모두 좁습니다. 추세 추종 전략은 비용·손절에 시달리기 쉬우며, "
            "박스 하단 매수·상단 매도형 전략과 소액 분할 대응이 상대적으로 유리할 수 있습니다."
        )
        return "SIDEWAYS", label, note

    # 3) 강세 / 4) 하락 — 두 지수 방향이 같을 때
    if spx_ok and ks_ok:
        if above_spx and above_ks:
            label = "🔥 강세장 · 안정적 상승장 (Bull Market)"
            note = (
                "S&P500·KOSPI 종가가 각각 20일선 위이고 VIX는 안정권입니다. 추세 추종·초신성·ema5 등 모멘텀 로직이 "
                "골디락스형으로 콜로세움 상단에 붙기 쉬운 장세입니다."
            )
            return "BULL", label, note
        if below_spx and below_ks:
            label = "🧊 하락장 · 늪지대 (Choppy Bear)"
            note = (
                "양 지수 모두 20일선 아래입니다. 다수 롱 전략이 불리할 수 있으며, 눌림목(nulrim) 등 짧은 구간만 노리고 "
                "현금·방어 비중을 높이는 편이 유리할 수 있습니다."
            )
            return "BEAR", label, note

    # 5) 혼재 → 횡보·방향성 탐색
    label = "⚖️ 횡보·방향성 탐색 (Sideways)"
    note = (
        "지수 간 MA20 위·아래가 엇갈리거나 박스 내에서 방향을 고르는 구간입니다. "
        "승률·합산수익 상위 소수 로직에만 비중을 축소해 편승하고, 과도한 신규 노출은 자제하는 편이 안전합니다."
    )
    return "SIDEWAYS", label, note


def _core_sig(sig: Any) -> str:
    clean_sig = re.sub(r"\[.*?\]", "", str(sig)).strip()
    return clean_sig if clean_sig else str(sig).replace("[", "").replace("]", "").strip()


def fetch_colosseum_summary(limit_days: int = 45, top_n: int = 4) -> Tuple[str, List[Dict[str, Any]]]:
    """
    forward_trades 읽기 전용 집계. DB 스키마 변경 없음.
    """
    rows_out: List[Dict[str, Any]] = []
    summary_line = "콜로세움 교차: 청산 표본 부족 또는 DB 미가동."
    since = (datetime.now() - timedelta(days=limit_days)).strftime("%Y-%m-%d")

    try:
        uri = f"file:{DB_PATH.replace(os.sep, '/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        try:
            df = pd.read_sql(
                """
                SELECT sig_type, final_ret FROM forward_trades
                WHERE status LIKE 'CLOSED%' AND final_ret IS NOT NULL
                  AND exit_date >= ?
                """,
                conn,
                params=(since,),
            )
        finally:
            conn.close()
    except Exception:
        return summary_line, rows_out

    if df is None or df.empty:
        return summary_line, rows_out

    df = df.copy()
    df["_sig"] = df["sig_type"].astype(str)
    df = df.loc[~df["_sig"].str.contains("INCUBATOR", na=False)].copy()
    df["logic"] = df["sig_type"].apply(_core_sig)
    df = df.loc[df["logic"].str.len() > 0].copy()
    df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce")
    df = df.dropna(subset=["final_ret"])

    agg = []
    for logic, g in df.groupby("logic"):
        fr = g["final_ret"]
        if fr.empty:
            continue
        agg.append(
            {
                "logic": str(logic)[:120],
                "n": int(len(fr)),
                "sum_ret": round(float(fr.sum()), 4),
                "mean_ret": round(float(fr.mean()), 4),
            }
        )

    if not agg:
        return summary_line, rows_out

    agg.sort(key=lambda x: x["sum_ret"], reverse=True)
    top = agg[:top_n]
    rows_out = top

    parts = [f"{r['logic'][:40]}: 합산 {r['sum_ret']:+.2f}% (n={r['n']})" for r in top[:3]]
    summary_line = "최근 청산 기준 상위 기여: " + " | ".join(parts)
    return summary_line, rows_out


def _blend_meta_insight(regime_key: str, base_note: str, colosseum_line: str, top_rows: List[Dict[str, Any]]) -> str:
    """규칙 기반 메타 코멘트 + 콜로세움 실측 한 줄 결합."""
    lines = [base_note.strip(), "", colosseum_line]

    if top_rows:
        best = top_rows[0]
        worst = min(top_rows, key=lambda x: x["sum_ret"]) if len(top_rows) > 1 else top_rows[0]
        lines.append(
            f"실측 기준 선두 로직은 「{best['logic'][:60]}」(합산 {best['sum_ret']:+.2f}%). "
            f"현재 장세 레짐({regime_key})과 엇박일 경우 다음 리밸런싱에서 비중 축소를 검토하세요."
        )
        if worst["sum_ret"] < 0:
            lines.append(
                f"동일 구간 하위 기여 「{worst['logic'][:50]}」({worst['sum_ret']:+.2f}%)은 레짐과 상관 없이 구조적 개선이 필요할 수 있습니다."
            )

    return "\n".join(lines)


def analyze_market_regime() -> None:
    print("🧠 [메타 분석기] 글로벌 장세(Market Regime) 판독 시작...")
    try:
        if yf is None:
            print("⚠️ yfinance 미설치")
            return

        # 스텔스 (요청 스니펫과 동일 대역)
        time.sleep(random.uniform(0.5, 1.2))

        df_spx = _yf_history("^GSPC", "1mo")
        _sleep_stealth()
        df_ks = _yf_history("^KS11", "1mo")
        _sleep_stealth()
        df_vix = _yf_history("^VIX", "1mo")

        spx_snap = _snapshot_index(df_spx, "^GSPC")
        ks_snap = _snapshot_index(df_ks, "^KS11")

        vix_close = None
        if df_vix is not None and not df_vix.empty and "Close" in df_vix.columns:
            vix_close = float(df_vix["Close"].astype(float).iloc[-1])

        regime_key, regime_name, regime_note = _classify_regime(spx_snap, ks_snap, vix_close)

        colosseum_line, top_rows = fetch_colosseum_summary()
        meta_full = _blend_meta_insight(regime_key, regime_note, colosseum_line, top_rows)

        cfg = load_config()
        cfg["REGIME_ANALYSIS"] = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "regime_key": regime_key,
            "kr_us_regime": regime_name,
            "meta_insight": meta_full,
            "vix_close": round(vix_close, 4) if vix_close is not None else None,
            "vix_high_threshold": VIX_HIGH,
            "narrow_range_pct_threshold": NARROW_RANGE_PCT,
            "indices": {
                "GSPC": spx_snap,
                "KOSPI": ks_snap,
                "VIX": {"close": round(vix_close, 4) if vix_close is not None else None},
            },
            "colosseum_cross_summary": colosseum_line,
            "top_strategies_recent": top_rows,
        }

        if save_config(cfg):
            print(f"✅ [판독 완료] 현재 장세: {regime_name}")
        else:
            print("⚠️ REGIME_ANALYSIS 저장 실패")
    except Exception as e:
        print(f"⚠️ 장세 판독 중 오류: {e}")


if __name__ == "__main__":
    analyze_market_regime()

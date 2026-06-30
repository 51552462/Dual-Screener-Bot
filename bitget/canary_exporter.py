"""
코인 선행 레이더 — `bitget_canary_state.json` 익스포터 (코인→주식 파일 브릿지).

[목적] 24×7 코인 시장의 '유동성/변동성 스트레스'를 산출해, 주식 국면 엔진이
read-only JSON 으로 **DB 락 없이** 흡수하도록 한다(주식 시스템 무결합).

산출 2지표:
  1) crypto_liquidity_stress (0.0~1.0)
     - 거래대금 상위 5 알트 선물(BTC 제외, 동적 선정)의 OI 24h 변화율 + 평균 펀딩비.
     - OI 급감 AND 펀딩 음수가 '동시'일 때만 높게(기하평균) → 스마트머니 극단 디리스킹.
  2) macro_contagion_risk (bool)
     - BTC 3일 수익률이 급락 AND (VIX 3일 상승 OR 둠스데이 악화)일 때만 True.
     - 코인 단독 악재(코인만 빠지고 거시 잠잠)를 걸러낸다.

설계 원칙: 절대 예외를 밖으로 던지지 않는다(파이프라인 tail 에서 무해). 모든 외부
호출은 best-effort, 결측 시 안전한 0/False 로 수렴.

상세 설계: docs/코인_선행레이더_canary_익스포터_설계_및_기대영향.md
"""
from __future__ import annotations

import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ── 산출 파라미터 (shadow 관찰 후 보정 전제) ──────────────────────────────
TOP_N_ALTS = 5
OI_DROP_REF = 0.15          # OI 24h -15% 면 oi_drop=1.0
FUND_THRESH = -0.0001       # 평균 펀딩 -0.01% 부터 디리스킹 가산 시작
FUND_REF = 0.0004           # 그로부터 -0.04%p 더 내려가면 funding_neg=1.0
BTC_DROP_THRESH = -0.07     # BTC 3일 -7% 이하면 급락
OI_HISTORY_MAX_AGE_SEC = 26 * 3600
OI_LOOKBACK_TARGET_SEC = 24 * 3600
OI_LOOKBACK_TOLERANCE_SEC = 3 * 3600   # 24h±3h 범위의 과거 포인트로 비교

# ── 비-크립토 네이티브 자산 제외(코인 유동성 스트레스 신호 오염원) ────────────
# 거래대금 상위 동적 선정에 끼어들면 신호를 왜곡하므로 base 기준으로 거른다.
#   · 금속/원자재: 위험회피 자산 → 코인 급락 시 역방향 → 스트레스 희석
#   · 토큰화 주식/지수/RWA: 주식과 동조 → "코인으로 주식 예측"의 순환논리(circularity)
#   · 스테이블/법정화폐: 변동 신호 없음
# 운영 중 새 RWA 가 상위에 끼면 CANARY_EXCLUDE_BASES="FOO,BAR" 로 코드수정 없이 확장.
_NON_CRYPTO_BASES = frozenset({
    "XAU", "XAG", "XPT", "XPD", "XAUT", "PAXG",          # 귀금속 / 금토큰(RWA)
    "WTI", "OIL", "USOIL", "UKOIL", "BRENT",             # 원유
    "NGAS", "NATGAS", "COPPER",                          # 기타 원자재
    "USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE",      # 스테이블
    "USDD", "PYUSD", "USD1", "EUR", "GBP", "JPY",        # 스테이블 / 법정화폐
    "SPCX",                                              # 사전IPO·토큰화 주식(예: SpaceX)
})


def _excluded_bases() -> frozenset:
    """기본 비-크립토 denylist + 환경변수 확장(CANARY_EXCLUDE_BASES, 콤마구분)."""
    extra = (os.environ.get("CANARY_EXCLUDE_BASES") or "").strip()
    if not extra:
        return _NON_CRYPTO_BASES
    add = {b.strip().upper() for b in extra.split(",") if b.strip()}
    return _NON_CRYPTO_BASES | add


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


# ---------------------------------------------------------------------------
# ccxt 공개 거래소 핸들 (비인증)
# ---------------------------------------------------------------------------
_pub_ex = None


def _ex():
    global _pub_ex
    try:
        import ccxt
    except Exception:
        return None
    if _pub_ex is None:
        try:
            _pub_ex = ccxt.bitget({"enableRateLimit": True, "options": {"defaultType": "swap"}})
            _pub_ex.load_markets()
        except Exception:
            _pub_ex = None
    return _pub_ex


# ---------------------------------------------------------------------------
# 1) 동적 알트 선정 — 24h 거래대금(quoteVolume) 상위 N (BTC 제외, USDT 선형 무기한)
# ---------------------------------------------------------------------------
def _top_alt_swaps(n: int = TOP_N_ALTS) -> List[str]:
    ex = _ex()
    if ex is None:
        return []
    try:
        from bitget.rate_limit_guard import throttle

        throttle("bitget.pub.fetch_tickers", 0.2)
        tickers = ex.fetch_tickers(params={"type": "swap"})
    except Exception:
        try:
            tickers = ex.fetch_tickers()
        except Exception:
            return []

    rows: List[Tuple[str, float]] = []
    markets = getattr(ex, "markets", {}) or {}
    excluded = _excluded_bases()
    for sym, t in (tickers or {}).items():
        try:
            m = markets.get(sym) or {}
            # USDT 선형 무기한 스왑만, BTC·비-크립토(금속/원자재/토큰화주식/스테이블) 제외
            if not (m.get("swap") and m.get("linear") and str(m.get("quote")) == "USDT"):
                continue
            base = str(m.get("base") or "").upper()
            if not base or base == "BTC" or base in excluded:
                continue
            qv = t.get("quoteVolume")
            if qv is None and isinstance(t.get("info"), dict):
                qv = t["info"].get("quoteVolume") or t["info"].get("usdtVolume")
            qv_f = float(qv or 0.0)
            if qv_f > 0:
                rows.append((sym, qv_f))
        except (TypeError, ValueError):
            continue

    rows.sort(key=lambda r: r[1], reverse=True)
    return [s for s, _ in rows[:n]]


# ---------------------------------------------------------------------------
# 2) OI 총합 + 24h 변화율 (링버퍼 차분 + 거래소 OI 히스토리 폴백 — 콜드스타트 제거)
# ---------------------------------------------------------------------------
def _last_prices(symbols: List[str]) -> Dict[str, float]:
    """심볼별 최근가(USDT). OI 가 base 수량만 줄 때 명목가치(USDT) 환산에 사용."""
    out: Dict[str, float] = {}
    ex = _ex()
    if ex is None or not symbols:
        return out
    tickers: Dict[str, Any] = {}
    try:
        from bitget.rate_limit_guard import throttle

        throttle("bitget.pub.fetch_tickers", 0.2)
        tickers = ex.fetch_tickers(symbols) or {}
    except Exception:
        tickers = {}
    for s in symbols:
        t = tickers.get(s) if isinstance(tickers, dict) else None
        t = t or {}
        px = t.get("last") or t.get("close") or t.get("mark") or t.get("markPrice")
        if px is None and isinstance(t.get("info"), dict):
            px = t["info"].get("lastPr") or t["info"].get("markPrice") or t["info"].get("last")
        try:
            if px is not None and float(px) > 0:
                out[s] = float(px)
        except (TypeError, ValueError):
            continue
    return out


def _oi_value_from_dict(o: Any, fallback_px: Optional[float] = None) -> Optional[float]:
    """ccxt OI(또는 OI 히스토리) 엔트리에서 명목가치(USDT) 추출.

    우선순위: openInterestValue → openInterestAmount(base수량) × 가격.
    가격은 엔트리 내 markPrice → 호출부가 넘긴 티커 fallback_px 순. (bitget 은 OI 응답에
    가격을 주지 않으므로 fallback_px 가 사실상 필수.)
    """
    if not isinstance(o, dict):
        return None
    val = o.get("openInterestValue")
    if val is None:
        amt = o.get("openInterestAmount")
        info = o.get("info") if isinstance(o.get("info"), dict) else {}
        px: Optional[float] = None
        try:
            cand = o.get("markPrice") or info.get("markPrice") or info.get("price")
            px = float(cand) if cand is not None and float(cand) > 0 else None
        except (TypeError, ValueError):
            px = None
        if px is None and fallback_px is not None and fallback_px > 0:
            px = float(fallback_px)
        if amt is not None and px:
            try:
                val = float(amt) * float(px)
            except (TypeError, ValueError):
                val = None
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _current_oi_total_usdt(
    symbols: List[str], prices: Optional[Dict[str, float]] = None
) -> Optional[float]:
    ex = _ex()
    if ex is None or not symbols:
        return None
    prices = prices or {}
    total = 0.0
    got = 0
    for sym in symbols:
        try:
            from bitget.rate_limit_guard import throttle

            throttle("bitget.pub.fetch_open_interest", 0.15)
            o = ex.fetch_open_interest(sym)
        except Exception:
            continue
        val = _oi_value_from_dict(o, prices.get(sym))
        if val is not None and val > 0:
            total += val
            got += 1
    return total if got > 0 else None


def _oi_change_via_history(
    symbols: List[str], prices: Optional[Dict[str, float]] = None
) -> Tuple[Optional[float], Optional[float]]:
    """폴백: 거래소 OI 히스토리(1h, ~24h)로 (현재총합, 24h변화율)을 즉시 산출.

    ccxt 가 해당 거래소에서 OI 히스토리를 지원할 때만 작동(예: 일부 거래소). bitget 은
    fetchOpenInterestHistory 미지원 → (None, None) 반환하고 링버퍼 경로가 24h 변화를 맡는다.
    """
    ex = _ex()
    if ex is None or not symbols:
        return None, None
    has = getattr(ex, "has", {}) or {}
    if not has.get("fetchOpenInterestHistory"):
        return None, None
    prices = prices or {}
    now_total = 0.0
    past_total = 0.0
    now_got = 0
    pair_got = 0
    for sym in symbols:
        try:
            from bitget.rate_limit_guard import throttle

            throttle("bitget.pub.fetch_open_interest_history", 0.2)
            hist = ex.fetch_open_interest_history(sym, "1h", limit=25)
        except Exception:
            continue
        if not isinstance(hist, list) or len(hist) < 2:
            continue
        latest = _oi_value_from_dict(hist[-1], prices.get(sym))
        # ~24h 전: limit=25(=24h+1) 의 맨 앞 포인트. 결측이면 다음으로 가까운 과거 포인트.
        past = None
        for entry in hist[:-1]:
            v = _oi_value_from_dict(entry, prices.get(sym))
            if v is not None and v > 0:
                past = v
                break
        if latest is not None and latest > 0:
            now_total += latest
            now_got += 1
        if latest is not None and latest > 0 and past is not None and past > 0:
            past_total += past
            pair_got += 1
    now_out = now_total if now_got > 0 else None
    if pair_got == 0 or past_total <= 0 or now_total <= 0:
        return now_out, None
    return now_out, (now_total / past_total - 1.0)


def _oi_change_pct_24h(current_total: Optional[float]) -> Optional[float]:
    """링버퍼에 현재 총 OI 를 적재하고, ~24h 전 포인트와 비교해 변화율 반환.

    cold start(24h 전 데이터 없음) 면 None → 호출부에서 oi_drop=0(안전) 처리.
    """
    if current_total is None or current_total <= 0:
        return None
    try:
        from bitget.infra.data_paths import canary_oi_history_path

        path = canary_oi_history_path()
        now = time.time()
        hist: List[Dict[str, float]] = []
        if os.path.isfile(path):
            try:
                with open(path, encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, list):
                    hist = [r for r in raw if isinstance(r, dict) and "ts" in r and "oi" in r]
            except (OSError, ValueError):
                hist = []

        # 24h 전(±tolerance)에 가장 가까운 과거 포인트
        target = now - OI_LOOKBACK_TARGET_SEC
        past_val: Optional[float] = None
        best_gap = OI_LOOKBACK_TOLERANCE_SEC + 1
        for r in hist:
            gap = abs(float(r["ts"]) - target)
            if gap <= OI_LOOKBACK_TOLERANCE_SEC and gap < best_gap:
                best_gap = gap
                past_val = float(r["oi"])

        # 현재 포인트 적재 + 노후 정리
        hist.append({"ts": now, "oi": float(current_total)})
        hist = [r for r in hist if now - float(r["ts"]) <= OI_HISTORY_MAX_AGE_SEC]
        tmp = f"{path}.tmp.{os.getpid()}"
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(hist, f)
        os.replace(tmp, path)

        if past_val is None or past_val <= 0:
            return None
        return current_total / past_val - 1.0
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 3) 평균 펀딩비 (기존 funding_fetcher 재사용)
# ---------------------------------------------------------------------------
def _avg_funding(symbols: List[str]) -> Optional[float]:
    if not symbols:
        return None
    try:
        from bitget.funding_fetcher import fetch_funding_snapshot
    except Exception:
        return None
    rates: List[float] = []
    for sym in symbols:
        base = sym.split(":")[0].replace("/", "_")  # 'ETH/USDT:USDT' → 'ETH_USDT'
        try:
            snap = fetch_funding_snapshot(base)
        except Exception:
            snap = None
        if isinstance(snap, dict) and snap.get("funding_rate") is not None:
            try:
                rates.append(float(snap["funding_rate"]))
            except (TypeError, ValueError):
                continue
    if not rates:
        return None
    return sum(rates) / len(rates)


# ---------------------------------------------------------------------------
# 4) BTC 3일 수익률
# ---------------------------------------------------------------------------
def _btc_ret_3d() -> Optional[float]:
    ex = _ex()
    if ex is None:
        return None
    try:
        from bitget.rate_limit_guard import throttle

        throttle("bitget.pub.fetch_ohlcv", 0.15)
        ohlcv = ex.fetch_ohlcv("BTC/USDT:USDT", timeframe="1d", limit=4)
    except Exception:
        try:
            ohlcv = ex.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=4)
        except Exception:
            return None
    if not ohlcv or len(ohlcv) < 4:
        return None
    try:
        close_now = float(ohlcv[-1][4])
        close_3d = float(ohlcv[-4][4])
        if close_3d <= 0:
            return None
        return close_now / close_3d - 1.0
    except (TypeError, ValueError, IndexError):
        return None


# ---------------------------------------------------------------------------
# 5) 거시 위험 방향 (VIX 상승 OR 둠스데이 악화) — 파일 기반 우선
# ---------------------------------------------------------------------------
def _macro_up() -> Tuple[bool, Optional[float]]:
    """반환: (macro_up, vix_3d_change). 이미 코인 폴더에 미러된 둠스데이 상태 우선."""
    vix_3d: Optional[float] = None

    # (a) 기존 둠스데이 미러(주식→코인) 활용: DEFCON 악화 = 위험 상승
    try:
        from bitget.infra.data_paths import bitget_data_dir

        dd_path = os.path.join(bitget_data_dir(), "bitget_doomsday_status.json")
        if os.path.isfile(dd_path):
            with open(dd_path, encoding="utf-8") as f:
                dd = json.load(f)
            if isinstance(dd, dict):
                metrics = dd.get("metrics") if isinstance(dd.get("metrics"), dict) else {}
                for k in ("vix_3d_change", "vix_change_3d", "vix_chg_3d"):
                    if metrics.get(k) is not None:
                        vix_3d = float(metrics.get(k))
                        break
                level = dd.get("defcon_level")
                if level is not None and int(level) <= 3:
                    return True, vix_3d  # 둠스데이 경보 = 거시 위험 상승
    except Exception:
        pass

    # (b) 폴백: yfinance ^VIX 3일 변화 직접 조회
    if vix_3d is None:
        try:
            import yfinance as yf

            h = yf.Ticker("^VIX").history(period="10d", auto_adjust=False)
            if h is not None and not h.empty and "Close" in h.columns:
                c = h["Close"].astype(float).dropna()
                if len(c) >= 4:
                    vix_3d = float(c.iloc[-1] / c.iloc[-4] - 1.0)
        except Exception:
            vix_3d = None

    macro_up = vix_3d is not None and vix_3d > 0.0
    return macro_up, vix_3d


# ---------------------------------------------------------------------------
# 산출 + 원자적 기록
# ---------------------------------------------------------------------------
def compute_canary_state() -> Dict[str, Any]:
    symbols = _top_alt_swaps(TOP_N_ALTS)
    prices = _last_prices(symbols)  # OI base수량 → USDT 명목가치 환산용

    oi_total = _current_oi_total_usdt(symbols, prices)
    oi_change = _oi_change_pct_24h(oi_total)  # 1순위: 자체 링버퍼 차분(파일 기반, 무DB)
    # [P1] null/0 또는 콜드스타트(링버퍼 24h 미충족) → OI 히스토리 지원 거래소면 즉시 산출.
    oi_change_source = "ring_buffer" if oi_change is not None else None
    if oi_total is None or oi_total <= 0 or oi_change is None:
        hist_total, hist_change = _oi_change_via_history(symbols, prices)
        if (oi_total is None or oi_total <= 0) and hist_total is not None:
            oi_total = hist_total
        if oi_change is None and hist_change is not None:
            oi_change = hist_change
            oi_change_source = "oi_history_24h"
    avg_funding = _avg_funding(symbols)
    btc_ret_3d = _btc_ret_3d()
    macro_up, vix_3d = _macro_up()

    # 유동성 스트레스: 두 성분의 기하평균 (둘 다 극단일 때만 0.8↑)
    oi_drop = _clip01((-oi_change) / OI_DROP_REF) if oi_change is not None else 0.0
    funding_neg = (
        _clip01((FUND_THRESH - avg_funding) / FUND_REF) if avg_funding is not None else 0.0
    )
    stress = math.sqrt(oi_drop * funding_neg)

    # 상관관계 역전(전염): BTC 급락 AND 거시 위험 상승 동기화
    contagion = bool(
        btc_ret_3d is not None and btc_ret_3d <= BTC_DROP_THRESH and macro_up
    )

    return {
        "schema": "bitget_canary.v1",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "crypto_liquidity_stress": round(float(stress), 4),
        "macro_contagion_risk": contagion,
        "components": {
            "oi_total_usdt": round(float(oi_total), 2) if oi_total is not None else None,
            "oi_total_24h_change_pct": round(float(oi_change), 4) if oi_change is not None else None,
            "oi_change_source": oi_change_source,
            "avg_funding_rate": round(float(avg_funding), 6) if avg_funding is not None else None,
            "btc_ret_3d": round(float(btc_ret_3d), 4) if btc_ret_3d is not None else None,
            "vix_3d_change": round(float(vix_3d), 4) if vix_3d is not None else None,
            "macro_up": bool(macro_up),
            "oi_drop_component": round(float(oi_drop), 4),
            "funding_neg_component": round(float(funding_neg), 4),
            "symbols_used": symbols,
        },
        "source": "bitget_canary_exporter",
    }


def _safe_print(msg: str) -> None:
    """일부 콘솔(cp949 등)에서 이모지 인코딩 실패로 tail 훅이 죽지 않도록 방어."""
    try:
        print(msg)
    except Exception:
        try:
            print(msg.encode("ascii", "replace").decode("ascii"))
        except Exception:
            pass


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def run_canary_export() -> Dict[str, Any]:
    """파이프라인 tail 훅 진입점. 예외는 절대 밖으로 던지지 않는다(무해)."""
    try:
        from bitget.infra.data_paths import canary_state_path

        state = compute_canary_state()
        path = canary_state_path()
        _atomic_write_json(path, state)
        _safe_print(
            f"🛰️ [Canary] stress={state['crypto_liquidity_stress']} "
            f"contagion={state['macro_contagion_risk']} "
            f"syms={state['components']['symbols_used']} -> {path}"
        )
        return {"ok": True, "path": path, "state": state}
    except Exception as ex:  # noqa: BLE001 — tail 훅: 실패해도 파이프라인 진행
        _safe_print(f"⚠️ [Canary] export skipped: {ex}")
        return {"ok": False, "error": str(ex)}


if __name__ == "__main__":
    import pprint

    pprint.pprint(run_canary_export())

import math
import re
import sqlite3
import pandas as pd
import numpy as np
import os
import sys
import json
import subprocess
import time
import random
from datetime import datetime, timedelta
import pytz
import requests
import yfinance as yf
import FinanceDataReader as fdr
import warnings
import logging
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

import telegram_env

from yf_download_flatten import flatten_yf_download_df, yf_close_series

# ==========================================
# 💡 [환경 설정]
# ==========================================
TELEGRAM_TOKEN_MAIN = telegram_env.get_report_token()
TELEGRAM_CHAT_ID = telegram_env.get_report_chat_id()
from factory_data_paths import market_data_db_path

DB_PATH = market_data_db_path()
from config_manager import load_system_config as load_system_config_kv
from inverse_etf_sniper import run_inverse_etf_sniper_cycle
from shadow_tracking import record_ops_snapshot_from_live_state
import ops_logger
from system_config_atomic import (
    CONFIG_PATH,
    config_persisted,
    load_config,
    save_config,
    update_config,
)

LOOKBACK_DAYS = 14
SMOOTHING_ALPHA = 0.3 
WARMUP_DAYS = 14

# ==========================================
# 💡 [유틸리티 함수]
# ==========================================
def send_telegram_report(message) -> bool:
    """주간/일일 리포트 등 HTML 텔레그램. 성공 True — 실패 시 터미널에 이유 출력."""
    token = (telegram_env.get_report_token() or TELEGRAM_TOKEN_MAIN or "").strip()
    chat_id = (telegram_env.get_report_chat_id() or TELEGRAM_CHAT_ID or "").strip()
    if not token or not chat_id:
        print(
            "텔레그램 전송 스킵: REPORT_BOT_TOKEN(또는 TELEGRAM_TOKEN_MAIN) / "
            "REPORT_BOT_CHAT_ID(또는 TELEGRAM_CHAT_ID) 미설정 — .env 로드 여부 확인"
        )
        logger.warning("telegram skip: missing token or chat_id")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code == 200:
            print(f"텔레그램 발송 OK (chars={len(message)})")
            return True
        print(
            f"텔레그램 API 실패: HTTP {resp.status_code} — "
            f"{(resp.text or '')[:300]}"
        )
        logger.warning("telegram API error: %s %s", resp.status_code, resp.text[:200])
        return False
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")
        logger.warning("telegram post exception: %s", e)
        return False


# 1분 주기: ops_snapshot + 인버스 스나이퍼 (모노토닉 시계)
_OPS_INVERSE_MINUTE_STATE = {"last_mono": 0.0, "inverse_mode": None}


def _telegram_inverse_sniper_critical_only(summary: dict, mode_after: bool) -> None:
    """
    인버스 스나이퍼 관련 텔레그램: 모드 전환·킬스위치 청산·테일 캡/잔액 거부만.
    (스캔 완료·정상 스킵·진입 성공은 발송하지 않음)
    """
    st = _OPS_INVERSE_MINUTE_STATE
    prev_mode = st["inverse_mode"]
    if prev_mode is not None and mode_after != prev_mode:
        send_telegram_report(
            f"🛡️ <b>[인버스 관제]</b> INVERSE_MODE_ACTIVE 변경: "
            f"<code>{prev_mode}</code> → <code>{mode_after}</code>"
        )
    kc = int(summary.get("kill_closed") or 0)
    if kc > 0:
        send_telegram_report(
            f"🚨 <b>[인버스 킬 스위치]</b> 잔존 OPEN 인버스 <b>{kc}</b>건 시장가 청산 후 테일 반환."
        )
    sk = summary.get("skipped")
    if isinstance(sk, str) and sk and (
        "테일 30% 캡" in sk or "Reserve OCC" in sk or "테일 잔액 0" in sk
    ):
        send_telegram_report(
            f"🚨 <b>[인버스 테일 캡/잔액]</b> 신규 진입 거부: {sk}"
        )
    st["inverse_mode"] = mode_after


def _gemini_status_for_ops() -> dict:
    try:
        import ai_overseer as ao

        fn = getattr(ao, "gemini_heartbeat_snapshot", None)
        if callable(fn):
            return dict(fn())
    except Exception:
        pass
    return {"phase": "unknown"}


def _minute_ops_snapshot_and_inverse_cycle() -> None:
    """국고/테일·OPEN 롱숏 스냅샷 1행 + 인버스 스나이퍼 1사이클 (텔레그램은 치명 이벤트만)."""
    mono = time.monotonic()
    st = _OPS_INVERSE_MINUTE_STATE
    if mono - st["last_mono"] < 60.0:
        return
    st["last_mono"] = mono
    try:
        summary = run_inverse_etf_sniper_cycle()
    except Exception as e:
        print(f"⚠️ [오토파일럿] run_inverse_etf_sniper_cycle 실패: {e}")
        summary = {"kill_closed": 0, "skipped": None, "entered": None}
    try:
        record_ops_snapshot_from_live_state()
    except Exception as e:
        print(f"⚠️ [오토파일럿] ops_snapshot 기록 실패: {e}")
    try:
        mode_after = bool(load_system_config_kv().get("INVERSE_MODE_ACTIVE"))
        _telegram_inverse_sniper_critical_only(summary, mode_after)
    except Exception as e:
        print(f"⚠️ [오토파일럿] 인버스 텔레그램 게이트 실패: {e}")
    try:
        from telegram_message_queue import count_all_pending_messages

        m: dict = {
            "telegram_queue_pending": int(count_all_pending_messages()),
            "gemini": _gemini_status_for_ops(),
        }
        try:
            from gemini_report_cache import get_gemini_gate_metrics

            m.update(get_gemini_gate_metrics())
        except Exception:
            pass
        ops_logger.record_gauge_snapshot("system_auto_pilot", m)
        ops_logger.record_heartbeat("system_auto_pilot")
    except Exception as e:
        print(f"⚠️ [오토파일럿] ops_events gauge 실패: {e}")

def load_or_create_config():
    # 1. 파일이 아예 없을 때 처음 생성하는 기본값 세팅
    if not config_persisted():
        default_config = {
            "ACTIVE_EXIT_MODE": "HYBRID",
            "WEIGHT_S1": 1.0, "WEIGHT_S4": 1.0,
            "ACCOUNT_SIZE": 20000000,         # 💡 각 로직별 기본 시드 2,000만 원
            "RISK_PCT": 0.02,                 # 💡 고정 리스크 2%
            "CENTRAL_TREASURY_KR": 300000000, # 🏦 [추가] 한국장 초기 국고 3억 원
            "CENTRAL_TREASURY_US": 300000000, # 🏦 [추가] 미국장 초기 국고 3억 원
            "TAIL_RISK_FUND_KR": 0.0,
            "TAIL_RISK_FUND_US": 0.0,
            "GLOBAL_CIRCUIT_BREAKER": "OFF",
            "ARCHIVED_TEMPLATES": {},
            "ANTI_PATTERNS": [],
            "INCUBATOR_TEMPLATES": {},
            "INVERSE_MODE_ACTIVE": False,
        }
        save_config(default_config)
        return default_config
        
    # 2. 기존 파일이 있을 때 읽어오기
    config = load_config()
        
    # 💡 [국고 자동 입금 로직] 기존 파일에 국고(Treasury) 데이터가 없다면 알아서 3억씩 채워줍니다.
    need_save = False
    if "CENTRAL_TREASURY_KR" not in config:
        config["CENTRAL_TREASURY_KR"] = 300000000  # 3억 원
        need_save = True
    if "CENTRAL_TREASURY_US" not in config:
        config["CENTRAL_TREASURY_US"] = 300000000  # 3억 원
        need_save = True
    if "TAIL_RISK_FUND_KR" not in config:
        config["TAIL_RISK_FUND_KR"] = 0.0
        need_save = True
    if "TAIL_RISK_FUND_US" not in config:
        config["TAIL_RISK_FUND_US"] = 0.0
        need_save = True
    if "GLOBAL_CIRCUIT_BREAKER" not in config:
        config["GLOBAL_CIRCUIT_BREAKER"] = "OFF"
        need_save = True
    if "ARCHIVED_TEMPLATES" not in config or not isinstance(config.get("ARCHIVED_TEMPLATES"), dict):
        config["ARCHIVED_TEMPLATES"] = {}
        need_save = True
    if "ANTI_PATTERNS" not in config or not isinstance(config.get("ANTI_PATTERNS"), list):
        config["ANTI_PATTERNS"] = []
        need_save = True
    if "INCUBATOR_TEMPLATES" not in config or not isinstance(config.get("INCUBATOR_TEMPLATES"), dict):
        config["INCUBATOR_TEMPLATES"] = {}
        need_save = True
    if "INVERSE_MODE_ACTIVE" not in config:
        config["INVERSE_MODE_ACTIVE"] = False
        need_save = True

    if "META_GOVERNOR_WINDOWS" not in config or not isinstance(config.get("META_GOVERNOR_WINDOWS"), dict):
        config["META_GOVERNOR_WINDOWS"] = {
            "calibrator_lookback_days": 90,
            "treasury_lookback_days": 90,
            "graveyard_rolling_days": 90,
            "dist_lookback_days": 90,
        }
        need_save = True
    if "META_GOVERNOR_SKIP_VIX" not in config:
        config["META_GOVERNOR_SKIP_VIX"] = False
        need_save = True

    if "STRATEGY_LIFECYCLE" not in config or not isinstance(config.get("STRATEGY_LIFECYCLE"), dict):
        from strategy_lifecycle_config import DEFAULT_STRATEGY_LIFECYCLE

        config["STRATEGY_LIFECYCLE"] = dict(DEFAULT_STRATEGY_LIFECYCLE)
        need_save = True

    if "DEATHMATCH" not in config or not isinstance(config.get("DEATHMATCH"), dict):
        from evolution.deathmatch_config import DEFAULT_DEATHMATCH

        config["DEATHMATCH"] = dict(DEFAULT_DEATHMATCH)
        need_save = True

    # 변경 사항이 있으면 JSON 파일에 덮어쓰기
    if need_save:
        save_config(config)
        print("🏦 [국고 세팅 완료] 시스템에 한국 3억, 미국 3억의 초기 자본이 성공적으로 세팅되었습니다.")
        
    return config


def _sync_inverse_mode_switch(current_config: dict, vix_last=0.0, regime_display=""):
    """
    폭락·신용경색 레짐과 연동해 INVERSE_MODE_ACTIVE를 갱신한다.
    - DOOMSDAY_DEFCON.level <= 2
    - regime_meta_analyzer가 기록한 REGIME_ANALYSIS.regime_key ∈ {HIGH_VOL, BEAR}
    - 자율 관제탑 로컬 판정: VIX 극단 + '극단적 공포장' 문자열
    (save_config와 동일 배치로 쓰여 원자적 일관성 유지)
    """
    dd = current_config.get("DOOMSDAY_DEFCON") or {}
    try:
        lvl = int(dd.get("level", 99))
    except (TypeError, ValueError):
        lvl = 99
    defcon_crash = lvl <= 2

    meta = current_config.get("REGIME_ANALYSIS") or {}
    rk = str(meta.get("regime_key") or "").strip().upper()
    meta_crash = rk in ("HIGH_VOL", "BEAR")

    try:
        vx = float(vix_last or 0.0)
    except (TypeError, ValueError):
        vx = 0.0
    vix_crash = vx >= 28.0 and "극단적" in str(regime_display)

    current_config["INVERSE_MODE_ACTIVE"] = bool(defcon_crash or meta_crash or vix_crash)


def get_first_entry_date():
    """forward_trades 장부의 최초 진입일(MIN(entry_date))을 조회한다."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        row = conn.execute("SELECT MIN(entry_date) FROM forward_trades").fetchone()
        conn.close()
        if row and row[0]:
            return datetime.strptime(row[0], '%Y-%m-%d').date()
    except Exception as e:
        print(f"최초 거래일 조회 에러: {e}")
    return None


def _mean_entry_shape_incubator_winners(promoted_name: str):
    """인큐베이터 승격 대상 로직의 forward_trades 승리 청산 건 entry_date 시점 종가로 20차원 shape 평균 역추적 (try_add와 동일 정규화·20분할)."""
    tag_sub = f"[INCUBATOR_{promoted_name}]"
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        wins = pd.read_sql(
            """SELECT market, code, entry_date FROM forward_trades
               WHERE sig_type LIKE ? AND status LIKE 'CLOSED%' AND final_ret > 0""",
            conn,
            params=(f"%{tag_sub}%",),
        )
        conn.close()
    except Exception:
        return None
    if wins is None or wins.empty:
        return None
    shapes = []
    for _, row in wins.iterrows():
        mkt = str(row.get("market") or "KR").upper()
        code = str(row.get("code") or "").strip()
        entry_d = row.get("entry_date")
        if not code or entry_d is None or (isinstance(entry_d, float) and pd.isna(entry_d)):
            continue
        try:
            end_dt = pd.Timestamp(str(entry_d))
            end_str = end_dt.strftime("%Y-%m-%d")
        except Exception:
            continue
        st_dt = (end_dt - pd.Timedelta(days=450)).strftime("%Y-%m-%d")
        try:
            if mkt == "KR":
                hist = fdr.DataReader(code, st_dt, end_str)
            else:
                hist = yf.download(
                    code,
                    start=st_dt,
                    end=(end_dt + pd.Timedelta(days=2)).strftime("%Y-%m-%d"),
                    progress=False,
                )
                hist = flatten_yf_download_df(hist)
            if hist is None or hist.empty or "Close" not in hist.columns:
                continue
            hist = hist.sort_index()
            hist.index = pd.to_datetime(hist.index).tz_localize(None)
            hist = hist.loc[: pd.Timestamp(end_str)]
            c = hist["Close"].astype(float).values
            if len(c) < 60:
                continue
            c = c[-300:]
            if len(c) < 60:
                continue
            c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
            new_shape = np.mean(np.array_split(c_norm, 20), axis=1)
            if new_shape.size != 20:
                continue
            shapes.append(np.nan_to_num(new_shape.astype(float)))
        except Exception:
            continue
    if not shapes:
        return None
    stacked = np.vstack(shapes)
    return [round(float(x), 6) for x in np.mean(stacked, axis=0)]


def _meta_governor_skip_vix_from_config(cfg) -> bool:
    if not isinstance(cfg, dict):
        return False
    v = cfg.get("META_GOVERNOR_SKIP_VIX")
    if v is True or v == 1:
        return True
    if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes", "on"):
        return True
    return False


# ==========================================
# 🚀 [메인 분석 엔진] 
# ==========================================
def run_autonomous_analysis():
    print(f"🚀 [자율 관제탑] 거시 경제(VIX) 기반 동적 룩백 윈도우 스캔 시작...")
    
    # ---------------------------------------------------------
    # 👑 [사전 작업] 변동성(VIX & KOSPI) 기반 동적 룩백(Lookback) 결정
    # ---------------------------------------------------------
    dyn_lookback = 14 # 기본값
    vix_status = "데이터 없음"
    regime = "분석 중"
    w_s1, w_s4 = 1.0, 1.0 
    vix_last = 0.0
    breadth_ratio = 1.0
    breadth_status = "미계산"

    early_cfg = load_or_create_config()
    skip_vix = os.environ.get("META_GOVERNOR_SKIP_VIX", "").strip().lower() in ("1", "true", "yes", "on") or _meta_governor_skip_vix_from_config(early_cfg)

    if skip_vix:
        dyn_lookback = 30
        vix_status = "META_GOVERNOR_SKIP_VIX — VIX 미조회, 보수 룩백 30일"
        vix_last = 0.0
        regime = "Bear/Chop (VIX 스킵/오프라인)"
        w_s1, w_s4 = 0.7, 1.3
    else:
        try:
            # 💡 [V24.0] SPY(시총비중), ^VIX(공포), RSP(동일비중) 데이터 동시 로드
            df_idx = yf.download("SPY ^VIX RSP", period="1y", interval="1d", group_by="ticker", progress=False)

            def _bench_close(panel, sym):
                s = yf_close_series(panel, sym)
                if not getattr(s, "empty", True):
                    return s.dropna()
                try:
                    if sym in panel.columns.levels[0]:
                        return panel[sym]['Close'].dropna()
                    return panel['Close'][sym].dropna()
                except Exception:
                    return pd.Series(dtype=float)

            spy_c = _bench_close(df_idx, 'SPY')
            vix_c = _bench_close(df_idx, '^VIX')
            rsp_c = _bench_close(df_idx, 'RSP')

            spy_last, vix_last = spy_c.iloc[-1], vix_c.iloc[-1]
            spy_ema200 = spy_c.ewm(span=200, adjust=False).mean().iloc[-1]

            # 💡 [핵심] 시장 폭(Breadth) 계산: (현재 RSP/SPY 비율) / (50일 평균 RSP/SPY 비율)
            breadth_ratio = (rsp_c.iloc[-1] / spy_c.iloc[-1]) / (rsp_c.rolling(50).mean().iloc[-1] / spy_c.rolling(50).mean().iloc[-1])

            # 1. 기본 국면 및 비중 설정 (지수 위치 기준)
            if spy_last > spy_ema200 and vix_last < 18:
                regime = "Bull (상승장)"
                base_w1, base_w4 = 1.2, 0.8
            else:
                regime = "Bear/Chop (하락/횡보)"
                base_w1, base_w4 = 0.5, 1.5

            # 2. 🚨 [V24.0 핵심] 시장 폭에 따른 비중 패널티/보너스 (지수 착시 방어)
            breadth_status = "건강 (Broad)"
            if breadth_ratio < 0.97:
                breadth_status = "취약 (Narrow/쏠림)"
                base_w1 *= 0.5
                base_w4 *= 1.2
            elif breadth_ratio > 1.03:
                breadth_status = "강력 (확산)"
                base_w1 *= 1.2

            w_s1, w_s4 = round(base_w1, 2), round(base_w4, 2)

            # 3. VIX 기반 동적 룩백 설정 결합
            if vix_last >= 28.0:
                dyn_lookback = 7
                regime = "Bear (극단적 공포장)"
                w_s1, w_s4 = 0.0, 2.0
                vix_status = f"VIX 폭발({vix_last:.1f}) | 폭:{breadth_ratio:.2f}({breadth_status}) - 룩백 7일"
            elif vix_last >= 18.0:
                dyn_lookback = 15
                vix_status = f"VIX 경계({vix_last:.1f}) | 폭:{breadth_ratio:.2f}({breadth_status}) - 룩백 15일"
            else:
                dyn_lookback = 45
                vix_status = f"VIX 평온({vix_last:.1f}) | 폭:{breadth_ratio:.2f}({breadth_status}) - 룩백 45일"

        except Exception as e:
            print(f"거시 지표 로드 에러: {e}")

    # 1. 계산된 [동적 룩백]으로 DB 데이터 로드
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        start_date = (datetime.now() - timedelta(days=dyn_lookback)).strftime('%Y-%m-%d')
        query = f"SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND entry_date >= '{start_date}'"
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"DB 로드 에러: {e}")
        return

    if len(df) < 10:
        # 표본 부족이어도 관제탑 핵심 키는 갱신해 UNKNOWN 뇌사 상태를 방지
        current_config = load_or_create_config()
        # 💡 [100년 영속 진화 로직 적용: Tail Risk Convexity Treasury Shield]
        try:
            for _mkt in ["KR", "US"]:
                t_key = f"CENTRAL_TREASURY_{_mkt}"
                f_key = f"TAIL_RISK_FUND_{_mkt}"
                treasury = float(current_config.get(t_key, 0.0) or 0.0)
                fund = float(current_config.get(f_key, 0.0) or 0.0)
                # 💡 [100년 영속 진화 로직 적용: Tail Fund Target Cap Guard]
                target_fund = max(0.0, treasury * 0.015)
                transfer = max(0.0, target_fund - fund)
                transfer = min(transfer, treasury)
                treasury -= transfer
                fund += transfer
                if float(vix_last) >= 35.0 and fund > 0:
                    treasury += fund * 30.0
                    fund = 0.0
                current_config[t_key] = round(max(0.0, treasury), 2)
                current_config[f_key] = round(max(0.0, fund), 2)
        except Exception:
            pass
        regime_key = "BULL" if "Bull" in regime else ("BEAR" if "극단적" in regime else "CHOP")
        optimal_risk = 0.01
        current_config["CURRENT_REGIME_KEY"] = regime_key
        current_config["DYNAMIC_KELLY_RISK"] = round(optimal_risk, 4)
        _sync_inverse_mode_switch(current_config, vix_last, regime)
        save_config(current_config)
        send_telegram_report(f"⚠️ <b>[자율 관제탑]</b>\n\n거시 국면 전환으로 룩백이 {dyn_lookback}일로 조정되었으나, 해당 기간 내 청산 표본이 10건 미만입니다. 기본 관제탑 키(CURRENT_REGIME_KEY/DYNAMIC_KELLY_RISK=1.00%)만 선반영하고 이번 주 조율을 스킵합니다.")
        return

    current_config = load_or_create_config()
    # 💡 [100년 영속 진화 로직 적용: Tail Risk Convexity Treasury Shield]
    try:
        for _mkt in ["KR", "US"]:
            t_key = f"CENTRAL_TREASURY_{_mkt}"
            f_key = f"TAIL_RISK_FUND_{_mkt}"
            treasury = float(current_config.get(t_key, 0.0) or 0.0)
            fund = float(current_config.get(f_key, 0.0) or 0.0)
            # 💡 [100년 영속 진화 로직 적용: Tail Fund Target Cap Guard]
            target_fund = max(0.0, treasury * 0.015)
            transfer = max(0.0, target_fund - fund)
            transfer = min(transfer, treasury)
            treasury -= transfer
            fund += transfer
            # VIX 공황장: 테일 리스크 펀드 30배 수익 시뮬레이션 후 국고 복원, 펀드 리셋
            if float(vix_last) >= 35.0 and fund > 0:
                payoff = fund * 30.0
                treasury += payoff
                report_tail = f"▪️ {_mkt} 테일리스크 펀드 발동: {fund:,.0f}원 ×30 => {payoff:,.0f}원 국고 복원"
                fund = 0.0
            else:
                report_tail = f"▪️ {_mkt} 테일리스크 적립: {transfer:,.0f}원 (누적 {fund:,.0f}원 / 목표 {target_fund:,.0f}원)"
            current_config[t_key] = round(max(0.0, treasury), 2)
            current_config[f_key] = round(max(0.0, fund), 2)
            # report_lines 선언 전이라 임시 변수로 보관
            if _mkt == "KR":
                _tail_msg_kr = report_tail
            else:
                _tail_msg_us = report_tail
    except Exception:
        _tail_msg_kr, _tail_msg_us = None, None
    try:
        from meta_governor_consumer import apply_meta_weight_bounds_clamp, load_meta_state_resolved

        w_s1, w_s4 = apply_meta_weight_bounds_clamp(float(w_s1), float(w_s4), load_meta_state_resolved())
    except Exception:
        pass
    current_config["WEIGHT_S1"] = round(float(w_s1), 4)
    current_config["WEIGHT_S4"] = round(float(w_s4), 4)
    
    report_lines = [f"<b>📊 [System B 자율 조율 리포트]</b>\n"]
    report_lines.append(f"<b>[1. 동적 거시 국면 판독 (Regime)]</b>\n▪️ 상태: {regime}\n▪️ <b>동적 룩백: {vix_status}</b>\n🚨 <b>액션:</b> S1 비중 {w_s1}배 / S4 비중 {w_s4}배 강제 조율\n")
    if '_tail_msg_kr' in locals() and _tail_msg_kr:
        report_lines.append(_tail_msg_kr)
    if '_tail_msg_us' in locals() and _tail_msg_us:
        report_lines.append(_tail_msg_us)

    # ---------------------------------------------------------
    # 👑 엔진 1.6: 미국장 고MFE 섹터 기반 글로벌 스필오버 저장
    # ---------------------------------------------------------
    report_lines.append("<b>[1.6 글로벌 스필오버 자동 연동]</b>")
    try:
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        us_hot_df = df[
            (df['market'] == 'US') &
            (df['status'].str.contains('CLOSED', na=False)) &
            (df['entry_date'] >= seven_days_ago) &
            (df['mfe'] >= 15.0)
        ] if all(col in df.columns for col in ['market', 'status', 'entry_date', 'mfe', 'sector']) else pd.DataFrame()

        if not us_hot_df.empty:
            top_us_sector = us_hot_df.groupby('sector').size().sort_values(ascending=False).index[0]
            sector_s = str(top_us_sector)
            as_of = datetime.now().strftime("%Y-%m-%d")
            current_config["US_SPILLOVER_SECTOR"] = sector_s
            current_config["US_SPILLOVER_SECTOR_LAST_GOOD"] = sector_s
            current_config["US_SPILLOVER_SECTOR_AS_OF"] = as_of
            report_lines.append(f"▪️ 최근 7일 미국장 고MFE(15%+) 주도 섹터 저장: <b>{top_us_sector}</b>")
        else:
            report_lines.append("▪️ 최근 7일 미국장 고MFE 섹터 표본 부족으로 기존 스필오버 섹터 유지")
            # 명시 방어: 표본 0이어도 LAST_GOOD / AS_OF / 기존 US_SPILLOVER_SECTOR 를 삭제·덮어쓰지 않음
            _lg = current_config.get("US_SPILLOVER_SECTOR_LAST_GOOD")
            _as = current_config.get("US_SPILLOVER_SECTOR_AS_OF")
            if _lg is not None:
                current_config["US_SPILLOVER_SECTOR_LAST_GOOD"] = str(_lg)
            if _as is not None:
                current_config["US_SPILLOVER_SECTOR_AS_OF"] = str(_as)
    except Exception as e:
        report_lines.append(f"▪️ 글로벌 스필오버 저장 에러: {e}")

    # ---------------------------------------------------------
    # 🛡️ 엔진 1.7.5: [V45.0 DNA 변위(Drift) 감지 선제적 방어막]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[1.7.5 DNA 변위 기반 선제적 국면 검증]</b>")
    dna_drift_warning = False
    
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        # 최근 진입한 10개 종목의 DNA 매칭 성적과 참사주 일치도 데이터 로드
        recent_dna_df = pd.read_sql("SELECT entry_cos_score, entry_dtw_score FROM forward_trades ORDER BY id DESC LIMIT 10", conn)
        conn.close()
        
        if len(recent_dna_df) >= 5:
            avg_alpha_sim = recent_dna_df['entry_cos_score'].mean()
            # 💡 핵심 로직: 지수가 BULL(상승장)이어도 종목들의 대장주 DNA 일치율이 
            # 갑자기 60% 밑으로 떨어지거나, 참사주 냄새가 짙어지면 'DNA 변위'로 간주
            if avg_alpha_sim < 0.65:
                dna_drift_warning = True
                report_lines.append(f"🚨 <b>[DNA 변위 감지]</b> 지수는 상승장이나 포착 종목의 대장주 일치율이 {avg_alpha_sim*100:.1f}%로 급감했습니다.")
                report_lines.append("⚠️ <b>조치:</b> 지수 판독 결과를 무시하고 '방어(CHOP)' 모드로 선제 전환합니다.")
    except Exception as e:
        logger.error(f"비치명적 에러 발생: {e}", exc_info=True)

    # 국면 판독 결과 강제 보정 (지수보다 DNA 우선)
    if dna_drift_warning and "Bull" in regime:
        regime = "Chop (DNA 변위로 인한 선제적 방어)"
        w_s1, w_s4 = 0.5, 1.2 # 공격 비중 강제 축소
    try:
        from meta_governor_consumer import apply_meta_weight_bounds_clamp, load_meta_state_resolved

        w_s1, w_s4 = apply_meta_weight_bounds_clamp(float(w_s1), float(w_s4), load_meta_state_resolved())
    except Exception:
        pass
    current_config["WEIGHT_S1"] = round(float(w_s1), 4)
    current_config["WEIGHT_S4"] = round(float(w_s4), 4)
    # 👆👆 [V45.0 엔진 끝] 👆👆

    # ---------------------------------------------------------
    # 👑 엔진 1.8: [V32.0 국면별 독립 기억소(Regime Memory) 로드]
    # ---------------------------------------------------------
    regime_key = "BULL" if "Bull" in regime else ("BEAR" if "극단적" in regime else "CHOP")
    last_analysed_regime = current_config.get("LAST_ANALYSED_REGIME", "")

    if last_analysed_regime != regime_key:
        report_lines.append(f"\n🔄 <b>[V32.0 국면 전환 감지]</b> {last_analysed_regime} ➔ {regime_key}")
        
        # 💾 과거 해당 국면의 챔피언 파라미터 뭉치 로드 (Zero-Lag)
        regime_memory = current_config.get(f"{regime_key}_CHAMPION_PARAMS", {})
        if regime_memory:
            for k, v in regime_memory.items(): current_config[k] = v
            report_lines.append(f"💾 <b>[기억소 로드]</b> 과거 {regime_key} 국면의 황금 파라미터를 즉시 복구했습니다.")
        
        current_config["LAST_ANALYSED_REGIME"] = regime_key

    # ---------------------------------------------------------
    # 👑 엔진 1.9: [V39.0 국면별 데이터 기반 켈리 베팅(Kelly Criterion) 도출]
    # ---------------------------------------------------------
    report_lines.append(f"\n<b>[1.9 {regime_key} 국면 최적 켈리(Kelly) 베팅 사이즈 조율]</b>")
    
    # 1. 내 장부에서 '현재와 동일한 국면(Regime)'에 진입했던 청산 종목들만 추출
    regime_df = df[df['entry_regime'] == regime_key] if 'entry_regime' in df.columns else df
    
    if len(regime_df) >= 10: # 데이터가 충분할 때만 켈리 공식 가동
        r_wins = regime_df[regime_df['final_ret'] > 0]
        r_loses = regime_df[regime_df['final_ret'] <= 0]
        
        r_win_rate = len(r_wins) / len(regime_df)
        r_pf = r_wins['final_ret'].sum() / (abs(r_loses['final_ret'].sum()) + 0.1)
        
        # 2. 켈리 공식 적용: f = W - (1-W)/R (안전성을 위해 Half-Kelly 적용)
        if r_pf > 0:
            kelly_fraction = r_win_rate - ((1 - r_win_rate) / r_pf)
            half_kelly = kelly_fraction / 2.0
            # 3. 리스크 허용 범위 강제 바운딩 (최소 0.2% ~ 최대 3.0%)
            optimal_risk = max(0.002, min(0.030, half_kelly * 0.1)) # 자본 보존을 위해 스케일 다운
        else:
            optimal_risk = 0.002 # 승률/손익비가 박살난 상태면 0.2% 극방어 모드
            
        report_lines.append(f"▪️ {regime_key} 과거 성적: 승률 {r_win_rate*100:.1f}% | PF {r_pf:.2f}")
        report_lines.append(f"💡 <b>수학적 최적 리스크(Half-Kelly): 계좌의 {optimal_risk*100:.2f}% (동적 스케일링)</b>")
    else:
        # 데이터가 부족하면 인간의 뇌피셜이 아닌, 가장 보수적인 베이스라인(1.0%) 적용
        optimal_risk = 0.01
        report_lines.append(f"▪️ 표본 부족으로 안전 베이스라인(1.0%) 적용")

    current_config["DYNAMIC_KELLY_RISK"] = round(optimal_risk, 4)
    current_config["CURRENT_REGIME_KEY"] = regime_key

    # ---------------------------------------------------------
    # 👑 엔진 2: 점수 티어 및 초정밀 필터 검증
    # ---------------------------------------------------------
    # (... 기존 엔진 2 코드 그대로 이어짐 ...)
    report_lines.append("<b>[2. 필터 및 티어 승률 검증]</b>")
    t1_wr, t1_pf = calculate_metrics(df[df['total_score'] >= 80])
    sub_wr, sub_pf = calculate_metrics(df[(df['total_score'] >= 50) & (df['total_score'] < 80)])
    report_lines.append(f"▪️ 1티어(80점↑): 승률 {t1_wr:.1f}% | PF {t1_pf:.2f}")
    report_lines.append(f"▪️ 서브(50~79점): 승률 {sub_wr:.1f}% | PF {sub_pf:.2f}")
    
    if 'is_death_combo' in df.columns:
        dc_wr, dc_pf = calculate_metrics(df[df['is_death_combo'] == 1])
        report_lines.append(f"▪️ 데스콤보 타점: 승률 {dc_wr:.1f}% (낮을수록 정상 방어 중)\n")

    # ---------------------------------------------------------
    # 👑 엔진 2.5: [R&D 실험실] 돌연변이 승자 에너지 기반 오버드라이브 허들 동기화
    # ---------------------------------------------------------
    report_lines.append("<b>[2.5 R&D 돌연변이 에너지 ➔ 오버드라이브 허들 연동]</b>")
    try:
        rnd_pool = df.sort_values('exit_date').tail(60) if 'exit_date' in df.columns else df.tail(60)
        rnd_winners = rnd_pool[rnd_pool['final_ret'] > 0] if 'final_ret' in rnd_pool.columns else pd.DataFrame()
        if not rnd_winners.empty and 'v_energy' in rnd_winners.columns:
            avg_rnd_energy = float(rnd_winners['v_energy'].dropna().mean())
            if not np.isnan(avg_rnd_energy):
                current_config["DYNAMIC_OD_HURDLE"] = round(avg_rnd_energy, 2)
                report_lines.append(f"▪️ R&D 승리 돌연변이 평균 응축 에너지: {avg_rnd_energy:.2f}")
                report_lines.append(f"✅ <b>동기화:</b> DYNAMIC_OD_HURDLE = {current_config['DYNAMIC_OD_HURDLE']}")
            else:
                report_lines.append("▪️ R&D 승자 에너지 계산 불가(결측)로 기존 OD 허들 유지")
        else:
            report_lines.append("▪️ R&D 승자 표본 부족으로 기존 OD 허들 유지")
    except Exception as e:
        report_lines.append(f"▪️ R&D 허들 동기화 에러: {e}")

    # ---------------------------------------------------------
    # 👑 엔진 3: 날것(Raw) 파라미터 스무딩 (베이지안 업데이트)
    # ---------------------------------------------------------
    report_lines.append("<b>[3. 네임스페이스 스무딩 (진입점 교정)]</b>")
    kr_s1_df = df[(df['market'] == 'KR') & (df['sig_type'].str.contains('S1'))]
    winners_rs = kr_s1_df[kr_s1_df['final_ret'] > 0]['v_rs'].dropna()
    
    if len(winners_rs) >= 5:
        raw_new_rs = np.percentile(winners_rs, 25) 
        old_rs = current_config.get("KR_S1_RS_CUTOFF", 165.0)
        smoothed_rs = round((old_rs * (1 - SMOOTHING_ALPHA)) + (raw_new_rs * SMOOTHING_ALPHA), 2)
        current_config["KR_S1_RS_CUTOFF"] = smoothed_rs
        report_lines.append(f"▪️ KR_S1_RS: {old_rs} ➔ <b>{smoothed_rs}</b> (새 파동 30% 스며듦)\n")
    else:
        report_lines.append("▪️ 표본 부족으로 진입점 스무딩 스킵\n")

    # ---------------------------------------------------------
    # 👑 엔진 4 ~ 6: [V51.0 다중 뇌(Multi-Brain) 자율 분할 최적화 엔진] 
    # ---------------------------------------------------------
    # 1. 종목별 출신 성분(Namespace) 매핑 함수
    def map_namespace(row): 
        m = row['market']
        st = str(row['sig_type']) 
        
        # 👇👇 [수정] 9대 퀀트 팩토리 모든 실무자 독립 뇌 완벽 분리 👇👇
        if "SUPERNOVA_BEAST" in st: return f"{m}_SUPERNOVA_BEAST_MASTER"
        elif "UNDERDOG" in st: return f"{m}_UNDERDOG_MASTER"
        elif "SUPERNOVA" in st: return f"{m}_SUPERNOVA_MASTER"
        elif "역매공파" in st or "역배열" in st: return f"{m}_REVERSE_MASTER"
        elif "밥그릇" in st: return f"{m}_BOWL_MASTER"
        elif "눌림" in st: return f"{m}_NULRIM_S4" if "S4" in st else f"{m}_NULRIM_S1"
        elif "5선" in st: return f"{m}_5EMA_S1"
        
        ns = f"{m}_MASTER_S1" # 기본값 
        if "S4" in st: ns = f"{m}_MASTER_S4" 
        return ns

    if 'market' in df.columns and 'sig_type' in df.columns:
        df['namespace'] = df.apply(map_namespace, axis=1)
    else:
        df['namespace'] = "KR_MASTER_S1" # Fail-safe

    unique_namespaces = df['namespace'].unique()

    report_lines.append(f"\n🧠 <b>[V51.0 다중 뇌(Multi-Brain) 분할 최적화 가동]</b>\n발견된 독립 전략 방: {', '.join(unique_namespaces)}")
    oos_barrier = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')

    # 💡 [V_NEXT 진화 로직 적용] 네임스페이스별 톰슨 샘플링 리스크를 수집해 글로벌 켈리에 합성
    ns_sampled_risks = []

    for target_ns in unique_namespaces:
        ns_df = df[df['namespace'] == target_ns].copy()
        if len(ns_df) < 5: continue # 해당 전략의 표본 부족 시 스킵
        
        report_lines.append(f"\n=========================================")
        report_lines.append(f"🧬 <b>[{target_ns} 전용 뇌수술 진행]</b> (표본: {len(ns_df)}개)")

        # 💡 [100년 영속 진화 로직 적용: Namespace Thompson Beta Params Harvest]
        try:
            cut_30 = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            ns_recent_30 = ns_df[ns_df['entry_date'] >= cut_30] if 'entry_date' in ns_df.columns else ns_df
            wins_30 = int((ns_recent_30['final_ret'] > 0).sum()) if 'final_ret' in ns_recent_30.columns else 0
            losses_30 = int((ns_recent_30['final_ret'] <= 0).sum()) if 'final_ret' in ns_recent_30.columns else 0
            current_config[f"{target_ns}_BETA_PARAMS"] = {
                "alpha": wins_30,
                "beta": losses_30,
                "updated_at": datetime.now().strftime('%Y-%m-%d')
            }
            report_lines.append(
                f"▪️ TS 베타 파라미터 저장: {target_ns} -> α={wins_30}, β={losses_30} (최근 30일)"
            )
        except Exception as _beta_e:
            report_lines.append(f"▪️ TS 베타 파라미터 저장 스킵: {_beta_e}")
        
        # --- [엔진 4: 독립 앙상블 생성] ---
        def get_period_stats(train_days):
            s_date = (datetime.now() - timedelta(days=14 + train_days)).strftime('%Y-%m-%d')
            p_df = ns_df[(ns_df['entry_date'] >= s_date) & (ns_df['entry_date'] < oos_barrier)].copy()
            n_trades = len(p_df)
            
            if n_trades < 5:
                if n_trades > 0 and (len(p_df[p_df['final_ret'] > 0]) == 0 or p_df['final_ret'].mean() <= -3.0): return "TOXIC"
                return None

            win_s = p_df[p_df['final_ret'] > 0]
            lose_s = p_df[p_df['final_ret'] <= 0]
            win_rate = len(win_s) / n_trades if n_trades > 0 else 0
            avg_win = win_s['final_ret'].mean() if len(win_s) > 0 else 0
            avg_loss = abs(lose_s['final_ret'].mean()) if len(lose_s) > 0 else 0.1
            expectancy = (win_rate * avg_win) - ((1.0 - win_rate) * avg_loss)
            
            if (expectancy < 0.5) and (n_trades >= 5): return "NO_EDGE"
            if (0.85 <= (avg_win/(avg_loss+0.1)) <= 1.25) and (n_trades < 20): return "NOISE"

            p_df['mae_pct'] = (p_df['min_low'] - p_df['entry_price']) / p_df['entry_price'] * 100
            p_df['mfe_pct'] = (p_df['max_high'] - p_df['entry_price']) / p_df['entry_price'] * 100
            
            opt_alpha, opt_trap, opt_dtw = 0.75, 0.75, 2.5
            is_drought = len(p_df[p_df['entry_date'] >= (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')]) == 0
            valid_win_cos = win_s['entry_cos_score'].dropna() if 'entry_cos_score' in win_s.columns else pd.Series(dtype=float)
            valid_lose_cos = lose_s['entry_cos_score'].dropna() if 'entry_cos_score' in lose_s.columns else pd.Series(dtype=float)
            
            if len(valid_win_cos) >= 3:
                opt_alpha = np.percentile(valid_win_cos, 15)
                opt_dtw = np.percentile(win_s['entry_dtw_score'].dropna(), 85)
                if len(valid_lose_cos) >= 3: opt_trap = np.percentile(valid_lose_cos, 50)
            elif is_drought:
                opt_alpha, opt_dtw, opt_trap = 0.60, 3.5, 0.85

            raw_sl = np.percentile(win_s['mae_pct'].dropna(), 15) if len(win_s) >= 3 else -3.5
            raw_tp = np.percentile(win_s['mfe_pct'].dropna(), 50) if len(win_s) >= 3 else 10.0
            
            return {"sl": raw_sl, "tp": raw_tp, "fatal_cpv": np.percentile(lose_s['v_cpv'].dropna(), 90) if len(lose_s) >= 3 else 0.85, "alpha_limit": opt_alpha, "trap_limit": opt_trap, "dtw_limit": opt_dtw}

        t14, t30, t60 = get_period_stats(14), get_period_stats(30), get_period_stats(60)
        is_toxic = False
        for name, t_stat in zip(['14일', '30일', '60일'], [t14, t30, t60]):
            if t_stat == "TOXIC": is_toxic = True; report_lines.append(f"🚨 {name}: <b>[TOXIC 붕괴]</b> 강제 방어")
            elif t_stat == "NO_EDGE": report_lines.append(f"✂️ {name}: <b>[기댓값 미달]</b> 배제.")
        
        # 💡 [핵심] 파라미터를 저장할 때 '전략의 방 이름(target_ns)'을 열쇠에 붙여서 독립 보관
        cand_key = f"{target_ns}_CANDIDATE_PARAMS"
        if is_toxic:
            current_config[cand_key] = {"DYNAMIC_MAE_SL": -2.5, "DYNAMIC_MFE_TP": 10.0, "TREE_FATAL_CPV": 0.70, "DYNAMIC_ALPHA_LIMIT": 0.85, "DYNAMIC_TRAP_LIMIT": 0.70, "DYNAMIC_DTW_LIMIT": 1.5}
        else:
            valid = [s for s in [t14, t30, t60] if isinstance(s, dict)]
            if len(valid) >= 2:
                w = [0.5, 0.3, 0.2] if t14 and t30 and t60 else [1/len(valid)] * len(valid)
                current_config[cand_key] = {
                    "DYNAMIC_MAE_SL": round(sum(s['sl']*w[i] for i,s in enumerate(valid)), 2),
                    "DYNAMIC_MFE_TP": round(sum(s['tp']*w[i] for i,s in enumerate(valid)), 2),
                    "TREE_FATAL_CPV": round(sum(s['fatal_cpv']*w[i] for i,s in enumerate(valid)), 2),
                    "DYNAMIC_ALPHA_LIMIT": round(sum(s['alpha_limit']*w[i] for i,s in enumerate(valid)), 3),
                    "DYNAMIC_TRAP_LIMIT": round(sum(s['trap_limit']*w[i] for i,s in enumerate(valid)), 3),
                    "DYNAMIC_DTW_LIMIT": round(sum(s['dtw_limit']*w[i] for i,s in enumerate(valid)), 3)
                }
                report_lines.append(f"▪️ 앙상블 생성: SL {current_config[cand_key]['DYNAMIC_MAE_SL']}% / TP {current_config[cand_key]['DYNAMIC_MFE_TP']}%")

        # --- [엔진 5: 독립 STAT vs TECH 결투] ---
        if 'sim_stat_ret' in ns_df.columns:
            st_df = ns_df[ns_df['sim_stat_status'].str.contains('CLOSED', na=False)]
            te_df = ns_df[ns_df['sim_tech_status'].str.contains('CLOSED', na=False)]
            s_pf = (st_df[st_df['sim_stat_ret']>0]['sim_stat_ret'].sum()) / (abs(st_df[st_df['sim_stat_ret']<=0]['sim_stat_ret'].sum()) + 0.1) if len(st_df)>0 else 0
            t_pf = (te_df[te_df['sim_tech_ret']>0]['sim_tech_ret'].sum()) / (abs(te_df[te_df['sim_tech_ret']<=0]['sim_tech_ret'].sum()) + 0.1) if len(te_df)>0 else 0
            winner = "TECH" if t_pf > s_pf * 1.1 else "STAT"
            current_config[f"{target_ns}_ACTIVE_EXIT_MODE"] = winner
            report_lines.append(f"▪️ 청산 결투: {winner} 모드가 우세함")

        # --- [엔진 5.5: 생존 호흡(bars_held) 기반 TIME_STOP 동기화] ---
        if 'bars_held' in ns_df.columns and 'entry_date' in ns_df.columns and 'final_ret' in ns_df.columns:
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            breath_df = ns_df[(ns_df['entry_date'] >= thirty_days_ago) & (ns_df['final_ret'] > 0)].copy()
            breath_df = breath_df[breath_df['bars_held'].notna()]
            if not breath_df.empty:
                avg_bars = float(breath_df['bars_held'].mean())
                sync_time_stop = max(3, int(round(avg_bars * 1.2)))
                current_config[f"{target_ns}_TIME_STOP"] = sync_time_stop
                report_lines.append(f"▪️ 생존 호흡 동기화: bars_held 평균 {avg_bars:.1f}일 ➔ TIME_STOP {sync_time_stop}일")

        # --- [엔진 6: 독립 OOS 진검승부 및 챔피언 승격] ---
        train_df = ns_df[ns_df['entry_date'] < oos_barrier]
        test_df = ns_df[ns_df['entry_date'] >= oos_barrier]
        
        def get_eq(ret_s):
            v = ret_s.dropna().values
            if len(v) < 3: return sum(v)
            return np.percentile([(np.prod(1+np.random.choice(v, size=len(v), replace=True)/100.0)-1)*100 for _ in range(1000)], 5)

        results = {}
        for col in ['live_a_ret', 'cand_b_ret', 'champ_c_ret']:
            if col in test_df.columns: results[col] = get_eq(test_df[col])

        # 💡 [V_NEXT 진화 로직 적용] Thompson Sampling 기반 전략별 리스크 가중치 산출
        def _ts_risk_from_returns(ret_series, base_kelly):
            try:
                rs = pd.to_numeric(ret_series, errors='coerce').dropna()
                if len(rs) < 3:
                    return None
                wins = int((rs > 0).sum())
                losses = int((rs <= 0).sum())
                # 승리/패배 횟수를 베타 분포 파라미터로 사용 (0 방지용 최소 1)
                alpha = max(1, wins)
                beta = max(1, losses)
                ts_sample = float(np.random.beta(alpha, beta))
                win_sum = float(rs[rs > 0].sum())
                loss_sum = abs(float(rs[rs <= 0].sum()))
                pf = float(win_sum / (loss_sum + 0.1))
                pf_weight = float(np.clip(pf / 1.5, 0.5, 1.8))
                risk = float(np.clip(base_kelly * ts_sample * pf_weight, 0.002, 0.030))
                wr = float(wins / max(1, wins + losses))
                return {
                    "risk": risk,
                    "alpha": alpha,
                    "beta": beta,
                    "sample": ts_sample,
                    "pf": pf,
                    "wr": wr
                }
            except Exception:
                return None
        
        if results:
            win_k = max(results, key=results.get)
            report_lines.append(f"▪️ OOS 성적(복리): LIVE({results.get('live_a_ret',0):.2f}%) B({results.get('cand_b_ret',0):.2f}%) C({results.get('champ_c_ret',0):.2f}%)")

            # 챔피언이 이미 존재하면 최근 14일 실전 MAE/MFE로 스무딩 업데이트
            champ_key = f"{target_ns}_CHAMPION_PARAMS"
            champ_params = current_config.get(champ_key, {})
            if isinstance(champ_params, dict) and champ_params:
                recent_14 = test_df.sort_values('entry_date').tail(14).copy() if 'entry_date' in test_df.columns else test_df.tail(14).copy()
                if not recent_14.empty and all(col in recent_14.columns for col in ['entry_price', 'min_low', 'max_high']):
                    recent_14 = recent_14[recent_14['entry_price'] > 0]
                    if not recent_14.empty:
                        recent_14['mae_pct'] = (recent_14['min_low'] - recent_14['entry_price']) / recent_14['entry_price'] * 100.0
                        recent_14['mfe_pct'] = (recent_14['max_high'] - recent_14['entry_price']) / recent_14['entry_price'] * 100.0

                        new_mae = float(recent_14['mae_pct'].mean())
                        new_mfe = float(recent_14['mfe_pct'].mean())
                        old_mae = float(champ_params.get("DYNAMIC_MAE_SL", new_mae))
                        old_mfe = float(champ_params.get("DYNAMIC_MFE_TP", new_mfe))

                        # 💡 [100년 영속 진화 로직 적용: EWC 기반 동적 스무딩]
                        # 과거 누적 표본이 많을수록 새 표본 영향력을 낮춘다. (alpha = new / (old + new), cap=0.4)
                        obs_key = f"{target_ns}_CHAMPION_OBS_COUNT"
                        old_obs = int(current_config.get(obs_key, 0) or 0)
                        new_obs = int(len(recent_14))
                        alpha_smooth = float(min(0.4, (new_obs / max(1, (old_obs + new_obs)))))
                        champ_params["DYNAMIC_MAE_SL"] = round((old_mae * (1 - alpha_smooth)) + (new_mae * alpha_smooth), 2)
                        champ_params["DYNAMIC_MFE_TP"] = round((old_mfe * (1 - alpha_smooth)) + (new_mfe * alpha_smooth), 2)
                        current_config[obs_key] = int(old_obs + new_obs)
                        current_config[champ_key] = champ_params
                        report_lines.append(
                            f"▪️ 챔피언 스무딩(14일): MAE {old_mae:.2f}%➔{champ_params['DYNAMIC_MAE_SL']:.2f}% | "
                            f"MFE {old_mfe:.2f}%➔{champ_params['DYNAMIC_MFE_TP']:.2f}% | "
                            f"α_smooth {alpha_smooth:.3f} (old={old_obs}, new={new_obs})"
                        )
            
            if win_k == 'cand_b_ret' and results['cand_b_ret'] > results.get('live_a_ret', 0) * 1.05:
                current_config[f"{target_ns}_CHAMPION_PARAMS"] = current_config.get(f"{target_ns}_LIVE_PARAMS", {})
                current_config[f"{target_ns}_LIVE_PARAMS"] = current_config.get(cand_key, {})
                report_lines.append("🏆 <b>[신규 승격]</b> B가 실전 배치됩니다.")
            elif win_k == 'champ_c_ret' and results['champ_c_ret'] > results.get('live_a_ret', 0) * 1.05:
                current_config[f"{target_ns}_LIVE_PARAMS"] = current_config.get(f"{target_ns}_CHAMPION_PARAMS", {})
                report_lines.append("♻️ <b>[챔피언 귀환]</b> C가 복귀합니다.")

            # 💡 [V_NEXT 진화 로직 적용] 승격 결과 이후 선택 전략 기준 Thompson 리스크 반영
            base_kelly_ns = float(current_config.get(f"{target_ns}_DYNAMIC_KELLY_RISK", current_config.get("DYNAMIC_KELLY_RISK", 0.01)))
            pick_col = win_k if win_k in ['live_a_ret', 'cand_b_ret', 'champ_c_ret'] else 'live_a_ret'
            ts_ret_series = test_df[pick_col] if pick_col in test_df.columns else (ns_df[pick_col] if pick_col in ns_df.columns else pd.Series(dtype=float))
            ts_pack = _ts_risk_from_returns(ts_ret_series, base_kelly_ns)
            if ts_pack is not None:
                current_config[f"{target_ns}_DYNAMIC_KELLY_RISK"] = round(ts_pack["risk"], 4)
                ns_sampled_risks.append(ts_pack["risk"])
                report_lines.append(
                    f"🎯 <b>[TS 자본 배분]</b> {target_ns} {pick_col}: "
                    f"Beta({ts_pack['alpha']},{ts_pack['beta']}) 샘플 {ts_pack['sample']:.3f} | "
                    f"승률 {ts_pack['wr']*100:.1f}% | PF {ts_pack['pf']:.2f} ➔ Kelly {ts_pack['risk']*100:.2f}%"
                )
            else:
                report_lines.append(f"▪️ {target_ns} TS 표본 부족으로 기존 Kelly 유지")

        _sync_inverse_mode_switch(current_config, vix_last, regime)
        save_config(current_config)

    # 💡 [V_NEXT 진화 로직 적용] 네임스페이스 TS 리스크를 글로벌 Kelly에 합성(중앙값 사용)
    if ns_sampled_risks:
        try:
            global_prev = float(current_config.get("DYNAMIC_KELLY_RISK", 0.01))
            global_ts = float(np.clip(np.median(ns_sampled_risks), 0.002, 0.030))
            current_config["DYNAMIC_KELLY_RISK"] = round(global_ts, 4)
            report_lines.append(
                f"\n🎛️ <b>[TS 글로벌 합성]</b> 네임스페이스 중앙값 Kelly {global_prev*100:.2f}% ➔ {global_ts*100:.2f}%"
            )
            _sync_inverse_mode_switch(current_config, vix_last, regime)
            save_config(current_config)
        except Exception:
            pass

    # ---------------------------------------------------------
    # 👑 엔진 6.5: [V30.0 알파 반감기(Alpha Decay) 및 노화 부검 엔진]
    # ---------------------------------------------------------
    report_lines.append("\n<b>[6.5 알파 반감기(Alpha Decay) 수명 추적]</b>")
    promo_date_str = current_config.get("LIVE_A_PROMOTION_DATE", None)
    
    if promo_date_str:
        promo_date = datetime.strptime(promo_date_str, '%Y-%m-%d')
        days_alive = (datetime.now() - promo_date).days
        
        # 승격(생일) 이후의 실전 데이터만 추출
        decay_df = df[df['entry_date'] >= promo_date_str]
        
        if len(decay_df) >= 8 and days_alive >= 3:
            # 반감기 분할 연산 (전반전 vs 후반전)
            half_point = len(decay_df) // 2
            early_phase = decay_df.iloc[:half_point]
            late_phase = decay_df.iloc[half_point:]
            
            _, early_pf = calculate_metrics(early_phase)
            _, late_pf = calculate_metrics(late_phase)
            
            report_lines.append(f"▪️ 현재 룰 생존 기간: <b>{days_alive}일차</b> (표본 {len(decay_df)}개)")
            report_lines.append(f"▪️ 승격 초기 PF: {early_pf:.2f} ➔ 최근(노화) PF: {late_pf:.2f}")

            # 진행성 노화(사형 직전): late/early 비율만큼 DYNAMIC_KELLY_RISK 패널티 (하한 0.2%)
            base_kelly = float(current_config.get("DYNAMIC_KELLY_RISK", 0.01))
            kelly_floor = 0.002
            touched_ns_kelly = False
            ns_kelly_vals = []

            # 💡 [V_NEXT 진화 로직 적용] 예측적 알파 반감기 방어: bars_held 분산 급증 + breadth 급락으로 PF<1.0 위험 근사
            try:
                recent20 = decay_df.sort_values('entry_date').tail(20).copy() if 'entry_date' in decay_df.columns else decay_df.tail(20).copy()
                prev20 = decay_df.sort_values('entry_date').iloc[-40:-20].copy() if ('entry_date' in decay_df.columns and len(decay_df) >= 40) else decay_df.iloc[0:0].copy()

                # 1) 체류기간 변동성(분산) 급증 시그널
                var_recent = float(pd.to_numeric(recent20.get('bars_held', pd.Series(dtype=float)), errors='coerce').dropna().var(ddof=0)) if not recent20.empty else 0.0
                var_base = float(pd.to_numeric(prev20.get('bars_held', pd.Series(dtype=float)), errors='coerce').dropna().var(ddof=0)) if not prev20.empty else var_recent
                if not np.isfinite(var_recent):
                    var_recent = 0.0
                if not np.isfinite(var_base) or var_base <= 0:
                    var_base = max(1e-6, var_recent)
                var_ratio = var_recent / max(1e-6, var_base)
                var_signal = float(np.clip((var_ratio - 1.5) / 2.5, 0.0, 1.0))

                # 2) 시장 폭(Breadth) 급락 시그널 (entry_breadth 시계열 + 거시 breadth_ratio 보조)
                breadth_signal = 0.0
                breadth_macro_signal = 0.0
                if 'entry_breadth' in recent20.columns and not recent20.empty:
                    b_now = float(pd.to_numeric(recent20['entry_breadth'], errors='coerce').dropna().mean())
                    b_ref = float(pd.to_numeric(prev20['entry_breadth'], errors='coerce').dropna().mean()) if ('entry_breadth' in prev20.columns and not prev20.empty) else 1.0
                    if not np.isfinite(b_now):
                        b_now = 1.0
                    if not np.isfinite(b_ref) or b_ref <= 0:
                        b_ref = 1.0
                    breadth_drop = max(0.0, b_ref - b_now)
                    breadth_signal = float(np.clip(breadth_drop / 0.08, 0.0, 1.0))
                try:
                    if 'breadth_ratio' in locals() and np.isfinite(float(breadth_ratio)):
                        breadth_macro_signal = float(np.clip((1.0 - float(breadth_ratio)) / 0.08, 0.0, 1.0))
                except Exception:
                    breadth_macro_signal = 0.0

                # 3) 다음 10개 거래에서 PF<1.0 붕괴 확률 근사
                pf_break_risk_prob = float(np.clip(
                    (0.55 * var_signal) + (0.35 * breadth_signal) + (0.10 * breadth_macro_signal),
                    0.0, 1.0
                ))
                report_lines.append(
                    f"▪️ [예측 반감기] PF<1.0 붕괴확률(다음10트레이드) ≈ {pf_break_risk_prob*100:.1f}% "
                    f"(bars분산배율 x{var_ratio:.2f}, breadth_sig {breadth_signal:.2f})"
                )

                if pf_break_risk_prob > 0.50:
                    current_config["DYNAMIC_KELLY_RISK"] = round(kelly_floor, 4)
                    report_lines.append(
                        f"🛡️ <b>[선제 방어 발동]</b> 붕괴확률 {pf_break_risk_prob*100:.1f}% > 50% "
                        f"➔ DYNAMIC_KELLY_RISK를 최소치 {kelly_floor*100:.1f}%로 긴급 축소"
                    )
            except Exception as _pred_e:
                report_lines.append(f"▪️ 예측 반감기 계산 스킵(안전우회): {_pred_e}")

            if 'namespace' in decay_df.columns:
                for tns in decay_df['namespace'].dropna().unique():
                    nd = decay_df[decay_df['namespace'] == tns]
                    if len(nd) < 8:
                        continue
                    h = len(nd) // 2
                    eph, lph = nd.iloc[:h], nd.iloc[h:]
                    _, epf_n = calculate_metrics(eph)
                    _, lpf_n = calculate_metrics(lph)
                    if epf_n <= 1e-9:
                        continue
                    if lpf_n < epf_n and not (lpf_n < epf_n * 0.7 or lpf_n < 1.0):
                        ratio_n = max(1e-9, lpf_n / epf_n)
                        k_prev = float(current_config.get(f"{tns}_DYNAMIC_KELLY_RISK", base_kelly))
                        k_new = max(kelly_floor, k_prev * ratio_n)
                        current_config[f"{tns}_DYNAMIC_KELLY_RISK"] = round(k_new, 4)
                        ns_kelly_vals.append(k_new)
                        touched_ns_kelly = True
                        report_lines.append(
                            f"⚙️ <b>[{tns} 알파 노화 선제 축소]</b> DYNAMIC_KELLY_RISK {k_prev*100:.2f}% → {k_new*100:.2f}% "
                            f"(late/early PF ×{ratio_n:.2f}, 하한 {kelly_floor*100:.1f}%)"
                        )
                if touched_ns_kelly and ns_kelly_vals:
                    current_config["DYNAMIC_KELLY_RISK"] = round(min(base_kelly, min(ns_kelly_vals)), 4)

            if not touched_ns_kelly:
                if late_pf < early_pf and not (late_pf < early_pf * 0.7 or late_pf < 1.0):
                    ratio_g = max(1e-9, late_pf / max(early_pf, 1e-9))
                    k_new_g = max(kelly_floor, base_kelly * ratio_g)
                    current_config["DYNAMIC_KELLY_RISK"] = round(k_new_g, 4)
                    report_lines.append(
                        f"⚙️ <b>[알파 노화 선제 축소]</b> DYNAMIC_KELLY_RISK {base_kelly*100:.2f}% → {k_new_g*100:.2f}% "
                        f"(late/early PF ×{ratio_g:.2f}, 하한 {kelly_floor*100:.1f}%)"
                    )
            
            # 🚨 [알파 붕괴 판정] 손익비가 초기 대비 30% 이상 날아갔거나 1.0 미만일 때
            if late_pf < early_pf * 0.7 or late_pf < 1.0:
                report_lines.append("🚨 <b>[알파 반감기 도달]</b> 룰의 수명이 다했습니다. 선제적 파라미터 폐기를 집행합니다.")
                
                # 🔬 [노화 원인 정밀 부검]
                late_losers = late_phase[late_phase['final_ret'] <= 0]
                if len(late_losers) >= 3:
                    avg_cpv = late_losers['dyn_cpv'].mean()
                    avg_breadth = late_losers['entry_breadth'].mean() if 'entry_breadth' in late_losers.columns else 1.0
                    
                    if avg_breadth < 0.98: 
                        cause = "거시적 시장 폭(Breadth) 붕괴. 지수 착시로 인한 무차별 하락장 전개."
                    elif (10-avg_cpv)*11.1 > 60: 
                        cause = "극단적 윗꼬리(CPV) 급증. 세력들이 해당 타점(룰)을 역이용하여 물량을 넘김."
                    else: 
                        cause = "해당 룰에 대한 시장 참여자들의 역이용 (과최적화 알파 소멸)."
                        
                    report_lines.append(f"💡 <b>[노화 원인 분석]</b>: {cause}")
                    
                # 🧠 자율 MAE 역추적: 최근 20개 패배 종목의 실제 MAE 평균으로 손절선 재설정
                try:
                    conn = sqlite3.connect(DB_PATH, timeout=60)
                    conn.execute("PRAGMA journal_mode=WAL;")
                    recent_loss_df = pd.read_sql("""
                        SELECT entry_price, min_low
                        FROM forward_trades
                        WHERE status LIKE 'CLOSED%'
                          AND final_ret <= 0
                          AND entry_price > 0
                          AND min_low IS NOT NULL
                        ORDER BY exit_date DESC, id DESC
                        LIMIT 20
                    """, conn)
                    conn.close()

                    if not recent_loss_df.empty:
                        recent_loss_df['real_mae_pct'] = (
                            (recent_loss_df['min_low'] - recent_loss_df['entry_price'])
                            / recent_loss_df['entry_price'] * 100.0
                        )
                        adaptive_sl = round(recent_loss_df['real_mae_pct'].mean(), 2)
                        current_config["DYNAMIC_MAE_SL"] = adaptive_sl
                        report_lines.append(f"💡 조치: 최근 20개 패배 종목의 실제 MAE 평균({adaptive_sl}%)으로 손절선을 자율 세팅했습니다.")
                    else:
                        report_lines.append("💡 조치: 최근 패배 표본이 부족하여 기존 손절선을 유지합니다.")
                except Exception as e:
                    report_lines.append(f"⚠️ MAE 역추적 계산 에러: {e}")
            else:
                report_lines.append("✅ <b>[알파 엣지 유지 중]</b> 현재 파라미터가 시장에서 여전히 강력하게 작동 중입니다.")
        else:
            report_lines.append(f"▪️ 생존 {days_alive}일차: 반감기를 판독하기엔 아직 표본이 부족합니다.")
    else:
        current_config["LIVE_A_PROMOTION_DATE"] = datetime.now().strftime('%Y-%m-%d')
        report_lines.append("▪️ 알파 반감기 추적을 위한 최초 승격일을 오늘로 기록했습니다.")

    # ---------------------------------------------------------
    # 👑 엔진 7: [V103.0 통합 시스템 데스매치 결산 (자본주의 복리 배분)]
    # ---------------------------------------------------------
    report_lines.append("\n⚔️ <b>[V103.0 통합 시스템 데스매치 결산]</b>")
    
    # 2. 발굴 대결 (STD vs SN vs BEAST vs UD) — 축당 최소 청산 N건 미만은 판정에서 제외
    try:
        n_min_dm = int(current_config.get("DEATHMATCH_MIN_TRADES_PER_ARM", 5) or 5)
    except (TypeError, ValueError):
        n_min_dm = 5
    n_min_dm = max(1, n_min_dm)

    try:
        from evolution.deathmatch_report import build_nway_deathmatch

        df_kr = df
        if "market" in df.columns:
            df_kr = df[df["market"].astype(str).str.upper() == "KR"]
        dm_hunt = build_nway_deathmatch(df_kr, current_config, market="KR")
        eligible_hunt = [
            a
            for a in dm_hunt.arms
            if a.n_valid >= n_min_dm
            and a.mean_ret is not None
            and math.isfinite(float(a.mean_ret))
        ]
        if not eligible_hunt:
            report_lines.append(
                f"🏁 <b>[발굴 대결 KR Battle Royal]</b> 표본 부족으로 판정 보류 "
                f"(축당 최소 유효 청산 {n_min_dm}건)"
            )
        else:
            top = max(eligible_hunt, key=lambda a: float(a.mean_ret))
            _dm_plain = re.sub(r"<[^>]+>", "", str(dm_hunt.verdict))
            report_lines.append(
                f"🏁 <b>[발굴 대결 1위·KR]</b> {top.label} {top.mean_ret:+.2f}% "
                f"(유효 n={top.n_valid}) · {_dm_plain}"
            )
    except Exception as _dm_ex:
        report_lines.append(f"⚠️ [발굴 대결] N-Way 산출 스킵: {_dm_ex}")
        
    # 💡 [핵심] 인위적인 WEIGHT(1.6 vs 0.4) 강제 배분 로직 완전 삭제
    report_lines.append("✅ <b>알림:</b> 인위적 가중치(WEIGHT) 배분 로직이 삭제되었습니다. 개별 시드의 복리 성장이 곧 자본 배분입니다.")

    # ---------------------------------------------------------
    # 👑 엔진 8: [V55.0 초신성 실전 흐름 역추적 및 MFE 가중치 템플릿 진화]
    # ---------------------------------------------------------
    report_lines.append("\n🔬 <b>[V55.0 초신성 실전 DNA 검증 및 MFE 가중치 템플릿]</b>")
    
    # 1. 가상매매 장부에서 청산 완료된 '초신성' 데이터만 발췌
    sn_closed = df[(df['sig_type'].str.contains('SUPERNOVA', na=False)) & (df['status'].str.contains('CLOSED', na=False))]
    
    if len(sn_closed) >= 5:
        # 2. [미래 흐름 연결성] 실전에서 MFE(최대 수익률) 10% 이상을 달성한 '진짜 대박주'만 추출
        high_mfe_sn = sn_closed[sn_closed['mfe'] >= 10.0]
        
        if not high_mfe_sn.empty:
            report_lines.append(f"▪️ <b>실전 고수익(MFE 10%↑) 초신성 표본:</b> {len(high_mfe_sn)}개 발견")
            
            # 3. 승리한 초신성들의 '실제 DNA 수치값' 평균 산출 (MFE 가중치 템플릿)
            real_cpv = high_mfe_sn['dyn_cpv'].mean()
            real_tb = high_mfe_sn['dyn_tb'].mean()
            real_bbe = high_mfe_sn['v_energy'].mean()
            
            # 기존 과거 백테스트 템플릿(Centroid) 값 가져오기 (비교용)
            multi_templates = current_config.get(f"DNA_SUPERNOVA_{high_mfe_sn['market'].iloc[0]}_MULTI", {})
            
            report_lines.append(f"💡 <b>[과거 하드코딩 vs 실전 MFE DNA 대조]</b>")
            
            # 4. 랭크 및 그룹별(RANK_A, RANK_B 등) 하드코딩 수치 생존 추적 및 오차율 계산
            for rank in ['RANK_A', 'RANK_B', 'RANK_C', 'RANK_D']:
                rank_df = high_mfe_sn[high_mfe_sn['sig_type'].str.contains(rank, na=False)]
                if not rank_df.empty:
                    rank_mfe = rank_df['mfe'].mean()
                    rank_cpv = rank_df['dyn_cpv'].mean()
                    rank_tb = rank_df['dyn_tb'].mean()
                    
                    report_lines.append(f" ↳ <b>[{rank}]</b> 평균 MFE: <b>{rank_mfe:.1f}%</b> 달성")
                    report_lines.append(f"    - 실전 CPV: {rank_cpv:.2f} | 실전 TB: {rank_tb:.1f}")
            
            # 5. [메타 최적화] 오리지널 로직의 '스무딩(Smoothing)' 오토 추적 시스템 이식
            # 과거의 템플릿을 한 번에 갈아엎지 않고, SMOOTHING_ALPHA(0.3) 비율만큼만 시장 흐름을 부드럽게 흡수합니다.
            
            old_mfe_template = current_config.get("DNA_SUPERNOVA_MFE_WEIGHTED", {"cpv": real_cpv, "tb": real_tb})
            
            # (기존 값 * 0.7) + (새로운 실전 값 * 0.3) = 점진적 오토 추적
            smoothed_cpv = (old_mfe_template["cpv"] * (1 - SMOOTHING_ALPHA)) + (real_cpv * SMOOTHING_ALPHA)
            smoothed_tb = (old_mfe_template["tb"] * (1 - SMOOTHING_ALPHA)) + (real_tb * SMOOTHING_ALPHA)
            smoothed_bbe = (old_mfe_template.get("bbe", real_bbe) * (1 - SMOOTHING_ALPHA)) + (real_bbe * SMOOTHING_ALPHA)
            
            # 💡 [핵심] 단일 덮어쓰기 대신 버전을 붙여서 황금 타점을 여러 개 누적 (세포 분열)
            mfe_payload = {
                "cpv": round(smoothed_cpv, 3),
                "tb": round(smoothed_tb, 3),
                "bbe": round(smoothed_bbe, 3),
                "last_updated": datetime.now().strftime('%Y-%m-%d')
            }
            # 기존 호환성을 위해 원본 키도 업데이트하고, MFE 전용 멀티 저장소에도 누적
            current_config["DNA_SUPERNOVA_MFE_WEIGHTED"] = mfe_payload
            
            mfe_multi_key = "DNA_SUPERNOVA_MFE_MULTI"
            mfe_pool = current_config.setdefault(mfe_multi_key, {})
            version_tag = datetime.now().strftime('V_%y%m%d_%H%M')
            mfe_pool[f"MFE_GOLDEN_{version_tag}"] = mfe_payload
            
            # 서버 과부하 방지 (최대 10개 유지)
            if len(mfe_pool) > 10:
                oldest_k = sorted(mfe_pool.keys())[0]
                mfe_pool.pop(oldest_k, None)
            current_config[mfe_multi_key] = mfe_pool
            
            report_lines.append(f"\n🧬 <b>[MFE 황금 템플릿 오토 스무딩]</b>")
            report_lines.append(f" ↳ CPV: {old_mfe_template.get('cpv', real_cpv):.2f} ➔ <b>{smoothed_cpv:.2f}</b>")
            report_lines.append(f" ↳ TB: {old_mfe_template.get('tb', real_tb):.1f} ➔ <b>{smoothed_tb:.1f}</b>")
            report_lines.append(f" ↳ BBE: {old_mfe_template.get('bbe', real_bbe):.1f} ➔ <b>{smoothed_bbe:.1f}</b>")

    # 👇👇 [기존 엔진 9 영역을 이걸로 완전히 덮어쓰세요] 👇👇
    # ---------------------------------------------------------
    # 👑 엔진 9: [V56.0 초신성 내부 서브-데스매치 & 컷오프 자율 튜닝]
    # ---------------------------------------------------------
    report_lines.append("\n⚙️ <b>[V56.0 초신성 내부 결투 및 자율 튜닝]</b>")
    
    # 코사인 진영과 ML박스 진영의 유동적 컷오프 자율 튜닝 (독립 진행)
    for tag_key, config_key in [("COSINE", "DYNAMIC_SUPERNOVA_CUTOFF"), ("MLBOX", "DYNAMIC_ML_BOX_CUTOFF")]:
        sub_df = df[(df['sig_type'].str.contains(tag_key, na=False)) & (df['status'].str.contains('CLOSED', na=False))]
        
        curr_val = current_config.get(config_key, 0.50) # 기본 50%
        
        if len(sub_df) >= 5:
            wr = len(sub_df[sub_df['final_ret'] > 0]) / len(sub_df)
            pf = sub_df[sub_df['final_ret'] > 0]['final_ret'].sum() / (abs(sub_df[sub_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
            
            report_lines.append(f"▪️ [{tag_key} 타점]: 승률 {wr*100:.1f}% | PF {pf:.2f} (표본 {len(sub_df)}개)")
            
            if wr < 0.45: # 승률 낮으면 허들 조이기
                new_val = min(0.90, curr_val + 0.05)
                current_config[config_key] = round(new_val, 2)
                report_lines.append(f" 🚨 <b>[방어력 강화]</b> 승률 저조 ➔ 허들을 {new_val*100:.0f}%로 상향 조율")
            elif wr > 0.65 and len(sub_df) < 10: # 승률 좋은데 표본 적으면 그물 넓히기
                new_val = max(0.40, curr_val - 0.03)
                current_config[config_key] = round(new_val, 2)
                report_lines.append(f" 🔥 <b>[공격적 포착]</b> 승률 우수 ➔ 허들을 {new_val*100:.0f}%로 하향 조율")
            else:
                report_lines.append(f" ✅ <b>[최적 균형]</b> 현재 커트라인({curr_val*100:.0f}%) 유지")
        else:
            # 💡 [100년 영속 진화 로직 적용: Cutoff Death-Spiral Relief Valve]
            try:
                from elastic_threshold import ElasticThreshold

                _et = ElasticThreshold.from_system_config(current_config, market="KR")
                new_val, reason = _et.relief_adjust_autonomous_cutoff(
                    config_key, float(curr_val), n_closed=len(sub_df)
                )
                current_config[config_key] = new_val
                report_lines.append(
                    f"▪️ [{tag_key} 타점]: 표본 기아 elastic 완화 {curr_val*100:.0f}% ➔ "
                    f"{new_val*100:.0f}% ({reason})"
                )
            except Exception:
                current_config[config_key] = round(max(0.40, curr_val - 0.02), 2)
                report_lines.append(
                    f"▪️ [{tag_key} 타점]: 표본 기아 완화로 커트라인 {curr_val*100:.0f}% ➔ "
                    f"{current_config[config_key]*100:.0f}% (표본 데이터 수집 중)"
                )

    # ---------------------------------------------------------
    # 💀 엔진 10: [V60.0 초신성 템플릿 생존 토너먼트 및 국고 환수]
    # ---------------------------------------------------------
    report_lines.append("\n💀 <b>[V60.0 진화론 도태 심판 및 국고 환수]</b>")
    anti_patterns = current_config.get("ANTI_PATTERNS", {})
    if not isinstance(anti_patterns, dict):
        anti_patterns = {}
    
    sn_all_closed = df[(df['sig_type'].str.contains('SUPERNOVA', na=False)) & (df['status'].str.contains('CLOSED', na=False))]
    
    for mkt in ['KR', 'US']:
        multi_key = f"DNA_SUPERNOVA_{mkt}_MULTI"
        if multi_key not in current_config: continue
        
        market_templates = current_config[multi_key]
        archive_root = current_config.setdefault("ARCHIVED_TEMPLATES", {})
        market_archive = archive_root.setdefault(mkt, {})
        treasury_key = f"CENTRAL_TREASURY_{mkt}"
        current_treasury = current_config.get(treasury_key, 0)
        culled_list = []
        
        for template_name in list(market_templates.keys()):
            t_trades = sn_all_closed[sn_all_closed['sig_type'].str.contains(template_name, na=False)]
            
            if len(t_trades) >= 5:
                t_wins = t_trades[t_trades['final_ret'] > 0]
                t_wr = len(t_wins) / len(t_trades)
                t_pf = t_wins['final_ret'].sum() / (abs(t_trades[t_trades['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
                
                # 🚨 [사형 선고 및 자금 회수]
                if t_wr < 0.35 or t_pf < 1.0:
                    # 도태 유전자는 삭제하지 않고 냉동 보관소로 이동
                    archived_payload = market_templates.pop(template_name)
                    market_archive[template_name] = {
                        "template_data": archived_payload,
                        "archived_at": datetime.now().strftime('%Y-%m-%d'),
                        "market": mkt,
                        "wr": round(t_wr, 4),
                        "pf": round(t_pf, 4),
                        "sample_size": int(len(t_trades))
                    }

                    # 안티 패턴 면역 체계: 도태(승률 35% 미만) 템플릿 DNA 축적
                    if t_wr < 0.35 and isinstance(archived_payload, dict):
                        anti_key = f"CULLED_{template_name}_{datetime.now().strftime('%y%m%d%H%M')}"
                        anti_patterns[anti_key] = {
                            "source": "CULLED_TEMPLATE",
                            "template": template_name,
                            "market": mkt,
                            "cpv": float(archived_payload.get("cpv", 0.0)),
                            "tb": float(archived_payload.get("tb", 0.0)),
                            "bbe": float(archived_payload.get("bbe", 0.0)),
                            "rs": float(archived_payload.get("rs", 0.0)),
                            "recorded_at": datetime.now().strftime('%Y-%m-%d')
                        }
                    
                    # 💡 [신규 추가] 해당 로직의 최종 잔고 역산 및 국고 반환
                    total_pnl = (t_trades['sim_kelly_invest'] * t_trades['final_ret'] / 100).sum()
                    final_balance = 20000000 + total_pnl
                    
                    # 국고에 잔고 더하기 (Plus)
                    current_treasury += final_balance 
                    
                    culled_list.append(f"{template_name} (회수금: {final_balance:,.0f}원)")
        
        current_config[multi_key] = market_templates
        current_config["ARCHIVED_TEMPLATES"] = archive_root
        current_config[treasury_key] = current_treasury # 업데이트된 국고 저장
        
        if culled_list:
            report_lines.append(f"▪️ <b>{mkt}장 도태 집행 및 국고 환수 완료</b>")
            for c_name in culled_list: 
                report_lines.append(f"  ❌ {c_name}")
            report_lines.append(f"💰 {mkt} 국고 총액: {current_treasury:,.0f}원")

    # 최근 1개월 치명적 참사주(-10% 이하) 평균 DNA를 안티 패턴에 축적
    try:
        one_month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        fatal_df = df[
            (df['status'].str.contains('CLOSED', na=False)) &
            (df['entry_date'] >= one_month_ago) &
            (df['final_ret'] <= -10.0)
        ] if all(col in df.columns for col in ['status', 'entry_date', 'final_ret']) else pd.DataFrame()
        if not fatal_df.empty:
            anti_key = f"FATAL_30D_{datetime.now().strftime('%y%m%d%H%M')}"
            anti_patterns[anti_key] = {
                "source": "FATAL_LOSERS_30D",
                "cpv": round(float(fatal_df['dyn_cpv'].mean()), 4) if 'dyn_cpv' in fatal_df.columns else 0.0,
                "tb": round(float(fatal_df['dyn_tb'].mean()), 4) if 'dyn_tb' in fatal_df.columns else 0.0,
                "bbe": round(float(fatal_df['v_energy'].mean()), 4) if 'v_energy' in fatal_df.columns else 0.0,
                "rs": round(float(fatal_df['dyn_rs'].mean()), 4) if 'dyn_rs' in fatal_df.columns else 0.0,
                "sample_size": int(len(fatal_df)),
                "recorded_at": datetime.now().strftime('%Y-%m-%d')
            }
    except Exception as e:
        report_lines.append(f"▪️ 안티 패턴(참사주) 축적 에러: {e}")

    # 최신 패턴 위주로 중복/폭주 방지
    if len(anti_patterns) > 200:
        sorted_keys = sorted(anti_patterns.keys())
        excess = len(anti_patterns) - 200
        for k in sorted_keys[:excess]:
            anti_patterns.pop(k, None)
    current_config["ANTI_PATTERNS"] = anti_patterns

    # ---------------------------------------------------------
    # 👑 엔진 12: [V105.0 순환매 예측 로직 자율 검증 및 가중치 부여]
    # ---------------------------------------------------------
    report_lines.append("\n🔄 <b>[V105.0 순환매 예측 로직 자율 검증]</b>")
    
    # 태그 유무로 일반 매매와 선취매 매매를 완벽히 분리
    rot_df = df[df['sig_type'].str.contains('#순환매_선취매', na=False)]
    std_df = df[~df['sig_type'].str.contains('#순환매_선취매', na=False)]
    
    def get_pf(target_df):
        if len(target_df) == 0: return 0
        wins = target_df[target_df['final_ret'] > 0]['final_ret'].sum()
        loses = abs(target_df[target_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1
        return wins / loses

    # 최소 표본 3개 이상일 때만 수학적 검증 진행
    if len(rot_df) >= 3:
        rot_pf = get_pf(rot_df)
        std_pf = get_pf(std_df)
        
        report_lines.append(f" ▪️ 예측그룹 PF: {rot_pf:.2f} vs 일반그룹 PF: {std_pf:.2f}")
        
        # 💡 [자율 진화 핵심] 1.5배 우위 증명 시 가중치 플래그 활성화
        if rot_pf > std_pf * 1.5:
            current_config["ROTATION_ADVANTAGE_ACTIVE"] = True
            report_lines.append("🚀 <b>[검증 성공]</b> 순환매 선취매 우위 증명 ➔ 다음 주 <b>켈리 비중 2배</b> 적용")
        else:
            current_config["ROTATION_ADVANTAGE_ACTIVE"] = False
            report_lines.append("🛡️ <b>[검증 실패]</b> 예측 우위 부족 ➔ 일반 베팅 유지")
    else:
        report_lines.append(" ▪️ 표본 부족으로 순환매 자율 검증 스킵")

    # ---------------------------------------------------------
    # 🔮 엔진 12.5: transitions 1위 기반 다음 섹터 예측 저장
    # ---------------------------------------------------------
    report_lines.append("\n🔮 <b>[V105.1 transitions 기반 다음 섹터 예측 저장]</b>")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        sixty_days_ago = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')

        def map_standard_sector(s):
            s_str = str(s).lower()
            if any(k in s_str for k in ["반도체", "it", "ai", "소프트웨어", "모바일", "테크", "데이터"]): return "반도체/IT"
            if any(k in s_str for k in ["바이오", "헬스", "의료", "제약"]): return "바이오/헬스케어"
            if any(k in s_str for k in ["배터리", "2차전지", "화학", "에너지", "정유"]): return "에너지/화학"
            if any(k in s_str for k in ["금융", "은행", "증권", "지주", "투자"]): return "금융/지주"
            if any(k in s_str for k in ["기계", "조선", "방산", "산업재", "로봇", "전력"]): return "산업재/기계"
            if any(k in s_str for k in ["소비", "유통", "식품", "화장품", "엔터", "미디어"]): return "소비재/엔터"
            return "기타/혼합"

        for mkt in ['KR', 'US']:
            flow_df = pd.read_sql("""
                SELECT entry_date, sector
                FROM forward_trades
                WHERE entry_date >= ?
                  AND market = ?
                ORDER BY entry_date ASC
            """, conn, params=(sixty_days_ago, mkt))

            flow_df['sector'] = flow_df['sector'].apply(map_standard_sector)

            if not flow_df.empty:
                daily_dom = flow_df.groupby('entry_date')['sector'].agg(
                    lambda x: x.mode().iloc[0] if not x.mode().empty else None
                ).dropna()

                transitions = {}
                prev_sector = None
                sec_path = [str(s).strip() for s in daily_dom.tolist() if str(s).strip()]
                for sec in sec_path:
                    if prev_sector is not None and prev_sector != sec:
                        key = (prev_sector, sec)
                        transitions[key] = transitions.get(key, 0) + 1
                    prev_sector = sec

                if transitions and sec_path:
                    # 💡 [V_NEXT 진화 로직 적용] 전이 카운트로 Markov 전이확률행렬 M 구성 후 2-step(M^2) 예측
                    states = sorted({a for a, _ in transitions.keys()} | {b for _, b in transitions.keys()} | set(sec_path))
                    s2i = {s: i for i, s in enumerate(states)}
                    M = np.zeros((len(states), len(states)), dtype=float)

                    for (a, b), cnt in transitions.items():
                        ia, ib = s2i.get(a), s2i.get(b)
                        if ia is None or ib is None:
                            continue
                        M[ia, ib] += float(cnt)

                    for i in range(len(states)):
                        row_sum = float(M[i].sum())
                        if row_sum > 0:
                            M[i] = M[i] / row_sum

                    today_state = sec_path[-1]
                    i0 = s2i.get(today_state)
                    predicted_next_sector = None
                    if i0 is not None:
                        M2 = np.matmul(M, M)
                        row2 = M2[i0]
                        if np.isfinite(row2).any() and float(np.nansum(row2)) > 0:
                            j = int(np.nanargmax(row2))
                            predicted_next_sector = states[j]

                    # 2-step 정보가 희박하면 1-step 최다 전이로 안전 폴백
                    if not predicted_next_sector:
                        top_transition = max(transitions.items(), key=lambda kv: kv[1])[0]
                        predicted_next_sector = top_transition[1]

                    current_config[f"PREDICTED_NEXT_SECTOR_{mkt}"] = predicted_next_sector
                    report_lines.append(
                        f"▪️ [{mkt}] Markov 2-Step 예측: {today_state} -> {predicted_next_sector} "
                        f"(상태 {len(states)}개, 전이 {len(transitions)}쌍)"
                    )
                else:
                    report_lines.append(f"▪️ [{mkt}] 유의미한 섹터 전이 패턴이 없어 기존 예측값을 유지합니다.")
            else:
                report_lines.append(f"▪️ [{mkt}] 순환매 예측 저장용 표본 데이터가 부족합니다.")
        conn.close()
    except Exception as e:
        report_lines.append(f"⚠️ transitions 예측 저장 에러: {e}")

    # ---------------------------------------------------------
    # 👑 엔진 13: [V106.0 주차별 로직 일관성 추적 및 시계열 DNA 부검]
    # ---------------------------------------------------------
    report_lines.append("\n⏳ <b>[V106.0 주차별 일관성 추적 및 시계열 DNA 부검]</b>")
    
    try:
        # 💡 [핵심 교정] VIX 동적 룩백에 의해 메인 df가 7일/15일로 잘렸을 경우를 대비하여,
        # 4주치(30일) 청산 데이터를 DB에서 독립적으로 무조건 로드합니다.
        conn = sqlite3.connect(DB_PATH, timeout=60)
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        df_closed_30d = pd.read_sql(f"SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%' AND exit_date >= '{thirty_days_ago}'", conn)
        conn.close()

        import re
        def get_core_group(sig):
            sig = str(sig).replace('💀[기각/관찰용] ', '')
            sig = re.sub(r'^\[.*?\]\s*', '', sig)
            return sig.split(' [')[0]

        if not df_closed_30d.empty:
            df_closed_30d['group'] = df_closed_30d['sig_type'].apply(get_core_group)
            df_closed_30d['exit_date_dt'] = pd.to_datetime(df_closed_30d['exit_date'])
            now_dt = datetime.now()
            df_closed_30d['week_idx'] = (now_dt - df_closed_30d['exit_date_dt']).dt.days // 7
            
            group_weekly_pf = {}
            
            for group in df_closed_30d['group'].unique():
                g_df = df_closed_30d[df_closed_30d['group'] == group]
                weekly_pfs = {}
                for w in range(4): # 0주차 ~ 3주차 (최근 1개월)
                    w_df = g_df[g_df['week_idx'] == w]
                    if len(w_df) >= 3: 
                        w_wins = w_df[w_df['final_ret'] > 0]['final_ret'].sum()
                        w_loses = abs(w_df[w_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1
                        weekly_pfs[w] = w_wins / w_loses
                    else:
                        weekly_pfs[w] = None
                group_weekly_pf[group] = weekly_pfs

            consistent_good, consistent_bad, consistent_mid = [], [], []

            for g, pfs in group_weekly_pf.items():
                valid_pfs = [p for p in pfs.values() if p is not None]
                if len(valid_pfs) >= 2: # 최소 2주 이상 활동 검증
                    if all(p >= 1.2 for p in valid_pfs): consistent_good.append(g)      
                    elif all(p <= 0.8 for p in valid_pfs): consistent_bad.append(g)     
                    else: consistent_mid.append(g)                                      
                    
            report_lines.append(f"▪️ <b>장기 우상향(S급) 로직:</b> {', '.join(consistent_good) if consistent_good else '없음'}")
            report_lines.append(f"▪️ <b>장기 우하향(폐급) 로직:</b> {', '.join(consistent_bad) if consistent_bad else '없음'}")

            def extract_cohort_dna(group_list):
                if not group_list: return "표본 없음"
                tgt_df = df_closed_30d[df_closed_30d['group'].isin(group_list)]
                if tgt_df.empty: return "표본 없음"
                c = tgt_df['dyn_cpv'].mean()
                t = tgt_df['dyn_tb'].mean()
                e = tgt_df['v_energy'].mean()
                return f"CPV {c:.2f} | 찐양봉 {t:.1f}배 | 응축 {e:.1f}"

            report_lines.append(f"\n💡 <b>[우상향 로직 절대 공통 DNA]</b>\n ↳ {extract_cohort_dna(consistent_good)}")
            report_lines.append(f"↔️ <b>[횡보/중간 로직 공통 DNA]</b>\n ↳ {extract_cohort_dna(consistent_mid)}")
            report_lines.append(f"💀 <b>[우하향 로직 만성질환 DNA]</b>\n ↳ {extract_cohort_dna(consistent_bad)}")
            
            # 💡 [자율 진화] 장기 우상향 DNA를 MFE 황금 타점으로 강제 흡수 (엔진 8과 시너지)
            if consistent_good:
                best_df = df_closed_30d[df_closed_30d['group'].isin(consistent_good)]
                current_config["DNA_SUPERNOVA_MFE_WEIGHTED"] = {
                    "cpv": round(best_df['dyn_cpv'].mean(), 3),
                    "tb": round(best_df['dyn_tb'].mean(), 3),
                    "bbe": round(best_df['v_energy'].mean(), 3),
                    "last_updated": datetime.now().strftime('%Y-%m-%d')
                }
                report_lines.append("✅ <b>조치:</b> 장기 우상향 DNA를 시스템의 황금 타점(MFE 템플릿)으로 강제 동기화 완료.")
        else:
            report_lines.append(" ▪️ 시계열 추적을 위한 청산 데이터가 아직 부족합니다.")
    except Exception as e:
        report_lines.append(f" ▪️ 시계열 분석 에러: {e}")

    # ---------------------------------------------------------
    # 👑 엔진 14: 인큐베이터 진화 심판 및 정규직 승격
    # ---------------------------------------------------------
    report_lines.append("\n🧪 <b>[V110.0 인큐베이터 진화 심판]</b>")
    try:
        incubator_templates = current_config.get("INCUBATOR_TEMPLATES", {})
        if isinstance(incubator_templates, dict) and incubator_templates:
            baseline_df = df[
                df['sig_type'].str.contains('STANDARD', na=False) &
                df['status'].str.contains('CLOSED', na=False)
            ] if all(col in df.columns for col in ['sig_type', 'status', 'final_ret']) else pd.DataFrame()

            if not baseline_df.empty:
                b_wr = len(baseline_df[baseline_df['final_ret'] > 0]) / len(baseline_df)
                b_pf = baseline_df[baseline_df['final_ret'] > 0]['final_ret'].sum() / (abs(baseline_df[baseline_df['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
            else:
                b_wr, b_pf = 0.0, 0.0

            promoted_name = None
            remove_keys = []
            for m_name, m_tpl in incubator_templates.items():
                tag = f"[INCUBATOR_{m_name}]"
                m_df = df[df['sig_type'].str.contains(tag, regex=False, na=False)] if 'sig_type' in df.columns else pd.DataFrame()
                if m_df.empty:
                    remove_keys.append(m_name)
                    continue

                oldest_raw = pd.to_datetime(m_df['entry_date'], errors='coerce').min() if 'entry_date' in m_df.columns else pd.NaT
                if pd.isna(oldest_raw):
                    age_days = 0
                else:
                    age_days = (datetime.now() - oldest_raw.to_pydatetime()).days
                m_closed = m_df[m_df['status'].str.contains('CLOSED', na=False)] if 'status' in m_df.columns else pd.DataFrame()
                if age_days < 30 or m_closed.empty:
                    report_lines.append(f"▪️ {m_name}: 생존 {age_days}일차 (심판 대기)")
                    continue

                m_wr = len(m_closed[m_closed['final_ret'] > 0]) / len(m_closed)
                m_pf = m_closed[m_closed['final_ret'] > 0]['final_ret'].sum() / (abs(m_closed[m_closed['final_ret'] <= 0]['final_ret'].sum()) + 0.1)

                # 💡 [100년 영속 진화 로직 적용: Synthetic Sandbox BlackSwan Gate]
                # 종가 -15% 충격 + 변동성 2배(수익률 스케일링) 가상 스트레스 테스트
                passed_synthetic_sandbox = False
                synthetic_pf = 0.0
                try:
                    synthetic_df = m_closed.copy()
                    # 💡 [100년 영속 진화 로직 적용: Synthetic Sandbox Severity Rebalance]
                    # 기존 (ret-15)*2는 과도한 학살을 유발하므로, 공황 쇼크(-15%) 후 변동성 2배를 ret*2-15로 완화 적용
                    synthetic_df['synthetic_ret'] = (pd.to_numeric(synthetic_df['final_ret'], errors='coerce') * 2.0) - 15.0
                    synthetic_df = synthetic_df.dropna(subset=['synthetic_ret'])
                    if not synthetic_df.empty:
                        syn_wins = synthetic_df[synthetic_df['synthetic_ret'] > 0]['synthetic_ret'].sum()
                        syn_loses = abs(synthetic_df[synthetic_df['synthetic_ret'] <= 0]['synthetic_ret'].sum()) + 0.1
                        synthetic_pf = float(syn_wins / syn_loses)
                        passed_synthetic_sandbox = synthetic_pf >= 1.0
                except Exception:
                    passed_synthetic_sandbox = False
                report_lines.append(
                    f"▪️ {m_name}: 승률 {m_wr*100:.1f}% | PF {m_pf:.2f} | Synthetic PF {synthetic_pf:.2f} "
                    f"({'통과' if passed_synthetic_sandbox else '탈락'})"
                )

                # 💡 [100년 영속 진화 로직 적용: Incubator Sample Threshold Relaxation]
                if m_pf >= 1.5 and m_wr >= 0.55 and m_pf > b_pf and m_wr > b_wr and len(m_closed) >= 7 and passed_synthetic_sandbox:
                    promoted_name = m_name
                else:
                    remove_keys.append(m_name)

            if promoted_name is not None:
                promoted_tpl = incubator_templates.get(promoted_name, {})
                # // [수정 전] "shape": [0.5] * 20  (평선 하드코딩)
                # // [수정 후] 승격 로직의 forward_trades 승리 청산 종목 진입일 종가 궤적 shape 평균
                evolved_shape = _mean_entry_shape_incubator_winners(promoted_name)
                if evolved_shape is None:
                    _tpl_sh = promoted_tpl.get("shape")
                    evolved_shape = _tpl_sh if isinstance(_tpl_sh, list) and len(_tpl_sh) == 20 else [0.5] * 20
                evo_key = f"DNA_ALPHA_EVO_{promoted_name}"
                current_config[evo_key] = {
                    "cpv": float(promoted_tpl.get("cpv", 0.0)),
                    "tb": float(promoted_tpl.get("tb", 0.0)),
                    "bbe": float(promoted_tpl.get("bbe", 0.0)),
                    "rs": float(promoted_tpl.get("rs", 0.0)),
                    "vcp": 1.0,
                    "vol": 1.0,
                    "ma": 0.0,
                    "shape": evolved_shape,
                    "passed_synthetic_sandbox": True
                }
                current_config["NEW_EVOLUTION_NAME"] = evo_key
                current_config["NEW_EVOLUTION_ACTIVE"] = True
                remove_keys.append(promoted_name)
                report_lines.append(f"🏆 <b>승격:</b> {promoted_name} ➔ [{evo_key}] 정규직 배치 완료")

            for k in set(remove_keys):
                incubator_templates.pop(k, None)
            current_config["INCUBATOR_TEMPLATES"] = incubator_templates
            report_lines.append(f"🗑️ 인큐베이터 정리 완료 (잔존 {len(incubator_templates)}개)")
        else:
            report_lines.append("▪️ 심판할 인큐베이터 템플릿이 없습니다.")
    except Exception as e:
        report_lines.append(f"⚠️ 인큐베이터 진화 심판 에러: {e}")

    # ==========================================
    # 🚀 최종 저장 및 발송 (중복 제거 완료)
    # ==========================================
    try:
        from evolution.fluid_evolution_bridge import run_fluid_evolution_weekend_hooks

        report_lines.extend(run_fluid_evolution_weekend_hooks(current_config, closed_df=df))
    except Exception as _fe_ex:
        report_lines.append(f"⚠️ Fluid evolution hooks skip: {_fe_ex}")

    _sync_inverse_mode_switch(current_config, vix_last, regime)
    save_config(current_config)
    send_telegram_report("\n".join(report_lines))
    print("✅ 분석 완료! JSON 파일 덮어쓰기 및 텔레그램 발송 성공.")

# ==========================================
# 👑 엔진 11: [V100.0 주간 흐름(Flow) 총결산 마스터 리포트]
# ==========================================
def send_weekly_flow_master_report():
    """WeeklyFlowSnapshot + ReportStateBinder SSOT (weekly_flow_report)."""
    try:
        from weekly_proprietary_regime import compute_weekly_proprietary_regime

        compute_weekly_proprietary_regime()
    except Exception as _pri_ex:
        print(f"⚠️ [주간 PRI Shadow] skip: {_pri_ex}")
    from weekly_flow_report import send_weekly_flow_master_report as _send_weekly

    _send_weekly(
        db_path=DB_PATH,
        sys_config=load_or_create_config(),
        send_fn=send_telegram_report,
    )

# ==========================================
# 일일 KR/US 종합 리포트(comprehensive_daily_report) SSOT: cron → factory.sh --daily-kr|--daily-us
# dante-factory --daemon 은 위성·유지보수만 담당 (16:30 하드코딩 알람 제거 — DUAL_EXECUTION_FIX)
# ==========================================

# ==========================================
# 🛰 위성 비블로킹 기동 (오토파일럿 메인 스레드의 GIL·장시간 블로킹 차단)
# ==========================================
_FACTORY_ROOT = os.path.dirname(os.path.abspath(__file__))


def _spawn_satellite_argv(argv: list, tag: str) -> None:
    """OS 분리 프로세스 — 반환 즉시 루프 진행. 로그는 satellite_{tag}.log"""
    log_path = os.path.join(_FACTORY_ROOT, f"satellite_{tag}.log")
    try:
        lf = open(log_path, "ab", buffering=0)
        try:
            subprocess.Popen(
                argv,
                cwd=_FACTORY_ROOT,
                stdin=subprocess.DEVNULL,
                stdout=lf,
                stderr=subprocess.STDOUT,
                close_fds=True,
                start_new_session=True,
            )
        finally:
            lf.close()
        print(f"🚀 [{tag}] 비블로킹 위성 기동 → {os.path.basename(log_path)}")
        time.sleep(5)
    except Exception as e:
        print(f"⚠️ [{tag}] 위성 기동 실패: {e}")


def _spawn_py_script(rel_name: str, tag: str) -> None:
    script = os.path.join(_FACTORY_ROOT, rel_name)
    if not os.path.isfile(script):
        print(f"⚠️ [{tag}] 스크립트 없음: {script}")
        return
    _spawn_satellite_argv([sys.executable, script], tag)


def _spawn_python_exec(code: str, tag: str) -> None:
    _spawn_satellite_argv([sys.executable, "-c", code], tag)


def _satellite_import_run_snippet(body_lines: str, tag: str) -> None:
    """동일 디렉터리 모듈 호출 (markets 등 인자 유지용)."""
    prefix = (
        "import sys, os\n"
        f"sys.path.insert(0, {repr(_FACTORY_ROOT)})\n"
        f"os.chdir({repr(_FACTORY_ROOT)})\n"
    )
    _spawn_python_exec(prefix + body_lines.strip() + "\n", tag)


# ==========================================
# 🕒 루프 실행기
# ==========================================
def system_main_loop():
    tz = pytz.timezone('Asia/Seoul')
    print("🕒 [완전 자율 오토파일럿 V12.0] 대기 중... (기준: 장부 최초 거래일 + 14일)")
    try:
        from factory_artifact_guard import ensure_factory_artifacts

        _boot_heal = ensure_factory_artifacts()
        print(f"🩹 [Self-Heal] autopilot boot: {_boot_heal}")
    except Exception as _boot_heal_err:
        print(f"⚠️ [Self-Heal] autopilot boot guard skipped: {_boot_heal_err}")

    while True:
        try:
            now = datetime.now(tz)
            # forward_trades 비어 있어도 watchdog(system_auto_pilot heartbeat) 유지
            try:
                ops_logger.record_heartbeat("system_auto_pilot")
            except Exception as _hb_err:
                print(f"⚠️ [오토파일럿] heartbeat: {_hb_err}")

            first_entry_date = get_first_entry_date()
            if first_entry_date is None:
                time.sleep(30)
                continue

            if (now.date() - first_entry_date).days >= WARMUP_DAYS:
                # 🇺🇸 매일 07:00 KST — US 독성 ML (블랙홀 08:30 전 us_toxic_ml_antipatterns.json 갱신)
                if now.hour == 7 and now.minute == 0:
                    print("🇺🇸 [오토파일럿] US 독성 부검(us_toxic_graveyard_analyzer) 비블로킹 기동…")
                    _spawn_py_script("us_toxic_graveyard_analyzer.py", "us_toxic_0700")
                    time.sleep(60)

                # 🌌 매주 토요일 00:00 — 합성 OHLCV 생성기 (무거움 → 프로세스 분리)
                elif now.weekday() == 5 and now.hour == 0 and now.minute == 0:
                    print("⏳ [오토파일럿] 정신과 시간의 방(합성 데이터 생성) 비블로킹 기동…")
                    _spawn_python_exec(
                        (
                            "import sys, os; sys.path.insert(0, %r); os.chdir(%r); "
                            "import synthetic_data_generator as S; "
                            "fn = getattr(S, 'stress_test_mutants', None); "
                            "(fn() if callable(fn) else S.generate_all_parallel_universes())"
                        )
                        % (_FACTORY_ROOT, _FACTORY_ROOT),
                        "synthetic_lab_sat0000",
                    )
                    time.sleep(60)

                # 🛡 매주 토요일 01:00 — 그림자 장부 성과 평가
                elif now.weekday() == 5 and now.hour == 1 and now.minute == 0:
                    print("🛡️ [오토파일럿] 그림자 장부 비블로킹 기동…")
                    _spawn_py_script("shadow_performance_tracker.py", "shadow_perf_sat0100")
                    time.sleep(60)

                # 🧬 매주 토요일 02:00 — 인큐베이터 (합성 돌연변이 평가)
                elif now.weekday() == 5 and now.hour == 2 and now.minute == 0:
                    print("🧬 [오토파일럿] incubator_engine 비블로킹 기동…")
                    _spawn_py_script("incubator_engine.py", "incubator_sat0200")
                    time.sleep(60)

                # ✅ 매주 토요일 03:00 — 뮤턴트 OOS 검증 (Hall of Fame 실데이터 게이트)
                elif now.weekday() == 5 and now.hour == 3 and now.minute == 0:
                    print("✅ [오토파일럿] mutant_oos_validator 비블로킹 기동…")
                    _spawn_py_script("mutant_oos_validator.py", "mutant_oos_sat0300")
                    time.sleep(60)

                # 🧬 토요일 03:10 — validated_live_mutants → PENDING_MUTANTS 브리지 (검증 프로세스와 분리·지연 안전)
                elif now.weekday() == 5 and now.hour == 3 and now.minute == 10:
                    print("🧬 [오토파일럿] mutant_pending_bridge (R&D 대기열 동기화) 비블로킹 기동…")
                    _spawn_python_exec(
                        (
                            "import sys, os; sys.path.insert(0, %r); os.chdir(%r); "
                            "from mutant_pending_bridge import sync_validated_json_into_pending as _s; "
                            "print(_s())"
                        )
                        % (_FACTORY_ROOT, _FACTORY_ROOT),
                        "mutant_pending_bridge_sat0310",
                    )
                    time.sleep(60)

                # 🧬 일요일 04:00 — 주간 알파 마이닝 (hunt_supernovas · cluster mining)
                elif now.weekday() == 6 and now.hour == 4 and now.minute == 0:
                    print("[오토파일럿] 주간 Alpha Mining Orchestrator 비블로킹 기동...")
                    try:
                        from alpha_mining_orchestrator import spawn_weekly_alpha_mining

                        spawn_weekly_alpha_mining(tag="alpha_mining_sun0400")
                    except Exception as _am_ex:
                        print(f"⚠️ [Alpha Mining] 기동 실패: {_am_ex}")
                    time.sleep(60)

                # 🔬 매일 06:00 — 미국 장 마감 직후 DNA 부검 (US 전용)
                elif now.hour == 6 and now.minute == 0:
                    print("🔬 [오토파일럿] limit_up_forensics US 비블로킹 기동…")
                    _satellite_import_run_snippet(
                        'import limit_up_forensics as L\nL.run_limit_up_forensics(markets=("US",))\n',
                        "limit_up_us_0600",
                    )
                    time.sleep(60)

                # 🚨 매일 08:00 — 거시 둠스데이 레이더 (스크립트 엔트리 = main())
                elif now.hour == 8 and now.minute == 0:
                    print("🚨 [오토파일럿] macro_doomsday_bot 비블로킹 기동…")
                    _spawn_py_script("macro_doomsday_bot.py", "doomsday_0800")
                    time.sleep(60)

                # 🔭 평일 09:05 — forensics_pioneer KR
                elif now.weekday() < 5 and now.hour == 9 and now.minute == 5:
                    print("🔭 [오토파일럿] forensics_pioneer KR 비블로킹 기동…")
                    _spawn_satellite_argv(
                        [sys.executable, os.path.join(_FACTORY_ROOT, "forensics_pioneer.py"), "KR"],
                        "forensics_kr_0905",
                    )
                    time.sleep(60)

                # 1. 토요일 10:00 — 자율 튜닝(뇌수술) — 동일 파일 서브프로세스
                elif now.weekday() == 5 and now.hour == 10 and now.minute == 0:
                    print("🚀 주말 관제탑 자율 튜닝(뇌수술) 비블로킹 기동…")
                    autop = os.path.join(_FACTORY_ROOT, "system_auto_pilot.py")
                    _spawn_satellite_argv(
                        [sys.executable, autop, "--run-autonomous-analysis-only"],
                        "autonomous_sat1000",
                    )
                    time.sleep(60)

                # 2. 토요일 10:05 — 주간 Flow 리포트 (가벼움·동기 유지)
                elif now.weekday() == 5 and now.hour == 10 and now.minute == 5:
                    try:
                        print("🚀 주간 흐름(Flow) 마스터 총결산 리포트를 발송합니다...")
                        send_weekly_flow_master_report()
                    except Exception as e:
                        print(f"⚠️ 주간 결과지 발송 실패(무시): {e}")
                    time.sleep(60)

                # 3. 토요일 10:10 — 타임머신 백테스트 (동일 인자 유지)
                elif now.weekday() == 5 and now.hour == 10 and now.minute == 10:
                    print("🚀 [오토파일럿] time_machine_backtester 비블로킹 기동…")
                    _satellite_import_run_snippet(
                        'import time_machine_backtester as T\n'
                        'T.run_time_machine_backtest('
                        '"COVID-19 코로나 폭락장", ["005930", "000660", "035420"])\n',
                        "time_machine_sat1010",
                    )
                    time.sleep(60)

                # 🔬 매일 11:50 — KR 상한가 부검
                elif now.hour == 11 and now.minute == 50:
                    print("🔬 [오토파일럿] limit_up_forensics KR (11:50) 비블로킹 기동…")
                    _satellite_import_run_snippet(
                        'import limit_up_forensics as L\nL.run_limit_up_forensics(markets=("KR",))\n',
                        "limit_up_kr_1150",
                    )
                    time.sleep(60)

                # 🔬 매일 15:40 — KR 상한가 최종 부검
                elif now.hour == 15 and now.minute == 40:
                    print("🔬 [오토파일럿] limit_up_forensics KR (15:40) 비블로킹 기동…")
                    _satellite_import_run_snippet(
                        'import limit_up_forensics as L\nL.run_limit_up_forensics(markets=("KR",))\n',
                        "limit_up_kr_1540",
                    )
                    time.sleep(60)

                # 💡 매일 16:10 — 스마트 머니 레이더
                elif now.hour == 16 and now.minute == 10:
                    print("🔄 [오토파일럿] smart_money_tracker 비블로킹 기동…")
                    _spawn_py_script("smart_money_tracker.py", "smart_money_1610")
                    time.sleep(60)

                # 🔬 평일 16:20 — KR 상한가 부검소
                elif now.weekday() < 5 and now.hour == 16 and now.minute == 20:
                    print("🔬 [오토파일럿] limit_up_forensics KR (16:20) 비블로킹 기동…")
                    _satellite_import_run_snippet(
                        'import limit_up_forensics as L\nL.run_limit_up_forensics(markets=("KR",))\n',
                        "limit_up_kr_1620",
                    )
                    time.sleep(60)

                # 4. 매일 17:00 — 둠스데이 레이더 (일일 종합 리포트는 cron factory.sh --daily-kr @ 16:35)
                elif now.hour == 17 and now.minute == 0:
                    print("🚨 [오토파일럿] macro_doomsday_bot (17:00) 비블로킹 기동…")
                    _spawn_py_script("macro_doomsday_bot.py", "doomsday_1700")
                    time.sleep(60)

                # 센티먼트·일일 종합 리포트: factory daily_audit 파이프라인(cron) SSOT.

                # 💀 매주 일요 02:00 — KR 독성 부검 (Graveyard ML)
                elif now.weekday() == 6 and now.hour == 2 and now.minute == 0:
                    print("💀 [오토파일럿] toxic_graveyard_analyzer 비블로킹 기동…")
                    _spawn_py_script("toxic_graveyard_analyzer.py", "toxic_kr_sun0200")
                    time.sleep(60)

                # 7. 매일 19:00 — KR 독성 부검 (일간)
                elif now.hour == 19 and now.minute == 0:
                    print("💀 [오토파일럿] toxic_graveyard_analyzer (일간) 비블로킹 기동…")
                    _spawn_py_script("toxic_graveyard_analyzer.py", "toxic_kr_1900")
                    time.sleep(60)

                # 🕳 매일 08:30 — 블랙홀 스캐너 (07:00 US ML 산출 이후)
                elif now.hour == 8 and now.minute == 30:
                    print("🕳️ [오토파일럿] blackhole_hunter 비블로킹 기동…")
                    _spawn_py_script("blackhole_hunter.py", "blackhole_0830")
                    time.sleep(60)

                # 🔭 매일 22:35 — forensics_pioneer US
                elif now.hour == 22 and now.minute == 35:
                    print("🔭 [오토파일럿] forensics_pioneer US 비블로킹 기동…")
                    _spawn_satellite_argv(
                        [sys.executable, os.path.join(_FACTORY_ROOT, "forensics_pioneer.py"), "US"],
                        "forensics_us_2235",
                    )
                    time.sleep(60)

                # 💡 매 루프(최대 1분 간격): ops_snapshot 시계열 + 인버스 스나이퍼
                _minute_ops_snapshot_and_inverse_cycle()

            time.sleep(30)
        except Exception as e:
            err_msg = f"🚨 <b>[오토파일럿 뇌수술 에러]</b> 주말 자율 학습 중 에러 발생:\n{e}"
            print(err_msg)
            send_telegram_report(err_msg)
            time.sleep(300) # 에러 후 5분 대기

def run_factory_cli(argv=None) -> int:
    """Ubuntu cron / factory.sh 단일 진입점."""
    import argparse

    from factory_pipelines import FACTORY_MODES, get_pipeline
    from factory_runtime import (
        dispatch_factory_mode,
        factory_exit_code,
    )

    parser = argparse.ArgumentParser(
        description="Dual-Screener Factory scheduler (unified entrypoint)",
    )
    parser.add_argument(
        "--mode",
        choices=sorted(FACTORY_MODES),
        help="Job pipeline to run once and exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List steps only; no lock, no side effects",
    )
    parser.add_argument(
        "--skip-telegram",
        action="store_true",
        help="Suppress factory PARTIAL_FAIL / lock notifications",
    )
    parser.add_argument(
        "--lock-timeout",
        type=float,
        default=None,
        help="Seconds to wait for factory_runtime.lock (daily_audit*: 7200, else 120)",
    )
    parser.add_argument(
        "--run-autonomous-analysis-only",
        action="store_true",
        help="Legacy: weekend MetaGovernor brain surgery only",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="24h system_main_loop (do not use from cron)",
    )
    args = parser.parse_args(argv)

    if args.run_autonomous_analysis_only:
        run_autonomous_analysis()
        return 0

    if args.daemon or (args.mode is None and not args.run_autonomous_analysis_only):
        if args.mode is not None:
            parser.error("Use either --mode or --daemon, not both")
        system_main_loop()
        return 0

    if args.mode is None:
        parser.error("Specify --mode <name> or --daemon")

    pipeline = get_pipeline(args.mode)
    print(f"🏭 [Factory] mode={args.mode} steps={[s.name for s in pipeline]}")
    lock_timeout = args.lock_timeout
    if lock_timeout is None:
        if str(args.mode or "").startswith("daily_audit"):
            lock_timeout = 7200.0
        else:
            try:
                from factory_scan_schedule import (
                    SCAN_LOCK_WAIT_SEC,
                    is_staggered_scan_mode,
                )

                if is_staggered_scan_mode(str(args.mode or "")) or str(
                    args.mode or ""
                ).startswith("scan_"):
                    lock_timeout = SCAN_LOCK_WAIT_SEC
                else:
                    lock_timeout = 120.0
            except Exception:
                lock_timeout = 120.0
    report = dispatch_factory_mode(
        args.mode,
        pipeline,
        send_fn=send_telegram_report,
        skip_telegram=args.skip_telegram,
        dry_run=args.dry_run,
        lock_timeout_sec=lock_timeout,
    )
    code = factory_exit_code(report)
    print(f"🏭 [Factory] finished status={report.status_label} exit={code}")
    if report.skipped_session and report.skipped_session_detail:
        print(f"🏭 [Factory] SKIPPED_SESSION: {report.skipped_session_detail}")
    if report.skipped_lock and report.skipped_lock_detail:
        print(f"🏭 [Factory] SKIPPED_LOCK: {report.skipped_lock_detail[:300]}")
    return code


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--run-autonomous-analysis-only":
            run_autonomous_analysis()
            raise SystemExit(0)
        raise SystemExit(run_factory_cli())
    system_main_loop()

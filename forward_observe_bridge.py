"""
가상매매 장부 관측 전용 편입 — 실매수·켈리·국고 없음 (MetaGovernor/Deathmatch 평가용).

- sig_type: [OBSERVE_ONLY][{strategy_id}] …
- sim_kelly_invest / shares / invest_amount = 0
- 둠스데이·오답노트·일일 쿼터 관문 우회 (표본 수집)
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Tuple

import pytz


def try_add_observe_forward_trade(
    *,
    market: str,
    code: str,
    name: str,
    sig_type: str,
    score: float,
    ep: float,
    strategy_id: str = "KR_BOWL",
    sector: str = "유망섹터",
    facts: Optional[dict[str, Any]] = None,
) -> Tuple[bool, str]:
    import sqlite3

    from auto_forward_tester import (
        DB_PATH,
        _insert_forward_trade_row,
        init_forward_db,
    )

    init_forward_db()
    mkt = str(market).upper()
    code_str = str(code).zfill(6) if mkt == "KR" else str(code)
    facts_d = dict(facts or {})

    try:
        score_f = float(score)
    except (TypeError, ValueError):
        score_f = 0.0
    try:
        ep_f = float(ep)
    except (TypeError, ValueError):
        ep_f = 0.0

    score_bucket = int(score_f // 10) * 10
    if score_bucket >= 100:
        score_bucket = 90
    tier_label = f"{score_bucket}점대"
    total_score = score_f * 10.0 if score_f <= 10.0 else score_f

    sig_body = str(sig_type or "").strip()
    sig_full = f"[OBSERVE_ONLY][{strategy_id}] {sig_body}".strip()

    tz = pytz.timezone("Asia/Seoul") if mkt == "KR" else pytz.timezone("America/New_York")
    today_str = datetime.now(tz).strftime("%Y-%m-%d")

    conn = sqlite3.connect(DB_PATH, timeout=60)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")
    try:
        cursor.execute(
            "SELECT id FROM forward_trades WHERE code=? AND status='OPEN'",
            (code_str,),
        )
        if cursor.fetchone():
            return False, "중복 OPEN 관측"

        insert_row = {
            "entry_date": today_str,
            "market": mkt,
            "code": code_str,
            "name": str(name),
            "sector": str(sector)[:64],
            "sig_type": sig_full,
            "tier": tier_label,
            "total_score": round(total_score, 2),
            "dyn_rs": float(facts_d.get("dyn_rs", 0) or 0),
            "dyn_cpv": float(facts_d.get("dyn_cpv", 0) or 0),
            "dyn_tb": float(facts_d.get("dyn_tb", 0) or 0),
            "is_death_combo": int(facts_d.get("is_death_combo", 0) or 0),
            "is_tenbagger": int(facts_d.get("is_tenbagger", 0) or 0),
            "entry_price": ep_f,
            "v_cpv": float(facts_d.get("v_cpv", 0) or 0),
            "v_yang": float(facts_d.get("v_yang", 0) or 0),
            "v_energy": float(facts_d.get("v_energy", 0) or 0),
            "v_rs": float(facts_d.get("v_rs", 0) or 0),
            "max_high": ep_f,
            "min_low": ep_f,
            "market_breadth": 1.0,
            "entry_breadth": 1.0,
            "entry_cos_score": float(facts_d.get("entry_cos_score", 0) or 0),
            "entry_dtw_score": float(facts_d.get("entry_dtw_score", 99.0) or 99.0),
            "entry_atr": float(facts_d.get("entry_atr", 0) or 0),
            "invest_amount": 0.0,
            "shares": 0,
            "sim_kelly_invest": 0.0,
            "entry_regime": str(facts_d.get("entry_regime", "OBSERVE_ONLY")),
            # [P3-1 스키마 드리프트 방지] forward/shared.py 의 _FORWARD_TRADE_INSERT_COLS 에
            # 교차검증 숫자 컬럼 5종이 추가되어 _insert_forward_trade_row 가 row[c] 로 전량
            # 조회한다 — 관측 전용 경로는 교차검증 팩터를 계산하지 않으므로 0.0 기본값으로 채운다.
            "flow_bonus": 0.0,
            "flow_divergence": 0.0,
            "short_net": 0.0,
            "fund_net": 0.0,
            "dart_net": 0.0,
        }
        _insert_forward_trade_row(cursor, insert_row)
        conn.commit()
        return True, f"관측 장부 등재: {name} ({code_str})"
    except sqlite3.Error as ex:
        conn.rollback()
        return False, f"DB_INSERT:{ex}"
    finally:
        conn.close()

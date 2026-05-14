import json
import os
import sqlite3
from datetime import datetime

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")


def load_config():
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_config(cfg):
    temp_path = f"{CONFIG_PATH}.temp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, CONFIG_PATH)


def scan_blackhole_targets():
    print("🕳️ [Bitget 블랙홀 헌터] Toxic DNA 군집 스캔 중...")
    cfg = load_config()
    anti = cfg.get("ANTI_PATTERNS", {})
    if not isinstance(anti, (dict, list)) or len(anti) == 0:
        cfg["BLACKHOLE_TOXIC_COUNT"] = {"count": 0, "symbols": [], "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M")}
        cfg["BLACKHOLE_SWITCH_SIGNAL"] = {"active": False, "action": "NONE"}
        save_config(cfg)
        print("💡 등록된 독성 패턴이 없어 스위칭 없음.")
        return

    conn = sqlite3.connect(DB_PATH, timeout=30)
    q = """
        SELECT symbol, final_ret, dyn_cpv, dyn_tb, v_energy, dyn_rs
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%' AND exit_date >= date('now', '-14 day')
    """
    df = pd.read_sql(q, conn)
    conn.close()
    if df.empty:
        print("⚠️ 최근 14일 청산 데이터가 부족합니다.")
        return

    df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0)
    toxic_df = df[df["final_ret"] <= -8.0].copy()
    toxic_symbols = toxic_df["symbol"].astype(str).unique().tolist()
    toxic_count = int(len(toxic_symbols))
    toxic_ratio = float(toxic_count / max(len(df["symbol"].unique()), 1))

    # 참사 군집이 일정 비율 이상이면 BTC 숏 방어 전환
    switch_on = (toxic_count >= 8) or (toxic_ratio >= 0.25)
    action = "BTC_SHORT" if switch_on else "NONE"
    msg = "⚠️ 시장 전반 Toxic 확산: BTC 숏 스위치 ON" if switch_on else "🟢 독성 군집 미약: 일반 모드 유지"

    cfg["BLACKHOLE_TOXIC_COUNT"] = {
        "count": toxic_count,
        "symbols": toxic_symbols[:50],
        "ratio": round(toxic_ratio, 4),
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
    }
    cfg["BLACKHOLE_SWITCH_SIGNAL"] = {
        "active": bool(switch_on),
        "action": action,
        "target_symbol": "BTC_USDT",
        "position_side": "SHORT" if switch_on else "NONE",
        "reason": msg,
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
    }
    save_config(cfg)
    print(f"✅ 완료: toxic_count={toxic_count}, toxic_ratio={toxic_ratio:.2%}, action={action}")


if __name__ == "__main__":
    scan_blackhole_targets()

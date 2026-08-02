# CAT-P · MegaTrend & Re-Evolution (Bitget)

> **위험도** 🟡 Medium · **Tier T2** · **also_load**: CAT-E, CAT-H, MAP  
> **never_with**: CAT-G regime write

---

## 1. 역할

Mega trend kill switch (background), re-evolution triggers, regime-extreme defense.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| mega trend kill BG | `trading/mega_trend_kill_bg.py` |
| doomsday | `doomsday_bot.py`, `trading/doomsday_gate.py` |
| regime relay | `trading/regime_capital_relay.py` |

---

## 3. Mega Trend Kill

- background process / daemon hook
- coordinates with doomsday DEFCON
- blocks or reduces long exposure on extreme trend

---

## 4. Claude 설계 대상

- kill trigger thresholds (config_kv)
- interaction with CAT-N execution gates
- re-evolution trigger after kill event (CAT-H)

---

## 5. CAT-E Link

Exit acceleration on mega trend signal — read kill state, don't write ledger schema

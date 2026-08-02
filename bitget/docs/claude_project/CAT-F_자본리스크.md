# CAT-F · 자본배분 & 리스크 (Bitget)

> **위험도** 🔴 **Critical** · **Tier T2** · **also_load**: CAT-G, CONSTANTS, MAP, SPOT-FUT  
> **never_with**: CAT-C scanner internals

---

## 1. 역할

Kelly sizing, Treasury SPOT/FUT, portfolio MDD breaker, deathmatch alloc (off), gates, concentration, leverage cap.

---

## 2. SSOT

| file | role |
|------|------|
| **`forward/ledger.py`** | Kelly at try_add, treasury debit/credit |
| **`governance/meta_consumer.py`** | Kelly merge, meta mult |
| **`trading/execution_safety.py`** | portfolio NAV MDD, MAX_LEVERAGE, real gates |
| `trading/concentration_gate.py` | position concentration |
| `trading/catastrophic_day_guard.py` | daily loss |
| `trading/doomsday_gate.py` | DEFCON |
| `trading/regime_capital_relay.py` | regime → capital |
| `trading/leverage_manager.py` | futures leverage |
| `trading/tail_risk_gate.py` | tail fund (**적립만**, P0-3) |
| `validation/regime_audit.py` | Kelly hard max 25% audit |

---

## 3. Kelly Chain (Bitget)

```
base = DYNAMIC_KELLY_RISK (config, default 0.01)
  → meta_consumer.apply_meta_kelly_merge:
      × META_GLOBAL_KELLY_MULT
      × META_NS_KELLY_MULT[ns]
      × META_GROUP_KELLY_MULT[group]
      → regime cap from meta_sync (DYNAMIC_KELLY_RISK sync)
  → template / bandit mult (if enabled)
  → sim_kelly_invest = min(cap, treasury available)
```

**Hard max (audit)**: 0.25 — **실매매 경로 강제 여부 확인 필요**

---

## 4. Treasury

| key | market |
|-----|--------|
| TREASURY_SPOT_USDT | spot |
| TREASURY_FUTURES_USDT | futures |

- ACCOUNT_SIZE_USDT: static anchor (partial compound)
- **Portfolio NAV**: SPOT + FUT sum — P0-2 circuit breaker target

---

## 5. Portfolio MDD Breaker (P0-2 · partial in execution_safety)

| tier | typical action |
|------|----------------|
| −15% | reduce size mult |
| −20% | block new entries |
| −30% | halt + alert |

**현행**: group −30% only — **portfolio layer incomplete**

---

## 6. Deathmatch Allocation

- `apply_deathmatch_allocation=False` (**의도적 off**)
- market key `BG` vs `SPOT`/`FUT` mismatch (CAT-H B-1)
- ranking informational only → bad strategies not starved

---

## 7. try_add Gate Order

→ CAT-D §4 (shared SSOT)

---

## 8. Claude 설계 대상 (work_phases 묶음A)

- A-1: Portfolio NAV MDD 3-tier breaker (complete execution_safety)
- A-2: Tail-risk fund → actual circuit trigger (P0-3)
- A-3: MAX_LEVERAGE enforce all paths (P0-4)
- A-4: Gross notional cap vs treasury (P1-5)
- A-5: Config write-time bounds (P1-4)

---

## 9. 🔴 Critical 변경 시

- 영향: D try_add, N real execution, G regime cap
- 롤백: config flag + tier revert
- 디렉터 승인 필수

---

## 10. 주식 performance_budget_governor

**Bitget 미이식** — 별도 설계. 루트 governor **import/수정 금지**. Adapter 필요 시 Bitget-native module.

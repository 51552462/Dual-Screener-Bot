# CAT-F · 자본배분 & 리스크

> **위험도** 🔴 **Critical** · **Tier T2** · **also_load**: CAT-G, CONSTANTS, MAP, KR-US  
> **never_with**: CAT-C scanner internals

---

## 1. 역할

Kelly merge chain, MAB, Live NAV, deathmatch alloc, try_add gates, circuit breaker.

---

## 2. SSOT

| file | role |
|------|------|
| **`forward/shared.py`** | Kelly chain execution at try_add |
| **`meta_governor_consumer.py`** | apply_meta_kelly_merge, weight clamp |
| `live_nav_manager.py` | NAV/HWM/MDD (`treasury_state.json`) |
| `mab_capital_allocator.py` | Thompson+UCB per arm |
| `template_bandit.py` | per-template Beta |
| `toxic_decay_bandit.py` | toxic forgiveness decay |
| `regime_kelly_failsafe.py` | UNKNOWN graceful |
| `strategy_promotion_engine.py` | lifecycle OBSERVING→LIVE |
| `evolution/deathmatch_*.py` | battle royale alloc |
| `capital_deathmatch.py` | report Kelly vs fixed |

---

## 3. Kelly Merge Chain (Claude 핵심)

```
Step0  base = DYNAMIC_KELLY_RISK (default 0.01)
       → regime_kelly_failsafe (UNKNOWN → [0.005,0.022])
Step1  × WEIGHT_S1 if S1/SUPERNOVA
       × WEIGHT_S4 if S4/눌림
Step2  × ts_mult  Thompson Beta(α+1,β+1)/0.5  clamp [0.20, 1.80]
Step3  × rotation 2.0 × spillover 1.5 × synthetic survival 1.5
       (incubator → notional=0)
Step4  apply_meta_kelly_merge:
       × META_GLOBAL_KELLY_MULT
       × doomsday_dampening: 1−(max(0,score−40)/60)^γ
       × META_NS_KELLY_MULT[ns] × META_GROUP_KELLY_MULT[group]
       → ACTION_BY_REGIME floor/cap
       → KILL_SWITCH / toxic bbox → 0
Step5  × template_bandit mult [0.10, 2.00]
Step6  cash brake:
       group_seed = ACCOUNT_SIZE(20M) + Σ realized PnL(group)
       available = group_seed − locked
       sim_kelly_invest = min(risk-parity notional, cap, available)
```

**Half-Kelly**: `f = W−(1−W)/R`, clamp(half×0.1, 0.002, 0.030) if n≥10 per regime

---

## 4. ACTION_BY_REGIME (meta_governor)

| regime | kelly_cap | W_S1 range | W_S4 range |
|--------|-----------|------------|------------|
| HIGH_VOL | 0.012 | 0.45–1.05 | 0.9–1.55 |
| BEAR | 0.010 | 0.35–0.95 | 1.05–1.75 |
| BULL | 0.028 | 1.0–1.85 | 0.55–1.15 |
| SIDEWAYS | 0.018 | 0.65–1.25 | 0.85–1.45 |
| UNKNOWN | 0.015 | 0.55–1.35 | 0.75–1.35 |

---

## 5. try_add Gate Order (변경 시 🔴 review)

1. GLOBAL_CIRCUIT_BREAKER (daily open loss ≤−5%)
2. DOOMSDAY DEFCON ≤2 → long block
3. toxic/anti bbox match
4. KILL_SWITCH → kelly=0
5. duplicate / same-day re-entry
6. **20 OPEN per market**
7. **4 per logic per day**
8. **sector 2 leader + 2 next**
9. available_cash ≤ 0
10. KR AUM penny brake (seed>50M & marcap<1000억)
11. zero-notional guard

---

## 6. Live NAV (treasury_state.json)

- seed: KR ₩3억 / US $30万
- compound: `E_t = E_{t-1}×(1 + f×R_t)`, f clamped
- `live_notional = live_nav × f`
- CENTRAL_TREASURY direct accounting **deprecated** — NAV engine 100%

**Tail Risk Shield**: weekly treasury×0.015 → TAIL_RISK_FUND; VIX≥35 → ×30 payoff  
`inverse_etf_sniper` — tail fund only, cap 30%

---

## 7. MAB & Bandit

| module | policy |
|--------|--------|
| mab | 90d lookback, exploit top 70% ×1.22, explore ×1.08, cap 1.45 |
| template_bandit | Beta posterior → mult; Capital Strangle wr<0.40 → 0.10 |
| toxic_decay | half-life 45d, block_floor 0.55, forgive 0.35 |

---

## 8. Deathmatch allocation

`run_battle_royal` → composite v2 (mdd_penalty **0.32**)  
Champion ×1.35 → `META_GROUP_KELLY_MULT` (cap 1.5)  
Gate: `DEATHMATCH_APPLY_ALLOCATION=1`

---

## 9. Strategy lifecycle

```
OBSERVING → CANDIDATE → LIVE → COOLED → RETIRED|CANDIDATE
```

Fast-track: INCUBATOR_/ACE_/MUTANT_ prefix & PF≥2.0 → LIVE  
KR: min trades 15, α-half 10d · US: 8, 30d

---

## 10. Claude 설계

- Kelly step 추가/순서 변경
- gate threshold
- deathmatch weights
- NAV compound f
- MAB exploit ratio

## 11. Cursor 구현

- try_add transaction, OCC with config, US FX 1350

---

## 12. Key config keys

`DYNAMIC_KELLY_RISK`, `WEIGHT_S1`, `WEIGHT_S4`, `META_GLOBAL_KELLY_MULT`, `META_GROUP_KELLY_MULT`, `GLOBAL_CIRCUIT_BREAKER`, `DEATHMATCH_APPLY_ALLOCATION`, `KILL_SWITCH`

*상수: CAT-CONSTANTS · regime: CAT-G*

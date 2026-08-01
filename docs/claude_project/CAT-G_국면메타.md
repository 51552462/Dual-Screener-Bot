# CAT-G · 국면판별 & 메타 거버넌스

> **위험도** 🔴 **Critical** · **Tier T2** · **also_load**: CONSTANTS, KR-US, MAP, CAT-F  
> **vocabulary SSOT**: BULL | BEAR | SIDEWAYS | HIGH_VOL (CHOP→SIDEWAYS)

---

## 1. 역할

5-factor predictive ensemble, 3 legacy detectors, MetaGovernor policy, PRI, analog memory, doomsday dampening input to Kelly.

---

## 2. 아키텍처 (4 layer)

| layer | module | output keys |
|-------|--------|-------------|
| **통합 SSOT** | `predictive_regime_ensemble.py` | REGIME_ENSEMBLE, KR/US_REGIME_KEY, CURRENT_REGIME_KEY |
| ① 결정트리 | `regime_meta_analyzer.py` | REGIME_ANALYSIS |
| ② 정책 | `meta_governor.py` | META_REGIME_KEY, META_REGIME_ACTION |
| ③ 자율관제 | `system_auto_pilot.run_autonomous_analysis` | weights, DYNAMIC_KELLY_RISK |
| sync | `meta_state_store.py` | triple-store |
| consume | `meta_governor_consumer.py` | Kelly merge (CAT-F) |

---

## 3. 5-Factor Ensemble (Claude 핵심)

**Factors** (each → [-1,+1] via tanh):  
`short_trend, long_trend, vix, breadth, pri`  
**Macro anchor**: `{long_trend, vix}` sum weight ≥ **0.15** (floor)

### Factor formulas

| factor | formula |
|--------|---------|
| short_trend | tanh(((close/ma20)−1)/0.03) |
| long_trend | tanh(((close/ma200)−1)/0.08) |
| vix | −tanh((vix−18)/8) |
| breadth | tanh((rsp/spy_ratio−1)/0.03) — **US only** |
| pri | clamp(pri_z, −1, 1) |

### Score & label

```
score = Σ w_f · s_f  (renorm if missing)
≥ +0.18 → BULL | ≤ −0.18 → BEAR | else SIDEWAYS
+ softmax probs
```

### Weight evolution (`evolve_weights`)

- record (date, market, states)
- after **5 days**: forward PnL reward per factor
- `reward_f = state_f × tanh(pnl%/4)`
- skill EMA (β=0.20) → softmax(skill, temp=2.2) → `project_weights`

### project_weights safety (8 iter)

- PRI ≤ **0.85** (excess redistributed)
- macro anchor ≥ 0.15

### Hysteresis & crisis

- default **2 days** consecutive before switch
- `is_vix_crisis`: vix≥30 OR (vix>p90 & ≥20)
- **US crisis → KR forced HIGH_VOL** (crisis_synced)

---

## 4. Live data (`collect_live_snapshots`)

| | US | KR |
|---|----|----|
| index | SPY MA20/EMA200 | ^KS11 |
| breadth | RSP/SPY | None (excluded) |
| vix | ^VIX + 60d p90 | shared global VIX |
| pri | PRI_US | PRI_KR |

---

## 5. Integration wiring

```
collect_live_snapshots → run_regime_ensemble → run_and_evolve
  → KR/US_REGIME_KEY, CURRENT_REGIME_KEY, REGIME_ENSEMBLE
analyze_market_regime(①) — ensemble overrides tree if valid
meta_governor(②) — VIX p90 promotion → META_REGIME_KEY
meta_state_store.sync — META → config_kv 1:1
auto_pilot(③) — _unified_regime_key prefers REGIME_ENSEMBLE
```

---

## 6. ① regime_meta_analyzer

Tree: vix>20→HIGH_VOL; narrow range→SIDEWAYS; both above MA20→BULL else BEAR  
**Ensemble blended overrides** when valid. Colosseum 45d top-4 blend.

---

## 7. ② meta_governor

7-step cycle. VIX p90 promotion (conf≥0.86).  
`resolve_asymmetric_treasury_lookback`: BEAR/HV 15–20d, BULL 120d.  
Triple-store persist.

---

## 8. ③ run_autonomous_analysis (weekend)

Local rules + VIX dynamic lookback (28→7d, 18→15d, else 45d).  
Extreme fear: S1=0, S4=2. Loads `{regime}_CHAMPION_PARAMS`.

---

## 9. meta_state_store

- normalize: CHOP/WHIPSAW→SIDEWAYS
- **Triple-store**: meta_state_log + config_kv META_GOVERNOR_STATE + JSON file
- sync: META→REGIME_ANALYSIS + CURRENT (15 retry)
- degraded >24h → rebuild

---

## 10. meta_learner

Internal PRI vs external macro trust:  
`W_internal(t+1) = clip(W + α·sign(winner)·tanh(|ΔPnL|/5), 0.20, 0.80)`  
Divergence days only, 7d PnL scoring. SSOT: `meta_trust_matrix.json`

---

## 11. PRI (shadow)

`weekly_proprietary_regime.py` → composite_z from pass_rate, mfe, mae, vol, dm pressure, starvation  
`pri_score = clip(50+15·composite, 0, 100)`  
Fed into ensemble pri factor + meta_learner. **Not live override alone.**

---

## 12. Analog & memory

| module | role |
|--------|------|
| regime_analog_engine | 6D vector vs HISTORICAL_EPISODES, Mahalanobis+DTW |
| regime_memory | black swan queue, 10y DTW archive, nice 19 |
| regime_self_heal | META≠config 3x → rebuild |
| shadow_macro_validator | counterfactual — **no META_ write** |

---

## 13. Doomsday layer

| module | role |
|--------|------|
| macro_doomsday_bot | radar score |
| doomsday_dampener | Kelly mult γ evolution |
| doomsday_bridge | scan halt bridge |

DEFCON ≤2 → CAT-F long block.  
γ ∈ [0.5, 3.0] weekly self-evolve.

---

## 14. VIX thresholds (cross-ref)

| threshold | module |
|-----------|--------|
| 18/8/30 | ensemble tanh |
| 20 | meta_analyzer HIGH_VOL |
| p90 dynamic | governor promotion |
| 28/18 | auto_pilot lookback |
| 35 | tail fund 30× |

---

## 15. Claude 설계

- factor weights/evolution
- hysteresis days
- crisis sync rules
- PRI cap usage
- ACTION_BY_REGIME (→ CAT-F)

## 16. Cursor 구현

- triple-store retry, JSON atomic write, snapshot collection

---

## 17. Key config keys

`CURRENT_REGIME_KEY`, `META_REGIME_KEY`, `REGIME_ENSEMBLE`, `REGIME_ANALYSIS`, `META_GOVERNOR_STATE`, `INVERSE_MODE_ACTIVE`

*상수: CAT-CONSTANTS*

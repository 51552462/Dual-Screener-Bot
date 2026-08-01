# CAT-E · 청산 엔진

> **위험도** 🔴 High · **Tier T2** · **also_load**: CONSTANTS, CAT-D( schema ), MAP  
> **math SSOT**: `exit_dynamics.py` (pure, no I/O)

---

## 1. 역할

OPEN 포지션 일일 청산 평가. P1~P3 priority ladder. Fluid partial, runner trail, pyramid, RL κ.

---

## 2. SSOT

| file | role |
|------|------|
| `forward/ledger.py` | ladder execution |
| **`exit_dynamics.py`** | F_out, κ, pyramid, blend (pure math) |
| `exit_ratchet_rl.py` | weekly κ evolution |
| `evolution/ace_exit_bridge.py` | ACE hold extension |
| `elastic_threshold.py` | entry cutoff (CAT-C boundary) |

---

## 3. 입력 계산

```
current_ret = (close - ep)/ep × 100
low_ret, high_ret from min_low, max_high
ns params: {ns}_TIME_STOP=10, {ns}_ATR_SL=2.0
DYNAMIC_MAE_SL=-3.5%, DYNAMIC_MFE_TP=+10.0%
ATR stop = ep - (ATR_SL × entry_atr)
```

**Breadth emergency** (<0.97): time_stop×0.5, all MAE_SL×0.5

---

## 4. 우선순위 사다리 (first match wins)

| P | trigger | exit_type |
|---|---------|-----------|
| **P1** | low_ret ≤ MAE_SL | STAT_MAE |
| **P1** | high_ret ≥ MFE_TP | fluid partial or STAT_MFE_FULL |
| **P1b** | free_runner & low ≤ trail | RUNNER_TRAIL |
| **P1c** | pyramid conditions | child row |
| RL ext | holding_edge>1.5 | time_stop +2 |
| **P2** | HYBRID/TECH/STAT modes | HYBRID_* / TECH / STAT |
| **P3** | bars ≥ 2×time_stop | ZOMBIE_FORCE_CLOSE @ ep |

---

## 5. exit_dynamics.py (Claude 수학 SSOT)

| function | policy |
|----------|--------|
| `fluid_scale_out_fraction(regime,vol,edge)` | DEF 0.78 / BULL 0.18 / CHOP 0.45 |
| `convex_ratchet_kappa(run_ret,state)` | κ: 0.12→0.05, convex prog |
| `update_ratchet_kappa_rl(..., eta=0.04)` | Δ=η·(whipsaw−giveback) |
| `pyramid_decision(...)` | bull & edge≥1.5 & adds<3, cap nav×0.10 |
| `blend_final_return(partial,frac,runner)` | partial+(1−frac)×runner |

**trail**: `trail_px = max_high × (1 − κ)`, exit if low ≤ trail_px

---

## 6. exit_ratchet_rl (weekly)

CLOSED runner trades:
- `giveback = mean((mfe−final)/mfe)`
- `whipsaw = early trail cut rate`
- n≥3 → update `EXIT_RATCHET_STATE`

---

## 7. ACE exit bridge

Gate: `ENABLE_ACE_EVOLUTION_WEIGHTING`  
Overrides: `time_stop_mult`, `mfe_tp_relax_pct`, tag `#에이스진화_보유연장`

---

## 8. PnL at close

```
ret = (actual_exit_price - ep)/ep × 100
if partial and not STAT_MFE_FULL: ret = blend_final_return(...)
```

**actual_exit_price** (not always close):
- MAE → ep×(1+mae/100)
- MFE full → ep×(1+tp/100)
- runner → trail_px
- ATR → sl_price
- zombie → ep

**currency PnL** = `sim_kelly_invest × final_ret/100`

---

## 9. Claude 설계

- ladder priority reorder (보수적)
- F_out by regime
- κ convexity params
- RL reward definition
- breadth emergency threshold

## 10. Cursor 구현

- ledger branching, DB UPDATE atomicity, ace bridge wiring

---

*상수: CAT-CONSTANTS · partial fields: CAT-D*

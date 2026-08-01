# CAT-H · 진화 엔진 (HTC — 정신과 시간의 방)

> **위험도** 🟡 Medium (live merge=🔴) · **Tier T2** · **also_load**: CONSTANTS, CAT-K, MAP  
> **격리 원칙**: 합성 R&D ↔ live capital ring-fenced

---

## 1. 역할

합성시장 → GP 변이 → OOS 실데이터 검증 → **INCUBATOR_TEMPLATES** 자동 주입. 실패 → toxic/immune.

---

## 2. 주말 파이프라인 (KST satellite)

| time | module | output |
|------|--------|--------|
| 토 00:00 | synthetic_data_generator | synthetic_market.sqlite |
| 토 01:00 | shadow_performance_tracker | SHADOW_PERFORMANCE |
| 토 02:00 | incubator_engine + genetic_expr_builder | mutant_hall_of_fame.json |
| 토 03:00 | mutant_oos_validator | validated_live_mutants.json |
| 토 03:10 | mutant_pending_bridge | PENDING_MUTANTS |
| 토 03:20 | clustered_immune_vaccine | ANTI_PATTERNS compress |
| 일 04:00 | alpha_mining_orchestrator | clusters, alpha factors |
| ongoing | template_evolution, time_machine_backtester | morph, validate |

---

## 3. synthetic_data_generator

HMM 4 regimes: SIDEWAYS / BULL / BEAR / BLACK_SWAN  
Merton jump-diffusion, 100 tickers × 1000 days, drift≈0 corrected  
Schema: `synthetic_ohlcv`, `synthetic_meta`

---

## 4. incubator_engine

- `MutantDataCube` from sqlite
- `is_safe_expression` — **AST whitelist only** (no Call/import)
- eval: fwd1 return, sharpe, expectancy, win_rate
- rank: n≥MIN_SIGNALS → sharpe → expectancy → n
- output: hall_of_fame + full_scoreboard(200)

---

## 5. genetic_expr_builder

Gene pool: PRICE_VARS, VOL_VARS, ret1, body, ratios  
**EvolutionGear** by regime + champion_survival_rate:

| gear | cx/mut/rnd | elite |
|------|------------|-------|
| BULL/SIDEWAYS | 0.80/0.13/0.07 | 12 |
| BEAR/HV/SWAN | 0.35/0.45/0.20 | 6 |

Panic shift: `(1−survival)×0.40` → mutation↑  
Population default: **1000**

---

## 6. mutant_oos_validator (live gate)

- RO market DB, KR_% + US_% samples, ≥130 bars
- **Promotion**: excess>0.00005 & wr>0.50 & n≥30
- chain: validate → sync_pending → **auto_merge** (if passed)

---

## 7. mutant_pending_bridge

- `GP_MUTANT_MAX_LIVE = **24**`
- `auto_merge_validated_into_incubator`: `INCUBATOR_TEMPLATES["GP_MUT_*"]`, cos_cutoff 0.99, EXPLORE_MIN_KELLY, `init_exploration_arm(0.10)`
- Manual gate: `APPROVE_PENDING_MUTANTS_TO_INCUBATOR` (default False for auto path)

---

## 8. alpha_mining & template_evolution

| module | output |
|--------|--------|
| alpha_mining_orchestrator | LIVE_CLUSTER, UNDERDOG, EVOLVED_ALPHA |
| template_evolution | morph_templates (EMA α=0.2), transcendent evolution |
| dna_mutator | DNA mutation |
| time_machine_backtester | all REGIME_PERIODS validation |

---

## 9. evolution/ package (ACE + deathmatch)

- `ace_evolution_*` — live bridges, schema, store, telegram
- `deathmatch_battle_royale`, `deathmatch_allocation` → CAT-F
- `champion_genesis`, `fluid_evolution_bridge`
- `us_fluid_upstream_bridge` — US fluid health

Gate: `ENABLE_ACE_EVOLUTION_WEIGHTING`

---

## 10. strategy_promotion (CAT-F boundary)

INCUBATOR → CANDIDATE (capital 0) → LIVE (fast-track PF≥2.0) → COOLED  
Failure → `register_failed_template` → CAT-I

---

## 11. KR/US HTC

- **Synthetic chamber**: shared (market-agnostic)
- **OOS**: both KR_% and US_% tables
- **Toxic**: **separate** KR config vs US JSON (CAT-I)
- Live scan: same INCUBATOR_TEMPLATES, per-market universe

---

## 12. Claude 설계

- HMM transition matrix
- GP gear / panic shift
- OOS promotion thresholds
- MAX_LIVE cap policy
- ACE weighting gates

## 13. Cursor 구현

- AST sandbox, cube memory, auto_merge idempotency

---

## 14. Key config keys

`INCUBATOR_TEMPLATES`, `PENDING_MUTANTS`, `LIVE_CLUSTER_TEMPLATES`, `UNDERDOG_CLUSTER_TEMPLATES`, `DNA_ALPHA_*`, `ENABLE_ACE_EVOLUTION_WEIGHTING`, `APPROVE_PENDING_MUTANTS_TO_INCUBATOR`

*상수: CAT-CONSTANTS · immune: CAT-I*

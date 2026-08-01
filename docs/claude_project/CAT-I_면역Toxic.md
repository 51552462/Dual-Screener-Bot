# CAT-I · 면역 & Toxic 방어

> **위험도** 🔴 High · **Tier T2** · **also_load**: CONSTANTS, KR-US, MAP  
> **KR/US toxic storage 완전 분리**

---

## 1. 역할

ANTI_PATTERNS, TOXIC_ML rules, bbox matching at scan + Kelly kill, graveyard mining, cluster compression, decay forgiveness.

---

## 2. SSOT

| file | role |
|------|------|
| **`toxic_antipattern_core.py`** | bbox matcher SSOT |
| `toxic_graveyard_analyzer.py` | KR weekly/daily → config |
| `us_toxic_graveyard_analyzer.py` | US → isolated JSON |
| `toxic_decay_bandit.py` | half-life forgiveness |
| `clustered_immune_vaccine.py` | cap 500→64 centroids |
| `immune_evolution.py` | immune evolution |

---

## 3. Toxic definition (graveyard)

**Loss toxic**: final_ret ≤ **−7%**  
**Or** exit STAT_MAE/ZOMBIE AND ≤ **−4%**

DecisionTree (depth 3, purity≥0.8) → TOXIC_ML_ANTIPATTERNS + ANTI_PATTERNS  
TTL: **90 days**

---

## 4. bbox matching

`collect_merged_antipattern_rules` — ANTI + TOXIC_ML  
`evaluate_toxic_bbox_match` / `any_toxic_rule_matches`  
**Cosine ≥ 0.85** → match → scan: TOXIC_TRAP; Kelly: **0**

3D vec fields: dyn_cpv, dyn_tb, v_energy (min/max bbox)

---

## 5. KR vs US (CAT-KR-US)

| | KR | US |
|---|----|----|
| storage | system_config (Korean sectors) | `us_toxic_ml_antipatterns.json` |
| merge | — | **never into KR config** |
| schedule | daily 19:00 + weekly | daily 07:00 |

---

## 6. clustered_immune_vaccine

When ANTI_PATTERNS > **500**:
- KMeans → Agglomerative fallback → quantile
- target **64** centroids
- `_span_bbox` per cluster → vaccine rules
- `register_failed_template` from promotion failures
- Saturday 03:20 maintenance

---

## 7. toxic_decay_bandit

```
decay = 0.5^(age / half_life)   half_life=45d
block_floor=0.55, forgive_ceil=0.35
forgiveness_scout max 8%
```

Allows aged rules to soften — balances false blocks.

---

## 8. Integration points

| consumer | usage |
|----------|-------|
| CAT-C supernova | TOXIC_ML_TREE funnel, ANTI_TOXIC, bbox trap |
| CAT-F try_add | bbox → kelly=0 |
| CAT-H failure | register_failed_template |

**Do not** edit scanner logic from CAT-I — call `toxic_antipattern_core` only.

---

## 9. Claude 설계

- toxic loss thresholds
- cosine match threshold
- cap/centroid targets
- decay half-life policy
- KR/US separation invariant

## 10. Cursor 구현

- sklearn KMeans version, JSON path, config OCC updates

---

## 11. Key config keys

`ANTI_PATTERNS`, `TOXIC_ML_ANTIPATTERNS`, US JSON file (not config_kv)

*상수: CAT-CONSTANTS*

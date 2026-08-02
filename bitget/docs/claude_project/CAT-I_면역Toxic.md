# CAT-I · 면역 & Toxic (Bitget)

> **위험도** 🔴 High · **Tier T2** · **also_load**: CAT-C, CAT-F, MAP  
> **never_with**: CAT-C scanner scoring rewrite

---

## 1. 역할

Toxic pattern detection, anti-pattern bbox, graveyard analyzer, decay bandit, try_add block hooks.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| toxic analyzer | `toxic_graveyard_analyzer.py` |
| anti-pattern eval | toxic hooks in try_add path |
| shadow tracking | `shadow_performance_tracker.py` |
| pump forensics | `pump_forensics.py` |

---

## 3. 인터페이스 (CAT-C, D)

```
evaluate_toxic / anti_pattern → block or kelly=0 at try_add
```

**금지**: scanner 본체 scoring 로직을 I에서 직접 수정

---

## 4. Toxic Decay

- half-life ~45d (align CONSTANTS if Bitget-specific override)
- forgiveness via bandit (if enabled)

---

## 5. Claude 설계 대상

- toxic ML bbox thresholds (config_kv)
- correlation with CAT-C bad tick filter (P1-6)
- shadow-only promotion before live block

---

## 6. Single Writer

ANTI_PATTERNS / TOXIC_ML config — I modules write, C/F read

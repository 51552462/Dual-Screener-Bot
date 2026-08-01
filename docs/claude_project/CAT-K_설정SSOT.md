# CAT-K · 설정 SSOT (중추신경계)

> **위험도** 🔴 High · **Tier T2** · **also_load**: MAP  
> **all modules read; OCC write only via config_manager**

---

## 1. 역할

`system_config.sqlite::config_kv` — single KV brain. Versioned OCC. Runtime cache. Lifecycle defaults.

---

## 2. SSOT

| file | role |
|------|------|
| **`config_manager.py`** | get/set/update_config_value |
| `system_config_atomic.py` | legacy JSON atomic bridge |
| `strategy_lifecycle_config.py` | KR/US/BG lifecycle defaults |

---

## 3. Schema

```sql
config_kv(key TEXT PK, value_json TEXT, version INTEGER)
```

WAL + OOM-safe pragmas.

---

## 4. API (Claude: key semantics / Cursor: impl)

| API | use |
|-----|-----|
| `get_config_value(key, default)` | read |
| `set_config_value(key, val)` | version+1 write |
| **`update_config_value(key, fn)`** | **OCC**: read(val,ver)→modify→UPDATE WHERE version=? |
| `load_system_config()` | full merge + autoinit ~40 keys |
| `load_runtime_system_config(ttl=60)` | cached read |
| `invalidate_runtime_system_config_cache()` | after write |

**ConfigConcurrencyError** if rowcount≠1 → retry policy (Cursor)

---

## 5. Security

- `_SENSITIVE_KEY_RE` (TOKEN/SECRET/...) — scrub on load/save
- `set_config_value` **rejects** secret keys → .env only

---

## 6. Autoinit keys (examples)

| key | default purpose |
|-----|-----------------|
| ENABLE_SPILLOVER_FALLBACK | spillover |
| PREDICTED_NEXT_SECTOR_{KR,US} | rotation |
| DEATHMATCH_APPLY_ALLOCATION | 1 |
| PENDING_MUTANTS | [] |
| APPROVE_PENDING_MUTANTS_TO_INCUBATOR | False |
| ENABLE_ACE_EVOLUTION_WEIGHTING | ace |
| CROSS_MARKET_SSOT | cross market |
| INCUBATOR_TEMPLATES | CAT-H |
| ANTI_PATTERNS | CAT-I |

**New key rule**: UPPER_SNAKE, JSON-serializable, document in Handoff table.

---

## 7. Snapshots

`save_system_config` → daily `config_snapshots/` backup (bulk replace — Cursor caution)

---

## 8. Claude 설계

- new key name + default + which CAT owns write
- value schema (dict shape)
- regime-specific bounds (coordinate CAT-G/F)
- migration from JSON file → config_kv

## 9. Cursor 구현

- OCC retry loop, cache invalidation, sensitive scrub

---

## 10. Cross-CAT write ownership

| key family | writer CAT |
|------------|------------|
| REGIME_*, META_* | G |
| INCUBATOR_*, PENDING_* | H |
| ANTI_PATTERNS, TOXIC_* | I |
| DYNAMIC_KELLY*, WEIGHT_S* | F/G |
| DEATHMATCH_* | F/H |

**Never** direct SQL from other modules — always config_manager.

---

*lifecycle defaults KR10/US30 alpha: strategy_lifecycle_config.py*

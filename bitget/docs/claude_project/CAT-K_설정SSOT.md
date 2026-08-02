# CAT-K · 설정 SSOT (Bitget)

> **위험도** 🔴 High · **Tier T2** · **also_load**: MAP, CONSTANTS  
> **never_with**: CAT-B schema, multiple CAT config redesign

---

## 1. 역할

All runtime parameters: SQLite config_kv SSOT, bootstrap, config_hub facade, write validation (target).

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| SQLite SSOT | `infra/config_manager.py` |
| public facade | `config_hub.py` |
| JSON bootstrap | config_manager internal only |
| architecture check | `validation/architecture_checks.py` |

**금지**: `bitget_system_config.json` 직접 read/write (legacy)

---

## 3. Critical Keys

| key | writer CAT |
|-----|-----------|
| DYNAMIC_KELLY_RISK | G (meta_sync) |
| CURRENT_REGIME_KEY | G |
| ENABLE_REAL_EXECUTION | K / ops |
| MAX_LEVERAGE | K |
| TREASURY_* | D,F |
| INCUBATOR_TEMPLATES | H |
| META_* | G |

---

## 4. Write Validation (P1-4 · A-5 구현됨)

**SSOT**: `set_config_value` → `CONFIG_WRITE_REJECT_BOUNDS` (`config_bounds.py`)

| KEY | reject bound | note |
|-----|--------------|------|
| DYNAMIC_KELLY_RISK | [0.002, 0.030] | reject — not clamp |
| MAX_LEVERAGE | [1, 10] | independent of A-3 ops cap 5 |

Kill-switch: `CONFIG_WRITE_VALIDATION_ENABLED` (default true; false → legacy clamp path).
Keys not in table → pass through unchanged.

---

## 5. Claude 설계 대상

- config write validator module (non-breaking default)
- migration: JSON → SQLite one-time
- config change audit log (P2 governance)
- OCC for concurrent pipeline + daemon writes

---

## 6. Single Writer Principle

ALL config_kv writes → `config_manager.set_config_value` — scattered dict mutation **금지**

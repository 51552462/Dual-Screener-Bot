# CAT-O · Practitioner (Bitget)

> **위험도** 🟢 Low · **Tier T2** · **also_load**: CAT-C, MAP  
> **never_with**: root practitioner_intelligence modification

---

## 1. 역할

Practitioner rules adapter, Bitget symbol/market normalization, rule scoring integration.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| adapter | `forward/practitioner_bitget_adapter.py` |
| intelligence core | root `practitioner_intelligence` (**read-only**) |
| config | `config_hub` |

---

## 3. Rules Inventory

- ~30 practitioner rules (root)
- 5 core signal engines (Bitget `signal_engines.py`)
- new rule types require **code change** — param mutation only is automatic

---

## 4. Interface (CAT-C)

```
practitioner_bitget_adapter → normalized candidate → try_add
```

---

## 5. Claude 설계 대상

- Bitget-native rule extension without root fork
- practitioner score → Kelly weight hook (read F chain)
- shadow enrollment before live

---

## 6. Import Rule

**금지**: copy-paste root practitioner into bitget/ — adapter pattern only

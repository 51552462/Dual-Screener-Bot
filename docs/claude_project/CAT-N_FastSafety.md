# CAT-N · Fast Safety (독립 서브시스템)

> **위험도** 🔴 **Critical** · **Tier T2** · **also_load**: CAT-F, CAT-C, MAP  
> **원칙**: production Kelly/scan에 **shadow 먼저** — direct gate 변경 금지 without review

---

## 1. 역할

Policy admin, shadow runtime, audit queue, supernova safety invariants — parallel safety layer for supernova lifecycle.

---

## 2. Module map

| file | role |
|------|------|
| `fast_safety_kernel.py` | core kernel |
| `fast_safety_policy_store.py` | policy persistence |
| `fast_safety_policy_admin.py` | admin API |
| `fast_safety_policy_admin_cli.py` | CLI |
| `fast_safety_policy_admin_artifacts.py` | artifacts |
| `fast_safety_runtime_shadow.py` | shadow runtime |
| `fast_safety_shadow_activation.py` | activation gates |
| `fast_safety_snapshot_builder.py` | state snapshots |
| `fast_safety_strategy_identity.py` | strategy ID |
| `fast_safety_audit_queue.py` | audit queue |
| `fast_safety_audit_runtime.py` | audit runner |
| `fast_safety_ops_sink.py` | ops events sink |

Tests: root `test_fast_safety_*.py` (isolated from tests/)

---

## 3. Integration model

```
CAT-C supernova scan
  ⇢ fast_safety_runtime_shadow (observe)
  ⇢ audit_queue record
Production gate OFF by default until shadow invariants pass
```

**CAT-N → CAT-F**: shadow flags only — **not** production Kelly overwrite  
**CAT-N → CAT-G**: read regime — no META write

---

## 4. Claude 설계

- shadow vs production activation criteria
- audit queue severity levels
- policy schema (what fields)
- supernova lifecycle invariants (when safe to promote)
- rollback: disable shadow activation flag

## 5. Cursor 구현

- wiring in supernova_hunter, ops_sink, config_kv atomic tests

---

## 6. Change protocol (🔴)

1. Claude spec + invariant list  
2. Shadow-only deploy  
3. audit queue clean N days  
4. Director approval → production gate  
5. CAT-F/G cross-review

---

*Never reference bitget/ fast safety variants*

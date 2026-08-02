# CAT-H · 진화 & HTC (Bitget)

> **위험도** 🟡 Medium · **Tier T2** · **also_load**: CAT-F, CAT-K, MAP  
> **never_with**: CAT-G meta_sync write path

---

## 1. 역할

Deathmatch, champion genesis, strategy registry lifecycle, GP/incubator, weekly evolution, OOS partial.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| deathmatch report | evolution / deathmatch modules |
| champion genesis | `evolution/champion_genesis_bg.py` ✅ 2026-07 |
| weekly evolution | `weekly_evolution` pipeline mode |
| registry lifecycle | meta_governor `_step_lifecycle` (Bitget DB) |
| time machine | `time_machine_backtester.py` |
| synthetic data | `synthetic_data_generator.py` |

---

## 3. Deathmatch (현행)

| 항목 | 값 |
|------|-----|
| apply_deathmatch_allocation | **False** |
| market key | `BG` vs `SPOT`/`FUT` **불일치** |
| effect | ranking only — capital not reallocated |

**P1-1**: enable alloc after key fix (work_phases B-2)

---

## 4. Champion Genesis ✅

- Bitget DB `champion_precursor_genesis` isolated
- Hook: `deathmatch_report_section.py`
- daily/weekly pipeline connected

---

## 5. Lifecycle Bugfix ✅ (2026-07)

- `forward_db_path=None` → stock DB pollution **fixed**
- fallback: `ctx.forward_db_path or ctx.bitget_db_path`

---

## 6. OOS / Walk-Forward

| | 상태 |
|---|------|
| engine6 14d OOS | partial |
| full pipeline walk-forward | **없음** (P2-1) |
| 90d rolling window | effectively in-sample |

---

## 7. Claude 설계 대상 (work_phases 묶음B)

- B-1: market key normalization (`BG` → `SPOT`/`FUT`)
- B-2: `apply_deathmatch_allocation=True` rollout + shadow period
- B-3: walk-forward pipeline integration
- B-4: registry lifecycle + MAB explore budget

---

## 8. INCUBATOR (CAT-K)

Writer: H via `config_manager.set_config_value("INCUBATOR_TEMPLATES")`  
Reader: C scanners

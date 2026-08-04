# CAT-J · 리포팅 (Bitget)

> **위험도** 🟢 Low · **Tier T2** · **also_load**: CAT-D, MAP  
> **never_with**: CAT-D track/close logic

---

## 1. 역할

Daily audit reports, weekend grand report, weekly action plan, deathmatch sections, telegram formatting.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| forward reports | `forward/reports.py` |
| weekend report | `weekend_grand_report.py` |
| weekly plan | `weekly_action_plan.py` |
| D-3a cost weekly | `observability/cost_report_bg.py` → `cost_report_weekly` ops |
| deathmatch section | evolution report hooks |
| root reports import | `reports/*` (read-only) |

---

## 3. Daily Audit Flow (CAT-A)

```
sentiment → track → deep_dive → comprehensive → overseer → reconcile
```

mode: `daily_audit` in `bitget_pipelines.py`

---

## 4. Treasury Display (SPOT/FUT)

- `TREASURY_SPOT_USDT` / `TREASURY_FUTURES_USDT` separate lines
- funding: **미표시** (P1-3)

---

## 5. Claude 설계 대상

- champion genesis section (✅ connected)
- funding fee line item
- portfolio MDD tier in daily header
- cost monitoring section (P2-6) — **D-3a 구현 ✅** (weekly ops `cost_report_weekly`)

### D-3a cost line 해석 (2026-08-04)

- `gemini_call_count`는 **실 API 청구 호출 수가 아닐 수 있음** — `gemini_call_count_source=llm_call_cache_proxy`이면 `llm_call_cache.sqlite` 행 수
- USD 비용·exchange fee는 **의도적 null** (단가/요율 SSOT 없음) — 디렉터가 단가를 정하면 별도 Handoff
- D-3b parity는 **아직 리포트에 미포함** (scaffold only · `PARITY_MONITOR_ENABLED=false`)

---

## 6. Read-Only Rule

J **never** writes forward_trades — hydrate + read only

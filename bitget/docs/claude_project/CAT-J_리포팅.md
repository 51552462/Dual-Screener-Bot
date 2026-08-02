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
- cost monitoring section (P2-6)

---

## 6. Read-Only Rule

J **never** writes forward_trades — hydrate + read only

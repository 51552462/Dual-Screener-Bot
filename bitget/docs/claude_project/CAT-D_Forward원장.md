# CAT-D · Forward 원장 (Bitget)

> **위험도** 🔴 High · **Tier T2** · **also_load**: MAP, SPOT-FUT, CONSTANTS  
> **never_with**: CAT-F Kelly merge internals redesign

---

## 1. 역할

Paper trading book of record: OPEN/CLOSED, try_add gates, Treasury accounting, PnL, identity/zombie heal.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| ledger core | `forward/ledger.py` |
| shared helpers / try_add | `forward/shared.py` |
| reports hydrate | `forward/reports.py` |
| practitioner reports | `forward/practitioner_bitget_adapter.py` |
| DB table | `bitget_forward_trades` |

---

## 3. Book 상태

| status | 의미 |
|--------|------|
| OPEN | 가상 보유 중 |
| CLOSED | 청산 완료 |

**실전 book**: 거래소 계좌 (OMS) — paper와 **분리**. parity 목표 P2-7.

---

## 4. try_add Gate Order (변경 시 🔴 review)

> Bitget-specific — CAT-F와 공유. 순서 변경 시 MAP + F 동시 review.

1. GLOBAL / portfolio circuit (partial — P0-2)
2. doomsday / DEFCON gates
3. toxic / anti pattern
4. KILL_SWITCH
5. duplicate / re-entry
6. max OPEN count
7. per-logic daily cap
8. concentration / sector (partial)
9. treasury available ≤ 0
10. zero-notional guard
11. execution_safety (real path only)

---

## 5. Treasury Keys (SPOT/FUT)

```text
treasury_key = TREASURY_SPOT_USDT | TREASURY_FUTURES_USDT
```

- partial compound: group seed = ACCOUNT_SIZE + realized PnL
- **한계**: ACCOUNT_SIZE_USDT static anchor — full NAV compound incomplete

---

## 6. PnL · Funding (CAT-E link)

| 항목 | 현행 | 목표 |
|------|------|------|
| spot PnL | ledger close path | 유지 |
| funding fee | tracked, **미차감** | P1-3 |
| slippage | **미반영** | P2 |

---

## 7. Claude 설계 대상

- try_add gate #6 ↔ CAT-F max open SSOT 정합
- funding → `final_ret` / `pnl` 차감 spec
- zombie / identity heal hooks
- reconcile with OMS (real execution path)

---

## 8. Single Writer

`bitget_forward_trades` — **ledger.py only** (close via E)

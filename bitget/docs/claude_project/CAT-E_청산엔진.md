# CAT-E · 청산 엔진 (Bitget)

> **위험도** 🔴 High · **Tier T2** · **also_load**: CAT-D, CONSTANTS, SPOT-FUT  
> **never_with**: CAT-D schema, CAT-N OMS order path 동시 redesign

---

## 1. 역할

Exit logic: TP/SL, trailing, funding bleed-stop, ROE-based liq approximation, mega trend kill, position_manager.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| position lifecycle | `trading/position_manager.py` |
| exit routing | `trading/order_routing.py` |
| tail risk | `trading/tail_risk_gate.py` |
| mega trend kill BG | `trading/mega_trend_kill_bg.py` |
| slippage guard | `trading/slippage_guard.py` |
| ledger close | `forward/ledger.py` (CLOSED write) |
| real exit | `executor.py` + `oms_core` ⚠️ **partial** |

---

## 3. Paper vs Real Exit

| 경로 | 상태 |
|------|------|
| paper close | ledger UPDATE — **동작** |
| real market exit | **미완** — ENABLE_REAL_EXECUTION=true 시 위험 |
| dry_run | execution_safety gate |

---

## 4. Funding (FUT)

- `funding_accum_usdt_est` 추적
- bleed-stop at exit (partial)
- **PnL 차감**: 미구현 (P1-3)

---

## 5. Leverage · Liq Model

- ROE approx: −100%/leverage
- **실거래소 liquidation price**: 미연동
- MAX_LEVERAGE cap: CAT-N / F

---

## 6. Claude 설계 대상

- P1-3 funding → ledger close formula
- real exit order spec (market, reduce-only)
- funding entry filter (pre try_add)
- exit ↔ OMS reconcile idempotency

---

## 7. 인터페이스 (CAT-D)

```
exit_signal → ledger.close_trade(...) → treasury update → F read
```

**타이밍**: F는 **closure 후** read — E가 D write 타이밍 결정

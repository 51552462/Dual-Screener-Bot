# CAT-N · Fast Safety & Execution (Bitget)

> **위험도** 🔴 **Critical** · **Tier T2** · **also_load**: CAT-F, CAT-D, MAP  
> **never_with**: CAT-E exit formula, CAT-D ledger schema

---

## 1. 역할

Real execution gate chain, OMS, dry_run, portfolio MDD, leverage, reconciliation, WS hydrate.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| execution gates | `trading/execution_safety.py` |
| OMS core | `trading/oms_core.py` |
| executor entry | `executor.py` |
| reconciliation | `trading/reconciliation.py` |
| order routing | `trading/order_routing.py` |
| account snapshot | `trading/account_snapshot.py` |
| order snapshot | `trading/order_snapshot.py` |
| market price WS | `trading/market_price_snapshot.py` |
| validation smoke | `validation/ws_oms_smoke.py` |

---

## 3. Gate Chain (SSOT: `evaluate_config_gates` + `run_pre_execution_gates`)

> **번호 SSOT** = `execution_safety.evaluate_config_gates` (1–8) 이후 `run_pre_execution_gates` 확장.  
> A-1 NAV MDD = **config gate 6**. A-2 tail consumption = **config gate 8** (snap은 gate 6과 `get_portfolio_mdd_snap_cached` 공유).

### Config gates 1–8 (`evaluate_config_gates`)

1. `ENABLE_REAL_EXECUTION`
2. `REAL_EXECUTION_DRY_RUN`
3. MetaGovernor `KILL_SWITCH`
4. `GLOBAL_CIRCUIT_BREAKER`
5. Orphan exchange inventory
6. **Portfolio NAV MDD** (reduce / block / halt) — A-1 `evaluate_portfolio_mdd_tier`
7. Gross notional cap
8. **Tail risk reserve** — A-2 consumption + legacy underfund path

**Default**: gates 1–2 block all real orders

### Extended pre-execution (`run_pre_execution_gates`, after 1–8 pass)

9. Funding squeeze clutch (inline, futures)
10. Catastrophic day guard (24h rolling)
11. Mega trend kill switch (inline)
12. Doomsday DEFCON (`evaluate_doomsday_gate`)
13. Concentration / BTC-proxy cluster (`evaluate_concentration_gate`)
14. Price sanity / bad tick (`evaluate_price_sanity_gate`)
15. Slippage / spread (`evaluate_slippage_gate`)
16+. Leverage / margin mode (`resolve_leverage` → `resolve_max_leverage`, default cap **5x**)

---

## 4. Paper vs Real

| path | SSOT |
|------|------|
| paper try_add/close | CAT-D ledger |
| real order | OMS → exchange API |
| parity | P2-7 target |

---

## 5. Portfolio MDD (CAT-F overlap)

`execution_safety` — config **gate 6** NAV MDD block/halt/reduce (A-1 PORTFOLIO_MDD_*)

---

## 6. Claude 설계 대상

- complete P0-2 tier actions in config gate 6
- real exit order path (CAT-E coordination)
- orphan position auto-handling
- ENABLE_REAL_EXECUTION pilot checklist (P2-5)
- A-4: gross gate 7 NAV SSOT alignment with A-1 treasury NAV

---

## 7. 🔴 Critical

Any gate bypass or default true for real execution → 디렉터 + rollback plan

---

## 8. Tests

`bitget/tests/test_trading_phase5.py`, `test_portfolio_mdd_a1.py`, `test_tail_fund_a2.py`, `test_ws_oms_smoke_bitget.py`, etc.

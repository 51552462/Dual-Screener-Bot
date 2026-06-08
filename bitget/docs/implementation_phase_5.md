# Bitget Phase 5 — OMS Upgrade

> 작성일: 2026-06-07  
> **주식 루트 미수정** — 모든 변경 `bitget/` 내부.

---

## 모듈 분리

| 파일 | 역할 |
|------|------|
| `bitget/trading/oms_core.py` | exchange factory, clientOid, market order |
| `bitget/trading/leverage_manager.py` | margin mode + leverage SSOT |
| `bitget/trading/position_manager.py` | LONG/SHORT abstraction, position index |
| `bitget/trading/reconciliation.py` | phantom OPEN, hydrate, orphan alert |
| `bitget/trading/slippage_guard.py` | 3-stage slippage pipeline |
| `bitget/oms.py` | **facade** — 기존 import 경로 유지 |

---

## 3-Stage Slippage

| Stage | 함수 | 호출 위치 |
|-------|------|-----------|
| Pre-scan | `check_pre_scan_liquidity()` | `forward/_core.py` `try_add_virtual_position` |
| Pre-trade | `run_pre_trade_gate()` | `executor.py` 실주문 직전 |
| Post-trade | `audit_post_trade_slippage()` | `reconciliation.py` order hydrate |

### Config 키

| 키 | 기본값 | 설명 |
|----|--------|------|
| `SEED_SLIPPAGE_GUARD_USDT` | 50000 | pre-scan 시드 임계 |
| `MIN_TRADE_VALUE_24H_SLIP_USDT` | 5000000 | 최소 24h 거래대금 |
| `ENABLE_SLIPPAGE_GUARD` | true | pre-trade WS gate |
| `SLIPPAGE_MAX_SPREAD_BPS` | 30 | spread 상한 |
| `POST_TRADE_MAX_SLIPPAGE_BPS` | 50 | fill vs expected 상한 |
| `LEVERAGE_BY_STRATEGY` / `LEVERAGE_BY_ENGINE` | — | per-strategy leverage |
| `MARGIN_MODE_BY_STRATEGY` | — | cross / isolated |

---

## 실행 안전 계층

```
ENABLE_REAL_EXECUTION=false (default)
  └── REAL_EXECUTION_DRY_RUN=true (default)
        └── MetaGovernor KILL_SWITCH
              └── pre-trade slippage gate
                    └── leverage_manager (futures)
                          └── oms_place_market_order()
```

---

## 테스트

```bash
python -m unittest bitget.tests.test_trading_phase5 -v
```

- dry-run spot LONG / futures SHORT
- reconciliation skip (dry_run)
- leverage / slippage unit cases

---

## 다음 단계 (Phase 6)

- `bitget/RUNBOOK.md`
- dashboard / heatmap systemd
- `update_bitget.sh` zero-downtime deploy
- remaining `BASE_DIR` → `data_paths` migration

# CAT-CONSTANTS · 핵심 상수 레퍼런스 (Bitget)

> **Tier T1** — 숫자·임계값 설계·검증 시 @멘션. 개별 CAT 파일에 상수 중복 없음.

---

## 스케줄·인프라 (CAT-A, L)

| 상수 | 값 | 비고 |
|------|-----|------|
| scan slots (staggered cron) | ~27 | `bitget/deploy/` crontab |
| lock max-age | 7200s | `infra/runtime.py` |
| watchdog miss | 100s × 3 → restart | factory only (WS 확장 P1-7) |
| snapshot interval | 5m | CQRS |
| cron overlap wait | 50min max | 초과 시 slot skip |

---

## 스캔·시그널 (CAT-C)

| 상수 | 값 |
|------|-----|
| DYNAMIC_SUPERNOVA_CUTOFF | config (≈0.50) |
| scan threads | config |
| universe min volume | 거래량 기준 자동 |
| `master_scanner` stagger | cron 분산 |

---

## 리스크·Kelly (CAT-F)

| 상수 | 값 | SSOT |
|------|-----|------|
| DYNAMIC_KELLY_RISK | 0.01 default | config_kv |
| Kelly hard max (audit) | 0.25 (25%) | `validation/regime_audit.py` |
| MAX_LEVERAGE | **5** default (`resolve_max_leverage`) | `execution_safety.py` |
| MAX_GROSS_NOTIONAL_PCT | **80** default (gate 7) | `execution_safety.py` |
| GROSS_NOTIONAL_CAP_ENABLED | true (kill-switch) | config_kv |
| MAX open positions | ~20 | ledger try_add |
| GROUP MDD breaker | −30% | legacy group |
| Portfolio NAV MDD tiers | −15% / −20% / −30% | **P0-2 목표** (`execution_safety` partial) |
| ENABLE_REAL_EXECUTION | **false** default | config |
| REAL_EXECUTION_DRY_RUN | **true** default | config |
| ACCOUNT_SIZE_USDT | static anchor | treasury partial compound |

---

## Regime Kelly cap (CAT-G)

| regime | typical kelly_cap |
|--------|-------------------|
| HIGH_VOL | ~0.012 |
| BEAR | ~0.010 |
| BULL | ~0.028 |
| SIDEWAYS | ~0.018 |
| CHOP / UNKNOWN | ~0.015 |

> `governance/meta_sync.py` → `DYNAMIC_KELLY_RISK` 동기화

---

## Execution safety (CAT-N)

| gate | policy |
|------|--------|
| ENABLE_REAL_EXECUTION | false → block |
| portfolio MDD block | config thresholds |
| MAX_LEVERAGE | hard cap (default 5, `resolve_max_leverage`) |
| doomsday / concentration | `trading/*_gate.py` |
| slippage / price sanity | pre-order |

---

## Forward·Treasury (CAT-D)

| 상수 | 값 |
|------|-----|
| paper book | `bitget_forward_trades` |
| treasury keys | SPOT / FUTURES separate |
| tail risk fund | 적립 O, **소비 X** (P0-3) |

---

## Evolution (CAT-H)

| 상수 | 값 |
|------|-----|
| apply_deathmatch_allocation | **False** (현행) |
| deathmatch market key | `BG` vs `SPOT`/`FUT` **불일치** |
| OOS window | engine6 14d partial |
| min trades lifecycle | config |

---

## Practitioner (CAT-O)

| 상수 | 값 |
|------|-----|
| practitioner rules | ~30 (루트 intelligence import) |
| core engines | 5 signal engines |

---

## Config bounds (P1-4 목표)

| KEY | proposed bound |
|-----|----------------|
| DYNAMIC_KELLY_RISK | [0.002, 0.030] |
| META_GLOBAL_KELLY_MULT | [0.1, 2.0] |
| MAX_LEVERAGE | [1, 10] |

> **현행**: 쓰기 시점 검증 **없음** — 사후 audit only

---

*새 상수 확정 시 본 파일 + `work_phases/00_전체현황판` SSOT 용어집 동시 갱신*

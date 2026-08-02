# CAT-SPOT-FUT · 현물 vs 선물 비대칭 (Bitget)

> **Tier T1** — SPOT/FUT 분기 설계 시 @멘션. 공통 로직 설계 시 생략 가능.

---

## 1. 시장·Treasury (CAT-F, D)

| | SPOT | FUTURES |
|---|------|---------|
| Treasury config key | `TREASURY_SPOT_USDT` | `TREASURY_FUTURES_USDT` |
| ledger `market_type` | `spot` | `futures` |
| 레버리지 | 1x (implicit) | `leverage_manager` / `MAX_LEVERAGE` |
| 펀딩비 | — | `funding_accum_usdt_est` (청산 시 PnL 반영 **P1-3 목표**) |
| 청산가 모니터 | — | ROE 근사 (실거래소 liq price **미연동**) |
| Deathmatch market key | `SPOT` (정규화 목표) | `FUT` (레거시 `BG` 혼재 **B-1 수정 대상**) |

---

## 2. 파이프라인·스캔 (CAT-A, C)

| | SPOT | FUTURES |
|---|------|---------|
| scan mode | `scan_spot` | `scan_futures` |
| 통합 scan | `scan_all` (둘 다) | 동일 |
| 유니버스 테이블 | `BITGET_SPOT_*` OHLCV | `BITGET_FUT_*` OHLCV |
| 슬리피지 가정 | 낮음 | funding + leverage amplifies |
| session | 24/7 (크립토) | 24/7 |

---

## 3. 실행·OMS (CAT-N, E)

| | SPOT | FUTURES |
|---|------|---------|
| OMS margin mode | cash | isolated/cross (`oms_margin_mode`) |
| `execution_safety` gate | 동일 13-gate | + leverage cap |
| 실전 기본 | `ENABLE_REAL_EXECUTION=false` | 동일 |
| exit 실주문 | market (partial) | market (partial) — **청산 경로 미완** |

---

## 4. Meta·Regime (CAT-G)

| | SPOT | FUTURES |
|---|------|---------|
| regime read | 공통 `CURRENT_REGIME_KEY` | 동일 (분리 키 **없음**) |
| Kelly cap | `DYNAMIC_KELLY_RISK` + meta merge | 동일 |
| Meta DB | Bitget `bitget_system_config.sqlite` | 동일 |
| MetaGovernor cycle | `governance/meta_sync.py` | 동일 |

> **설계 주의**: SPOT/FUT 별 regime가 필요하면 **새 키** (`CURRENT_REGIME_KEY_SPOT` 등) — 기존 키 덮어쓰기 금지.

---

## 5. 리스크·Deathmatch (CAT-F, H)

| | SPOT | FUTURES |
|---|------|---------|
| 그룹 MDD breaker | 그룹별 −30% (legacy) | 동일 |
| Portfolio NAV MDD | **공통** (P0-2 목표) | 공통 |
| deathmatch lookback | 90d (목표 SSOT) | 90d |
| `apply_deathmatch_allocation` | **False** (현행) | **False** |
| concentration gate | `trading/concentration_gate.py` | 동일 |

---

## 6. 리포팅 (CAT-J)

| | SPOT | FUTURES |
|---|------|---------|
| treasury 표시 | `TREASURY_SPOT_USDT` | `TREASURY_FUTURES_USDT` |
| daily audit track | `forward/reports.py` 분기 | 동일 |
| funding line item | N/A | 리포트 **미반영** (P1-3) |

---

## 7. Adapter 패턴 (KR-US 대응)

주식의 `market=KR|US` 분기 대신 Bitget은:

```text
market_type in ("spot", "futures")
treasury_key = f"TREASURY_{market_type.upper()}_USDT"  # 정규화 후
```

**금지**: `if symbol.endswith("USDT")` 로 SPOT/FUT 추론 — `market_type` SSOT 필드 사용.

---

*버전 2026-08-01 · audit: `13_institutional_grade_audit_and_roadmap.md`*

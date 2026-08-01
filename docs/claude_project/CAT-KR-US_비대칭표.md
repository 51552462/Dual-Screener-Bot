# CAT-KR-US · 한국 vs 미국 비대칭

> **Tier T1** — KR/US 분기 설계 시 @멘션. 공통 로직 설계 시 생략 가능.

---

## 1. 시간·스케줄 (CAT-A)

| | KR | US |
|---|----|----|
| cron TZ | Asia/Seoul 네이티브 | ET slot dispatcher (`*/5` KST polling) |
| 정규장 | 09:00–15:30 KST | 09:30–16:00 ET |
| 장중 슬롯 수 | 8 | 9 (종가 늦은 r2) |
| 일일 감사 | 18:45 월–금 | 06:45 화–토 |
| data_refresh | 08:00 KST | 동일 job 내 US bulk |

---

## 2. 데이터·유니버스 (CAT-B, C)

| | KR | US |
|---|----|----|
| OHLCV | FinanceDataReader per-ticker | yfinance batch 100 |
| 테이블 prefix | `KR_######` (zero-pad) | `US_TICKER` (`.`→`-`) |
| 최소가 | 1,000원 | $0.5 |
| 유동성 | 5일평균 5만주 | max(2000, $30만/price)주 |
| 잡주 필터 | 이름 regex+파생+우선주 강함 | 코드형태 위주 |

---

## 3. 스캐너 (CAT-C)

| | KR | US |
|---|----|----|
| master 슬롯 | **있음** | **없음** |
| bowl | shadow-only (ENROLLED_SHADOW) | live |
| blackhole | 없음 | `short_data.sqlite` |
| cross_market | **consume** (hydrate) | **produce** (publish) |

---

## 4. 자본·리스크 (CAT-F)

| | KR | US |
|---|----|----|
| FX (sizing) | 1.0 | 1350 |
| NAV seed | ₩3억 | $30만 |
| AUM 브레이크 | 시드>5천만 & 시총<1000억 거부 | — |
| 데스매치 lookback | 90d | 120d |
| crash buffer | −1.0% | −1.5% |
| MDD penalty ref | −14% | −16% |
| lifecycle min trades | 15 | 8 |
| alpha_half_life | 10d | 30d |
| cooloff / whipsaw | 3d / 2d | 7d / 3d |
| inverse ETF | KODEX 252670 (−1.5%) | SQQQ/SOXS (−2.0%) |

---

## 5. 국면 (CAT-G)

| | KR | US |
|---|----|----|
| breadth 팩터 | None (앙상블 투표 제외) | RSP/SPY vs 50d |
| VIX | 글로벌 ^VIX **공유** | ^VIX native + p90 승격 |
| crisis sync | US 위기 시 **강제 HIGH_VOL** | 트리거 송신 |
| PRI | PRI_KR | PRI_US |

---

## 6. Toxic (CAT-I)

| | KR | US |
|---|----|----|
| 저장 | `system_config` (한글 섹터) | `us_toxic_ml_antipatterns.json` |
| merge | — | **KR config에 merge 금지** |

---

## 7. 리포팅 (CAT-J)

| | KR | US |
|---|----|----|
| telegram env | `EQUITY_KR_*` | `EQUITY_US_*` |
| PIL / daily | KR 채널 | US 채널 |

---

## 8. 공유 (분기 없음)

Kelly merge chain 구조 · 시장당 OPEN 20 · 로직 4/일 · 섹터 2+2 · GLOBAL_CIRCUIT_BREAKER −5% · HTC 합성 챔버 · WEIGHT_S1/S4 듀얼암 · 슬롯 50분 간격

---

*설계 시: 공통 함수 + `market` 분기. KR 전용/US 전용 파일 분리는 이미 존재하는 패턴 따름.*

# CURSOR → CLAUDE · LANE_HIST3FIX

> **레인**: `LANE_HIST3FIX`  
> **sub-phase**: `FULL-BT-HIST-3-FIX`  
> **갱신**: 2026-08-28  
> **유형**: **VPS dry→10×2 실런 완료** · **WAIT_CLAUDE_OK**  
> **코드**: `1652687` · 추가 diff 없음 · 전체런 금지  
> **비게이팅**: `LANE_FASTCHECK` — 본 파일만 사용

---

## 7키 — Dry (MAX_SYMBOLS=3)

| 키 | SPOT `pilot-spot-20260828T013325Z` | FUTURES `pilot-fut-20260828T013326Z` |
|----|-----------------------------------|--------------------------------------|
| engine_call_total | **180** | **0** |
| engine_call_outcome_totals | candidate=**3** · none=**177** · exception=**0** | 0 / 0 / 0 |
| tf_ohlcv_coverage | 1D/4H/2H/1H 전부 **true** | 전부 **true** |
| exception_types | `{}` | `{}` |
| hit / reject | **3** / **3** | **0** / **0** |
| fetch_requested vs loaded | **1473** vs **900** | **993** vs **270** |
| call_total vs walk_bar_expected | **180** vs **753** | **0** vs **273** |

- SPOT 심볼당: `calls=60` · outcome `{candidate:1, none:59}` ×3  
- FUT: **warmup skip** `loaded=90 need=240 requested=331` (BTC/ETH/SOL)  
- paper **10→10** · trade_count=0 · reused_min_bars=**240** (`_U1_MIN_BARS`)

---

## 7키 — 10×2 (MAX_SYMBOLS=10)

| 키 | SPOT `pilot-spot-20260828T013502Z` | FUTURES `pilot-fut-20260828T013502Z` |
|----|-----------------------------------|--------------------------------------|
| engine_call_total | **600** | **0** |
| engine_call_outcome_totals | candidate=**11** · none=**589** · exception=**0** | 0 / 0 / 0 |
| tf_ohlcv_coverage | 1D/4H/2H/1H 전부 **true** | 전부 **true** |
| exception_types | `{}` | `{}` |
| hit / reject | **11** / **11** | **0** / **0** |
| fetch_requested vs loaded | **4910** vs **3000** | **3310** vs **900** |
| call_total vs walk_bar_expected | **600** vs **2510** | **0** vs **910** |

- SPOT 심볼당: `calls=60` · 대부분 `{candidate:1, none:59}` (ADA candidate=2)  
- FUT: 전 심볼 warmup skip `loaded=90 need=240`  
- paper **10→10** · trade_count=0 · banner_ok=true · results 0→0

---

## FIX 관측 (해석 최소화)

| 항목 | SPOT | FUTURES |
|------|------|---------|
| calls=1 고정 해소 | **해소** (60/심볼 · dry 180 · 10×2 600) | N/A (진입 전 skip) |
| candidate 생성 | dry=3 · 10×2=11 | 0 |
| warmup skip | 없음 | **전량** (1D depth≈90 &lt; 240) |

---

## Claude OK 수신 (2026-08-28)

- **SPOT HIST-3-FIX: OK** → 본 sub-phase **SUB_DONE** 가능 (Claude 판정)
- FUT warmup: FIX 잔여 **아님** · CAT-B 데이터 깊이 **별건** (Claude)
- 전체런 금지 **유지**
- 다음: Handoff 없음 · **FUT 1D 저장량/절단점 진단만** (코드 수정 금지)

---

## Append · FUT 1D depth 진단 (코드·경로 · 수정 없음)

### 한 줄 결론 (로컬 VPS DB 미접속 — 경로 추론 + 확인 SQL)

**`requested=331 → loaded=90` 은 `LIMIT`로 331을 90으로 자르는 버그가 아님.**  
`ohlcv_date_range_sql(..., bar_limit=requested)` → `LIMIT 331` (cap 50..20000).  
`loaded = len(df)` = **WHERE [fetch_start, end]에 실제로 걸린 행 수**.  
dry FUT: `requested=240+walk≈331`, `loaded=90`, 심볼당 동일 → **테이블(또는 해당 구간)에 ≈90봉만 존재**하는 정황이 강함 (데이터 깊이).

### 경로 (읽기만)

| 단계 | 위치 | 동작 |
|------|------|------|
| 윈도우 | `batch.get_full_bt_window_batches` | `_load_ohlcv` = **tail** `OHLCV_SIGNAL_BAR_LIMIT`(250) |
| fetch | `harness._load_ohlcv_fetch_range` | `[start−min_bars, end]` + `LIMIT=requested` |
| skip | `run_replay` | `loaded_n < REUSED_MIN_BARS(240)` → warmup skip |

FUT dry `walk≈91`·`loaded=90` 정합 → 배치 윈도우도 **얕은 FUT 1D**에서 나옴.  
SPOT dry `loaded≈300/심볼`·`walk≈251`과 비대칭 → **SPOT/FUT 저장 깊이 차이** 쪽.

### 구분 (Claude Ask에 답)

| 가설 | 판정 |
|------|------|
| fetch LIMIT/파라미터가 331→90 절단 | **기각** (LIMIT=requested=331; loaded=쿼리 결과 행수) |
| `BITGET_FUT_*_1D` 저장 로우 ≈90 (또는 구간 내 ≈90) | **유력** — VPS `COUNT(*)`로 확정 필요 |
| HIST-3-FIX 잔여 | **아님** (Claude OK와 동일) |

### VPS 확정 명령 (디렉터)

> `_vps_bar_depth.py` = **로컬 미커밋** → VPS `No such file` 정상. **sqlite3 직접**:

```bash
sqlite3 /var/lib/quant-bitget/data/bitget_market_data.sqlite "
SELECT 'FUT_BTC', COUNT(*), MIN(Date), MAX(Date) FROM \"BITGET_FUT_BTC_USDT_1D\"
UNION ALL SELECT 'FUT_ETH', COUNT(*), MIN(Date), MAX(Date) FROM \"BITGET_FUT_ETH_USDT_1D\"
UNION ALL SELECT 'FUT_SOL', COUNT(*), MIN(Date), MAX(Date) FROM \"BITGET_FUT_SOL_USDT_1D\"
UNION ALL SELECT 'SPOT_BTC', COUNT(*), MIN(Date), MAX(Date) FROM \"BITGET_SPOT_BTC_USDT_1D\";
"
```

출력을 본 레인 OUTBOX에 붙여넣으면 Claude가 백필 vs 기타로 Handoff 가능.

---

## Append · VPS COUNT 확정 (2026-08-28 · 디렉터 sqlite3)

DB: `/var/lib/quant-bitget/data/bitget_market_data.sqlite`

| 테이블 | n | MIN(Date) | MAX(Date) |
|--------|---|-----------|-----------|
| BITGET_FUT_BTC_USDT_1D | **90** | 2026-05-31 | 2026-08-28 |
| BITGET_FUT_ETH_USDT_1D | **90** | 2026-05-31 | 2026-08-28 |
| BITGET_FUT_SOL_USDT_1D | **90** | 2026-05-28 | 2026-08-25 |
| BITGET_SPOT_BTC_USDT_1D | **300** | 2025-11-02 | 2026-08-28 |

### 확정 한 줄
- FUT 1D **저장 로우 = 90** (= warmup skip `loaded=90`) → **데이터 깊이 부족** (fetch LIMIT 절단 아님)
- SPOT 1D **300** → HIST-3-FIX 워밍업(240) 통과 가능·실제 SPOT 파일럿과 정합
- HIST-3-FIX 잔여 **아님** · 다음 = Claude: 백필(CAT-B) vs park 여부 Handoff만

(코드 diff 없음 · 전체런 금지)

---

## Claude OK · PARK (2026-08-28 · 최종)

| 항목 | 판정 |
|------|------|
| VPS COUNT | FUT n=**90** · SPOT_BTC n=**300** — 저장 깊이 부족 확정 · fetch 버그 기각 |
| HIST-3-FIX | **SUB_DONE** 유지 (SPOT 목적 달성) |
| FUT warmup | CAT-B 영역 · 본 sub **스코프 외** |
| 백필 Handoff | **PARK** (지금 불필요) |
| 전체런 | **금지 유지** |
| 코드 | **diff 없음** |

### PARK 사유 (Claude)
1. FULL-BT = IV L1 참고만 · LIVE/실거래 영향 없음  
2. 전체런 금지 → FUT 동등깊이 백테스트 당장 불필요  
3. 거래소가 90일 이전 FUT 1D를 주는지 **미확인** → 백필 스펙 조기 작성 금지  

### 나중에 백필 시 선행 2항
1. 거래소 API — 90일 이전 FUT 1D 제공 여부 **1회 확인만** (코드 수정 아님)  
2. 디렉터 — FUT full BT를 SPOT 동등 깊이로 우선할지 결정  

### 문서 메모
CAT-B §알려진 갭: SPOT/FUT initial backfill lookback 비대칭 후보 (착수 아님).

**레인 status → DONE**


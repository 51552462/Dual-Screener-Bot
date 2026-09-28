# CURSOR → CLAUDE · LANE_FULLBT

> **레인**: **LANE_FULLBT** (HIST3FIX 비접촉)  
> **sub-phase**: **FULL-BT-FUT-RUN-2**  
> **갱신**: 2026-09-27  
> **유형**: RUN-2 실런 OUTBOX · **WAIT_CLAUDE_OK**  
> **bypass**: **false** 유지 (키 미설정)

---

## OUTBOX — FULL-BT-FUT-RUN-2 · 2026-09-27

IV L1 참고만. LIVE/R6/생존 단정 없음.

| 키 | 값 |
|----|-----|
| **run_id** | `pilot-fut-20260927T040953Z` |
| **COUNT 재조회** | BTC/ETH/SOL 각 302 · first **2025-10-31** · last **2026-08-30** |
| **실사용 start env** | `BITGET_FULL_BT_START_DATE=2025-10-31` |
| **실사용 walk log** | `start=2025-10-30 end=2026-08-30` (harness 로그, 심볼 공통) |
| **call** | engine_call **186** (62×3, 1D) |
| **candidate** | **3** (`engine_outcome_candidate`) |
| **trade_count (이 run_id)** | full_bt `bitget_forward_trades`에 **run_id 컬럼 없음**. 이 런 HIST **events=0**. 테이블 총행 런 전후 **4=4** (기존 RUN-1 북) |
| **L1 report trade_count** | **4** (격리 DB 전체 — 이번 런 신규 체결 아님) |
| **defcon_bypassed** | **0** (full_bt_diag) |
| **prod bitget_forward_trades** | 런 전 **11** → 후 **11** · delta **0** |
| **exception** | **0** (`engine_call_outcome_totals.exception=0`, exception_types `{}`) |
| **FULLBT_DEFCON_BYPASS_ENABLED** | unset · `.env` 키 없음 |
| **심볼** | BTC_USDT, ETH_USDT, SOL_USDT · FUT staging only |
| **paper_before/after** | 11 / 11 |
| **summary json** | `/var/lib/quant-bitget/data/full_bt_pilot_summary_20260927T040953Z.json` |

갭#4 프로덕션 적용 **미착수**. RUN-1 판정 불변. 롤백=이 run_id 결과 무시.

---

---

## 절차 편차 1줄 회신 (요청)

| # | 답 |
|---|-----|
| **(a)** | pilot/batch에 **기존 시작일 파라미터 없었음** (env·인자 전부 없음). |
| **(b)** | 1차 Handoff에서는 시그니처+A/B만 OUTBOX 회신·**코드 미착수**. 이후 Claude **A′ 구현 Handoff**(`load_full_bt_ohlcv` 신규 명시)를 받아 그 스펙대로 구현함. “시그니처만” 단계를 건너뛴 것이 아니라 **승인된 2차(구현) Handoff**를 따른 것. 신규 파일은 A′ 스펙에 포함. 다음에도 1차=시그니처만 / 2차=구현 Handoff 구분을 문서 status에 더 분명히 표기하겠음. |

---

## 코드 상태 (조건부 OK)

| 항목 | 값 |
|------|-----|
| Claude | **조건부 OK** — COUNT 진행 가능 · 편차 비차단 |
| 테스트 | 43 passed |
| `_load_ohlcv` | 무변경 |
| bypass | **false** |

## COUNT 실측 (지금 · 필수)

```bash
STAGING=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
sqlite3 "$STAGING" <<'SQL'
SELECT 'BITGET_FUT_BTC_USDT_1D', COUNT(*), MIN(Date), MAX(Date) FROM BITGET_FUT_BTC_USDT_1D
UNION ALL
SELECT 'BITGET_FUT_ETH_USDT_1D', COUNT(*), MIN(Date), MAX(Date) FROM BITGET_FUT_ETH_USDT_1D
UNION ALL
SELECT 'BITGET_FUT_SOL_USDT_1D', COUNT(*), MIN(Date), MAX(Date) FROM BITGET_FUT_SOL_USDT_1D;
SQL
```

| 심볼 | COUNT | first | last |
|------|-------|-------|------|
| BTC | **302** | 2025-10-31 | 2026-08-30 |
| ETH | **302** | 2025-10-31 | 2026-08-30 |
| SOL | **302** | 2025-10-31 | 2026-08-30 |

실측: `ubuntu@3.36.90.195` · 2026-09-26 13:12 UTC · staging 112K · `FULLBT_DEFCON_BYPASS_ENABLED` 키 **없음**(default false). **실런은 아직 안 함.** 데이터 last=08-30 → 오늘(09-26) 대비 약 4주 공백.

## 실런 (COUNT 후 · bypass off)

```bash
export BITGET_FULL_BT_START_DATE='YYYY-MM-DD'  # COUNT first 그대로
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
export BITGET_FULL_BT_MARKET_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
export BITGET_FULL_BT_ONLY_MT=futures
export BITGET_FULL_BT_MAX_SYMBOLS=3
# FULLBT_DEFCON_BYPASS_ENABLED 설정 금지
bash bitget/deploy/run_full_bt_hist_pilot.sh
```

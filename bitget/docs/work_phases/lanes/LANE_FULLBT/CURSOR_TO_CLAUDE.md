# CURSOR → CLAUDE · LANE_FULLBT

> **레인**: **LANE_FULLBT** (HIST3FIX 비접촉)  
> **sub-phase**: **FULL-BT-FUT-RUN-2**  
> **갱신**: 2026-08-31  
> **유형**: 조건부 OK 수신 · 절차 편차 회신 · **WAIT_CURSOR_VPS** (COUNT)  
> **bypass**: **false** 유지

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
| BTC | _(대기)_ | _(대기)_ | _(대기)_ |
| ETH | _(대기)_ | _(대기)_ | _(대기)_ |
| SOL | _(대기)_ | _(대기)_ | _(대기)_ |

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

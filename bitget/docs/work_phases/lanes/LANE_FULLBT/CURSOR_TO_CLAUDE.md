# CURSOR → CLAUDE · LANE_FULLBT

> **레인**: **LANE_FULLBT**  
> **sub-phase**: **FULL-BT-FUT-DEFCON-1**  
> **갱신**: 2026-08-30  
> **유형**: Claude **최종 OK** 수신 · **WAIT_CURSOR_VPS** (재파일럿 대기)  
> **VPS bypass**: 지시 = **true** (실행 후 결과 append)

---

## Claude 최종 OK (수신)

**Claude OK: 2026-08-30 (최종)** — 조건부 잔여 3건 해소.

### 자가 재확인 — 3중 단독실패 테스트명 (OUTBOX 원문)

| 조건 실패 | 테스트명 |
|-----------|----------|
| kill-switch off | `test_kill_switch_off_blocks_bypass` · `test_wrap_keeps_block_when_kill_off` |
| isolated=false | `test_not_isolated_blocks_bypass` |
| DB_PATH ≠ full_bt | `test_wrong_db_blocks_bypass` |

파일: `bitget/tests/test_fullbt_defcon_bypass.py` (실재 확인됨)

---

## VPS 재파일럿 지시 (승인 범위만)

```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
export BITGET_FUT_DEPTH_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
export FULLBT_DEFCON_BYPASS_ENABLED=true
BITGET_FUT_DEPTH_RUN_FULL_BT=1 BITGET_FULL_BT_MAX_SYMBOLS=3 \
  bash bitget/deploy/run_fut_1d_depth_pilot.sh
```

검증 SQL (재파일럿 후):

```bash
DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
# bypass 건수 (run_id는 파일럿 JSON의 futures run_id)
sqlite3 "$DATA/bitget_full_bt.sqlite" \
  "SELECT COUNT(*) FROM full_bt_diag WHERE metric='defcon_bypassed';"
# 프로덕션 forward 유입 0 (파일럿 전후 delta — OPEN/신규 없으면 0 보고)
sqlite3 "$DATA/bitget_market_data.sqlite" \
  "SELECT COUNT(*) FROM bitget_forward_trades WHERE status LIKE 'OPEN%' OR entry_date >= date('now');"
```

### 결과 표 (디렉터/VPS 붙여넣기 후 Cursor가 채움)

| 필드 | 값 |
|------|-----|
| run_id (FUT) | *(대기)* |
| defcon_bypassed count | *(대기)* |
| trade_count / paper | *(대기)* |
| production forward 유입 | *(대기 · 목표 0)* |

금지: 전체런 · 심볼>3 · 프로덕션 OHLCV write · LIVE/R6/생존 단정 · SPOT

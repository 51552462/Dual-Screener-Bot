# NEXT_ACTION — LANE_FULLBT

| 필드 | 값 |
|------|-----|
| **레인** | **LANE_FULLBT** |
| **sub-phase** | **FULL-BT-FUT-DEPTH-1** |
| **status** | **WAIT_CURSOR_VPS** (Claude OK · staging FULL-BT=1 Go) |
| **고정** | staging · MAX=3 · futures-only · 프로덕션 OHLCV write 금지 · IV L1만 |

---

## VPS 실행 (지금)

```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
export BITGET_FUT_DEPTH_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
BITGET_FUT_DEPTH_RUN_FULL_BT=1 BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_fut_1d_depth_pilot.sh
```

출력 futures diag(engine_call_total·outcome·trade_count·paper)를 채팅/`CURSOR_TO_CLAUDE.md`에 붙여넣기.

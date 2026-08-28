# CURSOR → CLAUDE · LANE_FULLBT

> **레인**: `LANE_FULLBT`  
> **sub-phase**: **FULL-BT-FUT-DEPTH-1**  
> **갱신**: 2026-08-29  
> **유형**: Claude **OK — Go** 수신 · **WAIT_CURSOR_VPS** (staging FULL-BT=1 · max=3)

---

## Claude OK 요약
- staging COUNT PASS 최종 OK
- FULL-BT=1 · MAX_SYMBOLS=3 · **staging 고정** · 프로덕션 OHLCV write **금지**
- IV L1 참고만 · LIVE/R6/생존 단정 금지
- hit/reject 각주는 결론 근거로 쓰지 말 것

---

## Cursor 조치
- `run_full_bt_hist_pilot.sh`: `BITGET_FULL_BT_MARKET_DB` + `BITGET_FULL_BT_ONLY_MT=futures` 지원
- `run_fut_1d_depth_pilot.sh`: FULL-BT=1 시 staging 필수·futures-only 연결
- VPS 실행 대기(아래 명령) → 결과 append

```bash
export BITGET_FUT_DEPTH_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
BITGET_FUT_DEPTH_RUN_FULL_BT=1 BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_fut_1d_depth_pilot.sh
```

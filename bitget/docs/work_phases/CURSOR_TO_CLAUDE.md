# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-28  
> **유형**: **FULL-BT-HIST-3-FIX Claude OK 수신** · **커밋·푸시 → VPS dry→10×2 대기**  
> **코드**: 추가 diff 없음(실행만)

---

## Claude OK 수령
Spec 1~4 OK. Handoff 원문 누락 caveat 수용 → 본 검증 응답을 `CLAUDE_TO_CURSOR.md`에 **append** 완료.

롤백: `harness.py` 해당 커밋 revert만 — 결과 스키마·paper·config_kv 무영향.

### VPS (푸시 후)
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_full_bt_hist_pilot.sh
BITGET_FULL_BT_MAX_SYMBOLS=10 bash bitget/deploy/run_full_bt_hist_pilot.sh
```
7키 숫자만 보고 → **WAIT_CLAUDE_OK** · 전체런 금지.

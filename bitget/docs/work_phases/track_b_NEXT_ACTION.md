# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **주 트랙** | **B1-LADDER-R1a** · **OBSERVE** |
| **병렬 R&D** | **FULL-BT-HIST-1 파일럿** · **WAIT_DIRECTOR** (VPS 실행) |
| **코드** | HIST-1 로컬 완료 · **원격 push 전** · VPS `run_full_bt_hist_pilot.sh` 준비됨 |

---

## 디렉터 · VPS 순서 (1번)

### A. 이 PC (Cursor에게 한 줄)
```
HIST-1+파일럿 스크립트 commit/push 해줘 (bitget/full_bt + deploy/run_full_bt_hist_pilot.sh + tests/full_bt)
```

### B. 코인 VPS (복붙)
```bash
cd ~/dante_bots/Dual-Screener-Bot
git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
bash bitget/deploy/run_full_bt_hist_pilot.sh
```

### C. 끝나면
터미널에 나온 JSON 전체(또는 `full_bt_pilot_summary_*.json` 내용)를 이 창에 붙여넣기 → Cursor가 OUTBOX 5항 정리.

# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **주 트랙** | **B1-LADDER-R1a** · **OBSERVE** |
| **병렬 R&D** | **UNIVERSE-BT L0** · **WAIT_DIRECTOR** (실데이터 실행 = **코인 VPS**) |
| **로컬 PC** | `bitget_market_data.sqlite` **비어 있음** (테이블 0) → 과거 검증 **불가** |
| **지표4** | 보류 |

---

## 디렉터 (지금)

### A · R1a
텔레그램 OPEN/CLOSED

### B · 과거 검증 실실행 (막힘 → VPS)
로컬에는 OHLCV가 없음. **코인 서버**에서:

```bash
cd ~/dante_bots/Dual-Screener-Bot   # 설치 경로에 맞게
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
# 코드 pull 후
bash bitget/deploy/run_universe_bt_l0.sh
# 심볼 수 조절: BITGET_UNIVERSE_BT_MAX_SYMBOLS=20
# 전체: BITGET_UNIVERSE_BT_MAX_SYMBOLS=0
```

끝나면 리포트 경로·숫자를 Cursor/Claude에 회신.

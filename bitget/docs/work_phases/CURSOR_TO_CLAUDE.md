# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-23  
> **유형**: **UNIVERSE-BT L0 실실행 시도** · 로컬 OHLCV **공백** · VPS 실행 스크립트 준비

---

## OUTBOX — 2026-08-23 · 본검증 실행 차단 (데이터 위치)

### 시도
로컬에서 `run_live_u2_u3.py` 실행.

### 실측
| 항목 | 값 |
|------|-----|
| `bitget/bitget_market_data.sqlite` | exists · **size≈0 · tables=0** |
| SPOT/FUT `_1D` | **0** |
| paper drift | 없음 (실행할 심볼 0) |

→ **과거 구조생존 숫자는 아직 없음.** 실험실만 있고 원재료(시세)가 PC에 없음.

### 조치
- `bitget/deploy/run_universe_bt_l0.sh` 추가 — 코인 VPS용
- `run_live_u2_u3.py` — 심볼 0이면 FAIL exit 2

### Ask (디렉터)
VPS에서 스크립트 실행 후 리포트 경로·표 숫자 회신 → Cursor가 OUTBOX에 L0 실측 기록.

지표4는 여전히 미착수.

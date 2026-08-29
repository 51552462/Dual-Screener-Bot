# CURSOR → CLAUDE · LANE_FULLBT

> **레인**: `LANE_FULLBT`  
> **sub-phase**: **FULL-BT-FUT-DIAG-2**  
> **갱신**: 2026-08-29  
> **유형**: read-only 조회 · **WAIT_DIRECTOR** (VPS SELECT 붙여넣기 대기)  
> **선행**: FULL-BT-FUT-DIAG-1 = **Claude OK**

---

## 원문 표 (가공·해석 없음)

### A) 로컬 `bitget_full_bt.sqlite` (read-only uri)

| 항목 | 값 |
|------|-----|
| path | `bitget/bitget_full_bt.sqlite` |
| tables | `bitget_full_bt_checkpoint` only |
| `full_bt_diag` | **없음** |
| `SELECT symbol, step, detail … gate_reject` · run_id=`pilot-fut-20260829T062221Z` | **0건** |

### B) 로컬 ops_events

| 항목 | 값 |
|------|-----|
| path tried | `bitget/bitget_ops_events.sqlite` (exists) |
| `ops_events` table | **없음** |
| `event='fullbt_candidate_reject'` · run_id 포함 | **0건** |

### C) VPS

| 항목 | 값 |
|------|-----|
| Cursor 이 PC → VPS SSH/DB | **미접속** (선행 SSOT와 동일) |
| VPS `full_bt_diag` / `ops_events` 원문 | **미조회** — “VPS도 0건” **단정 금지** |

코드 변경 · 재실행 · retag · DEPTH-2 · CAT-D/B 분기: **없음**

---

## 디렉터 할 일 (VPS에서 복붙 후 Cursor에 붙여넣기)

```bash
DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
sqlite3 "$DATA/bitget_full_bt.sqlite" "
SELECT symbol, step, detail
FROM full_bt_diag
WHERE run_id='pilot-fut-20260829T062221Z' AND metric='gate_reject';
"
sqlite3 "$DATA/bitget_ops_events.sqlite" "
SELECT ts_utc, payload_json
FROM ops_events
WHERE event='fullbt_candidate_reject'
  AND payload_json LIKE '%pilot-fut-20260829T062221Z%';
"
```

출력 그대로 채팅/파일에 주시면 OUTBOX 원문 표 채우고 `WAIT_CLAUDE_OK`로 넘김.  
둘 다 0건이면 Handoff대로 재파일럿 없이 대기 → 다음 Claude Handoff(≤3 DIAG-on).

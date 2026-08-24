# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-23  
> **유형**: **FULL-BT-2** 구현 완료 · **WAIT_CLAUDE_OK** · 원본 CAT-C/D/E **diff 없음**

---

## OUTBOX — 2026-08-23 · FULL-BT-2 완료

### 산출
| 경로 | 역할 |
|------|------|
| `bitget/full_bt/batch.py` | `run_full_bt_batch` · shards · window batches · paper 샤드별 불변 |
| `bitget/full_bt/checkpoint.py` | `bitget_full_bt_checkpoint` (격리 DB only) |
| `bitget/tests/full_bt/test_full_bt_batch_fb2.py` | resume + paper · **4 passed** |

### 재사용값 (룰5)
`재사용값: TIME_MACHINE_MAX_TABLES=300 · TIME_MACHINE_MAX_BARS_PER_TABLE=5000` — 출처 `bitget.infra.memory_policy`

### 엔진 5종 (선택 보고)
관여 엔진 5종 확인: `EMA5` · `MASTER` · `NULRIM` · `TV_SHORT_V1` · `TV_SHORT_V2` (`_build_engine_pool` base · 원본 import)

### paper 불변 (테스트 숫자)
- start/end/shard: **paper_count=2** 유지 · resume 재실행 시 `batches_run=0` · `batches_skipped=n1`

### 정책 승계
FULL-BT-1 `harness.run_replay` 재사용만 · TF `['1D','4H','2H','1H']` · funding 미추적 · 국면 UNKNOWN · step11 N/A skip

### 비접촉
`forward/ledger.py` · `shared.py` · `signal_engines` · exit 3파일 · config_kv · paper 원장 · FULL-BT-1 harness 로직 재작성 **없음**

### Ask
FULL-BT-2 검증. OK면 FULL-BT-3 Handoff만 파일로.

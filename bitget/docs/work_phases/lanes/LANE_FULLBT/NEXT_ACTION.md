# NEXT_ACTION — LANE_FULLBT

| 필드 | 값 |
|------|-----|
| **레인** | **LANE_FULLBT** |
| **sub-phase** | **FULL-BT-FUT-DEPTH-1** |
| **status** | **WAIT_DIRECTOR** (VPS staging COUNT) |
| **조건1** | 기본=**프로덕션 직접 write** · staging=`BITGET_FUT_DEPTH_DB` · mtf tail이 깊이 지울 수 있음 |
| **금지** | FULL_BT=1은 Go 전 · 전체런 · LIVE 단정 |

---

## 디렉터

```
VPS staging COUNT:
export BITGET_FUT_DEPTH_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
bash bitget/deploy/run_fut_1d_depth_pilot.sh
출력(write_mode·merged)을 lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md에 붙여넣기.
```

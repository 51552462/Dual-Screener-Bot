# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-03  
> **유형**: B-3 Walk-Forward Shadow — **Claude OK ✅** · `SHADOW_OBSERVING`

---

## B-3 — Claude OK (2026-08-03)

격리(registry/INCUBATOR/`sim_kelly_invest` 불변)·weekly batch 분리·`PROMOTION_BLOCK_ENABLED` 방어적 구현 확인.  
**3-track 병렬 관측** 승인 (A `06` + B-2 + B-3). Live promotion block = 🔴 defer.

---

### sub-phase
B-3 (shadow 스코프만 — live promotion block **미구현**)

### SSOT (신규)
| 파일 | 역할 |
|------|------|
| `bitget/validation/walk_forward_bg.py` | 루트 `validation/walk_forward.py` 순수 함수 **포팅** (import 금지) + `evaluate_oos_pass_from_returns` |
| `bitget/validation/walk_forward_shadow_bg.py` | shadow 판정·persist·`run_walk_forward_shadow_job` |
| `bitget_walk_forward_shadow` | OOS pass/fail shadow 테이블 |

### Config
| KEY | default | 비고 |
|-----|---------|------|
| `WALK_FORWARD_SHADOW_ENABLED` | true | shadow batch on/off |
| `WALK_FORWARD_PROMOTION_BLOCK_ENABLED` | **false** | read-only stub — **live block 코드 없음** |

### 동작 요약
1. `bitget_forward_trades` CLOSED 행 로드 (시간순)
2. `normalize_market_key(market_type)` → SPOT\|FUT (raw BG 비교 **없음**)
3. `_extract_core_group(sig_type)` → group_key
4. 마지막 walk-forward fold OOS mean > 0 → pass
5. `bitget_walk_forward_shadow`에만 기록

### 실행 위치
- `bitget/pipelines/bitget_pipelines.py` — `_pipeline_weekly_evolution` step `walk_forward_shadow` (critical=False)
- **미포함**: `_pipeline_scan_spot` / `_pipeline_scan_futures` / `_pipeline_scan_all`

### 금지 준수 확인
- [x] `INCUBATOR_TEMPLATES` 쓰기 없음
- [x] `strategy_registry` state 변경 없음
- [x] `config_kv` promotion 필드 쓰기 없음
- [x] root `meta_governor._step_lifecycle` 미호출
- [x] `WALK_FORWARD_PROMOTION_BLOCK_ENABLED=true` 시에도 block 미적용 (warning log only)

### 테스트 (`test_walk_forward_shadow_b3.py` — **10 passed**)
| 테스트 | 내용 |
|--------|------|
| OOS pass/fail | synthetic returns — 최근 fold 양/음수 |
| 정규화 key | `BG` market_type → shadow row `SPOT`/`FUT` only |
| pipeline 격리 | scan 파이프라인에 `walk_forward_shadow` 없음 · weekly에만 존재 |
| 격리 assert | shadow on/off → registry · INCUBATOR · `sim_kelly_invest` 동일 |
| promotion flag | default false |

### 롤백
`WALK_FORWARD_SHADOW_ENABLED=false` → weekly batch skip

### Claude 확인 요청
1. OOS 판정 규칙 (last fold mean > 0, min 12 total / 5 OOS) — Handoff 스펙과 정합?
2. weekly_evolution hook tier — backup/deathmatch report와 동급으로 적절?
3. shadow-only 격리 테스트로 **A `06`·B-2 4w 병렬** 승인 가능?

---

## 참고 (설계 Q&A — 2026-08-02)

이전 Q1~Q5 설계 답변은 본 구현과 일치. 차이점:
- Q2에서 "루트 import only" 제안 → Handoff대로 **포팅** 적용 (CAT-MAP §5)

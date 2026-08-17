# CLAUDE → CURSOR (Bitget Handoff INBOX)

> **갱신**: 2026-08-17 · **I-GMM-DNA-01b Claude OK** · 신규 Handoff 없음 (서버 관측)

---

## [CAT-I] I-GMM-DNA-01b — GMM DNA 관측 미니잡 · **Claude OK 2026-08-17**

### sub-phase ID
I-GMM-DNA-01b

### Claude OK
- 2026-08-17 — Handoff 스펙 100% 일치 · 수정 spec 없음
- 다음: 디렉터 서버 1~2주 관측 (설계 Handoff 없음)

### 위험도
🟢 — 읽기 전용 + ops_events 신규 1건. gates.py/sync 비접촉.

---

## [CAT-I] I-GMM-DNA-01b — (구현 Handoff 원문 보존)

### sub-phase ID
I-GMM-DNA-01b

### SSOT (변경 금지 unless noted)
- 신규: `bitget/observability/gmm_dna_alpha_report_bg.py`
- 읽기만: `bitget_forward_trades`(CAT-D), `config_kv CRYPTO_DNA_ALPHA_RANK*`(CAT-K), 서버 로그
- **비접촉**: `forward/gates.py`, `evolution/gmm_dna_alpha_sync.py`

### 변경 Spec
- `compute_weekly_gmm_dna_alpha_report_bg(window_days:int=7) -> dict`
- `run_gmm_dna_alpha_report_job()`
- 필드: cos_eff_sample_count / zero_ratio / mean_nonzero(nullable) / open·closed_count_by_market / dna_rank_keys_present / shape_source_distribution / log_source_used
- 로그 파싱: `Cos_eff=([\d.]+)` 정규식, journalctl 우선 → 실패 시 파일 로그 → 둘 다 실패 시 null+unavailable
- 저장: ops_events `gmm_dna_alpha_report_weekly` (component `observability.dna`)

### Config 변경
| KEY | old | new | default |
|-----|-----|-----|---------|
| GMM_DNA_ALPHA_REPORT_ENABLED | 없음 | 신규 | true |
| GMM_DNA_ALPHA_REPORT_WINDOW_DAYS | 없음 | 신규 | 7 |
| GMM_DNA_ALPHA_REPORT_LOG_SOURCE | 없음 | 신규 | journal |

### 인접 CAT 영향
- CAT-D: 읽기만 · CAT-K: 읽기만 · CAT-I(gates/sync): 미접촉
- weekly_evolution: D-3a cost_report_weekly 직후 non-critical 스텝 추가

### 롤백 조건
- `GMM_DNA_ALPHA_REPORT_ENABLED=false` → 즉시 미실행, 거래 로직 영향 없음

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- 루트 주식 경로 수정 금지 — bitget/ 하위만.
- **`forward/gates.py`, `evolution/gmm_dna_alpha_sync.py` 수정 금지** (이번 Handoff 조건)
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/test_gmm_dna_alpha_report_i01b.py`

### 세션 종료 의무
- `05_진행로그.md` I-GMM-DNA-01b 섹션
- `00_전체현황판.md` Phase·SSOT
- `CURSOR_TO_CLAUDE.md` 갱신 (구현 요약 + 로컬 스냅샷)
- `NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

### 위험도
🟢 — 읽기 전용 + ops_events 신규 1건. gates.py/sync 비접촉. C-2·MDD5%·live·실전 범위 밖.

---

## [CAT-I] I-GMM-DNA-01 — GMM DNA → CRYPTO_DNA_ALPHA_RANK 배선 · **배포 완료**

### sub-phase ID
I-GMM-DNA-01

### Cursor 구현 요약 (2026-08-12)
- 신규: `gmm_dna_alpha_sync.py` · data_miner · config_bootstrap · gates 폴백
- Claude 조건부 OK · R1/R2 반영 · **서버 배포 완료 (디렉터 2026-08-17)**

### 위험도
🟡 — 진입 게이트 변경 (paper). `manual` source 보호·실거래 경로 무변경.

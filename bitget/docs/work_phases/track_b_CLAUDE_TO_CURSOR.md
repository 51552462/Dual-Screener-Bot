# CLAUDE → CURSOR (Bitget Handoff INBOX)

> **갱신**: 2026-08-12 · **I-GMM-DNA-01** (Cursor 구현 완료 → Claude OK 대기)

---

## [CAT-I] I-GMM-DNA-01 — GMM DNA → CRYPTO_DNA_ALPHA_RANK 배선 · **Cursor 구현 완료**

### sub-phase ID
I-GMM-DNA-01

### SSOT (변경 금지 unless noted)
- `bitget/evolution/gmm_dna_alpha_sync.py` — GMM→ALPHA_RANK sync
- `bitget/data_miner.py` — GMM cluster prototype shape + post-mine sync
- `bitget/pipelines/bitget_pipelines.py` — `config_bootstrap` 훅
- `bitget/forward/gates.py` — `_facts_cos_scalar_01` sn_score=0 폴백
- config: `BITGET_GMM_DNA_TEMPLATES` → `CRYPTO_DNA_ALPHA_RANK1..3`

### 변경 Spec
- GMM 클러스터 bounds 중점 → 7D DNA vec (`cpv,tb,bbe,rs`)
- `mean_mfe` 상위 3클러스터 → `CRYPTO_DNA_ALPHA_RANK1..3`
- `shape` 20봉: MFE prototype OHLCV 또는 neutral fallback
- `source=manual` 랭크는 force 없이 보존
- `BITGET_GMM_DNA_UPDATED_AT` > `CRYPTO_DNA_ALPHA_SYNCED_AT` 시 재동기화

### Cursor 구현 요약 (2026-08-12)
- 신규: `gmm_dna_alpha_sync.py`
- data_miner: prototype_market/symbol/tf + shape 채굴
- config_bootstrap: `sync_gmm_dna_alpha_if_stale()`
- gates: sn_score=0 → signal score/100 Cos 폴백 (과도기)
- 테스트: `test_gmm_dna_alpha_sync.py` **6 passed**

### Claude 검증 Ask
1. neutral shape fallback이 doppelganger DTW 왜곡 리스크 수용 가능한지
2. sn_score=0 폴백이 paper 관측에 적절한지 (실거래 영향 없음 확인)
3. forward_trades 0일 때 GMM 재채굴 없이 기존 GMM만 sync해도 Cos_eff>0 기대 타당성

### Claude 조건부 OK (2026-08-12)
- paper 배포 OK · R1/R2 Cursor 반영 완료 (force=False 기본 · live fail-closed)

### 위험도
🟡 — 진입 게이트 변경 (paper). `manual` source 보호·실거래 경로 무변경.

---

## [CAT-C] C-1 — Bad Tick / Flash Crash Filter · **Cursor 구현 완료**

### sub-phase ID
C-1

### SSOT (변경 금지 unless noted)
- `bitget/signal_engines.py` — `evaluate_bad_tick` · `bad_tick_should_skip_candidate`
- `bitget/supernova_hunter.py` · `bitget/master_scanner.py` — 호출부
- `blackhole_hunter` / `underdog_miner` — N/A (closed-trade analytics, 주석만)
- config: `BAD_TICK_FILTER_ENABLED` · `BAD_TICK_LOOKBACK_BARS` · `BAD_TICK_ATR_MULT` · `BAD_TICK_GAP_PCT` · `BAD_TICK_ACTION`

### Cursor 구현 요약 (2026-08-04)
- ATR deviation **AND** gap_pct 동시 초과 시 skip
- `ops_events` `bad_tick_filtered` 기록
- P0-6: `master_scanner` 기존 `bse` import 유지 — 별도 수정 불필요
- 테스트: `bitget/tests/test_bad_tick_filter_c1.py` **5 passed**

### 디렉터 Ask (P0-6)
**옵션 A 채택** — master_scanner는 이미 `import bitget.signal_engines as bse` 로 해결됨.

### C-2
명시적 **defer** (close PnL attribution).

---

## [CAT-L] L-1 — Log Rotation (P0-1) · **구현 완료 (Cursor)**

### sub-phase ID
L-1

### SSOT
- `bitget/deploy/install_bitget_logrotate.sh`
- `deploy/logrotate/bitget-dante.conf.in`
- `deploy/scripts/bitget_journal_vacuum.sh`
- `dante-bitget-journal-vacuum.{service,timer}`

### 디렉터 승인
병렬 🟢 (A paper 관측 중 착수)

---

## 상태

| 트랙 | sub | status |
|------|-----|--------|
| A paper | A-1~A-5 | 배포·`06` 대기 |
| 병렬 L | **L-1** | **Claude OK ✅** · 디렉터 서버 검증 |
| B-1 | Claude OK ✅ | |
| B-2 | Claude OK ✅ · **4w shadow 관측** | |
| B-3 | Claude OK ✅ · **4w shadow 관측** (weekly batch) | |
| B-4 | **Claude OK ✅** · MAB log 관측 (소비처 없음) | |
| L-2 | **Claude OK ✅** · 서버 install·drill 대기 | |
| 다음 | A `06` 1차(2주) 후 | B-4b · C-1 · Kelly Go/No-Go Handoff |

---

## [CAT-L] L-2 — Integrity Backup Cron · **구현 완료 (Cursor)**

### sub-phase ID
L-2 (P0-5)

### SSOT
- `bitget/deploy/backup_bitget_db.sh` · `install_bitget_backup.sh`
- `bitget/infra/integrity_backup_l2.py`
- `dante-bitget-backup.timer` · `bitget_restore_drill.sh`

### Cursor 산출
- `BITGET_BACKUP_ENABLED` / `BITGET_BACKUP_RETENTION_DAYS` / `BITGET_BACKUP_DIR`
- `test_backup_l2.py` 8 passed · restore drill · stock DB exclude

---

## [CAT-H, CAT-F] B-2 — Deathmatch Allocation Shadow · **구현 완료 (Cursor)**

### sub-phase ID
B-2 (shadow 4w log-only)

### SSOT
- `bitget/evolution/deathmatch_allocation_shadow.py`
- `forward/ledger.py` — `observe_kelly_chain_shadow` (return unchanged)
- `forward/deathmatch_report_section.py` — post-BR shadow persist

### Cursor 산출
- `DEATHMATCH_ALLOCATION_SHADOW_ENABLED` (default true)
- `test_deathmatch_shadow_b2.py` 4 passed (격리: shadow on/off 동일 `sim_kelly_invest`)
- `apply_deathmatch_allocation` **미변경**

---

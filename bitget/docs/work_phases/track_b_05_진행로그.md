# 진행 로그 (Bitget)

> 각 **sub-phase** 완료 시 추가. 다음 창 시작 시 **이 파일 전체** 참조.  
> sub-phase ID: A-1~A-5, B-1~B-4, C-1~C-3, D-1~D-3  
> **3단계 완료** 기준: `06_검증체크리스트_및_실패기록.md`  
> **협업 루프**: `07_듀얼AI_협업루프.md`

---

## 갱신 규칙

| 누가 | 언제 | 무엇 |
|------|------|------|
| **Cursor** | sub-phase 구현 세션 **종료 전** | 해당 sub 섹션 (날짜·요약·config·파일·잔여) |
| **Claude Pro** | OK 사인 후 | **`Claude OK: YYYY-MM-DD`** 한 줄 |
| **디렉터** | 2~4주 후 | `06` 효과 검증 표 |

**Claude OK 없이 sub-phase Done 처리 금지.**

---

## NS-BG-CRON-ISO-01 — 코인 서버 주식 북극성 cron 제거 [2026-08-23]

| 항목 | 내용 |
|------|------|
| **계기** | 디렉터: 코인 텔레그램에 `📊 주식 북극성` + `no such table: forward_trades` |
| **원인** | `update_bitget.sh` → `install_director_digest_cron.sh` (주식 19:30) on Bot-2 |
| **수정** | 설치 제거 · `uninstall_stock_north_star_cron.sh` · audit fail-if-present |
| **비변경** | `gates.py` · Kelly · MDD · live · `dual_north_star_telegram.py` 본체 |
| **status** | **WAIT_DIRECTOR** (코인 VPS에서 uninstall 1회) · Claude Ops OK 선택 |
| **Claude OK** | (대기) |

---

## SHORT 최상경로 — FUNNEL/DANTE/OBS/SECTOR [2026-08-21]

| 항목 | 내용 |
|------|------|
| **계기** | 롱만 진입 · 숏 미사용 · predicted_sector=UNKNOWN · 디렉터 승인 best-path 로드맵 |
| **구현** | SHORT-FUNNEL-01 · SHORT-DANTE-FUT-01 · SHORT-OBS-GATE-01 · CRYPTO-SECTOR-01 |
| **파일** | `short_funnel_report_bg.py` · digest 연동 · `scanner_hooks`/`master_scanner` spot dante no-op · ledger SHORT 차단 기록 · `signal_engines._apply_short_score_guards` · `auto_pilot`/`system_auto_pilot` sector writer · `CAT-MAP` PREDICTED_NEXT_SECTOR Writer 행 |
| **테스트** | `test_short_funnel_best_path_bg.py` **8 passed** · schedule/short scoring 회귀 OK |
| **비변경** | Cos/funding **임계값** · C-2 · MDD5% · live · `ENABLE_REAL_EXECUTION` · exit rewrite |
| **SHORT-DNA-01** | **defer** — TF당 SHORT CLOSED(mfe≥`BITGET_MIN_MFE_FOR_MINING`) ≥ `GMM_FIT_MIN_ROWS_OBSERVED`(12) 후 Claude Handoff |
| **Claude 1차** | FUNNEL OK · DANTE OK(조건) · OBS OK · SECTOR 조건부 OK → Cursor 3확인 회신(OUTBOX) |
| **status** | **WAIT_CLAUDE_OK** (SECTOR 최종 OK 한 줄) |
| **Claude OK** | (최종 한 줄 대기 — 3확인 회신 후) |

---

## NS-BG-DASH-01 — Bitget 북극성 쉬운판 [2026-08-21]

| 항목 | 내용 |
|------|------|
| **계기** | 디렉터: 주식 북극성 스크린샷 참조 → Bitget에 목표·수익 보이게 |
| **구현** | `north_star_panel_bg.py` · POST_DEPLOY_OBS 첫 메시지 연동 · persist=읽기전용 |
| **표시** | MDD≤5% · 연 12~25%(B0=측정) · 게이트 · 일/주/월/연 · spot/futures NAV |
| **비변경** | `gates.py` · Kelly · MDD tier · `dual_north_star_telegram.py`(Track A) |
| **잔여** | 디렉터 VPS pull 후 20:00 텔레그램 첫 메시지 육안 1회 |
| **status** | **SUB_DONE · Claude OK 2026-08-21** |
| **Claude OK** | **2026-08-21** — [CAT-J] OK · 수정 spec 없음 · Handoff 불필요 · 원장 read-only · gates/sync 비접촉 |

---

## POST_DEPLOY_OBS-DNA-UX-01 — DNA digest why-진단 [2026-08-20]

| 항목 | 내용 |
|------|------|
| **Handoff** | Downloads → `track_b_CLAUDE_TO_CURSOR.md` / `CLAUDE_TO_CURSOR.md` |
| **구현** | `diagnose_dna_state` · TF/MFE probe · kid plain Spec2 · 숫자 메모 Spec3 · paste Spec5 · kill-switch `POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED` |
| **파일** | `post_deploy_obs_digest_bg.py` · `gmm_dna_alpha_report_bg.py`(read helpers) · `memory_policy.py` · `test_post_deploy_obs_digest_bg.py` |
| **테스트** | **10 passed** |
| **비변경** | `gates.py` · `gmm_dna_alpha_sync.py` · MFE 문턱 기본값 |
| **status** | **Claude OK 2026-08-20** (조건부 OK · Mirror 닫힘) |
| **Claude OK** | **2026-08-20** — 스펙 일치 · enum=`DB_PATH_OR_ENV` 정식명 확인 · 서버 텔레그램 1회 육안만 잔여 |

---

## 사전 완료 (Phase 0–8 · 코드 구현)

> work_phases 이전에 완료된 Bitget 구현 — **참조용**. sub-phase ID 없음.

### Phase 0–8 Infrastructure — [~2026-06]

- pipelines SSOT, infra/runtime, governance/meta_sync, OMS phase 5–7
- docs: `01_architecture_mapping` ~ `08_phase8_track_a`
- validation: architecture_checks PASS, cutover env pending

### Institutional Audit — [2026-07-04]

- `13_institutional_grade_audit_and_roadmap.md` — P0/P1/P2 정의
- champion genesis ✅, lifecycle DB fix ✅

### Dual-AI Doc Structure — [2026-08-01]

- `bitget/docs/claude_project/` — CAT A–Q + MAP + SPOT-FUT + CONSTANTS
- `bitget/docs/work_phases/` — 본 폴더 신설
- **다음**: A-1 Handoff (Claude Pro)

---

## Phase 9 · 묶음A

### 묶음A 3단계 현황 (2026-08-02)

| 단계 | 내용 | 상태 |
|------|------|------|
| **1. 구현·스펙** | Cursor 구현 + Claude OK | A-1~A-5 **전부 ✅** |
| **2. 가상매매 반영** | 2~4주 paper trading (24/7 스캔 사이클) | ❌ **미시작** |
| **3. 효과 검증** | `06` NAV MDD 전후·tier 전이·이벤트 로그 | ❌ **대기** |

**Done 조건**: 3단계 전부 통과 후 `00` Phase 9 → **완료** · B-1 Handoff 시작.

### 🔴 디렉터 Critical 승인 (Go/No-Go · A묶음 5건 + D-2 1건)

> 형식: `07_듀얼AI_협업루프.md` 4단계 — 승인 시 해당 행에 **`디렉터 승인: YYYY-MM-DD`** 기입.

| sub | Critical 항목 | 영향 CAT | 디렉터 승인 |
|-----|--------------|----------|-------------|
| A-1 | Portfolio NAV MDD 서킷 (REDUCE/BLOCK/HALT) | F, N | [x] **디렉터 승인: 2026-08-02** |
| A-2 | Tail-risk fund drawdown 소비 | F, D | [x] **디렉터 승인: 2026-08-02** |
| A-3 | MAX_LEVERAGE hard cap (ops 5x clamp) | F, N | [x] **디렉터 승인: 2026-08-02** |
| A-4 | Gross notional exposure cap (80% NAV) | F, D | [x] **디렉터 승인: 2026-08-02** |
| A-5 | Config write reject (Kelly/leverage bounds) | K, F, G | [x] **디렉터 승인: 2026-08-02** (meta_sync reject 배포 후 로그 확인) |
| D-2 | LLM proposal approve → config_kv (`set_config_value`) | K, F, G, M | [x] **디렉터 승인: 2026-08-04** · poll 배선 ✅ Claude OK 2026-08-04 |

### 묶음A 완주 순서 (디렉터)

1. ~~**Critical 승인 5건**~~ — **완료 2026-08-02**
2. **paper 배포** — A-1~A-5가 24/7 스캔 사이클에 연결 (A-5: `meta_sync` → `set_config_value` reject 로그 확인)
3. **2~4주 대기** — `06` 효과 검증 기록표 채우기
4. **판정** — 유지 / 롤백 / 추가조정 (NAV MDD 악화 → **무조건 롤백**, `06` 하단 원칙)
5. **통과 시** — `00` Phase 9 완료 → B-1 Handoff (Claude Pro **새 창**)

--- 

### A-1 Portfolio NAV MDD Circuit Breaker — [2026-08-01] · 구현 1단계 ✅ / Claude OK ✅ (2026-08-01) / 효과 3단계 ❌

- **SSOT**: `bitget/trading/execution_safety.py` — `evaluate_portfolio_mdd_tier`, `evaluate_portfolio_mdd_gate`, `evaluate_nav_risk_gate` (treasury NAV)
- **NAV**: `TREASURY_SPOT_USDT + TREASURY_FUTURES_USDT` (config_kv)
- **Peak**: `PORTFOLIO_NAV_PEAK` — monotonic HWM, init `ACCOUNT_SIZE_USDT`, single writer execution_safety
- **Tier**: REDUCE −15% / BLOCK −20% / HALT −30% (ratio keys `PORTFOLIO_MDD_*_PCT`)
- **ledger**: `try_add` — 기존 `evaluate_nav_risk_gate` 경로 유지 (shared SSOT)
- **Kelly**: `nav_size_mult` — Kelly chain **마지막** (`sim_kelly_invest` 직전) 곱
- **config 신규**:
  - `PORTFOLIO_MDD_REDUCE_PCT` (0.15), `PORTFOLIO_MDD_BLOCK_PCT` (0.20), `PORTFOLIO_MDD_HALT_PCT` (0.30)
  - `PORTFOLIO_MDD_REDUCE_SIZE_MULT` (0.5)
  - `PORTFOLIO_NAV_PEAK`, `PORTFOLIO_MDD_CURRENT_TIER` (state)
  - `PORTFOLIO_MDD_BREAKER_ENABLED` (true, kill-switch)
- **파일**: `execution_safety.py`, `memory_policy.py`, `config_bounds.py`, `forward/ledger.py` (import fix), `deploy/bitget.env.example`, `tests/test_portfolio_mdd_a1.py`, `tests/test_trading_phase5.py`
- **review patch [2026-08-01]**: persist 실패 시 HALT alert 억제 · try_add BLOCK·HALT integration test · ledger `_extract_core_group`/`_thompson_ns_prefix` import
- **테스트**: `pytest bitget/tests/test_portfolio_mdd_a1.py bitget/tests/test_trading_phase5.py` **35 passed**
- **Claude OK**: 2026-08-01 — NAV SSOT 분열(tail/gross)은 A-2/A-4에서 `evaluate_portfolio_mdd_tier` 재사용으로 해소 예정 (`00` 추적 항목)
- **잔여**: [x] Claude Pro OK · [x] 디렉터 Critical 승인 **2026-08-02** · [ ] 2~4주 `06` 효과 검증

### A-2 Tail-Risk Fund Consumption — [2026-08-02] · 구현 1단계 ✅ / Claude OK ✅ (2026-08-02) / 효과 3단계 ❌

- **SSOT**: `bitget/trading/tail_risk_gate.py` — debit on BLOCK/HALT drawdown_event, `evaluate_tail_fund_gate`
- **A-1 재사용**: `get_portfolio_mdd_snap_cached` → tier/dd_pct/nav_peak (재계산 금지)
- **debit**: `min(balance, nav_peak × (dd_pct − PORTFOLIO_MDD_BLOCK_PCT))` per request cycle
- **escalate**: `tail_exhausted` + BLOCK → HALT급 진입 차단 (auxiliary; PORTFOLIO tier/telegram 불변)
- **kill-switch**: `TAIL_FUND_CONSUMPTION_ENABLED` (default true; false → legacy accrual-only block path)
- **파일**: `tail_risk_gate.py`, `execution_safety.py`, `ledger.py`, `memory_policy.py`, `tests/test_tail_fund_a2.py`
- **테스트**: 42 passed (a2 7 + a1 8 + phase5 27)
- **Claude OK**: 2026-08-02
- **잔여**: [x] Claude Pro OK · [x] 디렉터 Critical 승인 **2026-08-02** · [ ] 2~4주 `06` 효과 검증
- **Claude 회신**: snap cache = A-1 wrapper only; CAT-N gate 번호 코드 SSOT 정렬

### A-3 MAX_LEVERAGE Hard Cap — [2026-08-02] · 구현 1단계 ✅ / Claude OK ✅ (2026-08-02) / 효과 3단계 ❌

- **선독 충돌 보고**: 운영 default **3x** (`FUTURES_LEVERAGE` / `resolve_leverage` default) — **5x 캡과 충돌 없음** → 구현 진행
- **SSOT**: `execution_safety.resolve_max_leverage` → `min(requested, MAX_LEVERAGE)` + clamp log
- **경로**: `leverage_manager.resolve_leverage` (real) · `ledger.try_add` FUT → `resolve_leverage` (paper)
- **config**: `MAX_LEVERAGE=5` (`DEFAULT_MAX_LEVERAGE` in `memory_policy.py`)
- **SPOT**: `leverage=1.0` — `resolve_max_leverage` 미호출
- **파일**: `execution_safety.py`, `leverage_manager.py`, `ledger.py`, `tests/test_max_leverage_a3.py`, `deploy/bitget.env.example`, CAT-N/CAT-CONSTANTS
- **테스트**: A-bundle **49 passed** (`test_max_leverage_a3` 6 + tail 8 + a1 8 + phase5 27)
- **Claude OK**: 2026-08-02
- **잔여**: [x] Claude Pro OK · [x] 디렉터 Critical 승인 **2026-08-02** · [ ] 2~4주 `06` 효과 검증

### A-4 Gross Notional Exposure Cap — [2026-08-02] · 구현 1단계 ✅ / Claude OK ✅ (2026-08-02) / 효과 3단계 ❌

- **SSOT**: `evaluate_gross_notional_gate_values` — gate 7 + `ledger.try_add` (`evaluate_gross_notional_gate`)
- **gross**: `Σ(quantity×entry_price)` OPEN — 레버리지 무관
- **nav_current**: `gross_gate_nav_current` → A-1 `get_portfolio_mdd_snap_cached`
- **config**: `MAX_GROSS_NOTIONAL_PCT=80`, `GROSS_NOTIONAL_CAP_ENABLED=true`
- **concentration_gate**: BTC-proxy per-cluster — **독립** (대체 아님)
- **테스트**: A-bundle **55 passed**
- **Claude OK**: 2026-08-02
- **잔여**: [x] Claude Pro OK · [x] 디렉터 Critical 승인 **2026-08-02** · [ ] 2~4주 `06` 효과 검증

### A-5 Config Write-Time Validation — [2026-08-02] · 구현 1단계 ✅ / Claude OK ✅ (2026-08-02) / 효과 3단계 ❌

- **SSOT**: `set_config_value` only — `CONFIG_WRITE_REJECT_BOUNDS` in `config_bounds.py`
- **keys**: `DYNAMIC_KELLY_RISK` [0.002, 0.030], `MAX_LEVERAGE` [1, 10] — **REJECT** (not clamp)
- **kill-switch**: `CONFIG_WRITE_VALIDATION_ENABLED=true` (false → legacy `clamp_config_value` path)
- **non-breaking**: ops values (0.01, 5) pass; unbounded keys (`TREASURY_*`) unchanged
- **meta_sync**: out-of-range `kelly_cap` → silent reject, prior value retained (no crash)
- **파일**: `config_bounds.py`, `config_manager.py`, `memory_policy.py`, `tests/test_config_write_validation_a5.py`, `deploy/bitget.env.example`
- **테스트**: A-bundle **67 passed** (a5 8 + hard_bounds 4 + a4 6 + a3 6 + tail 8 + a1 8 + phase5 27)
- **Claude OK**: 2026-08-02 — reject-not-clamp; meta_sync out-of-range safe
- **잔여**: [x] Claude Pro OK · [x] 디렉터 Critical 승인 **2026-08-02** (meta_sync reject 배포 확인) · [ ] 2~4주 `06` 효과 검증

### Paper 관측 중 병렬 작업 (2026-08-02 · Cursor 제안 · Claude 검토 대기)

> **디렉터 의도**: A paper 2~4주 **대기 ≠ 작업 중단** — B/C/D/L 순서대로 병렬 착수.  
> **SSOT**: `CURSOR_TO_CLAUDE.md` · 합의 후 `CLAUDE_TO_CURSOR.md` Handoff 1개씩.

#### Attribution 규칙 (제안)

| 등급 | paper 관측 중 |
|------|---------------|
| 🟢 NAV/gate/Kelly 무영향·오프라인 | 병렬 OK |
| 🟡 try_add/config 접촉 · default OFF/shadow | 조건부 |
| 🔴 Kelly·NAV·gate live 실효 | A `06` 후 |

#### Cursor 분류 요약

| 등급 | sub-phase |
|------|-----------|
| 🟢 | L-1, L-2, B-1, B-3, D-1, D-3(read-only) |
| 🟡 | B-4, C-1, D-2(queue), B-2 shadow |
| 🔴 | B-2 live, C-2, C-3 |

#### 제안 순서 (초안)

`L-1 → L-2 → B-1 → (B-3?) → D-1 → D-3` (paper 트랙과 병렬)

#### Claude 질문 (미답 → 2026-08-04 운영 확정)

- [x] 첫 Handoff: **L-1** (2026-08-02)
- [x] B-2 shadow 4w ↔ A paper 2~4w 병렬 — **승인됨** (B-2/B-3/B-4 Claude OK)
- [x] C-1/C-2: **C-1 = 다음 Handoff (🟡)** · **C-2 = 🔴 defer** (close PnL attribution)
- [ ] Green/Yellow/Red 전면 재분류 — C-1 Handoff 시 Claude에 한 줄 확인만

#### 목표 대비 갭 · 병렬 루프 (2026-08-04)

> **의도**: 검증만 기다리지 않음. Layer 1(디렉터) · Layer 2(C-1) 병렬 · Layer 3 금지.

| Layer | 내용 | 상태 |
|-------|------|------|
| 1 | L-1/L-2 설치 · MemoryMax · paper 배포·cutover | 디렉터 |
| 2 | **C-1** bad tick → D-1 → P0-6/P1-7 | **WAIT_CLAUDE_HANDOFF** |
| 3 | MDD 5% · B-2 live · C-2/C-3 · 실전 | 🔴 `06` 후 |

- **SSOT**: `00_전체현황판.md` 갭 표 · `CURSOR_TO_CLAUDE.md` C-1 요청
- **잔여**: [ ] Claude 갭 OK · [ ] C-1 Handoff · [ ] Cursor C-1 구현

---

## 인프라 · CAT-L

### L-1 Log Rotation (P0-1) — [2026-08-02] · 구현 1단계 ✅ / Claude OK ✅ (2026-08-02) / 효과 3단계 ❌

- **SSOT**: `deploy/install_bitget_logrotate.sh` + `logrotate/bitget-dante.conf.in` + `dante-bitget-journal-vacuum.timer`
- **file logs**: stamped `bitget_*.log` → logrotate daily×14 + compress; `bitget.log` → RotatingFileHandler (50MB×5) + logrotate safety net
- **journal**: `bitget_journal_vacuum.sh` — `BITGET_JOURNAL_MAX_USE` (400M) · `BITGET_JOURNAL_MAX_RETENTION` (30d)
- **stamped TTL**: `BITGET_STAMPED_LOG_RETENTION_DAYS=14` → `disk_manager.cleanup_stamped_shell_logs`
- **optional**: `BITGET_INSTALL_JOURNALD_DROPIN=1` — coin-dedicated journald cap
- **금지 준수**: execution_safety / ledger / tail_risk_gate **미수정**
- **서버 설치**: `sudo INSTALL_ROOT=... bitget/deploy/install_bitget_logrotate.sh` · `--test` = logrotate -d + vacuum dry-run (실 rotation/vacuum은 디렉터 서버 검증)
- **테스트**: `test_log_rotation_l1.py` **7 passed**
- **Claude OK**: 2026-08-02 — 스펙 일치, 거래 경로 미접촉 확인. 후속: journald vacuum이 ops_events heartbeat 조회에 영향 없는지 1줄 확인 요청(비차단).
- **모니터링 (확인 1줄)**: watchdog heartbeat SSOT = SQLite `ops_events` `heartbeat.tick` (`watchdog.py` / `ops_logger.py`) — journal vacuum은 **journald 디스크만** 정리하며 heartbeat 판정·stale(분 단위)과 무관; `journalctl -f`는 실시간 tail만 영향(30d/400M 밖 과거 로그 삭제).
- **잔여**: [x] Claude Pro OK · [ ] 서버 `install_bitget_logrotate.sh` **(2026-08-17 미확인)** · [ ] `06` 30일 disk 안정

### L-2 Integrity Backup Cron (P0-5) — [2026-08-02] · Cursor 구현 ✅ / Claude OK ✅ (2026-08-02)

- **SSOT**: `deploy/backup_bitget_db.sh` · `deploy/install_bitget_backup.sh` · `dante-bitget-backup.timer` · `deploy/scripts/bitget_restore_drill.sh`
- **Core**: `infra/integrity_backup_l2.py` — Online Backup API + `PRAGMA integrity_check` · `BITGET_DB_STORAGE_PATH` 하위 `bitget_*.sqlite` only
- **금지 준수**: `market_data.sqlite`(주식) 제외 · execution_safety / ledger / config_manager **미수정**
- **Retention**: daily×7 + weekly×4 (`BITGET_BACKUP_RETENTION_DAYS` / `BITGET_BACKUP_WEEKLY_KEEP`)
- **Config**: `BITGET_BACKUP_ENABLED` (default true) · `BITGET_BACKUP_DIR` · `BITGET_BACKUP_RETENTION_DAYS=7`
- **restore drill**: `bitget_restore_drill.sh` — 격리 임시 경로 복원 + row-count parity (`06` 판정 = drill pass)
- **테스트**: `test_backup_l2.py` **8 passed**
- **Claude OK**: 2026-08-02 — L-2 backup scope·integrity·restore drill 확인. P0-5 서버 설치는 **2026-08-17 기준 미확인**.
- **후속 (비차단)**: L-1 로그 영역(`BITGET_LOG_DIR`·journald)과 `BITGET_BACKUP_DIR` 디스크 예산 — **분리 권장**(백업은 별도 파티션 또는 `data/backups/db`; 로그·journal·백업 합산 80GB SSD 내 retention 상한으로 경쟁 최소화).
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [ ] 서버 `install_bitget_backup.sh` + restore drill **(2026-08-17 미확인)** · [ ] `06` restore drill pass 기록

---

## Phase 10 · 묶음B

### B-1 · Deathmatch Market Key Normalization (2026-08-02)

- **SSOT**: `bitget/evolution/market_key_normalize.py` — `normalize_market_key(raw) → "SPOT"|"FUT"`
- **Registry lifecycle hook**: `bitget/evolution/registry_lifecycle_bg.py` — `meta_sync._run_bitget_meta_governor_cycle` 종료 후 BG read-time resolve + write-through (루트 `meta_governor._step_lifecycle` 미수정)
- **Deathmatch report**: `forward/deathmatch_report_section.py` · `evolution/deathmatch_bg.py` — 동일 `normalize_market_key` emit
- **Adapter**: `infra/market_keys.to_deathmatch_key` → kill-switch on 시 `normalize_market_key` 위임 (기존 호출부 회귀 방지)
- **Config**: `DEATHMATCH_KEY_NORMALIZE_ENABLED` (default **true**) — env / config_kv / `memory_policy`
- **레거시 BG**: 일괄 rewrite 금지 — forward_trades majority hint로 결정적 SPOT/FUT resolve 후 write-through
- **테스트**: `test_market_key_b1.py` **7 passed** · `test_transplant_gaps::TestMarketKeys` **4 passed**
- **Claude OK**: 2026-08-02 — key SSOT·lifecycle hook(post-lifecycle adapter, 루트 미수정)·report alignment 확인. B-2 Handoff 가능.
- **후속 (비차단)**: forward_trades majority hint **동률** 시 `max(counts, key=count)` → 키 사전순 최대(`spot` > `futures`) · **이력 0건/hint 없음** → `SPOT` 기본.
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [x] B-2 Handoff 완료

### B-2 · Deathmatch Allocation Shadow (4w log-only) (2026-08-02)

- **SSOT**: `bitget/evolution/deathmatch_allocation_shadow.py` — counterfactual Kelly mult 계산·shadow 테이블만 기록
- **Hook**: `forward/ledger.py` `observe_kelly_chain_shadow` (Kelly chain 직후, **return unchanged**) · `deathmatch_report_section` post-BR shadow persist
- **Config**: `DEATHMATCH_ALLOCATION_SHADOW_ENABLED` (default **true**) · `apply_deathmatch_allocation` **False 불변**
- **격리**: shadow on/off → `sim_kelly_invest` 100% 동일 (필수 테스트)
- **테스트**: `test_deathmatch_shadow_b2.py` **4 passed**
- **Claude OK**: 2026-08-02 — B-2 shadow 격리·log-only 확인 (`sim_kelly_invest` 100% 동일, `apply_deathmatch_allocation` False 불변). Kelly 실효 반영은 🔴 Go/No-Go 별도 Handoff.
- **Attribution**: 격리 테스트 증명 → 🟡 **A paper `06`과 B-2 shadow 4w 관측 병렬 승인** (NAV/gate/Kelly live 영향 없음)
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [ ] shadow **4w 관측** (`06`) · [ ] Kelly 실효 (🔴 Go/No-Go · A `06` 후 별도 Handoff)

### B-3 · Walk-Forward Shadow (dry-run judgment log-only) (2026-08-03)

- **스코프**: shadow only — `WALK_FORWARD_PROMOTION_BLOCK_ENABLED` **false 불변** · registry/config/INCUBATOR **미접촉**
- **SSOT**: `bitget/validation/walk_forward_bg.py` (루트 `validation/walk_forward.py` 순수 함수 **포팅**, import 금지)
- **Shadow**: `bitget/validation/walk_forward_shadow_bg.py` — CLOSED `bitget_forward_trades` → B-1 `normalize_market_key` + `group_key` → OOS pass/fail
- **Table**: `bitget_walk_forward_shadow` (B-2 `bitget_deathmatch_alloc_shadow` 동일 패턴)
- **실행**: `weekly_evolution` pipeline step `walk_forward_shadow` (factory scan 루프 **밖** · non-critical)
- **Config**: `WALK_FORWARD_SHADOW_ENABLED` (default **true**) · `WALK_FORWARD_PROMOTION_BLOCK_ENABLED` (default **false**, 미적용)
- **격리**: shadow on/off → `strategy_registry` row · `INCUBATOR_TEMPLATES` · `sim_kelly_invest` 100% 동일
- **테스트**: `test_walk_forward_shadow_b3.py` **10 passed**
- **Claude OK**: 2026-08-03 — B-3 shadow 격리(registry/INCUBATOR/`sim_kelly_invest` 불변)·weekly batch 분리·`PROMOTION_BLOCK_ENABLED` 방어적 구현 확인. Live promotion block은 🔴 — A `06` 완료 + 디렉터 Go/No-Go 후 별도 Handoff.
- **Attribution**: 격리 테스트 증명 → 🟡 **A paper `06`·B-2 shadow·B-3 shadow 3-track 병렬 관측** (NAV/gate/Kelly live 영향 없음)
- **후속 (비차단)**: OOS min 12/5 fold 기준 근거 1줄 — `walk_forward_bg.py` 상수 docstring 보강
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [ ] shadow **4w 관측** (`06`) · [ ] live promotion block (🔴 Go/No-Go · A `06` 후 별도 Handoff)

### B-4 · Registry Lifecycle & Explore Budget (MAB log-only) (2026-08-03)

- **스코프**: compute + config log only — **소비처(스캐너/MAB 배선) 미포함**
- **SSOT**: `bitget/evolution/registry_lifecycle_bg.py` — `count_lifecycle_states_bg` · `compute_explore_budget_bg` · `refresh_lifecycle_explore_budget_bg`
- **Hook**: `normalize_bitget_registry_after_lifecycle` 종료 후 B-4 persist (B-1 동작 유지 · 루트 `meta_governor` 미수정)
- **읽기**: `strategy_registry` RETIRED/COOLED 등 — `(normalize_market_key, group_key)` dedupe (raw BG 이중 집계 금지)
- **쓰기**: `config_manager.set_config_value("MAB_EXPLORE_BUDGET_CURRENT", ratio)` only
- **Config**: `LIFECYCLE_EXPLORE_BUDGET_ENABLED` (default **true**) · `MAB_EXPLORE_BUDGET_CURRENT` (default **0.0**)
- **격리**: on/off → `sim_kelly_invest` · `INCUBATOR_TEMPLATES` · B-2/B-3 shadow 테이블 불변
- **테스트**: `test_registry_lifecycle_b4.py` **9 passed** · `test_champion_genesis_bg.py` 회귀 **통과**
- **Claude OK**: 2026-08-03 — B-4 격리(`sim_kelly_invest`/INCUBATOR/B-2·B-3 shadow row 불변)·B-1 hook 확장 방식·`config_manager` 경유 확인. `compute_explore_budget_bg` 계수는 소비처 없어 실효 영향 0, 재검토는 B-4b(소비 배선) 시점으로 defer.
- **Attribution**: 🟢 — A `06` + B-2 + B-3 shadow + **B-4 로그** 병렬 관측 승인 (NAV/gate/Kelly live 영향 없음)
- **후속 (비차단)**: cap 0.50은 B-4b MAB 소비 배선 시 재검토(통상 explore 5~20%) · `06` MAB explore events용 값 시계열(weekly 로그 등) — B-4b 또는 다음 세션
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [ ] MAB 소비 배선 B-4b (별도 Handoff · 착수 금지) · [ ] (비차단) 값 시계열 관측 방식 확인

---

## 병렬 NS · NS-1 듀얼 북극성 진행장부·텔레그램 (2026-08-03)

> **범위**: 루트 read-only aggregator — Bitget/주식 **NAV·config_kv·governor·Kelly·execution_safety 미접촉**

### NS-1 — 1차 구현 + Claude 조건부 OK (2026-08-03)

- **SSOT**: `dual_north_star_ledger.py` · `dual_north_star_telegram.py` · `dual_north_star_ledger.json`
- **목표**: Track A 10%/40~70% · Track B 5%/12~25% (`00` §0.4) — 병렬 페이스·상품화 게이트 G0~G3
- **텔레그램**: `REPORT_BOT_*` · `factory.sh --north-star-digest` · cron 일19:30/토11:00/월1일
- **문서**: `docs/work_phases/12_듀얼북극성_진행장부_및_상품화.md` · `00` §0.5
- **Claude OK**: **2026-08-03 조건부 OK** — R1/R3/R4/R6 + Q8/Q9/Q11 반영
- **조건부 수정**: R1 28일 미만 배너 · R3 Bitget paper 배너(C-2 전 고정) · R4 G2 trades>30 / G3 A06+C2+MDD4주 · Q8 B0 리더 폐지·B1+ 목표달성률% · 게이트=종합60/40만
- **config (읽기)**: `A06_CHECKLIST_FIRST_PASS` · `C2_FUNDING_PNL_COMPLETE` / `BITGET_FUNDING_PNL_IN_LEDGER` — 미설정=미통과
- **테스트**: `tests/test_dual_north_star_ledger.py` **7 passed**
- **Attribution**: 🟢 SHADOW_OBSERVING 유지 — 표시·게이트 조건만
- **잔여**: [x] Claude 조건부 OK · [x] Cursor R1~R6 반영 · [ ] 서버 cron 3줄 · [ ] 2주 ledger 이력 · [ ] NS-1b (30일+ NAV 연환산 페이스 · 별도 Handoff)

---

## Phase 11 · 묶음C

### C-1 Bad Tick / Flash Filter — [2026-08-04] · 구현 1단계 ✅ / Claude OK ✅ (2026-08-04)

- **audit**: P1-6 · **CAT**: C · **스펙**: `03_묶음C_작업지시서.md` · Handoff `CLAUDE_TO_CURSOR.md`
- **SSOT**: `signal_engines.evaluate_bad_tick` · `bad_tick_should_skip_candidate`
- **Hook**: `supernova_hunter` · `master_scanner` (engine hit + embedded supernova) — **pre-candidate**
- **N/A**: `blackhole_hunter` · `underdog_miner` (closed-trade analytics, candidate 경로 없음) — Claude 원안 정정 수용
- **Config**: `BAD_TICK_FILTER_ENABLED` · `BAD_TICK_LOOKBACK_BARS`(20) · `BAD_TICK_ATR_MULT`(6) · `BAD_TICK_GAP_PCT`(0.15) · `BAD_TICK_ACTION`(skip)
- **ops_events**: `bad_tick_filtered` · component `scanner.*`
- **이중 필터**: C-1(pre-candidate) + CAT-N `price_sanity_gate`(try_add) — **분리 유지** (통합 아님, Mirror 2026-08-04)
- **P0-6**: master_scanner `bse` import 기존 해결 — 별도 섹션 없음 (옵션 A)
- **격리**: try_add gate 순서 불변 · Kelly/NAV 미접촉
- **테스트**: `test_bad_tick_filter_c1.py` **5 passed** — `test_spike_flags_bad_tick` · `test_normal_volatility_no_false_block` · `test_disabled_filter_soft_pass` · `test_short_history_soft_pass` · `test_regression_sample_false_block_rate_low`
- **Attribution**: 🟡 A `06` + shadow 병렬
- **Claude OK**: 2026-08-04 — 스펙 100% 일치. blackhole/underdog N/A 판정 수용(원안 정정). price_sanity(CAT-N) 이중 필터는 분리 유지 권장(통합 아님), threshold 1회 대조는 비차단 후속.
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [ ] `06` 효과 (ops skip률 <0.5% 목표) · [x] ops 관측 미니잡 → **C-1b** · [ ] (비차단) threshold C-1↔price_sanity 1회 대조

### C-1b Bad Tick Skip Summary (weekly) — [2026-08-04] · 구현 ✅ / Claude OK 대기

- **audit**: Mirror #1 · **CAT**: C · Handoff CAT-C C-1b
- **SSOT**: `observability/bad_tick_skip_summary_bg.py` — `compute_bad_tick_skip_summary_bg` · `run_bad_tick_skip_summary_job`
- **읽기**: `ops_events` `bad_tick_filtered` (C-1 SSOT, schema 불변)
- **쓰기**: `ops_events` 1건 `bad_tick_skip_summary_weekly` · component `observability.bad_tick` — 신규 DB/테이블 없음
- **Hook**: `weekly_evolution` — `walk_forward_shadow` 직후 non-critical step `bad_tick_skip_summary`
- **Config**: `BAD_TICK_SKIP_SUMMARY_ENABLED`(true) · `BAD_TICK_SKIP_SUMMARY_WINDOW_DAYS`(7)
- **denominator**: ops_events에 scan-count 카운터 **없음** (`scan_funnel_summary` 등 후보도 미기록) → v1 `skip_rate_pct=null` · 분모 신설 범위 밖
- **격리**: read-only · Kelly/NAV/try_add 미접촉 · `BAD_TICK_SKIP_SUMMARY_ENABLED=false` 즉시 off
- **테스트**: `test_bad_tick_skip_summary_c1b.py` **8 passed** — `test_synthetic_rows_grouped_correctly` · `test_persist_writes_weekly_summary_event` · `test_no_denominator_skip_rate_null` · `test_scan_funnel_summary_denominator_computes_rate` · `test_disabled_job_returns_none` · `test_scan_pipelines_exclude_bad_tick_skip_summary` · `test_weekly_evolution_includes_bad_tick_skip_summary_after_walk_forward` · `test_walk_forward_shadow_step_unchanged_when_summary_disabled`
- **Attribution**: 🟢 read-only observability

### C-2 · C-3 — 🔴 defer

- C-2 funding PnL · C-3 correlation cap — A `06` / Go-No-Go **전 착수 금지**

---

## Phase 12 · 묶음D

### D-1 Structured JSON Proposal — [2026-08-04] · 구현 ✅ / Claude OK ✅ (2026-08-04)

- **audit**: P2-4 선행 · **CAT**: M · Handoff CAT-M D-1
- **SSOT**: `governance/ai_proposal_schema_bg.py` — `validate_llm_proposal` · `persist_proposal_bg` · `process_structured_llm_proposal`
- **Hook**: `ai_overseer.run_ai_auditor` — LLM 응답 후 structured block validate (minimal diff)
- **비변경**: config_kv 전체 · `weekly_action_plan.py` (수정 없음)
- **weekly_action_plan 확인**: LLM tail은 `WEEKLY_ACTION_PLAN_USE_LLM` opt-in **표시용**; `persist_weekly_baseline`은 `WEEKLY_REPORT_BASELINE` 스냅샷만 — **proposal auto-apply 없음** (D-2 대기)
- **risk_class**: LLM 값 무시 · CAT-MAP §6 category→`critical|high|medium|low` 서버 재계산
- **parse fail**: invalid proposal **미저장** · `ops_events.llm_proposal_parse_error` · telegram alert · config_kv **미접촉**
- **no JSON block**: silent skip (일일 free-text 감사 스팸 방지)
- **Config**: `AI_PROPOSAL_STRUCTURED_ENABLED`(true)
- **테이블**: `bitget_llm_proposals` (market_data SQLite)
- **테스트**: `test_ai_overseer_proposal_d1.py` **5 passed** — `test_valid_proposal_persisted` · `test_parse_fail_no_persist_config_kv_untouched_ops_event_recorded` · `test_disabled_is_no_op` · `test_server_risk_class_overrides_llm_value` · `test_no_json_block_is_silent_skip`
- **Attribution**: 🟢 config_kv·실행 미접촉
- **Claude OK**: 2026-08-04 — Handoff 100% 일치. silent-skip( no JSON block ) 수용 — parse fail과 구분, 일일 free-text 감사 공존.
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [x] D-1b · [x] D-2 코드 · [x] D-2 디렉터 Critical **2026-08-04** · [x] D-2 poll · [x] D-3

### D-1b LLM Proposal Summary (weekly) — [2026-08-04] · 구현 ✅ / Claude OK ✅ (2026-08-04)

- **audit**: Mirror #1 · **CAT**: M · Handoff CAT-M D-1b
- **SSOT**: `observability/llm_proposal_summary_bg.py` — `compute_llm_proposal_summary_bg` · `run_llm_proposal_summary_job`
- **읽기**: `bitget_llm_proposals` (D-1) · `ops_events.llm_proposal_parse_error` (D-1)
- **쓰기**: `ops_events` 1건 `llm_proposal_summary_weekly` · component `observability.llm_proposal` — 신규 DB/테이블 없음
- **Hook**: `weekly_evolution` — `bad_tick_skip_summary` 직후 non-critical step `llm_proposal_summary`
- **Config**: `AI_PROPOSAL_SUMMARY_ENABLED`(true) · `AI_PROPOSAL_SUMMARY_WINDOW_DAYS`(7)
- **parse_error_rate_pct**: 분모 = `total_count + parse_error_count` (structured parse 시도만; C-1b처럼 외부 scan 카운터 없음 → 빈 window는 `null`)
- **격리**: read-only · config_kv·`ai_proposal_schema_bg` 미접촉 · `AI_PROPOSAL_SUMMARY_ENABLED=false` 즉시 off
- **테스트**: `test_llm_proposal_summary_d1b.py` **7 passed** — `test_synthetic_rows_grouped_correctly` · `test_persist_writes_weekly_summary_event` · `test_parse_error_rate_when_denominator_exists` · `test_empty_window_parse_error_rate_null` · `test_disabled_job_returns_none` · `test_scan_pipelines_exclude_llm_proposal_summary` · `test_weekly_evolution_includes_llm_proposal_summary_after_bad_tick`
- **Attribution**: 🟢 read-only observability
- **Claude OK**: 2026-08-04 — Handoff 100% 일치. 빈 window `parse_error_rate_pct=null` 수용.
- **잔여**: [x] D-1b Cursor · [x] D-1b Claude OK · [x] D-2 Cursor · [x] D-2 Claude OK · [x] 디렉터 Critical 2026-08-04 · [x] D-2 poll · [x] D-2 poll Claude OK · [x] D-3 · [x] D-3 Claude OK

### D-2 Human Approval Gate — [2026-08-04] · 구현 ✅ / Claude OK ✅ (2026-08-04) · 🔴 High

- **audit**: P2-4 · **CAT**: M · Handoff CAT-M D-2
- **SSOT**: `governance/proposal_approval_bg.py` — `record_approval_decision` · `apply_approved_proposal` · `process_proposal_telegram_command`
- **테이블**: `bitget_llm_proposal_approvals` (append-only INSERT, Single Writer D-2)
- **status**: 이벤트 없음=pending · 최신 이벤트=approved/rejected (파생값, 컬럼 아님)
- **telegram**: `/proposal_approve <id>` · `/proposal_reject <id>` — `REPORT_BOT_*` whitelist chat_id만
- **risk_class critical/high**: full id만 (prefix 매칭 금지) · 알림 위험도 배지
- **config**: `set_config_value` 경유만 · A-5 bounds 부분 성공(키별 applied/rejected)
- **비변경**: `bitget_llm_proposals` schema · `ai_proposal_schema_bg.py`
- **Config**: `AI_PROPOSAL_APPROVAL_GATE_ENABLED`(true)
- **격리**: reject/미인증/duplicate → config 미접촉 · gate off → 전체 no-op
- **테스트**: `test_proposal_approval_d2.py` **9 passed** — `test_approve_records_event_and_applies_config` · `test_reject_does_not_touch_config` · `test_unauthorized_chat_ignored` · `test_duplicate_approve_ignored` · `test_partial_apply_a5_bounds` · `test_gate_disabled_no_op` · `test_critical_rejects_prefix_match` · `test_approvals_table_append_only_insert` · `test_derived_status_pending_until_event`
- **Attribution**: 🔴 config_kv 실쓰기 경로 최초 개방 (approve 후에만)
- **Claude OK**: 2026-08-04 — Handoff 100% 일치, 편차 없음.
- **디렉터 Critical 승인**: [x] **2026-08-04**
- **잔여**: [x] 디렉터 승인 · [x] D-2 poll 배선 · [x] D-2 poll Claude OK · [x] D-3 · [x] D-3 Claude OK

### D-2 poll 배선 — [2026-08-04] · 구현 ✅ / Claude OK ✅ (2026-08-04)

- **SSOT**: `governance/proposal_approval_poll_bg.py` — `poll_proposal_approval_updates_once` · `run_proposal_approval_poll_job`
- **Hook**: `ai_overseer.overseer_loop` — 30초마다 getUpdates `timeout=0` (non-blocking)
- **Token**: `telegram_env.get_report_token()` (REPORT_BOT_*)
- **State**: `proposal_approval_poll_state.json` (last_update_id)
- **Config**: `AI_PROPOSAL_APPROVAL_POLL_ENABLED`(true) · gate off 시 poll도 skip
- **격리**: `proposal_approval_bg.py` 로직 **불변** · 미인증 chat reply 없음
- **테스트**: `test_proposal_approval_poll_d2.py` **4 passed** · gate 본체 `test_proposal_approval_d2.py` **9 passed** (회귀 없음)
- **Claude OK**: 2026-08-04 — Mirror #2 제안 스펙(배선만) 100% 일치, gate 본체 회귀 없음. 서버 ai_overseer + REPORT_BOT env 기동은 **2026-08-17 기준 미확인**.

### D-3 — [2026-08-04] · D-3a 구현 ✅ · D-3b scaffold-only ✅ / Claude OK ✅ (2026-08-04)

- **D-3a SSOT**: `observability/cost_report_bg.py` — `compute_weekly_cost_report_bg` · `run_cost_report_job`
- **Hook**: `weekly_evolution` — `llm_proposal_summary`(D-1b) **직후**, non-critical
- **ops_events**: `cost_report_weekly` · component `observability.cost`
- **Config**: `COST_REPORT_ENABLED`(true) · `COST_REPORT_WINDOW_DAYS`(7)
- **선독 결과 (basis)**:
  - `llm_gemini_core.LlmResult` — **토큰/USD 필드 없음** → `gemini_cost_estimate_usd=null` · `cost_basis=no_usd_unit_rate`
  - `forward/ledger.py` — **exchange fee SSOT 없음** → `exchange_fee_estimate_usd=null` · `fee_basis=no_fee_rate_ssot`
  - `gemini_call_count` — ops gemini call 이벤트 없음 시 **`llm_call_cache.sqlite` proxy** (`gemini_call_count_source`)
- **D-3b SSOT**: `observability/parity_monitor_bg.py` — `compute_paper_vs_real_parity_bg` **함수만**
- **D-3b 격리**: `weekly_evolution`/cron **미배선** · `PARITY_MONITOR_ENABLED=false`(default dormant)
- **비변경**: `ai_overseer.py` · `execution_safety.py` · `oms_core.py`
- **테스트**: `test_cost_report_d3.py` **6 passed** · `test_parity_monitor_scaffold_d3.py` **6 passed**
- **Claude OK**: 2026-08-04 — cost/fee basis null 근거(SSOT 없음) 확인, 임의 상수 미창조. D-3b 미배선 재확인. `gemini_call_count` proxy(캐시 행수)는 원 스펙 밖 추가 — CAT-M/CAT-J 문서 동기화(비차단). D-3b 실배선 시 CAT-N 읽기 경로(`bitget_real_execution` vs interface) 재검토 조건.
- **잔여**: [x] D-3a Cursor · [x] D-3b scaffold · [x] D-3 Claude OK · [ ] P2-5 후 D-3b 실배선 Go/No-Go

### I-GMM-DNA-01 — [2026-08-12] GMM→CRYPTO_DNA_ALPHA 배선 ✅ / Claude 조건부 OK ✅

- **증상**: paper `forward_trades` 0 · `Cos_eff=0.000` 시계열 게이트 전량 거절
- **원인**: `BITGET_GMM_DNA_TEMPLATES` ≠ `CRYPTO_DNA_ALPHA_RANK*` (엔진 미배선)
- **SSOT**: `bitget/evolution/gmm_dna_alpha_sync.py`
- **배선**: `data_miner` post-mine sync · `config_bootstrap` stale sync · gates sn_score=0 paper 폴백
- **테스트**: `test_gmm_dna_alpha_sync.py` **8 passed**
- **Claude 조건부 OK**: 2026-08-12 — 핵심 배선/랭킹/manual보존 일치. paper 배포 OK.
- **R1 반영**: `data_miner` 주간 sync `force=False` 기본 · `BITGET_GMM_SYNC_FORCE_ON_MINE` opt-in
- **R2 반영**: `_facts_cos_scalar_01` live(`ENABLE_REAL_EXECUTION=true`) fail-closed · paper만 score/100 폴백
- **Mirror**: `shape_source` 태그 (neutral_fallback / prototype_ohlcv 관측)
- **live 전환 플래그**: CAT-F Handoff 예약 — sn_score 폴백 명시 스위치 재검토 (규칙11)
- **롤백**: config_kv `CRYPTO_DNA_ALPHA_RANK1~3` 삭제 시 즉시 복귀
- **배포 (디렉터 확인 2026-08-17)**: I-GMM-DNA-01 **서버 배포 완료**. 로컬 문서의 “git push · 서버 sync 대기”는 **폐기**.
- **잔여**: [x] Cursor · [x] Claude OK · [x] 서버 배포 · [ ] **OPEN/CLOSED COUNT · Cos_eff=0.000 고정 여부 · RANK/shape_source** (`06` paper 관측, 서버만)

### Ops 관측 세션 — [2026-08-17] · 코드 변경 없음 · 문서만 현실 맞춤

- **status**: `POST_DEPLOY_OBS`
- **체크리스트 SSOT**: `track_b_POST_DEPLOY_OBS_체크리스트.md`
- **로컬에서 확인 불가**: 서버 SQLite·journal의 OPEN/Cos/RANK (이 PC에 prod DB 없음)
- **L / overseer 잔여 (했는지·안 했는지 — 디렉터 서버 확인 전 = 미확인)**

| ID | 무엇을 | 코드·Claude | 서버에 설치/기동했는가 |
|----|--------|-------------|------------------------|
| **L-1** | logrotate + journal vacuum | ✅ 구현·OK | **❓ 미확인** — `install_bitget_logrotate.sh` 실행 기록 없음 |
| **L-2** | DB integrity backup + restore drill | ✅ 구현·OK | **❓ 미확인** — `install_bitget_backup.sh` · drill 기록 없음 |
| **D-2 ops** | `ai_overseer` 프로세스 + `REPORT_BOT_*` | ✅ poll 코드·OK | **❓ 미확인** — 기동/env 확인 기록 없음 |

> I-GMM 배포 완료 ≠ L-1/L-2/overseer 설치 완료. 세 항목은 **별도 서버 확인**.

### I-GMM-DNA-01b — [2026-08-17] GMM DNA 주간 관측 리포트 ✅ / Claude OK 대기

- **성격**: 읽기 전용 observability · **gates.py / gmm_dna_alpha_sync.py 미접촉**
- **SSOT**: `bitget/observability/gmm_dna_alpha_report_bg.py`
- **Hook**: `weekly_evolution` — `cost_report` 직후 `gmm_dna_alpha_report` (non-critical)
- **필드**: cos_eff_sample_count / zero_ratio / mean_nonzero · open·closed_count_by_market(B-1 normalize) · dna_rank_keys_present · shape_source_distribution · log_source_used
- **로그**: journalctl 우선 → 파일 → 실패 시 null + `unavailable` (추정 금지)
- **Config**: `GMM_DNA_ALPHA_REPORT_ENABLED`(true) · `WINDOW_DAYS`(7) · `LOG_SOURCE`(journal)
- **ops_events**: `gmm_dna_alpha_report_weekly` · component `observability.dna`
- **테스트**: `test_gmm_dna_alpha_report_i01b.py` **6 passed**
- **Claude OK**: 2026-08-17 — Handoff 스펙 100% 일치 · 수정 spec 없음 · Mirror #2(2주 unavailable→로그 경로) 수용(05 잔여, 선코딩 금지)
- **Mirror 잔여 표기**: 2주 연속 sample_count=0 + unavailable → **서버 로그 경로**(BITGET_LOG_DIR / journal unit) 확인 (코드 재배선 아님)
- **잔여**: [x] Cursor 구현 · [x] Claude Pro OK · [ ] 서버 1~2주 ops 관측 · [ ] L-1/L-2/overseer 병행 확인(별도)

### POST_DEPLOY_OBS 일일 텔레그램 다이제스트 — [2026-08-17] · Cursor 구현 · **대시보드 확장 2026-08-18**

- **SSOT**: `bitget/observability/post_deploy_obs_digest_bg.py`
- **발송**: REPORT_BOT · ①초등학생용 체크리스트 대시보드 ②숫자 메모 ③Cursor/Claude 복붙
- **대시보드 칸**: 🟢잘됨 · 🔴구멍 · 🟡기다림 · ⬜나중(금지) + 진행률 바
- **Cron**: UTC 11:00 = **20:00 KST**
- **테스트**: `test_post_deploy_obs_digest_bg.py` **3 passed**
- **잔여**: [x] Cursor · [ ] 서버 배포 후 메시지 확인 · [ ] Claude 사후 OK(선택)

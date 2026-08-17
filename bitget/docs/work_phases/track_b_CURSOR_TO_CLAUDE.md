# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-17  
> **유형**: **POST_DEPLOY_OBS 일일 텔레그램 다이제스트** 구현 (디렉터 요청) · Claude 사후 검증 요청

---

## OUTBOX — 2026-08-17 · POST_DEPLOY_OBS daily digest

### 왜
디렉터: 1~2주 관측 항목을 매일 텔레그램으로 받고, Cursor/Claude 복붙 문구 포함.

### 로컬 스냅샷
| 항목 | 내용 |
|------|------|
| 신규 | `observability/post_deploy_obs_digest_bg.py` |
| CLI | `bitget.sh --post-deploy-obs-digest` (락 무접촉) |
| Cron | UTC 11:00 = KST 20:00 daily |
| 전송 | REPORT_BOT direct HTTP (north-star와 동일) |
| 비접촉 | gates.py · gmm_dna_alpha_sync.py |
| 테스트 | `test_post_deploy_obs_digest_bg.py` **3 passed** |

### Ask
- 디렉터 요청 범위로 사후 OK 가능한지 (정식 Handoff 없이 디렉터 지시 구현)
- 복붙 블록 길이·REPORT_BOT 분할 발송 수용 여부

### 금지 준수
C-2 · MDD5% · live · 실전 — 미착수

---

## Claude OK — I-GMM-DNA-01b (2026-08-17)

- 판정: **OK** — Handoff 100% 일치 · 수정 spec 없음 · Adapter 불필요
- Mirror #2 수용: 2주 unavailable → 서버 로그 경로만 (05 잔여 · 선코딩 금지)
- 다음: **디렉터 서버 확인** (POST_DEPLOY_OBS · L-1/L-2/overseer · 01b 1~2주) · C-2/MDD5%/live defer

---

## OUTBOX — 2026-08-17 · I-GMM-DNA-01b 구현

### 로컬 구조 스냅샷
| 항목 | 내용 |
|------|------|
| 신규 | `bitget/observability/gmm_dna_alpha_report_bg.py` |
| Hook | `bitget_pipelines._pipeline_weekly_evolution` — `cost_report` 직후 `gmm_dna_alpha_report` (critical=False) |
| Config | `memory_policy`: `GMM_DNA_ALPHA_REPORT_ENABLED=true` · `WINDOW_DAYS=7` · `LOG_SOURCE=journal` |
| 비접촉 | `forward/gates.py` · `evolution/gmm_dna_alpha_sync.py` — **미수정** |
| 테스트 | `pytest bitget/tests/test_gmm_dna_alpha_report_i01b.py` → **6 passed** |

### 산출 필드
- cos_eff_sample_count / zero_ratio / mean_nonzero(nullable)
- open_count_by_market · closed_count_by_market (B-1 `normalize_market_key`)
- dna_rank_keys_present · shape_source_distribution · log_source_used
- 로그 실패 시 sample null + `unavailable` (추정 금지)

### Ask
- Handoff 스펙 일치 OK 여부
- Mirror #2: 2주 unavailable 시 서버 로그 경로 확인을 05 잔여로 둔 것 수용 여부

### 금지 준수
C-2 · MDD 5% · B-2 live · `ENABLE_REAL_EXECUTION` — 미착수

---

## OUTBOX — 2026-08-17 · POST_DEPLOY_OBS (코드 diff 없음)

**디렉터 확인:** I-GMM-DNA-01 포함 Bitget **서버 배포 완료**.  
로컬 `NEXT_ACTION` 등이 “git push + 배포 대기”로 남아 있어 **문서만** 현실에 맞춤. 알파/실전/C-2/MDD5%/live **미착수**.

### 로컬에서 확인 가능한 것
- 코드·테스트·Claude 조건부 OK · R1/R2 반영 이력 (`05` I-GMM)
- 배포 후 **무엇을** 보면 되는지 한 장: `track_b_POST_DEPLOY_OBS_체크리스트.md`
- L-1 / L-2 / ai_overseer+REPORT_BOT = **코드 OK**, 서버 설치·기동 **기록 없음** → 표기 = 미확인

### 서버에서만 확인 가능한 것 (이 세션에서 숫자 없음)
| 항목 | 왜 로컬 불가 |
|------|----------------|
| `bitget_forward_trades` OPEN/CLOSED COUNT | prod SQLite는 `BITGET_DB_STORAGE_PATH` |
| `Cos_eff=0.000` 고정 여부 | journal / BITGET_LOG_DIR |
| `CRYPTO_DNA_ALPHA_RANK*` · `shape_source` | config_kv prod |
| `gmm_dna_alpha_sync --force` 가 **이미** 돌았는지 | RANK 키 존재 여부가 증거. 채팅만으로는 모름 |

### 다음 Handoff 후보 **1개만**
- **I-GMM-DNA-01b** — Cos_eff / OPEN count / `shape_source` **읽기 전용** 관측 미니잡 (ops 로그·주간 숫자). gate/DNA 재배선 아님.
- **하지 말 것**: C-2 funding · 포트폴리오 MDD 5% · B-2 live alloc · `ENABLE_REAL_EXECUTION=true`

**Ask:** 01b Handoff를 쓸지, 아니면 디렉터 48h 관측 숫자 받은 뒤에만 쓸지.

---

## I-GMM-DNA-01 — Claude 조건부 OK (2026-08-12)

**판정:** 조건부 OK → **R1/R2 코드 반영 완료** (8 passed)

| 조건 | Claude 지적 | Cursor 반영 |
|------|-------------|-------------|
| R1 | data_miner `force=True` → manual 덮어쓰기 | `force=False` 기본 · `BITGET_GMM_SYNC_FORCE_ON_MINE` opt-in |
| R2 | score/100 폴백 live 공용 | `ENABLE_REAL_EXECUTION=true` 시 fail-closed (Cos=0) |
| Mirror | shape_source 관측 | `dna["shape_source"]` 태그 추가 |

**paper 배포:** 즉시 진행 가능  
**live 전환 전:** CAT-F Handoff에 폴백 스위치/fail-closed 재확인 예약

---

## I-GMM-DNA-01 — GMM→CRYPTO_DNA_ALPHA 배선 (2026-08-12)

### 증상 (서버)
- `forward_trades` 0건 · 텔레그램 스캔 ~1000건/일
- 로그: `Cos_eff=0.000 < elastic 0.588` (시계열 게이트 100% 거절)
- config: `BITGET_GMM_DNA_TEMPLATES` 있음 · `CRYPTO_DNA_ALPHA_RANK*` 없음

### 근본 원인
- `signal_engines._doppelganger_adjustment` → `CRYPTO_DNA_ALPHA_RANK1..3` (+ shape 20) 만 읽음
- `data_miner` → `BITGET_GMM_DNA_TEMPLATES` 만 채움 (**키 불일치**)
- `sn_score=0` 이 facts에 고정 → `_facts_cos_scalar_01` 이 signal score 폴백 불가

### 구현
| 파일 | 변경 |
|------|------|
| `evolution/gmm_dna_alpha_sync.py` | **신규** — sync SSOT |
| `data_miner.py` | prototype shape + post-mine sync |
| `pipelines/bitget_pipelines.py` | config_bootstrap 훅 |
| `forward/gates.py` | sn_score≈0 시 score/100 폴백 |

### 테스트
`pytest bitget/tests/test_gmm_dna_alpha_sync.py` → **6 passed**

### 서버 배포 후 1회
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
sudo INSTALL_ROOT=$PWD bash bitget/deploy/update_bitget.sh
.venv/bin/python -m bitget.evolution.gmm_dna_alpha_sync --force
sqlite3 /var/lib/quant-bitget/data/bitget_system_config.sqlite \
  "SELECT key FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA%';"
```

### Claude OK Ask
- neutral shape + bounds midpoint DNA가 paper bootstrap에 충분한지
- sn_score=0 폴백 허용 범위 (🟡 리스크 게이트)

---

## D-3 — Claude OK 수신 (2026-08-04)

- cost/fee basis null — SSOT 없음 확인 수용
- `gemini_call_count` llm_call_cache proxy — 수용 (CAT-M/CAT-J 동기화 완료)
- D-3b dormant · pipeline 미배선 재확인
- D-3b 실배선 시 `bitget_real_execution` vs CAT-N interface — P2-5 Handoff 체크 항목 예약

# Architect Mirror — Claude 능동 의견 로그 (Bitget)

> **역할**: Cursor가 **로컬에서 바꾼 구조**를 Claude가 “보는 것처럼” 대입해, OK/거절만이 아니라 **추가 설계·우선순위·리스크 의견**을 남기는 SSOT.  
> **갱신**: Claude Pro — **매 검증·Handoff·갭 리뷰 세션** 종료 시 본 파일 **상단에** 날짜 블록 추가 (이전 블록은 보존).  
> **Cursor**: 구현 전·후 `CURSOR_TO_CLAUDE.md`의 「로컬 구조 스냅샷」을 최신으로 맞춤.

---

## Mirror — 2026-08-30 · [CAT-D] · FULL-BT-FUT-DEFCON-1 SUB_DONE

### 판정
- Claude OK · **SUB_DONE** · 6항목 충족 (bypass=3, prod=0, FUT≤3, staging, 1회, 단정문구 없음)
- Adapter A 의도대로 동작 (step2 bottleneck=0 · bypass=3 · prod 유입 0)

### 지시
- VPS `FULLBT_DEFCON_BYPASS_ENABLED=false` **즉시** (코드 default 이미 False)
- `_load_bench_close` 노이즈 = 비차단 · 별건 CAT
- FULL-BT = IV L1 참고만 · LIVE/R6/생존 단정 금지

### 다음
- 이 레인 추가 구현 없음 · 새 Handoff 후만

---

## Mirror — 2026-08-29 · [CAT-D] · FULL-BT-FUT-DEFCON-1 조건부 OK

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영: 본체 비수정 · 3중 AND · default false · IV L1 로깅 · FUT=`market_type`만 · Adapter A(절차 적법) · 40 passed
- 편차(승인): STEP1 리터럴 wrapper → STEP0 공유 발견 후 harness ExitStack Adapter A (규칙6)
- **조건부**: ExitStack이 라이브 데몬 모듈 전역을 건드리지 않는지 스냅샷 필요 → Cursor 회신 대기 후 최종 OK · VPS bypass=true **보류**

### Cursor 격리 스냅샷 (회신 반영)
- `patch.object(doomsday_gate, "doomsday_long_entry_blocked")` = **`run_replay` ExitStack `with` 한정** · exit 시 원본 복원
- 라이브 데몬 = **별도 OS 프로세스** → 해당 ExitStack 미진입 · 디스크 `doomsday_gate.py` 불변
- 3중 단독실패 테스트: `test_kill_switch_off_blocks_bypass` · `test_not_isolated_blocks_bypass` · `test_wrong_db_blocks_bypass` (+ wrap/kill variants)

### 다음
1. ~~Claude 최종 OK~~ → **Claude OK: 2026-08-30 (최종)**
2. VPS `FULLBT_DEFCON_BYPASS_ENABLED=true` · FUT≤3 staging 재파일럿 (**WAIT_CURSOR_VPS**)
3. 결과 OUTBOX · 전체런·LIVE/R6 단정 금지 유지

---

## Mirror — 2026-08-21 · [CAT-J] · NS-BG-DASH-01 OK

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: MDD5%/연12~25%(B0=측정) SSOT 값 일치 · 원장 read-only · SPOT/FUT 분리 · gates/sync/forward_trades 비접촉 · 경로 격리(`bitget/observability/`)
- 빠졌거나 불일치: 없음

### 추가 제안 (지금 착수 금지)
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | 디렉터 20:00 텔레그램 육안 1회 확인 | ① 디렉터 | 최고 |
| 2 | DATA_WAIT streak 카운터 (별도 Ask 후) | 🟡 2 | 중 |

### 다음 Handoff 후보 재정렬
1. (설계 아님) 디렉터 서버 육안 확인
2. C-2 / MDD5% / live — 금지 유지, 이번 트랙 대상 아님

---

## Mirror — 2026-08-20 · [CAT-J] · POST_DEPLOY_OBS-DNA-UX-01 조건부 OK

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: 6-state 순서 · Spec2/3/5 · kill-switch · gates/sync/MFE 기본값 비접촉 · 10 passed · 실측→DATA_WAIT_LOW_MFE 정합
- OUTBOX 요약에 `DB_PATH`로 축약 표기했으나 **코드 enum 정식명 = `DB_PATH_OR_ENV`** (스펙 1:1)
- 숫자 메모 키: `n_closed_by_tf` · `n_mfe8_by_tf` · `gmm_cluster_n` · `last_error` (요청 4필드 + `_by_tf` 접미사)

### 추가 제안 (1~3개, Handoff 범위 밖 · 지금 착수 금지)
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | DATA_WAIT 연속 일수 카운터(며칠째 재료대기인지) → 관측 vs 구조 문제 판단 | 🟡 2 | 중 (별도 Handoff) |
| 2 | 01b 주간 report ↔ 일일 digest 공통 계산 헬퍼 통합 (정합성) | 🟡 2 | 낮음 (비차단) |
| 3 | 서버 pull 후 텔레그램 DNA「재료 덜 모였어요」👁️ 1회 | ① 디렉터 | 최고 |

### Cursor와 다른 의견 (있으면)
- 없음. 조건부 OK = 요약 표기 확인만 Mirror에 닫음.

### 다음 Handoff 후보 재정렬
1. **(설계 아님)** 디렉터 배포·텔레그램 육안 확인
2. DATA_WAIT streak 카운터 (별도 Ask 후)
3. C-2 / MDD5% / live — 금지 유지

---

## Mirror — 2026-08-17 · [CAT-I] · I-GMM-DNA-01b 구현 검증

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: SSOT 신규 파일·Hook 위치(cost_report 직후)·gates/sync 비접촉·필드 6종·로그 3단 폴백(journal→file→null+unavailable)·금지 항목(C-2/MDD5%/live) 미착수 — Handoff 스펙 100% 일치, 편차 없음
- 빠졌거나 불일치: 없음

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | 신규 설계 Handoff 없음 — 다음은 디렉터 서버 확인(L-1/L-2/overseer/OPEN·Cos·RANK 실측)이 선행 | ① 디렉터 | 최고 |
| 2 | 2주 연속 cos_eff_sample_count=0+unavailable 시에만 서버 로그 경로 조사 (조건부, 코드 아님) | 🟡 2 | 중간 (조건부) |
| 3 | shape_source=neutral_fallback 지속 관측 시 CAT-F live 폴백 Handoff 근거자료로 사용 예약 | 🟡 2 | 중간 |

### Cursor와 다른 의견 (있으면)
- 없음.

### 다음 Handoff 후보 재정렬
1. **(설계 아님) 디렉터 서버 확인** — POST_DEPLOY_OBS·L-1/L-2/overseer·01b 1~2주 관측치
2. C-2 / MDD 5% / B-2 live — **defer, 이번 트랙 대상 아님**

### 디렉터 한 줄
> 01b OK. 이제 새로 설계할 것 없음 — 서버에서 숫자가 쌓이는지만 1~2주 보면 됨. C-2·MDD5%·live는 그대로 손 안 댐.

---

## Mirror — 2026-08-17 · [CAT-I] · I-GMM-DNA-01b Handoff 발행

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 이번 세션은 구현 검증이 아닌 설계 Handoff 발행 — Cursor 로컬 스냅샷 없음(구현 대기)
- OUTBOX(POST_DEPLOY_OBS)는 05/00/NEXT_ACTION/NEXT_STEP과 100% 일치 확인

### 추가 제안
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | L-1/L-2/ai_overseer 서버 확인은 이 Handoff와 독립 · 병행 | ① 디렉터 | 높음 |
| 2 | 2주 연속 cos_eff_sample_count=0 + unavailable → 서버 로그 경로 확인 | 🟡 2 | 중간 |
| 3 | shape_source=neutral_fallback 지속 시 CAT-F live 폴백 Handoff 근거 데이터 | 🟡 2 | 중간 |

### 다음 Handoff 후보 재정렬
1. **I-GMM-DNA-01b** (본 Handoff) — 구현
2. (설계 아님) L-1/L-2/overseer 서버 확인 병행
3. C-2 / MDD 5% / B-2 live — defer

### 디렉터 한 줄
> OUTBOX 확인 끝. 다음 하나는 gates 안 건드리고 로그·장부만 읽는 주간 리포트. C-2·MDD5%·live는 손 안 댐.

---

## Mirror — 2026-08-12 · [CAT-I] · I-GMM-DNA-01 GMM→CRYPTO_DNA_ALPHA 배선

### 로컬 구조 이해
- **맞음**: GMM bounds midpoint → `CRYPTO_DNA_ALPHA_RANK1~3` · `mean_mfe` 랭킹 · `source=manual` 보존 · prototype OHLCV shape 우선
- **조건부 OK**: R1 weekly `force=True` · R2 paper/live 폴백 미분리 → **Cursor R1/R2 반영 완료**

### 추가 제안 (Claude 원문)
| # | 제안 | 상태 |
|---|------|------|
| 1 | data_miner sync `force=False` + opt-in | ✅ R1 반영 |
| 2 | live 전환 Handoff에 score-fallback fail-closed | 📋 CAT-F 예약 |
| 3 | `shape_source` 태그로 neutral 비중 관측 | ✅ 반영 |

### 디렉터 한 줄
> paper OPEN 0 원인은 DNA 키 불일치였음. 배선+조건 반영 후 **배포·sync·48h 로그**만 보면 됨. live 켤 때는 CAT-F Handoff 먼저.

---

## Mirror — 2026-08-04 · [CAT-J] · D-3a/D-3b Cost & Parity Monitor 구현 검증

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: `compute_weekly_cost_report_bg`·`run_cost_report_job`·weekly_evolution hook(llm_proposal_summary 직후)·`COST_REPORT_ENABLED`/`WINDOW_DAYS`·cost/fee basis 둘 다 SSOT 없어 null+플래그(`no_usd_unit_rate`/`no_fee_rate_ssot`) 확인·임의 상수 미창조·D-3b `compute_paper_vs_real_parity_bg` 함수만 존재·hook 미배선·`PARITY_MONITOR_ENABLED` default false·`ai_overseer.py`/`execution_safety.py`/`oms_core.py` 비변경 — Handoff 스펙 거의 100% 일치
- 빠졌거나 불일치: (Cursor 추가) `gemini_call_count`를 위한 전용 ops_events가 없어 `llm_call_cache.sqlite` 행 수를 proxy로 사용(`gemini_call_count_source` 플래그) — 원 Handoff에 방법 미지정이었으나 null-basis 정신과 부합해 수용. (경미한 편차) D-3b 읽기 대상이 스펙 문구(`oms_core`/`order_snapshot` 인터페이스)와 달리 `bitget_real_execution` 테이블 직접 참조 — 현재 dormant라 무해하나 P2-5 실배선 시 재확인 필요

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | D-3b 실배선 전 CAT-N 읽기 경로 재검토 조건부 명시 — `bitget_real_execution` 직접 read vs `oms_core`/`order_snapshot` 인터페이스 경유, P2-5 Go/No-Go Handoff에 필수 체크 항목으로 포함 | 🔴 3 (P2-5 후) | 중간 | 지금은 dormant라 무해, 나중에 활성화 시점에 스펙과의 편차를 다시 안 밟도록 미리 기록 |
| 2 | D 트랙(D-1~D-3) 전체 완료 — 다음은 신규 설계보다 **서버 운영 확인 3종 취합**: ai_overseer 기동, REPORT_BOT env, D-3a 실측 관측(1~2주) | ① 디렉터 | 최고 | 설계·구현 사이클은 끝났고 "코드 존재 ≠ 운영 작동"이 D-2 poll부터 반복된 패턴 — 이번엔 D-3도 같은 확인이 필요 |
| 3 | `gemini_call_count_source` proxy 방식을 CAT-M.md/CAT-J.md 문서에 동기화 — 다음 세션에서 "왜 이 숫자가 실제 API 호출이 아니라 캐시 행 수냐"는 혼동 방지 | 🟢 2(문서) | 낮음 | D-1b/C-1b의 null-분모 구분 사례와 동일 패턴 반복 |

### Cursor와 다른 의견 (있으면)
- 없음. proxy 방식과 basis null 처리 모두 스펙 취지(근거 없는 숫자 창조 금지) 부합으로 수용.

### 다음 Handoff 후보 재정렬
1. **(설계 아님) 디렉터 서버 확인 일괄** — ai_overseer 기동·REPORT_BOT env·D-3a 관측 누적 — D 트랙 신규 설계 없음, 운영 확인만
2. **(참고) P2-5 이후** — D-3b 실배선 Go/No-Go (CAT-N 읽기 경로 재검토 포함)
3. Layer 2 C-track (P0-6/P1-7) — D와 무관하게 병렬 진행 가능

### 디렉터 한 줄
> D-3 OK로 묶음D(D-1~D-3) 설계·구현·검증 사이클 완료. 비용·수수료는 근거 없어서 일부러 비워둠(null), parity 함수는 만들어만 두고 안 켬. 다음은 새 설계보다 **서버에서 다 잘 돌고 있는지 확인**하는 단계.

---

## Mirror — 2026-08-04 · [CAT-M] · D-2 poll 배선 구현 검증

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: `proposal_approval_poll_bg.py`(`poll_proposal_approval_updates_once`)·hook `ai_overseer.overseer_loop`(30s tick, getUpdates timeout=0)·`telegram_env.get_report_token()`(REPORT_BOT_* 재사용)·`proposal_approval_poll_state.json`(last_update_id)·kill-switch `AI_PROPOSAL_APPROVAL_POLL_ENABLED`·`proposal_approval_bg.py` 로직 완전 불변·미인증 chat 무응답 — 이전 Mirror 제안("getUpdates 루프만") 스펙 100% 일치
- 빠졌거나 불일치: 없음. D-2 gate 본체 테스트(`test_proposal_approval_d2.py` 9 passed)도 함께 재실행돼 회귀 없음 확인됨

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | 서버 실배선 체크 — `ai_overseer` 프로세스 기동 + `REPORT_BOT_TOKEN`/`REPORT_BOT_CHAT_ID` env 확인 | ① 디렉터 | 최고 | 코드 OK ≠ 운영 ON — poll이 30초마다 도는지는 서버에서만 확인 가능 |
| 2 | D-3 cost + parity monitor 설계 Handoff 착수 | 🟡 2 | 높음 | D 트랙 실행 인프라(gate+poll) 완성 — P2-5 전 설계 선행 가능 (`04_묶음D` 스펙) |
| 3 | `00_전체현황판.md` 체크리스트 "D-2 human gate ... poll 배선 대기" 항목 "완료"로 갱신 | 🟢 2(문서) | 낮음 | poll Claude OK로 해당 항목 잔여 사유 해소 |

### Cursor와 다른 의견 (있으면)
- 없음.

### 다음 Handoff 후보 재정렬
1. **(설계 아님) 디렉터 서버 배포 확인** — ai_overseer + REPORT_BOT env, 이게 없으면 poll 코드는 존재해도 미작동
2. **D-3** cost + parity monitor 설계 Handoff — P2-5 전 선행 가능
3. (참고) C 트랙 P0-6/P1-7 — Layer 2 병렬

### 디렉터 한 줄
> D-2 poll 배선 OK, gate 로직 회귀 없음. **서버에서 ai_overseer가 REPORT_BOT env로 실제로 도는지**만 확인하면 진짜 작동 시작. 다음은 D-3 설계.

---

## Mirror — 2026-08-04 · [CAT-M] · D-2 Human Approval Gate 구현 검증

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: `record_approval_decision`·`apply_approved_proposal`·telegram `/proposal_approve`·`/proposal_reject`·config_kv는 approve 후에만 `set_config_value`(reject/미인증/duplicate 미접촉)·status 파생값·`REPORT_BOT_*` 화이트리스트 재사용·critical/high full id only(prefix 거부)·A-5 부분성공(키별 applied/rejected)·D-1/D-1b SSOT(schema·코드) 완전 불변·append-only 테이블 강제 — Handoff 스펙 100% 일치, 이번엔 편차 없음
- 빠졌거나 불일치: 없음. (참고: D-1/D-1b는 매번 Cursor가 엣지케이스 1개씩 자체 보완했는데 D-2는 스펙 그대로 — Handoff 명세가 A-5/telegram 경계까지 충분히 구체적이었던 결과로 판단, 다음 Handoff 작성 시 이 정도 구체성 유지)

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | **디렉터 Critical 승인 기입** — `05_진행로그.md` D-2 섹션에 A-1~A-5와 동일 형식(`디렉터 승인: YYYY-MM-DD`) 기록 후에만 REPORT_BOT getUpdates poll 실배선 진행 | 🔴 승인 게이트 | 최고 | approve→config_kv 실쓰기 경로 최초 개방 — 코드 안전성과 별개로 "언제 실제로 켜질지"는 디렉터 결정 사항 |
| 2 | REPORT_BOT getUpdates poll 배선 — Cursor가 이미 "Handoff 범위 밖이면 별도"로 명시. 승인 후 **별도 미니 Handoff**(비-설계, 배선만)로 분리 권장, D-2 Critical 승인과 poll 배선 승인을 한 번에 묶지 말 것 | 🟡 2 | 높음 | 코드 존재 ≠ 운영 연결. 두 단계를 분리해야 "구현했는데 왜 안 켜지냐" 혼동 방지 |
| 3 | D-3(cost/parity) 설계는 지금 선행 가능하나, 실행은 P2-5 실전 전환 후 — D-2 poll 배선과 D-3 설계는 서로 무관하게 병렬 착수 가능함을 `00_전체현황판` Layer 표에 반영 | 🟢 2(문서) | 낮음 | 다음 세션에서 D-2 승인 대기 중 D-3를 막연히 미루지 않도록 |

### Cursor와 다른 의견 (있으면)
- 없음. 구현 스펙 100% 일치, 편차 없음.

### 다음 Handoff 후보 재정렬
1. **(설계 아님) 디렉터 Critical 승인** — D-2 `05` 기입, 이게 없으면 다음 어떤 D-트랙도 "실전 연결"로 넘어갈 수 없음
2. **D-2 poll 배선 미니 Handoff** (승인 후 별도, 코드 최소) — telegram getUpdates 루프만
3. **D-3** cost + parity monitor 설계 — P2-5 전 선행 가능, poll 배선과 병렬

### 디렉터 한 줄
> D-2 구현 OK, 코드 안전 확인 끝. 이제 A-1~A-5처럼 **디렉터가 직접 승인 날짜를 `05`에 적어야** 다음 단계(실제 텔레그램 연결)로 감. 승인 전엔 대기.

---

## Mirror — 2026-08-04 · [CAT-M] · D-1b LLM Proposal 주간 관측 구현 검증

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: `compute_llm_proposal_summary_bg`·`run_llm_proposal_summary_job`·weekly_evolution hook(bad_tick_skip_summary 직후, critical=False)·config_kv/ai_proposal_schema_bg 미접촉·ops_events 1건만 기록 — Handoff 스펙 100% 일치
- 빠졌거나 불일치: (Cursor 추가) parse_error_rate_pct 빈 window(0/0) → null 처리는 원 Handoff에 없던 엣지케이스. "분모 항상 존재" 전제가 0건 window를 놓침 — Cursor 보완 타당, 수용

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | D-2 Handoff — `bitget_llm_proposals` pending 상태 필드(status: pending/approved/rejected) 설계 + telegram approve command 포맷 확정 | 🟡 2 | 높음 | D-1/D-1b 완료로 관측 데이터 확보됨. D-1b 주간 분포로 실제 volume 파악 후 승인 UX 설계 — 추측 설계 방지 |
| 2 | CAT-M.md §4/§2 문서 동기화 — D-1b `parse_error_rate_pct` 빈 window null 케이스 명시, C-1b `skip_rate_pct` null과 원인 구분 주석 | 🟢 2(문서) | 중간 | 두 null이 "분모 없음" vs "분모=0" 서로 다른 원인 — 다음 세션 혼동 방지 |
| 3 | D-1b 요약값을 CAT-J weekly report에 노출할지 여부 — 이번 스코프 아니었으나 디렉터가 09 요약에서 "proposal 몇 번 왔는지" 텍스트로 보고 싶어할 가능성 | 🟢 3(디렉터 확인) | 낮음 | ops_events에만 있으면 비개발자가 직접 못 봄 — 09 갱신 시 디렉터 의향 확인 필요 |

### Cursor와 다른 의견 (있으면)
- 없음. null 엣지케이스 보완은 스펙 취지(정확한 집계) 부합으로 수용.

### 다음 Handoff 후보 재정렬
1. **D-2** human approval gate (telegram approve UX + pending 상태 스키마) — D-1b 관측 데이터 기반으로 착수 가능
2. **D-3** cost + parity monitor — P2-5 실전 전환 후 효과 검증 heavy, 설계만 선행 가능
3. (참고) C 트랙 P0-6/P1-7 — Layer 2 병렬, D 트랙과 무관하게 진행 중

### 디렉터 한 줄
> D-1b OK. 주간 proposal count/risk_class 분포 + parse error rate 확보 끝, config_kv 안 건드림 재확인. 다음은 D-2 — telegram 승인 게이트 Handoff.

---

## Mirror — 2026-08-04 · [CAT-M] · D-1 Structured JSON Proposal 구현 검증

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: `validate_llm_proposal`·`persist_proposal_bg`·hook 1곳·config_kv 미접촉·risk_class 서버 재계산(CAT-MAP §6)·parse fail 처리·weekly_action_plan.py 비변경+auto-apply 없음 — Handoff 스펙 100% 일치
- 빠졌거나 불일치: (Cursor 추가) "JSON block 자체 없음 → silent skip"은 원 Handoff에 없던 신규 분기(parse fail과 구분). config_kv 무접촉·스팸 방지라는 스펙 취지에는 부합 — 수용하되 CAT-M.md/HANDOFF 템플릿 미문서화 상태

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | D-1 proposal 발생 빈도 관측 미니잡 — `bitget_llm_proposals` weekly count/risk_class 분포 (read-only, C-1b 패턴 재사용) | 🟡 2 | 높음 | Cursor Mirror 요청("D-2 telegram approve UX와 pending 상태 스키마 선행 검토")에 대한 답 — 실제 빈도/분포 없이 D-2 UX 설계하면 추측 설계 됨 |
| 2 | D-2 Handoff 전 `bitget_llm_proposals` pending 상태 필드 설계 초안(status: pending/approved/rejected) — 코드 변경 아닌 스펙 논의만 | 🟡 2(설계) | 중간 | D-2가 다음 Handoff이므로 스키마 마이그레이션 왕복 줄이기 |
| 3 | CAT-M.md §2/§3 문서 동기화 — "no JSON block silent skip" 분기 명시, CAT-HANDOFF 템플릿에도 parse fail과 구분 주석 | 🟢 2(문서) | 낮음 | 다음 세션 혼동 방지 (C-1 blackhole/underdog 사례와 동일 패턴) |

### Cursor와 다른 의견 (있으면)
- 없음. silent-skip 분기는 스펙 취지 부합으로 수용.

### 다음 Handoff 후보 재정렬
1. **D-1 관측 미니잡** (read-only, 코드 최소) — D-2 설계 재료 확보, 선행
2. **D-2** human approval gate (telegram approve UX + pending 상태 스키마) — Layer 2
3. **D-3** cost + parity monitor — `04_묶음D` 스펙대로 P2-5 실전 전환 **후** 효과 검증 heavy, 설계만 선행 가능

### 디렉터 한 줄
> D-1 OK. config_kv 안 건드림, weekly_action_plan 자동적용 없음 확인 끝. 다음은 바로 D-2 가지 말고 관측 미니잡 한 번 → D-2, D-3는 뒤로.

---

## Mirror — 2026-08-04 · [CAT-C] · C-1 Bad Tick Filter 구현 검증

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: evaluate_bad_tick 시그니처·threshold 5키·candidate 조립 직전 호출·gate 순서 불변·ops_events 기록 — Handoff 스펙 100% 일치
- 빠졌거나 불일치: (Claude 원인) 원 Handoff가 blackhole/underdog까지 호출부 요구했으나 실제 candidate 경로 없음 — Cursor 판단 수용, 원안 정정. 또한 원 Handoff "인접 CAT 영향"에서 CAT-N `price_sanity_gate`(try_add 시점 median/gap 필터) 교차확인 누락 — 이번에 처음 인지

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|
| 1 | C-1 ops 관측 미니잡 — `bad_tick_filtered` 주간 skip률 집계(read-only, NS-1 패턴 재사용) | 🟡 2 | 높음 | `05` C-1 잔여 "`06` 효과 ops skip률 <0.5% 목표" — 수동 확인 방법 없음 |
| 2 | CAT-C_스크리닝.md §1/§6 문서 동기화 — blackhole/underdog candidate-없음 주석, bad tick resolved | 🟡 2 | 중간 | 다음 세션 혼동 재발 방지 |
| 3 | CAT-CONSTANTS 스캔·시그널 표 BAD_TICK_* + CAT-N price_sanity 상호 참조 | 🟢 2(문서) | 낮음 | SSOT 드리프트 방지 |

### Cursor와 다른 의견 (있으면)
- **C-1 vs price_sanity 이중 필터 — 분리 유지 권장, 통합 반대**
  - C-1(scanner pre-candidate) = 신호 품질 — 손상 캔들이 스코어링·candidate 생성 낭비 조기 차단
  - price_sanity(try_add) = 주문 직전 안전망 — API 지연/fat-finger 등 실행 시점 이상치 (CAT-N SSOT)
  - defense-in-depth. 통합 시 CAT-N·execution_safety 범위 번짐 + sub-phase 1개 원칙 충돌
  - threshold 정렬은 **비차단 후속** — 값 1회 대조 확인만 (코드 변경 아님)

### 다음 Handoff 후보 재정렬
1. **C-1 ops 관측 미니 Handoff** (read-only, 코드 최소) — `06` 판정 재료, D-1보다 선행
2. **D-1** (AI proposal / human gate) — Layer 2 다음
3. **P1-7** (watchdog WS 확장) — P0-6 해소·긴급도 낮음, 뒤로

### 디렉터 한 줄
> C-1 OK. price_sanity와는 분리 유지 — 통합 아님. 다음은 관측 미니잡 → D-1, P1-7은 뒤로.

---

| Cursor | Claude (기존) | Mirror 추가 |
|--------|---------------|-------------|
| 코드·테스트·문서를 **실시간** 갱신 | Knowledge + OUTBOX로 **간접** 인지 | 스냅샷 + **능동 제안**으로 시너지 |
| “구현했습니다” | “OK / 수정 spec” | “OK + **이 구조면 다음은 X, 중복은 Y, 리스크는 Z**” |

**금지**: Mirror가 Handoff 없이 🔴 live 변경을 지시하는 것. 제안은 Layer·우선순위만.

---

## Claude가 쓸 블록 형식 (매 세션 상단에 추가)

```markdown
## Mirror — YYYY-MM-DD · [CAT-X] · {sub-phase 또는 주제}

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점:
- 빠졌거나 불일치:

### 추가 제안 (1~3개, Handoff 범위 밖 가능)
| # | 제안 | Layer | 우선순위 | 근거 |
|---|------|-------|----------|------|

### Cursor와 다른 의견 (있으면)
- 

### 다음 Handoff 후보 재정렬
1. 
2. 

### 디렉터 한 줄
> 
```

---

## Cursor가 OUTBOX에 넣을 스냅샷 (매 구현 세션)

`CURSOR_TO_CLAUDE.md` §「로컬 구조 스냅샷」 — Claude가 코드를 못 봐도 **구조 지도**가 되게:

- 이번 세션 **변경 파일·함수·config 키**
- **기존 모듈과 겹침/중복** (예: C-1 vs `price_sanity_gate`)
- **알려진 부채** 한 줄 (테스트·서버·defer 항목)
- **Attribution** (🟢/🟡/🔴) 이번 변경

---

## 디렉터 사용법

1. Cursor 구현 후 → `CURSOR_TO_CLAUDE` 복붙  
2. Claude 답변에 **OK + Mirror 블록** 요구  
3. `ARCHITECT_MIRROR` 상단 갱신 확인 → `09`·`NEXT_STEP`에 “Claude가 제안한 다음” 반영 여부 결정

---

## Mirror 이력

_(이전 시드 — 2026-08-04 Cursor · Claude 작성 전 맥락 메모)_

- C-1 vs `price_sanity_gate` 이중 필터 — Mirror 본문에서 분리 유지로 확정

---

## 왜 필요한가

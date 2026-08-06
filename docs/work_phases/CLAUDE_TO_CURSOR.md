# CLAUDE → CURSOR (Handoff INBOX)

> **작성**: Claude Pro **만**  
> **현재**: **RP-1 + C-1 병합** — Cursor 착수 ✅

---

## [CAT-C] RP-1 + C-1 병합 — 15구간 레짐패널 baseline (RP-1) → 조건부 섹터부스트 A/B (C-1)

### SSOT (변경 금지 unless noted)
- 파일: `time_machine_backtester.py`(REGIME_PERIODS 확장, run_time_machine_regime_matrix 재사용)
- 참조(읽기전용): `performance_budget_governor.py`(tier 임계값), `meta_governor.py`(ACTION_BY_REGIME), `sector_rotation_store.py`(C-1)

### Stage 1 — RP-1 baseline (필수, 먼저)
- `REGIME_PERIODS` 6→15구간 확장 — 위 15구간 표 그대로. DB 미가용 구간은 백업 리스트로 즉시 치환(순연 금지, 치환 로그 남길 것)
- 시뮬 스택: S1(supernova) + S4(선택, timebox 되면) + S5(태그만) + **Phase A tier overlay**
- **Phase A overlay 스펙**: 라이브 모듈 풀 연동 아님. 백테스트 equity curve의 peak-to-trough 소진율을 계산해 `performance_budget_governor` tier 임계값(40/70/90%)과 동일 기준으로 `KELLY_THROTTLE_MULT`/`POSITION_QUOTA_MULT` **동일 로직으로 replay**만. config_kv 실제 write 없음.
- 시뮬레이션 단위: **KR+US 합산 포트폴리오** (개별 시장 분리 아님)
- 출력: `reports/regime_panel/rp1_{date}.json` — 구간별 CAGR/MDD/PF/n/진입0여부/tier소진로그
- **Lookahead**: v1(오늘 뇌 템플릿) 그대로, 리포트에 "상한선 추정치, Pass≠실전보장" 문구 고정 삽입. v2(point-in-time) 이번 스코프 아님.

### Stage 1 판정 (본 Handoff 규칙대로 자동 계산)
- Pass/Near-miss/Fail 상단 표 그대로 코드화
- Fail 시 원인 카테고리 A/B/C/D 자동 태깅 (§원인분석 트리 규칙 그대로 매핑: 진입n≈0→A, MDD>10%→C, 그 외 저수익→B)

### Stage 2 — C-1 A/B (조건부, Stage 1 결과로 자동 분기)
| Stage 1 결과 | Stage 2 행동 |
|--------------|---------------|
| Fail, 원인=A (신호부족) | **C-1 중단**. `report`에 "C-1 스킵: 원인 A" 명시하고 세션 종료 |
| Fail, 원인=C (MDD구조) | **C-1 중단**. 동일 처리 |
| Fail, 원인=B (수익부족·타이밍) | **C-1 축소 스코프**: sector spillover A/B만 (일반 기능화 아님), 15구간 중 원인B로 태깅된 구간만 재실행 |
| Near-miss (모든 원인) | **C-1 정상 진행** — baseline vs C-1 A/B, 15구간 전체 |
| Pass | C-1 진행(선택) — 이미 목표 달성이므로 우선순위 낮음, 스킵해도 무방 |

### 인접 CAT 영향
- CAT-F: read-only (tier 임계값 replay만, config_kv write 없음)
- CAT-G: read-only (REGIME_PERIODS 라벨 참조만)
- CAT-B: 신규 DB 없음 — JSON 리포트 파일만

### 롤백 조건
- 코드 자체가 배포되는 게 아니라 backtest 리포트 산출물이므로 롤백 대상 없음. 결과가 실망스러워도 **코드 삭제 금지** — 다음 Handoff 판단 자료로 보존.

### Cursor 지시
- Targeted diff only. Stage 1 무결론 2주 → **RP-1도 No-Go** (인프라/데이터 결함으로 태깅, C-1도 자동 스킵)
- 테스트: `tests/test_regime_panel_rp1.py` — 6→15 매핑 smoke, tier overlay 단위, Stage1→Stage2 분기 로직
- n<20 구간 자동판정 금지 로직 필수 (하드코딩 스킵)

### 위험도
🟡 High (목표 직결, 배포 아님) — 디렉터 승인 후 착수, 완료 후 Claude 검증

# 15 · POST-RP-1 단계별 로드맵 (Alpha Proof → 목표 달성)

> **SSOT 진행 표** — 디렉터·Claude·Cursor 공통. 세션마다 **현재 단계 1개만** 진행.  
> **갱신**: 2026-08-18 · **앵커**: `POST-RP1-ROADMAP-v1` · **ROADMAP-SYNC-01**: 단계2 동결 반영  
> **선행 완료**: RP-1 v2.3.3 (`rp1_20260811.json`) · Claude Alpha Proof baseline 확정  
> **현재 실행 줄기**: OBS-HOLD → daily n≥20 갈림길 (`NEXT_ACTION` / `00_SESSION_SYNC` §3) — 본 표의「다음 Go」는 **재소집 전 무효**

---

## 0. 한 줄 목표

RP-1으로 **손댈 구간을 지도에 표시**했고, 이제 **한 번에 하나씩** 수정·검증하며 North Star(CAGR 40~70% · MDD ≤10%)에 접근한다.

---

## 1. 단계 표 (체크리스트)

| 단계 | ID | 내용 | 담당 | 상태 | 산출물 / 기록 |
|------|-----|------|------|------|----------------|
| **0** | RP-1 | 15구간 full run + tier replay v2.3.3 | Cursor ✅ | **✅ 완료** | `reports/regime_panel/rp1_20260811.json` · `05_진행로그` §RP-1-INFRA |
| **1** | **SRV-01** | **STRATEGIC REVIEW** — 다음 sub-phase **1개** Go | Claude ✅ | **✅ 완료** | Go: **BULL-RECENCY-01** · `CLAUDE_TO_CURSOR.md` §BULL-RECENCY-01 |
| **2** | **BULL-RECENCY-01** | CLUSTER_1 타이트닝 → 15구간 rerun | Cursor | **🔴 동결(규칙1)** | rerun PASS · **근처놓침 레버 소진** · `NEXT_ACTION` 재접촉 금지 |
| **3** | OPS-01 | VPS 배포 (F-GATE → F-RETIRE → BEAR-UD) | 디렉터 | ⬜ 대기 (병렬 가능) | `06_검증체크리스트` · deploy_watch |
| **4** | ASG-01 | Forward 4주 조기경보 (n&lt;30 판정 금지) | 디렉터+시계 | ⬜ 대기 | north star digest |
| **5** | RP-2 | Point-in-time 템플릿 (lookahead 제거) | Claude→Cursor | ⬜ 후순위 | 별도 Handoff |

> **각주 (ROADMAP-SYNC-01)**: BULL-RECENCY-01 = 근처놓침 레버 소진 목록 포함(SIDE-ALPHA · BEAR-S5-SIM · C-1-REDUCED와 동일). `NEXT_ACTION` 규칙1 **재접촉 금지** 확인.

**규칙**: 단계 2는 **SRV-01 Claude Go 없이 착수 금지**. 단계 3은 단계 1·2와 **병렬** 가능. **동결 후**: 신규 Alpha Go는 daily n≥20 갈림길 재소집 전 금지.

---

## 2. RP-1 결과 스냅샷 (v2.3.3 · 기록용)

| 항목 | 값 |
|------|-----|
| JSON | `reports/regime_panel/rp1_20260811.json` (서버) · Desktop `rp1_20260811_v233.json` |
| schema | `regime_panel_rp1.v2.3.3` |
| overall_verdict | **PASS** (구조적 하한) |
| mdd_crosscheck | **MDD_OK** (tier 8.2~9.3%, 위반 0) |
| universe | KR200 + US200 |
| Stage2 C-1 | `OPTIONAL_SKIP` (Stage1 PASS) |

### 구간 판정 (3연속 패치 불변)

| bucket | PASS | NEAR_MISS | FAIL |
|--------|------|-----------|------|
| BULL ×5 | 3 | 0 | **2** (BULL_03, BULL_05) |
| SIDEWAYS ×5 | 3 | 2 | 0 |
| BEAR ×5 | 2 | 3 | 0 |

### 손댈 구간 (Cursor 추천 우선순위)

| 우선 | 구간 | 원인 | 추천 레버 |
|------|------|------|-----------|
| 1 | BULL_03, BULL_05 | B (수익 부족 / recency drift) | 템플릿·DNA 최신화 |
| 2 | SIDE_02, SIDE_03 | B (PF 근처) | 횡보 알파 / B-3 킬 |
| 3 | BEAR ×3 NEAR | B + 구조 | S5/인버스 시뮬 (RP-1 외) |
| 낮음 | C-1 섹터 | Stage1 PASS | OPTIONAL_SKIP |

### North Star 달성 여부 (명확히)

| 목표 | RP-1 결론 |
|------|-----------|
| MDD ≤10% | **tier 거버너 전제로 구조적 가능** (raw는 30~80%+) |
| CAGR 40~70% | **미증명** — 일부 BULL만 근접, lookahead v1 한계 |
| 15구간 일관 Pass | **아님** — BULL 2 FAIL |

---

## 3. 단계별 상세

### 단계 0 — RP-1 ✅

- 커밋: `e58baef` (INFRA-e kelly_cap) · `1f55d61` (INFRA-c metrics-only) · `cd606d0` (A-3 quota)
- 로그: 서버 `rp1_run_v233_metrics.log`
- Claude: v2.3.3 baseline 확정 (2026-08-11)

### 단계 1 — SRV-01 ✅ (2026-08-11)

1. 디렉터 → Claude Pro에 **§4 초안** 붙여넣기 + JSON 첨부 ✅
2. Claude → **sub-phase 1개 Go**: **BULL-RECENCY-01** ✅
3. Cursor → `CLAUDE_TO_CURSOR.md` append ✅ · `NEXT_ACTION` 단계 2로 갱신 ✅

### 단계 2 — BULL-RECENCY-01 🔴 동결(규칙1)

> **ROADMAP-SYNC-01**: 근처놓침 레버 소진. Claude OK 대기 상태 **폐기**. `NEXT_ACTION` 재접촉 금지.

- **1단계**: ✅ **완료** — classic drift 기각 · 공통 CLUSTER_1 edge compression · 03=SL스파이크 / 05=KR붕괴
- **2단계 코드+rerun**: ✅ **완료** (2026-08-12) — `rp1_bull_recency_01_20260812.json` · overall **PASS** · v2.3.4
- **DoD**: 1·3·4 추론 PASS · **2 baseline diff 보류** (`_dod.json` 미생성) — **동결로 종결 처리(추가 작업 없음)**
- **다음 Go**: OBS-HOLD 재소집(n≥20) 전 **없음**

### 단계 3 — OPS-01 ⬜ (병렬)

- `NEXT_ACTION.md` §VPS 배포 체크리스트 참조
- `DEPLOY_WATCH_PHASE=post_bear_underdog_01`

### 단계 4~5

- ASG: RP-1은 과거, forward는 **지금** — 4주 후 정성 체크
- RP-2: lookahead 제거 — 중기 Handoff

---

## 4. Claude STRATEGIC REVIEW 붙여넣기 초안

→ **`docs/work_phases/16_SRV01_Claude_붙여넣기초안.md`** (전문 복사용)

---

## 5. 갱신 로그

| 날짜 | 단계 | 내용 |
|------|------|------|
| 2026-08-11 | 0→1 | RP-1 v2.3.3 완료 · SRV-01 로드맵·초안 작성 · NEXT_ACTION/SYNC 갱신 |
| 2026-08-11 | 1→2 | SRV-01 Go **BULL-RECENCY-01** · Handoff `CLAUDE_TO_CURSOR.md` 기록 |
| 2026-08-11 | 2 | 1단계 ✅ trade-level: CLUSTER_1 단일지배 · 03 SL62.6% · 05 KR-0.38 · Claude 레버 대기 |
| 2026-08-12 | 2 | VPS rerun overall **PASS** · DoD 2 baseline 대조 · Claude OK 대기 |
| 2026-08-18 | 2 | **ROADMAP-SYNC-01**: 단계2 → **🔴 동결(규칙1)** · 소진 레버 · 재접촉 금지 |

---

*다음 갱신: daily n≥20 갈림길 재소집 후 — mega_trend / 목표하향 / 관측연장*

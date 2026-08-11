# 세션 동기화 앵커 (멀티 채널 · 멀티 창 SSOT)

> **새 Claude Pro 창 · 새 Cursor 채팅 · 텔레그램 회신 붙여넣기 전 — 이 파일을 먼저 읽는다.**  
> **갱신**: 2026-08-11 · **앵커 ID**: `SYNC-2026-08-11-B`

---

## 1. 한 줄 규칙 (디렉터·AI 공통)

| 규칙 | 내용 |
|------|------|
| **단일 진실** | KR/US 주식 팩토리 실행 SSOT = **`docs/work_phases/`** (본 폴더) **만** |
| **복사본 금지** | `Downloads/*.md`, 채팅에 붙인 예전 요약, `bitget/docs/work_phases/*` → **참고만**. 충돌 시 **본 폴더**가 이긴다 |
| **한 창 = 한 sub-phase** | 동시에 여러 Handoff 구현 금지 (`07_듀얼AI_협업루프.md`) |
| **상태 갱신 순서** | 구현/검증 후 → `05_진행로그` → `00_전체현황판`(필요 시) → **`00_SESSION_SYNC` §3 스냅샷** → `NEXT_ACTION` |
| **채팅 말고 파일** | Claude OK / Cursor 완료 / 조사 결과 → **`CURSOR_TO_CLAUDE` 또는 `CLAUDE_TO_CURSOR`에 append** |

---

## 2. 파일 권한 표 (누가·무엇을 쓰는가)

| 파일 | 역할 | 쓰는 쪽 | 읽는 쪽 |
|------|------|---------|---------|
| **`00_SESSION_SYNC.md`** | **멀티 창 앵커 · 현재 포커스 스냅샷** | Cursor (세션 종료 시 §3) | Claude · Cursor · 디렉터 (창 열 때) |
| `NEXT_ACTION.md` | **지금 디렉터가 할 일 1장** | Claude 또는 Cursor | 디렉터 |
| `05_진행로그.md` | sub-phase별 **이력·잔여** (append) | Cursor | Claude · Cursor |
| `00_전체현황판.md` | Phase 표 · **용어집** (가끔) | Cursor | Claude · Cursor |
| `CLAUDE_TO_CURSOR.md` | Handoff **INBOX** (설계·Go) | **Claude Pro 만** | Cursor |
| `CURSOR_TO_CLAUDE.md` | 검증·조사 **OUTBOX** | **Cursor 만** | Claude Pro |
| `06_검증체크리스트_*.md` | 배포 후 **효과** 검증 (2~4주) | 디렉터·Cursor | Claude |
| `bitget/docs/work_phases/` | **Bitget 트랙 미러** | Bitget 세션만 | BG 작업 시 — KR/US와 **혼동 금지** |

**`NEXT_STEP.md` vs `NEXT_ACTION.md`**: 실행 SSOT는 **`NEXT_ACTION.md`만**. `NEXT_STEP`은 레거시·참고.

---

## 3. 현재 포커스 스냅샷 (세션 종료 시 갱신)

| 필드 | 값 |
|------|-----|
| **앵커 ID** | `SYNC-2026-08-11-B` |
| **마지막 갱신** | 2026-08-11 (Cursor) |
| **활성 트랙** | KR/US 주식 — **POST-RP-1 Alpha Proof** |
| **진행 중 sub-phase** | **SRV-01** STRATEGIC REVIEW (Claude Go 대기) |
| **직전 완료** | RP-1 v2.3.3 ✅ · `rp1_20260811.json` PASS · Claude baseline 확정 |
| **Claude OK 완료** | RP-1 v2.3.3 baseline · F-GATE · F-RETIRE · CAT-E-BARS |
| **구현 완료·배포 대기** | BEAR-UNDERDOG-01 · L-OBS-02 · F-GATE/F-RETIRE (OPS-01) |
| **디렉터 블로커** | **`16_SRV01_Claude_붙여넣기초안.md`** → Claude 붙여넣기 + JSON 첨부 |
| **git main** | `e58baef+` — RP1-INFRA-e (kelly_cap v2.3.3) |

### 열린 작업 줄기 (꼬이지 않게)

```
[Alpha] SRV-01 Claude Review → Go 1개 → Handoff → Cursor 단계 2
[로드맵] docs/work_phases/15_POST_RP1_단계별로드맵.md (체크리스트 SSOT)
[Ops 병렬] OPS-01 VPS 배포 · deploy_watch · IV_OBS
[후순위] ASG 4주 · RP-2 lookahead
```

**다른 창에서 다른 sub-phase를 열었다면** → 그 창 닫기 전에 §3 이 표만이라도 갱신하거나, 디렉터에게 「앵커 갱신 필요」라고 남긴다.

---

## 4. 새 창 부팅 문구 (복붙용)

### Claude Pro (설계·검증 창)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기(순서):
1) docs/work_phases/00_SESSION_SYNC.md
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md (최상단 OUTBOX)
4) 필요 시 docs/work_phases/CLAUDE_TO_CURSOR.md

트랙: KR/US docs/work_phases/ 만 SSOT. bitget/ Downloads 복사본은 참고만.
충돌 시 00_SESSION_SYNC §3 스냅샷이 최신이면 그걸 따른다.

[여기에 이번 창 목적 한 줄, 예: "CAT-E-BARS-01 OUTBOX 검증"]
```

### Cursor (구현·조사 창)

```text
역할: Cursor Lead Engineer.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CLAUDE_TO_CURSOR.md (해당 sub-phase Handoff)

한 세션 = sub-phase 하나. 세션 종료 전: 05_진행로그 + 00_SESSION_SYNC §3 + NEXT_ACTION 갱신.

[여기에 sub-phase ID, 예: "L-OBS-01" 또는 "조사만 CAT-E-BARS-01"]
```

### 텔레그램 → Cursor / Claude (deploy_watch · IV_OBS)

```text
[DEPLOY_WATCH] 또는 [IV_OBS] 수신 시:
1) ---CURSOR--- 아래 블록 전체를 Cursor 새 채팅 첫 메시지로 붙여넣기
2) Cursor가 cursor_action 해석 → CURSOR_TO_CLAUDE OUTBOX (Claude 검증 필요 시)
3) Claude Pro는 docs/work_phases/ 만 읽음 — 텔레그램 직접 접근 없음

deploy_watch cursor_action SSOT:
  NONE · REPORT_TO_CLAUDE · INVESTIGATE · BLOCK_F_RETIRE_02_DEPLOY
  INVESTIGATE_BEAR_UNDERDOG_TAG · OBSERVE_BEAR_UNDERDOG_L2
```

**VPS 파일 SSOT** (SSH 가능 시): `deploy_watch_latest.json` · `iv_observation_latest.json`

---

## 5. 충돌·중복 발견 시 (AI 행동 규칙)

1. **`NEXT_ACTION`과 `05_진행로그`가 다르면** → `05`의 **해당 sub-phase 최신 섹션** 우선, `NEXT_ACTION`을 맞춘다.  
2. **`CLAUDE_TO_CURSOR` 상단 "현재"와 `00_SESSION_SYNC` §3이 다르면** → **§3을 먼저 디렉터에게 질문** ("어느 쪽이 최신인가?")  
3. **Downloads에 `CURSOR_TO_CLAUDE (1).md` 등** → 레포 `docs/work_phases/`에 merge된 뒤에만 신뢰.  
4. **두 Cursor 창이 동시에 같은 파일 수정** → 나중 커밋이 이기지 않게, 한 창은 조사-only(파일 미쓰기).  
5. **세션 종료 시** §3의 `앵커 ID`를 `SYNC-YYYY-MM-DD-B`처럼 bump (같은 날 두 번째 창이면 B, C…).

---

## 6. Cursor / Claude 세션 종료 체크리스트 (3줄)

- [ ] `05_진행로그` 해당 sub-phase 섹션 갱신  
- [ ] **`00_SESSION_SYNC.md` §3 스냅샷** 갱신  
- [ ] `NEXT_ACTION.md` 디렉터 할 일 1장 갱신  
- [ ] (구현 시) `CURSOR_TO_CLAUDE` OUTBOX 또는 (설계 시) `CLAUDE_TO_CURSOR` Handoff  

---

## 7. Bitget 트랙 분리

| | KR/US (주식) | Bitget |
|--|--------------|--------|
| SSOT 폴더 | `docs/work_phases/` | `bitget/docs/work_phases/` |
| 세션 앵커 | **본 파일** | `bitget/docs/work_phases/README.md` → 주식 앵커 참조 |
| 혼합 금지 | Handoff에 `bitget/` 경로 섞지 않기 (명시적 BG sub-phase 제외) | |

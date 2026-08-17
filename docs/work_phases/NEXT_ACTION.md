# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **OPS-01** (지금) → 이후 **S5-HARNESS-SCOPE-01** |
| **status** | `WAIT_DIRECTOR` — VPS SSH는 이 Cursor 창에서 **불가** · Alpha 코드 금지 |
| **디렉터 결정** | 2026-08-17 — **먼저 배포(OPS-01)** · **그다음 1번(S5 스코프)** |
| **앵커** | `SYNC-2026-08-17-I` |

---

## 디렉터 — 지금 할 일

1. **키 있는 셸**에서 VPS `git pull` + `sudo ./update_factory.sh` (이 Cursor 창 SSH 불가).
2. `DEPLOY_WATCH_PHASE=post_bear_underdog_01` 설정 후 1차 관측.
3. 배포가 끝나면 이 파일에서 OPS-01을 체크하고, Claude에 **S5-HARNESS-SCOPE-01** SRV-lite를 요청한다.

### Ops 창 부팅 (복붙)

```text
Track A — OPS만. Alpha/Handoff 구현 금지.

docs/work_phases/NEXT_ACTION.md §OPS-01 수행.
코드 diff 필요 시 멈추고 Handoff 요청만 보고.
결과: CURSOR_TO_CLAUDE.md 짧게.
```

---

## §OPS-01 — VPS 배포 체크리스트 (지금)

순서 권장: **F-GATE-01 → F-RETIRE-02 → BEAR-UNDERDOG-01 / L-OBS-02** (같은 날 묶을 때 원인 추적 어려우면 F-GATE·F-RETIRE를 먼저).

| # | 할 일 | 비고 |
|---|--------|------|
| 1 | VPS `git pull` | **미실행** — Cursor `publickey` 거부. 대상 `origin/main` **`2ecb6d7`** |
| 2 | `update_factory.sh` (또는 기존 표준 배포) | **미실행** — 디렉터 키 있는 셸 |
| 3 | F-GATE-01 배포·1차 관측 | 코드는 이미 main (`9cf0018`). COOLED/RETIRED 0건이면 미발화 정상 |
| 4 | F-RETIRE-02 배포 | 동일 커밋 · observe_only · 실notional 계속 블록 |
| 5 | BEAR-UNDERDOG-01 + L-OBS-02 | `4906d89` · `201dd74` · `DEPLOY_WATCH_PHASE=post_bear_underdog_01` |
| 6 | deploy_watch / `[DEPLOY_WATCH]` 텔레그램 | 이상 시 Ops OUTBOX만 |
| 7 | 짧은 OUTBOX → `CURSOR_TO_CLAUDE.md` | **차단 기록함** (`SYNC-2026-08-17-I`) |

### Ops 금지

- Alpha Handoff 구현 · S5 하네스 코드 · BULL/SIDE/BEAR 재개 · config_kv 라이브 실험

---

## 그다음 (배포 후 · 아직 착수 금지)

| 순서 | ID | 내용 |
|------|-----|------|
| **2** | **S5-HARNESS-SCOPE-01** | Claude SRV-lite — 태그리플레이 / 풀슬리브 / 페이퍼게이트 중 **스코프 1개** 확정 → Handoff |

배포 완료 후 디렉터 → Claude:

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.
docs/work_phases/00_SESSION_SYNC.md §3 · NEXT_ACTION.md 읽기.
목적: S5-HARNESS-SCOPE-01 SRV-lite — 스코프 1개만 Go → CLAUDE_TO_CURSOR.md에 Handoff.
OPS-01은 배포 진행 중/완료로 두고 Alpha 구현과 섞지 말 것.
```

---

## BEAR-S5-SIM-01 (1단계 Done · 2단계 보류)

| 항목 | 값 |
|------|-----|
| **1단계** | Done — 원인 B · S5 미배선 · RP-1 내 레버 없음 |
| **2단계** | 미착수 — S5-HARNESS-SCOPE-01 이후 |

---

## SIDE / BULL (종료 · 부분 Done)

- SIDE-ALPHA-01 · BULL-RECENCY-01 — 동결 유지 · 재시도 금지

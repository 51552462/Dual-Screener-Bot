# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | L-1 (서버 검증) → **B-1** Handoff 대기 |
| **status** | `WAIT_DIRECTOR` |

---

## 디렉터

1. **L-1 서버 검증** — `sudo INSTALL_ROOT=... bitget/deploy/install_bitget_logrotate.sh --test` (설치 + dry-run)
2. **L-1 `06`** — 30일 disk usage 관측 시작
3. **A paper** — 배포·관측 병행 (기존 트랙)

---

## Claude Pro

- **다음 Handoff**: **B-1** (deathmatch market key `BG`→`SPOT`/`FUT` 정규화, 🟢)
- L-1 1단계 OK 완료 — 서버 검증은 디렉터 몫

---

## Cursor

- B-1 Handoff 수신 후 `WAIT_CURSOR_IMPL`

---

## 병렬 트랙

| 트랙 | 상태 |
|------|------|
| A paper | `06` 2~4주 대기 |
| L-1 | Claude OK ✅ · **서버 설치·30d `06`** |
| B-1 | Handoff 대기 (Claude) |

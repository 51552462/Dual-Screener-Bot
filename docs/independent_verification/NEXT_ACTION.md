# NEXT_ACTION — Independent Verification

| 필드 | 값 |
|------|-----|
| **sub-phase** | **V-2a** scaffold OFF + IV 관측 리포트 |
| **status** | `WAIT_CLAUDE_OK` (V-1 OK ✅) |
| **병행** | `work_phases/NEXT_ACTION` — F-GATE/F-RETIRE 배포 (L1) |

---

## 디렉터 — 지금 할 일

### 1) 텔레그램 [IV_OBS] 주간 리포트 (일요 20:10 KST cron)

- 메시지 맨 아래 **`---CURSOR---` 아래 블록**을 Cursor 새창에 **그대로** 붙여넣기 → A-6 (`08_디렉터_중계_가이드`)
- 수동 실행: `./factory.sh --iv-observation --force-telegram`
- JSON SSOT: `~/dante_bots/.../iv_observation_latest.json`

### 2) V-2 BLOCK 켜기 — **readiness=READY 일 때만** (약 9월 중순+)

```bash
# 디렉터 명시적 Go 이후에만
export WALK_FORWARD_PROMOTION_BLOCK_ENABLED=1
```

### 3) 기존 배포 (L1)

`work_phases/NEXT_ACTION.md` — F-GATE/F-RETIRE

---

## 완료

- [x] V-0 `docs/independent_verification/` 10파일 + work_phases/claude_project 링크
- [x] V-1 `reality_audit_check` + `meta.wf_warn` (2026-08-09) · pytest 18 passed
- [x] IV 체크리스트 25항 (6+19)

## 대기

- [ ] V-1 Claude OK
- [ ] F-GATE/F-RETIRE 서버 배포 (`work_phases`)
- [ ] V-1 2주 WARN 오탐률 관측
- [ ] V-2 Handoff (WF BLOCK + config snapshot)

---

## 3줄 요약 (Cursor · 2026-08-09)

1. **V-1**: `deploy_watch.reality_audit_check` (IV-21) + `strategy_promotion_engine` `meta.wf_warn` (IV-04 WARN).
2. **차단 없음**: `WALK_FORWARD_PROMOTION_BLOCK` 미변경 · LIVE 승격 조건 무변경 · telegram WARN만.
3. **테스트**: `test_deploy_watch_l_obs_01` + `test_v1_wf_warn_meta` — **18 passed**.

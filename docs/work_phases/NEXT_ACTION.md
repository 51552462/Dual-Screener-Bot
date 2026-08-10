# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **CAT-C BEAR-UNDERDOG-01** ✅ · **L-OBS-02** deploy_watch 연동 ✅ · **서버 배포** (디렉터) |
| **status** | `WAIT_DIRECTOR` (VPS git pull + 배포 + DEPLOY_WATCH_PHASE 설정) |

---

## 디렉터 — 지금 할 일

### 1) VPS 배포 (한 번 pull · 순서대로 관측)

```bash
cd ~/dante_bots/Dual-Screener-Bot
git pull
sudo ./update_factory.sh
sudo systemctl status dante-factory.service
```

| 순서 | 대상 | 배포 후 확인 |
|------|------|----------------|
| 1 | **F-GATE-01** | COOLED/RETIRED 0건 → `registry_state_block` 로그 없음 = 정상 |
| 2 | **F-RETIRE-02** | 강등 1건 시 `LIFECYCLE_OBSERVE_ONLY` + $0 |
| 3 | **BEAR-UNDERDOG-01** | BEAR incubator underdog 진입 시 `sig_type`에 `_BEAR_UNDERDOG_SHADOW` |

### 2) deploy_watch phase 설정 (BEAR-UNDERDOG 배포 후)

cron 또는 `.env`에 추가:

```bash
DEPLOY_WATCH_PHASE=post_bear_underdog_01
```

- **효과**: KR BEAR incubator underdog인데 suffix 없으면 **WARN** → 텔레그램 `[DEPLOY_WATCH]` + `---CURSOR---`
- **파일 SSOT**: `~/dante_bots/.../deploy_watch_latest.json`

### 3) 텔레그램 → Cursor / Claude 루프 (붙여넣기 SSOT)

| 메시지 | 언제 | Cursor 첫 메시지 |
|--------|------|------------------|
| `[DEPLOY_WATCH]` | 19:35 KST · WARN/BREAK만 | `---CURSOR---` **아래 JSON 전체** 또는 `cursor_prompt` 줄 |
| `[IV_OBS]` | 일 20:10 KST 주간 | `---CURSOR---` 아래 `cursor_prompt` (BEAR_UD shadow·mae 포함) |

Claude Pro는 텔레그램을 읽지 않음 → Cursor가 `CURSOR_TO_CLAUDE.md` OUTBOX append.

### 4) (연기) F-QUOTA-LOG-01 · BEAR hard gate

- F-QUOTA: F-GATE/F-RETIRE 배포 + L-OBS 관측 이후
- BEAR hard block: L2 shadow `closed≥30` + pain cluster 재현 시 **별도 Handoff**

---

## 완료

- [x] CAT-E-BARS-01 Reality Audit · Claude OK
- [x] F-GATE-01 · F-RETIRE-02 구현 · Claude OK
- [x] CAT-C BEAR-UNDERDOG-01 구현 · push `4906d89`+
- [x] L-OBS-02 `c_bear_underdog_01` + `cursor_prompt` + IV 주간 BEAR_UD 요약

## 대기

- [ ] VPS 배포 (F-GATE → F-RETIRE → BEAR-UNDERDOG)
- [ ] `DEPLOY_WATCH_PHASE=post_bear_underdog_01` 설정
- [ ] 첫 `_BEAR_UNDERDOG_SHADOW` 태그 실측 (SQL 또는 deploy_watch metrics)

# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **L-OBS-01** 구현 완료 · **F-GATE-01 + F-RETIRE-02** 서버 배포(디렉터) |
| **status** | `WAIT_DIRECTOR_DEPLOY` · `WAIT_CLAUDE_OK` (L-OBS-01) |

---

## 디렉터 — 지금 할 일

### 1) 서버 배포 (순서: F-GATE-01 → 관측 → F-RETIRE-02)

```bash
cd ~/dante_bots/Dual-Screener-Bot
git pull
sudo ./update_factory.sh   # cron에 --deploy-watch 19:35 KST 포함
sudo systemctl status dante-factory.service
```

**배포 단계별 phase (선택)**

```bash
DEPLOY_WATCH_PHASE=post_f_gate_01 ./factory.sh --deploy-watch
# F-RETIRE-02 배포 후:
DEPLOY_WATCH_PHASE=post_f_retire_02 ./factory.sh --deploy-watch
```

> **자동 관측**: 매일 19:35 KST cron — WARN/BREAK만 텔레그램.  
> OK면 무음. 결과 JSON: `deploy_watch_latest.json` (factory data dir).  
> 텔레그램 `---CURSOR---` 블록을 Cursor에 붙여넣기 → 회신 초안 작성.

### 2) C-FUNNEL T+1

다음 스캔 후 `c_funnel_02`가 PASS로 바뀌는지 deploy_watch가 판정 (수동 SQL 불필요).

---

## 완료

- [x] F-GATE-01 · F-RETIRE-02 구현 · Claude OK 2026-08-09
- [x] L-OBS-01 `deploy_watch.py` + cron 19:35 KST
- [x] C-FUNNEL-02 배포 2026-08-09

## 대기

- [ ] F-GATE-01 / F-RETIRE-02 서버 배포
- [ ] L-OBS-01 Claude OK (선택)
- [ ] C-FUNNEL T+1 — deploy_watch `c_funnel_02` PASS 확인

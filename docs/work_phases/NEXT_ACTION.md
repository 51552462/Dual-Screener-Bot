# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **CAT-E-BARS-01** Claude OK ✅ · **F-GATE-01 + F-RETIRE-02** 서버 배포(디렉터) |
| **status** | `WAIT_DIRECTOR` (VPS SQL (a)~(d) + 배포) |

---

## 디렉터 — 지금 할 일

### 1) VPS SQL (CAT-E-BARS-01 — 코드 더 안 늘림)

`CURSOR_TO_CLAUDE.md` §CAT-E-BARS-01 「VPS 확인 SQL」 **(a)(b)(c)(d)** 실행 → 결과 회신.

- (a) 결측·표본 n  
- (b) `exit_type` 분포  
- (c) status별 `exit_type` 오염  
- (d) bars×ret 버킷 (1-3 / 4-6 / 7-10 / 11-14 / 15+)

> Claude: 신규 스크립트 **금지**. SQL만.

### 2) 서버 배포 (병렬 가능 · 순서: F-GATE-01 → 관측 → F-RETIRE-02)

```bash
cd ~/dante_bots/Dual-Screener-Bot
git pull
sudo ./update_factory.sh
sudo systemctl status dante-factory.service
```

### 3) (연기) F-QUOTA-LOG-01

F-GATE-01 / F-RETIRE-02 배포 + L-OBS-01 관측 **이후**. RL 연장 컬럼 = **No-Go**(지금).

---

## 완료

- [x] CAT-E-BARS-01 Reality Audit · **Claude OK 2026-08-09**
- [x] F-GATE-01 · F-RETIRE-02 구현 · Claude OK
- [x] C-FUNNEL-02 배포 · L-OBS-01 코드

## 대기

- [ ] VPS SQL (a)~(d) 결과
- [ ] F-GATE-01 / F-RETIRE-02 서버 배포
- [ ] (연기) F-QUOTA-LOG-01

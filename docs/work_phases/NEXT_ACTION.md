# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **SRV-01** STRATEGIC REVIEW (POST-RP-1) |
| **status** | `WAIT_CLAUDE` — 디렉터가 Claude에 초안 붙여넣기 |
| **로드맵 SSOT** | [`15_POST_RP1_단계별로드맵.md`](15_POST_RP1_단계별로드맵.md) |

---

## 디렉터 — 지금 할 일 (단계 1)

### 1) Claude STRATEGIC REVIEW (최우선)

1. 파일 열기: **`16_SRV01_Claude_붙여넣기초안.md`**
2. Claude Pro 새 채팅 → 부팅 문구 + 본문 **전체 복사**
3. 첨부: `rp1_20260811_v233.json` (바탕화면 또는 서버 JSON)
4. Claude 답변 → `CLAUDE_TO_CURSOR.md` **상단에 Handoff append** (또는 디렉터가 Cursor에 전달)

### 2) Claude Go 수신 후

- `15_POST_RP1_단계별로드맵.md` 단계 1 ✅ · 단계 2 ID 기입
- Cursor 새 창 → `CLAUDE_TO_CURSOR.md` Handoff 1개만 구현

---

## 단계별 로드맵 (요약)

| 단계 | ID | 상태 |
|------|-----|------|
| 0 | RP-1 v2.3.3 | ✅ 완료 |
| **1** | **SRV-01** | **🟡 지금** |
| 2 | Alpha-XX (Claude Go 1개) | ⬜ 대기 |
| 3 | OPS-01 VPS 배포 | ⬜ 병렬 가능 |
| 4 | ASG-01 (4주) | ⬜ 대기 |
| 5 | RP-2 lookahead | ⬜ 후순위 |

---

## OPS-01 — VPS 배포 (Alpha와 병렬 가능)

```bash
cd ~/dante_bots/Dual-Screener-Bot
git pull
sudo ./update_factory.sh
sudo systemctl status dante-factory.service
```

| 순서 | 대상 | 배포 후 확인 |
|------|------|----------------|
| 1 | **F-GATE-01** | COOLED/RETIRED 0건 → `registry_state_block` 없음 = 정상 |
| 2 | **F-RETIRE-02** | 강등 1건 시 `LIFECYCLE_OBSERVE_ONLY` + $0 |
| 3 | **BEAR-UNDERDOG-01** | BEAR incubator underdog → `sig_type` `_BEAR_UNDERDOG_SHADOW` |

```bash
DEPLOY_WATCH_PHASE=post_bear_underdog_01
```

---

## 완료 (최근)

- [x] RP-1 full 400 · v2.3.3 · `rp1_20260811.json` PASS · Claude baseline 확정
- [x] RP1-INFRA-a~e (OHLCV cache, metrics-only, A-3 quota, kelly_cap)
- [x] CAT-C BEAR-UNDERDOG-01 · L-OBS-02 deploy_watch
- [x] POST-RP-1 로드맵 · SRV-01 Claude 초안 (`15_` · `16_`)

## 대기

- [ ] **SRV-01** Claude STRATEGIC REVIEW → Go sub-phase 1개
- [ ] Alpha sub-phase Handoff 구현 (SRV-01 후)
- [ ] VPS 배포 (OPS-01)
- [ ] ASG 4주 관측 시작 (배포 후)

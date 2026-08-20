# 18 · 디렉터 VPS 원클릭 (KR/US) — 헷갈릴 때 이 파일만

> **SSOT**: 본 파일 · 새 창/배포 전 `00_SESSION_SYNC` §3 → `NEXT_ACTION` → **본 파일**  
> **트랙**: KR/US만 (`bitget/` 코인은 Track B 별도)  
> **갱신**: 2026-08-19 · OPS-LIQUIDITY-STALL CLOSED 이후

---

## 1. 한 줄 답 (가장 중요)

| 질문 | 답 |
|------|-----|
| PC에서 커밋·푸시 후 VPS에 **최신 코드·크론·서비스** 올리려면? | **`sudo bash ./update_factory.sh` 한 줄이면 됨** (아래 §2) |
| 그걸로 가상매매 **진입·청산이 무조건 생가나?** | **아니요.** 엔진·스케줄은 돌고, **게이트(예: LIQUIDITY)가 막으면 OPEN 0이 정상** |
| 매일 시세·스캔·리포트는? | **크론이 이미 돌림** — update_factory 때마다 수동 refresh 불필요 |

즉: **「구조(코드/서비스) 100% 배포」= update_factory**  
**「오늘 매매 기록 100%」≠ update_factory** (관측·게이트 결과).

---

## 2. 배포 원클릭 (거의 항상 이것만)

PC에서 Cursor가 커밋·푸시한 뒤, VPS:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
sudo bash ./update_factory.sh
```

이게 하는 일 (요약):

1. `git pull` (ubuntu)
2. systemd 유닛·venv 엔진 재배포
3. factory cron 재설치 (`/etc/cron.d/...`)
4. 스키마 마이그레이션 · import smoke
5. 레거시 DB 격리 · **data health 스모크** (RED면 **자동 data-refresh 1회** 후 재검사 · 그래도 RED면 경고만 · 배포는 완료)
6. `dante-factory` / dashboard / async · 타이머 재시작

### 배포 후 health RED가 뜨면

평소에는 **추가 입력 불필요** — `update_factory`가 RED일 때 `factory.sh --data-refresh`를 **1회만** 자동 실행합니다.  
경고가 **끝까지** 남으면(자동 치유 실패) 그때만:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
set -a && source .env && set +a
python3 scripts/diag_forward_staleness.py
TZ=Asia/Seoul bash ./factory.sh --data-refresh
```

- 정상이면 다음날부터 **08:00 data_refresh 크론**이 담당
- 자동 치유 끄기: `UPDATE_FACTORY_SKIP_DATA_HEAL=1 sudo bash ./update_factory.sh`
- **매 배포 상시 refresh는 하지 않음** (무거움) — RED일 때만

---

## 3. 언제 문구가 “늘어나나”

| 상황 | 추가 문구 | 어디서 보나 |
|------|-----------|-------------|
| **평소 코드 배포** | §2 `update_factory`만 | 본 파일 |
| **배포 health RED** | 보통 자동 heal · 실패 시 `diag` + `--data-refresh` | §2 아래 |
| **이번 세션 전용 진단** (예: stall 스크립트) | `NEXT_ACTION.md`에만 임시 복붙 | **NEXT_ACTION** |
| **Bitget 코인** | Track B SSOT · 본 파일 **쓰지 말 것** | `bitget/docs/work_phases/` |

Cursor/Claude가 새 진단 스크립트를 넣으면 **그 실행문은 `NEXT_ACTION`에만** 적는다.  
**영구 원클릭은 계속 §2 한 줄** — 진단이 늘어도 update_factory를 대체하지 않는다.

---

## 4. 디렉터 체크리스트 (배포 직후)

```text
[ ] PC: git push 완료 (Cursor가 커밋 해시 알려줌)
[ ] VPS: sudo bash ./update_factory.sh
[ ] (선택) health RED **끝까지** 남으면 §2 수동 복구 — 평소는 update_factory 자동 heal
[ ] systemctl is-active dante-factory 등 OK
[ ] 「진입 0」이면 update_factory 실패가 아님 → NEXT_ACTION / 관측(OBS-HOLD)
```

---

## 5. Cursor · 새 창 규칙

- 세션 시작: `00_SESSION_SYNC` §3 → `NEXT_ACTION` → **필요 시 본 파일 §2**
- 배포 안내를 채팅에만 쓰지 말고, **임시 명령은 `NEXT_ACTION`에 갱신**
- 본 파일 §2를 바꾸려면 디렉터 확인 후 (원클릭 SSOT)

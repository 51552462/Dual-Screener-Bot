# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | **NS-BG-CRON-ISO-01** · 코인 일보 미수신 |
| **status** | **WAIT_DIRECTOR** |
| **코드** | 진단 스크립트·발송 로그 보강 완료 · **서버에서 --send 1회** |

---

## 디렉터 (지금 · 코인 북극성 안 올 때)

주식 북극성만 오고 코인이 안 오면 → **코인 cron이 없거나 한 번도 안 돌았거나 실패**인 경우가 대부분.  
(코인은 **매일 20:00 KST만**. 주간 텔레그램 없음.)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
git pull --ff-only

# 1) 주식 오염 cron 제거
sudo bash bitget/deploy/uninstall_stock_north_star_cron.sh

# 2) 코인 일보 cron 재설치 (post-deploy-obs 줄 강제)
sudo INSTALL_ROOT=$PWD bash bitget/deploy/install_bitget_cron.sh

# 3) 진단 + 지금 텔레그램 1회 강제 발송
bash bitget/deploy/diagnose_coin_digest.sh --send
```

기대: 텔레그램에 **`📊 코인 북극성 · Bitget`** + 코인 연습 대시보드.  
안 오면 `diagnose` [4] 로그 tail / REPORT_BOT 줄을 이 채팅에 붙여넣기.

---

## 병행

- Claude: `track_b_CURSOR_TO_CLAUDE` OUTBOX (cron 격리 + 미수신)
- SHORT SECTOR 최종 OK 대기

**금지:** C-2 · MDD 5% · live · `ENABLE_REAL_EXECUTION`

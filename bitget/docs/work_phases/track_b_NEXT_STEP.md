# NEXT_STEP — 다음 한 걸음

> **갱신**: 2026-08-23 · 코인 채팅에 주식 북극성 → cron 격리 · 서버 uninstall

---

| 필드 | 값 |
|------|-----|
| **지금 단계** | NS-BG-CRON-ISO-01 · 코인 VPS에서 주식 north-star cron 제거 |
| **status** | `WAIT_DIRECTOR` |
| **한 줄** | `sudo bash bitget/deploy/uninstall_stock_north_star_cron.sh` 후 audit [1b] 확인 |

---

## 당신 체크리스트

- [ ] 코인 VPS: `uninstall_stock_north_star_cron.sh` (또는 update_bitget.sh)
- [ ] `audit_bitget_stack.sh` → [1b] no stock north-star · post-deploy-obs OK
- [ ] 다음날: 19:30 주식 북극성 **안 옴** · 20:00 코인 북극성 **옴**
- [ ] (병행) Claude SECTOR 최종 OK
- [ ] C-2 · MDD 5% · live · 실전 **금지**

---

## 다음 코드 우선순위

| 순서 | ID | 등급 |
|------|-----|------|
| **0** | 서버 cron 격리 (본 이슈) | Ops |
| **1** | Claude SECTOR 최종 OK 후 관측 | ① |
| **2** | SHORT-DNA-01 | 재료 조건 + Handoff |
| 금지 | C-2 · MDD 5% · alloc live · 실전 ON | 🔴 |

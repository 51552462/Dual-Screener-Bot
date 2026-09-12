# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **SMARTMONEY-PERSIST-FIX-01** |
| **status** | **Claude OK** · **WAIT_DIRECTOR** (배포) · DoD#1은 월 16:10 |
| **직전** | Claude OK 2026-09-12 완전 승인 |
| **앵커** | `SYNC-2026-09-12-SMARTMONEY-OK` |

---

## 디렉터 — 지금 할 일

1. **배포**: 커밋·푸시 후 VPS `sudo bash ./update_factory.sh` (`18_디렉터_VPS_원클릭.md`)
2. **월요일 16:10 이후** `kr_investor_flow` 행수 0→N · 로그 `🗄️ [수급 시계열]` · 네이버 맵 대조
3. 실측 OK면 CLOSED. JSON RADAR sqlite 정합은 backlog `SMARTMONEY-RADAR-SQLITE-01` (급하지 않음)

### 금지

- DoD#1 전에 sub-phase CLOSED
- PyKRX 로그인 · lookback 20 · `get_flow_score` 변경
- NAV/승격/텔레그램 · bitget

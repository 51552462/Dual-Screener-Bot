# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **OPS-LIQ-FORK-01 Phase 1** (품질 밴드 계측) |
| **status** | 스크립트 Done · **VPS 실행 후** `WAIT_CLAUDE_OK` · 임계 **금지** |
| **직전** | Claude **(A′) 채택** · Phase 2는 디렉터 숫자 승인 전 금지 |
| **앵커** | `SYNC-2026-08-19-G` |

---

## 디렉터 — 지금 할 일

1. **커밋·푸시 후 VPS:**

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && git pull
set -a && source .env && set +a
python3 scripts/ops_liq_fork_01_quality_band_phase1.py
```

(유니버스 스캔이라 **수 분** 걸릴 수 있음)

2. 로그를 Cursor에 붙여 → OUTBOX 분포표 → Claude 검증.  
3. **Phase 2 / 임계 변경 = 지금 금지.**  
   - 중간~상위 집중 → Phase 2 **논의** (숫자 디렉터 지정)  
   - 하위 극단 집중 → **(B) 관측연장**

### 한 줄

```text
(A′) Phase1 계측만 — 탈락 표본이 유동성 중간~상위인지 하위 극단인지 분포 확인. 잡주 개방 완화 금지.
```

---

## North Star SSOT (고정)

| 항목 | 값 |
|------|-----|
| **SSOT** | VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| **로컬** | `*.LOCAL_DEV_DO_NOT_USE.json` — **사용 금지** |

---

## 근처놓침 레버 — 전원 소진·동결

BULL-RECENCY · SIDE-ALPHA · BEAR-S5-SIM · C-1-REDUCED — 규칙1 재접촉 금지

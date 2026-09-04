# NEXT_ACTION — Independent Verification

| 필드 | 값 |
|------|-----|
| **sub-phase** | **V-2-WFBLOCK-01** (발사 대기) |
| **status** | **`WAIT_READY`** · Handoff 초안 고정 · **구현·env ON 금지** |
| **발사 조건** | ① `readiness=READY` (9/6+ dry-run) ② 디렉터 **"가자"** |
| **스코프** | WF BLOCK만 · DSR/V-2b = backlog |

---

## 디렉터 — 지금 할 일

### 1) ~9/6 이후 READY 재확인 (텔레그램 스팸 방지)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
./factory.sh --iv-observation --dry-run
# 또는: python3 scripts/run_iv_observation_report.py --dry-run
```

확인할 값 (`iv_observation_latest.json`):

| 필드 | READY 요건 |
|------|------------|
| `days_elapsed` | ≥ 28 |
| `wf_warn_count` | ≥ 3 |
| `false_positive_rate` | ≤ 0.25 |
| `reality_audit.status` | ≠ BREAK |
| `v2.readiness` | **READY** |

2026-09-04 실측: 26/28 · warn=4 · FP=0.0 · reality=WARN · **NOT_READY** (일수만 부족)

### 2) READY + **"가자"** → Cursor에 V-2-WFBLOCK-01 발사

- Handoff SSOT: `docs/independent_verification/CLAUDE_TO_CURSOR.md` INBOX (이미 붙여둠)
- **새 코드 없음** — env `WALK_FORWARD_PROMOTION_BLOCK_ENABLED=1` + 회귀 검증

### 3) 롤백

```bash
export WALK_FORWARD_PROMOTION_BLOCK_ENABLED=0
```

---

## Backlog (착수 금지)

- **V-2-DSR-01** — DSR→승격 배선
- **V-2B-SNAPSHOT-01** — 승격 config 동결

---

## 완료

- [x] V-0 SSOT 폴더
- [x] V-1 WARN + reality audit (구현)
- [x] V-2 스코프 3갈래 확정 (2026-09-04)
- [x] V-2-WFBLOCK-01 Handoff 초안 대기 고정

## 대기

- [ ] 9/6+ dry-run → READY
- [ ] 디렉터 Go → V-2-WFBLOCK-01 구현 세션
- [ ] V-2-DSR-01 / V-2B-SNAPSHOT-01 (이후)

---

## 3줄 요약 (Cursor · 2026-09-04)

1. **WF BLOCK만** 이번 후보 — 이미 배선·OFF · 스위치+검증.
2. **DSR·V-2b** 별도 backlog — 한 Handoff에 묶지 않음.
3. **지금은 발사 안 함** — READY+"가자" 전 env ON·구현 금지.

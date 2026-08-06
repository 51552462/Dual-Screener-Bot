# NEXT_STEP — 디렉터 다음 한 걸음

> **갱신**: 2026-08-07 · Alpha Proof **압축** · G2 유지 · ASG=조기경보

---

| 필드 | 값 |
|------|-----|
| **지금** | ① 일괄 배포 ② north star cron ③ C-1 Handoff (backtest 절차 포함) |
| **한 줄** | **2주 backtest Go/No-Go** → 4주 ASG(조기경보) → G2는 그대로 |

---

## 타임라인 (기다리기만 하지 않음)

```
Week 1–2   C-1 backtest timebox → Go / No-Go (무결론=No-Go)
Week 4     ASG — 정성 6항 (`06` §ASG) · CAGR 판정 금지
Week 8+    G2 근접 여부 (≥56일·trades>30) — 상품화 게이트
```

---

## 1. 일괄 배포

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo ./update_factory.sh
```

## 2. north star 일일 cron

```bash
TZ=Asia/Seoul bash ./factory.sh --north-star-digest daily
```

## 3. Claude Pro

C-1 Handoff — **2주 backtest + 무결론 No-Go** 본문 포함

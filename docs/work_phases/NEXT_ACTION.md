# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **S5-HARNESS-SCOPE-01** (VPS 실측) |
| **status** | `WAIT_DIRECTOR` — SSH publickey 거부 · VPS는 디렉터 실행 |
| **git** | origin **`dc90e39`** (push 완료) |
| **앵커** | `SYNC-2026-08-17-P` |

---

## 디렉터 — 지금 할 일

1. **VPS SSH**(이 Cursor PC 키 아님)로 접속.
2. `update_factory` → HEAD=`dc90e39` 확인.
3. 아래 CLI 1회 실행 후 JSON 요약 회신.

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
sudo ./update_factory.sh
git rev-parse --short HEAD
python3 scripts/run_s5_defense_contribution_report.py --start 2026-08-17 --as-of 20260817
```

산출: `reports/s5_defense/s5_contribution_20260817.json`

---

## §OPS-01 — Done

- `0efc750` 관측 PASS 유지 · 이제 목표 HEAD **`dc90e39`**

---

## BEAR / SIDE / BULL

- 동결 · 재시도 금지

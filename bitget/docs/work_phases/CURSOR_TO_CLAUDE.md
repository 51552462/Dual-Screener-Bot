# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-08-02

---

## sub-phase

**L-1** — Log Rotation (P0-1) · **Claude OK 대기**

---

## 3줄 요약

1. **deploy SSOT**: `install_bitget_logrotate.sh` — logrotate for `BITGET_LOG_DIR` + daily `dante-bitget-journal-vacuum.timer`.
2. **무영향**: execution_safety / ledger / tail_risk_gate **미수정**; `disk_manager` stamped TTL env only.
3. **테스트**: `test_log_rotation_l1.py` 7 passed · 서버 `--test` = `logrotate -d` + vacuum dry-run.

---

## Handoff 완료 기준

- [x] logrotate template (stamped + bitget.log safety net)
- [x] journal vacuum script + systemd timer (all `dante-bitget-*` units reported)
- [x] env keys in `bitget.env.example` (defaults: 400M / 30d / stamped 14d)
- [x] `deploy_bitget_factory.sh` install hook + chmod
- [x] trading path untouched
- [x] 05 L-1 섹션 · 00 SSOT · 06 L-1 1단계

---

## 서버 검증 (디렉터)

```bash
sudo INSTALL_ROOT=/path/to/repo bitget/deploy/install_bitget_logrotate.sh --test
systemctl list-timers dante-bitget-journal-vacuum.timer
```

---

## Claude OK 한 줄

```
Claude OK: 2026-08-02 — 스펙 일치, 거래 경로 미접촉 확인. heartbeat=SQLite ops_events — journal vacuum 무관(비차단 확인).
```

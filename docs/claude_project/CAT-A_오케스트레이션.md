# CAT-A · 오케스트레이션 & 스케줄링

> **위험도** 🟡 Medium · **Tier T2** · **also_load**: MAP, KR-US(스케줄), CONSTANTS  
> **never_with**: CAT-B schema, CAT-L deploy paths 동시 설계

---

## 1. 역할

24/7 데몬 + cron factory 파이프라인. mode→step 순서, 글로벌 락, 세션 게이트, US ET 슬롯 디스패치.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| mode→StepSpec | `factory_pipelines.py` |
| flock·dispatch·zombie guard | `factory_runtime.py` |
| 마스터 진입 | `system_auto_pilot.py`, `factory.sh` |
| 슬롯 수학 | `factory_scan_schedule.py` |
| US ET dispatcher | `factory_slot_dispatcher.py` |
| 세션 | `market_session_gate.py`, `session_deduplication_guard.py` |
| watchdog | `watchdog.py` |

---

## 3. 두 스케줄러 (핵심 개념)

1. **데몬** `system_main_loop()` — KST, 30s loop, heartbeat, **비블로킹** 위성 subprocess (HTC·toxic·limit_up 등)
2. **Factory cron** `factory.sh --mode` — **동기**, `.factory_runtime.lock` 직렬, scan/audit/report

둘 다 **같은 글로벌 락** 공유 (OOM 4GB 서버 가드).

---

## 4. FACTORY_MODES

**장중 scan** (KR 8 / US 9 slots):  
`scan_{kr|us}_{supernova|nulrim|dante|ema5|bowl|master(kr only)|*_r2}`

**일일**: `daily_audit_kr`, `daily_audit_us`, `data_refresh`, `smart_money_refresh`, `limit_up_forensics`, `doomsday_radar`

**주간**: `weekly_master`, `monthly_master`

---

## 5. KR 장중 스캔 (KST, 월–금)

| KST | mode | scanner | prelude |
|-----|------|---------|---------|
| 10:00 | scan_kr_supernova | 초신성 | full |
| 10:50 | scan_kr_nulrim | 눌림목 | none |
| 11:40 | scan_kr_dante | 역매공파 | none |
| 12:30 | scan_kr_ema5 | 5일선 | none |
| 13:20 | scan_kr_master | 마스터 | none |
| 14:10 | scan_kr_bowl | 밥그릇 | doomsday tail |
| 15:00 | scan_kr_supernova_r2 | 초신성 2차 | light |
| 15:20 | scan_kr_nulrim_r2 | 눌림 2차 | none |

## 6. US 장중 (ET, dispatcher)

| ET | mode | notes |
|----|------|-------|
| 10:00–15:55 | scan_us_* | KR과 유사, master 없음, r2 더 늦음 |
| cron | `*/5` KST 22–23, 0–6 | `factory_slot_dispatcher --market US` |

---

## 7. 동시성 정책 (Claude 설계 대상)

| 메커니즘 | 정책 |
|----------|------|
| `.factory_runtime.lock` | 단일 flock; stale: dead PID / orphan >60s / age>7200s |
| zombie pipeline | `critical=True` step fail → **잔여 step 전부 skip** |
| session skip L1 | pre-lock: 장 닫힘 → SKIPPED_SESSION |
| session skip L2 | runtime: KR 09:00–15:30 / US 09:30–16:00 |
| SessionDedup | OPEN book 있을 때만 재스캔 차단 |
| lock timeout | daily_audit 7200s, scan 3300s, else 120s |

**override**: `FACTORY_FORCE_SCAN_OUTSIDE_SESSION=1`

---

## 8. CLI (`run_factory_cli`)

| flag | effect |
|------|--------|
| `--mode` | 1회 pipeline |
| `--daemon` | 24h loop |
| `--run-autonomous-analysis-only` | 주말 CAT-G 두뇌수술 |
| `--dry-run` | step list only |
| `--skip-telegram` | 알림 off |

---

## 9. 데몬 위성 (KST, 비블로킹) — CAT-H/I 연동

| KST | tag | module |
|-----|-----|--------|
| 토 00:00 | synthetic_lab | synthetic_data_generator |
| 토 02:00 | incubator | incubator_engine |
| 토 03:00 | mutant_oos | mutant_oos_validator |
| 일 04:00 | alpha_mining | alpha_mining_orchestrator |
| 매일 | limit_up, toxic, doomsday, forensics | 각 hunter |

---

## 10. Prelude 체인 (설계 시 순서 중요)

- **US scan prelude**: meta sync → US health gate → repair → incremental → toxic-ml sync
- **KR scan prelude**: meta sync → kr fluid health → spillover prerequisite
- **daily_audit_us**: health gate → fluid → repair → post-incremental upstream

Prelude 실패 정책: `critical=False`면 scan 진행, `critical=True`면 zombie guard.

---

## 11. Claude 설계 범위

- 새 factory mode 추가 시 step 순서·critical flag
- 슬롯 간격·Cycle-2 종가 margin 변경
- session/dedup 정책
- KR cron vs US dispatcher 분리 유지

## 12. Cursor 구현 (Claude 금지)

- flock 구현, subprocess spawn, systemd unit, crontab literal

---

## 13. Outbound 인터페이스

- `_step_*` → CAT-C scans, CAT-J reports, CAT-G meta_governor_sync
- heartbeat → `ops_logger` → CAT-L watchdog

*상수: CAT-CONSTANTS · KR/US cron: CAT-KR-US*

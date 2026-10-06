# CAT-L · 인프라 & 배포 (Bitget)

> **위험도** 🟡 Medium · **Tier T2 (ops only)** · Claude: 정책만 / Cursor: 셸·systemd

---

## 1. 역할

systemd units, cron, venv, snapshot, watchdog, backup, log rotation, resource limits, Ubuntu deploy.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| deploy wrapper | `deploy/bitget.sh`, `deploy_bitget_factory.sh` |
| update script | `deploy/update_bitget.sh` |
| systemd templates | `deploy/systemd/` (dante-bitget-*) |
| runtime lock | `infra/runtime.py` |
| ops logger | `ops_logger.py` (Bitget) |
| logging setup | `infra/logging_setup` (fix: doc 12) |
| **cron SSOT** | `deploy/generate_bitget_crontab.py` → `/etc/cron.d/dual-screener-bitget` |
| disk / log | P0-1 target — logrotate / RotatingFileHandler |
| RUNBOOK | `bitget/RUNBOOK.md` |

`/etc/cron.d/dual-screener-bitget`의 SSOT는 `generate_bitget_crontab.py`. 서버 수동 편집 금지(편집하면 다음 설치에서 소실).

**CAT-L-FENCE-03 drift guard** (주석 마커 + `body-sha256`. 이 장치는 2026-09-29 cron 펜스 소실(FENCE-02)과 같은 패턴을 차단 시점에 잡기 위함.):
- 상태 4종: `ABSENT` / `PRISTINE`(마커=현재 본문 해시) / `DRIFTED`(수동 편집) / `UNMARKED`(마커 없음).
- 설치기: `DRIFTED` 또는 `UNMARKED`+생성≠라이브 → **차단 exit 3**. 해결: ① 수동 편집을 생성기에 반영·커밋 후 재실행 ② `--force-overwrite-drift`. 덮어쓰기 전 백업 `/var/backups/bitget-cron/dual-screener-bitget.<UTC>`.
- 읽기전용: `python bitget/deploy/generate_bitget_crontab.py --diff-live` (무인자 시 LIVE=`/etc/cron.d/dual-screener-bitget`). 종료 0=동일 · 10=PRISTINE+diff · 20=DRIFTED · 30=UNMARKED+diff · 40=ABSENT · 2=읽기 실패. `update_bitget.sh`는 pull 전에 20/30/2면 중단.

---

## 3. systemd Topology

| unit | role |
|------|------|
| dante-bitget-factory | `bitget_auto_pilot --daemon` |
| dante-bitget-ws | WebSocket |
| dante-bitget-async | Telegram queue |
| dante-bitget-dashboard / heatmap | Streamlit :8511/:8512 |
| dante-bitget-watchdog.timer | 5m heartbeat |
| dante-bitget-snapshot.timer | 5m CQRS |

---

## 4. Watchdog (CAT-A link)

- every 5m: `ops_events` heartbeat.tick
- miss 100s × 3 → telegram + restart **factory only**
- **Gap P1-7**: WS / async / queue-worker not auto-restarted

---

## 5. Backup

| | 상태 |
|---|------|
| 5m CQRS snapshot | ✅ |
| institutional_db_backup (integrity) | exists, **cron 미연결** P0-5 |
| PRAGMA integrity_check | in institutional backup |

---

## 6. Log Rotation (P0-1 · Critical for 1yr unmanned)

- **현행**: unlimited timestamp log files → disk exhaustion
- **목표**: logrotate or RotatingFileHandler + `disk_manager` hook
- **L-1 (2026-08-02)**: `install_bitget_logrotate.sh` + journal vacuum timer — see `05` L-1

---

## 7. Environment

| var | role |
|-----|------|
| BITGET_ROOT | package root |
| BITGET_DB_STORAGE_PATH | prod data separation |
| BITGET_PIPELINE_SSOT | cutover flag |
| PYTHONPATH | must include repo root (root imports) |

---

## 8. Claude 설계 대상

- P0-1 log rotation policy
- P0-5 backup cron spec
- P1-7 watchdog extension matrix
- Windows dev vs Ubuntu prod matrix (`07_phase8`)

---

## 9. Resource Limits

4GB OOM motivation — global flock (CAT-A), MemoryMax in systemd

---

## 운영 규칙 — 서버 실행 스크립트 공개 의무 (2026-09-30 신설)

Handoff에 명시되지 않은 스크립트·wrapper를 Bot-2에서 실행할 경우, 실행과 동시에(사후 요약 아님) 그 원문을 OUTBOX에 포함한다. 결과 요약만으로 실행 사실을 대체할 수 없다. 위반 시 해당 검증 결과는 원문 확인 전까지 잠정 상태로만 인정된다.

## 운영 규칙 — 서버 실행 경로 (2026-10-01, 공개 의무 규칙 보강)

> 출처: `CLAUDE_TO_CURSOR.md` 2026-10-01 Handoff §5 문구 그대로. **디렉터 D4 승인 (2026-10-01)** — 6개 규칙 승인 + 아래 보충 1줄.
> **보충(디렉터 지시)**: Handoff에 적힌 명령은 그대로 실행하되, Cursor(또는 Claude)가 더 나은 방향·아이디어·구현을 보면 **의견으로 제시하고 반영 여부는 Handoff(파일)에서 확정**한다. 의견은 OUTBOX/Handoff에 적으며, 명령을 조용히 바꿔 실행하지 않는다(규칙 1(a)와 병행).

1. Bot-2에서 실행 가능한 것은 둘뿐:
   (a) Handoff(`CLAUDE_TO_CURSOR.md`)에 원문으로 적힌 명령·블록 — 내용 바이트 동일. 한 줄이라도 고쳤으면 고친 줄을 OUTBOX에.
   (b) 커밋·push되어 서버에 pull된 스크립트를 **서버 디스크에서** 실행 — 실행 직전 `git rev-parse HEAD` + `sha256sum <스크립트>` 출력을 OUTBOX에.
2. 로컬 미커밋 파일을 ssh로 파이프해 실행 금지 — 실행본 증명 불가, CR 혼입 실증(2026-09-30 `$'\r'`).
3. 상태를 바꾸는 명령(pull/merge · 설치기 · restart · `.env`/DB/state 파일 쓰기 · `bitget.sh --start-parallel` 등)에 `timeout`을 씌우지 않는다. 오래 걸리면 끊지 말고 원인(락 대기 등)부터 회신.
4. 서버 코드 이동은 두 경로만: 전체 배포 = `update_bitget.sh`(pre-pull `--diff-live` 내장) / 코드만 이동 = 아래 표준 pull 레시피. **맨 `git pull` 금지.**
   - 주의(2026-10-01): `update_bitget.sh`는 서버 작업트리의 tracked 변경을 `git restore .`로 경고 없이 버린다(2026-09-28 실증; 코드 `update_bitget.sh:206–215` — 경고 1줄만 출력, diff 보관 없음). 서버에서 직접 고친 파일이 있으면 먼저 저장소로 옮긴 뒤 실행.
5. 진단용 일회성 실행에서 `.env` 전체 source 금지 — 필요한 키만 grep. 러너와 같은 env 조건이 필요하면 러너 경로(`runner --mode … --skip-telegram`)를 Handoff에 명시.
6. (Claude 의무) 서버 명령이 들어간 Handoff는 요약이 아니라 **전문**을 `CLAUDE_TO_CURSOR.md`에 둔다. `Downloads/` 원문만 있는 상태 금지.
7. (2026-10-01 디렉터 보충) Handoff 명령은 그대로 실행한다. 더 나은 방법·위험·대안이 보이면 **실행 전** OUTBOX에 '의견'으로 제시하고, 반영 결정 뒤에 바꾼다. 조용한 변경 금지. 실행 **수단만** 바꾸고 내용 바이트가 같다면 해시 증명과 함께 같은 회신에서 공개하면 된다.
8. (2026-10-06 신설 · 즉시 시행) 서버에서 journal·로그·환경 관련 출력을 내는 모든 Handoff 블록은 **서버 측에서** 최종 출력 직전에 토큰 패턴(`[0-9]{6,}:[A-Za-z0-9_-]{30,}`)을 가린다(블록 전체를 `{ …; } 2>&1 | sed -E 's#[0-9]{6,}:[A-Za-z0-9_-]{30,}#[REDACTED]#g'`로 감싼다). 로컬 원문은 커밋 금지·작업 종료 시 삭제. 새 토큰·키는 어떤 채팅·Handoff·OUTBOX에도 붙이지 않는다.

실행 수단 표준: 커밋된 Handoff blob에서 블록을 바이트 그대로 추출 → 줄 수·CR 0·sha256 공개 → ssh stdin(바이너리) `bash -s`. 도구 `vb_run.py`(로컬 전용, 서버 미배포, `bitget/docs/work_phases/tools/vb_run.py`).

**표준 pull 레시피** (코드만 이동 — 설치기·재시작 없음. 이 레시피도 Handoff 인라인으로만 실행):

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}" || exit 1
EXPECT=<Handoff가 지정한 40자 전체 SHA>
[ -z "$(git -c core.fileMode=false status --porcelain --untracked-files=no)" ] || { echo "STOP: worktree dirty"; exit 1; }
PYTHONPATH="$PWD" python3 bitget/deploy/generate_bitget_crontab.py --diff-live; R=$?; [ "$R" -eq 0 ] || { echo "STOP: pre diff-live RC=$R"; exit 1; }
git fetch origin || { echo "STOP: fetch"; exit 1; }
U="$(git rev-parse '@{u}')"; [ "$U" = "$EXPECT" ] || { echo "STOP: upstream=$U expected=$EXPECT"; exit 1; }
git merge --ff-only "$EXPECT" || { echo "STOP: merge"; exit 1; }
[ "$(git rev-parse HEAD)" = "$EXPECT" ] || { echo "STOP: HEAD mismatch"; exit 1; }
PYTHONPATH="$PWD" python3 bitget/deploy/generate_bitget_crontab.py --diff-live; echo "POST_DIFF_LIVE_RC=$?"
```

- 핵심: **고정(SHA 비교)이 이동보다 먼저**, 비교는 전체 SHA.
- `POST_DIFF_LIVE_RC`가 0이 아니면(생성기 출력이 바뀐 pull) 설치하지 말고 보고 — 설치는 별도 Handoff.
- 재시작이 필요한 변경이면 이 레시피가 아니라 `update_bitget.sh` 경로 + 별도 Handoff.

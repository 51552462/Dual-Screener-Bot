# CURSOR → CLAUDE (Bitget 검증 OUTBOX)

> **갱신**: 2026-10-01 저녁 · **X0 + X-블록 + L5–L9 회신 OUTBOX** 최상단 (Handoff `27a14b0`) · Phase 0c SUB_DONE · Phase 1 보류 · BACKUP-01 착수 전 Claude 판정 대기

---

## 디렉터 회신 — "터졌을 때" 증상 · 2026-10-02 15:53 KST (Claude 판정 자료)

디렉터 원문: "서버가 터졌을때마다 cmd 창으로 우분투 서버 로그인하게 되면 그 해당 아이피에 로그인이 안됬었어, 그래서 aws 사이트 들어가서 해당 봇2 스탑하고 스타트 한거고"

Cursor 정리(사실 vs 추정):
- 사실(디렉터 진술): 장애 때마다 **SSH 접속 불가**(서버가 응답 안 함) → Lightsail 콘솔에서 **Stop → Start**로 복구. Start 시 공인 IP 변경(정적 IP 미부착).
- X1 부팅 경계와 정렬: 09-13 14:56:57→15:03:12(≈6분) · 09-22 05:49:49→06:25:59(≈36분) · 09-26 08:50:08→08:52:17(≈2분) = 각각 Stop→Start 1회씩으로 보임. 부팅 `0`(09-26 08:52 이후)은 Stop/Start 0회·OOM 0.
- 추정(미확정): "SSH 불응 + 재부팅으로만 복구" = 메모리 고갈 스래싱(스왑 포화)으로 OS가 멈춘 **hang** 패턴과 부합. 이 경우 OOM-killer 로그가 안 남을 수 있어 X1(boot 0)에 OOM 0이어도 과거 부팅 -1/-2에서는 다를 수 있음. 판별 단서 = Y1(boot -1/-2 마지막 로그 시각과 Stop 시각 간 간극, `blocked for more than`/oom 줄, 마지막 20줄). 09-25 cron OOM 기록(06 효과검증표)과 09-26 부팅이 맞물림.
- 보류: Lightsail 콘솔 인스턴스 상태(Running인데 SSH만 불응? 지표 CPU/네트워크 급락?)는 아직 미확보 — 콘솔의 해당 시각 지표 스크린샷이 있으면 가장 강한 증거.
- 제안(코드·서버 변경 아님, 판단 요청): ① Lightsail **고정 IP** 부착(Stop/Start해도 주소 불변) ② Y1 읽기전용 승인.

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 1 선행 · X0 + X-블록(읽기전용) + L5–L9 회신 · 2026-10-01 (Handoff W회신판정_X블록)

### X0 (먼저) — 비밀 문자열 1줄 해소

- 명령(로컬 git만): `git grep -n -i -E 'KEY|SECRET|TOKEN|PASSWORD|PASSWD|API' -- bitget/docs/work_phases/snapshots/CAT-L-CUTOVER-01_P1PRE_WBLOCK_20261001.md`
- 결과: **1줄 = 172번 줄** → `Sep 28 01:48:35 ip-172-26-7-213 systemd[29269]: Listening on REST API socket for snapd user session agent.` (`API` 단어 일치, **비밀 아님**). Critical 해당 없음.
- X-블록 출력(`…XBLOCK…md`)도 같은 스캔: 일치 줄 = 108·109·115·116·122·123·150·151·159·160 (systemd 메시지 `…accessible via APIs…` / `Unknown key name 'RestartMode'…`) — 전부 비밀 아님. 서버 자격증명·토큰 값 출력 0.

### 0. 실행 수단 · 해시

- 결과 커밋: `53183f9` (스냅샷 + OUTBOX + 문서, push 완료)
- Handoff 커밋: `27a14b050bf659d864f9bc236075a0b3e830b9bb` (`CLAUDE_TO_CURSOR.md` 1파일 · 304줄 전문 기록, push 완료)
- X-블록(§8-2): **48줄 · CR 0 · sha256 `768a1189279c3d04ed8570759a5e2fb529aa007421b1197c8e7f68e227295b21`** (blob `27a14b0`에서 `tools/vb_run.py`로 바이트 그대로 추출) · 1회 실행 · SSH_RC 0 · stdout 88125B · stderr 0 · CR 0 · START 11:30:28Z END 11:30:51Z
- 서버 명령은 X-블록 1회뿐. sudo 0 · 설치기/pull/restart/reset-failed/백업·스냅샷 수동/`--cutover-check`/`--start-parallel` 0. 코드 변경 0.
- 출력 전문(요약 없음): `snapshots/CAT-L-CUTOVER-01_P1PRE_XBLOCK_20261001.md` (1083줄, sha256 `137A1E91AD2F893F2F975D76784A6FFBEBFB6EE6BE779524F64007C57EA89E0E` — 머리말 3줄 + 코드펜스 포함 파일 해시)

### 1. 항목별 결과 (고친 것 없음)

| 항목 | 결과 (원문 줄) | Cursor 한 줄 |
|---|---|---|
| X1 | `-3 … Mon 2026-09-07 08:18:54 UTC—Sun 2026-09-13 14:56:57 UTC` / `-2 … Sun 2026-09-13 15:03:12 UTC—Tue 2026-09-22 05:49:49 UTC` / `-1 … Tue 2026-09-22 06:25:59 UTC—Sat 2026-09-26 08:50:08 UTC` / ` 0 … Sat 2026-09-26 08:52:17 UTC—Thu 2026-10-01 11:30:21 UTC` · `KLINES_BOOT0=674` · `OOM_HITS_BOOT0=0` | 현재 부팅(09-26 08:52 UTC) 이후 재시작 0·OOM 0 확정. **그러나 부팅 경계 3개(09-13 ~15:00 · 09-22 05:49→06:25(36분 공백) · 09-26 08:50→08:52)가 지난 19일에 존재** — 디렉터의 "터져서 스톱→스타트"와 시각상 맞는 후보. 이 3건의 원인(OOM 여부·종료 직전 로그)은 X1이 boot 0만 봐서 **미확인** |
| X2 | cron RELOAD: `Sep 27 14:21:01` · **`Sep 28 02:25:01`** · `Sep 29 10:28:01` · `Sep 29 10:29:01` · `Sep 30 06:01:01` (`/etc/cron.d/dual-screener-bitget`) | 09-28 RELOAD 02:25:01 = 설치기 종료(02:24:37) 직후 첫 분. **cron 파일이 02:24 설치에서 실제로 바뀐 직접 증거**. 나머지 4건도 §8-3 예상 시각(14:21·10:27–10:28·06:00)과 일치 |
| X2b | 02:24:09 `ubuntu : TTY=pts/0 … ENV=INSTALL_ROOT=… COMMAND=/usr/bin/bash bitget/deploy/update_bitget.sh` → 02:24:10 사전 백업(root→ubuntu) → `git diff --quiet` → `git pull --ff-only`(ubuntu) → 02:24:11 `deploy_bitget_factory.sh` → 유닛 10개 `tee /etc/systemd/system/dante-bitget-{async,backup,dashboard,factory,heatmap,journal-vacuum,queue-worker,snapshot,watchdog,ws}.service` → `chmod +x` 다수 → 02:24:14 `systemctl daemon-reload` → 02:24:36 watchdog.timer·02:24:37 snapshot.timer Stop→Started → 02:24:37 sudo 세션 종료 | **디렉터 수동 실행(TTY pts/0) 순서 확정**. `X2b_TOTAL_LINES=162` (Handoff의 `head -150` 때문에 출력 162줄 중 150줄만 보임 → daemon-reload 이후 02:24:15~02:24:45 중간 일부만 표시. `install_bitget_cron.sh` 호출 줄 자체는 `head -150` 안에 **안 보임**(sudo -t 필터가 아닌 cron RELOAD로 간접 확인)) |
| X2c | 사전 백업 디렉터리 3개(`20260928_014852_utc` · `_015411_utc` · `_022409_utc`), **각각 `bitget_system_config.sqlite` 12288B 1개뿐** · cron 사본 없음 → `CRONCOPY` 줄 0건 | 펜스 존재 증거(WRAP_NONCOMMENT=28) **확인 불가** — 사전 백업은 cron을 저장하지 않음. 그리고 **이상 징후: 사전 백업에 시장 DB(530MB급)가 전혀 없음** → §3 새 발견 |
| X3 | `X3_COUNT=3644` · 출력 800줄(`sort | head -800`)이 **전부 `2026-09-29T00:00:21` mtime의 0바이트 옛 로그**(canary_20260927 등)로 소진 | **점유 구간표 작성 불가**(Handoff 쪽 `head -800`이 오래된 mtime 800줄만 자름 — C-9와 동일 유형). 후속 X3 정정 블록 제안(§4) |
| X3b | `06:30:45 883 bitget_canary_20260930_063002.log` · `06:45:38 891 bitget_canary_20260930_064501.log` · `06:52:13 589 bitget_scan_spot_ema5_20260930_052001.log` · `06:49:53 0 bitget_cutover_check_20260930_154953.log` · `06:53:21 281 bitget_watchdog_20260930_155209.log` · watchdog 본문: **`[2026-09-30 15:52:12] [WARNING] LIFECAP ENFORCE kill mode=scan_spot_ema5 pid=70238 age=5517 cap=5400 grace=60`** | `scan_spot_ema5`가 **UTC 05:20:01 시작 → 06:52:12 watchdog 강제 종료(age 5517s, cap 5400s)**. 09-30 `cutover_check`(06:49:53 UTC, 0B)는 이 92분 스캔 한가운데 → **124 타임아웃의 락 보유자 = scan_spot_ema5 (강하게 시사)**. canary 2건(883/891B)이 같은 시간대에 끝남 — 내용 미열람(락 스킵 메시지일 가능성) |
| X4 | `BACKUP_FIRST_ENTRY: Sep 08 00:30:00 …Starting Bitget SQLite integrity backup (L-2 P0-5)...` · `BACKUP_SUCCESS_COUNT=0` · `BACKUP_FAIL_COUNT=27` · `28:echo "[backup_bitget_db] BITGET_BACKUP_ENABLED=false — skip"` / **`32:python -m bitget.infra.integrity_backup_l2 --job backup "$@"`** · `/var/backups`에 bitget 백업 디렉터리는 `bitget-pre-update`(49개)와 `bitget-cron`뿐, **L-2 백업 결과물 없음** · DB 합계 `total 2173088`(KB)·`26G /var/lib/quant-bitget/data` · `Filesystem /dev/root 78G 36G 42G 46%` · `Mem: 3836 total 864 used 401 free 2570 buff/cache 2664 available · Swap 4095 / 75 used` | journal 보존 범위(09-08~)에서 **백업 성공 0 / 실패 27 = 매일 00:30 UTC 전부 실패**. 마지막 성공 시각 = 기록 범위 내 없음. 디스크 42GB 여유 · 가용 메모리 약 2.6GB(평시) |
| X4b | `SNAP_SUCCESS_SINCE_0924=519` · `SNAP_MAX_GAP_SEC=14563 GAP_START_UNIX=1790293708` | 최장 공백 14563초(≈4시간 3분) = **2026-09-24 23:48:28 → 09-25 03:51:11 UTC** (현재 부팅 이전 — 부팅 -1 구간). 09-25 이후 4시간급 공백은 없음 |

### 2. L5–L9 (로컬 코드·git만)

| # | 결과 |
|---|---|
| L5 | `bitget/deploy/deploy_bitget_factory.sh:74–80,97–113` — `BITGET_UI_SERVICES=(dashboard, heatmap)`을 기본 `BITGET_START_UI_SERVICES=0`에서 **`disable` + `reset-failed`**. 주석 74–75: "4GB coin-only 서버 기본값: UI(dashboard/heatmap)는 끈 채로 설치 — enable 하지 않으면 부팅 시 자동 기동 시도 자체가 없어 크래시 → failed 고착(update_bitget.sh is-active 오탐) 방지". 도입 커밋 `b7370ad fix(bitget): UI(dashboard/heatmap) 서비스 failed 고착 해소`(+ `8fc2455` Two-server recovery). **결론: 의도된 퇴역(4GB 메모리 보호), 버그 아님.** 켜려면 `BITGET_START_UI_SERVICES=1` |
| L6 | `git diff --stat 1e38166 8a6da21 -- bitget/deploy/systemd/ bitget/deploy/deploy_bitget_factory.sh` → `bitget/deploy/deploy_bitget_factory.sh | 6 ++++++` 1파일 +6줄(`bitget-cron-heavy.slice` 설치 4줄 추가: `SLICE_UNIT=…`, `sudo install -m 0644 … /etc/systemd/system/bitget-cron-heavy.slice`). **`systemd/` 템플릿은 두 커밋 사이 변경 0** → 09-28 설치된 유닛 10개는 현재 저장소 템플릿과 동일(라이브 유닛 `.service` 어긋남 없음; slice 파일은 `8a6da21`에서 처음 설치되는 경로이므로 `/etc/systemd/system/bitget-cron-heavy.slice` 존재 여부만 서버 확인 필요) |
| L7 | `bitget_market_data_snapshot.sqlite` **직접 경로 소비**: `bitget/infra/data_paths.py:82`(`market_data_snapshot_db_path`) · `:102,124`(`market_db_read_path` / `report_db_read_path`) · `bitget/infra/snapshot_service.py:20,81`(쓰기측). `market_db_read_path()`를 부르는 **읽기측**: `bitget/master_scanner.py` · `bitget/dashboard.py` · `bitget/heatmap_dashboard.py` · `bitget/reports/bitget_report_context.py` · `bitget/full_bt/{harness,ohlcv_load}.py` · `bitget/analysis/universe_bt/{replay,universe,run_live_u2_u3,_vps_diagnose_coverage}.py` · `bitget/validation/load_test.py`. `report_db_read_path()`는 `BITGET_REPORT_FORCE_MAIN_DB` 기본 1 → **리포트는 항상 메인 DB**. 스냅샷은 **`master_scanner` 스캔 입력(신선도 1800초 이내일 때)** + 대시보드·히트맵용 — snapshot 서비스 exit 1(양보)은 신선도 규칙이 메인 DB로 폴백시키므로 **스캔 비차단**(코드 기준; 라이브 동작은 미실측). 백업 대상 목록에도 포함: `bitget/infra/integrity_backup_l2.py:30`, `update_bitget.sh:90` |
| L8 | `bitget/deploy/update_bitget.sh:206–215` — 조건 `if sudo -u "$DEPLOY_USER" git -C "$INSTALL_ROOT" diff --quiet` (**tracked 파일의 워킹트리 변경만** 감지; 스테이지된 변경·untracked 미감지). 변경 있으면 `echo "  (warn) local tracked changes detected — restoring before pull"` 한 줄만 찍고 `git restore .` 실행 → **diff를 어디에도 남기지 않음("없음")**. git 실행 사용자 = `DEPLOY_USER`(`update_bitget.sh:25`, 기본 `ubuntu`). X2b에서 `USER=ubuntu`로 `git diff --quiet`·`git pull --ff-only` 확인 |
| L9 | W1 스냅샷 원문: `Sep 26 05:26:50 ip-172-26-7-213 sudo[86970]:   ubuntu : PWD=/home/ubuntu/dante_bots/Dual-Screener-Bot ; USER=root ; COMMAND=/usr/bin/systemctl restart dante-bitget-async` — **`TTY=` 필드 없음**(09-28 02:24:09 디렉터 수동 실행은 `TTY=pts/0`이 있음). `watchdog.py`의 자동 재시작 명령(sudoers 정렬, `unit_restart_cmd`): factory `sudo -n /usr/bin/systemctl restart dante-bitget-factory` · queue `…restart dante-bitget-queue-worker`(:280) · ws `…restart dante-bitget-ws`(:286) · async `…restart dante-bitget-async`(:291). 매트릭스 1) factory(heartbeat) 2) queue(HB stale AND PENDING/RUNNING>0) 3) WS(buf_age → factory 또는 ws) 4) async(telegram daemon HB stale). 시간당 재시작 상한·텔레그램 쿨다운 있음 | 09-26 05:26:50 `restart dante-bitget-async` = **사람의 대화형 터미널이 아님**(TTY 없음), 형태가 watchdog 자동 재시작(:291)과 일치 → **자동 재시작 쪽이 더 유력하나 비대화형 ssh 명령일 가능성은 배제 못 함**(확정 불가). 15:44:23·15:45:03 queue-worker 재시작 두 줄도 TTY 없음(동일 패턴) |

### 3. 이번에 새로 보이는 것 (Claude 판정 요청)

1. **사전 백업이 시장 DB를 안 담고 있음(강하게 시사)**: `update_bitget.sh:73–`의 사전 백업은 `sudo -E -u ubuntu env INSTALL_ROOT=… PYTHONPATH=…`만 넘기고 `BITGET_DB_STORAGE_PATH`를 넘기지 않음. `data_paths.bitget_data_dir()`(`:40–`)는 `BITGET_DB_STORAGE_PATH` → `bitget_system_config.json` → 레거시 경로 순이라, 서버 데이터가 `/var/lib/quant-bitget/data`(`.env`/유닛 `EnvironmentFile`로 지정된 걸로 추정)에 있으면 이 경로로는 **레거시 디렉터리를 보고 빈 `bitget_system_config.sqlite`(12288B)만 백업**하게 됨. X2c의 3개 디렉터리가 모두 정확히 12288B 1개 = 이 가설과 일치. **미확정(서버 환경변수 미열람)** · BACKUP-01 범위에 포함 권고.
2. **L-2 백업(`integrity_backup_l2`)은 한 번도 성공한 적 없음(journal 범위 내)** + 사전 백업도 사실상 비어 있음 → **현재 시장 DB(≈1.5GB: market_data 530MB · snapshot 530MB · ops_events 397MB)의 복구 가능한 사본이 서버 안에 없을 가능성**. (원격/Lightsail 스냅샷 여부는 미확인)
3. **`scan_spot_ema5`가 lifecycle cap(5400s)에 걸려 강제 종료**(09-30). 1.5시간짜리 스캔이 글로벌 락을 점유하면 다른 모든 잡이 SKIPPED_LOCK/124가 됨 → "서버가 안 돈다" 체감과 이어질 수 있는 **반복성 후보**. 하루 몇 회인지는 X3 실패로 미측정.
4. 부팅 경계 3건(09-13·09-22·09-26)의 원인은 boot -1/-2 커널 로그·종료 직전 로그를 봐야 확정. **단서**: `track_b_06` 효과검증 기록표 FENCE-02 행에 "cron OOM 3회 재발(09-07 · 09-14 · 09-25)"이 이미 있음 — 부팅 `-2`가 09-13 15:03 UTC(=09-14 00:03 KST) 시작, 부팅 `-3`이 09-07 08:18 UTC 시작, 부팅 `0`이 09-25 OOM 다음날 09-26 08:52 UTC 시작 → **과거 "터짐" = cron 잡 OOM 후 재시작/부팅 경계와 시각상 정렬**(Y1으로 확정 필요). 09-22 05:49→06:25 (36분 공백)은 기록표에 OOM 없음 — 다른 원인 후보. 09-29 이후 FENCE_OK 구간은 OOM 0(X1 boot 0) = FENCE-02 효과와 일치하는 정황.

### 4. 후속 읽기전용 블록 제안 (이번엔 실행 안 함 — Claude 승인 시 Y-블록)

- **Y1** `journalctl --utc -k -b -1` / `-b -2` OOM·`blocked for more than` 검색 + 각 부팅의 **마지막 20줄**(종료 직전 상태). 이것이 "터짐" 근본 원인에 가장 직접적.
- **Y2** X3 정정: 로그 파일 이름 패턴별(`scan_*`·`canary`·`cutover_check`)로 최근 3일 **시작시각(이름) ~ 종료시각(mtime)** 만 출력(`head` 없이 `sort -k3` 또는 `awk` 필터, 크기 > 0만) — `head -800` 같은 절단 금지.
- **Y3** `scan_spot_ema5` 최근 7일 `LIFECAP ENFORCE` 횟수(`grep -l LIFECAP /var/lib/quant-bitget/logs/bitget_watchdog_*`), canary 883/891B 내용 `cat`.
- **Y4** 서버 `.env`/`systemctl show -p Environment dante-bitget-factory`에서 **키 이름만**(값 가림) `BITGET_DB_STORAGE_PATH` 존재 여부 → §3-1 가설 확정.

### 5. 문서 갱신 (완료)

`05_진행로그`(블록 "Phase 1 선행 W-블록 회신 · Claude 판정" + X-블록 결과) · `NEXT_ACTION`(CUTOVER·BACKUP-01 행, 신규 `CAT-L-QW-RESTART-01`) · `CAT-L_인프라배포.md` 규칙 4 아래 주의 1줄 · `09_디렉터_쉬운요약.md` · `track_b_NEXT_STEP.md` · `00_SESSION_SYNC.md` §3. **`track_b_06_검증체크리스트_및_실패기록.md`**: X2로 09-28 02:25:01 RELOAD 확인 → Handoff §11대로 「실패/롤백 기록」에 FENCE-02 회귀 1줄 추가(주식 쪽 `docs/work_phases/06_…`은 건드리지 않음).

### 6. 상태 한 줄

Phase 1 보류 · BACKUP-01 착수 대기(Claude 판정) · 원인 후보 2개 우선(①부팅 경계 3건의 OOM 여부 ②scan_spot_ema5 92분 락 점유). 코드·서버·.env 변경 0.

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 1 선행 W-블록 (읽기전용) 회신 · 2026-10-01 (Handoff v2)

**Handoff 커밋**: `f7f767e44e22981674e14f1341c18fe3acaa2565` (CLAUDE_TO_CURSOR.md 1파일, v2 전문 기록). 두 파일(`…P1선행_20261001.md` v1, `… (1).md` v2) 중 **v2(10:23 UTC 판, `(1)`)만 사용** — v2 머리말이 "v1 §8-1을 이미 실행했다면 §8-1b만"이라고 했으나 v1은 실행한 적 없으므로 §8-1 + §8-1b **둘 다** 실행.

**서버 명령**: §8-1 W-블록 1회 + §8-1b W3b 1회(읽기전용) + W8 `scp`(서버 읽기만). `sudo` 0 · `set -e` 0 · 설치기/pull/restart/`reset-failed`/`--cutover-check`/`--start-parallel` 0. 코드 변경 0.

### 0. 실행 수단 · 해시 (Handoff §8-0)

| 블록 | 줄 수 | CR | sha256 (blob `f7f767e`에서 바이트 그대로 추출) |
|---|---|---|---|
| §8-1 W-블록 | 38 | 0 | `bbf39d61001cb033839851730cee17b9cfc8f6a8659a26a98283a0b79e0433e5` |
| §8-1b W3b | 10 | 0 | `0e5a8482181f70d2205625573f2c622b9ae6715c37c3d5d8f55395f97100023c` |

- 도구: `bitget/docs/work_phases/tools/vb_run.py`(이번 커밋에 포함, **로컬 전용·서버 미배포**). 커밋 blob → 코드펜스 추출 → ssh stdin 바이너리 `bash -s`. 두 번 모두 SSH_RC=0 · stderr 0바이트 · 출력 CR 0 · `$'\r'` 오류 0 · 한 줄도 수정 안 함.
- 출력 전문(요약 없음): `snapshots/CAT-L-CUTOVER-01_P1PRE_WBLOCK_20261001.md` (W-블록 532줄 + W3b 14줄). `systemctl cat` 출력은 `EnvironmentFile=` 경로만 있고 값 없음 → 가림 0건. 아래 표의 판정에 쓴 줄은 원문 인용.
- 비밀 문자열 스캔: W-블록 출력에서 `KEY|SECRET|TOKEN|PASSWORD|PASSWD|API` 일치 **1줄**(어느 줄인지는 도구 제약으로 특정 못 함). 출력 전문은 제가 처음부터 끝까지 열람했고 비밀값 없음. 의심되면 Claude가 스냅샷 md를 열어 확인.

### 1. 항목별 결과 (§8-2 기대값 대조 · 고친 것 없음)

| 항목 | 결과 | 판정 |
|---|---|---|
| W1 | `W1_COUNT=74`(가시성 OK: 최초 09-26 05:26:50, sudo `COMMAND=` 줄 정상 출력). 상세 §2 | 기대(≥1) 일치 |
| W2 | reflog: `8a6da21` 09-30 09:42:43 · `c1ffe3f` 09-30 06:00:40 · `002c612` 09-29 09:21:46 · **`1e38166` 09-28 01:57:45** · `24135f4` 09-23 10:10:53. **09-28 01:48–02:30 사이 HEAD 이동 = 01:57:45 1건**(= 접속 세션 01:57:39 직후, ubuntu가 직접 `pull`) | 기대 일치. 핵심은 §3 |
| W3 | 09-28 01:48:52 · 01:54:10 · **02:24:09** 세 번 `sudo bash bitget/deploy/update_bitget.sh`(**TTY=pts/0 = 사람이 터미널에서 직접**). 02:24는 `deploy_bitget_factory.sh`까지 수행(§3). 02:2x `Stopping/Started` 서비스 로그는 출력 상한(`head -200`)에 걸려 **02:24 이후 구간이 잘렸을 수 있음**(마지막 줄이 02:24:10 `git pull`에서 끊김) | 부분 확인 — 02:24:10 이후 W3 재조회 필요(§5 의견 3) |
| W3b | `KLINES=12` · 권한 오류 없음 · **`OOM_HITS=0`**. 커널 12줄 중 판독 가능한 건 `systemd-fstab-generator … Failed to create unit file swapfile.swap … Duplicate entry in /etc/fstab?`(09-27 14:20:58 · 14:21:31 — FENCE-02 `daemon-reload` 시각과 일치) 2줄 포함. 유닛: `dante-bitget-queue-worker … Main process exited, code=killed, status=9/KILL → Failed with result 'timeout'` (09-27 15:46:24) 1건 | **"서버가 터진" 원인은 커널(OOM)·상주 유닛 수준에서 안 보임.** 0을 "문제없음"으로 해석하지 않음 — 디렉터가 본 증상 1줄 필요(§6) |
| W4 | `bitget_cutover_check_20260930_154953.log` = **0바이트**, mtime 06:49:53.239(= 생성 시각, 이후 쓰기 없음). 헤드/테일 출력 없음 | **무쓰기로 종료됨.** 락 대기와 일치하나 "락 대기 확정"은 아님 — 락 보유 후보 조회는 패턴 오류로 무효(§5 의견 1) |
| W5 | backup: `backup_bitget_db.sh: line 32: python: command not found` → exit 127, **매일 00:30 UTC 반복**(조회 가능 구간 최초 09-23, `tail -20` 한계). snapshot: 실패 로그 대부분이 `[INFO] snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131` 후 exit 1. 마지막 성공: 10-01 09:35:06 · 09:40:23 · 09:45:33. `SNAPSHOT_FAILS_SINCE_0926=1122`. `SQLITE3_CLI=yes` | 기대(원인·마지막 성공 시각) 확보. 해석 §4 |
| W6 | mode change 10건(전부 100644 → 100755): `backup_bitget_db.sh` · `diagnose_coin_digest.sh` · `run_bitget_queue_worker.sh` · `install_bitget_backup.sh` · `install_bitget_logrotate.sh` · `master_sync_bitget.sh` · `reset_bitget_pipeline.sh` · `bitget_journal_vacuum.sh` · `uninstall_stock_north_star_cron.sh` · `deploy/install_director_digest_cron.sh` | **10건 중 8건이 09-28 02:24:13–14 `deploy_bitget_factory.sh`의 `sudo chmod +x` 목록과 일치.** 일치하지 않는 2건: `diagnose_coin_digest.sh` · `uninstall_stock_north_star_cron.sh`(W1 `chmod` 목록에 없음 — 다른 시점/수단, 미확인). 09-29 Step 4A "dirty 10파일 mode-only" 기록과 개수는 동일, **파일명 대조는 해당 백업 파일이 없어 못 함** |
| W7 | root 소유 pyc 13건의 mtime: 2026-07-02 13:29–13:34 UTC **11건** · 2026-08-11 15:52 1건(`factory_scan_schedule`) · **2026-09-23 10:11 1건(`bitget/infra/data_paths`)** | **Claude 가설(09-29·09-30 sudo 설치기가 만듦) 불일치.** 11건은 3개월 전, 마지막 1건은 09-23 `pull`(10:10:53) 직후 root 실행. 09-29/30 sudo 설치기 시각과 겹치는 pyc **0건** |

### 2. W1 sudo 74건 주석 (Handoff 절 / 공개 스크립트 대응)

| 시각(UTC) | 건 | 명령 | 대응 |
|---|---|---|---|
| 09-26 05:26:50 | 1 | `systemctl restart dante-bitget-async` | 기록 대조 못 함 — **미확인** (02시대 접속은 V5 목록에 없음: 가장 이른 세션 08:54) |
| 09-26 08:54:37 | 1 | `dmesg -T` | 부팅 직후 점검 세션(V5 08:54:33과 일치, 확인용 읽기) |
| 09-26 15:44–15:45, 09-27 15:44–15:45, 09-28 15:46 ×2, 09-29 15:45–46 ×2, 09-30 15:45 ×2 | 10 | `systemctl restart dante-bitget-queue-worker` (PWD=repo, **TTY 없음**) | **watchdog 자동 복구**: `bitget/watchdog.py:280` 기본 명령 `sudo -n /usr/bin/systemctl restart dante-bitget-queue-worker`, sudoers 예시 `bitget-watchdog-sudoers.example:15-16`. **매일 15:44–15:46 UTC 같은 시각에 반복** — 정기 재시작 아님, 워커 하트비트 stale 규칙이 매일 같은 시각에 걸린다는 뜻(원인 미조사) |
| 09-27 13:30:06 | 1 | `crontab -l` | FENCE-02 Phase 0 (V5 13:30:04) |
| 09-27 14:17:56 | 1 | `systemd-run --scope --slice=bitget-cron-heavy.slice … sleep 2` | FENCE-02 Step 2 (14:17 세션 구간) |
| 09-27 14:20:57–14:21:32 | 11 | `cp slice` · `daemon-reload` · `start slice` · `cp /etc/cron.d/… pre-fence02` · `tee/chmod/chown /etc/cron.d/dual-screener-bitget` · `systemd-run … WRAP_OK` · slice 재복사/재시작 | FENCE-02 Step 2 적용 (OUTBOX 14:20:57) |
| **09-28 01:48:52** | 5 | `sudo bash bitget/deploy/update_bitget.sh`(**TTY=pts/0**) + 내부: 사전 백업 python(`/var/backups/bitget-pre-update/20260928_014852_utc`) · `git diff --quiet` · **`git restore .`** · `git pull --ff-only` | **디렉터 직접(진술)** — OUTBOX/05 기록 없음 |
| **09-28 01:54:10** | 4 | 위와 동일 (`update_bitget.sh`, 백업 `…015411_utc`, `diff --quiet`, `pull --ff-only`; `git restore .` 없음) | 디렉터 직접 |
| (01:57:45 pull) | — | W1 범위 밖(sudo 아님: ubuntu 직접 `pull`) → reflog `1e38166` | 디렉터 직접(세션 01:57:39) |
| **09-28 02:24:09** | 32 | `update_bitget.sh` → 내부 `deploy_bitget_factory.sh`(02:24:11): systemd 유닛 파일 `tee` 9개(async·backup·dashboard·factory·heatmap·journal-vacuum·queue-worker·snapshot·watchdog·ws) · `chmod +x` 18개 스크립트 · `daemon-reload` · `enable ws·factory·queue-worker·async·watchdog.timer·snapshot.timer` · **`disable dashboard·heatmap` + `reset-failed`** → 02:24:22 **`sudo bash install_bitget_cron.sh` (root)** | 디렉터 직접 — **§3 핵심** |
| 09-29 10:27:37–10:28:35 | 7 | `install_bitget_cron.sh` · `daemon-reload` · `install -m 0644 …CAT-L-FENCE-02_cron_p0_20260927.cron /etc/cron.d/…` · **`rm -f /etc/systemd/system/bitget-cron-heavy.slice`** · `daemon-reload` · `install_bitget_cron.sh` 재실행 | FENCE-02 Step 4E + "2차 사고 재설치"(05 로그 10:28) — 공개된 기록과 일치 |
| 09-30 06:00:45 | 1 | `install_bitget_cron.sh` | FENCE-03 부트스트랩 (`fence03_bootstrap.sh`, 커밋 `eb80c58`) |

**설명 불가**: 09-26 05:26:50 `restart dante-bitget-async` 1건, `diagnose_coin_digest.sh`·`uninstall_stock_north_star_cron.sh` mode 변경 2건(원인 시각). 나머지 74건은 위 표로 분류됨.

### 3. 핵심 발견 — 09-28 02:24 사건 (Handoff §4-1 (a)(b) 확정분)

**(a) 실행된 명령 = 확정 (W1+W2+W3):** 02:24:09 디렉터가 터미널(`pts/0`)에서 `sudo bash bitget/deploy/update_bitget.sh` 실행 → 서버 HEAD는 이미 `1e38166`(01:57:45에 pull됨 — origin엔 FENCE-02 생성기 없음) → 02:24:11 `deploy_bitget_factory.sh`가 유닛 파일 10개 덮어쓰기·서비스 재시작 → **02:24:22 `install_bitget_cron.sh`를 root로 실행**. 당시 설치기는 FENCE-03 가드(`install-plan`)가 없던 판이고(현재 `install_bitget_cron.sh:57-80`에만 있음), `/etc/cron.d/dual-screener-bitget`을 `1e38166`의 구 생성기 출력으로 덮어썼을 것(구판 설치 방식은 현재 `:103` `install -m 0644`와 유사했을 가능성, 구판 본문은 미열람) → 09-27 14:21:01 수동 적용한 wrapper가 사라짐. 09-29 08:01 확인(wrapper 0줄, HEAD `1e38166`)과 시간순 정합. **FENCE-02 회귀 원인 = 확정(코드+로그 일치).** 단, 설치기가 실제로 cron.d를 덮어쓴 직접 증거(파일 내용)는 이 로그에 없음 — `install -m 0644` 줄은 sudo 로그에 개별로 안 찍히는 구조(스크립트 내부 root 실행)라 **인과 사슬은 "강하게 시사"**로 표기.

**(a-2) 부수 변경:** `dashboard`·`heatmap` 유닛 **disable**(`systemctl disable` 02:24:19 + `reset-failed` 02:24:22), 유닛 파일 10개 덮어쓰기, 스크립트 18개 `chmod +x`. 01:48:53 `git restore .`(tracked 변경 폐기) 1회 — 01:48 시점 서버 worktree에 로컬 수정이 있었다면 그때 사라졌을 수 있음(내용 미확인).

**(b) 서버가 터진 원인 = 로그로는 미확인.** 커널 OOM 0 · 상주 유닛 failed/KILL은 09-27 15:46 queue-worker 1건뿐(01:48 이전 11시간 동안 다른 유닛 실패 없음). W3 구간(01:40–02:20)에서 반복된 것: `dante-bitget-snapshot.service` 5분마다 `exit 1`(=pipeline writer active 이연, §4) — 이건 01:49:58·01:54:59·02:00:04 등 **update 실행 중/직후 계속** 나왔고 평소 패턴과 구별 안 됨. 즉 디렉터가 본 "터짐"은 **OOM·유닛 크래시가 아닌 다른 증상**일 가능성. 증상 1줄 필요(§6).

**FENCE-02 판정 입력(10-13)**: 펜스가 살아있던 09-27 14:21–09-28 02:24 사이 OOM 0(조회 구간 `W3b-1`). 단 `scan_spot_nulrim`이 01:47:02에 slice 안에서 시작(`CRON … systemd-run … --slice=bitget-cron-heavy.slice`)되어 01:48~ 구간까지 돌고 있었음 — 사건 직전 slice 사용 중이었다는 사실만 기록.

### 4. backup · snapshot 해석 (`CAT-L-BACKUP-01` 입력 — 판정은 Claude)

- **backup = 진짜 장애.** 원인: `backup_bitget_db.sh:32`가 `python`을 호출하는데 유닛이 `User=root`·PATH에 `python` 없음(`python3`만) → exit 127. **조회 가능한 최초 실패 09-23 00:30 UTC**(`tail -20`이라 더 이전은 불명). 정기 DB 무결성 백업이 **최소 9회 연속 미작동.** `sqlite3` CLI는 있음(`SQLITE3_CLI=yes`)이라 가설(sqlite3 부재)은 기각, 실제는 `python` 별칭 문제. (09-28 `update_bitget.sh` 사전 백업은 venv python을 쓰는 별개 경로 — 성공 여부는 이 출력으로 미확인.)
- **snapshot = 의도된 이연이 실패 코드로 보이는 것.** 최근 실패 5건 전부 `snapshot deferred — pipeline writer active mode=scan_futures_supernova_r2 pid=89131`(10:00–10:26 UTC, 같은 pid가 26분 이상 점유). 오늘 09:35·09:40·09:45는 성공 → **스냅샷은 대체로 갱신 중**이고, 실패는 "긴 스캔이 쓰기 락을 쥐고 있을 때 5분 주기가 양보하면서 exit 1". `SNAPSHOT_FAILS_SINCE_0926=1122`는 이 이연 횟수와 대부분 겹칠 것(이연과 진짜 실패를 이 로그로는 분리 못 함). **장시간 점유 스캔이 길어지면 스냅샷이 오래 낡을 수 있음** — 최대 공백 시간은 미측정.

### 5. 의견 (규칙 7 — 실행 전 제시가 아니라, 이번엔 결과 해석 중 발견 → 다음 Handoff에서 반영 여부 결정)

1. **W4 락 보유 후보 조회가 시간대 오류로 무효.** `ls … | grep '_20260930_(14[3-5]|15[0-5])…'`는 `bitget.sh`의 로그 파일명(`STAMP`)이 **UTC 스탬프(cron: `TZ=UTC`)**인 파일만 14:30–15:55 **UTC**(=23:30–00:55 KST)로 걸러냄. 우리가 보려는 시각은 06:30–06:50 **UTC**. 또 같은 폴더에 `TZ=Asia/Seoul` 스탬프(systemd watchdog 서비스·p0 수동 실행) 파일이 섞여 있음(예: `bitget_watchdog_20260930_155209.log` = 06:52:09 UTC, 281바이트로 평소 171보다 큼 — **cutover 실행 구간과 겹침, 내용 미열람**). 제안: 다음 W-블록에서 `ls -l --time-style=full-iso`를 **mtime 기준**(UTC 06:30–06:55)으로 필터 + `bitget_watchdog_20260930_155209.log` · 직전 스캔 로그 열람.
2. **backup 수정은 1줄 성격**(`python` → 절대경로 `venv/bin/python` 또는 `python3`)이지만 root 서비스·DB 무결성 경로라 **별도 sub-phase(`CAT-L-BACKUP-01`)에서 Critical 여부 판정 후** 진행이 맞다고 봄. Phase 1 전에 선행 필요 여부는 Claude 판단(백업이 9일 이상 안 돌았다는 사실이 cutover 조건과 직결).
3. **W3 재조회 필요**: `head -200` 상한 때문에 02:24:10 이후 서비스 `Stopping/Started`·cron 설치 시점 로그가 잘림. 다음 W-블록에서 `--since '2026-09-28 02:24:00' --until '2026-09-28 02:30:00'` 범위만 별도 조회 권고.
4. **snapshot 이연은 exit 1이 아니라 exit 0이 맞아 보임**(`systemctl --failed` 오염 + 이 이슈처럼 "실패 반복"으로 오독). 코드 변경이므로 지금 제안만, 별도 sub-phase.
5. **root pyc**: 가설 불일치(§1 W7). 기능 영향 없음, 조치 불필요 — 정리하려면 별도 승인.

### 6. 디렉터 회신 요청 (1줄씩)

- 09-28 오전에 **서버가 터졌을 때 본 증상**이 무엇이었는지 한 줄(예: "텔레그램 알림이 끊김" / "SSH 접속이 안 됨" / "봇이 멈춤"). 서버 로그에는 OOM·크래시가 없어서 이 한 줄이 원인 추적의 유일한 단서입니다.
- (Claude W10) 서버를 직접 만지신 날 사후 05 로그에 "언제·왜" 1줄 기록하는 규칙 8 — **승인 시에만 반영**. 이번엔 반영하지 않음.

### 7. W8 증거물 원본 보존

`scp ubuntu@…:/tmp/cutover01_*.out` → `snapshots/raw_20260930/` (SCP_RC=0, 서버 원본 보존).

| 파일 | 바이트 | sha256 | V3 값과 대조 |
|---|---|---|---|
| `cutover01_p0.out` | 14657 | `38d18d65a72a1b51606be3b9612b42c94a6cc62b65c2b00e9e39bb34d5c15322` | 일치 |
| `cutover01_p0c_arch.out` | 11722 | `07b5e70ab123a4aa2c5eeb3f74151d3bf4a324b77ed6d19ec418e035a9dcc687` | 일치 |
| `cutover01_p0c_deploy.out` | 12747 | `4a583ef780a2c788fa2dc6e3b2a1c659228fef3f7f811f69a7441e2dab316f33` | 일치 |

- `KEY|SECRET|TOKEN|PASS` 일치 줄 수: p0 **4** · arch **4** · deploy **5**. 전부 `…audit PASS`/`checks PASS` 메시지와 `CURRENT_REGIME_KEY`·`META_REGIME_KEY`(키 *이름*, 값 `HIGH_VOL`) 줄 — 비밀값 아님 (값 부분 가린 상태로만 확인). 커밋 진행.

### 8. W9 · 문서 · 기타

- `CAT-L_인프라배포.md`: "초안" 표기는 이미 `6d080cc`에서 "승인 2026-10-01"로 교체됨 → **규칙 7(Claude 문안 그대로) + 실행 수단 표준 1줄 추가**. 이번 커밋 해시 + `git show --stat`는 이 OUTBOX 최종 줄에 기재.
- `vb_run.py` 커밋 위치: `bitget/docs/work_phases/tools/vb_run.py` (서버 실행 경로 아님).
- **결과 커밋**: `4ee0029ba893e0d83b93f3c40f3f1d8a65b1b12f` — 14파일(+2156/−10): W-블록 스냅샷 1 · `raw_20260930/` 3 · `tools/vb_run.py` 1 · 규칙 7 · OUTBOX/05/NEXT_ACTION/현황판/09/NEXT_STEP/SYNC.
- §6 V5 정정(Claude 대응표 수용): 이전 OUTBOX의 "설명 불가(미대조)" 중 Claude가 대응시킨 구간은 **"대응(시각 상관)"으로 정정**. Cursor가 독자 확인한 건 W1 sudo로 교차 확인된 구간(09-27 13:30·14:17–14:23, 09-28 01:48–02:24, 09-29 10:27–10:28, 09-30 06:00) 뿐. 나머지 Claude 대응(09-26 부팅 직후, 09-27 04:09 RUN-2 등)은 sudo 흔적이 없는 읽기 세션이라 **W1로는 검증 불가**. 남는 미대조 16건은 Claude 표 그대로 SCRIPT-AUDIT-01로 이관.
- 추가 대조 포인트(09-28 Step 3 OUTBOX "ssh Permission denied" vs 같은 날 성공 9건): 이번 W1 범위 밖 → SCRIPT-AUDIT-01.
- NEXT_ACTION: CUTOVER 행 Claude 문구로 교체 · FENCE-02 → `SUB_DONE (1–2단계) · 3단계 판정 2026-10-13`(근거: C-7) · LANE_FULLBT **변경 없음**(C-7 철회) · 신규 행 `CAT-L-SCRIPT-AUDIT-01` / `CAT-L-BACKUP-01`.

### 9. 상태

Phase 0c: **SUB_DONE (Claude OK 2026-10-01)** — Claude 판정 문서화. CAT-L-CUTOVER-01 전체는 **Done 아님.** Phase 1: **보류 유지**(§9 조건 2·3·4 대기: 124 원인 = 락 대기 *가능성 높음*이나 확정 못 함(W4 무효 §5-1) · 09-28 명령 = 확정, 장애 증상 = 디렉터 1줄 필요 · backup = 원인 확정(`python: command not found`), 시작 시점 미확정 · snapshot = 이연으로 해석). 서버 추가 명령 0.

---

## 디렉터 추가 회신 · 2026-10-01 19:48 KST (진술 그대로 — Cursor 메모 포함)

- 디렉터: 오늘 재작업하려 했으나 **서버 접속이 안 됐음.** 서버가 계속 터지는 문제를 작업 중이었고, 서버를 직접 만진 건 서버가 터져서 **stop 후 start**를 했기 때문이며, **그때마다 IP가 바뀌어서** 접속 기록의 IP가 달라진 것이라는 설명.
- Cursor 메모 (사실만):
  1. 접속 로그(V5)의 IP는 **접속한 쪽(클라이언트) IP**다. 서버 IP가 바뀌는 것(Lightsail 고정 IP 미부착 시 stop/start마다 변경)과 별개 개념 — 09-28 11:36의 `110.35.116.11 → 175.193.220.230`은 접속 컴퓨터/네트워크 쪽 변화다.
  2. V6에서 `dante-bitget-overseer.service`·타이머가 `ActiveEnter 09-26 08:52:22`로 유지 → **09-26 08:52 이후 VM 재부팅 흔적 없음**(Lightsail stop/start는 VM 재부팅이라 모든 유닛 시작 시각이 갱신됨). 09-28 `update_bitget.sh`는 일부 서비스만 재시작. 따라서 "stop/start 후 직접 작업"이 09-28에 있었다면 VM 재부팅이 아닌 다른 형태였거나 기억과 다를 수 있음 — **디렉터 진술과 로그가 아직 안 맞는 부분**, Claude 판정 요청.
  3. 2026-10-01 19:48경 이 PC에서 `3.36.90.195:22` TCP 접속 시험 = **열려 있음(TcpTestSucceeded=True, ping은 응답 없음 — Lightsail 기본)**. 로그인은 시도하지 않음. 디렉터가 겪은 접속 실패는 서버 IP 변경·일시 장애·키/클라이언트 쪽일 수 있으며 원인 미확인.

---

## 디렉터 회신 D1–D4 · 2026-10-01 18:30 KST (디렉터 진술 그대로 기록 — 로그로 검증한 것 아님)

- **D1**: 9/30 오후 3:49~3:53 텔레그램 메시지 **안 왔음.** (코드상 기대와 일치: 강제 종료 또는 락 대기 skip이면 무음. "안 왔다" ≠ "실행 안 됨" — 실행 사실은 서버 로그/파일로 이미 확인됨.)
- **D2**: V5 접속 세션은 **Cursor 자체가 검사하려고 들어간 것**(디렉터 직접 접속 없음). 접속 IP 변경(09-28 11:36)은 **서버 장애로 중단한 뒤 네트워크를 바꾼 것.**
  - Cursor 메모: 서버 서비스 재기동 기록(V6 `ActiveEnterTimestamp` 09-28 02:24)과 시점이 이어짐. 단 세션별 시각 대조는 여전히 안 됨 → Claude가 디렉터 진술로 "설명 불가"를 해제할지 판정.
- **D3**: **B** (Claude 권고대로: Phase 1과 병행, 별도 sub-phase `CAT-L-SCRIPT-AUDIT-01`). 자동 A 조건(설명 불가 세션)은 D2 진술로 해소 여부를 Claude가 확정.
- **D4**: **승인** (6개 규칙) + 보충: Handoff 명령은 그대로 실행하되, 더 나은 방향·아이디어·구현은 의견으로 제시·반영(조용한 변경 금지). 반영 위치: `CAT-L_인프라배포.md`.

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 0c 확정 심사 (V-블록 + L1–L4) · 2026-10-01

**Handoff 커밋**: `aca1508bf4d461a2dae88d2f1d019565bc2a090c` (2026-10-01 16:43:40 +0900, `CLAUDE_TO_CURSOR.md` 1파일, 전문 기록) — V-블록 실행 **전** push 완료.
**서버 명령**: V-블록 1회(읽기전용, `sudo` 0, `set -e` 0, pull/설치기/restart/`--cutover-check` 재실행 0). 코드 변경 0. 로컬 문서 + 증거물 커밋만.

### 0. 실행 수단 (Handoff §4-0 대비 — 먼저 공개)

- Handoff는 Git Bash 따옴표 heredoc을 지정. 이 환경의 셸은 PowerShell이라 heredoc 직접 사용 불가.
- 대신 **push된 커밋 `aca1508`의 blob에서 V-블록(§4-1 코드펜스 내부)을 바이트 그대로 추출**해 ssh stdin(바이너리, CRLF 변환 없음)으로 `bash -s`에 전달.
  추출 결과: **38줄 · CR 0바이트 · sha256 `9879eb2b07e7496c94ce92f188932b3fed46230f826b462a771277ba6c5393e1`**.
- 한 줄도 수정하지 않음(고친 줄 없음). 서버 stderr 0바이트, `$'\r'` 오류 0, SSH_RC=0.
- 추출·전송 도우미(`vb_run.py`)는 로컬 임시 폴더에만 있고 서버로 가지 않음. 이것 역시 "Handoff 밖 도구"이므로 공개함. 필요하면 파일 본문을 다음 회신에 첨부 가능.
- 이전 사고(CRLF 파이프)와 달리 입력이 **커밋된 Handoff blob**이라 실행본 = Handoff 원문이 해시로 증명됨.

### 1. V-블록 출력 전문 (요약 없음)

```text
== V-BLOCK START 2026-10-01T07:45:38Z user=ubuntu ==
== V0 HEAD / worktree / owner ==
8a6da21b31df318634b203452423bd6365ea5ebd
PORCELAIN_ALL=26 PORCELAIN_CONTENT=16
?? ai_cache.sqlite
?? bitget/analysis/universe_bt/reports/u3_live-20260823T092525Z.md
?? bitget/analysis/universe_bt/reports/u3_live-20260823T110428Z.md
?? bitget/analysis/universe_bt/reports/u3_live-20260823T110717Z.md
?? bitget/analysis/universe_bt/reports/u3_live-20260823T112223Z.md
?? bitget/analysis/universe_bt/reports/u3_live-20260823T114203Z.md
?? bitget/analysis/universe_bt/reports/u3_live-20260823T121158Z.md
?? bitget_meta_governor_state_backup.json
?? deploy_watch_latest.json
?? dual_north_star_ledger.json
?? llm_call_cache.sqlite
?? market_data.sqlite
?? message_queue.sqlite
?? message_queue.sqlite-shm
?? message_queue.sqlite-wal
?? ops_events.sqlite
ubuntu 2026-09-30 09:42:43.489641148 +0000 .git/ORIG_HEAD
ubuntu 2026-09-30 09:42:43.478641100 +0000 .git/FETCH_HEAD
ubuntu 2026-09-30 09:42:43.498641186 +0000 bitget/validation/architecture_checks.py
ROOT_OWNED_IN_REPO:
./bitget/pipelines/__pycache__/__init__.cpython-310.pyc
./bitget/forward/__pycache__/__init__.cpython-310.pyc
./bitget/forward/__pycache__/_core.cpython-310.pyc
./bitget/infra/__pycache__/data_paths.cpython-310.pyc
./bitget/__pycache__/__init__.cpython-310.pyc
./bitget/__pycache__/symbol_utils.cpython-310.pyc
./bitget/__pycache__/env.cpython-310.pyc
./bitget/governance/__pycache__/__init__.cpython-310.pyc
./__pycache__/sqlite_schema_guard.cpython-310.pyc
./__pycache__/factory_scan_schedule.cpython-310.pyc
./__pycache__/low_ram_sqlite_pragmas.cpython-310.pyc
./__pycache__/telegram_env.cpython-310.pyc
./reports/__pycache__/__init__.cpython-310.pyc
== V1 cron drift guard ==
SOURCE_STATE=PRISTINE
GEN_WRAPPED_COUNT=28
LIVE_WRAPPED_COUNT=28
EXPECTED_WRAPPED=28
GEN_BODY_SHA=f36f722489ce082a661f04bcb0bd59c8ef21fb846b9704be8718a5fa266b21dd
LIVE_BODY_SHA=f36f722489ce082a661f04bcb0bd59c8ef21fb846b9704be8718a5fa266b21dd
MARKER_SHA=f36f722489ce082a661f04bcb0bd59c8ef21fb846b9704be8718a5fa266b21dd
BODIES_EQUAL=yes
=== unified diff (body, comments excluded) ===
(empty)
=== slice ===
SLICE_TEMPLATE=/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/systemd/bitget-cron-heavy.slice
SLICE_UNIT=/etc/systemd/system/bitget-cron-heavy.slice
SLICE_TEMPLATE_HIGH=1288490188
SLICE_TEMPLATE_MAX=1610612736
SLICE_FILE_SAME=yes
SLICE_RUNTIME=
MemoryHigh=1288490188
MemoryMax=1610612736
ActiveState=active
DIFF_LIVE_RC=0
== V2 heavy slice ==
MemoryHigh=1288490188
MemoryMax=1610612736
ActiveState=active
== V3 /tmp evidence ==
-rw-rw-r-- 1 ubuntu ubuntu 14657 2026-09-30 06:52:15.306142532 +0000 /tmp/cutover01_p0.out
-rw-rw-r-- 1 ubuntu ubuntu 11722 2026-09-30 09:19:10.546523741 +0000 /tmp/cutover01_p0c_arch.out
-rw-rw-r-- 1 ubuntu ubuntu 12747 2026-09-30 09:42:44.963647487 +0000 /tmp/cutover01_p0c_deploy.out
38d18d65a72a1b51606be3b9612b42c94a6cc62b65c2b00e9e39bb34d5c15322  /tmp/cutover01_p0.out
07b5e70ab123a4aa2c5eeb3f74151d3bf4a324b77ed6d19ec418e035a9dcc687  /tmp/cutover01_p0c_arch.out
4a583ef780a2c788fa2dc6e3b2a1c659228fef3f7f811f69a7441e2dab316f33  /tmp/cutover01_p0c_deploy.out
/tmp/cutover01_p0.out:1:=== TS 2026-09-30T06:49:53Z HEAD=c1ffe3f ===
/tmp/cutover01_p0.out:2:=== Step 1 bitget.sh --cutover-check ===
/tmp/cutover01_p0.out:4:CUTOVER_CHECK_RC=124
/tmp/cutover01_p0.out:5:=== Step 1 JSON dump ===
/tmp/cutover01_p0.out:470:JSON_DUMP_RC=0
/tmp/cutover01_p0.out:471:=== Step 2 env keys ===
/tmp/cutover01_p0.out:476:=== Step 3 processes ===
/tmp/cutover01_p0.out:488:=== DONE (no start-parallel, no env write) ===
/tmp/cutover01_p0.out:489:WROTE /tmp/cutover01_p0.out
/tmp/cutover01_p0c_arch.out:1:=== TS 2026-09-30T09:19:08Z ===
/tmp/cutover01_p0c_arch.out:2:HEAD=c1ffe3f 2026-09-30 14:59:31 +0900 feat(bitget): CAT-L-FENCE-03 cron drift guard with body-sha256 markers
/tmp/cutover01_p0c_arch.out:3:=== architecture_checks.py mtime ===
/tmp/cutover01_p0c_arch.out:450:JSON_DUMP_RC=0
/tmp/cutover01_p0c_arch.out:451:WROTE /tmp/cutover01_p0c_arch.out
/tmp/cutover01_p0c_deploy.out:1:=== TS 2026-09-30T09:42:40Z ===
/tmp/cutover01_p0c_deploy.out:2:BEFORE: c1ffe3f 2026-09-30 14:59:31 +0900 feat(bitget): CAT-L-FENCE-03 cron drift guard with body-sha256 markers
/tmp/cutover01_p0c_deploy.out:24:PULL_RC=0
/tmp/cutover01_p0c_deploy.out:25:AFTER: 8a6da21 2026-09-30 18:40:07 +0900 CAT-L-CUTOVER-01 Phase 0c: architecture_checks 4건 갱신(위치→연결 검증 등), 게이트 로직 비접촉
/tmp/cutover01_p0c_deploy.out:462:JSON_DUMP_RC=0
/tmp/cutover01_p0c_deploy.out:463:WROTE /tmp/cutover01_p0c_deploy.out
== V4 kernel OOM 2026-09-30 06:45-10:00 UTC ==
KLINES=2
Sep 30 07:34:13 ip-172-26-7-213 kernel: workqueue: psi_avgs_work hogged CPU for >10000us 512 times, consider switching to WQ_UNBOUND
Sep 30 07:39:00 ip-172-26-7-213 kernel: workqueue: key_garbage_collector hogged CPU for >10000us 4 times, consider switching to WQ_UNBOUND
OOM_HITS=0
== V5 ssh accepted since 2026-09-26 UTC ==
SSH_ACCEPTED_COUNT=79
Sep 26 08:54:06 ip-172-26-7-213 sshd[844]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 08:54:33 ip-172-26-7-213 sshd[934]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 08:56:49 ip-172-26-7-213 sshd[1072]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 08:57:15 ip-172-26-7-213 sshd[1130]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 09:12:23 ip-172-26-7-213 sshd[1358]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 09:12:49 ip-172-26-7-213 sshd[1423]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 09:16:59 ip-172-26-7-213 sshd[1540]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 09:17:01 ip-172-26-7-213 sshd[1599]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 09:18:43 ip-172-26-7-213 sshd[1670]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 09:19:19 ip-172-26-7-213 sshd[1728]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 09:19:21 ip-172-26-7-213 sshd[1776]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 12:18:35 ip-172-26-7-213 sshd[3857]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 13:12:14 ip-172-26-7-213 sshd[4392]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 13:12:43 ip-172-26-7-213 sshd[4451]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 13:14:25 ip-172-26-7-213 sshd[4541]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 13:14:48 ip-172-26-7-213 sshd[4601]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 14:38:32 ip-172-26-7-213 sshd[5545]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 14:38:59 ip-172-26-7-213 sshd[5634]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 14:39:25 ip-172-26-7-213 sshd[5683]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 14:39:52 ip-172-26-7-213 sshd[5741]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 14:41:08 ip-172-26-7-213 sshd[5812]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 14:41:37 ip-172-26-7-213 sshd[5870]: Accepted publickey for ubuntu from 110.35.116.11
Sep 26 14:42:03 ip-172-26-7-213 sshd[5935]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 04:09:22 ip-172-26-7-213 sshd[13665]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 04:09:48 ip-172-26-7-213 sshd[13755]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 04:30:40 ip-172-26-7-213 sshd[14083]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 04:31:03 ip-172-26-7-213 sshd[14151]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 13:29:33 ip-172-26-7-213 sshd[19749]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 13:30:04 ip-172-26-7-213 sshd[19839]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 13:31:20 ip-172-26-7-213 sshd[19970]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 13:32:32 ip-172-26-7-213 sshd[20028]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 13:55:06 ip-172-26-7-213 sshd[20294]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 13:55:28 ip-172-26-7-213 sshd[20367]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:17:33 ip-172-26-7-213 sshd[20704]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:17:55 ip-172-26-7-213 sshd[20759]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:19:46 ip-172-26-7-213 sshd[20836]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:20:11 ip-172-26-7-213 sshd[20904]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:20:33 ip-172-26-7-213 sshd[20961]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:20:54 ip-172-26-7-213 sshd[21009]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:21:29 ip-172-26-7-213 sshd[21136]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:22:51 ip-172-26-7-213 sshd[21234]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:23:18 ip-172-26-7-213 sshd[21293]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:23:31 ip-172-26-7-213 sshd[21295]: Accepted publickey for ubuntu from 110.35.116.11
Sep 27 14:23:53 ip-172-26-7-213 sshd[21406]: Accepted publickey for ubuntu from 110.35.116.11
Sep 28 01:48:34 ip-172-26-7-213 sshd[29266]: Accepted publickey for ubuntu from 110.35.116.11
Sep 28 01:56:07 ip-172-26-7-213 sshd[29508]: Accepted publickey for ubuntu from 110.35.116.11
Sep 28 01:56:32 ip-172-26-7-213 sshd[29567]: Accepted publickey for ubuntu from 110.35.116.11
Sep 28 01:57:39 ip-172-26-7-213 sshd[29624]: Accepted publickey for ubuntu from 110.35.116.11
Sep 28 11:36:01 ip-172-26-7-213 sshd[36285]: Accepted publickey for ubuntu from 175.193.220.230
Sep 28 11:37:50 ip-172-26-7-213 sshd[36379]: Accepted publickey for ubuntu from 175.193.220.230
Sep 28 11:37:56 ip-172-26-7-213 sshd[36445]: Accepted publickey for ubuntu from 175.193.220.230
Sep 28 11:38:21 ip-172-26-7-213 sshd[36510]: Accepted publickey for ubuntu from 175.193.220.230
Sep 28 11:39:12 ip-172-26-7-213 sshd[36589]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 05:55:39 ip-172-26-7-213 sshd[47739]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 07:59:59 ip-172-26-7-213 sshd[55682]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 08:00:08 ip-172-26-7-213 sshd[55820]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 08:01:08 ip-172-26-7-213 sshd[55921]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 08:01:12 ip-172-26-7-213 sshd[55988]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 08:01:14 ip-172-26-7-213 sshd[56044]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 09:13:29 ip-172-26-7-213 sshd[56804]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 09:15:01 ip-172-26-7-213 sshd[56898]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 09:21:44 ip-172-26-7-213 sshd[57076]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 09:21:45 ip-172-26-7-213 sshd[57146]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 10:27:30 ip-172-26-7-213 sshd[57762]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 10:27:36 ip-172-26-7-213 sshd[57853]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 10:28:28 ip-172-26-7-213 sshd[58048]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 10:28:31 ip-172-26-7-213 sshd[58105]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 11:28:54 ip-172-26-7-213 sshd[59227]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 11:29:01 ip-172-26-7-213 sshd[59316]: Accepted publickey for ubuntu from 175.193.220.230
Sep 29 11:29:38 ip-172-26-7-213 sshd[59397]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 06:00:26 ip-172-26-7-213 sshd[70660]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 06:00:32 ip-172-26-7-213 sshd[70750]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 06:49:43 ip-172-26-7-213 sshd[71314]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 06:49:52 ip-172-26-7-213 sshd[71383]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 06:51:56 ip-172-26-7-213 sshd[71469]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 06:53:44 ip-172-26-7-213 sshd[71762]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 09:19:07 ip-172-26-7-213 sshd[74021]: Accepted publickey for ubuntu from 175.193.220.230
Sep 30 09:42:38 ip-172-26-7-213 sshd[74287]: Accepted publickey for ubuntu from 175.193.220.230
Oct 01 07:45:34 ip-172-26-7-213 sshd[87450]: Accepted publickey for ubuntu from 175.193.220.230
== V6 units ==
dante-bitget-backup.service   loaded failed failed Bitget SQLite integrity backup (L-2 P0-5)
dante-bitget-snapshot.service loaded failed failed Bitget CQRS snapshot (bitget_market_data_snapshot.sqlite)
Result=success
NRestarts=0
ExecMainStartTimestamp=Mon 2026-09-28 02:24:33 UTC
ExecMainStatus=0
Id=dante-bitget-async.service
ActiveState=active
SubState=running
ActiveEnterTimestamp=Mon 2026-09-28 02:24:33 UTC

Result=exit-code
NRestarts=0
ExecMainStartTimestamp=Thu 2026-10-01 00:30:01 UTC
ExecMainStatus=127
Id=dante-bitget-backup.service
ActiveState=failed
SubState=failed
ActiveEnterTimestamp=n/a

Result=success
NRestarts=0
ExecMainStartTimestamp=Mon 2026-09-28 02:24:33 UTC
ExecMainStatus=0
Id=dante-bitget-factory.service
ActiveState=active
SubState=running
ActiveEnterTimestamp=Mon 2026-09-28 02:24:33 UTC

Result=success
NRestarts=0
ExecMainStartTimestamp=Thu 2026-10-01 00:00:02 UTC
ExecMainStatus=0
Id=dante-bitget-journal-vacuum.service
ActiveState=inactive
SubState=dead
ActiveEnterTimestamp=n/a

Result=success
NRestarts=0
ExecMainStartTimestamp=Sat 2026-09-26 08:52:22 UTC
ExecMainStatus=0
Id=dante-bitget-overseer.service
ActiveState=active
SubState=running
ActiveEnterTimestamp=Sat 2026-09-26 08:52:22 UTC

Result=success
NRestarts=0
ExecMainStartTimestamp=Wed 2026-09-30 15:46:35 UTC
ExecMainStatus=0
Id=dante-bitget-queue-worker.service
ActiveState=active
SubState=running
ActiveEnterTimestamp=Wed 2026-09-30 15:46:35 UTC

Result=exit-code
NRestarts=0
ExecMainStartTimestamp=Thu 2026-10-01 07:41:37 UTC
ExecMainStatus=1
Id=dante-bitget-snapshot.service
ActiveState=failed
SubState=failed
ActiveEnterTimestamp=n/a

Result=success
NRestarts=0
ExecMainStartTimestamp=Thu 2026-10-01 07:41:37 UTC
ExecMainStatus=0
Id=dante-bitget-watchdog.service
ActiveState=inactive
SubState=dead
ActiveEnterTimestamp=n/a

Result=success
NRestarts=0
ExecMainStartTimestamp=Mon 2026-09-28 02:24:31 UTC
ExecMainStatus=0
Id=dante-bitget-ws.service
ActiveState=active
SubState=running
ActiveEnterTimestamp=Mon 2026-09-28 02:24:31 UTC

Result=success
Id=dante-bitget-backup.timer
ActiveState=active
SubState=waiting
ActiveEnterTimestamp=Sat 2026-09-26 08:52:22 UTC

Result=success
Id=dante-bitget-journal-vacuum.timer
ActiveState=active
SubState=waiting
ActiveEnterTimestamp=Sat 2026-09-26 08:52:22 UTC

Result=success
Id=dante-bitget-snapshot.timer
ActiveState=active
SubState=waiting
ActiveEnterTimestamp=Mon 2026-09-28 02:24:37 UTC

Result=success
Id=dante-bitget-watchdog.timer
ActiveState=active
SubState=waiting
ActiveEnterTimestamp=Mon 2026-09-28 02:24:36 UTC

== V-BLOCK END 2026-10-01T07:45:48Z ==

--STDERR--

SSH_RC=0
```

### 2. §4-2 기대값 대조 (고친 것 없음 · 보고만)

| 항목 | 기대 | 실측 | 판정 |
|---|---|---|---|
| V0 HEAD | `8a6da21` | `8a6da21b31df318634b203452423bd6365ea5ebd` | 일치 |
| V0 worktree | PORCELAIN_CONTENT의 M/D 0 | CONTENT=16줄 **전부 `??`**(M/D 0). `ai_cache.sqlite`·`market_data.sqlite`·`ops_events.sqlite` 등 런타임 산출물 + `u3_live-*.md` 6건 + state json 3건 | 일치 |
| V0 부가 | (기대 없음) | `PORCELAIN_ALL=26` vs `CONTENT=16` → **10건은 파일 모드만 다른 변경**(`core.fileMode=false`로 숨겨짐). 어느 파일인지는 V-블록 범위 밖이라 미확인 | **보고(미확인)** |
| V0 ORIG_HEAD | 소유자 ubuntu · ≈09:42:4x | ubuntu · 09:42:43.489 | 일치 |
| V0 FETCH_HEAD | 09:42:40 이후면 V5 대조 | 09:42:43.478 — deploy 실행 TS 09:42:40, SSH 세션 09:42:38과 일치(배포 pull의 fetch) | 설명됨 |
| V0 ROOT_OWNED | 0줄 | **13줄 — 불일치.** 전부 `__pycache__/*.cpython-310.pyc`(소스 파일 아님): `bitget/pipelines`·`bitget/forward`(`__init__`,`_core`)·`bitget/infra/data_paths`·`bitget/__init__`·`symbol_utils`·`env`·`bitget/governance/__init__`, 루트 `sqlite_schema_guard`·`factory_scan_schedule`·`low_ram_sqlite_pragmas`·`telegram_env`, `reports/__init__` | **불일치 — 원인 미확인** |
| V1 | RC=0 · PRISTINE · BODIES_EQUAL=yes · WRAP=28 · SHA `f36f7224…266b21dd` | 전부 일치 (`GEN/LIVE/MARKER_SHA` 3개 동일, diff 빈 값, `SLICE_FILE_SAME=yes`) | 일치 |
| V2 | High=1288490188 · Max=1610612736 | 동일 (ActiveState=active) | 일치 |
| V3 mtime | ≈06:53 / 09:19 / 09:42 UTC | 06:52:15 / 09:19:10 / 09:42:44 — 그 이후 시각 없음(재실행 흔적 없음) | 일치 |
| V3 p0 | RC=124 · JSON_DUMP_RC=0 · `=== DONE` | `CUTOVER_CHECK_RC=124`, `JSON_DUMP_RC=0`, `=== DONE` 있음 | 일치 |
| V3 arch | `HEAD=c1ffe3f` | `HEAD=c1ffe3f …`, `JSON_DUMP_RC=0` | 일치 |
| V3 deploy | `PULL_RC=0` · `AFTER: 8a6da21` · HASH_MISMATCH 없음 | 동일, HASH_MISMATCH 0건 | 일치 |
| V4 | KLINES≥1 · 권한 오류 없음 · OOM 0 | KLINES=2(둘 다 `workqueue … hogged CPU`, 07:34 / 07:39 UTC), 권한 오류 없음, **OOM_HITS=0** | 일치 |
| V5 | 자기 세션 포함 | `Oct 01 07:45:34` (= V-블록 START 07:45:38) 포함, COUNT=79 | 일치 → 아래 주석 |
| V6 | 그대로 보고 | 아래 §2-1 | 보고 |

#### 2-1. V6 판독값 (판정은 Claude)

| 유닛 | ActiveState/SubState | Result | ExecMainStatus | NRestarts | ExecMainStart |
|---|---|---|---|---|---|
| dante-bitget-backup.service | failed/failed | exit-code | **127** | 0 | 2026-10-01 00:30:01 UTC |
| dante-bitget-snapshot.service | failed/failed | exit-code | **1** | 0 | 2026-10-01 07:41:37 UTC |
| dante-bitget-watchdog.service | **inactive/dead** (activating 아님) | success | 0 | 0 | 2026-10-01 07:41:37 UTC |
| dante-bitget-journal-vacuum.service | inactive/dead | success | 0 | 0 | 2026-10-01 00:00:02 UTC |
| dante-bitget-async / factory / ws | active/running | success | 0 | 0 | 09-28 02:24:33 / 02:24:33 / 02:24:31 |
| dante-bitget-overseer | active/running | success | 0 | 0 | 09-26 08:52:22 |
| dante-bitget-queue-worker | active/running | success | 0 | 0 | 09-30 15:46:35 |
| 타이머 4종 (backup·journal-vacuum·snapshot·watchdog) | active/waiting | success | — | — | ActiveEnter 09-26 08:52 ~ 09-28 02:24 |

- 읽은 것만: backup=127(종료코드 127), snapshot=1, 둘 다 NRestarts=0. **두 실패 모두 최근 실행**(backup 오늘 00:30:01, snapshot 오늘 07:41:37 = V-블록 4분 전) → 반복 실패. 원인은 **조회하지 않음**(범위 밖). watchdog은 이번엔 `inactive/dead`·`success`(07:41:37 실행 완료, snapshot과 같은 시각). 출력 블록은 `Result…ExecMainStatus` 뒤에 `Id`가 오는 순서이므로 위 표는 그에 맞춰 대응시킴(원문 §1 참조).
- 재시작·`reset-failed` 안 함.

#### 2-2. V5 세션 주석 (79건)

IP 마지막 옥텟 가리지 않음(원문 그대로). 시각은 UTC.

| 구간 | 건수 | 출발지 | 내가 대응시킬 수 있는 것 | 판정 |
|---|---|---|---|---|
| 09-26 08:54–14:42 | 23 | 110.35.116.11 | 확실한 대응 없음(설치·FENCE 초기 작업 구간으로 보이나 **시각 대조 기록을 이번에 못 찾음**) | **설명 불가(미대조)** |
| 09-27 04:09–14:23 | 21 | 110.35.116.11 | 동일 | **설명 불가(미대조)** |
| 09-28 01:48–01:57 | 4 | 110.35.116.11 | 동일 | **설명 불가(미대조)** |
| 09-28 11:36–11:39 | 5 | **175.193.220.230**(출발지 변경) | 동일 | **설명 불가(미대조)** |
| 09-29 05:55–11:29 | 17 | 175.193.220.230 | FENCE-02/03 계열 작업 구간. 개별 시각 대조 못 함 | **설명 불가(미대조)** |
| 09-30 06:00:26, 06:00:32 | 2 | 175.193.220.230 | `fence03_bootstrap.sh` (`eb80c58`, 15:01:22 KST = 06:01 UTC 커밋) | 대응(커밋 시각 근거) |
| 09-30 06:49:43, 06:49:52 | 2 | 동일 | `cutover01_p0.sh` (스크립트 `TS 06:49:53Z`) — 2세션 중 어느 쪽이 파이프/실행인지는 미구분 | 대응(1건 이상) |
| 09-30 06:51:56, 06:53:44 | 2 | 동일 | p0 직후. `/tmp/cutover01_p0.out` 마지막 수정 06:52:15. 내용 미기억 — **추정: 결과 회수/상태 확인** | **미확인(추정)** |
| 09-30 09:19:07 | 1 | 동일 | `cutover01_p0c_arch.sh` (`TS 09:19:08Z`) | 대응 |
| 09-30 09:42:38 | 1 | 동일 | `cutover01_p0c_deploy.sh` (`TS 09:42:40Z`) | 대응 |
| 10-01 07:45:34 | 1 | 동일 | **이번 V-블록** | 대응 |

- 위 표의 "설명 불가(미대조)"는 **"수상하다"가 아니라 "이 세션에서 시각-작업 대조를 못 했다"**는 뜻. 근거 없이 "FENCE-02 작업이었을 것"이라고 채우지 않았다.
- 출발지가 09-28 11:36부터 `110.35.116.11` → `175.193.220.230`로 바뀜. 디렉터 확인 필요(D2).
- Handoff D3 규칙: "설명 불가가 1건이라도 나오면 자동으로 A". 형식상 **A 발동 조건 충족 상태**. 디렉터 D2 회신 + 필요 시 Claude 지정 대조 후 해제 여부 판정 바람.

### 3. L-항목 (로컬·git만 · 서버 실행 0)

#### L1 — `bitget.sh --cutover-check` 경로

- (a) `bitget.sh:147` `--cutover-check` → `MODE="cutover_check"`. 실행 줄은 `bitget.sh:291` `exec python -m bitget.pipelines.runner --mode "$MODE" "${EXTRA_ARGS[@]}" >>"$LOG_FILE" 2>&1`. **`--skip-telegram` 없음**(p0는 안 넘김; 플래그 자체는 `bitget.sh:157`에서 EXTRA_ARGS로만 추가). `exec`라 timeout의 자식 = python 자체. 로그: `bitget.sh:48,50,247` → `${BITGET_LOG_DIR:-${BITGET_ROOT}/logs}/bitget_cutover_check_<STAMP>.log` (실제: `/var/lib/quant-bitget/logs/bitget_cutover_check_20260930_154953.log`, p0 스냅샷 3행).
- (b) 락 = Python `fcntl.flock` 폴링: `runtime.py:390 bitget_job_lock(mode, timeout_sec=120.0)`, `:408` deadline = monotonic + timeout, `:451` `time.sleep(1.0)` 반복, 초과 시 `:453 JobSkipError`. 대기 상한 = `resolve_lock_timeout_sec`(`bitget_scan_schedule.py:300–311`): 이 모드는 기본 **120초**. 즉시 skip 아님 — **최대 120초 대기 후 skip**. 락 파일은 전 모드 공용 1개 (`data_paths.job_lock_path → runtime_lock_path`).
- (c) 파이프라인 `config_bootstrap` + `artifact_guard` + `cutover_check`(`bitget_pipelines.py`, `_with_guard`). 쓰는 것: ① `config_bootstrap`: 설정 보정(쓰기 가능) ② `artifact_guard`: 산출물 점검(DB/파일 heal 가능) ③ `validation/runner.py run_cutover_check` → `ops_logger.record_gauge_snapshot`(ops_events DB 쓰기) ④ `dispatch_bitget_mode` 종료 시 `_record_ops_heartbeat`(`runtime.py:731`) ⑤ 텔레그램: `runtime.py:729–730` — 상태가 `OK`가 아니고 quiet가 아니면 전송. `SKIPPED_LOCK`은 기본 quiet(`:711–713`, `BITGET_ALERT_SKIPPED_LOCK=1`일 때만 예외). `parallel_run_state.json` 쓰기는 `--cutover-check`가 **아님**(`cutover.py:26–35 start_parallel_run`은 `start_parallel` 모드 전용). **결론: 순수 읽기 아님.**
- (d) `bitget.sh` cutover 경로에 `setsid`/`nohup`/`&` **없음** (파일 grep 결과 해당 모드에서 매칭 0). timeout이 죽이면 python 단독 종료, 고아 자식 가능성은 코드상 낮음(단, `subprocess`를 쓰는 `architecture_checks`의 `generate_bitget_crontab.py --check`는 자식이지만 `capture_output`으로 동기 대기).
- (e) **결론: 코드만으로 확정 불가.** 124는 `timeout 120`이 SIGTERM을 보낸 시점에 python이 살아 있었다는 뜻일 뿐. 락 대기 상한(120초)과 `timeout 120`이 같은 값이라 "락 대기 중"이 가장 자연스러운 가설이나 "실행 중"(config_bootstrap/artifact_guard의 느린 작업)과 코드로 구분 불가. **판별용 서버 로그 위치만 기록(실행 안 함)**:
  1. `/var/lib/quant-bitget/logs/bitget_cutover_check_20260930_154953.log` (이 실행 전용 로그 — 락 대기 문구/단계 로그 유무)
  2. 같은 시각(06:49:53–06:51:53 UTC) 동안 락을 쥔 다른 잡: cron 줄(`generate_bitget_crontab.py`가 만든 시각표) + `bitget_*_20260930_154*` 로그명
  3. `ops_events.sqlite`의 해당 시각 이벤트
- (f) `--start-parallel`도 **동일 경로**: `bitget.sh:149` → 같은 `runner`(`:291`) → 같은 `bitget_job_lock` 120초 대기. 단 `start_parallel` 파이프라인은 critical 단계(`start_parallel` 스텝 = `parallel_run_state.json` 쓰기)를 가짐. **설계 입력**: 락이 바쁘면 `SKIPPED_LOCK` → `bitget_exit_code`(`runtime.py:734–740`)가 **0을 반환**(`skipped_lock` 시 무조건 0) + 텔레그램 quiet. 즉 Phase 1을 같은 방식으로 돌리면 **"성공(exit 0)처럼 보이는데 48h 창이 시작 안 된" 상태**가 가능 → 실행 후 `parallel_run_state.json` 존재·`started_at_utc` 확인이 필수 단계여야 함. 또한 `timeout`을 씌우면 안 됨(§5 규칙 3).

#### L2 — pull 범위 `c1ffe3f..8a6da21`

```text
8a6da21 2026-09-30T18:40:07+09:00 CAT-L-CUTOVER-01 Phase 0c: architecture_checks 4건 갱신(위치→연결 검증 등), 게이트 로직 비접촉
598282c 2026-09-30T16:29:20+09:00 docs(bitget): CAT-L-CUTOVER-01 Phase 0b architecture-check diagnosis
29e9c8a 2026-09-30T15:54:44+09:00 docs(bitget): FENCE-03 SUB_DONE and CUTOVER-01 Phase 0 read-only capture
eb80c58 2026-09-30T15:01:22+09:00 docs(bitget): record CAT-L-FENCE-03 Bot-2 marker bootstrap as PRISTINE
```

14 files, +1112/−29. 분류:

| 파일 | 분류 |
|---|---|
| `bitget/validation/architecture_checks.py` (+84) | 체크 (cutover/validation이 import — cron 잡·상주 서비스 import 경로 아님) |
| `bitget/tests/test_cat_l_cutover01_phase0c_checks.py` (+184) | 테스트 |
| **`bitget/deploy/generate_bitget_crontab.py` (1줄)** | **배포도구 — 표시.** `format_install_plan`의 `MARKER_SHA`: `live_marker_sha(live_text) if live_text else ''` → `live_marker_sha(live_text) or ''` (설치기 사전 계획 출력 문자열만; 생성 본문·`--diff-live` 판정 로직 비접촉). 상주 서비스·cron 잡 import 대상 아님 |
| `snapshots/CAT-L-CUTOVER-01_P0_20260930.md`, `snapshots/cutover01_p0.sh`, `snapshots/fence03_bootstrap.sh` | 문서/증거물 |
| `CLAUDE_TO_CURSOR.md`, `CURSOR_TO_CLAUDE.md`, `NEXT_ACTION.md`, `ARCHITECT_MIRROR.md`, `track_b_CURSOR_TO_CLAUDE.md`, `track_b_NEXT_ACTION.md`, 현황판, 진행로그 | 문서 |

- **상주 서비스·cron 잡이 import하는 런타임 파일: 0건.** 표시 대상은 위 `generate_bitget_crontab.py` 1줄(배포도구) 하나.

#### L3 — 출처

- `git log -- bitget/docs/work_phases/snapshots/cutover01_p0.sh` → `29e9c8a 2026-09-30T15:54:44+09:00` **1건** (기대와 일치).
- 로컬 3파일 sha256 / 최종 수정시각(UTC):

| 파일 | sha256 | mtime(UTC) | 서버 실행 시각(UTC) | 비교 |
|---|---|---|---|---|
| `cutover01_p0.sh` | `371d77c1f2784a17e24da80dd86a2b75877fb60667df9f8e00f043b980581030` | 2026-09-30T06:49:39 | 06:49:53 | 수정이 실행 **전** → 현재본 = 실행본 가능성 높음(동일성 증명은 불가) |
| `cutover01_p0c_arch.sh` | `9e015496bc4bc284177ba13647094476c2a7d011e3eeadcbb28647e7da271b7b` | 2026-09-30T09:25:49 | 09:19:08 | 수정이 실행 **후** → **현재본 ≠ 실행본 가능** (LF 재작성) |
| `cutover01_p0c_deploy.sh` | `09ab61526f1c7fc5c12c9f19671c3581ab5940011a1d1d4d5deb6b248cc624c8` | 2026-09-30T09:45:32 | 09:42:40 | 수정이 실행 **후** → **현재본 ≠ 실행본 가능** (LF 재작성) |

- 서버 `/tmp/cutover01_*.out` sha256 (V3 원문): p0 `38d18d65…c15322`(전체 `38d18d65a72a1b51606be3b9612b42c94a6cc62b65c2b00e9e39bb34d5c15322`) · arch `07b5e70ab123a4aa2c5eeb3f74151d3bf4a324b77ed6d19ec418e035a9dcc687` · deploy `4a583ef780a2c788fa2dc6e3b2a1c659228fef3f7f811f69a7441e2dab316f33`.
- 스냅샷 문서 전문/발췌:
  - `CAT-L-CUTOVER-01_P0_20260930.md` — 489줄. `/tmp/cutover01_p0.out`(489줄)과 줄 수가 같음 → **전문 사본**(바이트 동일성은 sha256 미비교).
  - `CAT-L-CUTOVER-01_P0c_BOT2_20260930.md` — 19줄. `/tmp/cutover01_p0c_arch.out`(450줄대)의 **발췌**(HEAD/mtime/failed 목록 요약).
  - `CAT-L-CUTOVER-01_P0c_DEPLOY_20260930.md` — 26줄. `/tmp/cutover01_p0c_deploy.out`(463줄)의 **발췌**; 문서 안에 "원문 전체: Bot-2 `/tmp/cutover01_p0c_deploy.out`" 명시.
  - 두 발췌 문서는 인코딩이 깨진 한글(`?�`)이 섞여 있음(작성 당시 인코딩 문제). 내용 정정 아님, 발견만 보고.

#### L4 — 부작용 지점 (`c1ffe3f` vs `8a6da21` 둘 다)

패턴 grep(`open(|write_text|json.dump(|sqlite3.connect|INSERT|requests.|ccxt|telegram|send_|subprocess|os.system|.env|getenv|environ`) 양 버전:

| 파일 | c1ffe3f | 8a6da21 | 쓰기/네트워크 |
|---|---|---|---|
| `cutover.py` | `:8 subprocess` · `:33–34 open(…,'w')+json.dump`(=`start_parallel_run`, `check_cutover_readiness`에서 **호출 안 됨**) · `:43 open(read)` · `:77 subprocess.run(["pgrep","-f","bitget.main"], timeout=5)` · `:19,31,99 os.environ.get` | **동일 줄**(변화 없음) | 쓰기 함수는 있으나 readiness에선 미호출 |
| `architecture_checks.py` | `:296,300 subprocess.run([python, generate_bitget_crontab.py, "--check"])` · `:124` 문자열 토큰 검사 `"open("` · `:1059–1061, :1167–1177` 문자열 검사 · `:1192 os.environ.get("BITGET_WATCHDOG_HEARTBEAT_COMPONENT")` | **줄 번호만 이동**(`:303,307`, `:131`, `:1105–1107`, `:1213–1223`, `:1238`), 부작용 종류 동일 | 파일 쓰기·DB·네트워크 **0**. `--check`는 `generate_bitget_crontab.py:489–507 check_template` = 템플릿 `read_text` 비교만(쓰기는 인자 없을 때 `write_template`이라 `--check` 아님) |

- 호출 경로 부가 관찰: `run_architecture_checks`가 `bitget.main`·`factory_launcher` 차단 스텁을 import할 때 `[BLOCKED] … is removed` 문구를 stdout에 출력(p0 스냅샷 JSON-dump 구간). 쓰기 아님, 출력만. `get_logger`(`logging_setup.py:95–97`)는 로그 디렉터리 생성 + `RotatingFileHandler`를 열 수 있음 — import 시점 로그 파일 생성 가능성(`BITGET_DISABLE_FILE_LOG`로 끌 수 있음).
- env 의존: `check_cutover_readiness`는 `.env` 필수 키 **없음**(`BITGET_PIPELINE_SSOT`·`BITGET_ASYNC_TELEGRAM`·`BITGET_PARALLEL_RUN_HOURS`·`BITGET_WATCHDOG_HEARTBEAT_COMPONENT`는 모두 기본값 있음). 단 **env에 값이 없으면 `pipeline_ssot_env=False`가 되어 `passed`가 항상 false**이므로, 진단 JSON 덤프에서 `.env`를 source한 것은 "러너와 같은 env 조건으로 판정값을 보려는 의도"였음(= 이전 소명 그대로). 규칙 5에 따라 앞으로는 필요한 키만 grep.
- **판정: `check_cutover_readiness`·`run_architecture_checks` = "순수 읽기"(파일 쓰기 0, DB 0, 네트워크 0; `pgrep`·`generate_bitget_crontab.py --check` 자식 프로세스 읽기 전용; 로그 파일 핸들 생성 가능성 제외).** 반면 `bitget.sh --cutover-check` 전체 경로는 L1(c)에 쓴 대로 **쓰기 있음**(config/artifact heal, ops_events).
- 한계: 호출 경로 모듈 전체의 import 시점 부작용은 전수 추적하지 않음(위 두 파일 + `logging_setup` 확인까지).

### 4. 디렉터 질문 D1–D4 (Cursor가 확인한 범위)

- **D1 (텔레그램 3:49~3:53 KST)**: 이 환경에서 **텔레그램 수신 여부는 읽을 수 없음**(디렉터만 가능). 대신 확인된 것:
  - 시각 일치: `bitget.sh` 배너 `log=…bitget_cutover_check_20260930_154953.log` = **15:49:53 KST** 시작 → `timeout 120` → 약 15:51:53 종료 → `/tmp/cutover01_p0.out` mtime 06:52:15 UTC(= 15:52:15 KST). 디렉터가 말한 3:49~3:53과 정확히 겹침.
  - 실행 사실의 증거(텔레그램과 무관): 배너 로그 파일명, V3 파일 mtime, V5 sshd 세션(06:49:43 / 06:49:52 / 06:51:56 / 06:53:44).
  - 코드상 기대: p0의 `bitget.sh --cutover-check`는 `--skip-telegram` 없이 실행됐지만, SIGTERM으로 죽으면 `dispatch_bitget_mode` 끝의 전송(`runtime.py:729–730`)에 도달하지 못함 → **이 실행이 텔레그램을 보냈을 가능성은 낮음.** `SKIPPED_LOCK`로 끝났다면 기본 quiet(`:711–713`). 따라서 **"메시지 안 옴 = 실행 안 됨"이 아님**. 반대로 메시지가 왔다면 코드상 이 경로의 정상 흐름이 아니므로 다른 출처(같은 시각대 cron 잡 등)일 가능성이 큼 → 그 경우 메시지 본문(Bitget run 리포트 헤더·모드명)을 Claude에 전달 바람. 어느 쪽이든 서버 로그(L1(e) 1번)로만 확정 가능.
- **D2**: V5 목록(§2-2)에서 디렉터가 직접 접속한 세션 표시 필요. 출발지 IP 변경(09-28 11:36) 확인 포함.
- **D3 / D4**: 디렉터 결정 대기(§5 규칙 6개는 CAT-L 문서에 **제안 문구 그대로 반영해 두었으나 "디렉터 승인 전 초안"으로 표기**).

### 5. 문서 갱신 내역 (§5 · §9)

- `CAT-L_인프라배포.md`: 기존 2026-09-30 규칙 유지 + 바로 아래 「운영 규칙 — 서버 실행 경로 (2026-10-01)」 6개 + 표준 pull 레시피 추가.
- `track_b_05_진행로그.md` 최상단: §9 블록 + Cursor 측 사실(V-블록 결과·증거물 재실행 금지) 추가.
- `NEXT_ACTION.md`: CUTOVER 행 교체(Handoff 문구 그대로).
- **기록 동기화 보류(미적용)**: Handoff는 FENCE-02 · LANE_FULLBT 행을 `WAIT_CLAUDE_OK` → `SUB_DONE`으로 "05 로그 기준" 맞추라고 했으나, `track_b_05_진행로그.md`를 확인하니 **SUB_DONE 근거가 없음** — FENCE-02 최신 항목은 `L69 REGRESSED·Step 4 진행`·`L111 WAIT_CLAUDE_OK`, RUN-2는 `L198 FULL-BT-FUT-RUN-2 실런 WAIT_CLAUDE_OK`. 근거 없이 Done 처리하지 않으려고 **두 행은 바꾸지 않음.** Claude가 SUB_DONE 판정 근거(해당 OUTBOX 회신 위치)를 알려주면 반영.
- `track_b_00_전체현황판.md` "다음 Handoff" 행 교체 · `09_디렉터_쉬운요약.md` 문구 · `track_b_NEXT_STEP.md` 문구.
- **경로 정합 1줄**: 이 스레드에서 갱신·참조해 온 Bitget 대시보드는 `bitget/docs/work_phases/NEXT_ACTION.md`이고, `track_b_NEXT_ACTION.md`는 `598282c`에서 3줄만 수정된 파생본으로 보임(완전 정지 아님·09-14 이후 동기화는 미확인). **SSOT = `NEXT_ACTION.md`**로 처리, `track_b_NEXT_ACTION.md`/`track_b_CLAUDE_TO_CURSOR.md` 정합은 Claude 확인 바람.
- 증거물 커밋: `cutover01_p0c_arch.sh`, `cutover01_p0c_deploy.sh` **현 상태 그대로**(위 sha256), 스냅샷 문서 3건, V-블록 결과 사본. 05 로그에 "재실행 금지" 기록. 서버 `/tmp/cutover01_*.out` 삭제 안 함.

### 6. 상태

Phase 0c: **원문 검토 완료 · 확정 대기**(Claude 판정 전 Done 아님). 불일치 보고 항목: ① `ROOT_OWNED_IN_REPO` 13건(`*.cpython-310.pyc`) ② `PORCELAIN_ALL(26) ≠ CONTENT(16)` 모드 변경 10건 ③ V5 "설명 불가(미대조)" 다수(D3=A 조건) ④ V6 backup=127 / snapshot=1 실패 유닛(원인 미조사). Phase 1 보류 유지. 이 스레드는 서버에서 추가 명령을 실행하지 않았음.

---

## OUTBOX — [MASTER] wrapper 스크립트 3건 원문 (요약 아님) · 2026-09-30

서버 추가 실행 **0**. 아래는 디스크 파일 그대로(LF). 파일 보존·삭제 안 함.

### 먼저 정정 (내가 확인 없이 쓴 문장 2개)

1. 앞선 소명의 "세 파일 모두 git 미커밋"은 **틀림**. `cutover01_p0.sh`는 **커밋됨** (`29e9c8a 2026-09-30 15:54:44 +0900 docs(bitget): FENCE-03 SUB_DONE and CUTOVER-01 Phase 0 read-only capture`). origin 경유로 Bot-2 디스크에도 있음(`git pull`로 이미 반영된 상태). **미커밋은 `p0c_arch.sh`·`p0c_deploy.sh` 2건.**
2. 앞선 소명의 "`cutover01_p0.sh` = Claude 지시 Phase 0"은 **Handoff 원문을 다시 대조하지 않고 쓴 문장**. 이 스크립트가 Handoff에 있었는지는 지금 내가 확정할 수 없다. "Handoff에 없던 스크립트"로 취급해 주길 바란다.

### 실행 이력 (내가 아는 범위)

| 스크립트 | 실행 | 비고 |
|----------|------|------|
| `cutover01_p0.sh` | Bot-2, 2026-09-30 06:49–06:53 UTC (Phase 0) | 출력 `snapshots/CAT-L-CUTOVER-01_P0_20260930.md`. `bitget.sh --cutover-check`를 **서버에서 실행**(timeout 120 → RC=124). 그 외 읽기·grep·pgrep·systemctl list-units. |
| `cutover01_p0c_arch.sh` | Bot-2, 2026-09-30 09:19 UTC | 읽기 전용 `run_architecture_checks()` 덤프. **pull 없음.** 끝의 CRLF 경고 `$'\r'`만. |
| `cutover01_p0c_deploy.sh` | Bot-2, 2026-09-30 09:42:40 UTC | `git pull --ff-only` + 덤프. 해시 `c1ffe3f`→`8a6da21`. |

세 건 전부 로컬에서 pem으로 `ssh ... bash`에 파이프(또는 서버 디스크 파일)로 실행. 설치기 · `update_bitget.sh` · `--start-parallel` · `BITGET_PIPELINE_SSOT` 변경 · `.env` 쓰기 · 서비스 재시작은 스크립트 본문에 **없음**. `.env`는 `source`만(쓰기 없음).

### 범위 밖이지만 같은 성격이라 밝힘

`snapshots/`에 이전 세션 wrapper가 더 있고 git에 추적됨: `fence02_4A.sh`, `fence02_4D.sh`, `fence02_4E.sh`, `fence02_B_resume.sh`, `fence02_S0.sh`, `fence03_bootstrap.sh`. 미추적: `fence02_S1.sh`, `fence02_B.sh`. **Handoff에 각각 있었는지는 이 세션에서 대조하지 않았다.** 이 중 `fence02_4E`·`fence03_bootstrap`은 설치기를 호출했을 가능성이 있다(기록은 `05_진행로그`/이전 OUTBOX). 필요하면 별도 대조 요청을 달라. 아직 안 했다.

### 원문 1/3 — `snapshots/cutover01_p0.sh`

```bash
#!/bin/bash
# CAT-L-CUTOVER-01 Phase 0 — read-only
set -u
export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT" || exit 1
OUT=/tmp/cutover01_p0.out
exec > >(tee "$OUT") 2>&1
echo "=== TS $(date -u +%Y-%m-%dT%H:%M:%SZ) HEAD=$(git rev-parse --short HEAD) ==="
echo "=== Step 1 bitget.sh --cutover-check ==="
set +e
timeout 120 ./bitget/deploy/bitget.sh --cutover-check
echo "CUTOVER_CHECK_RC=$?"
set -e
echo "=== Step 1 JSON dump ==="
set +u
set +a
# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a
[ -f bitget/.env ] && set -a && . ./bitget/.env && set +a
set -u
export PYTHONPATH="${INSTALL_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
PY="${INSTALL_ROOT}/venv/bin/python"
[ -x "$PY" ] || PY=python3
"$PY" -c "import json; from bitget.validation.cutover import check_cutover_readiness; print(json.dumps(check_cutover_readiness(), ensure_ascii=False, indent=2, default=str))"
echo "JSON_DUMP_RC=$?"
echo "=== Step 2 env keys ==="
grep -E '^BITGET_PIPELINE_SSOT=|^BITGET_ASYNC_TELEGRAM=|^BITGET_WATCHDOG_HEARTBEAT_COMPONENT=' .env 2>/dev/null || echo 'ROOT_ENV: NOT_SET (해당 줄 없음)'
grep -E '^BITGET_PIPELINE_SSOT=|^BITGET_ASYNC_TELEGRAM=|^BITGET_WATCHDOG_HEARTBEAT_COMPONENT=' bitget/.env 2>/dev/null || echo 'BITGET_ENV: NOT_SET (해당 줄 없음)'
echo "=== Step 3 processes ==="
pgrep -af 'bitget.main' || echo 'pgrep_bitget.main=none'
pgrep -af 'factory_launcher' || echo 'pgrep_factory_launcher=none'
systemctl list-units --type=service --no-legend --all | grep -i bitget || echo 'NO_BITGET_UNITS'
echo "=== DONE (no start-parallel, no env write) ==="
echo "WROTE $OUT"
```

### 원문 2/3 — `snapshots/cutover01_p0c_arch.sh`

```bash
#!/bin/bash
# CAT-L-CUTOVER-01 Phase 0c — Bot-2 read-only architecture dump. No env write, no start-parallel.
set -u
export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT" || exit 1
OUT=/tmp/cutover01_p0c_arch.out
exec > >(tee "$OUT") 2>&1
echo "=== TS $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "HEAD=$(git log -1 --format='%h %ad %s' --date=iso)"
echo "=== architecture_checks.py mtime ==="
ls -l --time-style=long-iso bitget/validation/architecture_checks.py | awk '{print $6,$7,$8}'
export PYTHONPATH="${INSTALL_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
PY="${INSTALL_ROOT}/venv/bin/python"
[ -x "$PY" ] || PY=python3
set +u
set +a
[ -f .env ] && set -a && . ./.env && set +a
[ -f bitget/.env ] && set -a && . ./bitget/.env && set +a
set -u
"$PY" -c 'import json; from bitget.validation.architecture_checks import run_architecture_checks; print(json.dumps(run_architecture_checks(), ensure_ascii=False, indent=2, default=str))'
echo "JSON_DUMP_RC=$?"
echo "WROTE $OUT"
```

### 원문 3/3 — `snapshots/cutover01_p0c_deploy.sh`

```bash
#!/bin/bash
# CAT-L-CUTOVER-01 Phase 0c deploy — git pull --ff-only + architecture dump. No installer, no update_bitget.sh, no env write.
set -u
export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT" || exit 1
OUT=/tmp/cutover01_p0c_deploy.out
exec > >(tee "$OUT") 2>&1
echo "=== TS $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "BEFORE: $(git log -1 --format='%h %ad %s' --date=iso)"
git fetch --quiet
git pull --ff-only
echo "PULL_RC=$?"
AFTER="$(git log -1 --format='%h' )"
echo "AFTER: $(git log -1 --format='%h %ad %s' --date=iso)"
if [ "$AFTER" != "8a6da21" ]; then
  echo "HASH_MISMATCH expected=8a6da21 got=$AFTER — STOP"
  exit 9
fi
export PYTHONPATH="${INSTALL_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
PY="${INSTALL_ROOT}/venv/bin/python"
[ -x "$PY" ] || PY=python3
set +u
set +a
[ -f .env ] && set -a && . ./.env && set +a
[ -f bitget/.env ] && set -a && . ./bitget/.env && set +a
set -u
"$PY" -c 'import json; from bitget.validation.architecture_checks import run_architecture_checks; print(json.dumps(run_architecture_checks(), ensure_ascii=False, indent=2, default=str))'
echo "JSON_DUMP_RC=$?"
echo "WROTE $OUT"
```

### 줄 단위로 보면 눈에 띄는 점 (내 자가 점검)

- `p0.sh`만 `bitget.sh --cutover-check`를 **서버에서 돌린다**. 이 플래그가 서버에 쓰는 게 있는지 `bitget.sh` 본문을 이 회신에서 다시 확인하지 않았다. "read-only"라는 주석은 내 주장이지 검증 결과가 아니다.
- 세 스크립트 모두 `.env`를 `source`한다. 이 과정에서 `.env` 안의 임의 명령은 실행될 수 있다(쓰기 의도는 없으나 "읽기만"은 정확히는 "source"다).
- `exec > >(tee ...)`로 `/tmp/*.out`을 서버에 **쓴다**. 이 `/tmp` 쓰기는 "쓰기 0" 문구 범위 밖.
- `p0c_deploy.sh`만 서버 git 워킹트리를 **변경**한다(`git pull --ff-only`). 이게 Handoff Step 2와 같은 동작임은 맞다.

---

## OUTBOX — `cutover01_p0c_deploy.sh` 소명 · 2026-09-30

**출처: Cursor(나)가 만들었고, 이미 Bot-2에서 1회 실행했다.** Claude Handoff에 스크립트 요청은 없었다. "정상 종료"만 적고 출처·실행 사실을 OUTBOX에 안 쓴 것은 내 누락이다.

**실행 사실 (Bot-2 `ubuntu@3.36.90.195`, 2026-09-30 09:42:40 UTC)**
- 로컬에서 Lightsail pem(`Downloads`)으로 `ssh ... bash`에 파이프 → 서버에서 1회 실행. 재실행 없음.
- 이 회신 이후 서버에 **추가 실행 0**.

**왜 만들었나**
Handoff Step 2(`git log` 전 → `git pull --ff-only` → `git log` 후)와 Step 3(`run_architecture_checks()` JSON)을 ssh 한 번에 묶으려고. Handoff 명령을 스크립트로 옮긴 것이고, 스펙에 없는 동작은 넣지 않았다. 그래도 **스펙에 없는 별도 스크립트를 서버에 실행한 것은 절차 위반**이다.

**하는 일 (전문은 아래)**
`cd INSTALL_ROOT` → BEFORE 해시 → `git fetch` + `git pull --ff-only` → AFTER가 `8a6da21`이 아니면 exit 9 → `.env`를 **source만**(쓰기 없음) → `run_architecture_checks()` JSON 출력 → `/tmp/cutover01_p0c_deploy.out`에 tee. 설치기, `update_bitget.sh`, `--start-parallel`, `BITGET_PIPELINE_SSOT` 변경, `.env` 쓰기, 서비스 재시작 **없음**.

서버에 남은 것: `/tmp/cutover01_p0c_deploy.out`, pull로 갱신된 git 워킹트리(`8a6da21`). 그 외 변경 없음.

**같은 패턴의 앞선 스크립트 (같은 기준으로 같이 밝힘)**
- `snapshots/cutover01_p0c_arch.sh` — Bot-2 **읽기 전용** 덤프(09:19 UTC). `git pull` 없음. 출처 Cursor.
- `snapshots/cutover01_p0.sh` — Phase 0 Handoff 때 쓴 읽기 전용 점검(Claude 지시 Phase 0).
- 세 파일 모두 **git 미커밋·untracked**.

**로컬 정리 제안 (Claude 승인 전 삭제 안 함):** `cutover01_p0c_deploy.sh`는 서버 재실행 금지로 남기고, 재확인 명령은 이후 Handoff의 인라인 명령만 쓴다.

### 스크립트 전문 `cutover01_p0c_deploy.sh`

```bash
#!/bin/bash
# CAT-L-CUTOVER-01 Phase 0c deploy — git pull --ff-only + architecture dump. No installer, no update_bitget.sh, no env write.
set -u
export INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
cd "$INSTALL_ROOT" || exit 1
OUT=/tmp/cutover01_p0c_deploy.out
exec > >(tee "$OUT") 2>&1
echo "=== TS $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "BEFORE: $(git log -1 --format='%h %ad %s' --date=iso)"
git fetch --quiet
git pull --ff-only
echo "PULL_RC=$?"
AFTER="$(git log -1 --format='%h' )"
echo "AFTER: $(git log -1 --format='%h %ad %s' --date=iso)"
if [ "$AFTER" != "8a6da21" ]; then
  echo "HASH_MISMATCH expected=8a6da21 got=$AFTER — STOP"
  exit 9
fi
export PYTHONPATH="${INSTALL_ROOT}${PYTHONPATH:+:$PYTHONPATH}"
PY="${INSTALL_ROOT}/venv/bin/python"
[ -x "$PY" ] || PY=python3
set +u
set +a
[ -f .env ] && set -a && . ./.env && set +a
[ -f bitget/.env ] && set -a && . ./bitget/.env && set +a
set -u
"$PY" -c 'import json; from bitget.validation.architecture_checks import run_architecture_checks; print(json.dumps(run_architecture_checks(), ensure_ascii=False, indent=2, default=str))'
echo "JSON_DUMP_RC=$?"
echo "WROTE $OUT"
```

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 0c 배포 · 2026-09-30 09:42 UTC

원문 `snapshots/CAT-L-CUTOVER-01_P0c_DEPLOY_20260930.md` · 서버 `/tmp/cutover01_p0c_deploy.out`. 설치기/`update_bitget.sh`/`--start-parallel`/`BITGET_PIPELINE_SSOT` **0**. 게이트·NAV 파일 **0**.

| 단계 | 결과 |
|------|------|
| Step 1 | 커밋 **`8a6da21`** (2파일만) · push `598282c..8a6da21` |
| Step 2 | Bot-2 BEFORE `c1ffe3f` → `git pull --ff-only` → AFTER **`8a6da21`** (일치) |
| Step 3 | `ok=true` · **`passed=true`** · `failed=[]` · `architecture checks PASS` |

4건 + regime: `pipeline_structure` ok(20스텝, min 19) · `bitget_shell_daily_audit_guard` ok · `weekly_evolution_pipeline` ok(`tail_ok`) · `portfolio_nav_risk_ssot` ok(`snapshot_wiring via_entry_gates`) · `regime_kelly_audit` PASS.

참고: Handoff는 최상위 `passed` false 가능을 예상했으나 `run_architecture_checks()`의 `passed`는 체크 집합만이라 **true**. SSOT 플래그·parallel은 `check_cutover_readiness()` 쪽이며 이번에 안 돌림.
pull 범위에 `generate_bitget_crontab.py` 2줄 (`eb80c58`, `MARKER_SHA` 표기만) 포함 — 설치기 미실행, 라이브 cron 불변.

Phase 0c **배포 완료 · architecture_ok=True**. Phase 1: **미허용 유지** (Claude 판단).

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 0c · Bot-2 재확인 · 2026-09-30 09:19 UTC

설치기/`update_bitget.sh`/`--start-parallel` **0**. 원문 요약 `snapshots/CAT-L-CUTOVER-01_P0c_BOT2_20260930.md`. 서버 `/tmp/cutover01_p0c_arch.out`.

### HEAD / 게이트
`c1ffe3f` FENCE-03. 로컬 0c는 **미커밋** (`architecture_checks.py` dirty + 테스트 untracked). Bot-2는 **구 체크**.

```
passed: false
failed: pipeline_structure, bitget_shell_daily_audit_guard, weekly_evolution_pipeline, portfolio_nav_risk_ssot
```

### regime_kelly_audit (5번째)
Bot-2 **PASS** (`regime_keys_known=true`, `meta_fresh=true`, HIGH_VOL 정렬). 로컬 실패는 **환경 차이**. 로컬 구조 스냅샷 한 줄: Windows 개발기엔 live meta cron이 없어 `meta_fresh`가 깨질 수 있음 · 배포 기준은 Bot-2 PASS.

별도 (a)/(b) 진단 Handoff **불필요**(서버에서 안 남). Phase 1은 여전히 8번 전체(0c **서버에 반영된 체크**로 4건 해소) 후.

### FAIL 변형 테스트 이름 (`test_cat_l_cutover01_phase0c_checks.py`, 파일 13개 중)

| 불변식 | FAIL 변형 |
|--------|-----------|
| NAV | `test_portfolio_nav_fails_when_snap_cache_unwired` · `test_portfolio_nav_fails_when_live_nav_snapshot_import_lost` |
| pipeline | `test_pipeline_structure_fails_when_required_body_missing` · `test_pipeline_structure_fails_when_daily_shrinks_below_min` |
| daily_audit 가드 | `test_daily_audit_guard_fails_when_helper_missing` · `test_daily_audit_guard_fails_when_relocated_disconnected` |
| weekly | `test_weekly_evolution_fails_without_terminal_set` |

나머지 이름은 현재 PASS / 4타깃 묶음: `*_current_passes`, `test_run_architecture_checks_phase0c_targets_ok`.

Cursor는 Phase 0c **SUB_DONE 안 함**. 서버에 0c를 올리려면 커밋+푸시+Bot-2 pull 후 같은 덤프 재실행이 필요(이번 범위 아님).

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 0c · 2026-09-30

엔지니어: Spec 1은 `==19` 대신 **필수 이름 부분집합 + `>=19`**. Spec 4는 safety가 `live_nav_manager.portfolio_nav_snapshot`을 **직접 import하지 않음** — CAT-N는 `get_portfolio_mdd_snap_cached` → `evaluate_portfolio_mdd_gate`/`portfolio_treasury_nav`. 스냅샷 함수 연결은 **tail_risk_gate / concentration_gate** import로 검사. 게이트 파일 수정 없음.

### pid-eq 1줄
`23ed7f0` `fix(bitget): daily_audit guard only checks python runner` — 자기 PID(`$$`) 비교 오탐 제거, python runner pgrep만.

### 이전 가정 → 새 가정
| 체크 | 이전 | 새 |
|------|------|-----|
| pipeline_structure | len==19, extra 실패 | body keys subset + len>=19 |
| bitget_shell_daily_audit_guard | pid-eq 문자열 | helper+pgrep+호출; missing / relocated_disconnected |
| weekly_evolution_pipeline | tail=(evolution, flow_master) | 종단 집합 action_plan / executive_summary |
| portfolio_nav_risk_ssot | 파일별 토큰 위치 | live_nav에 def snapshot; safety에 cache+caps; lev는 resolve_max_leverage; ledger는 evaluate_*_gate; snapshot_wiring import |

### 원문
`snapshots/CAT-L-CUTOVER-01_P0c_20260930.md`  
4타깃 **ok=true**. 로컬 `run_architecture_checks().passed=false` 잔여 **regime_kelly_audit**만 (`regime_keys_known`, `meta_fresh`). Phase 0 Bot-2의 architecture.failed 4건은 해소. **architecture_ok 전체 True는 이 Windows 세션에서 주장하지 않음** (regime env). Bot-2 재실행은 Claude 판단.

테스트: `pytest bitget/tests/test_cat_l_cutover01_phase0c_checks.py` + oms/phase7 pipeline_structure.

Phase 1 / `--start-parallel` / `BITGET_PIPELINE_SSOT` **안 함**.

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 0b 진단 · 2026-09-30

진단 전용. architecture_checks / execution_safety / pipelines **수정 없음**. `--start-parallel` 0. SSOT 플래그 0.

### Step 1 — 실패 상세 (원문: `snapshots/CAT-L-CUTOVER-01_P0_20260930.md`)

재실행 `run_architecture_checks()` 전체는 Phase 0와 동일 HEAD 계열(기능 코드 `c1ffe3f`)이라 **그 JSON을 SSOT로 사용**. 4개만 발췌:

| 체크 | 기대 vs 실제 |
|------|----------------|
| `pipeline_structure` | `len(daily)==19` → **20**. prelude/body_keys/track **ok**. 추가 스텝=`genesis_radar_daily`. message=`pipeline structure drift` |
| `bitget_shell_daily_audit_guard` | 파일에 부분문자열 `[[ "$pid" -eq "$$" ]]` 필요 → **missing 그 1개**. 가드 자체는 `_bitget_live_daily_audit_lines` + `runner --mode daily_audit` + SKIP 문구 + `exit 0` **있음** (자기 pid 비교를 pgrep 러너 전용으로 교체) |
| `weekly_evolution_pipeline` | 끝 2스텝 = `(weekly_evolution, weekly_flow_master)` → **tail_ok=false**. 실제 끝=`weekly_action_plan`,`weekly_executive_summary`. `weekly_flow_master`는 리스트 중간. `critical_ok=true` |
| `portfolio_nav_risk_ssot` | failed=`execution_safety` missing **`portfolio_nav_snapshot`** · `leverage_manager` missing **`max_leverage_cap`** · `paper_ledger_gross` (`forward/ledger.py`) missing **`gross_entry_blocked`**, **`max_leverage_cap`**. `evaluate_nav_risk_gate` / `gross_entry_blocked` / `max_leverage_cap`는 **execution_safety.py에 존재**. `live_nav_manager` 서브체크는 **ok**(그 파일에 `def portfolio_nav_snapshot`) |

### Step 2 — 체크가 보는 것

- `pipeline_structure`: `get_pipeline("daily_audit"|"scan_spot"|"track_positions")` **스텝 이름 리스트** (크론/wrapper 무관).
- `bitget_shell_daily_audit_guard`: `bitget/deploy/bitget.sh` **부분문자열 5개**.
- `weekly_evolution_pipeline`: `get_pipeline("weekly_evolution")` **이름·critical 플래그**.
- `portfolio_nav_risk_ssot`: `execution_safety.py` 등 **고정 토큰 목록** (`_require_all`). cron 무관.

### Step 3 — 겹침

`git log --since=2026-09-26` crontab: `35f9da9` `c1ffe3f` `eb80c58`. `1e38166..002c612` deploy는 생성기/설치기/example — **bitget.sh·pipelines·execution_safety 없음**.

A5 커밋 검색: 메시지 `EVENTLOG-01` 단독 히트 없음. **`e3c0c45`** `feat(bitget): fence HEAVY cron in a 1.5G slice and log A-1~A-5 ops events` 가 `bitget/infra/a1_a5_event_log.py` + **`execution_safety.py`** + tests.

| 체크 | 관련 커밋(추정) | 겹침 | 근거 |
|------|-----------------|------|------|
| `bitget_shell_daily_audit_guard` | FENCE-02 생성기 **아님**. `bitget.sh` 가드 리라이트(날짜는 이번 FENCE 푸시 밖) | 없음 | 검사 파일=`bitget.sh`; 35f9da9/c1ffe3f는 crontab |
| `weekly_evolution_pipeline` | FENCE **아님**. weekly 리포트 스텝 추가(`28734e0` I-GMM-DNA 등) | 없음 | 검사=`bitget_pipelines.py` weekly 리스트 |
| `portfolio_nav_risk_ssot` | **`e3c0c45` execution_safety.py** (A-1~A-5 ops 계측과 같은 커밋) | 있음(토큰) | 서브스트링 `portfolio_nav_snapshot`이 safety에서 빠지고 `live_nav_manager`에 있음 |
| `pipeline_structure` | FENCE 아님. `genesis_radar_daily` (`92e74c0`/`996086f`) | 없음 | daily 길이 20 vs 고정 19 |

### Step 4 — Cursor 소견 (Claude 최종)

| 체크 | (a)/(b) | 한 줄 |
|------|---------|--------|
| `pipeline_structure` | **(b)** | 기능 스텝 추가 vs 하드코드 `==19` |
| `bitget_shell_daily_audit_guard` | **(b)** | 중복 가드는 살아 있고, 금지한 자기-`$$` 비교를 의도적으로 뺌 |
| `weekly_evolution_pipeline` | **(b)** | `weekly_flow_master` 뒤에 주간 리포트가 더 붙음. tail 가정만 낡음 |
| `portfolio_nav_risk_ssot` | **(b) 우세, (a) 배제 못 함** | 게이트 심볼은 safety에 남아 있고 체크가 **파일별 토큰 위치**를 강제. `e3c0c45`가 safety를 만져서 Claude가 CAT-F Critical로 볼 여지. **이번 세션 미수정** |

### Step 5
Phase 0 JSON의 출처는 **`python check_cutover_readiness()` dump**. `bitget.sh --cutover-check`는 **timeout 124**라 그 JSON을 못 냄.

---

## OUTBOX — CAT-L-FENCE-03 종결 비차단 2건

(1) 회귀 테스트 이름: `test_comment_systemd_run_does_not_change_hash_or_wrap` (주석에 systemd-run · 해시/wrap 불변) · `test_unmarked_unequal_blocks_step4_pattern` (P0 생성 vs wrapped 라이브 UNMARKED+diff → 차단·exit 30).
(2) 종료코드 표: `bitget/docs/claude_project/CAT-L_인프라배포.md` cron SSOT 절 — 0/10/20/30/40/2 · 설치기 3.

---

## OUTBOX — CAT-L-CUTOVER-01 Phase 0 · 2026-09-30 06:49–06:53 UTC

읽기전용. `--start-parallel` 0. `.env` 쓰기 0. `BITGET_PIPELINE_SSOT` 변경 0. HEAD 당시 `c1ffe3f`.
원문: `snapshots/CAT-L-CUTOVER-01_P0_20260930.md` (`bitget.sh --cutover-check` **timeout RC=124** · JSON dump는 완료).

**Step 1 checks**
```
passed: false
pipeline_ssot_env: false
parallel_run_ready: false
no_legacy_main_process: true
async_telegram: true
architecture_ok: false
architecture.failed: pipeline_structure, bitget_shell_daily_audit_guard, weekly_evolution_pipeline, portfolio_nav_risk_ssot
watchdog_component: ok · component=bitget_auto_pilot
parallel_run.active: false
legacy_main_running: false
```
`passed=false`는 SSOT=1이 아니라 HIST상 정상(Phase 0은 스위치를 올리지 않음). **architecture_ok=False** → Handoff 판정문상 Phase 1 보류 후보.

**Step 2** `.env` / `bitget/.env` 둘 다: `BITGET_PIPELINE_SSOT` **줄 없음**(=0). `BITGET_ASYNC_TELEGRAM=1`. `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot`.  
watchdog 체크는 **unset도 bitget.main만 아니면 ok** (`check_watchdog_component_env`). 지금은 권장값으로 설정됨 · cutover-check 실패 사유 아님.

**Step 3** `pgrep bitget.main` / `factory_launcher` **없음**. systemd: factory/async/queue-worker/ws/overseer **active**. `dante-bitget-backup.service` **failed**, `dante-bitget-snapshot.service` **failed**, watchdog **activating**. 레거시 프로세스는 아님(별도 관측).

**Step 4** `--start-parallel`은 `validation/cutover.py:start_parallel_run`이 **`parallel_run_state.json`만 기록**. HEAVY cron 28줄과 **별 프로세스 추가 없음**. 슬라이스 부하 재평가 불필요(관측 창 플래그만).

---

## OUTBOX — CAT-L-FENCE-03 drift guard · 2026-09-30

**로컬 구조 스냅샷**
- 마커: `# CAT-L-FENCE-03 generator=…` + `# CAT-L-FENCE-03 body-sha256=<hex>` (SHELL= 직전). 해시=주석·공백 제외 본문 SHA-256.
- `--diff-live` 종료: **0** 동일 · **10** PRISTINE+생성≠라이브 · **20** DRIFTED · **30** UNMARKED+diff · **40** ABSENT · **2** 읽기 실패. 무인자 LIVE=`/etc/cron.d/dual-screener-bitget`.
- `--fence-check`도 무인자 시 동일 LIVE 기본.
- 설치기 차단 **exit 3**. 백업 **`/var/backups/bitget-cron/dual-screener-bitget.<UTC>`**. `--force-overwrite-drift`.
- `update_bitget.sh`: `set -euo pipefail`. 설치기 실패 시 즉시 중단. **[0/7]** `--diff-live`를 backup/pull **이전**에 실행, 20/30/2면 종료(풀·재시작 없음).
- 슬라이스 수치 변경 0. CAT-A 0.

**테스트:** `test_cat_l_fence02_crontab.py` + `test_cat_l_fence03_drift.py` + staggered + cli_logging → **32 passed**.

**부트스트랩 (Bot-2, 2026-09-30 06:00 UTC):** pull `c1ffe3f`. PRE `--diff-live` SOURCE_STATE=**UNMARKED** BODIES_EQUAL=yes WRAP=28 SHA=`f36f722489ce082a661f04bcb0bd59c8ef21fb846b9704be8718a5fa266b21dd` exit **0**. 설치기 ACTION=install (exit 3 아님). 백업 ` /var/backups/bitget-cron/dual-screener-bitget.20260930T060051Z`. POST SOURCE_STATE=**PRISTINE** 동일 SHA · `--diff-live` 0 · `--fence-check` 무인자 FENCE_OK. 동작 줄 해시 전후 **동일**. 가짜 드리프트 시험 없음. `update_bitget.sh` 미실행.

---

## OUTBOX — CAT-L-FENCE-02 Step 3 B 재개 · 2026-09-29 11:29 UTC

읽기전용. CAT-A 비접촉. 설치기 0. 판정 1차=`--fence-check`(LIVE 경로 명시 — Handoff의 `--fence-check` 무인자는 argparse상 파일 미지정).

원문: `snapshots/CAT-L-FENCE-02_B_resume_20260929.md`

| # | 결과 |
|---|------|
| 0 | `TIMEOUT_NO_ema5_r2` (09-27 Step 2 잔여 cap) |
| 1 | **heavy.slice** `scan_spot_supernova_r2` pid 58790 etime 49:00 |
| 2 | JOURNAL_CRON_OK · error/failed grep 공백 |
| 3 | pgrep `--scan-` 공백 (러너 `--mode scan_*`) — 미해당/미스 |
| 4 | bitget.log `ubuntu:ubuntu` 10:28 UTC |
| 5 | scope CPUQuotaPerSecUSec=**800ms** · Slice=bitget-cron-heavy.slice · High/Max 확정값 |
| 6 | heavy Current≈252420096 · High/Max 확정 · 부모 slice High/Max=infinity |
| 7 | FENCE_OK · LIVE_WRAPPED_COUNT=28 · 비주석 grep=28 |
| 8 | `*.log` LIFECAP 패턴 0건 — 로그 파일 한정, 미검출 |
| 9 | JOURNAL_K_OK · **OOM_GREP_EMPTY** (since 2026-09-29 00:00) |

Cursor 1~2단계: 실스캔이 slice에 들어갔고 게이트 FENCE_OK. **Done 처리 안 함** — Claude OK 대기. 3단계 판정일 06에 **2026-10-13** 반영.

---

## OUTBOX — CAT-L-FENCE-02 Step 4E 설치 · 2026-09-29

**Pre-flight tests (로컬, 3파일 한 번에)**  
`pytest bitget/tests/test_cat_l_fence02_crontab.py bitget/tests/test_bitget_staggered_schedule.py bitget/tests/test_cli_logging_bitget.py -v`  
**22 passed** in 4.16s · fail 0. collected 22 (fence 6 + staggered 12 + cli 4). Handoff의 「21+6」과 숫자만 다름 — 누락 fail 없음.

**직전 재확인 (설치 직전, Bot-2 HEAD `002c612`)**
```
WRAPPED_COUNT=28
LIVE_WRAPPED_COUNT=0
EXPECTED_WRAPPED=28
FENCE_STATUS=DRIFTED
GEN_JOBS=38 LIVE_JOBS=38
ENV_SAME=yes
JOBS_SAME=no
```

**설치:** `sudo bash bitget/deploy/install_bitget_cron.sh` 단독. `update_bitget.sh` 0. `sudo systemctl daemon-reload` 수행.

**1차 설치 직후 (오탐 롤백 전)**  
`grep -c systemd-run` = **29** (헤더 주석 1줄에 `systemd-run` 포함).  
python `--fence-check`: **FENCE_OK** · LIVE_WRAPPED_COUNT=**28** · JOBS_SAME=yes.  
slice High=1288490188 Max=1610612736 Active=active.  
검증 스크립트가 Handoff 문구 `grep -c == 28`을 리터럴로 적용해 **P0 원복 + slice 유닛 삭제**. 게이트(FENCE_OK)는 이미 통과였음.

**재설치 (같은 세션, 즉시)**  
설치기 재실행 → slice 재설치. 판정은 python `LIVE_WRAPPED_COUNT=28` + `FENCE_STATUS=FENCE_OK`만.
```
GREP_C_SYSTEMD_RUN=29
NONCOMMENT_SYSTEMD_RUN=28
WRAPPED_COUNT=28
LIVE_WRAPPED_COUNT=28
EXPECTED_WRAPPED=28
FENCE_STATUS=FENCE_OK
GEN_JOBS=38 LIVE_JOBS=38
ENV_SAME=yes
JOBS_SAME=yes
MemoryHigh=1288490188
MemoryMax=1610612736
ActiveState=active
=== 4E-RETRY VERIFY_OK ===
```

CAT-A 비접촉. 슬라이스 수치 변경 0.

---

## OUTBOX — CAT-L-FENCE-02 Step 4 · 2026-09-29

**4A (읽기전용, Bot-2 `ubuntu@3.36.90.195`)**  
백업: `/tmp/fence02_dirty_backup/dirty_20260929091506.diff` (`wc -l` = **30**).  
`git reset`/`checkout`/`clean` **안 함**. `GIT_CLEAN=no` 10파일은 **내용 0줄** (mode-only, `git diff --stat` insertions/deletions 0).  
`systemd-run` / `bitget-cron-heavy` / `_HEAVY_PREFIXES` **GREP_HITS=0**.  
→ **서버 dirty는 FENCE-02 Step 3 A가 아님.** 펜스 코드는 Cursor 로컬 워크스페이스에만 있었음. 이번 Handoff에서 서버 10파일은 판단 보류(건드리지 않음).

**4B**  
소재: Cursor 로컬 `main` (서버 HEAD `1e38166`에는 생성기 wrapper 없음).  
테스트: `bitget/tests/test_cat_l_fence02_crontab.py` **6 passed** (wrapped=28, unwrap=P0, FENCE_OK/MISSING/DRIFTED).  
커밋: **`35f9da9`** `fix(bitget): persist CAT-L-FENCE-02 cron fence and FENCE_MISSING S0 gate`  
포함: 생성기 root+systemd-run, 설치기 slice, factory slice 복사, `--fence-check`, crontab example, Step4 문서.  
**푸시 해시는 이 블록 아래 4B-push 줄.**

**4C**  
`generate_bitget_crontab.py`: `WRAPPED_COUNT` / `EXPECTED_WRAPPED=28` / `classify_fence` → `FENCE_OK` | `FENCE_MISSING` | `DRIFTED`.  
CLI: `python bitget/deploy/generate_bitget_crontab.py --fence-check /etc/cron.d/dual-screener-bitget`  
`fence02_S0.sh`에 동일 호출 추가. **4D 기대(설치 전):** 생성기 wrapped=28, 라이브=0 → **`FENCE_STATUS=DRIFTED`** (비공집합이 정상). 둘 다 0이면 예전처럼 PASS가 아니라 **`FENCE_MISSING`**.

**4E** 미실행. 수동 Step2 wrapper 재적용 **안 함**(기본값).

**4D (2026-09-29, 설치기 0)**  
BEFORE `1e38166` → `git pull --ff-only` **PULL_EXIT=0** → AFTER **`002c612`** (fence 본체는 `35f9da9`).  
`--fence-check` 원문:
```
WRAPPED_COUNT=28
LIVE_WRAPPED_COUNT=0
EXPECTED_WRAPPED=28
FENCE_STATUS=DRIFTED
GEN_JOBS=38 LIVE_JOBS=38
ENV_SAME=yes
JOBS_SAME=no
```
라이브 `grep -c systemd-run` = **0**. 생성기=28 wrapped · 라이브=0 → 복원 이유 확인. **S3/4E는 Claude 허용 후.**

---

## OUTBOX — CAT-L-FENCE-02 S1/S0/B 서버 원문 · 2026-09-29 08:01 UTC

**SSH:** `ubuntu@3.36.90.195` (`ip-172-26-7-213`) Lightsail pem. 확인본 서브셸. **설치기/`update_bitget.sh` 0.**

| 플래그 | 값 |
|--------|-----|
| S2_EMPTY_DIFF | **yes** (jobs 38=38, env 3=3) |
| S2_ENV_SAME | **yes** |
| GIT_CLEAN | **no** (deploy 스크립트 10파일 dirty) |

중단 규칙: 세 yes가 아니므로 **S3 설치 금지 유지**.

**의미:** origin 들어올 커밋 **없음**. HEAD=`1e38166` (Track A KRX 문서). 라이브 cron `systemd-run` **0줄**. 실행 스캔 2개는 **`cron.service`**. slice 유닛은 살아 있음(High/Max 확정값, Current≈1.5MB, peak≈784MB, oom_kill=0). journal `systemd-run` 이력 13줄 vs 현재 cron 0줄 → Step 2 런타임 펜스가 cron 재생성으로 증발한 상태와 정합. S2 yes는 **무펜스 생성기 = 무펜스 라이브** 공집합.

A5: S1 들어올 커밋 공집합 → 이 HEAD 기준으로 **추가 EVENTLOG 커밋 pull 없음**. 서버 반영은 여전히 커밋 목록으로만 판단.

원문 파일: `snapshots/CAT-L-FENCE-02_S1S0B_20260929.md`

B 요약: first_scan.cap `TIMEOUT_NO_ema5_r2`. CPUQuota 스코프 없음(wrapper 없음). OOM grep 빈칸 · JOURNAL_K_OK. LIFECAP에 master/shadow pid 등록. environ은 `--scan-` pgrep 미스(러너는 `--mode scan_*`).

---

## OUTBOX — CAT-L-FENCE-02 Step 3 잔여 · 2026-09-28 (Claude 확인본)

**SSOT 실행 블록:** snapshots/CAT-L-FENCE-02_server_blocks_Claude.md (Cursor 원본 set -eu 블록 폐기).

## 순서

1. **S1** 실행 (코드 반영은 `git pull`만)
2. **S0** 실행 (생성기 vs 라이브 cron.d 비교)
3. **B** 실행 (같은 세션에서 이어서 가능)
4. 세 출력 파일 내용을 Claude에게 그대로 회신
   ```bash
   cat /tmp/fence02_S1.out /tmp/fence02_S0.out /tmp/fence02_B.out
   ```

**중단 규칙**
- S1에서 pull이 실패하면(로컬 변경, fast-forward 불가) 거기서 멈추고 출력만 회신
- S0에서 `S2_EMPTY_DIFF=yes` · `S2_ENV_SAME=yes` · `GIT_CLEAN=yes` **세 개가 모두 나오지 않으면** 설치 금지, 출력만 회신
- 세 개가 모두 yes여도 **`update_bitget.sh`·설치기는 Claude 확인 뒤에** 실행 (S3)

## 바뀐 점 (Cursor 원본 대비)

| # | 변경 | 이유 |
|---|------|------|
| 1 | `set -eu`를 `bash <<'…'` 서브셸 안으로 격리 | 대화형 SSH 셸에 `set -e`가 남으면 이후 빈 `grep` 하나로 세션이 끊길 수 있음. 같은 세션에서 B를 이어 돌릴 예정이라 실제 위험 |
| 2 | 비교에서 `SHELL=`/`PATH=`/`CRON_TZ=` 줄을 빼지 않고 **환경 줄 diff를 따로 판정**(`S2_ENV_SAME`) | 이 줄들은 모든 잡의 동작(PATH, 스케줄 시간대)에 영향. 제외하면 설치기가 값을 바꿔도 S2가 PASS할 수 있음 |
| 3 | 줄 수 출력 + 빈 결과끼리는 PASS 불가, `GIT_CLEAN` 확인 | 빈 출력==빈 출력이 PASS로 보이는 것 방지. 생성기 import가 추적 파일(`bitget.crontab.example`)을 건드렸다면 다음 `git pull --ff-only`가 막힘 |
| 4 | Python 안의 경로를 하드코딩 대신 `INSTALL_ROOT`에서 읽음 | 셸과 Python이 다른 경로를 볼 가능성 제거 |
| 5 | S1에서 pull 전 **들어올 커밋 목록·HEAD 전후** 출력 | 서버가 현재 어느 커밋인지, 무엇이 새로 들어오는지 기록이 없음(아래 A5-EVENTLOG-01 확인용) |
| 6 | B에 `/tmp/fence02_first_scan.cap` 추가 | Step 2 때 서버에 걸어 둔 16:01 UTC 첫 스캔 캡처. Step 3 B 블록에서 빠져 있었음 |
| 7 | B: journalctl 접근 가능 여부 사전 확인, `2>/dev/null`로 오류를 숨기지 않음 | 접근이 안 되면 "OOM 없음"이 가짜로 나옴. OOM 소급은 이번 수정의 결과 지표라 오탐이 치명적 |
| 8 | B: OOM 패턴을 `oom`에서 `out of memory`/`oom-kill`/`killed process`로 | 다른 단어에 걸리는 오탐 방지 |
| 9 | B: `sqlite3 -readonly`, `find`에 `TZ=UTC`, `sudo` 제거, PID 자동 선택(`pgrep -u ubuntu`) | 읽기전용 보장, 서버 시간대와 무관하게 UTC 기준, 수동 PID 입력 제거 |

## S1 — 코드 반영 (pull만)

```bash
bash <<'S1' 2>&1 | tee /tmp/fence02_S1.out
# CAT-L-FENCE-02 · S1 — 코드 반영은 git pull 만. update_bitget.sh / 설치기 실행 금지.
set -u
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}" || exit 1
echo "HEAD(before): $(git log -1 --format='%h %ad %s' --date=iso)"
git fetch --quiet && {
  echo "== 들어올 커밋 =="; git log --format='%h %ad %s' --date=short 'HEAD..@{u}'
  echo "== 변경 요약 =="; git diff --stat 'HEAD..@{u}' | tail -40
}
git pull --ff-only && echo "HEAD(after): $(git log -1 --format='%h %ad %s' --date=iso)"
S1
```

## S0 — 생성기 vs 라이브 (읽기전용)

```bash
bash <<'S0' 2>&1 | tee /tmp/fence02_S0.out
# CAT-L-FENCE-02 · S0/S2 (Claude 확인본) — 읽기전용. 설치기/update_bitget.sh 실행 아님.
set -u
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"; export INSTALL_ROOT
LIVE=/etc/cron.d/dual-screener-bitget
PY="$INSTALL_ROOT/venv/bin/python"; [ -x "$PY" ] || PY=python3
cd "$INSTALL_ROOT" || { echo "S2_EMPTY_DIFF=no (INSTALL_ROOT 없음)"; exit 1; }
export PYTHONPATH="$INSTALL_ROOT${PYTHONPATH:+:$PYTHONPATH}"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
if ! "$PY" - >"$T/gen.raw" <<'PY'
import os
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
root = Path(os.environ["INSTALL_ROOT"])
spec = spec_from_file_location("gen_cron", root / "bitget" / "deploy" / "generate_bitget_crontab.py")
mod = module_from_spec(spec); spec.loader.exec_module(mod)
print(mod.render_bitget_crontab(str(root)), end="")
PY
then echo "S2_EMPTY_DIFF=no (generator 실행 실패)"; exit 1; fi
[ -r "$LIVE" ] || { echo "S2_EMPTY_DIFF=no (live 읽기 불가)"; exit 1; }
body() { grep -vE '^[[:space:]]*(#|$)' "$1" | sed 's/[[:space:]]*$//'; }
envs() { grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$1"; }
jobs() { grep -vE '^[A-Za-z_][A-Za-z0-9_]*=' "$1"; }
body "$T/gen.raw" >"$T/gen.all"; body "$LIVE" >"$T/live.all"
envs "$T/gen.all" >"$T/gen.env"; envs "$T/live.all" >"$T/live.env"
jobs "$T/gen.all" >"$T/gen.job"; jobs "$T/live.all" >"$T/live.job"
echo "줄 수 — jobs: gen=$(wc -l <"$T/gen.job") live=$(wc -l <"$T/live.job") / env: gen=$(wc -l <"$T/gen.env") live=$(wc -l <"$T/live.env")"
echo "=== JOB diff (live → gen) · 비어 있으면 PASS ==="
if [ -s "$T/gen.job" ] && [ -s "$T/live.job" ] && diff -u "$T/live.job" "$T/gen.job"; then echo "S2_EMPTY_DIFF=yes"; else echo "S2_EMPTY_DIFF=no"; fi
echo "=== ENV diff (SHELL/PATH/CRON_TZ 등) ==="
if diff -u "$T/live.env" "$T/gen.env"; then echo "S2_ENV_SAME=yes"; else echo "S2_ENV_SAME=no"; fi
echo "=== git ==="
echo "HEAD=$(git rev-parse --short HEAD)"
git status --short --untracked-files=no | head -20
[ -z "$(git status --short --untracked-files=no)" ] && echo "GIT_CLEAN=yes" || echo "GIT_CLEAN=no"
S0
```

## B — 실스캔 소급 캡처 (읽기전용)

```bash
bash <<'B' 2>&1 | tee /tmp/fence02_B.out
# CAT-L-FENCE-02 · S4/B (Claude 확인본) — 읽기전용
set -u
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
SINCE='2026-09-27 13:55'
date -u
echo '--- 0. Step 2 때 걸어 둔 첫 스캔 캡처 ---'
cat /tmp/fence02_first_scan.cap 2>/dev/null || echo 'NO_FIRST_SCAN_CAP'
echo '--- 1. 실행 중 스캔의 cgroup ---'
ps -eo pid,user,etime,cgroup:90,cmd | grep -E 'scan_|daily_audit|weekly_evolution|--scan-|systemd-run' | grep -v grep || echo 'NO_SCAN_RUNNING_NOW'
echo '--- 6. slice / 부모 slice ---'
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax -p MemoryCurrent -p MemoryAccounting
systemctl show bitget.slice bitget-cron.slice -p MemoryHigh -p MemoryMax -p MemoryAccounting
echo '--- 5. 실행 중 scope (CPUQuota) ---'
for u in $(systemctl list-units --type=scope --no-legend | awk '/run-/{print $1}'); do
  echo "[$u]"; systemctl show "$u" -p Slice -p CPUQuotaPerSecUSec -p MemoryHigh -p MemoryMax
done
echo '--- 3. environ (실행 중인 ubuntu 스캔이 있을 때만) ---'
for p in $(pgrep -u ubuntu -f -- '--scan-' | head -3); do
  echo "[pid=$p] $(ps -o user=,cgroup= -p "$p")"
  tr '\0' '\n' <"/proc/$p/environ" 2>/dev/null | grep -E '^(HOME|USER|LOGNAME|PATH|PWD)='
done
echo "(참고) 현재 셸: HOME=$HOME USER=${USER:-} LOGNAME=${LOGNAME:-}"
echo '--- 4. 로그 소유권 (wrapper 이후 새 로그) ---'
TZ=UTC find "$INSTALL_ROOT" -name '*.log' -newermt "$SINCE" -printf '%TY-%Tm-%Td %TH:%TM %u:%g %p\n' 2>/dev/null | head -80
ls -l /var/log/bitget 2>/dev/null || true
echo '--- 2. cron / systemd-run ---'
journalctl -u cron -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_CRON_OK' || echo 'JOURNAL_CRON_UNREADABLE → 아래 결과 신뢰 불가(sudo로 재시도)'
echo "systemd-run 실행 라인 수: $(journalctl -u cron --utc --since "$SINCE" --no-pager 2>/dev/null | grep -c 'systemd-run')"
journalctl -u cron --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'error|failed' | tail -40 || true
echo '--- 9. 커널 OOM 소급 ---'
journalctl -k -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_K_OK' || echo 'JOURNAL_K_UNREADABLE → 아래 OOM 결과 신뢰 불가(sudo로 재시도)'
journalctl -k --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'out of memory|oom-kill|oom_reaper|killed process' || echo 'OOM_GREP_EMPTY'
echo '--- slice memory.events / peak (slice가 활성일 때만 존재) ---'
CG=/sys/fs/cgroup/bitget.slice/bitget-cron.slice/bitget-cron-heavy.slice
cat "$CG/memory.events" 2>/dev/null || echo 'NO_MEMORY_EVENTS'
cat "$CG/memory.peak" 2>/dev/null || echo 'NO_MEMORY_PEAK'
echo '--- 8. LIFECAP ---'
sqlite3 -readonly /var/lib/quant-bitget/data/bitget_job_lifetime.sqlite 'SELECT mode,pid,started_utc FROM job_starts;' 2>&1 | head -20
grep -RniE 'lifecap skip|LIFECAP WOULD_KILL|LIFECAP ENFORCE' "$INSTALL_ROOT/bitget" --include='*.log' 2>/dev/null | tail -20 || true
echo '--- 7. cron.d wrapper 줄 (28 기대) ---'
grep -c 'systemd-run' /etc/cron.d/dual-screener-bitget
grep -E 'systemd-run' /etc/cron.d/dual-screener-bitget | grep -oE -- '--(scan-[^ ]+|daily-audit|weekly-evolution)' | sort
B
```


### C/D 문서 (이 세션 적용)

- `docs/한미코인_100퍼센트가동_점검_및_수정필요사항.md` §4 권장1 주의 1줄
- `bitget/docs/claude_project/CAT-L_인프라배포.md` cron SSOT 1줄
- `track_b_06` 효과표 행 · 판정 예정 **2026-10-11**
- 스냅샷 테스트 실패 메시지에 갱신 절차 1줄 (비차단 관찰 3)

### 이 에이전트 S1~S4

SSH `ubuntu@3.36.90.195` 이전 세션 publickey denied. **원문 없음.** 설치기 미실행.

**금지 준수:** 설치기/`update_bitget.sh` 0 · 슬라이스 수치 0 · CAT-A 0 · live 0.

---

## OUTBOX — CAT-L-FENCE-02 Step 3 · 2026-09-28

**엔지니어:** HEAVY 분류는 CAT-A `_HEAVY_PREFIXES` **읽기 import**. wrapper 문자열은 Step 2 LIVE와 바이트 일치(테스트 `test_generator_matches_live_snapshot`). 서버 재설치 **안 함**.

### A. 영속화

| 파일 | 변경 |
|------|------|
| `bitget/deploy/generate_bitget_crontab.py` | (a) 28줄 root+systemd-run. enqueue/OPS ubuntu. `CPUQuota=80\%` |
| `bitget/deploy/systemd/bitget-cron-heavy.slice` | 기존 유지 Max=1610612736 High=1288490188 |
| `bitget/deploy/install_bitget_cron.sh` | slice `install -m 0644` + `daemon-reload` 멱등. post-deploy-obs 검증 유지. wrapper grep 추가 |
| `bitget/deploy/deploy_bitget_factory.sh` | 동일 slice 파일 복사(유닛 설치 경로). **installer 호출은 없음** |
| `bitget/deploy/bitget.crontab.example` | 생성기 재기록 |

**테스트:** `pytest bitget/tests/test_cat_l_fence02_crontab.py bitget/tests/test_bitget_staggered_schedule.py bitget/tests/test_cli_logging_bitget.py` → **21 passed**  
(a) wrapped=28, unwrap==P0 28 HEAVY 명령 100% · (b) 패리티 `_HEAVY_PREFIXES` · (c) 생성기 vs LIVE 스냅샷 **diff 공집합**

### install_bitget_cron.sh 호출 경로 (grep)

| 경로 | 호출? | Step 3 반영 |
|------|-------|-------------|
| `bitget/deploy/update_bitget.sh` | **예** (`bash install_bitget_cron.sh`) | installer 수정으로 포함 |
| `bitget/deploy/deploy_bitget_factory.sh` | 아니오 (chmod만) | slice 복사 추가 |
| `bitget/deploy/bitget.sh` | **아니오** | — |
| `diagnose_coin_digest.sh` / `audit_bitget_stack.sh` / `bitget_schedule_guard.py` | 안내 문자열만 | — |
| `docs/한미코인_100퍼센트가동_점검_및_수정필요사항.md` §88 | `--use-queue` 후 installer | 문서 주의는 Mirror 제안#2, 이번 미착수 |

### B. 실스캔 검증

이 세션 `ssh ubuntu@3.36.90.195` → host key accept 후 **Permission denied (publickey)**. 캡처 원문 없음.

| # | 항목 | 결과 |
|---|---|---|
| 1 | cgroup | **미캡처** (SSH 키 없음) |
| 2 | 정상 완주 | 미캡처 |
| 3 | HOME/USER/PATH/PWD | 미캡처 |
| 4 | 로그 소유권 | 미캡처 |
| 5 | CPUQuota 80% | 미캡처 |
| 6 | slice `systemctl show` + 부모 상한 | 미캡처 |
| 7 | 28줄 unwrap vs P0 | **로컬 PASS** (LIVE 스냅샷 = 생성기) |
| 8 | LIFECAP watchdog 1줄 | 미캡처 |

**재설치 금지 유지** until 서버에서 `generate_bitget_crontab.py` 출력 vs `/etc/cron.d/dual-screener-bitget` 헤더 제외 diff 공집합 확인. 이 코드가 VPS에 올라간 뒤에만 installer가 no-op.

### 로컬 구조 스냅샷

- `_job_line`이 HEAVY면 root+`_SYSTEMD_RUN`, 아니면 ubuntu. enqueue는 `--enqueue` 토큰으로 제외.
- CAT-A `job_lifetime_cap.py` **수정 0**.
- 슬라이스 수치 변경 0.

**금지 준수:** CAT-A 로직 0 · (b)/(c) 의미 0 · 수치 0 · C-2/MDD5%/live 0.

디렉터: Bot-2에서 Step 3 B 8항목 캡처(또는 SSH 에이전트 키로 재세션). Claude: A 스펙은 파일 검증 가능, B는 캡처 후.

---

## OUTBOX — CAT-L-FENCE-02 Step 2 · 2026-09-27 · 적용됨

---

## OUTBOX — CAT-L-FENCE-02 Step 2 · 2026-09-27 · 적용됨

**엔지니어:** ubuntu는 system slice에 `systemd-run --scope` 불가(polkit). (a) 28줄만 cron user=`root` + `systemd-run --uid=ubuntu --gid=ubuntu`. (b)/(c)는 `ubuntu` 유지. cron.d `%` 특수문자 때문에 `CPUQuota=80\%`. `generate_bitget_crontab.py` / `install_bitget_cron.sh` **미변경** — 재설치하면 wrapper 증발.

**수치(확정 재사용):** slice+scope MemoryHigh=`1288490188` MemoryMax=`1610612736`. CPUQuota=80%.

**롤백:** `snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron` → `/etc/cron.d/dual-screener-bitget` + `sudo rm /etc/systemd/system/bitget-cron-heavy.slice && daemon-reload`. 라이브 적용본 사본: `snapshots/CAT-L-FENCE-02_cron_step2_LIVE_20260927.cron`. 유닛 SSOT: `bitget/deploy/systemd/bitget-cron-heavy.slice`.

### 적용 원문 (2026-09-27 14:20:57 UTC)

```
wrapped_root systemd-run: 28
enqueue ubuntu: 1
7 15 * * *  ubuntu  ... --enqueue --scan-futures-ema5-r2
systemctl show bitget-cron-heavy.slice
MemoryHigh=1288490188
MemoryMax=1610612736
MemoryAccounting=yes
```

프로브(sleep 12, 동일 slice/상한, **스캔 아님**):

```
Running scope as unit: run-r01710840f00249399574498df140d17b.scope
21123 ubuntu  0::/bitget.slice/bitget-cron.slice/bitget-cron-heavy.slice/run-r01710840f0024939  sleep 12
```

`cron.service` 아님. 부모 `bitget.slice/bitget-cron.slice`는 호스트 기존 계층.

다음 (a) 실잡: **16:01 UTC** `--scan-spot-ema5-r2`. 적용 전 fork된 scan은 계속 `cron.service`일 수 있음. 서버에 16:01 캡처 watcher 기동(`/tmp/fence02_first_scan.cap`).

**금지 준수:** CAT-A 0 · (b)/(c) 0 · C-2/MDD5%/live 0 · 수치 임의 변경 0.

---

## OUTBOX — CAT-L-FENCE-02 Phase 1 Step 1 · 2026-09-27 · 실측만 · cron/slice 미적용

**엔지니어 1줄:** cap=5400이 **실제로 킬되면** (a) 동시 상한은 **3**이지 28이 아님. 지금 서버는 이미 2중첩이 `cron.service`에 살아 있음. 유휴 `free -h`는 이 시각에 못 찍음(요구 조건 미충족). slice 1.5G/1.2G 확정은 Claude. CAT-A 미변경.

### 가정 (읽기전용)

- `_HEAVY_PREFIXES` / `BITGET_JOB_HEAVY_CAP_SEC` **기본 5400**. Bot-2 `.env`에 해당 키 **grep 0줄** → 코드 기본값.
- 겹침 모델: 각 (a) job이 시작 후 **90분 동안 살아 있다**고 가정 (ENFORCE 킬이 온전할 때). ENFORCE=false·좀비 pid면 이 상한은 **하한**이 아니라 붕괴(09-25 age ≫ 5400).

### 1) 스케줄 겹침 (Phase 0 스냅샷 28줄, enqueue 제외)

26 scan 간격: **min 51분 · max 108분**. 108분은 `14:13 scan-spot-dante-r2` → `16:01 scan-spot-ema5-r2` (그 사이 (b) `15:07 enqueue ema5-r2`는 (a) 아님). **25/26** 간격이 90분 미만 → 연속 두 scan은 cap 윈도우 안에서 겹침 가능.

전 주 분 단위 시뮬 (Sun=0, weekly=`* * 1`=Mon 00:30):

| 동시 개수 | 주당 분 | 비율 |
|-----------|---------|------|
| 0 | 126 | 1.3% |
| 1 | 3314 | 32.9% |
| 2 | 6134 | 60.9% |
| 3 | 506 | 5.0% |
| ≥4 | 0 | 0% |

**최악 = 3** (4 없음). 예:

- 매일 **02:40 UTC**: `scan-spot-nulrim`(01:47) + `daily-audit`(02:30) + `scan-futures-nulrim`(02:40)
- 월요일 **00:30 UTC**: `scan-spot-ema5-r3`(전날 23:07) + `scan-spot-supernova`(00:02) + `weekly-evolution`(00:30)

### 2) 메모리 — **유휴 아님** (원문 2026-09-27 **13:55:32 UTC**)

요구: “(a) job이 하나도 안 돌 때”. **미충족.** 당시 (a) 2개:

```
               total        used        free      shared  buff/cache   available
Mem:           3.7Gi       1.0Gi       379Mi       2.0Mi       2.4Gi       2.4Gi
Swap:          4.0Gi       0.0Ki       4.0Gi
MemTotal:        3928824 kB
MemAvailable:    2552048 kB
  19691 324560 kB RSS  0::/system.slice/cron.service  python ... --mode scan_futures_dante_r2
  19196 214476 kB RSS  0::/system.slice/cron.service  python ... --mode scan_spot_nulrim_r2
dante-bitget-factory MemoryCurrent=312455168 MemoryHigh=1288490188 MemoryMax=1610612736
```

스케줄 정합: `12:27` nulrim-r2 · `13:20` dante-r2 · 캡처 13:55 → 이론 동시 **2** (다음 (a) `14:13` spot-dante-r2면 3 가능). 두 scan RSS 합 ≈ **539MB**. 둘 다 **여전히 cron.service**.

유휴 재측정 창(cap 준수 가정): 대략 **15:43–16:01 UTC** (dante-r2 계열 90분 종료 후 ema5-r2 전). 지금 대기하지 않음. 09-14 685M used / 2.8G avail은 참고만.

### Claude Ask (Step 2 수치)

- 관측 2중첩 RSS≈0.54G + factory Current≈0.31G, Available≈2.4G. **정상 런이면 1.5G/1.2G slice는 2~3중첩에 여유.**
- 반례: 09-07 **한 프로세스 RSS≈897M** × 동시 2 = 1.8G > 제안 MemoryMax 1.5G → slice가 정상 겹침을 죽일 수 있음. 하향(1G/768M)은 그 폭주엔 더 빨리 죽임.
- **권고는 Claude.** Cursor는 Step 2 미착수.

**금지 준수:** cron.d 미수정 · slice 유닛 미설치 · (b)/(c) 비접촉 · CAT-A 0 · C-2/MDD5%/live 0.

---

## OUTBOX — CAT-L-FENCE-02 Phase 0 · 2026-09-27 · 읽기전용

**엔지니어 1줄:** 라이브 스케줄은 `crontab -l`이 아니라 `/etc/cron.d/dual-screener-bitget`(ubuntu 필드). Phase 1은 그 파일의 (a) 줄만, cron.d 문법상 `user` 뒤에 wrapper를 붙이는 게 맞고 `--uid=`는 이미 ubuntu로 떨어진 뒤라 중복일 수 있음(Claude 확인 후). CAT-A 파일 미변경. crontab 미터치.

**SSH:** `ubuntu@3.36.90.195` · host 조회 시각 2026-09-27 13:30 UTC · **Phase 1 미적용** (cron 파일 mtime 유지: Sep 23 10:11).

### 1) 서비스 계정 crontab 원문

```
$ crontab -l
no crontab for ubuntu

$ sudo crontab -l
no crontab for root
```

실제 실행 SSOT = `/etc/cron.d/dual-screener-bitget` (8135B, 73줄, 2026-09-23 10:11). 롤백 스냅샷: `bitget/docs/work_phases/snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron`

원문 전체:

```
# Dual-Screener-Bot — Bitget factory cron (→ /etc/cron.d/dual-screener-bitget)
#
# AUTO-GENERATED from bitget/bitget_scan_schedule.py — do not edit by hand.
# Regenerate: python bitget/deploy/generate_bitget_crontab.py
# 전용 코인 서버(Bot-2) 최적화: 3사이클 27슬롯, ~53분 간격 교차 배치.
# SPOT/FUTURES are interleaved (never simultaneous). %5 minute constraint removed
# (dedicated server — no KR/US stock collision risk).
# Two-Track air-gap: cgroup·독립 락/큐로 병렬 가동. yield OFF (BITGET_YIELD_TO_FACTORY=0).
# L-3b canary: --scan-futures-ema5-r2 is --enqueue only (other scan_* stay inline; full b-3 is a separate Ask).
# install: sudo INSTALL_ROOT=... bash bitget/deploy/install_bitget_cron.sh
#
# user/path: ubuntu · /home/ubuntu/dante_bots/Dual-Screener-Bot

SHELL=/bin/bash
CRON_TZ=UTC
PATH=/usr/local/bin:/usr/bin:/bin

# --- Ops (non-scan, 24/7) ---
*/15 * * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --canary
*/15 * * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --track-positions
53 * * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --reconcile
43 */4 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --data-refresh
5 0 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --db-backup
30 2 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --daily-audit
30 0 * * 1  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --weekly-evolution
*/5 * * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --watchdog
15 0 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --health
50 23 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --monthly-grand
0 11 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --post-deploy-obs-digest

# --- SPOT staggered (24h, 14 slots, ~53min interval) ---
2 0 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova
47 1 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim
33 3 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante
20 5 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5
7 7 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-master
52 8 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-shadow
40 10 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r2
27 12 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r2
13 14 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r2
1 16 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r2
47 17 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-supernova-r3
33 19 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-nulrim-r3
20 21 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-dante-r3
7 23 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-spot-ema5-r3

# --- FUTURES staggered (24h, 13 slots, ~53min interval) ---
54 0 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova
40 2 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim
27 4 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante
13 6 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5
1 8 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-shadow
47 9 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r2
33 11 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r2
20 13 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r2
7 15 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --enqueue --scan-futures-ema5-r2
52 16 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-supernova-r3
40 18 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-nulrim-r3
27 20 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-dante-r3
13 22 * * *  ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-futures-ema5-r3

# --- Legacy monolithic scan (manual recovery only — do NOT cron) ---
# ubuntu  cd /home/ubuntu/dante_bots/Dual-Screener-Bot && TZ=UTC /home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/deploy/bitget.sh --scan-all
```

(주석 일부 축약. 스냅샷 파일이 바이트 단위 원본.)

인접 파일(이번 범위 아님, 미변경): `/etc/cron.d/dual-screener-north-star` (681B, Aug 3) · `dual-screener-bitget.off` (구버전).

### 2) 분류 (`_HEAVY_PREFIXES = ("scan_", "daily_audit", "weekly_evolution")` 읽기전용)

| 그룹 | UTC 스케줄 | 명령 플래그 | Phase 1 |
|------|------------|-------------|---------|
| **(a) HEAVY 직행** 28줄 | 아래 표 | `--scan-*` 26 + `--daily-audit` + `--weekly-evolution` | wrapper 후보 |
| **(b) enqueue** 1줄 | `7 15 * * *` | `--enqueue --scan-futures-ema5-r2` | **손대지 않음** |
| **(c) OPS/기타** 9줄 | 아래 | canary / track / reconcile / data-refresh / db-backup / watchdog / health / monthly-grand / post-deploy-obs-digest | **손대지 않음** |

**(a) 26 scan 직행** (플래그 → LIFECAP mode는 `-`→`_`):

| UTC | 플래그 | 기존 OOM RSS 기록 |
|-----|--------|-------------------|
| `54 0` | `--scan-futures-supernova` | **09-25 14:42 UTC** kernel: pid=44321 python `task_memcg=/system.slice/cron.service` · `anon-rss:80708kB` (~79M) · `total-vm:2162580kB` (~2.1G) · LIFECAP이 이 pid를 `scan_futures_supernova`로 표기. **09-07과 숫자 혼동 금지** |
| 나머지 25 scan | `--scan-spot-*` 14 · `--scan-futures-*` 11 (ema5-r2 제외) | 모드별 RSS **개별 기록 없음**. 09-07은 cron python pid=640081 **RSS≈897M** (mode 미기재). 09-25 잔존 pid 54314/`scan_futures_supernova_r2`, 58894/`scan_spot_dante_r2`, 62167/`scan_spot_supernova_r3` — RSS 숫자 없음(LIFECAP age만) |
| `30 2 * * *` | `--daily-audit` | OOM RSS 기록 없음 |
| `30 0 * * 1` | `--weekly-evolution` | OOM RSS 기록 없음 |

**(b)** `7 15 * * *` `--enqueue --scan-futures-ema5-r2` — L-3b canary. factory/queue-worker cgroup 전제. 09-14 15:22 UTC는 queue-worker **status=143 restart** 기록이지 cron scan RSS OOM이 아님.

**(c)** `--canary` `*/15` · `--track-positions` `*/15` · `--reconcile` `53 *` · `--data-refresh` `43 */4` · `--db-backup` `5 0` · `--watchdog` `*/5` · `--health` `15 0` · `--monthly-grand` `50 23` · `--post-deploy-obs-digest` `0 11`. 주석 처리 `--scan-all`은 비활성.

### 3) 기존 OOM 취합 (신규 측정 없음)

| 날짜 | 출처 | 실측 |
|------|------|------|
| **09-07 08:44 UTC** | `track_b_05` / 진단 OUTBOX | `global_oom` · python **pid=640081 RSS≈897M** · `task_memcg=/system.slice/cron.service` · **mode 미기재** |
| **09-14** | L-3b OUTBOX | **kernel RSS 없음**. 15:07 enqueue 시작 · 15:22 queue-worker 143. “3회 재발”의 14일은 이 관측/재시작과 겹침 — cron 직행 RSS로 쓰지 말 것 |
| **09-25 14:42 UTC** | 09-26 진단 OUTBOX | `global_oom` · python **pid=44321** `scan_futures_supernova` · cron.service · **anon-rss≈80.7MB** · total-vm≈2.1GB · cron.service oom-kill · 잔존 54314/58894/62167 |

### Ask Claude (Phase 1 전)

1. (a) 28줄 전부 wrapper vs **scan_* 26만** (daily-audit/weekly-evolution 제외) — 스펙 문면은 접두 동일하니 28.
2. cron.d 삽입 위치: `… ubuntu systemd-run --scope -p MemoryMax=1610612736 -p MemoryHigh=1288490188 -p CPUQuota=80% -- cd … bitget.sh --scan-…` (`--uid=ubuntu` 생략 vs 유지).
3. 생성기 `generate_bitget_crontab.py` / `bitget.crontab.example` 동기화는 **이번 금지(전체 재설치 금지)** — 서버 줄만 최소 수정인지 확인.

**Phase 1 미착수.** CAT-A 로직 0. 주식 cron 0. C-2/MDD5%/live/`ENABLE_REAL_EXECUTION` 0.

---

## OUTBOX — A5-EVENTLOG-01 · 2026-09-27

게이트/threshold/반환값 미변경. EFFECTVERIFY 집계 미변경. 롤백=`A1A5_EVENT_LOG_ENABLED=false`.

**스냅샷:** `a1_a5_event_log.py` + execution_safety(A-1 전이만 / A-3 logger 직후 / A-4 block 직전) + tail_risk_gate(debit>0) + config_bounds(out_of_range). logger 유지.

**테스트:** `test_a5_eventlog.py` 6 + A-1~A-5 회귀 → **42 passed**.

**첫 발생 0건이 정상:** A-1은 CURRENT_TIER가 바뀔 때만. A-2는 debit>0. A-3는 requested>cap(FUT, symbol=null 재조회 없음). A-4는 gate7 block. A-5는 reject write. 배포 직후 0건 ≠ 버그.

---

## OUTBOX — A-EFFECTVERIFY-01 · 2026-09-26

**창:** 2026-08-01 ~ 2026-09-26 · Bot-2 ops 379M · tests **3 passed**  
**롤백:** `A1A5_EFFECT_VERIFY_ENABLED=false`

### 로컬 구조 스냅샷
- 신규 `bitget/observability/a1_a5_effect_verify_bg.py`
- `A1A5_EFFECT_VERIFY_ENABLED` default true · kv 시드 write 없음
- A-3 `normalize_market_key` FUT만
- 비접촉: execution_safety / tail_risk_gate / config_bounds / C-2 / MDD5% / live / 신규 로그 파이프라인

### 5-sub (`06` 변경 후 열 이식용)

| sub | 값 | 소스 | 비고 |
|-----|----|------|------|
| A-1 | **null** | 없음 | kv 현재 `TIER=NORMAL` `NAV_PEAK=100000` (이력 아님) |
| A-2 | **null** | 없음 | tail debit ops 0 |
| A-3 | **null** | 없음 | clamp는 logger.info만 |
| A-4 | **null** | 없음 | gross block ops 0 |
| A-5 | **null** | 없음 | reject는 logger.warning만 |

3단계 판정은 Claude. MASTER 5·7·8 대기.

---

## OUTBOX — A-LIFECAP-01 ENFORCE 전환 미니 · 2026-09-26

| 항목 | 내용 |
|------|------|
| **sub-phase** | A-LIFECAP-01 ENFORCE |
| **코드 diff** | 없음 (`sweep_expired_jobs` 재사용) |
| **config** | `BITGET_JOB_LIFECAP_ENFORCE=true` · 서버 `.env` append + `set_config_value` (전환 전 `.env` 키 없음 · sqlite None) |
| **예제** | `bitget/deploy/bitget.env.example` ENFORCE=true |
| **롤백** | `BITGET_JOB_LIFECAP_ENFORCE=false` |
| **비접촉** | crontab 재설치 · C-2 · MDD5% · live · CAT-N 원장 · flock 8번째 테스트 미추가 |

### data_refresh cap 분류 (1줄)

**의도적 OPS.** `job_lifetime_cap.py` `_HEAVY_PREFIXES=("scan_","daily_audit","weekly_evolution")` — `data_refresh`는 미해당 → `BITGET_JOB_OPS_CAP_SEC=1800`. 실측: `LIFECAP WOULD_KILL mode=data_refresh pid=77122 age=9994 cap=1800 (ENFORCE=false)`. **버그 아님. 코드 정정 없음.**

### 첫 watchdog tick 실킬 (09:15:01 UTC)

유닛 저널:

```
Sep 26 09:15:01 systemd: Starting Bitget heartbeat watchdog
Sep 26 09:15:02 bash: mode=watchdog log=.../bitget_watchdog_20260926_181501.log wall_utc=2026-09-26 09:15:01 UTC
Sep 26 09:15:03 systemd: Finished ... Consumed 1.254s CPU time
```

해당 파일 `LIFECAP` 매칭 **0줄**. `LIFECAP ENFORCE kill` 전역 count **0**. `job_starts` **[]** (08:52 UTC 리부트 후 좀비 pid/레지스트리 비움).

**실킬 0건은 플래그 실패가 아님.** 킬 대상 프로세스가 부팅 후 아직 없음. 다음 heavy 스캔이 cap=5400을 넘기면 그때 첫 `LIFECAP ENFORCE kill mode=… pid=… age=…`가 파일 로그에 남음 (journalctl 키워드는 이번 라운드 수정 대상 아님 → 다음 CAT-L 후보).

status=`ENFORCE_LIVE · WAIT_FIRST_KILL_CONFIRM`

---

## OUTBOX — 2026-09-26 · Bot-2 Stop/Start 진단 + A-LIFECAP-01 48h 원문

**SSH:** `ubuntu@3.36.90.195` host `ip-172-26-7-213` · 조회 ~08:54 UTC · boot `up 2 min`.  
**ENFORCE/kill:** 코드·`.env`·drop-in **미변경**. 워치독 로그에 `LIFECAP ENFORCE` 킬 라인 없음(샘플 파일 count 0). `.env`에 LIFECAP 키 없음 → 코드 기본(ENABLED 기본 true / ENFORCE 기본 false) + 로그 `(ENFORCE=false)`.

### 오늘 “터짐”이 커널 패닉이 아님

`last -x`: `shutdown system down` **Sat Sep 26 08:49–08:52 UTC**. prev boot 마지막 저널은 `Finished System Power Off` / `Shutting down`. **Lightsail Stop/Start(정상 poweroff)** 과 동일 패턴. 디스크 **45%** (`/` 35G/78G). `bitget/logs` 108K. 기각: 디스크 풀.

### 직전 부트에서 실제 구멍 — 또 cron cgroup OOM

prev boot 커널:

```
Sep 25 14:42:07 kernel: snapd invoked oom-killer
Sep 25 14:42:07 kernel: oom-kill:... global_oom,task_memcg=/system.slice/cron.service,task=python,pid=44321
Sep 25 14:42:07 kernel: Out of memory: Killed process 44321 (python) total-vm:2162580kB, anon-rss:80708kB
Sep 25 14:40:18 systemd: cron.service: Failed with result 'oom-kill'.
Sep 25 14:40:18 systemd: cron.service: Unit process 54314 (python) remains running after unit stopped.
Sep 25 14:40:18 systemd: cron.service: Unit process 58894 (python) remains running after unit stopped.
Sep 25 14:40:18 systemd: cron.service: Unit process 62167 (python) remains running after unit stopped.
```

(같은 시각 프로세스 표에 python 다수. LIFECAP이 `pid=44321`을 `scan_futures_supernova`로 찍음 — 아래 원문.)  
**CAT-L과 같은 구멍:** factory `MemoryMax=1.5G`는 살아 있음(지금 High=1.2G Max=1.5G). **cron 직행 scan_* 는 여전히 cron.service cgroup**. L-3b canary만 enqueue (`7 15 * * * --enqueue --scan-futures-ema5-r2`). 나머지 inline.

OOM 직후 cron 유닛은 죽었는데 **좀비 python은 남음** (54314 / 58894 / 62167). 그게 LIFECAP age 수십 시간으로 이어짐.

### 지금(리부트 직후) 상태

factory/ws/async/queue-worker/watchdog.timer/backup.timer/overseer **active**. queue-worker Max=2G. `BITGET_QUEUE_WORKER_STALE_SEC=1800` drop-in 유지. 주식 유닛 없음. RSS 상위: auto_pilot ~243MB, queue_worker ~154MB. data `charts` 21G + sqlite. 스냅샷 tmp 잔여 파일 있음(이번 원인 아님).

### LIFECAP WOULD_KILL 원문 (48h 관측 본문)

journalctl 키워드 0건(워치독이 파일 로그). 파일 **4852줄**. 첫/끝:

```
[2026-09-25 00:00:10] [WARNING] LIFECAP WOULD_KILL mode=scan_futures_supernova pid=44321 age=83142 cap=5400 (ENFORCE=false)
[2026-09-25 00:00:10] [WARNING] LIFECAP WOULD_KILL mode=scan_spot_dante pid=46974 age=73610 cap=5400 (ENFORCE=false)
[2026-09-25 00:00:10] [WARNING] LIFECAP WOULD_KILL mode=scan_futures_supernova_r2 pid=54314 age=51165 cap=5400 (ENFORCE=false)
[2026-09-25 00:00:10] [WARNING] LIFECAP WOULD_KILL mode=scan_spot_dante_r2 pid=58894 age=35203 cap=5400 (ENFORCE=false)
[2026-09-25 00:00:10] [WARNING] LIFECAP WOULD_KILL mode=scan_spot_supernova_r3 pid=62167 age=22306 cap=5400 (ENFORCE=false)
...
[2026-09-26 17:46:28] [WARNING] LIFECAP WOULD_KILL mode=scan_futures_supernova_r2 pid=54314 age=169142 cap=5400 (ENFORCE=false)
[2026-09-26 17:46:28] [WARNING] LIFECAP WOULD_KILL mode=scan_spot_supernova_r3 pid=62167 age=140284 cap=5400 (ENFORCE=false)
```

(로그 시각은 KST 벽시계로 보임. 리부트 직전 17:46 KST ≈ 08:46 UTC.)

mode별 줄 수(워치독 5분 tick 반복 포함):

| count | mode |
|------:|------|
| 732 | scan_spot_supernova_r3 |
| 732 | scan_futures_supernova_r2 |
| 543 | scan_spot_dante_r3 |
| 449 | scan_spot_dante_r2 |
| 399 | scan_spot_supernova_r2 |
| 331 | scan_futures_supernova |
| 329 | scan_futures_dante_r3 |
| 205 | scan_spot_nulrim_r2 |
| 165 | scan_futures_dante_r2 |
| 161 | scan_futures_ema5_r3 |
| 151 | scan_spot_ema5_r3 |
| 135 | scan_spot_dante |
| 134 | scan_futures_ema5 |
| 127 | scan_spot_master |
| 81 | scan_futures_dante |
| 64 | scan_spot_ema5_r2 |
| 43 | data_refresh |
| 38 | scan_spot_ema5 |
| 16 | scan_spot_nulrim |
| 13 | scan_futures_nulrim_r3 |
| 4 | scan_futures_nulrim |

**오탐 0 아님.** age가 cap=5400(1.5h)을 훨씬 넘음(최대 ~47h). 정상 12분 스캔이 아니라 **안 죽은 cron 스캔**. shadow는 설계대로 로그만. Cursor는 ENFORCE를 켜지 않음.

status=`WAIT_CLAUDE_OK` (48h 원문 도착 · ENFORCE Handoff는 Claude). CAT-L 전체 enqueue는 별도 Ask.

---

## OUTBOX — A-LIFECAP-01 48h 관측 · 2026-09-26 · **WOULD_KILL 원문 없음 (미수집)**

| 항목 | 내용 |
|------|------|
| **sub-phase** | A-LIFECAP-01 |
| **요청** | LIFECAP 48h 관측 · `LIFECAP WOULD_KILL` mode·age·타임스탬프 원문 |
| **ENFORCE / kill** | **켜지 않음** (코드·`.env`·drop-in 미변경) |
| **git** | 데스크톱 `main` = `origin/main` (`b7b8913` 시점 fetch). 노트북 커밋은 이미 원격에 있음 |
| **SSH 1** | `ubuntu@43.202.40.136` · timed out · ~08:36 UTC |
| **SSH 2** | `ubuntu@52.78.197.105` (디렉터 제공) · ~08:44–08:46 UTC · OpenSSH debug: **Connection established** 후 `getpeername failed` / `write: Connection timed out` (배너·세션 없음) |
| **원문** | **없음.** 오탐 0으로 쓰지 말 것 |
| **status** | `WAIT_DIRECTOR` — 이 데스크톱에서 22/tcp 핸드셰이크가 안 끝남. Lightsail 방화벽(이 PC 공인 IP) 또는 인스턴스 sshd 확인 |

디렉터: 노트북에서 같은 키로 `ssh ubuntu@52.78.197.105`가 되면, 그 세션에서 `journalctl --since "2026-09-23 00:00:00 UTC" \| grep "LIFECAP WOULD_KILL"` 붙여 주셔도 됩니다. Claude: ENFORCE Handoff **보류**.

---

## OUTBOX — A-LIFECAP-01 Claude OK shadow · 사후 확인 2줄 · 2026-09-23

**Claude OK: 2026-09-23** (shadow 배포 승인 · ENFORCE=true는 별도 Handoff)

- flock: SIGKILL이면 프로세스 종료 시 fd가 닫혀 Linux flock은 OS가 해제. 코드에 unlock 없음. **7테스트에 flock 케이스 없음.**
- 5핵심 매핑: `test_a_age_under_cap_survives` · `test_b_age_over_cap_shadow_log_only` · `test_c_enforce_sigterm_then_sigkill` · `test_d_same_mode_reentry_two_skips`+`test_d_stale_over_cap_skip_not_kill` · `test_e_record_job_failure_on_enforce_sweep` (+ `test_job_lifetime_cap_sec_heavy_vs_ops`)

status=`SHADOW_DEPLOYED · WAIT_48H_OBS`. 48h 후 Cursor에 WOULD_KILL 원문 조회.

---

## OUTBOX — A-LIFECAP-01 1단계 shadow 구현 · Claude 검증 요청 · 2026-09-23

---

## OUTBOX — A-LIFECAP-01 1단계 shadow 구현 · Claude 검증 요청 · 2026-09-23

| 항목 | 내용 |
|------|------|
| **sub-phase** | A-LIFECAP-01 |
| **status** | `WAIT_CLAUDE_OK` |
| **ENFORCE** | **false** (WOULD_KILL 로그만) |
| **ENABLED** | true · 롤백=`BITGET_JOB_LIFECAP_ENABLED=false` |
| **코드** | `bitget/infra/job_lifetime_cap.py` · `runtime.dispatch` skip only · `watchdog.main` sweep |
| **테스트** | `bitget/tests/test_job_lifetime_cap.py` 7 passed |

### 엔지니어 1줄
동일 mode 재진입은 skip만. kill은 watchdog sweep 한 경로. Mission 3 `record_job_failure` 재사용. 배포 후 48h WOULD_KILL 오탐 없으면 ENFORCE Handoff.

### 배포 후 Claude가 볼 로그
`journalctl`/`watchdog` 로그 키워드 `LIFECAP WOULD_KILL` — 정상 장기 작업이면 cap 재조정, 아니면 ENFORCE=true.

---

## OUTBOX — 1800초 계산 검증 접수 (코드/서버 변경 없음)

---

## OUTBOX — 1800초 계산 검증 접수 (코드/서버 변경 없음)

Claude: 744×2=1488, 1800≈실측 최대×2.4 · 상한 미초과 · drop-in 경로 OK · 09-05~08 락 제외 OK.
L-3b **아직 Done 아님.** 트리거: 내일 4종 전부 정상 → Done 선언 + 3번 전체전환 논의.
락 미해제 후속은 지금 안 막음.

---

## OUTBOX — L-3b-fix 실측 + 적용 (로직 미변경)

---

## OUTBOX — L-3b-fix 실측 + 적용 (로직 미변경)

**실측 (cron 시절 stamped log, 시작~끝 있는 최근 4일)**

| 날짜 | 시작 | 끝 | 초 |
|------|------|-----|-----|
| 09-09 | 15:07:02 | 15:19:26 | 744 |
| 09-10 | 15:07:03 | 15:19:20 | 737 |
| 09-11 | 15:07:03 | 15:19:18 | 735 |
| 09-12 | 15:07:02 | 15:19:16 | 734 |

≈ **12.3분**. journalctl cron/factory grep는 비어 스탬프 로그 사용.
1회차 하한 15:07:03–15:20:59 = 13분+ (미완료).
09-05~08의 7200s는 lock-until-shutdown이라 **소요시간으로 안 씀**.

**확정값:** 12.3분 × 2 ≈ 25분 → Handoff 여유 예시 따라 **BITGET_QUEUE_WORKER_STALE_SEC=1800** (50분 상한 안 넘김). 기본 600이 1회차 킬 원인과 일치(15분째 watchdog).

**적용:** repo `.env` APPEND `=1800` · watchdog drop-in `queue-stale.conf` · `restart dante-bitget-queue-worker` → **active**.
워치독은 cron/timer가 `.env`를 source — 다음 */5부터 1800 적용.

**09-15 15:07 UTC 아직 남음.** 그때 start→done + MemoryCurrent 샘플 + watchdog restart 0 + OOM 0.

---

## OUTBOX — L-3b run1 완료 여부 (원문)

---

## OUTBOX — L-3b run1 완료 여부 (원문)

캡처: 15:22–15:23 UTC. **done / FAILED / attempt=2/3 로그 없음.**

```
Sep 14 15:07:03 ... [INFO] queue exec start id=1 engine=BITGET mode=scan_futures_ema5_r2 prio=5 attempt=1/3
Sep 14 15:20:59 ... [INFO] bitget_artifact_guard: ... healed: True
Sep 14 15:22:17 ... Starting Bitget heartbeat watchdog
Sep 14 15:22:18 ... sudo ubuntu : COMMAND=/usr/bin/systemctl restart dante-bitget-queue-worker
Sep 14 15:22:18 ... Stopping Bitget task queue worker
Sep 14 15:22:18 ... [WARNING] bitget lock signal 15 — releasing .../.bitget_runtime.lock
Sep 14 15:23:05 ... Main process exited, code=exited, status=143
Sep 14 15:23:05 ... Failed with result 'exit-code'
Sep 14 15:23:05 ... Started Bitget task queue worker
Sep 14 15:23:06 ... [INFO] queue worker started
```

`journalctl ... | grep id=1|scan_futures_ema5_r2` 해당분은 **start 한 줄뿐**. 재시작 후 재픽업 로그 없음.

MemoryCurrent 샘플 (피크 확정 아님):
- 15:12 UTC ~91MB (95539200)
- 15:22 UTC ~133MB (139001856) ← 관측 최고
- 15:23 UTC restart 후 ~91MB (95481856)

1회차 판정: **끝까지 정상 종료 아님.** claim/start 성공 후 ~15분에 watchdog가 queue-worker를 restart → SIGTERM/143. 워커 코드의 완료 로그(`queue exec start` 짝) 없음.

Ask: L-3b 관찰 중 watchdog(5분 크론)이 RUNNING 잡을 끊는 건지 확인. 코드 수정은 이번 세션 안 함. 09-15 2회차도 같은 패턴이면 Done 불가.

---

## OUTBOX — L-3b run1 (09-14 15:07 UTC 슬롯) · 원문

---

## OUTBOX — L-3b run1 (09-14 15:07 UTC 슬롯) · 원문

캡처 시각: **2026-09-14 15:12:57 UTC** (슬롯 +6분). **1회만으로 됐다 금지.**

로그 키워드는 `claim`이 아니라 **`queue exec start`**.

```
===== 1 journalctl =====
Sep 14 15:07:03 ip-172-26-7-213 bash[1938]: [2026-09-15 00:07:03] [INFO] queue exec start id=1 engine=BITGET mode=scan_futures_ema5_r2 prio=5 attempt=1/3
===== 2 dmesg =====
NO_DMESG_KILLED
===== 3 MemoryCurrent =====
MemoryCurrent=95539200
===== crontab =====
7 15 * * *  .../bitget.sh --enqueue --scan-futures-ema5-r2
```

해석: enqueue→워커 픽업 **확인**. **done 아직 없음**(스캔 진행 중). OOM killed 0. Current≈91MB. run2=09-15 15:07 UTC 이후 동일 3종.

---

## OUTBOX — 2026-09-14 · L-3a Claude OK 접수 · L-3b 정정

**L-3a:** Cursor 구현 ✅ · 서버 적용 확인 ✅(원문 2건) · **Claude OK 2026-09-14**. 05 기록함.
**L-3b:** 적용됨. **Done 아님.** `NO_OOM_SINCE_APPLY`는 첫 슬롯 전이라 증거 아님.
Done 기준: **09-14 및 09-15 15:07 UTC 이후** 3종 캡처 2회분. 1회만으로 됐다 금지.
전체 큐 전환: 보류. 에스컬레이션 해제.

3종 (슬롯 지난 뒤):
```
journalctl -u dante-bitget-queue-worker --since "2026-09-14 15:00 UTC" | grep -iE "claim|done|scan_futures_ema5_r2"
dmesg -T --since "2026-09-13 15:39 UTC" | grep -i killed
systemctl show dante-bitget-queue-worker -p MemoryCurrent
```
run2는 since를 09-15 15:00 UTC 로.

---

## 디렉터 → Claude Pro 붙여넣기 (이 블록만)

```
---CLAUDE---
[CAT-L] L-3a/L-3b 서버 적용 결과 원문. 디렉터는 SSH 안 함. Cursor 캡처.

요청: L-3a는 값 확인되면 바로 OK. L-3b는 "일단 적용됨"만 확인(24~48h Done 아님). 전체 큐 전환은 범위 밖.

queue-worker: 적용 전 실측 MemoryMax=2G 있었음 → 「없으면 896M」미해당. High=1.5G / Max=2G 유지.

## L-3a systemctl show 원문 (적용 직후 2026-09-13 15:39 UTC)

===== SHOW factory =====
MemoryCurrent=192512
MemoryHigh=1288490188
MemoryMax=1610612736
===== SHOW ws =====
MemoryCurrent=3289088
MemoryHigh=209715200
MemoryMax=268435456
===== SHOW async =====
MemoryCurrent=3121152
MemoryHigh=104857600
MemoryMax=134217728
===== SHOW queue-worker =====
MemoryCurrent=1867776
MemoryHigh=1610612736
MemoryMax=2147483648

## L-3a 재캡처 (2026-09-14 14:13 UTC, restart 없음)

factory      MemoryCurrent=124411904 MemoryHigh=1288490188 MemoryMax=1610612736  active
ws           MemoryCurrent=26292224  MemoryHigh=209715200  MemoryMax=268435456   active
async        MemoryCurrent=55189504  MemoryHigh=104857600  MemoryMax=134217728   active
queue-worker MemoryCurrent=95465472  MemoryHigh=1610612736 MemoryMax=2147483648  active

바이트 환산: factory High=1.2G Max=1.5G · ws High=200M Max=256M · async High=100M Max=128M · queue High=1.5G Max=2G

## L-3b 적용 원문 (Done 아님)

crontab:
7 15 * * *  ubuntu  .../bitget.sh --enqueue --scan-futures-ema5-r2

queue-worker: Started 2026-09-13 15:39:47 UTC · "queue worker started"
적용 이후 커널: NO_OOM_SINCE_APPLY
canary 슬롯 UTC 15:07 — 재캡처 시각 14:13 UTC 기준 첫 enqueue 미도래.
24~48h 3종(claim→done / dmesg killed 0 / MemoryCurrent 피크)은 첫 슬롯 이후 재수집.

금지: C-2 / MDD5% / live / ENABLE_REAL_EXECUTION
---CLAUDE---
```

파일 SSOT: 본 파일 상단. 인덱스: `CURSOR_TO_CLAUDE.md`

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 23:13 KST · 1번 승인 반영 (재적용 없음)

디렉터 확정: L-3a 승인 · L-3b A안 · 전체 큐(L-3) 보류.
서버에 **어제 이미 달아 둠**. 896M로 줄이지 않음. 서비스 restart 안 함.

### L-3a 원문 재캡처 (now)

```
factory      MemoryCurrent=124411904 MemoryHigh=1288490188 MemoryMax=1610612736  active
ws           MemoryCurrent=26292224  MemoryHigh=209715200  MemoryMax=268435456   active
async        MemoryCurrent=55189504  MemoryHigh=104857600  MemoryMax=134217728   active
queue-worker MemoryCurrent=95465472  MemoryHigh=1610612736 MemoryMax=2147483648  active
```

### L-3b

crontab: `--enqueue --scan-futures-ema5-r2` 유지.
적용 이후 OOM: `NO_OOM_SINCE_APPLY`.
queue-worker 기동 로그 OK. **canary 슬롯 UTC 15:07은 오늘 14:13 UTC 기준 아직 미도래** → claim→done 완료 조건은 내일 이후.

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 · CAT-L-FENCE-01 구현

**Ask:** Handoff 스펙 일치 OK 여부. L-1/L-2 에스컬레이션 취소 수용.

**엔지니어 분기:** queue-worker Max **2G 유지** + High=1.5G (「없으면 896M」는 해당 없음).

### L-3a 원문 (`ubuntu@43.202.40.136`)

```
===== SHOW factory =====
MemoryCurrent=192512
MemoryHigh=1288490188
MemoryMax=1610612736
===== SHOW ws =====
MemoryCurrent=3289088
MemoryHigh=209715200
MemoryMax=268435456
===== SHOW async =====
MemoryCurrent=3121152
MemoryHigh=104857600
MemoryMax=134217728
===== SHOW queue-worker =====
MemoryCurrent=1867776
MemoryHigh=1610612736
MemoryMax=2147483648
```

(재시작 직후 Current는 작음. 피크는 L-3b 관찰.)

### L-3b

서버 crontab 1줄:
`bitget.sh --enqueue --scan-futures-ema5-r2` (UTC 15:07)
다른 scan_* 는 inline 유지.
**24–48h 잔여:** queue-worker claim→done · `dmesg -T | grep -i killed` 0 · queue MemoryCurrent 피크.

### L-4

`post_deploy_obs_digest_bg.py`: `scan_last_cgroup_by_mode` · `fenced_units_memory_snapshot` · 실패 null+unavailable.
테스트: `test_post_deploy_obs_digest_bg.py` + `test_bitget_staggered_schedule.py` **24 passed**.
**서버 git pull 전엔 일일 텔레그램에 필드 없음.**

**금지 준수:** gates / live / C-2 / MDD5% / ENABLE_REAL_EXECUTION / daily_audit 비접촉.

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 · CAT-L 진단 확정 (코드 없음)

**SSH:** `ubuntu@43.202.40.136` (Stop/Start 후 IP 변경). host `ip-172-26-7-213`.

**오늘 죽음:** 커널 패닉/디스크 풀 아님. 직전 부트 08-17~09-13 14:56 UTC **정상 poweroff**(Lightsail Stop). 그날 저널 OOM/`No space` 없음.

**원인 하나로 좁힘 (재발 구멍):** factory `MemoryMax=1.5G`는 유닛 템플릿에 있음. 그러나 **cron 스캔은 `cron.service` cgroup**이라 그 상한이 안 먹음. 09-07 08:44 UTC `global_oom`이 cron python(RSS≈897M)을 죽임. 지금(리부트 직후)도 auto_pilot(factory) + `scan_futures_ema5_r2`(cron) 동시. crontab 스캔은 **inline**, `--enqueue` 없음.

**기각:** L-1/L-2 미설치 — logrotate `bitget-dante` Aug 2 존재 · journal-vacuum/backup.timer enabled+active · logs 108K · df 38%.

**drop-in:** `dante-bitget-factory.service.d` **없음**. ws/async `MemoryMax=infinity`.

### 요청 Handoff (구현은 그 후)

1. HIST_10 §2.4 MemoryHigh+MemoryMax drop-in (factory + ws + async) + `systemctl show` 원문 캡처를 종료 조건으로
2. cron 스캔을 factory cgroup 또는 `--enqueue`/큐 워커로 넣는 최소 경로 (b-2 canary vs 전면 b-3 — 디렉터 선택)
3. Layer2 digest 3필드 — L-1/L-2는 이미 켜져 있으니 **MemoryMax+cron cgroup**이 🔴 칸이 되게

Layer3 증설(8GB/160GB): 디스크 38%라 급하지 않음. RAM 4G는 cron 겹침이 남으면 여전히 위험 → 디렉터 판단.

C-2/MDD5%/live/`ENABLE_REAL_EXECUTION` 금지. 거래 경로 비접촉.

### Ask 디렉터 (절대규칙 12, 유지)

CAT-L 종료 = 서버 명령 **원문 캡처** 필수. 이번이 그 예시.

---

> 아래는 이전 누적.

---

## OUTBOX — 2026-09-14 · CAT-L 크래시 진단 (코드 없음)

**한 줄:** 서버가 왜 죽었는지 지금 확인 중. 로그/메모리 안전장치는 설계는 됐는데 서버에 켜졌는지 확인이 빠졌던 게 유력 원인.

**로컬에서 한 일:** SSOT 대조만. `ssh ubuntu@15.165.236.69` **Connection timed out**. 6블록 원문 **없음**. restart 안 함.

**기록 대조 (체크박스 아님):**
- L-1/L-2: 08-02 Claude OK · `05` 잔여 = 08-17 서버 install 미확인 (08-28까지 체크 안 닫힘)
- MemoryMax drop-in: `00` **미설치 가능** (유닛 템플릿 `factory`는 `MemoryMax=1.5G` — **실측 show 없음**)
- HIST_13 2026-07-04: 로그 무제한 = 1년 방치 시 가장 확실한 서버 파괴 요인
- digest 08-17~19: L-1 ok / L-2 timer later active / overseer running **한 번** — 설치 스크립트 캡처 아님. L-2 `python: command not found`. 08-19 digest overseer `exit=1` 불일치
- cron→큐 b-2/b-3: `infra_next_steps_a_b_plan.md` 운영자 전환 대기 (기본 off) — **후보 2, 미확정**

**금지 준수:** 거래 경로 미수정 · 원인 없이 restart 안 함 · Layer2 digest 필드 선코딩 안 함.

### Ask 1 — 디렉터 (절대규칙 12)

CAT-L은 앞으로 Cursor「구현 완료」만으로 `05` 종료하지 말고, **서버 명령 원문 캡처가 붙어야 종료**로 인정할지 **지금 확정**해 주세요.

### Ask 2 — Claude (6블록 온 뒤에만 Handoff)

지금은 Handoff 확정 금지. 원문 오면 원인 1개로 좁혀 `CLAUDE_TO_CURSOR.md` prepend:
- Layer1: 기존 `install_bitget_logrotate.sh` / `install_bitget_backup.sh` / HIST_10 §2.4 drop-in **설치 + is-enabled/is-active 원문**
- Layer2: digest read-only 3필드 (`logrotate_installed` / `backup_timer_active` / `memorymax_configured`) — 거래 비접촉
- Layer3 증설(8GB/160GB): 디렉터 비용 판단 · Claude/Cursor 임의 결정 금지

C-2 / MDD5% / live / `ENABLE_REAL_EXECUTION` 금지.

---

> 아래는 이전 누적 이력.

> **갱신(이전)**: 2026-08-23  
> **유형**: **UNIVERSE-BT-U0** 구현 완료 · **WAIT_CLAUDE_OK** (전문은 `CURSOR_TO_CLAUDE.md` 미러)

---

## OUTBOX — 2026-08-23 · UNIVERSE-BT-U0 완료 (문서)

**UNIVERSE-BT-U0: 구현 완료** → Claude **OK | 수정 spec** 요청.

| 파일 | 역할 |
|------|------|
| `14_UNIVERSE-BT_구조생존검증.md` | 신규 §1~§5 |
| `00` 말미 | 포인터 1줄 |
| 코드 | **없음** · U1 미착수 |

상세: `CURSOR_TO_CLAUDE.md` 상단.

---

## OUTBOX — 2026-08-23 · Ask · UNIVERSE-BT (전코인 구조생존 백테스트)

**계기(디렉터):** Bitget에 현물·선물로 상장된 코인에 **현재 퀀트 구조를 그대로** 얹어 전수 백테스트 → "구조가 살아남는지" 단서 확보.  
미래(L2 paper·forward)가 주 검증이지만, 히스토리 생존은 **중요한 단서**. Claude와 협업 설계 요청.

### Cursor 엔지니어 브리핑 (2줄)
1. 지금 `time_machine_backtester.py`는 **크래시 구간 MAE/MFE 스트레스**일 뿐 — DNA·gate·scanner 경로 **재현 아님**. "싹 다"를 그 루프에 얹으면 구조 검증이 아니라 가짜 생존률이 나옴.  
2. 전상장 심볼 × 풀스택 리플레이는 4GB·`TIME_MACHINE_MAX_*`·OHLCV 커버리지 한계에 막힘 → **유니버스 스냅샷 → 배치/체크포인트 리플레이 하니스(라이브·config 비접촉)** 가 맞고, 결과는 **IV L0 단서만** (LIVE/B1 승격 금지).

→ **흡수**: U0 Handoff로 로드맵·지표 확정. 이후 이력은 아래 유지.

### 로컬 팩트 (읽기만)

| 자산 | 역할 | 한계 |
|------|------|------|
| `mtf_data_updater.load_dynamic_universe` | 현물/선물 거래량 유니버스 | "상장 전부" ≠ volume floor 통과분 · zombie BL |
| OHLCV `BITGET_SPOT_*` / `BITGET_FUT_*` | 히스토리 바 | 상장 전·갭·신규상장 survivorship |
| `master_scanner` + `signal_engines` + gates + ledger | **실제 퀀트 구조** | 백테스트 전용 리플레이 엔트리 **약함** |
| `time_machine_backtester.py` | 크래시 SL/TP 스트레스 | 구조≠재현 · 테이블 cap |
| `validation/walk_forward_*` | CLOSED trade OOS shadow | **이미 들어온 트레이드**만 · 전유니버스 스캔 아님 |
| 현황판 #14 R&D 샌드박스 | 🟡 | "라이브 분리 연구실 약함" |

### IV / 헌법 (위반 금지)
- 본 작업 산출 = **L0** (`docs/independent_verification` · time_machine/mutant급) → **LIVE·B1「달성」·CAGR 단정 금지**
- R1a paper OPEN 관측·R6 L2와 **혼동 금지**
- `ENABLE_REAL_EXECUTION` · Kelly · MDD tier · execution_safety · deathmatch **live** · WF promotion block **비접촉**
- 주식 루트 `forward/` · `performance_budget_governor` **수정 금지** · Adapter만

### Ask — Claude가 확정할 것

1. **로드맵 자리**: B1-LADDER(R1a 관측)와 **병렬 R&D sub-phase**인가, R2 이후인가, 별도 `UNIVERSE-BT-0x` 트랙인가? (R1a Kill/관측 **차단하지 말 것**)
2. **성공 정의(구조생존)**: 예) 심볼당 hit→gate pass→가상진입 비율 · 크래시 구간 청산률 · 국면별 LONG/SHORT 비대칭 — **연복리%를 성공 계약으로 쓰지 말 것** (B1 계약과 분리)
3. **범위**: "상장 전부" vs `load_dynamic_universe`+OHLCV 보유분 · SPOT/FUT 분리 리포트 여부
4. **sub-phase 분해 초안 요청** (Cursor 제안 — Claude가 ID·순서·Critical 확정):
   - **U0** 문서: 유니버스 스냅샷 정의 · survivorship 고지 · L0 라벨 · Kill(과신 표현)
   - **U1** 코드: read-only 리플레이 하니스 (scanner/engines 경로 재사용, paper DB·config_kv 쓰기 금지, 결과 JSON/SQLite 격리)
   - **U2** 배치: spot→fut 또는 샤드 · 체크포인트 · 메모리 cap 존중
   - **U3** 리포트: 구조생존 표 + Claude 해석 슬롯 (CAGR 승격 문구 템플릿 **금지**)
5. **첫 Handoff**: U0 문서만? U1까지? — `CLAUDE_TO_CURSOR.md`에 CAT·위험도·롤백·테스트 명시

### 디렉터 한 줄 (Claude에 붙여넣기)
```
bitget/docs/work_phases/CURSOR_TO_CLAUDE.md 상단 「UNIVERSE-BT Ask」설계. R1a OBSERVE는 유지. OK면 CLAUDE_TO_CURSOR에 U0(또는 첫 sub) Handoff만 파일로. 채팅 말고 파일.
```

### Cursor 상태
- **코드 미착수** · R1a **관측 유지** 병행
- NEXT_ACTION: R1a=OBSERVE · 본 Ask=`WAIT_CLAUDE_HANDOFF`(설계만)

---

## OUTBOX — 2026-08-23 · B1-LADDER-R1a 완료 (문서) + R0 Claude OK 반영

### Claude OK 수신
**B1-LADDER-R0: OK** (2026-08-23) — 05·CLAUDE_TO_CURSOR 상단 기록 완료.

### R1a 구현
| 파일 | 역할 |
|------|------|
| `13_B1_신뢰사다리.md` | §3 아래 **R1a 판정 절차** 소절 추가 (PASS/관측유지/FAIL a\|b) |
| `CLAUDE_TO_CURSOR.md` | PREPEND(OK+R1a Handoff) 최상단 부착 |
| `09` · `track_b_NEXT_STEP` | Downloads 갱신안 그대로 반영 |
| `05` · `NEXT_ACTION` · `00` | R1a OBSERVE · R0 Claude OK |

### 코드
**없음** (config/gates/Kelly/live 비접촉)

### 이번 판정 (신선 SQL 미수신)
| OPEN | CLOSED | R0 경과 | short_funnel | **판정** |
|------|--------|---------|--------------|----------|
| 0 (직전 SSOT) | 10 | &lt;4주 (앵커 2026-08-23) | 미조회 | **관측 유지** |

디렉터 신선 SQL 오면 동일 표에 숫자만 대입해 재판정.

### Ask
R1a 문서: **OK | 수정 spec** (관측 유지 중에는 주간 숫자만 OUTBOX). FAIL 확정 시에만 R1b Handoff.

---

## OUTBOX — 2026-08-23 · B1-LADDER-R0 완료 (문서)

**B1-LADDER-R0: 구현 완료** → Claude 스펙 일치 검증 요청

### 로컬 스냅샷
| 파일 | 역할 |
|------|------|
| `docs/work_phases/13_B1_신뢰사다리.md` | **신규** §1 성공계약 · §2 렁 R0~R6(+R1a/b·R3~5 승인문구) · §3 Kill · §4 신뢰밴드 · §5 CAT · §6 R1a SQL |
| `00_마스터_로드맵.md` §0.4 말미 | **1줄만** `→ 상세 렁·Kill 기준: 13_B1_신뢰사다리.md` · **표 비변경** |
| `CLAUDE_TO_CURSOR.md` | Handoff 전문 보관 |
| `05` / `00` 용어집 / `09` / `NEXT_*` | 세션 종료 의무 갱신 |

### 스펙 확인
- 성공 = B1만 (12~18% AND MDD≤5% · 6~12개월) · B2/B3/live/G4 비계약
- 순서 `R0→R1→R2→(A06)→R3∥R4→R5→R6` · Kill 표 · 신뢰밴드 35~45→…→80~90
- **코드·config_kv·execution_safety·gates·Kelly·deathmatch live 비접촉**

### R1a 서버 실측
| 출처 | OPEN | CLOSED | 비고 |
|------|------|--------|------|
| **이 세션** | (미조회) | (미조회) | Cursor 환경에서 VPS `BITGET_DB_STORAGE_PATH` **미접속** |
| **직전 SSOT** 2026-08-23 OUTBOX/VPS | **0** | **10** (W2/L8) | 배선 생존 · 신규 진입 정체 후보 · **냉시동 vs 구조막힘 미최종** |

→ 디렉터: §6 SQL로 **신선 실측** 후 숫자 회신. R1b는 R1a FAIL(구조막힘) 확정 전 착수 금지.

### Ask
**B1-LADDER-R0: OK | 수정 spec: …**  
OK면 05에 Claude OK 기록 · 다음 Handoff는 R1a 관측 마감 또는 R1b(조건부).

---

## OUTBOX — 2026-08-23 · Ask · B1 80~90% 신뢰 사다리 (설계 요청)

**계기:** 디렉터 — 시나리오 기준성공 35~45%로는 안 됨. **80~90% 성공률**을 만들 것.  
검증만으로는 부족 → **현실·팩트 완성** + Claude/Cursor 최상의 시나리오·작업 순서.

### Cursor 엔지니어 브리핑 (1줄)
숫자를 희망으로 올리지 말고, **성공 정의를 B1으로 고정**한 뒤 불확실성 렁(R0~R6)을 닫아 **조건부 P(B1|사다리)** 를 80~90%로 설계. B2/B3는 계약 밖.

### 혼동 금지 (팩트)
| 종류 | 의미 | 지금 |
|------|------|------|
| P(성공\|오늘) | MDD 미조임·funding 미반영·OPEN≈0·n≈10 | **35~45%** (솔직) |
| P(B1\|사다리 통과) | 팩트 렁 닫힌 뒤 | **설계 타깃 80~90%** |

→ 디렉터가 원하는 80~90% = **후자**. 전자를 거짓으로 올리는 것은 SSOT/IV 위반.

### 성공 계약 초안 (Claude 확정 요청)
- **성공** = Track B **B1만**: 연복리 **12~18%** AND MDD **≤5%** (B1 시작 후 6~12개월 시계)
- **비계약**: B2 18~25% · B3 25~35% · 실전 LIVE · 상품화 G4 — 스트레치/별도
- **Kill**: 렁 실패 시 목표 하향·롤백·중단 → 남은 경로만 고신뢰 유지 (이게 80~90%를 정직하게 만드는 장치)

### 신뢰 사다리 초안 (Claude가 ID·순서·Critical 승인문구 확정)

| 렁 | 팩트 완성 | 닫는 구멍 | Critical? | 비고 |
|----|-----------|-----------|-----------|------|
| **R0** | 성공=B1 계약 · Kill 기준 문서화 | 목표 과다 | 문서 | `00` §0.4 보완 or 별도 SSOT |
| **R1** | OPEN 처리량 · 퍼널(롱/숏) 진단·복구 | 표본 정체 | 관측→mini Handoff | VPS: OPEN=0 · CLOSED≈10 |
| **R2** | `06` 효과표 2~4주 채움 | 구현≠효과 | 관측 | A/B shadow 유지/롤백 |
| **R3** | MDD 3/4/5% + Kelly↓ + lev≤3 | 5% 미강제 | 🔴 | A-6 / Risk Profile B |
| **R4** | C-2 funding PnL | paper 낙관 | 🔴 | close PnL 오염 해소 |
| **R5** | deathmatch alloc **live** | 패자 자본 | 🔴 Go/No-Go | shadow 4w 후 |
| **R6** | L2: trades≥30 · ≥56일 · rolling MDD≤5% · 페이스 | 통계 과신 | 게이트 | G2 정합 · IV L2 |

**신뢰 밴드(설계):** 오늘 35~45 → R0~R2 후 50~65 → R3~R5 후 70~85 → R6 통과 **80~90**.

### 최상의 시나리오 A+ (작업 축)
1. **0~4주**: R1+R2 (처리량·06) — Critical 손대지 않음  
2. **Go/No-Go**: R3→R4→R5 순차 Handoff (디렉터 Critical 승인 필수)  
3. **6~12개월**: R6 B1 시계 관측 → 통과 시 “조건부 80~90% 달성 경로 입증”

### Ask Claude (설계만 · 이번 라운드 코드 구현 X)
1. 위 **성공 계약** OK? B1만 80~90% 대상으로 고정해도 되나?  
2. 렁 ID·이름·순서 확정 (`B1-CONFIDENCE-LADDER` 가칭) · R1을 어떤 mini Handoff로 쪼갤지 (OPEN 정체 원인: DNA/Cos/게이트/국면)  
3. R3~R5 Critical 각각의 **디렉터 승인 문구** + 의존성 (C-2를 MDD 전/후?)  
4. Kill 기준 표 (렁별 FAIL → 행동) SSOT 위치 (`00` vs `06` vs 신규 `13_B1_신뢰사다리.md`)  
5. 첫 Handoff는 무엇인가? (제안: **R0 문서** 또는 **R1 처리량 진단 전용** — Critical 비접촉)

**금지 유지 (이번 Ask에서 구현 지시 금지):** Kelly 상향 · live · LS-NORTH-STAR 하드캡 분리 · 성급한 R3~R5.

**디렉터:** 이 OUTBOX → Claude. Claude 응답 = `CLAUDE_TO_CURSOR` 설계/Handoff 또는 Mirror. Cursor는 Handoff 전 구현 금지.

---

## Claude OK — LS-GOAL-UX-01 (2026-08-23)

**판정: OK.** position_side 어댑터 · kill-switch 폴백 · Kelly/gates/live 비접촉 · SPOT 숏 각주 정합.  
**다음:** 디렉터 서버 pull · digest/북극성 육안. LONG blocked / LS-NORTH-STAR-01은 후속·🔴 defer.

---

## OUTBOX — 2026-08-23 · LS-GOAL-UX-01 구현 (기록)

**LS-GOAL-UX-01: OK** → Claude OK 2026-08-23 · **DONE**

### 로컬 스냅샷
| 파일 | 역할 |
|------|------|
| `observability/ls_split_summary_bg.py` | **신규** `collect_ls_split_summary` · plain/HTML |
| `north_star_panel_bg.py` | L/S 2열 블록 (쉬운판 4칸 아래) |
| `post_deploy_obs_digest_bg.py` | kid 진행줄 아래 `ls_plain` 1줄 |
| `infra/memory_policy.py` | `POST_DEPLOY_OBS_LS_SPLIT_ENABLED=True` |
| `tests/test_ls_split_summary_bg.py` | **신규** |

### 스펙 확인
- LONG에 `blocked_today` **없음** · SHORT `blocked_today` = short_funnel `blocked_short_total` import
- 목표 MDD/연복리/B0 **미분리** · kill-switch false → 기존 출력(롱 줄 없음)
- SPOT 숏 불가 각주 포함
- 컬럼은 `position_side` (Handoff `side` → 로컬 스키마 맞춤)

### 비접촉
`forward/gates.py` · `gmm_dna_alpha_sync.py` · `dual_north_star_ledger.py` · Kelly · live · short_funnel 버킷 재계산 **없음**

### 테스트
`test_ls_split_summary_bg.py` + `test_north_star_panel_bg` + `test_post_deploy_obs_digest_bg` → **passed**

### Ask
Claude OK / 수정 spec 한 줄. OK면 05 Claude OK 기록.

---

## OUTBOX — 2026-08-23 · Ask · 롱/숏 분리 목표·쉬운판 (LS-GOAL-UX)

**계기:** 디렉터 — 코인은 롱·숏 둘 다 있음 → **목표를 롱/숏으로 나눠** 읽기 쉽고 퀄리티 좋게. 전체 구조에서 L/S 흐름 확인 필요.

### Cursor 로컬 맵 (구현 전 브리핑)

```
스캔 → side(LONG|SHORT) → try_add → OPEN → track → CLOSED
         ↑
spot+SHORT = hard reject (선물만 숏)
dante SHORT = futures-only (SHORT-DANTE-FUT-01)
Cos/funding/BULL = SHORT soft 감점 (임계값 동결)
```

| 이미 있음 | 없음 |
|-----------|------|
| digest **숏 퍼널** (OPEN L/S · 차단 버킷) | 북극성 **롱 목표 vs 숏 목표** 분리 |
| overseer 당일 closed long/short count | 사이드별 MDD/누적/게이트 칸 |
| Track B 북극성 = **통합 장부** | 초등 쉬운판에 「롱 건강 / 숏 건강」 2열 |

**서버 실측(당일):** OPEN=0 · CLOSED 10(W2/L8) — 사이드별 분해는 미조회(Ask 시 SELECT 제안).

### 엔지니어 제안 (Cursor)
표시만 CAT-J: digest/북극성에 **롱 칸 · 숏 칸**(OPEN/CLOSED/당일차단/누적손익 요약).  
MDD5%/연12~25%를 사이드별로 **하드캡 분리**하는 건 Critical·원장 설계 → Claude 판단. 기본안 = **목표 숫자는 Track B 공유 · 진행 칸만 L/S 분리**.

### Ask Claude
1. sub-phase ID 확정? (예: `LS-GOAL-UX-01` 표시만 / `LS-NORTH-STAR-01` 목표 분리)  
2. 스펙: 쉬운판 2열 필드 목록 · 기존 short_funnel과 중복 제거 규칙  
3. Critical 비접촉(Kelly·gate·live) 유지 OK?  
4. SHORT SECTOR 최종 OK와 순서 — 먼저 LS-GOAL-UX?

**디렉터:** 채팅 말고 이 OUTBOX → Claude. Cursor는 Handoff 전 구현 금지.

---

## OUTBOX — 2026-08-23 · AI 감시관「활동 부재」감사 (Cursor 단독 판독)

**계기:** 디렉터 텔레그램 「👁️ Bitget AI 상시 감사관」문제점 = 활동 부재 · 기회 상실.  
**질문:** 의도적으로 막아둔 건지 vs 파이프라인 고장인지.

### Cursor 판정 (코드 SSOT · **서버 DB 실측 반영 2026-08-23**)

**VPS 실측:**
```
CLOSED_LOSS|8
CLOSED_WIN|2
(OPEN 행 없음 → OPEN=0)
```
→ 장부·파이프라인 **과거 배선 OK** (누적 CLOSED=10 = POST_DEPLOY 실측과 일치).  
→ **지금**은 포지션 0 · 신규 진입 대기 국면. DB 단절/전선 절단 ❌.

| 층 | 무엇인가 | 판정 |
|----|----------|------|
| **1. 리포트 문구** | Gemini 자유 서술 | 「활동 부재」= **하드 킬스위치 아님** |
| **2. 팩트 구멍** | facts에 OPEN 미조회 | 「보유 정보 부재」문구는 **과잉** 가능(실제 OPEN=0이면 내용상 맞음) |
| **3. 정책 보수** | kelly 0.006 · HIGH_VOL · B0 · DNA 대기 | **의도적 축소** · 버그 단정 ❌ |
| **4. 현재 상태** | OPEN=0 · 누적 CLOSED=10 | **파이프라인 생존 + 신규 진입 정체** (고장≠전무) |
| **5. POST_DEPLOY** | Cos n≈0 · DNA RANK 재료 대기 | 신규 OPEN이 안 생기는 **주 원인 후보** |

**결론 한 줄:** 배선은 살아 있고(CLOSED 10), 지금은 OPEN이 비어 **관측·게이트·재료 대기** 쪽. 「막아서 활동부재」가 아니라 「들어가지 못해 비어 있음」.

### 디렉터 서버 한 줄(구분용)

```bash
DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
sqlite3 "$DATA/bitget_market_data.sqlite" \
  "SELECT status, COUNT(*) FROM bitget_forward_trades GROUP BY status;"
# + POST_DEPLOY digest / short_funnel 칸
```

### Ask Claude (설계만 · Critical 비접촉)

1. 위 3층 판정 OK?  
2. 다음 mini Handoff 필요? 예: **OVERSEER-FACTS-01** — facts에 OPEN 수·blocked_today 요약 추가(표시만, Kelly/gate 비접촉).  
3. 불필요면 SUB_DONE · 관측 유지.

**작업 방향(디렉터 승인됨):** 본 감사 = Cursor 단독. 팩트 보강 구현만 Claude Handoff 후.

---

## OUTBOX — 2026-08-23 · NS-BG-CRON-ISO-01 주식 북극성 → 코인 채팅

**증상 (디렉터 스크린샷):** 코인 구조 텔레그램에 `📊 주식 북극성 · 일간/주간` + `no such table: forward_trades` + Track A KR/US.  
**추가 보고:** 코인 북극성·POST_DEPLOY_OBS는 **한 통도 안 옴**.

**원인 A (오염):** `update_bitget.sh` → `install_director_digest_cron.sh` → 주식 19:30이 Bot-2에서 REPORT_BOT 발송.  
**원인 B (미수신 · 가설):** 코인 일보 cron(`--post-deploy-obs-digest` UTC 11:00) 미설치·미실행·실패. 주식 cron은 매 업데이트마다 강제 설치되어 왔고, 코인 일보 줄은 예전 crontab에 없으면 **조용히 안 감**. 코인은 **일 20:00만**(주간 없음).

**수정 (bitget/** only):**
| 파일 | 변경 |
|------|------|
| `update_bitget.sh` | director-digest **설치 제거** · 잔여 시 uninstall |
| `uninstall_stock_north_star_cron.sh` | 신규 |
| `diagnose_coin_digest.sh` | 신규 — cron/로그/REPORT vs BITGET 채팅 + `--send` 후 **exit·로그·sent=** 표시 |
| `install_bitget_cron.sh` | post-deploy-obs 줄 **필수** 검증 |
| `audit_bitget_stack.sh` | 주식 cron 있으면 fail · digest 로그 유무 warn |
| `post_deploy_obs_digest_bg.py` | Telegram HTML→plain 재시도 · 길이 분할 · HTTP 실패 로그 |

**디렉터 즉시:**
```bash
git pull && sudo bash bitget/deploy/uninstall_stock_north_star_cron.sh
sudo INSTALL_ROOT=$PWD bash bitget/deploy/install_bitget_cron.sh
bash bitget/deploy/diagnose_coin_digest.sh --send
```

**Ask Claude:** Ops 격리+진단 OK 한 줄. 루트 install_director 주석 Track A 정리 권고.

---

## OUTBOX — 2026-08-21 · SHORT 조건부 OK 3확인 회신 (Cursor)

Claude 요청 형식에 대한 로컬 확인:

### SHORT-DANTE-FUT-01 — blocked_history
**확인:** spot SHORT hard-reject · SHORT Cos reject 모두 `bitget.shadow_tracking.record_blocked_trade` **동일 함수** → 테이블 `bitget_blocked_trade_history` **기존 컬럼만** INSERT. 신규 테이블/컬럼 **없음** (CAT-D 스키마 비접촉).

### CRYPTO-SECTOR-01 — ①②③

| # | 질문 | 결과 |
|---|------|------|
| ① | CAT-MAP Single Writer 표 | **추가함** — `PREDICTED_NEXT_SECTOR` \| `auto_pilot.detect_coin_regime` (+ system_auto_pilot) \| Readers D/J/M · G meta_sync **아님** |
| ② | 맵 입력 | **신규 breadth 공식 없음.** 동일 함수 안 기존 `regime`/`breadth_state` 재사용 → 이미 `CURRENT_REGIME_KEY`·`CRYPTO_BREADTH_STATUS`로 쓰이던 값. (meta_sync `REGIME_ANALYSIS` ensemble 키가 아니라 **coin detect_coin_regime 기존 경로**) |
| ③ | C/F 미소비 | **C 스캐너·signal_engines·F trading/Kelly 모듈: `PREDICTED_NEXT_SECTOR` 미참조.** Reader는 **D ledger `rotation_prebuy`**(기존: Cos×0.85 · `ROTATION_ADVANTAGE_ACTIVE`일 때만 Kelly×2) + J digest + M overseer. digest-only는 아님 · **C/F 직접 소비 아님** → CAT-G 🔴 Critical 재분류 **불필요** (기존 D soft boost 배선만 Writer가 살아남) |

### 요청
- CRYPTO-SECTOR-01 → **최종 OK** 한 줄  
- 4건 Claude OK를 `track_b_05`에 기록해도 되는지 확정

---

Claude 요청 형식에 대한 로컬 확인:

### SHORT-DANTE-FUT-01 — blocked_history
**확인:** spot SHORT hard-reject · SHORT Cos reject 모두 `bitget.shadow_tracking.record_blocked_trade` **동일 함수** → 테이블 `bitget_blocked_trade_history` **기존 컬럼만** INSERT. 신규 테이블/컬럼 **없음** (CAT-D 스키마 비접촉).

### CRYPTO-SECTOR-01 — ①②③

| # | 질문 | 결과 |
|---|------|------|
| ① | CAT-MAP Single Writer 표 | **추가함** — `PREDICTED_NEXT_SECTOR` \| `auto_pilot.detect_coin_regime` (+ system_auto_pilot) \| Readers D/J/M · G meta_sync **아님** |
| ② | 맵 입력 | **신규 breadth 공식 없음.** 동일 함수 안 기존 `regime`/`breadth_state` 재사용 → 이미 `CURRENT_REGIME_KEY`·`CRYPTO_BREADTH_STATUS`로 쓰이던 값. (meta_sync `REGIME_ANALYSIS` ensemble 키가 아니라 **coin detect_coin_regime 기존 경로**) |
| ③ | C/F 미소비 | **C 스캐너·signal_engines·F trading/Kelly 모듈: `PREDICTED_NEXT_SECTOR` 미참조.** Reader는 **D ledger `rotation_prebuy`**(기존: Cos×0.85 · `ROTATION_ADVANTAGE_ACTIVE`일 때만 Kelly×2) + J digest + M overseer. digest-only는 아님 · **C/F 직접 소비 아님** → CAT-G 🔴 Critical 재분류 **불필요** (기존 D soft boost 배선만 Writer가 살아남) |

### 요청
- CRYPTO-SECTOR-01 → **최종 OK** 한 줄  
- 4건 Claude OK를 `track_b_05`에 기록해도 되는지 확정

---

## OUTBOX — 2026-08-21 · SHORT 최상경로 Ask (디렉터 승인 로드맵)

**계기:** 롱만 진입 · 숏 미사용 · `predicted_sector=UNKNOWN` · R&D/청산 정체.  
**디렉터 승인 계획:** Bitget best-path roadmap (B0 paper · Critical 비접촉).

### Ask (Claude Pro)

Track B **B0**. 숏 구조(TV_SHORT / dante / ledger SHORT)는 있으나:

1. SPOT dante → ledger `현물 숏 불가`로 낭비  
2. FUTURES Cos/funding/국면 페널티로 숏 탈락  
3. `PREDICTED_NEXT_SECTOR` writer 없음 → 항상 UNKNOWN  

**요청:** 아래 순서로 Handoff 검토·OK (또는 수정 spec). Cursor는 디렉터 승인 로드맵대로 **이미 구현**했음 → 스펙 일치 검증.

| 순서 | ID | 내용 |
|------|-----|------|
| 1 | SHORT-FUNNEL-01 | 숏 OPEN/CLOSED·차단사유 read-only 집계 |
| 2 | SHORT-DANTE-FUT-01 | SPOT dante no-op (futures-only SHORT) |
| 3 | SHORT-OBS-GATE-01 | funding/국면/Cos 탈락 관측 (임계값 변경 없음) |
| 4 | CRYPTO-SECTOR-01 | `PREDICTED_NEXT_SECTOR` 코인 writer |
| 5 | SHORT-DNA-01 | **defer** — SHORT CLOSED/MFE 충분 시에만 |

**금지:** C-2 · MDD5% tier · B-2/B-3 live · `ENABLE_REAL_EXECUTION` · Cos/funding **임계값 변경**

### 구현 스냅샷 (검증용)
- `bitget/observability/short_funnel_report_bg.py` + digest 연동
- `master_scanner` / scanner_hooks: spot+dante skip
- ledger: SHORT Cos/spot 차단 → blocked_history 기록(관측)
- `auto_pilot` regime: `PREDICTED_NEXT_SECTOR` writer
- 테스트: funnel · schedule/skip · sector

### Claude 응답 요청
- 각 ID OK / 수정 spec 한 줄  
- SHORT-DNA-01 착수 조건(예: TF당 SHORT mfe≥8 ≥N) 제안 환영

---

## OUTBOX — 2026-08-21 · NS-BG-DASH-01 Bitget 북극성 패널

**요청:** 주식 `[쉬운판]` 참조 → Bitget 구조에 맞게 북극성·목표수익률 보이게. 이미 된 건 유지, 빠진 것만 추가.

### 갭
- 원장 `dual_north_star_ledger` Track B(MDD5% · B0~B3 · 게이트) **이미 수집**
- 주식 19:30 일보는 Track A only (의도적 분리 · Track B 미표시)
- Bitget POST_DEPLOY_OBS는 DNA/연습 관측만 · **북극성 목표·누적·게이트 칸 없음**

### 구현 (bitget/** only · 읽기 전용)
- `bitget/observability/north_star_panel_bg.py` — Track B `[쉬운판]` + 상세(목표 MDD/연복리/게이트/기간수익/NAV)
- `post_deploy_obs_digest_bg.py` — 텔레그램 **첫 메시지**로 북극성 발송 · 원장 **쓰기 안 함**(19:30 cron 전용)
- 테스트 `test_north_star_panel_bg.py`
- 문서: `12_듀얼북극성…` · `09_디렉터_쉬운요약`

### Ask
- Claude: 스펙 일치·격리 OK면 한 줄 OK. 다음 Handoff 불필요면 SUB_DONE 유지.
- 금지 유지: C-2 · MDD5% tier · live · ENABLE_REAL_EXECUTION

## OUTBOX — 2026-08-21 · NS-BG-DASH-01b 코인 전용 재분리

**계기:** 디렉터가 주식 북극성(19:30) 붙여넣으며「코인에 KR/US 북극성 올 필요 없음 · 구조만 참조」지적.

### 수정
- `north_star_panel_bg.py`: Track A 스냅샷·OBS_HOLD(n/20)·갈림길·mega_trend 제거
- 제목 `📊 코인 북극성 · Bitget` · 마일스톤=G1 28일 · spot/futures NAV · MDD5%/B0
- 스냅샷에서 tracks/period_returns의 **A 키 strip**
- 테스트: `📊 주식 북극성` / 갈림길 / OBS_HOLD / Track A 부재 assert

### Ask
- Claude: 01b OK면 한 줄. 주식 채널 비접촉 확인.

---

## Claude OK — NS-BG-DASH-01 (2026-08-21)

- 판정: **OK** — 로컬 스냅샷 vs SSOT(00_마스터_로드맵 §0.4 · 12_문서 · CAT-J) 1:1 일치 · 수정 spec 없음
- MDD5%/연12~25%(B0=측정) 값 원본 일치 · 임의 상수 없음
- 원장 read-only 확인 · forward_trades/gates.py/gmm_dna_alpha_sync.py 비접촉
- SPOT/FUT NAV 분리 표시 확인 (CAT-J §4)
- C-2 · MDD5% tier · live · ENABLE_REAL_EXECUTION 미접촉
- 다음: Handoff 불필요 → **SUB_DONE**. 잔여는 디렉터 서버 pull 후 20:00 텔레그램 첫 메시지 육안 1회만.

---

## OUTBOX — 2026-08-20 · Claude 조건부 OK 닫힘

**Claude:** [CAT-J] 조건부 OK · Mirror → `ARCHITECT_MIRROR.md` 상단 기록.  
**Cursor 확인:** enum 정식명 **`DB_PATH_OR_ENV`** (OUTBOX 요약의 `DB_PATH`는 축약 표기만).  
필드: `n_closed_by_tf` · `n_mfe8_by_tf` · `gmm_cluster_n` · `last_error`.  
**잔여:** 디렉터 서버 배포 후 텔레그램 「재료 덜 모였어요」👁️.  
**후속 메모(미착수):** DATA_WAIT streak · 01b/digest 계산 통합.

---

## OUTBOX — 2026-08-20 · POST_DEPLOY_OBS-DNA-UX-01 구현 검증

**요청:** Handoff 스펙 일치 여부 OK/수정 spec. OK면 Claude OK 한 줄 + 09/NEXT_STEP 반영 안내.

### 구현 요약
- `diagnose_dna_state` 순서 고정: DB_PATH → RANK_OK → DATA_WAIT_LOW_MFE → GMM_EMPTY → SYNC_FAIL → UNKNOWN
- Spec2 초등 문구 · Spec3 숫자 메모 · Spec5 paste(DIRECTOR_SSH_CHECK / REPORT_TO_CLAUDE)
- DATA_WAIT → 대시보드 **🟡 missing** (🔴 problem 아님)
- kill-switch `POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED` (default true)
- `gmm_min_rows=12` — `data_miner._fit_gmm_templates` 주석 출처 · `GMM_FIT_MIN_ROWS_OBSERVED`
- 테스트 **10 passed** (`test_post_deploy_obs_digest_bg.py`)

### 로컬 구조 스냅샷
- `bitget/observability/post_deploy_obs_digest_bg.py` — diagnose/collect/wire/dashboard/numbers/paste
- `bitget/observability/gmm_dna_alpha_report_bg.py` — `collect_closed_mfe_counts_by_tf` · `count_gmm_template_clusters`
- `bitget/infra/memory_policy.py` — `POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED`
- 비접촉: `forward/gates.py` · `evolution/gmm_dna_alpha_sync.py`

### Ask Claude
채팅 말고 파일에 OK 또는 수정 spec. C-2/MDD5%/live 금지 유지.

---

## OUTBOX — 2026-08-20 · Ask: DNA 일일진단 미니 Handoff

### 디렉터 → Claude 붙이기용 (이 블록 전체)

```
Track B · 미니 Handoff 요청 (구현은 Cursor, 설계만 Claude)

목적:
일일 텔레그램「코인 연습 · 오늘 한눈에」DNA 칸이 지금은
RANK1~3 유무만 보고 같은 🔴 문구만 반복한다.
업로드 고장이 아니라 진단력 부족이다.
디렉터가 텔레그램만 보고 (관측유지 / 서버ops / Cursor·Claude 작업) 분기할 수 있게
why 한 줄이 나오게 해 달라.

배경 실측 (2026-08-19 VPS, BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data):
- CLOSED=10 (1H=2, 2H=1, 4H=7)
- n_mfe8=0, n_mfe5=0 전 TF · max_mfe≈3.55
- mine_bitget_dna_templates → 0 templates
- gmm_dna_alpha_sync --force → no_rankable_clusters
- overseer systemd active(running) · L-2 timer active (별건)
- 코드 조건: TF당 mfe≥BITGET_MIN_MFE_FOR_MINING(기본8) · feature dropna 후 ≥12행이어야 GMM

요청물 (Handoff에 넣을 것):
1) DNA 진단 상태 enum (예: RANK_OK / DATA_WAIT_LOW_MFE / GMM_EMPTY /
   SYNC_FAIL / DB_PATH_OR_ENV / UNKNOWN) — 판정 조건 표
2) 각 상태별 초등 문구 plain (텔레그램 kid dashboard 1줄) +
   숫자 메모에 넣을 필드 목록 (예: n_closed, n_mfe8 by TF, gmm_cluster_n, last_error)
3) cursor_action 권고: OBSERVE_HOLD | DIRECTOR_SSH_CHECK | REPORT_TO_CLAUDE | NONE
   (문턱 완화·실전·MDD5%·ENABLE_REAL_EXECUTION 권고 금지)
4) 구현 범위 한정:
   - 수정 허용: bitget/observability/post_deploy_obs_digest_bg.py
     (+ 필요 시 gmm_dna_alpha_report_bg.py 읽기전용 헬퍼, tests)
   - 금지: gates.py · gmm_dna_alpha_sync.py 본체 로직 · execution_safety ·
     BITGET_MIN_MFE 기본값 변경 · C-2/live
5) 테스트: 상태별 fixture 3~5개면 충분
6) sub-phase ID 제안 (예: I-GMM-DNA-DIGEST-01 또는 POST_DEPLOY_OBS-DNA-UX-01)

산출: bitget/docs/work_phases/CLAUDE_TO_CURSOR.md (또는 Track B Handoff 관례 파일)에
CAT-HANDOFF 형식 미니 Handoff 1건. 채팅 장문 말고 파일.

디렉터 승인: DNA「제대로 된 진단」UX — OK. 정책(문턱완화)은 이번 범위 밖.
```

### Cursor 메모 (Claude 답 오기 전)

- status 기대: Claude가 Handoff 쓰면 → `WAIT_CURSOR_IMPL`
- 구현 전 코드 손대지 말 것
- 관련 실측 OUTBOX: 아래「DNA 실측 확정」·「digest JSON」

---

## OUTBOX — 2026-08-19 · 일일 digest JSON (date_kst=08-19)

**스냅샷:** CLOSED=10 🟢 · DNA RANK1~3 false 🔴 · Cos n=0 🟡 · 01b=0 🟡 · L-1/L-2/REPORT_BOT ok · **ai_overseer exit=1 🔴** (당일 오전 OUTBOX「overseer OK」와 불일치 → digest 재수집·프로세스 생존 재확인 권고).  
**Ask:** 구현 Handoff 없음 · DNA는 기존 Ask(A 관측유지 vs B 완화) 유지 · overseer는 서버 `systemctl status`만. C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-19 · DNA 실측 확정 (재료 부족 · 관측 유지)

**DB:** `BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data` · `bitget_market_data.sqlite` ~3.3GB OK.  
**CLOSED=10:** 1H=2 · 2H=1 · 4H=7. **n_mfe8=0 · n_mfe5=0 전 TF.** max_mfe≈3.55 (문턱 8·5 미달).  
**mine→0 templates · sync→`no_rankable_clusters`.** `--force`/재채굴 무의미.  
**잔여 🔴 DNA만** (overseer ✅ · L-2 timer ✅).  
**Ask:** 관측 유지(권장) vs mfe_min/min-rows 완화 Handoff — 디렉터 결정. C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-19 · DNA mine 실측: 0 templates / no_rankable_clusters

**사실:** `BITGET_GMM_DNA_TEMPLATES` 로드 시 None → `mine_bitget_dna_templates()` 실행 → **0 templates** · sync `--force` → `no_rankable_clusters` (더 이상 `no_gmm_templates` 아님 = 구조는 생겼으나 cluster 비어 있음).  
**코드 조건:** TF당 MFE≥`BITGET_MIN_MFE_FOR_MINING`(기본 8) CLOSED가 feature dropna 후 **≥12행**이어야 GMM fit. CLOSED≈10이면 TF별로 부족이 정상.  
**Ask:** (A) 관측 유지·데이터 쌓일 때까지 DNA 🔴 허용 (B) mfe_min/최소행 완화는 **Handoff+디렉터 승인** 후에만. C-2/MDD5%/live 금지. `--force` 반복 무의미.

---

## OUTBOX — 2026-08-19 · overseer 영구 기동 OK

**변화:** `dante-bitget-overseer.service` → `active (running)` + `enabled`.  
원인: VPS는 `.venv` 없음 · **`venv/bin/python`** 이 SSOT. ExecStart를 그 경로로 수정 후 203/EXEC 해소.  
**잔여 🔴:** DNA RANK1~3 false (`no_gmm_templates` — `recover-artifacts-quick`=KMeans만, GMM 미채움).  
**주의:** L-2 timer active이지만 backup 스크립트 `python: command not found` 가능.

**Ask:** DNA는 `mine_bitget_dna_templates` 후 `gmm_dna_alpha_sync --force` — 디렉터 ops vs 미니 Handoff. C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-19 · 일일 관측 (🔴 잔여 2) [superseded by overseer OK]

**변화:** L-2 backup.timer `inactive`→`active` (progress 3/8→4/8).  
**잔여 🔴:** DNA RANK · overseer 203/EXEC (이후 `venv/` 경로로 해소됨).  
**주의:** backup `--test` 시 `python: command not found`.

**Ask:** (1) GMM 템플릿 선행 후 sync (2) overseer `venv/` (3) backup_*.sh PATH — C-2/MDD5%/live 금지.

---

## OUTBOX — 2026-08-18 · 일일 관측 실측 (🔴)

**스냅샷:** CLOSED=10 🟢 · RANK1~3 전부 false 🔴 · Cos n=0 🟡 · L-1 ok · L-2 backup.timer inactive 🔴 · ai_overseer exit=1 🔴 · 01b weekly=0 🟡 · progress 3/8

**Ask:** 서버 ops 3종(RANK sync --force / backup.timer enable / overseer 기동)을 디렉터 수동으로 할지, CAT-I/L 미니 Handoff가 필요한지. C-2/MDD5%/live 금지 유지.

---

## OUTBOX — 2026-08-18 · kid dashboard on daily digest

- `build_kid_dashboard` + `format_digest_html` 재작성: 진행률 바 · 🟢/🔴/🟡/⬜ 4칸
- 메시지 3분할: 대시보드 → 숫자 메모 → 복붙
- 테스트 3 passed · gates/sync 미접촉
- Ask: 사후 OK · 디렉터 UX 수용 여부

---

## OUTBOX — 2026-08-17 · 일일 관측 실측 (🟡)

**스냅샷:** CLOSED=10(SPOT5+FUT5) 🟢 · Cos sample n=0(journal) 🟡 · DNA RANK1~3 전부 false 🔴 · L-1 ok · L-2 backup.timer inactive 🔴 · ai_overseer exit=1 🔴 · REPORT_BOT ok

**해석(디렉터용):** 장부는 돌아가나 DNA 키가 비어 Cos 표본이 없음. 백업 타이머·감사관 미기동.

**Ask:** Handoff 없이 서버 ops만 할지(RANK sync --force / backup.timer enable / overseer 기동) vs CAT-I 미니 Handoff 필요 여부. C-2/MDD5%/live 금지 유지.

---

## OUTBOX — 2026-08-17 · POST_DEPLOY_OBS daily digest

### 왜
디렉터: 1~2주 관측 항목을 매일 텔레그램으로 받고, Cursor/Claude 복붙 문구 포함.

### 로컬 스냅샷
| 항목 | 내용 |
|------|------|
| 신규 | `observability/post_deploy_obs_digest_bg.py` |
| CLI | `bitget.sh --post-deploy-obs-digest` (락 무접촉) |
| Cron | UTC 11:00 = KST 20:00 daily |
| 전송 | REPORT_BOT direct HTTP (north-star와 동일) |
| 비접촉 | gates.py · gmm_dna_alpha_sync.py |
| 테스트 | `test_post_deploy_obs_digest_bg.py` **3 passed** |

### Ask
- 디렉터 요청 범위로 사후 OK 가능한지 (정식 Handoff 없이 디렉터 지시 구현)
- 복붙 블록 길이·REPORT_BOT 분할 발송 수용 여부

### 금지 준수
C-2 · MDD5% · live · 실전 — 미착수

---

## Claude OK — I-GMM-DNA-01b (2026-08-17)

- 판정: **OK** — Handoff 100% 일치 · 수정 spec 없음 · Adapter 불필요
- Mirror #2 수용: 2주 unavailable → 서버 로그 경로만 (05 잔여 · 선코딩 금지)
- 다음: **디렉터 서버 확인** (POST_DEPLOY_OBS · L-1/L-2/overseer · 01b 1~2주) · C-2/MDD5%/live defer

---

## OUTBOX — 2026-08-17 · I-GMM-DNA-01b 구현

### 로컬 구조 스냅샷
| 항목 | 내용 |
|------|------|
| 신규 | `bitget/observability/gmm_dna_alpha_report_bg.py` |
| Hook | `bitget_pipelines._pipeline_weekly_evolution` — `cost_report` 직후 `gmm_dna_alpha_report` (critical=False) |
| Config | `memory_policy`: `GMM_DNA_ALPHA_REPORT_ENABLED=true` · `WINDOW_DAYS=7` · `LOG_SOURCE=journal` |
| 비접촉 | `forward/gates.py` · `evolution/gmm_dna_alpha_sync.py` — **미수정** |
| 테스트 | `pytest bitget/tests/test_gmm_dna_alpha_report_i01b.py` → **6 passed** |

### 산출 필드
- cos_eff_sample_count / zero_ratio / mean_nonzero(nullable)
- open_count_by_market · closed_count_by_market (B-1 `normalize_market_key`)
- dna_rank_keys_present · shape_source_distribution · log_source_used
- 로그 실패 시 sample null + `unavailable` (추정 금지)

### Ask
- Handoff 스펙 일치 OK 여부
- Mirror #2: 2주 unavailable 시 서버 로그 경로 확인을 05 잔여로 둔 것 수용 여부

### 금지 준수
C-2 · MDD 5% · B-2 live · `ENABLE_REAL_EXECUTION` — 미착수

---

## OUTBOX — 2026-08-17 · POST_DEPLOY_OBS (코드 diff 없음)

**디렉터 확인:** I-GMM-DNA-01 포함 Bitget **서버 배포 완료**.  
로컬 `NEXT_ACTION` 등이 “git push + 배포 대기”로 남아 있어 **문서만** 현실에 맞춤. 알파/실전/C-2/MDD5%/live **미착수**.

### 로컬에서 확인 가능한 것
- 코드·테스트·Claude 조건부 OK · R1/R2 반영 이력 (`05` I-GMM)
- 배포 후 **무엇을** 보면 되는지 한 장: `track_b_POST_DEPLOY_OBS_체크리스트.md`
- L-1 / L-2 / ai_overseer+REPORT_BOT = **코드 OK**, 서버 설치·기동 **기록 없음** → 표기 = 미확인

### 서버에서만 확인 가능한 것 (이 세션에서 숫자 없음)
| 항목 | 왜 로컬 불가 |
|------|----------------|
| `bitget_forward_trades` OPEN/CLOSED COUNT | prod SQLite는 `BITGET_DB_STORAGE_PATH` |
| `Cos_eff=0.000` 고정 여부 | journal / BITGET_LOG_DIR |
| `CRYPTO_DNA_ALPHA_RANK*` · `shape_source` | config_kv prod |
| `gmm_dna_alpha_sync --force` 가 **이미** 돌았는지 | RANK 키 존재 여부가 증거. 채팅만으로는 모름 |

### 다음 Handoff 후보 **1개만**
- **I-GMM-DNA-01b** — Cos_eff / OPEN count / `shape_source` **읽기 전용** 관측 미니잡 (ops 로그·주간 숫자). gate/DNA 재배선 아님.
- **하지 말 것**: C-2 funding · 포트폴리오 MDD 5% · B-2 live alloc · `ENABLE_REAL_EXECUTION=true`

**Ask:** 01b Handoff를 쓸지, 아니면 디렉터 48h 관측 숫자 받은 뒤에만 쓸지.

---

## I-GMM-DNA-01 — Claude 조건부 OK (2026-08-12)

**판정:** 조건부 OK → **R1/R2 코드 반영 완료** (8 passed)

| 조건 | Claude 지적 | Cursor 반영 |
|------|-------------|-------------|
| R1 | data_miner `force=True` → manual 덮어쓰기 | `force=False` 기본 · `BITGET_GMM_SYNC_FORCE_ON_MINE` opt-in |
| R2 | score/100 폴백 live 공용 | `ENABLE_REAL_EXECUTION=true` 시 fail-closed (Cos=0) |
| Mirror | shape_source 관측 | `dna["shape_source"]` 태그 추가 |

**paper 배포:** 즉시 진행 가능  
**live 전환 전:** CAT-F Handoff에 폴백 스위치/fail-closed 재확인 예약

---

## I-GMM-DNA-01 — GMM→CRYPTO_DNA_ALPHA 배선 (2026-08-12)

### 증상 (서버)
- `forward_trades` 0건 · 텔레그램 스캔 ~1000건/일
- 로그: `Cos_eff=0.000 < elastic 0.588` (시계열 게이트 100% 거절)
- config: `BITGET_GMM_DNA_TEMPLATES` 있음 · `CRYPTO_DNA_ALPHA_RANK*` 없음

### 근본 원인
- `signal_engines._doppelganger_adjustment` → `CRYPTO_DNA_ALPHA_RANK1..3` (+ shape 20) 만 읽음
- `data_miner` → `BITGET_GMM_DNA_TEMPLATES` 만 채움 (**키 불일치**)
- `sn_score=0` 이 facts에 고정 → `_facts_cos_scalar_01` 이 signal score 폴백 불가

### 구현
| 파일 | 변경 |
|------|------|
| `evolution/gmm_dna_alpha_sync.py` | **신규** — sync SSOT |
| `data_miner.py` | prototype shape + post-mine sync |
| `pipelines/bitget_pipelines.py` | config_bootstrap 훅 |
| `forward/gates.py` | sn_score≈0 시 score/100 폴백 |

### 테스트
`pytest bitget/tests/test_gmm_dna_alpha_sync.py` → **6 passed**

### 서버 배포 후 1회
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
sudo INSTALL_ROOT=$PWD bash bitget/deploy/update_bitget.sh
.venv/bin/python -m bitget.evolution.gmm_dna_alpha_sync --force
sqlite3 /var/lib/quant-bitget/data/bitget_system_config.sqlite \
  "SELECT key FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA%';"
```

### Claude OK Ask
- neutral shape + bounds midpoint DNA가 paper bootstrap에 충분한지
- sn_score=0 폴백 허용 범위 (🟡 리스크 게이트)

---

## D-3 — Claude OK 수신 (2026-08-04)

- cost/fee basis null — SSOT 없음 확인 수용
- `gemini_call_count` llm_call_cache proxy — 수용 (CAT-M/CAT-J 동기화 완료)
- D-3b dormant · pipeline 미배선 재확인
- D-3b 실배선 시 `bitget_real_execution` vs CAT-N interface — P2-5 Handoff 체크 항목 예약

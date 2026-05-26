# Dante 팩토리 — 운영 런북 (1페이지)

**배포:** Docker 없이 **systemd** 만 사용. 생명 주기·로그는 전부 **`journalctl`** 로 통합한다 (cron 워치독 없음).

| 유닛 | 역할 |
|------|------|
| `dante-main.service` | 스캐너·매매 코어 (`main.py`) — `deploy/ubuntu/install.sh` 경로 |
| `dante-factory.service` | 동일 코어 — `deploy_quant_factory.sh` + `deploy/systemd/` 경로 |
| `dante-dashboard.service` / `dante-streamlit.service` | Streamlit 관제 (8501) |
| `dante-async.service` | 비동기 텔레그램 (`async_telegram_daemon.py`, 코어와 분리) |
| `dante-watchdog.timer` | 부팅 **2분** 후 최초 실행, 이후 **5분**마다 `watchdog.py` (oneshot `dante-watchdog.service`) |
| `dante-snapshot.timer` | CQRS 스냅샷 주기 실행 (`deploy_quant_factory.sh` 일괄 설치 시) |
| `dante-backup.timer` | 매일 **03:00**(서버 로컬 시각) `backup_to_cloud.sh` (oneshot `dante-backup.service`) — S3/rsync 등은 `.env` |

**표준 업데이트(휴먼 에러 방지):** 저장소 루트에서 아래 **한 줄**만 실행한다.

```bash
sudo ./update_factory.sh
```

또는 `make update` (동일 동작). `post_update_notify.sh` 는 `bash` 로 호출되므로 `chmod +x` 는 선택 사항이다.

**데이터 영속:** `update_factory.sh` 는 `git pull` 전에 `INSTALL_ROOT/*.sqlite` 및 `.env` 의 `DB_STORAGE_PATH` 디렉터리(코드 루트와 다를 때) 상단 `*.sqlite` 을 `/var/backups/dante-pre-update/<UTC타임스탬프>/` 로 복사한다. 스키마 변경 시 `CREATE TABLE IF NOT EXISTS` 뒤 `sqlite_schema_guard.py` 의 `KNOWN_COLUMN_MIGRATIONS` 에 `(컬럼명, ADD COLUMN …)` 만 추가하면 기존 행을 유지한 채 컬럼이 붙는다. DB·JSON 을 Git 작업 트리 밖에 두려면 `.env` 또는 `system_config.json` 에 `DB_STORAGE_PATH=/var/lib/quant-factory/data` 등 절대 경로를 설정한다 (`factory_data_paths.factory_data_dir`).

**가상환경:** systemd·`update_factory.sh`·`factory.sh` 는 `INSTALL_ROOT/venv` 를 표준으로 한다 (레거시 `.venv` 는 `deploy/dante_venv.sh` 폴백만). `update_factory.sh` 는 DB/JSON 을 삭제하지 않고, graceful stop → `.venv` 잔존 프로세스 SIGTERM → `sqlite_schema_guard` ALTER → `venv` 로 재기동한다.

**재시작 정책:** `Restart=on-failure`, `RestartSec=5`, `StartLimitIntervalSec=300`, `StartLimitBurst=5`. 시크릿: `INSTALL_ROOT/.env` (`EnvironmentFile=-`).

---

## 로그 모니터링

```bash
journalctl -u dante-main -f
```

```bash
journalctl -u dante-streamlit -f
```

```bash
journalctl -u dante-main -u dante-streamlit --since "1 hour ago" -r --no-pager | less
```

**팩토리 전체(코어·대시보드·비동기 텔레그램·워치독) 로그 한 스트림:**

```bash
journalctl -u dante-factory -u dante-dashboard -u dante-async -u dante-watchdog -f
```

**워치독 타이머 상태:**

```bash
systemctl list-timers dante-watchdog.timer dante-snapshot.timer dante-backup.timer --no-pager
```

---

## 전체 재시작 / 중지

```bash
sudo systemctl restart dante-main dante-streamlit
```

```bash
sudo systemctl stop dante-main dante-streamlit
```

```bash
sudo systemctl start dante-main dante-streamlit
```

유닛 설치·갱신:

- `deploy/ubuntu` 만: `sudo INSTALL_ROOT=... ./deploy/ubuntu/install.sh`
- **스냅샷 + 워치독 + 비동기 텔레그램 포함** 일괄 설치/갱신: `sudo INSTALL_ROOT=... ./deploy_quant_factory.sh`
- **코드 pull 후 전부 재기동까지 한 번에:** `sudo ./update_factory.sh` (저장소 루트)

---

## DR 백업 (`backup_to_cloud.sh`)

- **systemd (권장):** `deploy_quant_factory.sh` 가 `dante-backup.timer` + `dante-backup.service` 를 설치한다. 매일 **03:00** 서버 로컬 시각에 oneshot 실행(스케줄은 `deploy/systemd/dante-backup.timer` 에서 변경). `S3_BUCKET` / `BACKUP_MODE` / `DATA_ROOT` 등은 `INSTALL_ROOT/.env` 에 설정.
- **crontab (대안):**  
  `0 3 * * * INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot S3_BUCKET=s3://YOUR-BUCKET/dante-dr/ /home/ubuntu/dante_bots/Dual-Screener-Bot/backup_to_cloud.sh >>/var/log/dante-backup.log 2>&1`
- **포함:** `market_data.sqlite`, `ops_events.sqlite`, (있으면) `ops_health.sqlite`, `message_queue.sqlite`, `system_config.sqlite`, `system_config.json` — SQLite 는 `sqlite3 ".backup"` 후 tar.gz.
- **S3:** `aws s3 cp` (IAM 권한·`aws configure` 필요). **rsync:** `BACKUP_MODE=rsync RSYNC_TARGET=user@host:/path/`. **로컬만:** `BACKUP_MODE=local`.

---

## 복원 (Restore)

1. **서비스 중지** (DB 쓰기 정지):  
   `sudo systemctl stop dante-main dante-streamlit`
2. **백업 아카이브 내려받기** (S3 예):  
   `aws s3 cp s3://YOUR-BUCKET/dante-dr/dante-dr-YYYYMMDDTHHMMSSZ.tar.gz /tmp/restore.tgz`
3. **임시 풀기:**  
   `mkdir -p /tmp/dante-restore && tar -xzf /tmp/restore.tgz -C /tmp/dante-restore`
4. **파일 반영** (`DATA_ROOT` = 보통 `INSTALL_ROOT`):  
   `cp -a /tmp/dante-restore/system_config.json "$DATA_ROOT/"`  
   SQLite 는 기존 파일 이름으로 덮어쓰기 전 백업:  
   `cp -a "$DATA_ROOT/market_data.sqlite" "$DATA_ROOT/market_data.sqlite.bak"`  
   그 다음:  
   `cp -a /tmp/dante-restore/market_data.sqlite "$DATA_ROOT/"`  
   (`ops_events.sqlite`, `ops_health.sqlite`, `message_queue.sqlite`, `system_config.sqlite` 동일)
5. **권한:** `sudo chown -R ubuntu:ubuntu "$DATA_ROOT"` (실행 유저에 맞게 조정)
6. **기동:**  
   `sudo systemctl start dante-main dante-streamlit`

복구 후 `journalctl -u dante-main -b` 로 기동 로그를 확인한다.

---

## 💡 전문가의 역제안 (Counter-Proposal) — 스케일업 타이밍·관제 시너지

**1) “한계 신호” 게이트 (설정값 검증 루프)**  
지금 넣은 `MAX_WORKERS=1`, `TELEGRAM_CONCURRENCY_LIMIT=4`, SQLite PRAGMA는 **OOM 방어 우선**이다. 실전에서 **백테스트 wall-clock**이 SLA를 밑돌고, `ops_events`의 `gauge.snapshot`에 텔레그램 **PENDING 큐 깊이**가 임계(예: 수백 건 이상)로 **지속**되면 “워커·동시성이 아니라 **처리량**이 병목”이라는 신호로 본다. 반대로 큐는 얕은데 CPU가 **장시간 100%**에 가깝고 스캔 주기가 밀리면 **RAM이 아니라 CPU** 병목이다. 이 두 신호를 **분리해 dashboard + journalctl**로 2주 관찰한 뒤, 둘 다 빨간색일 때만 **8GB RAM급 인스턴스 업그레이드**를 검토한다.

**2) `update_factory.sh` 후킹 + 텔레그램 배포 알림 (코어 비수정)**  
`update_factory.sh` 맨 끝에 **성공 시에만** 실행되는 훅 스크립트를 두는 방식이다. 예: `deploy/ubuntu/hooks/post_update_notify.sh`에서 `git rev-parse --short HEAD`와 `is-active` 결과를 한 줄로 붙여 **기존 `enqueue_telegram` / 메시지 큐로 내면**, 매매 로직은 손대지 않고 운영만 알게 된다. 실패 시에는 `exit 1`로 훅을 실패시켜 CI처럼 “빨간 불”을 남길 수 있다.

**3) (선택) `systemd` Path 단위로 `update_factory` 고정**  
`/etc/systemd/system/dante-update.service` + **수동 트리거만** 허용하는 oneshot으로 래핑하면, SSH 접속 없이 `sudo systemctl start dante-update` 한 번으로 동일 절차를 강제할 수 있다(스크립트 내용은 지금의 `update_factory.sh`와 동일).

---

## 리소스 기본값 파일

- `deploy/ubuntu/factory_resource_limits.env.example` — `.env`에 붙여 넣을 `MAX_WORKERS` / `TELEGRAM_CONCURRENCY_LIMIT`
- `deploy/ubuntu/system_config.resource_limits.fragment.json` — `system_config.json`에 병합할 `MAX_WORKERS` (예: `jq -s '.[0] * .[1]' system_config.json system_config.resource_limits.fragment.json > merged.json` 후 원자 교체)

---

## ReportTimekeeper · 딥다이브 Staleness (배포)

**요약:** 딥다이브·최우수 성적표·듀얼트랙은 `market_data.sqlite` **메인 DB만** 읽는다 (`REPORT_DEEP_DIVE_FORCE_MAIN_DB=1` 기본). US `session_anchor`는 **US Last Trading Day (ET 16:00 근사)**.

**배포 절차**

1. 저장소 갱신: `sudo ./update_factory.sh` (또는 `git pull` + 서비스 재기동).
2. `.env` 확인 (선택):
   - `REPORT_DEEP_DIVE_FORCE_MAIN_DB=1` — 메인 DB 강제 (권장, 기본값).
   - `TZ=Asia/Seoul` — 팩토리 cron·Timekeeper KST 일치.
3. 스냅샷 타이머 유지: `dante-snapshot.timer` — CQRS 복제는 유지하되 **리포트는 메인 워터마크 기준**.
4. 검증 (KR 장 마감 후):
   ```bash
   cd $INSTALL_ROOT && ./factory.sh --daily-kr
   ```
   텔레그램 딥다이브 헤더에 `리포트일 KST` · `세션앵커` · `DB청산워터마크` · `읽기 MAIN` · `Staleness GREEN|YELLOW|RED` 표기 확인.
5. 검증 (US, KST 06:45 이후):
   ```bash
   ./factory.sh --daily-us
   ```
   US 리포트의 `세션앵커`가 **직전 US 거래일**(KST 화요일 새벽 → US 월요일)인지 확인.
6. RED 시: `track_daily_positions` 로그 · `forward_trades` 최신 `exit_date` · `journalctl` factory 로그 점검. `system_config`의 `LAST_REPORT_STALENESS_KR` / `_US` JSON 참고.

**모니터링**

- `ops_events.sqlite` — `event=report.staleness`
- `ops_snapshot` — `kr_exit_watermark`, `us_exit_watermark`, `staleness_grade_*` (dante-snapshot 주기 기록)

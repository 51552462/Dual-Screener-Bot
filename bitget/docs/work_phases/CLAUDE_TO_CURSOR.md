# CLAUDE → CURSOR · CAT-L-CUTOVER-01 · W-블록 회신 판정 + X-블록(읽기전용, 잔여 확인)

> **작성**: Claude Pro (Architect) · 2026-10-01
> **입력**: `track_b_CURSOR_TO_CLAUDE.md` 맨 위 「Phase 1 선행 W-블록 회신 (Handoff v2)」 + 「디렉터 추가 회신 19:48 KST」 — Claude 스냅샷(2026-10-01 10:53 UTC 갱신분)
> **CAT**: CAT-L 🟡 (deep) · CAT-A 읽기 참조(락·queue-worker) · 코드 변경 0 · 서버 변경 0 · Critical 해당 없음(단 §2 X0 결과에 따라 즉시 에스컬레이션)
> **이 파일**: `CLAUDE_TO_CURSOR.md` 상단에 **전문** → 커밋·push → **X0(로컬) 먼저** → §8 X-블록
> **시각**: UTC (KST는 괄호)

---

## 0. 결론 (디렉터용)

1. **실행본 = Handoff 원문 다시 증명.** Claude가 v2 원문에서 직접 계산한 해시 `bbf39d61…`(W-블록)·`0e5a8482…`(W3b)가 Cursor 보고와 **일치**. CAT-L 규칙 1–7 반영도 원문과 일치.
2. **9/28 펜스 소실 원인 = 사실상 확정.** 디렉터님이 터미널에서 업데이트 스크립트를 3번 돌렸고(10:48·10:54·11:24 KST), 마지막 회차가 예약작업 설치기를 다시 실행했다. 그때 서버 코드는 펜스가 없는 옛 판이었다. **복구 행동이 잘못된 게 아니라, 펜스 코드가 공식 저장소에 아직 없었던 게 근본 원인**이다. 지금은 FENCE-03 장치가 같은 경로를 막는다. 직접 증거 한 줄(cron 파일 갱신 기록)만 X2로 받는다.
3. **"서버가 터졌다"는 흔적은 로그에 없다.** 09-26 17:52 KST 이후 서버 재시작 0, 확인한 구간의 메모리 부족(OOM) 0. 디렉터님 기억의 "stop 후 start"는 로그상 마지막 재시작인 **09-26 오후**일 가능성이 크다. 무엇을 보고 "터졌다"고 느끼셨는지 한 줄이 남은 단서다.
4. **자동 백업은 진짜 고장**: 스크립트가 `python`이라는 명령을 찾지 못해 최소 09-23부터 매일 실패. → **`CAT-L-BACKUP-01`을 Phase 1보다 먼저** 한다(§4).
5. **새로 발견**: 대기열 작업자(queue-worker)가 **매일 00:44–00:46 KST에 watchdog에 의해 강제 재시작**된다. 매일 00:07 KST에 넣는 선물 스캔이 그 재시작으로 중간에 끊기고 있을 수 있다 → 조사 항목 등록(§5).
6. 오늘 접속 실패: 같은 시간대에 Cursor는 `3.36.90.195`로 정상 접속·실행했다 → **서버는 살아 있었다.** 접속 주소·키 쪽 문제일 가능성(§10 쉬운요약 팁).

---

## 1. Claude 독립 검증

| 항목 | 방법 | 결과 |
|---|---|---|
| §8-1 W-블록 실행본 | v2 원문 코드펜스 추출 재계산 | 38줄 · `bbf39d61001cb033839851730cee17b9cfc8f6a8659a26a98283a0b79e0433e5` **일치** |
| §8-1b W3b 실행본 | 동일 | 10줄 · `0e5a8482181f70d2205625573f2c622b9ae6715c37c3d5d8f55395f97100023c` **일치** |
| CAT-L 규칙 1–7 + 실행 수단 표준 + pull 레시피 | 스냅샷 `CAT-L_인프라배포.md` L105–138 대조 | Handoff 문안과 **일치** |
| NEXT_ACTION · 05 | 스냅샷 대조 | 신규 행 2개·D2 정정 줄 반영 확인 |
| W-블록 출력 전문(`snapshots/…P1PRE_WBLOCK…md`) | — | **내 스냅샷에 없음** → OUTBOX 인용 줄만으로 판정(한계) |

---

## 2. 항목별 판정

| 항목 | 판정 | 메모 |
|---|---|---|
| **비밀 문자열 1줄** | ❌ **불수용 → X0 즉시** | "어느 줄인지 특정 못 함"은 받을 수 없다. 해당 md는 **이미 커밋·push됨**(`4ee0029`). 로컬 `grep -n` 한 번이면 특정된다. 비밀값이면 키 교체 + git 이력 정리 = **Critical, 디렉터 즉시 보고** |
| W1 | ✅ | 74건·가시성 OK. 분류 수용. 09-26 05:26:50 `restart dante-bitget-async`(접속 세션 없음, 이전 부팅) → 해당 줄의 `TTY`/`PWD` 회신 — watchdog 자동 복구면 queue-worker와 같은 패턴(§5) |
| W2 | ✅ | 핵심 확인. 추가 관찰: 01:48·01:54 `update_bitget.sh`는 **HEAD를 못 움직였고**, 01:57:45 수동 `pull`이 움직임 → 앞의 두 회차가 pull 단계 전후에서 실패했을 가능성(원인은 SCRIPT-AUDIT) |
| W3 | ⚠️ 부분 | 02:24:10 이후가 `head -200`에 잘림(내 설계 결함, §6 C-9) → X2 |
| W3b | ✅ | 커널 OOM 0 · 상주 유닛 크래시 0. 09-27 15:46 queue-worker `KILL`은 매일 반복되는 watchdog 재시작의 일부(§5). `swapfile.swap Duplicate entry in /etc/fstab` 2줄은 FENCE-02 `daemon-reload` 시각에 나온 무해 경고(스왑 줄 중복) — 비차단 기록 |
| W4 | 판정 유보 → 대체 | 0바이트 = "아무것도 쓰지 못하고 종료". 락 대기와 부합하지만, 파이썬은 파일로 내보내는 출력을 버퍼에 모았다가 쓰므로 **SIGTERM으로 죽으면 출력이 유실될 수 있다** → 대기/실행 단정 불가. 내 조회식은 시간대 오류(§6 C-8). **124의 정밀 원인은 더 파지 않는다** — Phase 1에 필요한 건 "락이 언제 비는가"이므로 X3(락 점유 시각표)로 대체 |
| W5 | ✅ 원인 / ⚠️ 시작 시점 | backup 원인 확정(`line 32: python: command not found`, `User=root`). `SQLITE3_CLI=yes`라 내 가설(sqlite3 부재) 기각. "언제부터"는 `tail -20` 한계(§6 C-9) → X4 |
| W6 | ✅ 설명됨 | 8건 = 09-28 02:24 `deploy_bitget_factory.sh`의 `sudo chmod +x`. 나머지 2건은 ubuntu가 자기 파일에 `chmod`하면 sudo 기록이 안 남으므로 **의심 사유 아님**. 근본: 저장소 모드 `100644` vs 서버 `+x` → 영구 dirty(그리고 `update_bitget.sh`의 `git restore .`가 되돌리고 배포가 다시 `+x` 하는 순환). 처방(별도): 저장소에 실행 모드를 커밋. `uninstall_stock_north_star_cron.sh`는 주식 쪽 파일 — **범위 밖, 손대지 않음** |
| W7 | ✅ Cursor 판정 수용 | 내 시각 가설 틀림(§6 C-6 갱신). 메커니즘은 "root로 도는 파이썬이 저장소 모듈을 import": 09-23 10:11 1건은 pull(10:10:53) 직후 = `update_bitget.sh`의 root 사전 백업 파이썬과 같은 패턴. 조치 불필요 |
| W8 | ✅ | sha256 3건 일치. 매칭 줄은 키 *이름*·PASS 메시지 → 비밀 아님 |

---

## 3. 09-28 사건 종합 판정

| UTC (KST) | 사건 | 근거 |
|---|---|---|
| 09-27 14:20:57 | FENCE-02 wrapper 수동 적용(라이브만, 생성기는 Cursor 로컬) | W1 · OUTBOX |
| 09-28 01:47:02 | `scan_spot_nulrim`가 heavy slice 안에서 시작 | W3 |
| 01:48:52 (10:48) | `update_bitget.sh` #1 — `git restore .` · `pull --ff-only` · HEAD 안 움직임 | W1 · W2 |
| 01:54:10 (10:54) | `update_bitget.sh` #2 — HEAD 안 움직임 | W1 · W2 |
| 01:57:45 (10:57) | 수동 `pull` → `1e38166`(origin에 펜스 생성기 없음) | W2 |
| 02:24:09 (11:24) | `update_bitget.sh` #3 → 02:24:11 `deploy_bitget_factory.sh`(유닛 파일 10개 덮어씀 · `chmod +x` 18 · enable/**disable dashboard·heatmap** · `reset-failed`) → **02:24:22 `install_bitget_cron.sh`(root)** | W1 |
| 09-29 08:01 | 라이브 wrapper 0줄 발견 | 05 |

**판정**
- **FENCE-02 회귀 원인 = 09-28 02:24:22 설치기 재실행(구 생성기)** — 현재 등급 "강하게 시사". X2(cron 파일 갱신 기록 · 사전 백업 안의 cron 사본)로 **"확정"**으로 올린다.
- **근본 원인(06 실패기록용 문구)**: "라이브 수동 변경(Step 2)이 SSOT(origin 생성기)에 없던 상태에서, 정상 배포 도구가 SSOT대로 되돌렸다." 재발 방지 = FENCE-03(라이브 ≠ 생성기면 `update_bitget.sh` [0/7]에서 중단) — **이미 적용됨.**
- **신원·의도 = 해소.** 디렉터 진술(서버 장애 대응)과 TTY=`pts/0`(사람이 터미널에서 직접)이 일치. 추가 추궁 불필요.
- **디렉터 기억 정합**: "stop 후 start, 그때마다 IP가 바뀜" ↔ 로그상 마지막 VM 재시작은 **09-26 08:49–08:52 UTC(17:49–17:52 KST, `last -x` 기록)**. 09-28은 VM 재시작이 아니라 업데이트 스크립트 실행. 두 날의 기억이 합쳐진 것으로 보는 게 자연스럽다. 접속 기록의 IP는 디렉터 쪽 네트워크라 서버 IP와 별개라는 Cursor 설명이 맞다.

**부수 영향 3건** (이번 회신으로 새로 보임)
1. **dashboard·heatmap 유닛이 09-28부터 disable 상태** — 의도된 퇴역인지, 배포 스크립트의 부작용인지 미상(L5). 디렉터가 이 화면을 쓰고 있었다면 "서버가 안 된다"는 체감의 후보.
2. **라이브 유닛 파일 10개 = `1e38166` 시점 템플릿.** 이후 저장소 템플릿이 바뀌었다면 라이브가 저장소와 어긋나 있다(L6).
3. **`update_bitget.sh`의 `git restore .`** — 서버에서 직접 고친 tracked 파일을 경고 없이 버린다(01:48 실증, 무엇이 버려졌는지 미상). → CAT-L 규칙 4에 주의 1줄(§11) + 설계 개선은 SCRIPT-AUDIT.

---

## 4. backup · snapshot 판정

**`CAT-L-BACKUP-01` → Phase 1 선행으로 승격.**
- 근거: (a) cutover는 프로젝트 문서가 "리스크 높음 — 이중 실행·텔레그램 폭주"로 적은 작업 (b) 무결성 백업이 최소 9일(아마 그 이상) 미작동 (c) 원인이 분명하고 수정 규모가 작다.
- 단 **"1줄 고치고 끝"으로 하지 않는다.** 고치는 순간 **처음으로 실제 도는 백업**이 4GB 서버에서 root로, 메모리 펜스 밖에서, 00:30 UTC(09:30 KST)에 돈다. 이 서버는 OOM 이력이 3회다. → 첫 실행 자원 계획(DB 크기 · 예상 소요 · 메모리 상한 · 시간대 · 수동 1회 관찰)이 Handoff에 들어가야 한다. X4로 입력을 모으고, **BACKUP-01 Handoff는 다음 창(그 sub-phase 단독)**에서 발행.
- Critical 사전 판단: 데이터 로직(CAT-B) 변경 아님 · 운영 자원 위험 → 🟡. 확정은 BACKUP-01 Handoff에서.

**snapshot — 비차단 후보, X4·L7로 판정**
- 실패의 대부분은 "긴 스캔이 쓰기 중이라 이번 회차는 양보" → exit 1. **의도된 양보를 실패 코드로 내는 설계**라 `systemctl --failed`가 오염되고, 이번처럼 "반복 고장"으로 읽힌다.
- Cursor 의견 4(exit 0) **수정 채택**: exit 0이면 양보 횟수가 사라진다. **전용 코드(예: 75) + 유닛 `SuccessExitStatus=75`** 권고 — 진짜 실패(1)와 양보를 구분하면서 기록은 남는다. 코드 변경이므로 별도 sub-phase.
- 비차단 여부: 성공 사이 **최대 공백**(X4)과 이 DB를 **누가 읽는지**(L7). 스캔 입력이면 CAT-B 영향 → Phase 1 전 처리.

---

## 5. 신규 등록 — queue-worker 매일 강제 재시작 (`CAT-L-QW-RESTART-01`, 조사)

| 근거 | 출처 |
|---|---|
| `sudo systemctl restart dante-bitget-queue-worker`가 **매일 15:44–15:46 UTC(00:44–00:46 KST)**, TTY 없음 → watchdog 자동 복구(`watchdog.py:280`) | W1 (7일 연속) |
| 09-27 15:46:24 queue-worker `status=9/KILL` → `Failed with result 'timeout'`(정지 대기 시간 초과로 강제 종료) | W3b |
| 매일 15:07 UTC `--enqueue --scan-futures-ema5-r2`(L-3b canary)가 queue-worker에서 실행 | FENCE-02 Phase 0 기록 |
| 09-14 15:22 UTC queue-worker `status=143` 재시작 | FENCE-02 Phase 0 기록 |

- **가설**: 15:07에 넣은 긴 스캔이 도는 동안 워커 하트비트가 "멈춤"으로 판정 → 약 37–39분 뒤 watchdog이 강제 재시작 → **canary 선물 스캔이 매일 중간에 끊길 수 있다.** 데이터 공백 문제일 수 있어 무겁게 본다.
- Phase 1과의 관계: 비차단. 다만 48h 창에 2회 발생 예정 → Phase 1 설계에서 "예정된 재시작"으로 관측 노이즈에서 분리.
- 인접: CAT-A(대기열·오케스트레이션) · CAT-L(watchdog P1-7). 조사는 다음 창, 읽기전용부터.

---

## 6. Claude 측 정정

| # | 정정 |
|---|---|
| C-6 (갱신) | root pyc 시각 가설(09-29·30 설치기) **틀림**. 메커니즘 범주(root 파이썬의 import)만 유효 |
| C-8 | W4 조회식이 로그 이름의 시간대 혼재(cron 실행 = UTC 스탬프, systemd·수동 실행 = KST 스탬프)를 몰라 **무효** |
| C-9 | W3 `head -200`·W5 `tail -20` — 기간 전체를 못 덮는 출력 상한을 내가 넣어 02:24 이후 잘림·시작 시점 미상. 앞으로는 **"처음·끝 + 총개수"** 형태로 설계 |

---

## 7. Cursor 의견 1–5 처리

| # | 처리 |
|---|---|
| 1 W4 mtime 기준 재조회 | **채택** → X3(시각표 전체) · X3b(09-30 06:30–06:56 창 + `watchdog_20260930_155209` 열람) |
| 2 backup 별도 sub-phase | **채택 + 강화**: Phase 1 선행, 첫 실행 자원 계획 포함(§4) |
| 3 W3 재조회 | **채택** → X2 + cron 갱신 기록·사전 백업 사본으로 직접 증거 |
| 4 snapshot exit 0 | **수정 채택** → 전용 코드 + `SuccessExitStatus`(§4) |
| 5 root pyc 조치 불필요 | **동의** |
| (Cursor 판단) 근거 없는 Done 미처리 · `vb_run.py` 표준화 · 미대조를 추측으로 채우지 않음 | 모두 맞다. 유지 |

---

## 8. X0(로컬, 최우선) · X-블록(서버, 읽기전용) · L-항목(로컬)

### 8-0. X0 — 비밀 문자열 1줄 특정 (서버 아님, **다른 것보다 먼저**)

```text
grep -n -i -E 'KEY|SECRET|TOKEN|PASSWORD|PASSWD|API' bitget/docs/work_phases/snapshots/CAT-L-CUTOVER-01_P1PRE_WBLOCK_20261001.md
```
- 결과 줄을 **줄 번호와 함께**, 값으로 보이는 부분만 `***`로 가려 OUTBOX에.
- 비밀값이면 **즉시 중단** → 디렉터 보고(키 교체 + git 이력 정리, Critical). X-블록 실행 보류.

### 8-1. X-블록 실행 규칙

- 실행 수단 표준(규칙 · `vb_run.py`). 추출 줄 수·CR·sha256 공개. `set -e` 없음 · `sudo` 0.
- 출력 전문은 `snapshots/CAT-L-CUTOVER-01_P1PRE_XBLOCK_20261001.md`, OUTBOX엔 경로 + sha256 + 판정에 쓴 줄 원문. **커밋 전 X0과 같은 grep으로 자기 점검.**
- 권한 거부(`Permission denied`)가 난 항목은 "거부"로 보고 — `sudo`로 우회 금지.

### 8-2. X-블록

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}" || exit 1
echo "== X-BLOCK START $(date -u +%Y-%m-%dT%H:%M:%SZ) user=$(id -un) =="

echo "== X1 boots / kernel OOM since current boot =="
journalctl --list-boots --no-pager 2>&1 | tail -5
uptime -s
K="$(TZ=UTC journalctl --utc -k -b 0 --no-pager 2>&1)"
echo "KLINES_BOOT0=$(printf '%s\n' "$K" | wc -l)"; printf '%s\n' "$K" | head -1
echo "OOM_HITS_BOOT0=$(printf '%s\n' "$K" | grep -c -i -E 'out of memory|oom-kill')"
printf '%s\n' "$K" | grep -i -E 'out of memory|oom-kill|killed process|blocked for more than' | head -20

echo "== X2 cron.d reload timeline since 2026-09-26 UTC =="
TZ=UTC journalctl --utc -t cron -t CRON --since '2026-09-26 00:00:00' --no-pager 2>&1 | grep -E 'RELOAD|dual-screener-bitget' | head -60
echo "== X2b 2026-09-28 02:24-02:30 UTC systemd/sudo =="
TZ=UTC journalctl --utc --since '2026-09-28 02:24:00' --until '2026-09-28 02:30:00' --no-pager -t systemd -t sudo 2>&1 | grep -v -E 'Started Session|session-[0-9]+\.scope|run-[a-z0-9]+\.scope|user@[0-9]+\.service' | head -150
echo "X2b_TOTAL_LINES=$(TZ=UTC journalctl --utc --since '2026-09-28 02:24:00' --until '2026-09-28 02:30:00' --no-pager -t systemd -t sudo 2>/dev/null | wc -l)"
echo "== X2c pre-update backups 2026-09-28 =="
ls -la --time-style=full-iso /var/backups/bitget-pre-update/ 2>&1 | grep -E '20260928|total|denied' | head -10
for d in /var/backups/bitget-pre-update/20260928_*; do echo "-- $d"; ls -la "$d" 2>&1 | head -20; for f in "$d"/*cron* "$d"/*/*cron*; do [ -f "$f" ] && echo "CRONCOPY $f WRAP_NONCOMMENT=$(grep -v '^[[:space:]]*#' "$f" | grep -c systemd-run)"; done; done

echo "== X3 bitget logs by mtime since 2026-09-29 00:00 UTC (lock occupancy input) =="
TZ=UTC find /var/lib/quant-bitget/logs -maxdepth 1 -name 'bitget_*.log' -newermt '2026-09-29 00:00:00' -printf '%TY-%Tm-%TdT%TT %s %f\n' 2>/dev/null | sort | head -800
echo "X3_COUNT=$(TZ=UTC find /var/lib/quant-bitget/logs -maxdepth 1 -name 'bitget_*.log' -newermt '2026-09-29 00:00:00' 2>/dev/null | wc -l)"
echo "== X3b 2026-09-30 06:30-06:56 UTC window =="
TZ=UTC find /var/lib/quant-bitget/logs -maxdepth 1 -name 'bitget_*.log' -newermt '2026-09-30 06:30:00' ! -newermt '2026-09-30 06:56:00' -printf '%TY-%Tm-%TdT%TT %s %f\n' 2>/dev/null | sort
echo "-- bitget_watchdog_20260930_155209.log --"
head -40 /var/lib/quant-bitget/logs/bitget_watchdog_20260930_155209.log 2>&1

echo "== X4 backup history / targets / sizes =="
B="$(TZ=UTC journalctl --utc -u dante-bitget-backup.service --no-pager 2>&1)"
echo "BACKUP_FIRST_ENTRY: $(printf '%s\n' "$B" | head -1)"
echo "BACKUP_SUCCESS_COUNT=$(printf '%s\n' "$B" | grep -c -E 'Deactivated successfully|Succeeded')"
echo "BACKUP_FAIL_COUNT=$(printf '%s\n' "$B" | grep -c 'Failed with result')"
printf '%s\n' "$B" | grep -E 'Deactivated successfully|Succeeded' | tail -2
printf '%s\n' "$B" | grep -E 'Failed with result' | head -1
BS="$(find bitget -name backup_bitget_db.sh 2>/dev/null | head -1)"; echo "BACKUP_SCRIPT=$BS"
[ -n "$BS" ] && grep -n -E 'BACKUP|DEST|/var/backups|python|sqlite' "$BS" | head -30
ls -la --time-style=full-iso /var/backups/ 2>&1 | head -30
ls -la --time-style=full-iso /var/lib/quant-bitget/data/ 2>&1 | grep -E '\.sqlite$|total' | head -40
du -sh /var/lib/quant-bitget/data 2>&1
df -h / 2>&1
free -m 2>&1
echo "== X4b snapshot success gaps since 2026-09-24 UTC =="
S="$(TZ=UTC journalctl --utc -u dante-bitget-snapshot.service --since '2026-09-24 00:00:00' --no-pager -o short-unix 2>/dev/null | grep 'Deactivated successfully' | cut -d' ' -f1 | cut -d. -f1)"
echo "SNAP_SUCCESS_SINCE_0924=$(printf '%s\n' "$S" | grep -c .)"
printf '%s\n' "$S" | awk 'NR>1{g=$1-p; if(g>m){m=g; a=p}} {p=$1} END{print "SNAP_MAX_GAP_SEC=" m " GAP_START_UNIX=" a}'

echo "== X-BLOCK END $(date -u +%Y-%m-%dT%H:%M:%SZ) =="
```

### 8-3. 기대값 / 볼 것

| 항목 | 기대 / 볼 것 | 주의 |
|---|---|---|
| X1 | 현재 부팅 시작 ≈ 2026-09-26 08:52 UTC · `KLINES_BOOT0 ≥ 1` · `OOM_HITS_BOOT0=0`이면 **09-26 이후 OOM·재부팅 없음 확정** | 0을 해석하기 전 가시성 확인 |
| X2 | `dual-screener-bitget` RELOAD가 **09-27 14:21 무렵 · 09-28 02:24 무렵 · 09-29 10:27–10:28 · 09-30 06:00 무렵**에 찍히는지. 09-28 02:24 RELOAD = 설치기가 cron 파일을 실제로 바꾼 직접 증거 | 시각만 대조, 해석은 Claude |
| X2b | 02:24 이후 `Stopping/Started`·`Reloading` 순서, `X2b_TOTAL_LINES`(잘림 여부 판단용) | |
| X2c | 사전 백업 안에 cron 사본이 있으면 `WRAP_NONCOMMENT=28`(02:24 직전 펜스 존재 증거). 권한 거부·사본 없음도 그대로 보고 | 주석 제외 개수만 근거(교훈 3) |
| X3 | 로그 목록(로컬에서 "파일명 시작시각(UTC/KST 구분) ~ mtime" → 점유 구간표로 가공). **Phase 1 실행 가능 창(락 비는 시간대) 후보 3개** 제안 | 이름 스탬프 시간대 혼재 주의(C-8) |
| X3b | 06:30–06:56 창에 끝난 잡 = 09-30 06:49 락 보유 후보. watchdog 로그 내용 | 열람만 |
| X4 | 백업 **마지막 성공 시각(없으면 "기록 범위 내 성공 0")** · 처음 보이는 기록 시각(journal 보존 한계일 수 있음) · 백업 대상·저장 위치 · DB 크기 합계 · 디스크 여유 · 메모리 | 백업 수동 실행 금지 |
| X4b | `SNAP_MAX_GAP_SEC`(스냅샷 최장 공백, 초) | |

### 8-4. L-항목 (로컬 코드·git만)

| # | 내용 |
|---|---|
| L5 | `deploy_bitget_factory.sh`가 dashboard·heatmap을 `disable` + `reset-failed` 하는 줄과 그 이유(주석·커밋 메시지). 의도된 퇴역인지 1줄 결론 |
| L6 | `git diff --stat 1e38166 8a6da21 -- bitget/deploy/systemd/ bitget/deploy/deploy_bitget_factory.sh` → 라이브 유닛(09-28 설치)이 현재 저장소와 어긋나는지 |
| L7 | `bitget_market_data_snapshot.sqlite`를 **읽는** 모듈 목록(파일:줄) — 스캔 입력인지, 대시보드·리포트용인지 |
| L8 | `update_bitget.sh`의 `git restore .` 줄과 조건, 버리기 전에 diff를 어디 남기는지(없으면 "없음"). git을 어느 사용자로 실행하는지 |
| L9 | W1의 09-26 05:26:50 `restart dante-bitget-async` 줄의 `TTY`/`PWD` 원문 · `watchdog.py`의 자동 재시작 대상 목록 |

---

## 9. Phase 1 착수 조건 (갱신)

| # | 조건 | 상태 |
|---|---|---|
| 1 | Phase 0c SUB_DONE | ✅ |
| 2 | 락 점유 시각표 → Phase 1 실행 창 · SKIPPED_LOCK 처리 · `timeout` 금지 · 완료 확인(`parallel_run_state.json`·`started_at_utc`) | X3 |
| 3 | 09-28: 명령 ✅ 확정 · 원인 직접 증거 X2 · 장애 원인 = 로그 근거 없음 → X1 + 디렉터 증상 1줄로 재발성 판정 | X1·X2 + 디렉터 |
| 4 | **`CAT-L-BACKUP-01` 완료**(승격) · snapshot은 X4b·L7로 비차단 여부 판정 | 다음 창 |
| 5 | X0 비밀 문자열 해소 | 최우선 |
| 6 | queue-worker 일일 재시작 → Phase 1 설계에 "예정된 재시작"으로 반영(조사 결과는 비차단) | 등록 |

**작업 순서(제안)**: X0 → X-블록 + L5–L9 → Claude 판정 → `CAT-L-BACKUP-01`(다음 창) → Phase 1 Handoff. 큐 원칙 유지: 11번 대형 백필은 8번(cutover) 종결 후.

---

## 10. 출력 형식 체크

- **SSOT 변경**: `CAT-L_인프라배포.md` 규칙 4 주의 1줄(§11) · 05 · NEXT_ACTION · 06 실패기록(X2 확정 후) · 09 · NEXT_STEP · 신규 등록 `CAT-L-QW-RESTART-01`
- **SSOT 비변경**: 코드 0 · 서버 0(X-블록 읽기전용) · 게이트·NAV·CAT-F/G/I/N/B/D 0 · cron·slice·유닛·`.env`·`BITGET_PIPELINE_SSOT` 0 · `ENABLE_REAL_EXECUTION` OFF
- **SPOT/FUT**: 해당 없음(운영 절차). 단 §5의 canary는 FUT 스캔 — 조사 시 FUT 분기로 다룸
- **인접 CAT**: CAT-A(락·queue-worker) · CAT-B(snapshot 소비자, L7 결과에 따라) · CAT-F(A5 — 범위 밖)
- **롤백**: 문서 커밋 revert. 서버 변경 없음
- **Critical**: 현재 없음. **X0이 비밀값이면 즉시 Critical**

---

## 11. 문서 갱신 (붙여넣을 문구)

**`05_진행로그.md` 최상단**
```markdown
## CAT-L-CUTOVER-01 Phase 1 선행 W-블록 회신 · Claude 판정 [2026-10-01]
실행본=Handoff v2 원문(Claude 재계산: W bbf39d61… · W3b 0e5a8482… 일치). CAT-L 규칙 1–7 원문 일치.
09-28: 디렉터가 터미널에서 update_bitget.sh 3회(01:48·01:54·02:24 UTC). 02:24 → deploy_bitget_factory → install_bitget_cron(root, HEAD 1e38166 구 생성기) = FENCE-02 회귀 원인(강하게 시사, X2로 확정 예정). 근본 원인 = 라이브 수동 변경이 SSOT에 없던 상태. FENCE-03이 재발 차단.
로그상 09-26 08:52 UTC 이후 VM 재시작 0 · 확인 구간 OOM 0 → "서버 터짐" 근거 없음(디렉터 증상 1줄 대기).
backup = python 미존재로 최소 09-23부터 매일 실패 → CAT-L-BACKUP-01을 Phase 1 선행으로 승격. snapshot = 의도된 양보가 exit 1(비차단 후보).
신규 등록: CAT-L-QW-RESTART-01(queue-worker 매일 15:44–15:46 UTC watchdog 재시작, canary 스캔 중단 가능성).
비밀 문자열 1줄 미특정 → X0 즉시. Claude 정정 C-6 갱신·C-8·C-9.
Phase 1 보류.
```

**`NEXT_ACTION.md`**
- CUTOVER 행: `Phase 0c SUB_DONE · Phase 1 보류 · X0 → X-블록(읽기전용) 대기 · 선행: CAT-L-BACKUP-01`
- BACKUP-01 행: `등록 · **Phase 1 선행** · 원인 확정(python 미존재) · Handoff는 X4 회신 후 다음 창`
- 신규: `CAT-L-QW-RESTART-01 | 등록 · 조사(읽기전용부터) · Phase 1 비차단`

**`CAT-L_인프라배포.md` 규칙 4 아래 주의 1줄**
```markdown
   - 주의(2026-10-01): `update_bitget.sh`는 서버 작업트리의 tracked 변경을 `git restore .`로 경고 없이 버린다(2026-09-28 실증). 서버에서 직접 고친 파일이 있으면 먼저 저장소로 옮긴 뒤 실행.
```

**`06_검증체크리스트_및_실패기록.md`** — **X2로 확정된 뒤에만** 추가:
```markdown
- CAT-L-FENCE-02 회귀(2026-09-28 02:24 UTC): 장애 대응 중 update_bitget.sh → install_bitget_cron.sh(root)가 origin 구 생성기로 cron 재생성 → 수동 적용 wrapper 소실. 근본 원인: 라이브 수동 변경이 SSOT에 없었음. 재발 방지: CAT-L-FENCE-03(마커 + pull 전 가드).
```

**`09_디렉터_쉬운요약.md`**
```markdown
✅ Cursor가 서버에서 돌린 확인이 Claude가 쓴 내용과 정확히 같은 것으로 다시 증명됐어요.
🔍 9/28 안전장치가 사라진 이유를 거의 찾았어요. 디렉터님이 서버를 살리려고 업데이트를 돌렸을 때, 서버에 있던 코드가 안전장치가 없는 옛날 판이었어요. 디렉터님 잘못이 아니라, 안전장치 코드가 아직 공식 저장소에 안 올라가 있었던 게 원인이에요. 지금은 이런 일이 생기면 자동으로 멈추는 장치가 있어요.
🩺 기록상 9/26 오후 이후 서버가 꺼졌다 켜지거나 메모리가 터진 흔적은 없어요. 그래서 "터졌다"고 느끼신 순간 무엇을 보셨는지가 중요해요.
🔴 자동 백업은 진짜로 매일 실패하고 있었어요(필요한 프로그램 이름을 못 찾음). 48시간 관찰보다 백업 고치기를 먼저 해요.
💡 팁: ① 서버가 이상할 때 '업데이트 스크립트'는 고치는 도구가 아니라 새 코드를 까는 도구예요. 장애 때는 Lightsail에서 재시작만 하고 알려 주세요. ② 서버 주소는 지금 3.36.90.195예요. Lightsail에서 '고정 IP'를 붙여 두면 껐다 켜도 주소가 안 바뀌어요(인스턴스에 붙어 있는 동안 무료).
```

**`NEXT_STEP`**
```markdown
다음 할 일: Cursor가 ① 비밀 문자열 1줄 확인(X0, 컴퓨터 안에서만) ② 서버 "보기만" 확인(X-블록) ③ 코드 읽기 5가지(L5~L9). 그다음 백업 고치기(BACKUP-01) → 48시간 관찰(Phase 1).
디렉터: 질문 1개 — "서버가 터졌다 / 접속이 안 된다"고 느끼신 순간 화면에 보인 것 한 줄.
```

---

## 금지 (이번)

X-블록 외 서버 명령 일체 · `sudo` · 백업/스냅샷 수동 실행 · 백업 스크립트 수정 · dashboard·heatmap enable · 유닛 재설치 · 서버에서 `chmod`/`git update-index`/`git restore` · pyc 삭제 · `--cutover-check`·`--start-parallel` · `.env` 수정 · LANE_FULLBT 파일 · 주식 쪽 파일 · `BITGET_PIPELINE_SSOT` · C-2 · MDD5% · live · `ENABLE_REAL_EXECUTION`

## 완료 정의

X0 결과(먼저) + 이 Handoff 커밋 해시 + X-블록 추출 sha256 + 출력(스냅샷 경로·해시·판정 줄 원문) + L5–L9 + §11 문서 갱신 → `track_b_CURSOR_TO_CLAUDE.md` 상단 OUTBOX. Claude 판정 전 Phase 1·BACKUP-01 착수 금지.

## sub-phase ID

`CAT-L-CUTOVER-01` (Phase 1 선행 점검 — 잔여)

---

# ARCHIVE (이전 Handoff · P0c확정/P1선행 W-블록)

# CLAUDE → CURSOR · CAT-L-CUTOVER-01 · Phase 0c 판정(SUB_DONE) + Phase 1 선행 W-블록(읽기전용)

> **작성**: Claude Pro (Architect) · 2026-10-01
> **입력**: `track_b_CURSOR_TO_CLAUDE.md` 맨 위 「디렉터 회신 D1–D4」 + 「Phase 0c 확정 심사 (V-블록 + L1–L4)」 — Claude 프로젝트 스냅샷(2026-10-01 09:53 UTC 갱신분)에서 원문 확인
> **CAT**: CAT-L 🟡 (deep) · CAT-A 읽기 참조(runtime lock) · 코드 변경 0 · 서버 변경 0 · Critical 해당 없음
> **이 파일**: `CLAUDE_TO_CURSOR.md` 상단에 **전문**. 커밋·push 후 해시를 OUTBOX에 적고 §8 W-블록 실행
> **시각**: 전부 UTC (KST는 괄호)
> **v2 (2026-10-01)**: 디렉터 D2 정정("09-28 오전은 서버가 터져서 내가 직접 했을 것") 반영 — §0·§2·§4-1·§6·§7·§8(8-1b 신규)·§9·§11 갱신. v1을 이미 커밋했다면 이 판으로 교체 커밋. v1 §8-1을 이미 실행했다면 §8-1b만 추가 실행.

---

## 0. 결론 (디렉터용)

1. **Phase 0c = SUB_DONE (Claude OK).** 서버 코드 이동이 안전했는지에 대한 확인이 전부 기대값과 일치했다 — 서버 HEAD `8a6da21`, cron 안전장치 PRISTINE·28줄, 상주 서비스가 쓰는 코드 변경 0, 재실행 흔적 0, 체크 함수는 순수 읽기.
2. 실행 출처 증명: Claude가 Handoff 원문의 V-블록 sha256을 직접 다시 계산했고 Cursor 보고값(`9879eb2b…`)과 **일치**. 이번엔 "실행된 것 = Handoff 원문"이 해시로 증명됨.
3. 대신 **새로 드러난 것 2건이 Phase 1을 막는다**: ① **09-28 02:24(11:24 KST) 서비스 재시작** — 디렉터 정정: 서버가 터져서 디렉터가 직접 복구한 것. 남은 확인은 *그때 실행된 명령*(FENCE-02 펜스 소실 원인일 가능성)과 *서버가 터진 원인*(펜스가 살아 있던 시간대라 FENCE-02 효과검증의 직접 증거) ② **백업(exit 127)·스냅샷(exit 1) 서비스가 반복 실패 중.** 둘 다 읽기전용으로 먼저 원인부터 본다(§8).
4. Phase 1: **보류 유지.** 조건은 §9.

---

## 1. Claude 독립 검증 (보고를 믿지 않고 직접 대조한 것)

| 항목 | 방법 | 결과 |
|---|---|---|
| V-블록 실행본 = Handoff 원문 | Claude가 자기 Handoff 원문의 §4-1 코드펜스를 추출해 재계산 | 38줄 + 끝 개행 → sha256 **`9879eb2b07e7496c94ce92f188932b3fed46230f826b462a771277ba6c5393e1`** = Cursor 보고값 **일치** |
| V5 79건 | 날짜별 재집계 | 09-26: 23 · 09-27: 21 · 09-28: 4+5 · 09-29: 17 · 09-30: 8 · 10-01: 1 = **79 일치** |
| V6 유닛 대응 | `systemctl show`는 인자 순서가 아니라 자기 순서로 출력 → 빈 줄 구간 단위로 `Id` 재대응 | Cursor 표 대응 **정확** (backup=127, snapshot=1, watchdog=inactive/success) |
| V3 마커 | 스크립트 echo 문장 순서 대비 | 3파일 모두 1:1. p0.out 489줄 = 스냅샷 md 489줄 |
| 스냅샷 한계 | — | 내 스냅샷의 `CLAUDE_TO_CURSOR.md`·`CAT-L_인프라배포.md`는 아직 09-30 판 → `aca1508` 내용과 CAT-L 규칙 반영은 **스냅샷으로 대조 불가**. 핵심(V-블록 바이트)은 해시 일치로 갈음. §8-3 W9로 확인 |

---

## 2. 항목별 판정

| 항목 | 판정 | 메모 |
|---|---|---|
| V0 HEAD | ✅ | `8a6da21b31df…` — 09-30 이후 서버 pull 없음 |
| V0 worktree | ✅ | 16줄 전부 `??`(수정·삭제 0). 모드 변경 10건(26−16)은 **09-29 Step 4A 기록 "서버 dirty 10파일 = mode-only"와 같은 수** → 기존 사실과 부합. 목록 대조는 W6(낮은 우선순위) |
| V0 ORIG_HEAD/FETCH_HEAD | ✅ | ubuntu · 09:42:43 = deploy pull 1회 |
| V0 ROOT_OWNED 13건 | ⚠️ 0c 무관 · 원인 가설 있음 | 전부 `__pycache__/*.cpython-310.pyc`, 소스 0·디렉터리 0. pull이 건드린 파일은 전부 ubuntu 소유 → 0c 배포와 무관. **가설**: `sudo bash install_bitget_cron.sh`(09-29 Step 4E·재설치, 09-30 부트스트랩)가 root로 생성기를 돌리고, 생성기가 CAT-A `_HEAVY_PREFIXES`를 import하면서 root 소유 pyc를 남김. 기능 영향 없음(파이썬은 쓰기 실패 시 캐시만 생략). W7로 시각 대조. 내 기대값 "0줄"이 이 경로를 몰랐던 것(§5 C-6) |
| V1 | ✅ | PRISTINE · GEN/LIVE/MARKER SHA 3개 동일 · 28/28 · slice 파일 동일 → **pre-pull 가드 생략은 실해 없음** 확정 |
| V2 | ✅ | 1288490188 / 1610612736 |
| V3 | ✅ | mtime 06:52:15 / 09:19:10 / 09:42:44 — 이후 재실행 흔적 없음 |
| V4 | ✅ | 가시성 확인(KLINES=2, 권한 오류 없음) · OOM 0. `workqueue … hogged CPU` 2줄(07:34·07:39)은 CPU 경합 신호 — 비차단 관찰 |
| V5 | ⚠️ | 신원은 D2로 해소. 내용 대조는 §6·§7 |
| V6 | ⚠️ **새 이슈** | watchdog: Phase 0 때의 "activating"은 **타이머가 띄우는 1회성 작업이 도는 중**이었던 것 → 정상, C-3의 watchdog 부분 **종결**. backup·snapshot 반복 실패 → §4-2 |
| L1 | ✅ 우수 | 락 = `fcntl.flock` 120초 폴링 후 skip. **`--cutover-check`는 읽기전용이 아님**(config_bootstrap·artifact_guard·ops gauge·heartbeat). **SKIPPED_LOCK → exit 0 + 무음** 발견은 Phase 1 설계에 결정적(§4-3) |
| L1(e) 124 원인 | 가설 강화 · 확정은 W4 | `bitget.sh` 시작 06:49:53, `timeout`은 06:51:53에 SIGTERM, 락 대기 상한은 python 기동 후 120초라 그보다 몇 초 늦음 → **락 대기였다면 124가 정확히 나오는 구조.** 반대로 락을 잡고 실행 중이었다면 prelude+체크가 ~115초 걸려야 하는데, 같은 체크를 직접 부른 경로는 JSON 덤프·Step 2–3 포함 22초에 끝남. 09-26의 "로그 비어 있는 채 지연"과도 부합. **W4 로그로 확정** |
| L2 | ✅ | 런타임 파일 0. 혼합 버전 우려 **종결**. (참고: 생성기 1줄이 `docs(...)` 메시지 커밋에 섞여 있음 — 커밋 메시지 정확도만 지적) |
| L3 | ✅ 잔여 수용 | p0.sh는 실행 전 수정·실행 직후 커밋 → 실행본=커밋본. arch/deploy는 실행 **후** LF 재작성 → 실행본 바이트 증명 불가. 다만 출력 마커가 echo 순서와 1:1이고, deploy의 git 동작은 W2 reflog로 따로 증명 가능 → **수용**. 발췌뿐인 두 스냅샷 md(한글 깨짐)는 원본 출력으로 보존(W8) |
| L4 | ✅ | `check_cutover_readiness`·`run_architecture_checks` = 순수 읽기(로그 핸들 생성 가능성 제외) |
| D1 | ✅ 기록 | 텔레그램 안 옴 — 락 대기 skip(무음)이든 SIGTERM이든 코드상 기대와 일치. 판별은 W4 |
| D2 | ✅ 신원 해소 (v2 정정) | 디렉터 정정: **09-28 오전(10:48~11:24 KST)은 디렉터 직접 장애 복구**(기억 기반), 그 외는 Cursor. 제3자 접속 가능성 해소. 실행 명령은 W1–W3로 확인. Cursor 메모 "02:24 재기동과 시점이 이어짐"은 여전히 부정확(IP 변경 11:36 ≠ 재시작 02:24) |
| D3 | B 유지 + 1건 앞당김 | §7 |
| D4 | ✅ 승인 반영 | 규칙 7 추가 + "초안" 표기 제거(§8-3 W9) |

---

## 3. Phase 0c 판정 — **SUB_DONE (Claude OK, 2026-10-01)**

근거: 산출물(체크 4건 갱신, Bot-2 `8a6da21`에서 `passed=true`) 유효 + 배포 부작용 검증(V0 HEAD·worktree, V1 PRISTINE, L2 런타임 0, V3 재실행 0, L4 순수 읽기) 전부 기대값 일치.

**이관(0c를 막지 않음)**: 124 원인 → Phase 1 조건 · 09-28 사건 → Phase 1 조건 + SCRIPT-AUDIT · backup/snapshot → Phase 1 조건(진단) · root pyc·모드 10건 → W6·W7 낮은 우선순위.

CAT-L-CUTOVER-01 **전체는 Done 아님.**

---

## 4. 새로 드러난 것 (중요도 순)

### 4-1. 09-28 02:24 UTC(11:24 KST) 서비스 재시작 — 디렉터 장애 복구(v2 정정) ★ Phase 1 전 확인 2건

| 사실 | 출처 |
|---|---|
| FENCE-02 wrapper 수동 적용 09-27 14:20:57 | OUTBOX Step 2 적용 원문 |
| 09-28 01:48–01:57(10:48–10:57 KST) 접속 4건 · 출발지 `110.35.116.11`(그 무렵 Cursor 세션과 같은 네트워크) · OUTBOX·05 기록 없음 | V5 · 기록 검색 |
| 02:24:31–33 factory·async·ws 재시작 + snapshot·watchdog 타이머 재활성(02:24:36–37) | V6 |
| overseer·backup/journal-vacuum 타이머는 09-26 08:52(부팅) 그대로 → **VM 재부팅은 아님**, 특정 유닛만 재시작 | V6 |
| 09-29 08:01 확인 시 wrapper 0줄 · 서버 HEAD `1e38166` | 05 L87–90 |
| **디렉터 정정(10-01): "그때는 내가 직접 했을 것, 서버가 터져서"** | 디렉터 진술(기억 기반) |

**해석**
- **누가·왜 = 해소.** 서버 주인이 장애를 직접 복구한 것 — 정당한 조치이고 절차 위반이 아니다.
- **남은 확인 (a) — 그때 실행된 명령** (W1 sudo · W2 reflog · W3): 복구에 `update_bitget.sh`류가 쓰였다면 설치기가 **당시 origin의 구 생성기**(펜스 코드가 Cursor 로컬에만 있던 상태)로 cron을 다시 만들어 wrapper가 사라진 것 → **FENCE-02 회귀 원인 확정.** 이 경우 근본 원인은 복구 행동이 아니라 **"라이브 변경이 origin(SSOT)에 없었던 것"**이다. 지금은 FENCE-03(마커 + `update_bitget.sh` [0/7] pull 전 가드)이 같은 경로를 막는다 — 라이브가 생성기와 다르면 DRIFTED/UNMARKED로 멈춤.
- **남은 확인 (b) — 서버가 터진 원인** (W3b, 신규): 09-27 14:21 이후는 **펜스가 살아 있던 시간대**다. 이때 OOM으로 터졌다면 1.5G 상한이 부족했거나 cron 밖(상주 서비스) 메모리 문제 → **FENCE-02 3단계(10-13) 판정의 직접 입력.** 재발성 원인이면 Phase 1 48h 창의 위험으로 판정.

### 4-2. backup(127) · snapshot(1) 반복 실패 ★ Phase 1 전 진단

- backup: 오늘 00:30 실행이 exit 127. 127은 셸 관례상 **"명령을 찾을 수 없음"**(systemd가 실행 파일 자체를 못 찾으면 203/EXEC이 나옴) → 스크립트 안에서 부르는 명령(예: `sqlite3` CLI, 또는 `python`↔`python3`)이 없을 가능성. **가설일 뿐**, W5로 확인.
- snapshot(CQRS `bitget_market_data_snapshot.sqlite`): 07:41:37에도 exit 1. 이 DB를 스캔·리포트가 읽는다면 **입력 데이터가 낡아 있을 수 있다** → CAT-B 영향 가능성. 언제부터인지가 핵심.
- 백업이 언제부터 깨졌는지 모른 채 cutover(이중 실행 위험이 문서화된 작업)를 진행하지 않는다. 수리는 별도 sub-phase **`CAT-L-BACKUP-01`**(등록만, 진단 결과 보고 착수 판단).

### 4-3. `bitget.sh` 경로 특성 — Phase 1 설계 입력 (Cursor 발견, 채택)

- 단일 전역 락 + 120초 대기 후 `SKIPPED_LOCK` → **exit 0 + 텔레그램 무음.** `--start-parallel`도 같은 경로 → "성공처럼 보이는데 48h 창이 시작 안 됨"이 가능.
- Phase 1 Handoff에 반드시 들어갈 것: 락이 비는 시간대 선택(cron 시각표 기준) · `timeout` 금지(규칙 3) · 실행 후 `parallel_run_state.json` 존재와 `started_at_utc` 확인을 **완료 조건**으로 · skip이면 재시도 규칙.

### 4-4. 비차단 관찰 (기록만)

- 저장소 루트에 런타임 DB(`ops_events.sqlite`·`message_queue.sqlite`(+wal/shm, 사용 중)·`market_data.sqlite` 등)가 추적 안 되는 파일로 존재. `BITGET_DB_STORAGE_PATH`(`/var/lib/quant-bitget/data`)와의 관계 미상 → **A5-EVENTLOG-01 판정 때 "ops_events가 어느 파일에 쌓이나"로 재사용.** 지금 판단 안 함.
- 커널 `workqueue … hogged CPU` — 소형 인스턴스 CPU 경합 신호. 조치 없음.

---

## 5. Claude 측 정정 (추가분)

| # | 정정 | 조치 |
|---|---|---|
| C-5 | Phase 0 Handoff에서 `bitget.sh --cutover-check`를 **"읽기전용"으로 표기 — 틀림.** L1(c)대로 러너 경로는 config_bootstrap·artifact_guard·ops gauge·heartbeat 쓰기를 포함(정기 cron 잡과 같은 종류의 쓰기) | 앞으로 진짜 읽기전용 진단 = `check_cutover_readiness()` 직접 호출(L4로 순수 읽기 확인). `bitget.sh` 모드는 "운영 실행"으로 분류 |
| C-6 | V0 기대값 "ROOT_OWNED 0줄"이 sudo 설치기의 import 부산물을 고려 못 함 | W7로 가설 확인. 설치기 개선(생성 단계에 `PYTHONDONTWRITEBYTECODE=1` 또는 ubuntu로 생성)은 SCRIPT-AUDIT 뒤 별도 제안 |
| C-7 | 기록 동기화 지시의 근거를 "05 로그"라고 씀 — **틀림.** FENCE-02 SUB_DONE 근거는 `CLAUDE_TO_CURSOR.md`의 FENCE-03 Handoff "선행 상태: CAT-L-FENCE-02 전체 SUB_DONE … 3단계 판정 2026-10-13" 줄. RUN-2는 **파일 근거 없음**(이전 채팅의 Claude OK가 레인 파일에 기록되지 않음) | FENCE-02 행: 위 줄을 근거로 동기화 **허용**. RUN-2 행: **지시 철회** — LANE_FULLBT 창에서 레인 파일에 정식 기록(규칙 4, 이 창에서 레인 혼합 금지). 근거 없이 Done 처리하지 않은 Cursor 판단이 맞았다 |

---

## 6. Cursor OUTBOX 정정 요청 — V5 "설명 불가" 과다

79건 중 상당수는 **같은 OUTBOX·05 로그에 이미 적힌 시각**과 맞는다. 아래는 **시각 상관만**(내용 증명 아님)으로 Claude가 대응시킨 것. SCRIPT-AUDIT 출발점으로 쓸 것.

| 구간(UTC) | 건 | 대응 후보(기록) |
|---|---|---|
| 09-26 08:54–08:57 | 4 | 부팅 직후 점검(OUTBOX "~08:54 UTC · up 2 min", 08:49–08:52 Lightsail Stop/Start) |
| 09-26 09:12–09:19 | 7 | 첫 watchdog tick 실킬 확인(09:15:01) |
| 09-26 13:12–13:14 | 4 | MASTER 3·6 대행(05 로그 "up 4:20" = 부팅 08:52 + 4:20 ≈ 13:12) |
| 09-27 04:09–04:31 | 4 | LANE_FULLBT RUN-2(`run_id=pilot-fut-20260927T040953Z`) — 대응만, 레인 작업 혼합 아님 |
| 09-27 13:29–13:32 | 4 | FENCE-02 Phase 0(host 조회 13:30) |
| 09-27 13:55 | 2 | FENCE-02 Phase 1 메모리 실측(13:55:32) |
| 09-27 14:17–14:23 | 11 | FENCE-02 Step 2 적용(14:20:57) |
| 09-29 07:59–08:01 | 5 | S1/S0/B(08:01) |
| 09-29 09:13–09:21 | 4 | Step 4A dirty 진단(백업 파일명 `…091506`) |
| 09-29 10:27–10:28 | 4 | Step 4E 설치·2차 사고 재설치 구간(로그 10:28 기록) |
| 09-29 11:28–11:29 | 3 | Step 3 B 재개(11:29) |

**남는 미대조 16건**: 09-26 12:18(1) · 14:38–14:42(7) · 09-28 11:36–11:39(5) · 09-29 05:55(1) · 09-30 06:51:56·06:53:44(2). (v2: 09-28 01:48–01:57 4건 = 디렉터 장애 복구, 진술 기준)
추가 대조 포인트: 09-28 Step 3 OUTBOX는 "이 세션 ssh **Permission denied**"라고 적었는데 같은 날 접속 성공 9건이 있다 — 모순은 아닐 수 있으나(시각이 다를 수 있음) SCRIPT-AUDIT에서 시각 정합.

---

## 7. D3 판정 — **B 유지 + 1건 앞당김**

- 자동 A 조건("설명 안 되는 세션")은 두 가지를 섞어 놓은 규칙이었다: **누가**(신원)와 **무엇을**(내용). D2로 "누가"는 해소(제3자 없음). "무엇을"은 원래 SCRIPT-AUDIT-01의 범위다.
- 단 V6가 **기록 없던 상태 변경 1건**(§4-1)을 보여줬다. v2: 디렉터 정정으로 누가·왜는 해소. **실행 명령(W1–W3)과 장애 원인(W3b)** 2가지만 Phase 1 전에 본다.
- 나머지 미대조 세션 내용 대조 + fence02/03 스크립트 8건 → **`CAT-L-SCRIPT-AUDIT-01`**(Phase 1과 병행). W1(sudo 기록)이 그 대부분의 상태 변경을 덮는다.
- 내가 정한 자동 규칙을 내가 고쳐 적용하는 것이므로 근거를 여기 남긴다.

---

## 8. W-블록 (서버, 읽기전용) + 로컬 항목

### 8-0. 실행 규칙

- **실행 수단 = 지난번 Cursor 방식 채택**: 커밋된 Handoff blob에서 §8-1 코드펜스를 바이트 그대로 추출 → 줄 수·CR 0·sha256 공개 → ssh stdin(바이너리) `bash -s`. Git Bash heredoc보다 출처 증명이 강하다. 도구 `vb_run.py`는 서버 실행 경로가 아닌 곳(예: `bitget/docs/work_phases/tools/`)에 **커밋**(W9).
- 실행 순서: §8-1 → §8-1b (블록마다 추출 sha256 따로 공개). v1 §8-1을 이미 실행했다면 §8-1b만.
- `set -e` 없음 · `sudo` 0 · W-블록 외 서버 명령 0.
- 출력 **전문** OUTBOX. `systemctl cat` 출력에 비밀값(토큰·키)이 보이면 `***`로 가리고 "가림" 표기.
- 출력이 너무 길면 전문은 `snapshots/CAT-L-CUTOVER-01_P1PRE_WBLOCK_20261001.md`에 두고 OUTBOX엔 경로 + sha256 + 판정에 쓴 줄 원문.

### 8-1. W-블록

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}" || exit 1
echo "== W-BLOCK START $(date -u +%Y-%m-%dT%H:%M:%SZ) user=$(id -un) =="

echo "== W1 sudo commands since 2026-09-26 UTC =="
J="$(TZ=UTC journalctl --utc -t sudo --since '2026-09-26 00:00:00' --no-pager 2>&1)"
printf '%s\n' "$J" | grep -E 'COMMAND='
echo "W1_COUNT=$(printf '%s\n' "$J" | grep -c 'COMMAND=')"; printf '%s\n' "$J" | head -2

echo "== W2 git reflog =="
git reflog --date=iso-strict -n 40

echo "== W3 2026-09-28 01:40-02:40 UTC systemd/cron/sudo =="
TZ=UTC journalctl --utc --since '2026-09-28 01:40:00' --until '2026-09-28 02:40:00' --no-pager -t systemd -t sudo -t CRON 2>&1 | grep -v -E 'Started Session|session-[0-9]+\.scope|run-[a-z0-9]+\.scope|user@[0-9]+\.service' | head -200

echo "== W4 cutover_check log (2026-09-30 15:49:53 KST) =="
F=/var/lib/quant-bitget/logs/bitget_cutover_check_20260930_154953.log
ls -l --time-style=full-iso "$F"; wc -l "$F"; head -40 "$F"; echo "--TAIL--"; tail -15 "$F"
echo "-- logs started 2026-09-30 14:30-15:59 KST --"
ls -l --time-style=full-iso /var/lib/quant-bitget/logs | grep -E '_20260930_(14[3-5]|15[0-5])[0-9]{3}\.log'

echo "== W5 backup / snapshot =="
systemctl cat dante-bitget-backup.service dante-bitget-snapshot.service --no-pager
if command -v sqlite3 >/dev/null 2>&1; then echo "SQLITE3_CLI=yes"; else echo "SQLITE3_CLI=no"; fi
TZ=UTC journalctl --utc -u dante-bitget-backup.service -n 30 --no-pager
TZ=UTC journalctl --utc -u dante-bitget-snapshot.service -n 30 --no-pager
echo "-- backup results since 2026-09-01 --"
TZ=UTC journalctl --utc -u dante-bitget-backup.service --since '2026-09-01 00:00:00' --no-pager | grep -E 'Deactivated successfully|Failed with result|status=' | tail -20
echo "-- snapshot last successes --"
TZ=UTC journalctl --utc -u dante-bitget-snapshot.service --since '2026-09-01 00:00:00' --no-pager | grep -E 'Deactivated successfully' | tail -3
echo "SNAPSHOT_FAILS_SINCE_0926=$(TZ=UTC journalctl --utc -u dante-bitget-snapshot.service --since '2026-09-26 00:00:00' --no-pager | grep -c 'Failed with result')"

echo "== W6 mode-only changes =="
git diff --summary | grep 'mode change'

echo "== W7 root-owned in repo (UTC mtime) =="
TZ=UTC find . -xdev -path ./venv -prune -o -user root -printf '%TY-%Tm-%TdT%TH:%TM %u %p\n' 2>/dev/null | sort

echo "== W-BLOCK END $(date -u +%Y-%m-%dT%H:%M:%SZ) =="
```

### 8-1b. W3b — 09-28 장애 원인 (v2 신규, 읽기전용)

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}" || exit 1
echo "== W3b START $(date -u +%Y-%m-%dT%H:%M:%SZ) user=$(id -un) =="
echo "== W3b-1 kernel 2026-09-27 14:20 ~ 09-28 02:40 UTC =="
K="$(TZ=UTC journalctl --utc -k --since '2026-09-27 14:20:00' --until '2026-09-28 02:40:00' --no-pager 2>&1)"
echo "KLINES=$(printf '%s\n' "$K" | wc -l)"; printf '%s\n' "$K" | head -2
printf '%s\n' "$K" | grep -i -E 'out of memory|oom-kill|killed process|blocked for more than|hung_task' | head -40
echo "OOM_HITS=$(printf '%s\n' "$K" | grep -c -i -E 'out of memory|oom-kill')"
echo "== W3b-2 bitget long-running units, same window =="
TZ=UTC journalctl --utc --since '2026-09-27 14:20:00' --until '2026-09-28 02:40:00' --no-pager -t systemd 2>&1 | grep -E 'dante-bitget-(factory|async|ws|queue-worker|overseer)|bitget-cron-heavy' | grep -E 'Failed|failed|Killed|oom|Main process exited|Stopping|Stopped|Started' | head -80
echo "== W3b END $(date -u +%Y-%m-%dT%H:%M:%SZ) =="
```

### 8-2. 기대값 (Cursor 1차 대조 → 불일치는 고치지 말고 보고)

| 항목 | 기대 / 볼 것 | 주의 |
|---|---|---|
| W1 | `W1_COUNT ≥ 1` — 기록상 sudo 설치기 실행이 최소 1회(09-30 부트스트랩) 있었으므로 **0이면 가시성 실패**로 보고. 각 `COMMAND=` 줄 옆에 대응 Handoff 절 주석. **09-28 01:40–02:40 구간 줄은 별도 표시** | 0을 "sudo 없음"으로 해석 금지(교훈 2) |
| W2 | `8a6da21 … pull --ff-only: Fast-forward` ≈ 09-30 09:42:43 · `c1ffe3f` ≈ 09-30 06:00. **09-28 01:48–02:30 사이 HEAD 이동 여부가 핵심** | reflog 메시지가 곧 실행된 git 명령 |
| W3 | 02:2x의 `Stopping/Started dante-bitget-*`, `Reloading`(daemon-reload), 직전 `sudo`·`CRON` 줄 | 디렉터 복구 때 무엇이 돌았는지 — 사람을 탓하는 항목 아님, 명령만 |
| W3b | `KLINES ≥ 1`·권한 오류 없음 확인 후: 01:48 이전의 OOM·`Killed process`·`hung_task`, 상주 유닛의 `Main process exited`/`Failed` 시각. **0건이면 "터짐"은 커널·유닛 수준이 아님** → 그때 디렉터가 본 증상(텔레그램 끊김 등)을 다음에 1줄로 받음 | 0을 "문제없음"으로 해석 금지 |
| W4 | 로그 크기·내용: 비어 있거나 락 대기 문구뿐 → **락 대기 확정** / 단계 로그(config_bootstrap 등) 있음 → **실행 중 종료** → 그 단계의 쓰기 범위 별도 판정. 15:30–15:49 KST에 시작해 그 시각에도 돌던 잡 = 락 보유 후보 | 로그를 열기만, 수정 금지 |
| W5 | `ExecStart`, 127/1의 실제 메시지, **마지막 성공 시각**(언제부터 실패인지), `SQLITE3_CLI` | 재시작·`reset-failed`·수동 실행 금지 |
| W6 | 10줄 내외 mode change | 09-29 Step 4A 백업 diff와 같은 파일인지만 |
| W7 | pyc mtime이 W1의 sudo 설치기 시각과 겹치는지 | 삭제·chown 금지 |

### 8-3. 로컬 항목

- **W8 증거물 원본 보존**: 로컬에서 `scp "ubuntu@3.36.90.195:/tmp/cutover01_*.out" bitget/docs/work_phases/snapshots/raw_20260930/` (서버 쪽은 읽기만) → 3파일 sha256이 V3 값(`38d18d65…`·`07b5e70a…`·`4a583ef7…`)과 **같아야 함** → 커밋 전 `KEY|SECRET|TOKEN|PASS` 문자열 건수 확인(내용 말고 건수만 보고) → 커밋. 서버 `/tmp` 원본은 계속 보존.
- **W10 (디렉터 승인 시에만)**: `CAT-L_인프라배포.md` 규칙 8 — "디렉터가 서버를 직접 작업한 날은 사후 05 로그에 '언제·왜' 1줄(Cursor가 대신 기록 가능). 승인·허가 절차 아님, 기록용."
- **W9 규칙 문서**: `CAT-L_인프라배포.md` 「서버 실행 경로 (2026-10-01)」에서 "디렉터 승인 전 초안" 표기 제거(D4 승인 2026-10-01 18:30 KST) + 아래 규칙 7 추가 + 실행 수단 표준 1줄. 커밋 해시와 `git show --stat`를 OUTBOX에.

```markdown
7. (2026-10-01 디렉터 보충) Handoff 명령은 그대로 실행한다. 더 나은 방법·위험·대안이 보이면 **실행 전** OUTBOX에 '의견'으로 제시하고, 반영 결정 뒤에 바꾼다. 조용한 변경 금지. 실행 **수단만** 바꾸고 내용 바이트가 같다면 해시 증명과 함께 같은 회신에서 공개하면 된다.

실행 수단 표준: 커밋된 Handoff blob에서 블록을 바이트 그대로 추출 → 줄 수·CR 0·sha256 공개 → ssh stdin(바이너리) `bash -s`. 도구 `vb_run.py`(로컬 전용, 서버 미배포).
```

---

## 9. Phase 1 착수 조건 (갱신)

| # | 조건 | 상태 |
|---|---|---|
| 1 | Phase 0c SUB_DONE | ✅ 이번 판정 |
| 2 | 124 원인 확정 → Phase 1 실행 방식 설계(§4-3) | W4 |
| 3 | 09-28 복구 때 실행된 명령(→ FENCE-02 회귀 원인 기록) + 서버가 터진 원인이 재발성인지(→ 48h 창 위험 판정, FENCE-02 10-13 판정 입력) | W1–W3 · W3b |
| 4 | backup·snapshot 원인과 시작 시점 → `CAT-L-BACKUP-01` 착수 여부 및 Phase 1과의 선후 판정(snapshot이 스캔 입력이면 **수리 먼저**) | W5 |
| 5 | V4 OOM 0 · L4 순수 읽기 · D3/D4 | ✅ |

전부 충족 시 Claude가 Phase 1 Handoff를 **별도** 발행. 큐 순서 불변(11번 백필은 8번 종결 후).

---

## 10. 출력 형식 체크

- **SSOT 변경**: `CAT-L_인프라배포.md` 규칙 7 + 초안 표기 제거(문서) · 05/NEXT_ACTION/00/09/NEXT_STEP 상태 · 신규 등록 `CAT-L-SCRIPT-AUDIT-01`·`CAT-L-BACKUP-01`(대기)
- **SSOT 비변경**: 코드 0 · 서버 0(W-블록 읽기전용) · 게이트·NAV·CAT-F/G/I/N/B/D 0 · cron·slice·`.env`·`BITGET_PIPELINE_SSOT` 0 · `ENABLE_REAL_EXECUTION` OFF
- **SPOT/FUT**: 해당 없음(운영·검증 절차, `market_type` 무관)
- **인접 CAT**: CAT-A(전역 락·SKIPPED_LOCK exit 0 — 이번엔 설계 변경 없음, Phase 1 설계 때 참조) · CAT-B(snapshot DB가 스캔 입력이면 영향 → 수리 시 Critical 여부 판정) · CAT-F(A5 — §4-4 관찰만)
- **롤백**: 문서 커밋 revert. 서버 변경 없음
- **Critical**: 현재 해당 없음. W5 결과가 CAT-B 데이터 경로 수리로 이어지면 그때 판정

---

## 11. 문서 갱신 (붙여넣을 문구)

**`05_진행로그.md` 최상단**
```markdown
## CAT-L-CUTOVER-01 Phase 0c · **SUB_DONE (Claude OK 2026-10-01)**
V-블록 실행본 = Handoff 원문(Claude 재계산 sha256 9879eb2b… 일치). V0 HEAD 8a6da21·worktree 수정 0 · V1 PRISTINE 28/28 · L2 런타임 0 · V3 재실행 0 · L4 순수 읽기.
이관: 124 원인(W4) · 09-28 02:24 서비스 재시작 = 디렉터 장애 복구(정정 진술) → 실행 명령(W1–W3)·장애 원인(W3b) 확인 · backup 127/snapshot 1 반복 실패(W5) · root pyc 13·mode 10(W6·W7).
Claude 정정: C-5 `bitget.sh --cutover-check`는 읽기전용 아님 · C-6 ROOT_OWNED 기대값 · C-7 FENCE-02 근거=CLAUDE_TO_CURSOR FENCE-03 선행 상태 줄, RUN-2 동기화 지시 철회(LANE_FULLBT 창에서).
D2 정정(10-01): 09-28 오전은 디렉터 직접 복구(서버 장애), 그 외 세션은 Cursor. D3: B 유지 + 09-28 명령·장애 원인만 앞당김. D4 승인 → 규칙 7 추가.
Phase 1 보류(§9 조건). 신규 등록: CAT-L-SCRIPT-AUDIT-01 · CAT-L-BACKUP-01.
```

**`NEXT_ACTION.md`**
- CUTOVER 행: `CAT-L-CUTOVER-01 | **Phase 0c SUB_DONE** · Phase 1 보류 · 선행 W-블록(읽기전용) 회신 대기`
- FENCE-02 행: `SUB_DONE (1–2단계) · 3단계 판정 2026-10-13` — 근거: CLAUDE_TO_CURSOR FENCE-03 Handoff 선행 상태 줄
- LANE_FULLBT 행: **변경 안 함**(레인 창에서 처리)
- 신규 행: `CAT-L-SCRIPT-AUDIT-01 | 등록 · Phase 1과 병행 · 대기` / `CAT-L-BACKUP-01 | 등록 · W5 진단 후 착수 판단`

**`00_전체현황판.md`** "다음 Handoff": `CAT-L-CUTOVER-01 Phase 1 선행 W-블록(읽기전용) · 0c SUB_DONE`

**`09_디렉터_쉬운요약.md`**
```markdown
✅ 지난번 서버 점검 결과를 Claude가 직접 다시 계산해서 맞춰봤어요. 서버 프로그램 업데이트(0c)는 안전하게 끝난 걸로 확정!
🔍 대신 새로 2가지를 발견했어요.
  ① 9/28(일) 오전 재시작은 디렉터님이 서버 장애를 직접 복구하신 거였어요. 그때 어떤 명령이 돌았는지, 서버가 왜 터졌는지를 서버 기록을 "보기만" 해서 확인해요. 예약작업 안전장치가 그 무렵 사라진 이유가 여기서 밝혀질 가능성이 커요 — 복구가 잘못된 게 아니라, 안전장치 코드가 아직 공식 저장소에 안 올라가 있었던 게 원인일 가능성이 커요.
  ② 서버의 자동 백업과 데이터 사본 만들기가 계속 실패하고 있어요. 언제부터인지, 왜인지 먼저 확인해요.
⏸ 그래서 "48시간 비교 관찰"은 이 두 가지가 풀린 뒤에 열어요. 지금 서버에서는 아무것도 바꾸지 않아요.
```

**`NEXT_STEP`**
```markdown
다음 할 일: Cursor가 "보기만 하는 확인"(W1~W7)을 서버에서 한 번 실행하고 결과를 그대로 붙여넣기 + 점검 원본 파일 3개를 저장소에 보관(W8) + 규칙 문서 마무리(W9).
디렉터: 지금 추가로 하실 일 없음. (제안) 앞으로 서버를 직접 만지신 날은 "언제·왜" 한 줄만 Cursor에게 알려 주세요 — 기록에만 남깁니다.
```

---

## 금지 (이번)

W-블록 외 서버 명령 일체(pull · 설치기 · restart · `reset-failed` · backup/snapshot 수동 실행 · `--cutover-check`·`--start-parallel` · `.env` 수정 · `sudo` · pyc 삭제/chown) · 증거물 삭제·수정 · fence02/03 스크립트 재실행 · LANE_FULLBT 파일 수정 · `BITGET_PIPELINE_SSOT` · C-2 · MDD5% · live · `ENABLE_REAL_EXECUTION`

## 완료 정의

이 Handoff 커밋 해시 + §8-1·§8-1b 추출 sha256(각각) + 출력 전문(또는 스냅샷 경로+해시) + W8 sha256 대조 + W9 커밋 + §11 문서 갱신 → `track_b_CURSOR_TO_CLAUDE.md` 상단 OUTBOX. Claude 판정 전 Phase 1 착수 금지.

## sub-phase ID

`CAT-L-CUTOVER-01` (Phase 0c SUB_DONE → Phase 1 선행 점검)

---

# ARCHIVE (이전 Handoff · 2026-10-01 스크립트검토)

# CLAUDE → CURSOR · CAT-L-CUTOVER-01 · Phase 0c 확정 심사 — 스크립트 3건 줄 단위 검토 + V-블록(읽기전용)

> **작성**: Claude Pro (Architect) · 2026-10-01
> **입력**: `track_b_CURSOR_TO_CLAUDE.md` 상단 OUTBOX「[MASTER] wrapper 스크립트 3건 원문」 — Claude 프로젝트 스냅샷(2026-09-30 12:09 기준)에서 원문 3건 확인
> **대조한 Handoff**: `CLAUDE_TO_CURSOR.md`「Phase 0(사전점검)」 전문 · 「Phase 0c(checks 갱신)」 요약본. **Phase 0c 배포 Handoff 원문은 스냅샷에 없음**(§3 C-1)
> **CAT**: CAT-L 🟡 (deep) · CAT-A 읽기 참조만(runtime lock) · 코드 변경 0 · 서버 변경 0 · Critical 해당 없음
> **이 파일**: `bitget/docs/work_phases/CLAUDE_TO_CURSOR.md` 상단에 **전문** 붙여넣기(요약 금지 — §5 규칙 6). 커밋·push 후 그 해시를 OUTBOX에 적고 나서 §4 V-블록 실행.
> **시각 표기**: 전부 UTC (KST는 괄호)

---

## 0. 결론 (디렉터용)

1. 3건 원문에서 **위험 동작은 0** — 설치기 · `update_bitget.sh` · `--start-parallel` · `BITGET_PIPELINE_SSOT` 변경 · `.env` 쓰기 · 서비스 재시작 · `sudo` 전부 본문에 없음. Cursor의 서술은 본문 대조로 사실. **Phase 0c 결과(Bot-2 `8a6da21` architecture PASS)를 무효화할 사유 없음.**
2. 대신 **결함 3건**: ① deploy가 pull **전** cron 안전장치(`--diff-live`) 확인을 건너뜀 ② "8a6da21 맞나" 확인이 pull **후**(순서 역전) ③ p0가 `bitget.sh --cutover-check`를 120초에 강제 종료 — **대기 중이었는지 실행 중이었는지 미확정**(이번 검토의 유일한 미확정).
3. 상태: Phase 0c **원문 검토 완료 · 확정 대기** — §4 V-블록(서버 읽기전용) + L-항목(로컬) 회신 → Claude OK → SUB_DONE. **Phase 1 보류 유지.**

---

## 1. 판정 요약

| 스크립트 | 실행 (UTC) | 서버 상태 변경 | Handoff 대조 | 판정 |
|---|---|---|---|---|
| `cutover01_p0.sh` (커밋 `29e9c8a`) | 09-30 06:49–06:53 | 본문상 없음(`/tmp` 제외). **단 11행 `bitget.sh --cutover-check` 내부 동작 미확정** | 명령 = Phase 0 Handoff Step 1–3 그대로. 추가(Handoff 밖) = `timeout 120` · `.env` source + `check_cutover_readiness()` 직접 호출 · `/tmp` tee | **수용** — L1·D1로 1건 종결 필요 |
| `cutover01_p0c_arch.sh` (미커밋) | 09-30 09:19 | 없음(`/tmp` 제외) | 스냅샷에 지시 근거 없음(0c 요약본에 서버 실행 지시 없음, 당시 OUTBOX "Bot-2 재실행은 Claude 판단") → **Handoff 밖으로 기록** | **수용** |
| `cutover01_p0c_deploy.sh` (미커밋) | 09-30 09:42:40 | **git 워킹트리 `c1ffe3f`→`8a6da21`** (3건 중 유일) | 명령 구조(커밋→push→pull→덤프)는 배포 Handoff 유래 정황 인정(배포 OUTBOX의 Step 1/2/3 구성·"Handoff는 최상위 passed false 가능을 예상" 문구). 원문 대조는 불가(§3 C-1). 파일화·tee·`.env` source = Handoff 밖 | **수용** — 결함 2건 사후확인(V0·V1·L2) |

`cutover01_p0.sh`의 "Claude 지시 여부"는 이제 판정 가능: **명령은 Handoff, 스크립트 파일과 3개 추가 동작은 Handoff 밖.** Cursor 정정("Handoff에 없던 것으로 취급")은 실제보다 보수적이며, 위 분류로 기록할 것.

---

## 2. 줄 단위 검토

줄 번호 = OUTBOX에 붙인 각 스크립트의 1행 기준.

### 2-1. `cutover01_p0.sh` (34줄)

| 줄 | 내용 | 판정 | 메모 |
|---|---|---|---|
| 1–2 | shebang · 주석 "read-only" | 주장 | 11행 확인 전까지 사실인 범위는 "본문에 쓰기 명령 없음"까지 |
| 3 | `set -u` | OK | |
| 4–5 | `INSTALL_ROOT` · `cd` | OK | 기본 경로 = Handoff와 동일 |
| 6–7 | `/tmp/cutover01_p0.out` tee | 서버 쓰기 1 | 증거물 — 보존(§4-4) |
| 8 | TS · `git rev-parse --short HEAD` | OK | 읽기 |
| 10–12 | `set +e` · **`timeout 120 ./bitget/deploy/bitget.sh --cutover-check`** · RC 출력 | ★ **미확정** | 명령 = Handoff Step 1. `timeout`은 Handoff 밖. RC=124 = 120초 시점 SIGTERM. CAT-A상 `bitget.sh` 모드는 **Bitget runtime lock 직렬** → "락 대기 중 종료" 가설 유력(09-26 MASTER 6에도 "로그 비어 있는 채 지연" — 같은 증상 2회째). "실행 중 종료"였다면 러너 `cutover_check` 모드가 텔레그램 전송(`--skip-telegram` 미지정 시)·실행 기록 쓰기를 하던 중 끊겼을 수 있음. GNU `timeout`은 기본(`--foreground` 없음) 프로세스 그룹 전체에 SIGTERM → 고아 프로세스 가능성 낮음, 단 `setsid`/백그라운드 자식은 예외 → **L1·D1** |
| 13 | `set -e` | **결함** | 복원이 아니라 errexit **신규 활성**(3행에서 안 켰음). 이후 24행 python이 실패하면 25행 전에 종료 → `JSON_DUMP_RC`는 **0만 찍힐 수 있는 줄**. 완주 증거는 33행 `=== DONE` 출력 존재(V3) |
| 15–20 | `set +u`·`set +a`·`.env`/`bitget/.env` source(`set -a`)·`set -u` | Handoff 밖 | 쓰기 아님. 단 source = `.env`를 셸 코드로 실행 + 모든 키(API 키 포함) export. 러너의 env 로딩(파이썬)과 따옴표·`$` 해석이 다를 수 있음 — 이번 판정 키 3개는 단순값이라 **판정 영향 없음**. 16행 `set +a`는 중복(무해) |
| 21–23 | PYTHONPATH · venv python | OK | |
| 24 | `check_cutover_readiness()` 직접 호출 | Handoff 밖(대체 경로) | **Phase 0 판정 JSON의 실제 출처.** Phase 0 OUTBOX엔 "JSON dump는 완료"뿐, 출처가 이 경로라는 건 Phase 0b Step 5(Claude 질문 후)에 명시 — 공개 지연. 부작용 여부 → **L4** |
| 25 | `JSON_DUMP_RC=$?` | 증거력 없음 | 13행 때문 |
| 26–28 | env 3키 grep (`.env`, `bitget/.env`) | OK | Handoff Step 2 + `bitget/.env` 확장. 비밀값 출력 없음 |
| 29–32 | `pgrep` 2 · `systemctl list-units` | OK | Handoff Step 3. **32행 출력의 `dante-bitget-backup`/`-snapshot` failed · watchdog activating은 Claude가 Phase 0 판정에서 누락**(§3 C-3) |
| 33–34 | DONE · WROTE | OK | 33행 문구는 본문상 사실 |

출처 정황: 커밋 `29e9c8a` 시각 06:54:44 UTC(15:54:44 KST) = 실행 종료 약 1분 뒤 → **실행본 = 커밋본 정황 강함.** L3로 이후 수정 없음만 확인.

### 2-2. `cutover01_p0c_arch.sh` (22줄)

| 줄 | 내용 | 판정 | 메모 |
|---|---|---|---|
| 1–2 | 주석 | 주장 | |
| 3 | `set -u` (errexit 없음) | OK | 21행 RC가 정확함(2-1과 대조) |
| 4–7 | 경로 · `/tmp` tee | OK / 서버 쓰기 1 | 보존 |
| 8–9 | TS · HEAD | OK | 실행 당시 HEAD=`c1ffe3f` → **구버전 체크**가 돌았음 |
| 10–11 | `architecture_checks.py` mtime 출력 | 좋음 | "어느 버전 코드가 돌았나"를 남기는 습관. 이후 표준으로 |
| 12–14 | PYTHONPATH · python | OK | |
| 15–19 | `.env` source | 불필요 | 구조 검사에 env 전체가 필요할 이유 없음. 특정 키 의존이 있으면 L4에서 밝힐 것. 쓰기 아님 |
| 20 | `run_architecture_checks()` (c1ffe3f판) | 읽기 추정 | **L4**(c1ffe3f판) |
| 21–22 | RC · WROTE | OK | |
| (실행 흔적) | 끝의 `$'\r'` 경고 | ★ **출처** | 디스크 파일은 LF인데 실행 스트림 끝에 CR이 섞였음 = **실행된 바이트 ≠ 디스크 파일**의 실증. 이번엔 빈 줄이라 무해. 로컬 파이프 실행 금지 근거(§5 규칙 2) |

### 2-3. `cutover01_p0c_deploy.sh` (29줄)

| 줄 | 내용 | 판정 | 메모 |
|---|---|---|---|
| 1–2 | 주석 "No installer, no update_bitget.sh" | 사실 | 단 `update_bitget.sh`를 안 거치면서 그 [0/7] **pull 전 `--diff-live`도 함께 빠짐** |
| 3 | `set -u` | OK | 12·28행 RC 정확 |
| 4–9 | 경로 · `/tmp` tee · TS · BEFORE | OK / 서버 쓰기 1 | |
| (9–10 사이, 부재) | pull 전 `--diff-live` · `git status` | ★ **결함** | FENCE-03 Spec 4("변경 작업(pull/재시작 등) **이전**에 `--diff-live`") 위반. pull 범위에 생성기 변경(`eb80c58`)이 들어 있었는데 가드 미확인. 원인 일부는 Claude(§3 C-2). 사후 확인 = **V1** |
| 10 | `git fetch --quiet` | 경미 | RC 미기록(11행 실패로 드러나므로 영향 작음) |
| 11 | `git pull --ff-only` | ★ **서버 변경** | 3건 중 유일한 상태 변경. upstream tip으로 이동 — **대상 SHA 고정 아님** |
| 12 | `PULL_RC` | OK | |
| 13 | `AFTER=$(git log -1 --format='%h')` | 경미 결함 | 짧은 해시는 저장소가 커지면 7→8자 이상으로 늘 수 있음 → 오탐 STOP. 전체 SHA로 비교 |
| 14–18 | 8a6da21 비교 → 불일치 시 `exit 9` | ★ **결함(순서)** | 고정이 **이동 후**. origin에 8a6da21 이후 커밋이 있었다면 서버는 미검토 커밋에 머문 채 "STOP"만 함(되돌림 없음). 이번엔 일치라 실해 없음. 올바른 순서는 §5 레시피 |
| (부재) | 서비스 재시작 없음 | 범위상 정당 | 결과: 상주 서비스 = `c1ffe3f` 코드(메모리), cron이 새로 띄우는 잡 = `8a6da21` 코드 → **혼합 버전**. `8a6da21` 커밋은 2파일이지만 **pull 범위 `c1ffe3f..8a6da21`**엔 `29e9c8a`·`eb80c58` 등이 포함 → **L2**로 런타임 코드 포함 여부 확인 |
| 19–21 | PYTHONPATH · python | OK | |
| 22–26 | `.env` source | 불필요 | 2-2와 동일 |
| 27 | `run_architecture_checks()` (8a6da21판) → `passed=true` | **유효** | 해시 일치 확인 뒤 실행이라 결과 신뢰 가능. **L4**(8a6da21판) |
| 28–29 | RC · WROTE | OK | |

### 2-4. Cursor 자가점검 4건 + Claude 추가 발견

Cursor 4건 — **전부 동의.**
1. `--cutover-check` 쓰기 여부 미확인 → 이번 검토의 유일한 미확정. L1·D1.
2. `.env` source → 맞음. 이번 판정값엔 영향 없음.
3. `/tmp` 쓰기 → 맞음. 증거물로 보존.
4. deploy만 워킹트리 변경 → 맞음.

Claude 추가 발견(Cursor 미기재): p0 13행 errexit로 RC 무력화 · deploy pull 전 가드 생략 · 해시 고정이 pull 후 · 짧은 해시 비교 · pull 범위 ≠ 커밋 범위(혼합 버전) · CR 경고 = 실행본≠디스크본 실증 · Phase 0 Step 3 failed unit 미처리.

---

## 3. Claude 측 누락 — 자기 정정

| # | 누락 | 결과 | 조치 |
|---|---|---|---|
| C-1 | Phase 0c **배포 Handoff를 `CLAUDE_TO_CURSOR.md`에 전문으로 남기지 않음**(현재 파일엔 checks 갱신 요약 + `Downloads/` 원문 포인터뿐) | p0c_deploy 명령이 Handoff와 같았는지 원문 대조 불가 | §5 규칙 6 |
| C-2 | 배포 단계에 **pull 전 `--diff-live`가 빠짐.** 이 규칙은 내가 하루 전 FENCE-03 Spec 4로 정한 것 | 배포 Handoff가 맨 `git pull --ff-only`였다면 **내 Handoff 결함**(C-1 때문에 확정 불가 — 양쪽 다 고침) | V1 사후 확인 · §5 레시피 |
| C-3 | Phase 0 Step 3 출력의 `dante-bitget-backup`/`dante-bitget-snapshot` **failed**, watchdog **activating**을 판정에 반영 안 함(기준이 "Step 3 이상 없음"이었는데 레거시 프로세스만 봄) | 미처리로 떨어져 있었음 | V6 · Phase 1 착수 조건 |
| C-4 | `--cutover-check` RC=124를 Phase 0b에서 "참고, 낮은 우선순위"로 내림 | 같은 지연 2회째(09-26·09-30). HIST 절차의 Phase 1(`--start-parallel`)·Phase 3(48h 후 `--cutover-check`)이 같은 `bitget.sh` 경로를 씀 | **Phase 1 착수 조건으로 격상** · L1 |

---

## 4. 확정 심사 — V-블록(서버, 읽기전용) + L-항목(로컬)

### 4-0. 실행 규칙 (이번 V-블록)

- 이 Handoff를 먼저 커밋·push. **V-블록은 아래 텍스트를 바이트 그대로** 실행. 수단은 Git Bash(LF)에서 `ssh <bot2> 'bash -s' <<'EOF'` … `EOF`(따옴표 EOF로 로컬 확장 차단) 또는 인터랙티브 붙여넣기.
- `set -e` 넣지 말 것(한 줄 실패가 나머지를 막지 않게). **`$'\r'` 오류가 한 줄이라도 나오면 즉시 중단·보고.**
- 따옴표 문제로 한 줄이라도 고쳤으면 고친 줄을 출력과 함께 OUTBOX에.
- 출력은 **요약 없이 전문** OUTBOX. 해석은 출력 아래 별도 칸.
- V-블록 외 서버 명령 0. `sudo` 0.

### 4-1. V-블록

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}" || exit 1
echo "== V-BLOCK START $(date -u +%Y-%m-%dT%H:%M:%SZ) user=$(id -un) =="

echo "== V0 HEAD / worktree / owner =="
git rev-parse HEAD
echo "PORCELAIN_ALL=$(git status --porcelain | wc -l) PORCELAIN_CONTENT=$(git -c core.fileMode=false status --porcelain | wc -l)"
git -c core.fileMode=false status --porcelain | head -20
TZ=UTC stat -c '%U %y %n' .git/ORIG_HEAD .git/FETCH_HEAD bitget/validation/architecture_checks.py
echo "ROOT_OWNED_IN_REPO:"; find . -xdev -path ./venv -prune -o -user root -print 2>/dev/null | head -20

echo "== V1 cron drift guard =="
PYTHONPATH="$PWD" python3 bitget/deploy/generate_bitget_crontab.py --diff-live; echo "DIFF_LIVE_RC=$?"

echo "== V2 heavy slice =="
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax --no-pager

echo "== V3 /tmp evidence =="
ls -l --time-style=full-iso /tmp/cutover01_*.out
sha256sum /tmp/cutover01_*.out
grep -n -E '^=== |_RC=|^HEAD=|^BEFORE:|^AFTER:|HASH_MISMATCH|^WROTE ' /tmp/cutover01_*.out

echo "== V4 kernel OOM 2026-09-30 06:45-10:00 UTC =="
K="$(TZ=UTC journalctl -k --since '2026-09-30 06:45:00' --until '2026-09-30 10:00:00' --no-pager 2>&1)"
echo "KLINES=$(printf '%s\n' "$K" | wc -l)"; printf '%s\n' "$K" | head -3
echo "OOM_HITS=$(printf '%s\n' "$K" | grep -c -i -E 'out of memory|oom-kill')"

echo "== V5 ssh accepted since 2026-09-26 UTC =="
S="$(TZ=UTC journalctl --utc -t sshd -t sshd-session --since '2026-09-26 00:00:00' --no-pager 2>&1 | grep -o -E '^.*Accepted (publickey|password) for [^ ]+ from [^ ]+')"
echo "SSH_ACCEPTED_COUNT=$(printf '%s' "$S" | grep -c .)"
printf '%s\n' "$S"

echo "== V6 units =="
systemctl --failed --no-legend --plain
for u in $(systemctl list-units --all --no-legend --plain 'dante-bitget-*' | cut -d' ' -f1); do
  systemctl show "$u" -p Id -p ActiveState -p SubState -p Result -p ExecMainStatus -p NRestarts -p ActiveEnterTimestamp -p ExecMainStartTimestamp --no-pager; echo
done

echo "== V-BLOCK END $(date -u +%Y-%m-%dT%H:%M:%SZ) =="
```

### 4-2. 기대값 (Cursor 1차 대조 → 불일치는 고치지 말고 보고)

| 항목 | 기대값 | 불일치 시 |
|---|---|---|
| V0 | HEAD가 `8a6da21`로 시작(09-30 이후 서버 pull 없음) · `PORCELAIN_CONTENT`의 수정(M)·삭제(D) 0 (`??` 추적 안 되는 파일은 목록만) · `.git/ORIG_HEAD` 소유자 `ubuntu`, 시각 ≈ 09-30 09:42:4x · `ROOT_OWNED_IN_REPO` 0줄 | `FETCH_HEAD`가 09:42:40 이후면 V5 세션과 대조해 설명 |
| V1 | `DIFF_LIVE_RC=0` · `PRISTINE` · `BODIES_EQUAL=yes` · `WRAP=28` · SHA가 부트스트랩 기록 `f36f722489ce…66b21dd`와 동일 | 보고만. **설치기 실행 금지** |
| V2 | `MemoryHigh=1288490188` · `MemoryMax=1610612736` (ActiveState는 참고) | 보고만 |
| V3 | 3파일 존재 · mtime ≈ 06:53 / 09:19 / 09:42 UTC (**그 이후 시각이면 재실행 흔적**) · p0: `CUTOVER_CHECK_RC=124`·`JSON_DUMP_RC=0`·`=== DONE` 포함 · arch: `HEAD=c1ffe3f` · deploy: `PULL_RC=0`·`AFTER: 8a6da21`, `HASH_MISMATCH` 없음 | 파일이 없으면 "없음" + 재부팅 여부 |
| V4 | `KLINES ≥ 1`이고 head 3줄에 권한 오류 문구 없음 · `OOM_HITS=0` | 권한 오류면 **"0"을 안전으로 해석 금지**(교훈 2) |
| V5 | `SSH_ACCEPTED_COUNT ≥ 1` — **V-블록 세션 자신이 반드시 포함**(0이면 가시성 실패로 보고). 각 줄 옆에 Cursor가 "무엇을 했나(Handoff 절 / 공개된 스크립트명)" 주석. IP 마지막 옥텟은 가려도 됨 | 설명 못 하는 줄은 지우지 말고 **"설명 불가"**로 |
| V6 | backup·snapshot 실패의 `Result`/`ExecMainStatus`, watchdog의 `SubState`·`NRestarts` 그대로 | 판정은 Claude. 재시작·reset-failed 금지 |

### 4-3. L-항목 (로컬 코드·git만 — 서버 실행 0)

**L1 — `bitget.sh --cutover-check` 경로** (코드 읽기, 파일:줄로)
- (a) 그 분기가 실제 실행하는 명령(러너 `--mode cutover_check`? `--skip-telegram` 여부)
- (b) Bitget runtime lock 방식(flock fd / `flock -w` / PID 파일), 대기 vs 즉시 skip, 대기 상한
- (c) 그 모드가 쓰는 것: 텔레그램 전송 · DB insert(ops_events·실행 기록) · 파일(`parallel_run_state.json` 등)
- (d) `setsid`/백그라운드로 프로세스 그룹을 벗어나는 자식 유무
- (e) 결론 1줄: 06:49–06:51 RC=124가 "락 대기 중 종료"인지 "실행 중 종료"인지. 코드만으로 확정 불가면 **판별에 필요한 서버 로그 위치만 적고 실행하지 말 것**(다음 Handoff에서 Claude가 명령 지정)
- (f) `--start-parallel`도 같은 락 경로인지 1줄 — Phase 1 설계 입력

**L2 — pull 범위**
```bash
git log --format='%h %ad %s' --date=iso-strict c1ffe3f..8a6da21
git diff --stat c1ffe3f 8a6da21
```
파일마다 분류: 런타임(상주 서비스·cron 잡이 import) / 체크·테스트 / 문서 / 배포도구. **런타임 1건이라도 있으면 표시.**

**L3 — 출처**
- `git log --format='%h %ad' --date=iso-strict -- bitget/docs/work_phases/snapshots/cutover01_p0.sh` → `29e9c8a` 1건이어야
- 로컬 3파일 `sha256sum` + 최종 수정시각(UTC). arch/deploy 수정시각이 실행 시각(09:19 / 09:42:40) **이후**면 "현재본 ≠ 실행본 가능" 명시
- `snapshots/CAT-L-CUTOVER-01_P0_` / `_P0c_BOT2_` / `_P0c_DEPLOY_20260930.md`가 `/tmp` 출력의 **전문인지 발췌인지** 각 1줄

**L4 — 부작용 지점**
- 대상: `check_cutover_readiness`(`bitget/validation/cutover.py`), `run_architecture_checks`(`bitget/validation/architecture_checks.py`) — **c1ffe3f판과 8a6da21판 둘 다**(`git show c1ffe3f:<path>`). 09:19엔 구판, 09:42엔 신판이 돌았음
- 찾을 것: 파일 쓰기(`open(...,'w'/'a')`·`write_text`·파일로 `json.dump`) · DB(`sqlite3.connect`·`INSERT`) · 네트워크(`requests`·`ccxt`·telegram·`send_`) · `subprocess` · 호출 경로 모듈의 import 시점 부작용
- env 의존: `.env`가 필요한 키가 있는지(왜 source했는지의 답)
- 결과: 파일:줄 목록 + "순수 읽기 / 쓰기 있음" 판정 1줄

### 4-4. 증거물 보존

- 로컬 `cutover01_p0c_arch.sh`·`cutover01_p0c_deploy.sh`: **삭제·수정 금지.** L3 sha256 기록 후 **현 상태 그대로** `snapshots/`에 커밋(증거물). "재실행 금지"는 파일 본문이 아니라 05 로그에 적는다(해시 보존).
- 서버 `/tmp/cutover01_*.out`: **삭제 금지**(V3 sha256 기록). `/tmp`는 재부팅 등으로 사라질 수 있으니 V-블록을 미루지 말 것. 정리 여부는 Phase 0c 확정 때 Claude가 지정.

---

## 5. SSOT 변경 Spec — `CAT-L_인프라배포.md` 운영 규칙 보강

기존 「서버 실행 스크립트 공개 의무(2026-09-30 신설)」는 **유지**하고 바로 아래에 추가.

```markdown
## 운영 규칙 — 서버 실행 경로 (2026-10-01, 공개 의무 규칙 보강)

1. Bot-2에서 실행 가능한 것은 둘뿐:
   (a) Handoff(`CLAUDE_TO_CURSOR.md`)에 원문으로 적힌 명령·블록 — 내용 바이트 동일. 한 줄이라도 고쳤으면 고친 줄을 OUTBOX에.
   (b) 커밋·push되어 서버에 pull된 스크립트를 **서버 디스크에서** 실행 — 실행 직전 `git rev-parse HEAD` + `sha256sum <스크립트>` 출력을 OUTBOX에.
2. 로컬 미커밋 파일을 ssh로 파이프해 실행 금지 — 실행본 증명 불가, CR 혼입 실증(2026-09-30 `$'\r'`).
3. 상태를 바꾸는 명령(pull/merge · 설치기 · restart · `.env`/DB/state 파일 쓰기 · `bitget.sh --start-parallel` 등)에 `timeout`을 씌우지 않는다. 오래 걸리면 끊지 말고 원인(락 대기 등)부터 회신.
4. 서버 코드 이동은 두 경로만: 전체 배포 = `update_bitget.sh`(pre-pull `--diff-live` 내장) / 코드만 이동 = 아래 표준 pull 레시피. **맨 `git pull` 금지.**
5. 진단용 일회성 실행에서 `.env` 전체 source 금지 — 필요한 키만 grep. 러너와 같은 env 조건이 필요하면 러너 경로(`runner --mode … --skip-telegram`)를 Handoff에 명시.
6. (Claude 의무) 서버 명령이 들어간 Handoff는 요약이 아니라 **전문**을 `CLAUDE_TO_CURSOR.md`에 둔다. `Downloads/` 원문만 있는 상태 금지.
```

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

---

## 6. 디렉터 확인·결정

| # | 종류 | 내용 | Claude 권고 |
|---|---|---|---|
| D1 | 사실 확인(디렉터만 가능) | **9/30(수) 오후 3:49~3:53 KST**에 텔레그램으로 cutover·점검 결과 류 메시지가 왔는지 | 왔다면 `--cutover-check`가 실제 실행 중이었다는 확증 → 쓰기 경로 점검 필요. 안 왔다면 판별은 L1 코드로 |
| D2 | 사실 확인 | V5 결과가 오면, 목록 중 디렉터가 **직접** 접속한 세션 표시(없으면 "없음") | — |
| D3 | 결정 | `snapshots/`의 fence02/03 스크립트 **8건**(추적 6: `4A`·`4D`·`4E`·`B_resume`·`S0`·`fence03_bootstrap` / 미추적 2: `S1`·`B`) 원문·Handoff 대조 시점 — **A** Phase 1 전 / **B** Phase 1과 병행, 별도 sub-phase `CAT-L-SCRIPT-AUDIT-01` | **B.** 그 스크립트들이 만든 결과 상태(cron 28줄·마커·slice 값)는 V1·V2로 직접 확인되고, Phase 1의 서버 변경은 `parallel_run_state.json` 1파일. 단 **V5에 "설명 불가" 세션이 1건이라도 나오면 자동으로 A** |
| D4 | 승인(에스컬레이션 회신) | "동일 이슈 3회"(FENCE-02 생성기 미푸시 회귀 · A5 서버 반영 미확인 · 미공개 스크립트)의 공통 원인 = **로컬↔서버 경로가 통제·기록되지 않음.** 양쪽 책임: Claude(Handoff 전문 미기록, 검증 명령 오류 — `grep -c` 29, `pgrep` 패턴) / Cursor(미공개 실행, 확인 안 한 서술 2건) | OUTBOX 파이프라인 전면 재설계보다 **§5 규칙 6개 승인**을 권고. 다음 재발 시 재평가 |

---

## 7. 상태 전이 · 다음 단계

- **Phase 0c**: 잠정 SUB_DONE → **원문 검토 완료 · 확정 대기**. V0·V1·V3 + L2·L3·L4 기대값 일치 → Claude OK → **SUB_DONE**.
- **Phase 1**: **보류 유지.** 아래 전부 충족 시 Claude가 Phase 1 Handoff 별도 발행.
  1. Phase 0c SUB_DONE
  2. L1: 124 원인 확정 + `--start-parallel` 락 경로 → Phase 1 실행 방식(시점·대기 처리) 설계 입력
  3. V6: backup/snapshot 실패 · watchdog activating 설명 — cutover 체크가 watchdog component를 보므로, 관측 창 동안 watchdog이 불안정하면 48h 결과를 해석할 수 없음
  4. V4 OOM 0(가시성 확인 포함) · V5 세션 전부 설명됨
  5. D3 결정 · D4 승인
- **큐 순서 불변**: 11번(CAT-B 갭#4 프로덕션 백필)은 8번 완전 종결 후.
- **A5-EVENTLOG-01**: 이 Handoff 범위 밖(별도 sub-phase). V6의 서비스 기동 시각(`ActiveEnterTimestamp`)은 그때 재사용 — 이번엔 판정 안 함.

---

## 8. 출력 형식 체크

- **SSOT 변경**: `CAT-L_인프라배포.md` 운영 규칙 보강(§5, 문서만) · 05/NEXT_ACTION/00/09/NEXT_STEP 상태 갱신(§9)
- **SSOT 비변경**: 코드 0 · 서버 0(V-블록 읽기전용) · 게이트·NAV·CAT-F/G/I/N/B/D 0 · cron·slice·`.env`·`BITGET_PIPELINE_SSOT` 0 · `ENABLE_REAL_EXECUTION` OFF 유지
- **SPOT/FUT 분기**: 해당 없음 — 배포·검증 절차, `market_type` 무관(V-블록·레시피에 분기 없음)
- **인접 CAT**: CAT-A(runtime lock — L1 읽기 참조만, 설계 변경 없음. CAT-MAP never_with「CAT-A/CAT-L deploy paths 동시 설계」 해당 없음) · CAT-F(A5-EVENTLOG-01 — 범위 밖, 메모만)
- **롤백**: 이번 산출물은 문서 규칙 → 해당 커밋 revert. 09-30 pull(`8a6da21`) 되돌림은 현재 불필요 판단. V0·V1·L2에서 이상이 나와도 **Cursor는 아무것도 바꾸지 말고 보고** — 되돌림 방법은 Claude가 지정(사전 승인 없음)
- **Critical**: 해당 없음

---

## 9. 문서 갱신 (붙여넣을 문구)

**`05_진행로그.md` 최상단**
```markdown
## CAT-L-CUTOVER-01 Phase 0c — 스크립트 3건 원문 줄 단위 검토 [2026-10-01] · Claude 판정
원문: `track_b_CURSOR_TO_CLAUDE.md` 상단 3건. 위험 동작(설치기·update_bitget.sh·--start-parallel·SSOT 플래그·.env 쓰기·restart·sudo) 본문 0 — 확인.
서버 상태 변경: p0c_deploy.sh의 git pull(c1ffe3f→8a6da21) 1건. 미확정 1건: p0.sh의 `bitget.sh --cutover-check`(timeout 120 → RC 124) 내부 동작.
결함: deploy pull 전 `--diff-live` 생략(FENCE-03 Spec 4) · 해시 고정이 pull 후 · p0 `set -e`로 RC 무력화 · 짧은 해시 비교.
p0.sh 지시 여부 판정: 명령=Phase 0 Handoff Step 1–3 / 파일·timeout·.env source+직접 python·tee = Handoff 밖.
Claude 자기 정정 4건: 배포 Handoff 전문 미기록 · pre-pull 가드 누락 · Phase 0 failed unit 누락 · 124 우선순위 하향.
증거물: cutover01_p0c_arch.sh·p0c_deploy.sh 현 상태 그대로 커밋(sha256 L3), 재실행 금지. 서버 /tmp/cutover01_*.out 삭제 금지.
상태: Phase 0c 원문 검토 완료 · 확정 대기(V0–V6 · L1–L4). Phase 1 보류. 서버 실행 = CLAUDE_TO_CURSOR.md V-블록만.
```

**`NEXT_ACTION.md`** — CUTOVER 행 교체:
`| **CAT-L (공용·레인 아님)** | CAT-L-CUTOVER-01 Phase 0c | **원문 검토 완료 · 확정 대기**(V-블록/L-항목 회신) · Phase 1 보류 · 서버 실행 = Handoff V-블록만 | track_b_* · OUTBOX=track_b_CURSOR_TO_CLAUDE.md |`

기록 동기화(판정 변경 아님 — 05 로그 기준으로 맞추기): FENCE-02 행 `WAIT_CLAUDE_OK` → `SUB_DONE`(3단계 판정일 2026-10-13) · LANE_FULLBT RUN-2 행 `WAIT_CLAUDE_OK` → `SUB_DONE`.
경로 정합 1줄 회신: `track_b_NEXT_ACTION.md`(09-14 동기화에서 멈춤)와 `NEXT_ACTION.md` 중 어느 쪽이 SSOT인지. 같은 맥락으로 현재 Handoff 스택은 `CLAUDE_TO_CURSOR.md`이고 `track_b_CLAUDE_TO_CURSOR.md`는 08-20에서 멈춤 — 다음 세션 연속성 팩의 경로 표기를 이에 맞출 것.

**`00_전체현황판.md`** — "다음 Handoff" 행: `CAT-L-CUTOVER-01 Phase 0c 확정 심사(V-블록 읽기전용) · Phase 1 보류`

**`09_디렉터_쉬운요약.md`**
```markdown
🔍 지난번 보고 없이 서버에서 돌았던 점검 프로그램 3개를 Claude가 한 줄씩 전부 읽었어요.
✅ 위험한 일은 없었어요 — 설정 바꾸기, 자동매매 스위치, 프로그램 재시작, 관리자 권한 사용 모두 0.
⚠️ 고칠 점 3가지: ① 업데이트 직전에 "예약작업 안전장치가 그대로인지" 확인을 건너뜀 ② "정확히 이 버전으로 업데이트" 확인을 업데이트 뒤에 함(순서가 거꾸로) ③ 점검 하나를 2분 만에 강제로 끊었는데, 그때 그냥 기다리던 중이었는지 뭔가 하던 중이었는지 아직 몰라요.
➡️ 서버를 "보기만" 하는 확인을 한 번 요청했어요. 깨끗하게 오면 이번 단계는 확정, 그다음 "48시간 비교 관찰"을 열지 정해요. 그 전까지 서버에서는 아무것도 바꾸지 않아요.
```

**`NEXT_STEP`**
```markdown
다음 할 일: Cursor가 Claude가 준 "보기만 하는 확인 목록"(V0~V6)을 서버에서 그대로 한 번 실행하고 결과를 고치지 않고 붙여넣기 + 컴퓨터 안에서만 하는 확인 4가지(L1~L4).
디렉터: 질문 1개(9/30 오후 3:49~3:53 텔레그램 메시지 여부) · 결정 2개(D3 fence 스크립트 감사 시점, D4 규칙 승인).
```

---

## 금지 (이번)

V-블록 외 서버 명령 일체(pull · 설치기 · restart · `reset-failed` · `--start-parallel` · **`--cutover-check` 재실행** · `.env` 수정 · `sudo`) · 로컬 스크립트 파이프 실행 · 증거물(`/tmp` 3파일, 로컬 2스크립트) 삭제·수정 · fence02/03 스크립트 재실행 · `BITGET_PIPELINE_SSOT` · C-2 · MDD5% · live · `ENABLE_REAL_EXECUTION`

## 완료 정의

이 Handoff 커밋 해시 + V-블록 출력 전문 + L1–L4 + §9 문서 갱신을 `track_b_CURSOR_TO_CLAUDE.md` 상단 OUTBOX에. Claude 판정 회신 전 **Done 아님**.

## sub-phase ID

`CAT-L-CUTOVER-01` (Phase 0c 확정 심사)

---

# ARCHIVE (이전 Handoff)

# CLAUDE → CURSOR · CAT-L-CUTOVER-01 · Phase 0c (architecture_checks 갱신)

> **작성**: Claude Pro (Architect) · 2026-09-30  
> **선행**: Phase 0b 진단 완료, 4건 전부 (b) 체크 노후화로 Claude 판정.  
> **범위**: 오직 `validation/architecture_checks.py` (+ 테스트). 게이트/NAV 파일 비접촉.  
> **원문**: `Downloads/CAT-L-CUTOVER-01_Phase0c_checks_update_Handoff.md`

## 원칙
게이트 로직·NAV 계산 코드는 1바이트도 안 건드린다. 체크를 무력화하지 않는다 — 지금 구조를 정확히 검증하는 새 기준.

## Spec 1 — pipeline_structure
필수 단계 이름 **부분집합** + 길이 `>= 19` (권장). 향후 정당한 단계 추가가 또 실패로 뜨지 않게.

## Spec 2 — bitget_shell_daily_audit_guard
`[[ "$pid" -eq "$$" ]]` 대신 pgrep 가드가 daily_audit 경로에 걸려 있는지. missing vs relocated 구분.

## Spec 3 — weekly_evolution_pipeline
종단 집합 `{weekly_action_plan, weekly_executive_summary}` — 끝이 집합 중 하나이거나 집합을 포함.

## Spec 4 — portfolio_nav_risk_ssot
`portfolio_nav_snapshot`은 live_nav_manager, `max_leverage_cap`/`gross_entry_blocked`는 execution_safety를 정답으로. 위치 토큰이 아니라 **연결**(스냅샷 import/호출 + CAT-N `get_portfolio_mdd_snap_cached`).

## 테스트
각 체크 PASS(현재) + FAIL(불변식 파괴). Phase 1/`--start-parallel`/SSOT 플래그 금지.

---

# ARCHIVE · CAT-L-CUTOVER-01 · Phase 0b (진단 전용 — 수정 금지)

> **작성**: Claude Pro (Architect) · 2026-09-30
> **선행**: Phase 0 — `architecture_ok=false`, failed=[`pipeline_structure`, `bitget_shell_daily_audit_guard`, `weekly_evolution_pipeline`, `portfolio_nav_risk_ssot`]. SSOT 기록(`00_전체현황판.md`·`05_진행로그.md` 과거분)엔 `architecture_checks PASS`로 남아 있어 **회귀로 판단**.
> **CAT**: CAT-L(진단, 비접촉) · also_load: **CAT-F 🔴**(portfolio_nav_risk_ssot 관련 파일일 가능성), CAT-A(pipeline_structure 관련 가능성) — 이번 Phase는 **읽기전용 진단만, 어떤 파일도 수정하지 않는다.**
> **구현**: Cursor only.

---

## 목적
Phase 1(48h parallel) 착수 전, 4개 architecture check 실패가 (a) 최근 변경(FENCE-02 cron wrapper, A5-EVENTLOG-01 게이트 계측)으로 인한 **실제 기능 회귀**인지, 아니면 (b) 그 변경이 기능적으로는 옳은데 **체크 자체의 패턴 가정이 낡아서** 오탐하는 것인지 구분한다. 이번 Handoff는 **진단만** — 어느 쪽이든 수정은 다음 Handoff에서, 특히 (a)이고 CAT-F 관련이면 별도 Critical 검토를 거친다.

## Step 1 — 실패 원문 확보 (읽기전용)

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
python3 -c "
from bitget.validation.architecture_checks import run_architecture_checks
import json
r = run_architecture_checks()
print(json.dumps(r, indent=2, default=str))
"
```
4개 각각의 **구체적 에러 메시지/기대값 vs 실제값**(단순 True/False 말고 상세)을 OUTBOX에 그대로 첨부. 코드 수정 없음, 실행만.

## Step 2 — 각 체크가 보는 파일 확인 (읽기전용)

```bash
grep -n "pipeline_structure\|bitget_shell_daily_audit_guard\|weekly_evolution_pipeline\|portfolio_nav_risk_ssot" bitget/validation/architecture_checks.py
```
각 체크 함수가 실제로 어떤 파일·패턴을 검사하는지(예: 정규식으로 cron.d 라인 형태를 보는지, 함수 시그니처를 보는지, import 구조를 보는지) 함수 본문 요약해 회신.

## Step 3 — 최근 변경과의 상관관계 (읽기전용, git만)

```bash
# FENCE-02가 건드린 파일들과 체크 대상 파일 겹침 확인
git log --oneline --since="2026-09-26" -- bitget/deploy/generate_bitget_crontab.py bitget/deploy/install_bitget_cron.sh
git diff 1e38166..002c612 -- bitget/deploy/ | head -100

# A5-EVENTLOG-01이 건드린 파일들
git log --oneline --grep="A5-EVENTLOG\|EVENTLOG-01" --all
git show --stat <A5-EVENTLOG-01 커밋 해시>   # 해시는 위 log 결과에서
```
Step 2에서 확인한 "체크가 보는 파일"과 이 diff들이 겹치는지 표로 정리:

| 체크 | 관련 커밋(추정) | 겹침 여부 | 근거 |
|---|---|---|---|
| `bitget_shell_daily_audit_guard` | FENCE-02(35f9da9/002c612)? | | |
| `weekly_evolution_pipeline` | FENCE-02? | | |
| `portfolio_nav_risk_ssot` | A5-EVENTLOG-01? | | |
| `pipeline_structure` | 미상 | | |

## Step 4 — (a)/(b) 분류 (Cursor 소견, 최종 판단은 Claude)

Step 1~3 근거로 각 체크에 대해:
- **(a) 실제 회귀**: 변경이 실제로 그 체크가 보장하려던 불변식을 깼다
- **(b) 체크 노후화**: 변경은 기능적으로 옳고, 체크의 패턴 가정만 새 형태(wrapper, 계측 삽입)를 못 알아본다

소견만 제시. **이번 Handoff에서 어느 쪽이든 코드 수정 금지.**

## Step 5 — `bitget.sh --cutover-check` timeout (참고, 낮은 우선순위)
Step 1의 직접 python 호출과 `bitget.sh --cutover-check`(timeout 124) 중 어느 쪽이 이번 JSON의 실제 출처인지 1줄 확인.

## 금지
- 4개 체크 중 **어느 것도 이번에 고치지 않음**(진단 전용)
- `portfolio_nav_risk_ssot` 관련 파일(execution_safety.py/tail_risk_gate.py 등) **읽기만**, 수정 금지 — (a)로 판명되면 별도 Critical Handoff
- `--start-parallel` · `BITGET_PIPELINE_SSOT` 변경
- CAT-A/CAT-F 로직 변경 · 슬라이스 수치 변경 · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## 완료 정의
Step 1~5 원문 + (a)/(b) 표 확보. `05_진행로그.md`·`CURSOR_TO_CLAUDE.md`·`NEXT_ACTION.md` 갱신(진단 결과만, 수정 없음이므로 Done/SUB_DONE 표기 없음 — "진단 완료, Claude 판정 대기"로 표기).

## 다음 (이번 범위 아님, 예고만)
- 전부 (b)면: 체크 패턴 갱신 Handoff(낮은 위험) → 재확인 → Phase 1
- `portfolio_nav_risk_ssot`가 (a)면: CAT-F 전용 세션으로 분리, Critical 검토 후 수정 → 재확인 → Phase 1. **이 경우 cutover는 그 수정이 끝날 때까지 전체 보류.**

## sub-phase ID
`CAT-L-CUTOVER-01` (Phase 0b)

---

# CLAUDE → CURSOR · CAT-L-CUTOVER-01 · Phase 0 (사전점검, 48h parallel 착수 전)

> **작성**: Claude Pro (Architect) · 2026-09-30
> **CAT**: CAT-L 🟡 Medium(운영 플래그) · also_load: CAT-A(`BITGET_PIPELINE_SSOT=1` cutover 조건, 읽기전용 참조만) · CAT-MAP §3(never_with: CAT-A/CAT-L deploy paths 동시 설계 — 이번은 설계 아닌 기존 플래그·기존 검증 하네스 운영이라 해당 없음)
> **근거**: `HIST_06/07/08/09_...cutover...md` — 프로젝트 자체가 "리스크: 높음 — 잘못된 cutover 시 이중 실행·텔레그램 폭주"로 명시. 알려진 안티패턴: "`BITGET_PIPELINE_SSOT=1`만 설정하고 완료로 간주 — parallel 48h·async_telegram·서버 프로세스 미검증". 이번 Phase 0는 그 안티패턴을 피하기 위한 순서.
> **구현**: 서버 접속 가능한 주체. 이번 Phase는 **읽기전용 진단만** — `--start-parallel`·`.env` 변경·`BITGET_PIPELINE_SSOT` 값 변경 전부 금지.

---

## 목적
09-26 마지막 실측 이후(FENCE-02/03, A5-EVENTLOG-01, RUN-2 등 여러 변경 발생) cutover 전제조건이 여전히 유효한지 재확인 없이 48h parallel을 시작하지 않는다. HIST 문서의 표준 절차는 (1) `--cutover-check`로 현재 상태 확인 (2) `--start-parallel`로 48h 병렬 관측 시작 (3) 48h 후 재확인 (4) 통과 시에만 `BITGET_PIPELINE_SSOT=1`. Phase 0은 (1)만 한다.

## Step 1 — 현재 cutover 상태 (읽기전용)

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
./bitget/deploy/bitget.sh --cutover-check
```
전체 JSON(`passed`·`checks={...}`·`architecture_passed`·`failed=[...]`) 원문 그대로 OUTBOX 첨부. `architecture_ok`뿐 아니라 다른 sub-check가 있으면 전부.

## Step 2 — HIST가 요구하는 추가 env 확인

`.env` 또는 실행 환경에서:
```bash
grep -E '^BITGET_PIPELINE_SSOT|^BITGET_ASYNC_TELEGRAM|^BITGET_WATCHDOG_HEARTBEAT_COMPONENT' .env 2>/dev/null || echo 'NOT_SET (해당 줄 없음)'
```
`BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot`는 이전 세션에서 확인된 적이 없다 — 설정돼 있는지, 없다면 무슨 영향이 있는지(watchdog 하트비트 컴포넌트 누락이 cutover-check 실패 사유가 되는지) 회신.

## Step 3 — 레거시 프로세스 재확인 (09-26 이후 재검증)

```bash
pgrep -f bitget.main; pgrep -f factory_launcher
systemctl list-units --type=service | grep -i bitget
```
09-26엔 없었음(레거시 미실행) — 그 사이 FENCE-02/03·A5-EVENTLOG 작업 중 우연히 뭔가 켜졌을 가능성 배제 목적.

## Step 4 — CAT-L-FENCE-02/03과의 상호작용 확인 (읽기전용)

cutover 48h parallel이 시작되면 파이프라인 두 경로(레거시 판정용 vs SSOT)가 동시에 관측 대상 스캔을 늘릴 수 있는지 확인:
- `--start-parallel`이 (a) 28줄 HEAVY 직행 cron과 별개의 새 프로세스를 띄우는지, 아니면 기존 스캔 결과를 재사용/비교만 하는지 1줄 회신 (메모리 슬라이스 부하 재평가 필요 여부 판단용)

## 판정 (Claude, Step 1~4 회신 후)
- `architecture_ok=True` **AND** Step 2/3 이상 없음 **AND** Step 4가 슬라이스 부하를 유의미하게 늘리지 않음 → Phase 1(`--start-parallel`, 48h 관측) Handoff 발행
- 하나라도 미충족 → 그 항목만 먼저 해소, Phase 1 보류

## 금지 (이번 Phase)
`--start-parallel` 실행 · `.env`의 `BITGET_PIPELINE_SSOT` 변경 · CAT-A 로직 변경 · 슬라이스 수치 변경 · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## 완료 정의
Step 1~4 원문 확보, Claude 판정 회신. `05_진행로그.md`·`00_전체현황판.md`·`CURSOR_TO_CLAUDE.md`·`NEXT_ACTION.md` 갱신(Phase 0 결과만, Done 아님).

## sub-phase ID
`CAT-L-CUTOVER-01` (Phase 0)

---

# CLAUDE → CURSOR · CAT-L-FENCE-03 (drift guard) — 착수 승인

> **작성**: Claude Pro (Architect) · 2026-09-30
> **디렉터 승인**: 2026-09-28 "drift guard 진행" · 2026-09-30 "권장 순서부터 순서대로 가자"(12 우선)
> **선행 상태**: CAT-L-FENCE-02 전체 SUB_DONE. 라이브 origin=`19d3858`, `FENCE_STATUS=FENCE_OK`, 실스캔 cgroup=`bitget-cron-heavy.slice` 확인됨. 3단계 판정 2026-10-13.
> **CAT**: CAT-L 🟡 Medium · also_load: CAT-A(비접촉)
> **구현**: Cursor only. CAT-A 로직·슬라이스 수치·(b)/(c) 줄 의미 변경 금지.

---

## 이게 왜 필요한지 — 방금 겪은 사고와의 관계

CAT-L-FENCE-02 Step 4에서 실제로 벌어진 일: Step 2는 **생성기 없이 라이브 cron.d를 손으로 고친 것**이었고, 그 뒤 FENCE-02 생성기 코드는 로컬에만 있고 origin에 없는 채로 무언가(설치기 또는 유사 경로)가 **구코드로 재생성**해 그 수동 wrapper를 조용히 지웠다. 이번 설계(마커/지문)가 그때 이미 있었다면: Step 2 직후 라이브는 `UNMARKED`(생성기로 안 만들어졌으니), 이후 설치 시도는 "생성 결과(wrapper 없음, 구코드) ≠ 라이브(wrapper 있음)" → **UNMARKED+diff = 차단**되어 사람이 먼저 확인해야 했을 것이다. 즉 이번 회귀는 FENCE-03이 정확히 막도록 설계된 그 패턴이다. 아래 스펙은 그때 정한 원안을 그대로 쓰되, 카운트 방식은 이번 사고에서 확정된 규칙(주석 제외, python 게이트 단일 기준)을 반영한다.

## 설계 원칙
diff만으로는 "손으로 고침"과 "저장소가 정당하게 바뀜"을 구분 못 한다. **출처 기반**: 설치기가 "내가 마지막으로 쓴 내용의 지문"을 파일에 남기고, 다음 설치 때 지금 파일이 그 지문과 다르면 = 누군가 손댄 것.

## Spec 1 — 마커(지문)
- 생성기가 만드는 cron.d 헤더에 마커 2줄(주석): 생성기 식별 + `body-sha256`.
- 해시 대상 = **동작에 영향 주는 줄만**: 주석·공백 제외한 모든 줄(`SHELL=`/`PATH=`/`MAILTO=`/`CRON_TZ=` 포함). 마커 줄 자체는 제외. 줄 끝 공백 정규화. 결정적(재생성해도 동일).
- **이번 사고에서 확정된 규칙 반영**: 해시·모든 "wrapper 줄 수" 계산은 `grep -v '^\s*#'`로 주석을 제외한 뒤 계산. 셸 `grep -c systemd-run`을 그대로 판정에 쓰지 않는다(헤더 주석에 "systemd-run"이 언급되면 오탐 — 실제로 한 번 겪음).
- 마커는 주석이라 cron 동작에 영향 없음.

## Spec 2 — 상태 판정 함수 (생성기/설치기 공용)
| 상태 | 조건 |
|---|---|
| `ABSENT` | 라이브 파일 없음 |
| `PRISTINE` | 마커 있음 + 현재 본문 해시 == 마커 해시 |
| `DRIFTED` | 마커 있음 + 해시 불일치(수동 편집) |
| `UNMARKED` | 마커 없음 |

## Spec 3 — 설치기 동작
| 상태 | 동작 |
|---|---|
| `ABSENT` | 정상 설치 |
| `PRISTINE` | 백업 후 덮어쓰기, 변경 요약(추가/삭제/변경 줄 수) 출력, 막지 않음(정당한 저장소 변경 통과) |
| `UNMARKED` | 생성 결과 == 라이브(헤더 제외)면 마커만 채택(기능 변화 0). 다르면 `DRIFTED`와 동일 차단 — **이번 사고를 막았을 경로** |
| `DRIFTED` | **차단**(구분되는 종료 코드, 예: 3). diff 출력 + 해결 2가지: ① 수동 편집을 생성기에 반영·커밋 후 재실행 ② `--force-overwrite-drift`(명시 플래그, 기본 아님)로 폐기 |
- 모든 덮어쓰기 전 `/etc/cron.d` 밖(예: `/var/backups/bitget-cron/…<UTC>`)에 백업.
- fail-closed: 라이브 읽기/해시 계산 오류 시 진행하지 않고 차단(`--force-overwrite-drift`로만 우회).

## Spec 4 — `generate_bitget_crontab.py --diff-live` (읽기전용, sudo 불필요)
출력: (1) 상태(4종) (2) 생성 결과 vs 라이브 unified diff(마커/타임스탬프 제외) (3) **wrapped 줄 수**(주석 제외 카운트, `LIVE_WRAPPED_COUNT`/`GEN_WRAPPED_COUNT` 형태로 사람이 바로 읽게) (4) 슬라이스 보고 — `bitget-cron-heavy.slice` 파일 vs 템플릿, `systemctl show` 유효값(MemoryHigh/Max) vs 템플릿 값(런타임 오버라이드 감지).
종료 코드 구분(예: 0=동일, 10=PRISTINE+diff, 20=DRIFTED, 30=UNMARKED+diff) — 정확한 값은 Cursor 확정 후 문서화.
슬라이스 불일치는 경고만(차단 아님): 설치기가 덮어쓰되, 덮어쓰기 전 경고 남김.
**인자 없이 실행해도 `LIVE=/etc/cron.d/dual-screener-bitget`을 기본값으로 사용**(이번 Step 3 B 재개 때 무인자 실행이 안 돼서 매번 경로를 붙였음 — 이번에 고정).

## Spec 5 — `update_bitget.sh` 사전 점검
- 변경 작업(pull/재시작 등) **이전**에 `--diff-live` 상태 확인. `DRIFTED`(또는 `UNMARKED`+diff)면 아무것도 바꾸기 전에 중단.
- 현재 `update_bitget.sh`가 설치기 실패를 어떻게 다루는지(`set -e` 등) 먼저 확인해 OUTBOX 회신.

## Spec 6 — 부트스트랩 (서버, 코드 반영 후 1회)
서버는 현재 `PRISTINE`에 가까운 상태일 수 있음(FENCE-02 Step 4E 설치가 이미 이번 생성기로 이뤄짐) — 정확한 상태는 부트스트랩 전 `--diff-live`로 먼저 확인. `ABSENT`/`UNMARKED`로 나오면 절차대로, 이미 그 생성기 출력과 라이브가 같다면 마커만 추가(기능 변화 0).

## 테스트 (임시 디렉터리, root 불필요)
1. 마커 존재, 재생성해도 해시 동일(주석 변경엔 불변)
2. 상태 4종 판정
3. 동작 줄 1개 수동 변경 → `DRIFTED`. 헤더 주석에 "systemd-run" 문자열이 있어도 wrapped count·해시에 영향 없음(**이번 사고 재발 방지 회귀 테스트**)
4. 정당한 저장소 변경(생성기 상수 변경) + `PRISTINE` → 차단 없이 덮어쓰기
5. `DRIFTED` → 차단·종료 코드, `--force-overwrite-drift` → 진행+백업
6. `UNMARKED` + 생성==라이브 → 마커 채택, 동작 줄 불변
7. `UNMARKED` + 생성≠라이브(**Step 4가 실제로 겪은 시나리오 재현**: 구코드 생성 결과 vs Step2식 수동 wrapper 라이브) → 차단
8. 라이브 읽기 불가 → fail-closed
9. `--diff-live` 무인자 실행 시 기본 LIVE 경로 사용
10. `--diff-live` 종료 코드 매트릭스
11. 기존 FENCE-02 테스트(22개) 회귀 0
운영 파일에 가짜 드리프트를 만들어 시험하지 말 것. 시뮬레이션은 임시 복사본에서만.

## 인접 CAT
CAT-A 비접촉. CAT-N/F 비접촉.

## 문서 (Cursor)
`CAT-L_인프라배포.md`: 마커 방식·상태 4종·차단 시 해결법 2가지·`--diff-live` 사용법(무인자 기본 동작 포함) 각 1~2줄. 이번 사고 한 줄 각주: "이 장치는 2026-09-29 cron 펜스 소실(FENCE-02)과 같은 패턴을 차단 시점에 잡기 위함."

## 롤백
커밋 revert. 라이브 마커는 주석이라 남아 있어도 무해. 백업 디렉터리 유지.

## 완료 정의
코드+테스트 all pass(회귀 포함) · 서버 부트스트랩 전후 동작 줄 해시 동일 캡처 · `05`·`00`·`CURSOR_TO_CLAUDE`·`NEXT_ACTION`(→`WAIT_CLAUDE_OK`) 갱신 · 「로컬 구조 스냅샷」에 종료 코드 표·백업 경로 명시.

## 금지
슬라이스 수치 변경 · CAT-A 로직 변경 · (b)/(c) 의미 변경 · 운영 cron.d에 가짜 드리프트 시험 · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## sub-phase ID
`CAT-L-FENCE-03`

---

# CLAUDE → CURSOR · CAT-L-FENCE-02 · Step 3 B (재개) + Step 4 종결 기록

> **작성**: Claude Pro (Architect) · 2026-09-29
> **선행**: Step 4(펜스 소실→복구) Claude OK. 라이브 `FENCE_STATUS=FENCE_OK`, 28 wrapped, 코드는 origin(002c612)에 있고 서버가 그 커밋 기준. 원 Step 3 B(실스캔 캡처)는 SSH 불가로 미실행 상태였다가 이번에 대상(펜스)이 다시 유효해짐.
> **CAT**: CAT-L 🟡 Medium · also_load: CAT-A(읽기전용)
> **구현**: 서버 접속 가능한 주체(디렉터/Cursor). CAT-A 비접촉. 판정 원칙: **`--fence-check`(python 게이트)만 자동 판정 기준으로 사용, 셸 grep은 보조 참고만.**

---

## A. Step 4 종결 기록 (그대로 옮겨 적기)

`05_진행로그.md`의 기존 "REGRESSED" 절 **바로 아래에 이어서** 추가(새 절 아님):

```markdown
### Step 4 종결 — Claude OK [2026-09-29]

4A(dirty 무관 확인)·4B(커밋 35f9da9, push 1e38166..002c612, 서버 pull 확인)·4C(게이트 3분류 FENCE_OK/FENCE_MISSING/DRIFTED)·4D(재검증 DRIFTED 정확)·4E(설치, 최종 `FENCE_STATUS=FENCE_OK`·`LIVE_WRAPPED_COUNT=28`·`JOBS_SAME=yes`·slice 값 정상) 전부 확인.

**Step 4E 진행 중 1회 오작동**: Claude가 준 확인 명령(`grep -c systemd-run`)이 헤더 주석을 포함 카운트해 29로 나와, 이미 `FENCE_OK`였던 1차 설치를 불필요하게 원복(P0 스냅샷 복원+slice 삭제)시킴. Cursor가 즉시 재판정(python 게이트 기준)해 재설치, 같은 세션 내 복구. 근본 원인은 Claude의 확인 스크립트, 설치 로직 아님. **이후 모든 wrapper 개수 판정은 `--fence-check` 단일 기준으로 통일**, 셸 grep 쓸 경우 `grep -v '^\s*#' … | grep -c systemd-run`(주석 제외)만 사용.

Pre-flight 테스트: 지정 3파일 22 passed, fail 0.
```

## B. 3단계(효과 검증) 판정일 재설정

`06_검증체크리스트_및_실패기록.md`의 `CAT-L-FENCE-02 cron 슬라이스 펜스` 행 판정일을 `2026-10-11` → **`2026-10-13`(2026-09-29부터 2주)**로 정정. 사유: 09-27~09-29 사이 펜스가 실제로 꺼져 있던 구간이 있었음이 확인됨(원인 불명 + Step 4E 왕복 포함) — 그 구간은 관측 자격 없음. 시작점을 origin에 코드가 고정되고 라이브가 `FENCE_OK`로 확인된 2026-09-29로 재설정.

## C. Step 3 B — 실스캔 소급 캡처 (당초 스펙 유지, 판정 기준만 정정)

대상은 지금 실행 중이거나 최근 완주한 (a) 스캔. `Handoff CAT-L-FENCE-02 Step 3`의 원 8항목 + 이전 잔여 Handoff의 9번(글로벌 OOM 소급) 그대로, 아래 **정정 1건**만 반영:

> 원 8항목 중 "7. cron.d wrapper 줄(28 기대)" 판정은 `grep -c systemd-run`(원문 그대로) 대신 **`--fence-check`의 `LIVE_WRAPPED_COUNT`**를 1차 기준으로 쓰고, 보조로 `grep -v '^\s*#' /etc/cron.d/dual-screener-bitget | grep -c systemd-run`(주석 제외)를 병기.

나머지 1~6·8~9 항목은 이전 Handoff(`CAT-L-FENCE-02_server_blocks_Claude.md`의 B 블록) 그대로 유효. 그 블록을 그대로 재실행:

```bash
bash <<'B' 2>&1 | tee /tmp/fence02_B.out
# CAT-L-FENCE-02 · Step 3 B (재개, 2026-09-29) — 읽기전용
set -u
INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
SINCE='2026-09-29 00:00'
date -u
echo '--- 0. Step 2 첫 스캔 캡처(참고, TIMEOUT 가능성 있음) ---'
cat /tmp/fence02_first_scan.cap 2>/dev/null || echo 'NO_FIRST_SCAN_CAP'
echo '--- 1. 실행 중 스캔 cgroup ---'
ps -eo pid,user,etime,cgroup:90,cmd | grep -E 'scan_|daily_audit|weekly_evolution|--scan-|systemd-run' | grep -v grep || echo 'NO_SCAN_RUNNING_NOW'
echo '--- 6. slice / 부모 slice ---'
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax -p MemoryCurrent -p MemoryAccounting
systemctl show bitget.slice bitget-cron.slice -p MemoryHigh -p MemoryMax -p MemoryAccounting
echo '--- 5. 실행 중 scope (CPUQuota) ---'
for u in $(systemctl list-units --type=scope --no-legend | awk '/run-/{print $1}'); do
  echo "[$u]"; systemctl show "$u" -p Slice -p CPUQuotaPerSecUSec -p MemoryHigh -p MemoryMax
done
echo '--- 3. environ ---'
for p in $(pgrep -u ubuntu -f -- '--scan-' | head -3); do
  echo "[pid=$p] $(ps -o user=,cgroup= -p "$p")"
  tr '\0' '\n' <"/proc/$p/environ" 2>/dev/null | grep -E '^(HOME|USER|LOGNAME|PATH|PWD)='
done
echo '--- 4. 로그 소유권 ---'
TZ=UTC find "$INSTALL_ROOT" -name '*.log' -newermt "$SINCE" -printf '%TY-%Tm-%Td %TH:%TM %u:%g %p\n' 2>/dev/null | head -80
echo '--- 2. cron / systemd-run ---'
journalctl -u cron -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_CRON_OK' || echo 'JOURNAL_CRON_UNREADABLE'
journalctl -u cron --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'error|failed' | tail -40 || true
echo '--- 9. 커널 OOM 소급 ---'
journalctl -k -n1 --no-pager >/dev/null 2>&1 && echo 'JOURNAL_K_OK' || echo 'JOURNAL_K_UNREADABLE'
journalctl -k --utc --since "$SINCE" --no-pager 2>/dev/null | grep -Ei 'out of memory|oom-kill|oom_reaper|killed process' || echo 'OOM_GREP_EMPTY'
echo '--- 8. LIFECAP ---'
grep -RniE 'lifecap skip|LIFECAP WOULD_KILL|LIFECAP ENFORCE' "$INSTALL_ROOT/bitget" --include='*.log' 2>/dev/null | tail -20 || true
echo '--- 7. wrapper 개수 (1차 python 게이트, 2차 grep 보조) ---'
python3 "$INSTALL_ROOT/bitget/deploy/generate_bitget_crontab.py" --fence-check
grep -v '^\s*#' /etc/cron.d/dual-screener-bitget | grep -c systemd-run
B
```

## 인접 CAT
CAT-A 비접촉(8번 LIFECAP은 로그 확인만).

## 완료 정의
- B 9항목 캡처(미해당은 사유 명시)
- A/B 기록 반영: `05_진행로그.md` · `06_검증체크리스트_및_실패기록.md`(판정일 2026-10-13) · `00_전체현황판.md` · `CURSOR_TO_CLAUDE.md` · `NEXT_ACTION.md`
- 통과 시 CAT-L-FENCE-02 전체 SUB_DONE(1~2단계), 3단계는 2026-10-13

## 롤백
이 Step은 읽기전용 — 롤백 대상 없음.

## 금지
CAT-A 로직 변경 · 슬라이스 수치 변경(3-way 재관측 전 고정) · (b)/(c) 변경 · grep을 자동 판정 기준으로 사용 · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## sub-phase ID
`CAT-L-FENCE-02` (Step 3 B, 재개)

---

# CLAUDE → CURSOR · CAT-L-FENCE-02 · Step 4E (설치 허용)

> **작성**: Claude Pro (Architect) · 2026-09-29
> **Claude 허용**: 2026-09-29 — Step 4A(dirty 무관 확인) · 4B(커밋 35f9da9, 푸시 1e38166..002c612) · 4C(게이트 3분류) · 4D(서버 pull 확인, WRAPPED_COUNT=28 vs LIVE=0, FENCE_STATUS=DRIFTED — 설치 전 상태로 정확) 전부 검증 완료.
> **CAT**: CAT-L 🔴(현재 무방비 상태의 복구) · 신규 Ask 아님, 기승인(옵션 A, 2026-09-27) 범위 내 복구 실행
> **구현**: Cursor + 서버 실행 주체. CAT-A 비접촉.

---

## Pre-flight (설치 직전, 순서 고정)

1. **전체 테스트 재확인**: Step 3 A 원 스위트(`test_cat_l_fence02_crontab.py` `test_bitget_staggered_schedule.py` `test_cli_logging_bitget.py`, 21개) + Step 4C 신규(fence-check 3분류, 6개)를 **이번 커밋(002c612) 기준으로 한 번에 재실행**, 전부 pass 원문 OUTBOX 첨부. 하나라도 fail이면 설치 중단, 결과만 회신.
2. **직전 상태 재확인**: `--fence-check` 1회 더 실행 → `WRAPPED_COUNT=28` · `LIVE_WRAPPED_COUNT=0` · `FENCE_STATUS=DRIFTED` 재확인(4D와 동일해야 함 — 그 사이 아무도 안 건드렸다는 뜻).

## 설치 (Pre-flight 통과 후에만)

**`install_bitget_cron.sh`만 직접 실행** — 이번은 좁은 긴급 복구이므로 `update_bitget.sh`(다른 배포 단계도 포함) 대신 설치기 단독 실행을 우선한다. `update_bitget.sh` 경유가 꼭 필요하면 그 사유를 먼저 회신.

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
sudo bash bitget/deploy/install_bitget_cron.sh
sudo systemctl daemon-reload
```

## 설치 직후 즉시 확인 (지연 없이, 같은 세션)

```bash
grep -c systemd-run /etc/cron.d/dual-screener-bitget            # 28 기대
python3 bitget/deploy/generate_bitget_crontab.py --fence-check  # FENCE_STATUS=FENCE_OK 기대
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax
```

- 28 아니거나 `FENCE_OK`가 아니면 **즉시** `snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron`으로 원복 후 Claude에 보고, 원인 조사 전까지 재시도 금지.
- 통과하면 OUTBOX에 위 세 출력 원문 첨부.

## 후속 (이번 Step 완료 조건은 아님, 참고)
- Step 3 B(실스캔 소급 캡처, cgroup·environ·로그 소유권·CPUQuota·LIFECAP)는 이번 설치로 대상이 다시 생기므로 다음 세션에서 재개.
- 3-way 겹침 실측 2~3회 후 슬라이스 수치 재확정 — 여전히 대기.
- FENCE-03(drift guard)은 이번 사고로 얻은 교훈(WRAPPED_COUNT류 이중 검증) 반영해 설계 갱신 후 별도 착수.

## 롤백
`snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron` → `/etc/cron.d/dual-screener-bitget` + `sudo rm /etc/systemd/system/bitget-cron-heavy.slice && daemon-reload`.

## 완료 정의
Pre-flight 전체 pass 원문 · 설치 · 즉시 확인 3항목 통과 · `05_진행로그.md`(기존 "REGRESSED" 절에 이어서 기록, 새 절 아님) · `00_전체현황판.md` · `CURSOR_TO_CLAUDE.md` · `NEXT_ACTION.md` 갱신.

## 금지
Pre-flight fail 시 설치 강행 · `update_bitget.sh` 임의 대체 없이 사유 없이 실행 · CAT-A 로직 변경 · 슬라이스 수치 변경 · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## sub-phase ID
`CAT-L-FENCE-02` (Step 4E)

---

## 기록 (이 파일 내, 05_진행로그 이어쓰기용)

```markdown
Step 4E 허용: 2026-09-29 — 4A~4D 검증 완료(dirty 무관·커밋 35f9da9/002c612 pull 확인·게이트 3분류 정상·DRIFTED 정확). Pre-flight(21+6 테스트 재확인, 직전 상태 재확인) 통과 후 `install_bitget_cron.sh` 단독 실행, 즉시 28줄+FENCE_OK 확인이 완료 조건.
```

---

# CLAUDE → CURSOR · CAT-L-FENCE-02 · Step 4 (긴급 — 펜스 소실 복구)

> **작성**: Claude Pro (Architect) · 2026-09-29
> **선행**: Step 3 S0(확인본) 서버 실행 결과 — `S2_EMPTY_DIFF=yes`지만 **생성기·라이브 둘 다 wrapper 0줄**로 일치한 것. 즉 진짜 비교 대상이 없었다. `GIT_CLEAN=no`(deploy 10파일 dirty). 서버 HEAD=1e38166, FENCE-02 생성기 커밋이 origin에 없음.
> **CAT**: CAT-L 🔴 (현재 무방비 상태 — Critical 취급, 단 신규 Ask 아님. FENCE-02/옵션 A는 이미 승인됨. 이번은 그 복구)
> **구현**: Cursor only. CAT-A 비접촉.

---

## 지금 상태 (1줄)
**cron 펜스가 꺼져 있습니다.** `scan_spot_master`·`scan_futures_shadow`가 지금 `cron.service`에서 돕니다(3회 OOM 때와 동일 무방비). 다만 `oom_kill=0`·커널 OOM grep 공백이라 활성 사고는 아닙니다 — 서두르되 검증 없이 되돌리지 않습니다.

## 왜 이렇게 됐는지 (Cursor 데이터 기준 추정, 확정 아님)
Step 3 A(생성기·설치기 영속화, 21 tests passed)는 origin에 **푸시되지 않았습니다.** 서버가 그 사이 어떤 경로로든(설치기·`update_bitget.sh`·또는 다른 절차) cron.d를 재생성했다면, 서버가 받는 코드는 여전히 FENCE-02 이전(wrapper 없음) 버전이라 결과적으로 펜스가 사라집니다. **이건 추정입니다 — Step 4A에서 실제 경로를 확인하기 전까지 다른 원인(수동 롤백 등)도 배제하지 않습니다.**

## 지금 하지 말 것 (Step 4A 전)
- `git checkout` / `git reset --hard` / `git clean` — dirty 10파일에 무엇이 들어있는지 모릅니다. FENCE-02 작업의 흔적일 수 있습니다. **읽기만.**
- 설치기 · `update_bitget.sh` 재실행

---

## Step 4A — dirty 10파일 진단 (읽기전용, 안전한 곳에 백업)

```bash
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
mkdir -p /tmp/fence02_dirty_backup
git status --short --untracked-files=no
git diff > /tmp/fence02_dirty_backup/dirty_$(date -u +%Y%m%d%H%M%S).diff
wc -l /tmp/fence02_dirty_backup/dirty_*.diff
git diff --stat
```

이 diff 원문을 OUTBOX에 그대로 첨부. 판단 기준:
- diff에 `systemd-run`/`bitget-cron-heavy.slice`/`_HEAVY_PREFIXES` 관련 내용이 있으면 → **이게 FENCE-02 Step 3 A 작업 그 자체일 가능성** → Step 4B에서 이걸 커밋
- 무관한 내용이면 → 왜 dirty한지 별도 확인(누가 언제 수정했는지 `git log -1 -- <파일>` 등), 이번 Handoff 범위에서 판단 보류 가능(디렉터에 별도 보고)

## Step 4B — FENCE-02 코드 소재 확인 + origin 반영

1. FENCE-02 Step 3 A 코드(생성기 root+wrapper 로직, slice 파일, 설치기 수정, 21개 테스트)가 **어느 워크스페이스/브랜치**에 있는지 보고(Cursor의 로컬 개발 환경, 아니면 Step 4A의 dirty 파일 자체인지)
2. 그 코드 기준으로 로컬에서 21개 테스트 전부 다시 통과 확인
3. `git add`(대상 파일만, dirty 10개 중 무관한 것 제외) → 커밋 → **`git push`**
4. 커밋 해시를 OUTBOX에 명시

## Step 4C — S0 게이트 결함 수정 (이번 사고의 근본 원인)

기존 S0는 "diff가 비었나"만 봤습니다. **둘 다 wrapper 없이 일치해도 PASS로 나온다는 결함**이 이번에 실제로 발생했습니다. `generate_bitget_crontab.py` 검증 스크립트(또는 별도 체크)에 다음을 추가:

```
WRAPPED_COUNT = 생성 결과에서 systemd-run 줄 수
EXPECTED_WRAPPED = 28  # Phase 0 (a) 분류 수, _HEAVY_PREFIXES 기준으로 동적 계산 가능하면 더 좋음
```

판정을 아래로 변경:
| 상태 | 조건 |
|---|---|
| `FENCE_OK` | diff 공집합 **AND** `WRAPPED_COUNT == EXPECTED_WRAPPED`(28) |
| `FENCE_MISSING` | diff 공집합 **AND** `WRAPPED_COUNT == 0` — **이번에 실제 발생한 케이스**, 이전엔 이것도 "PASS"로 잘못 표기됨 |
| `DRIFTED` | diff 비공집합 |

이 로직은 이번 Step 4에서 임시 스크립트로만 써도 되고, FENCE-03(drift guard)의 상태 판정과 합치면 더 좋음 — 합칠지는 Cursor 판단, 이번 Step 4는 최소 `FENCE_OK`/`FENCE_MISSING` 구분만 있으면 충분.

## Step 4D — 서버 재검증

```bash
# S1: pull (Step 4B 커밋이 origin에 있어야 함)
cd "${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
git log -1 --format='%h %ad %s' --date=iso   # before
git pull --ff-only
git log -1 --format='%h %ad %s' --date=iso   # after — Step 4B 커밋 해시와 일치해야 함
```
이후 Step 4C가 반영된 S0을 재실행. 이번엔 **`FENCE_OK`가 나와야 정상**(생성기가 이제 28줄 wrapped를 만들고, 라이브는 여전히 0줄이라 diff는 **비공집합**이 정상 — 즉 이번엔 diff가 있어야 펜스를 복원할 이유가 있는 것). OUTBOX에 새 WRAPPED_COUNT·diff 원문 첨부.

## Step 4E — 설치 (재검증 통과 후에만, Claude 확인 후)

Step 4D가 "생성기=28줄 wrapped, 라이브=0줄"을 확인하면 Claude가 S3(설치기 또는 `update_bitget.sh`) 실행을 허용. 실행 **직후** 다음을 즉시 재확인해 OUTBOX 첨부(이번엔 지연 없이):

```bash
grep -c systemd-run /etc/cron.d/dual-screener-bitget   # 28 기대
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax
```
28이 아니면 즉시 `snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron`으로 원복하고 중단, Claude에 보고.

## 디렉터 결정 필요 — 임시 조치 여부

Step 4A~E는 순서대로 하면 며칠 걸릴 수 있습니다(제대로 하려면). 그동안 프로덕션은 무방비입니다(3-way 겹침 월 1회 안팎, 지금 당장 사고는 아님). 두 선택지:

- **(권장) 그대로 Step 4 순서대로** — 근본 원인(git 미푸시)까지 고치고 감. 조금 느림.
- **(대안) Step 2와 동일한 수동 wrapper를 지금 바로 재적용**(Step 2 때 썼던 명령 그대로) — 빠르게 방어선만 복구, 단 이번 사고와 똑같이 "라이브에만 있고 저장소엔 없는" 상태를 또 만드는 것이라 **임시**로만 쓰고 Step 4B 커밋 완료 즉시 정식 배포로 교체 필요. 쓸 경우 `snapshots/`에 재적용 시각·명령 기록 필수.

디렉터가 정하지 않으면 기본값은 **권장(그대로 진행)**입니다.

## 인접 CAT
CAT-A 비접촉.

## 롤백
Step 4B 커밋: revert. Step 4E 설치: `snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron` 원복.

## 완료 정의
- Step 4A diff 원문 확보(데이터 손실 없음 확인)
- Step 4B 커밋+푸시, 해시 기록
- Step 4C 게이트 결함 수정(코드 또는 최소 스크립트)
- Step 4D 재검증 `FENCE_OK` 도달 경로 확인(생성기=28 wrapped)
- Step 4E 설치 후 즉시 28줄 확인
- `05_진행로그.md`(이번 회귀를 별도 절로 기록, 기존 Step 3 절과 병합하지 말 것) · `00_전체현황판.md` · `CURSOR_TO_CLAUDE.md` · `NEXT_ACTION.md` 갱신
- 「로컬 구조 스냅샷」에 dirty 파일 정체·근본 원인·게이트 수정 diff 포함

## 금지 (재확인)
Step 4A 전 dirty 파일 건드리기 · Step 4B 커밋·푸시 전 설치기 실행 · CAT-A 로직 변경 · (b)/(c) 줄 변경 · 슬라이스 수치 변경 · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## sub-phase ID
`CAT-L-FENCE-02` (Step 4 — 긴급)

---

# CLAUDE → CURSOR · CAT-L-FENCE-02 · Step 3 잔여

> **작성**: Claude Pro (Architect) · 2026-09-28
> **선행**: Step 3 A(생성기·설치기 영속화) OUTBOX 검증 완료. B(실스캔 캡처)는 SSH publickey denied로 미캡처.
> **CAT**: CAT-L 🟡 Medium · also_load: CAT-A(읽기전용)
> **구현**: Cursor(코드/문서) + SSH 가능한 주체(디렉터 또는 키 보유 세션)가 서버 명령 실행. CAT-A 비접촉.

---

## Claude 판정 — A: OK

- 생성기: HEAVY 직행 28줄만 root+wrapper, enqueue/OPS는 ubuntu 유지, `\%` 이스케이프, `_HEAVY_PREFIXES`는 읽기 import + 패리티 테스트 — 스펙 일치.
- 설치기: slice 설치 + `daemon-reload` 멱등, post-deploy-obs 검증 유지, wrapper grep 추가. `update_bitget.sh`가 설치기를 호출함을 발견·반영(스펙 A-5 충족). `bitget.sh`·`deploy_bitget_factory.sh`는 미호출 확인.
- 테스트 21 passed: wrapped=28, unwrap==P0 100%, 패리티, 생성기==LIVE 스냅샷.
- 편차 없음. 슬라이스 수치 변경 0, CAT-A 수정 0.

**비차단 관찰 3건**
1. "생성기==LIVE" 공집합은 **Step 2 시점에 캡처한 스냅샷** 기준이다. 서버 실파일 대비 검증은 아직 없음 → 아래 S2 필수.
2. `update_bitget.sh`가 설치기를 자동 호출하므로, "재설치 금지"는 **표준 업데이트 절차(`update_bitget.sh`) 자체를 포함**한다. 코드만 받으려면 `git pull`만 사용.
3. `test_generator_matches_live_snapshot`은 스냅샷을 정답으로 쓴다. 잡을 정당하게 추가/변경하면 테스트가 깨지는 것이 정상 — 실패 메시지에 "스냅샷 갱신 절차"를 한 줄 안내하면 좋음(권장).

## 서버 절차 (순서 고정)

**S0 (Cursor, 지금)** — OUTBOX 상단에 서버에서 그대로 복붙할 **검증 명령**을 기재: 생성기 출력 vs `/etc/cron.d/dual-screener-bitget` diff(헤더/타임스탬프 제외). 코드 변경 없음, 명령만.

**S1 (서버)** — 코드 반영은 `git pull`만. `update_bitget.sh`·설치기 실행 금지.

**S2 (서버)** — S0 명령으로 diff 실행 → 원문을 OUTBOX에 첨부. **공집합이면 S3, 아니면 즉시 중단하고 diff만 회신**(설치기 실행 금지).

**S3 (서버, S2 공집합 후에만)** — 이후 `update_bitget.sh`/설치기는 정상 사용 가능(no-op 기대). 최초 1회는 실행 전후 `md5sum /etc/cron.d/dual-screener-bitget`이 같음을 확인.

**S4 (서버)** — 아래 B 캡처.

## B. 실스캔 캡처 — 소급 방식

wrapper는 2026-09-27 오후부터 라이브라 첫 슬롯(16:01 UTC)은 이미 지났다. **특정 시각을 기다리지 말고 지금 쌓인 로그·프로세스로 확인한다.**

| # | 항목 | 기대 결과 |
|---|------|-----------|
| 1 | 실행 중 (a) 스캔의 cgroup | `bitget-cron-heavy.slice` 소속 (`cron.service` 아님) |
| 2 | wrapper 이후 정상 완주 | 스케줄이 도래한 (a) 줄의 로그가 끝까지 기록, wrapper 시작 실패 흔적 없음 |
| 3 | 실행 중 environ (HOME/USER/LOGNAME/PATH/PWD) | wrapper 없는 ubuntu 잡(예: OPS)과 비교해 동작에 영향 주는 차이 없음 |
| 4 | 로그 파일 소유권 | wrapper 이후 새로 생긴 로그가 ubuntu 잡·logrotate 쓰기에 지장 없음 |
| 5 | 스코프 CPUQuota | 80%(`CPUQuotaPerSecUSec`=800ms)로 유효 적용 |
| 6 | slice 속성 + 부모 slice | High=1288490188 / Max=1610612736, 부모(`bitget.slice`, `bitget-cron.slice`) 상한 유무 |
| 7 | 28줄 중 wrapper 이후 한 번도 안 돈 줄 | 스케줄이 도래했는데 로그 mtime이 갱신 안 된 줄 0개(줄별 오타 소급 탐지). 주간/일간 등 아직 도래 안 한 줄은 제외 |
| 8 | LIFECAP watchdog | wrapped 스캔이 watchdog 로그(파일)에서 감시 대상으로 잡힘(cap 분류 라인) |
| 9 | **글로벌 OOM 소급** | Step 2 적용 시점 이후 커널 로그에 OOM/`Killed process` 없음(있다면 cgroup 경로가 slice 내부인지 글로벌인지 구분) |

예시 명령(환경에 맞게 Cursor가 확정):

```bash
ps -eo pid,user,etime,cgroup,cmd | grep -E 'scan_|daily_audit|weekly_evolution' | grep -v grep
systemctl show bitget-cron-heavy.slice -p ActiveState -p MemoryHigh -p MemoryMax -p MemoryCurrent
systemctl show bitget.slice bitget-cron.slice -p MemoryHigh -p MemoryMax
systemctl list-units --type=scope | grep -i run-        # 실행 중 scope 이름 확인 후
systemctl show <scope> -p CPUQuotaPerSecUSec -p MemoryMax
tr '\0' '\n' < /proc/<pid>/environ | grep -E '^(HOME|USER|LOGNAME|PATH|PWD)='
journalctl -u cron --utc --since "2026-09-27 13:55" | grep -Ei 'error|failed|systemd-run'
journalctl -k --utc --since "2026-09-27 13:55" | grep -Ei 'out of memory|oom|killed process'
```

`memory.events`/`memory.peak`(cgroup v2, 커널 지원 시)가 slice에 남아 있으면 함께 캡처 — 3-way 재관측(상한 재확정)의 공짜 데이터. 없어도 실패 아님.

## C. 문서 (Cursor, 문서만)
- `STRUCT_한미코인_100퍼센트가동_점검_및_수정필요사항.md` 88행 `--use-queue` 재설치 절차에 주의 1줄: "설치기 재설치는 생성기(HEAVY wrapper 포함) 반영 상태에서만. 서버 수동 수정본은 재설치 시 사라진다."
- `CAT-L_인프라배포.md`에 1줄: "`/etc/cron.d/dual-screener-bitget`의 SSOT는 `generate_bitget_crontab.py`. 서버 수동 편집 금지(편집하면 다음 설치에서 소실)."

## D. 3단계 효과검증 등록 (Cursor, 문서)
`06_검증체크리스트_및_실패기록.md` 「효과 검증 기록표」에 `CAT-L-FENCE-02_Step3_records.md`의 [3] 행을 추가. **판정 예정일 2026-10-11**(적용 후 2주), 판정 주체 디렉터+Claude. 이번 건은 A-1~A-5처럼 "(대기)"로 방치되지 않게 날짜를 박아 둔다.

## 완료 정의
- S0~S4 완료, S2 diff 공집합 원문 확보, B 9항목 캡처(미해당은 사유 명시)
- `05_진행로그.md` · `00_전체현황판.md` · `CURSOR_TO_CLAUDE.md` · `NEXT_ACTION.md`(→`WAIT_CLAUDE_OK`) 갱신
- 통과 시 Claude가 **SUB_DONE** 판정(1~2단계). 3단계(효과)는 2026-10-11 판정.

## 롤백
서버 cron.d 원복: `snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron`. 코드는 커밋 revert. 이번 잔여 Step은 서버 변경이 없음(읽기전용 확인 + 조건부 no-op 설치).

## 금지
S2 공집합 확인 전 설치기·`update_bitget.sh` 실행 · 슬라이스 수치 변경(3-way 재관측 전 고정) · CAT-A 로직 변경 · (b)/(c) 줄 변경 · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## sub-phase ID
`CAT-L-FENCE-02` (Step 3 잔여)

---

# CLAUDE → CURSOR · CAT-L-FENCE-02 · Step 3

> **작성**: Claude Pro (Architect) · 2026-09-28
> **선행**: Step 2 적용 완료(슬라이스 1.5G/1.2G, (a) 28줄 root+`--uid=ubuntu` wrapper, 프로브 cgroup=`/bitget.slice/bitget-cron.slice/bitget-cron-heavy.slice/`). Cursor 자진 보고: `generate_bitget_crontab.py` / `install_bitget_cron.sh` 미변경 → 재설치 시 wrapper 증발.
> **CAT**: CAT-L 🟡 Medium · also_load: CAT-A(읽기전용), CAT-MAP §3
> **구현**: Cursor only. CAT-A 로직 비접촉.

---

## 목적
Step 2는 **런타임(서버의 cron.d)에만** 적용됐다. 생성기(SSOT)가 모르는 상태라, 다음 `install_bitget_cron.sh` 재실행·재배포·`--use-queue` 재생성 때 펜스가 **경고 없이 사라진다**(OOM 노출 원복). 이 Step은 (1) 생성기·설치기에 wrapper와 slice 유닛을 반영해 재설치해도 유지되게 하고, (2) 프로브(sleep)가 못 본 실스캔 동작을 검증한다.

## A. 영속화 (코드/배포 스크립트)

1. `bitget/deploy/generate_bitget_crontab.py`
   - HEAVY 직행 줄(= `_HEAVY_PREFIXES` 해당, enqueue 아님)만 `root` + Step 2와 **동일한 wrapper 문자열**로 생성. enqueue/OPS 줄은 기존대로 `ubuntu`.
   - `%` 이스케이프(`CPUQuota=80\%`) 포함. `--use-queue` 모드에서 enqueue로 바뀐 줄은 wrapper 대상 아님(이미 factory cgroup).
2. slice 유닛 `bitget-cron-heavy.slice`(MemoryHigh=1288490188 / MemoryMax=1610612736)를 `deploy/systemd/` 템플릿으로 추가, `install_bitget_cron.sh`가 설치 + `daemon-reload` (멱등).
3. `install_bitget_cron.sh`의 기존 검증(post-deploy-obs 줄 필수)은 유지.
4. `_HEAVY_PREFIXES` 분류: 가능하면 CAT-A 상수를 **읽기 import**(side-effect 없을 때). 불가하면 생성기에 복제하고 **패리티 테스트**(생성기 목록 == `job_lifetime_cap._HEAVY_PREFIXES`)로 드리프트 방지. CAT-A 코드 수정 금지.
5. 어떤 스크립트가 `install_bitget_cron.sh`를 호출하는지 grep해 OUTBOX에 회신(`update_bitget.sh` / `deploy_bitget_factory.sh` / `bitget.sh`). 호출한다면 그 경로도 이번 Step 반영 대상.

**테스트(최소)**: (a) 생성 결과의 wrapped 줄 수 = 현재 28, wrapper 제거 시 Phase 0 스냅샷의 원 명령과 100% 동일 (b) 패리티 테스트 (c) 멱등성 — 생성기 결과 vs 현재 라이브 `/etc/cron.d/dual-screener-bitget` diff = 비어야 함(헤더 타임스탬프 제외). **diff가 비기 전에는 서버에서 재설치 금지**(재설치가 no-op임을 증명한 뒤에만).

## B. 실스캔 검증 (프로브가 못 본 것)

첫 실제 (a) 슬롯 16:01 UTC `scan_spot_ema5_r2` 이후, 다음을 OUTBOX에 캡처:

| # | 항목 | 확인 내용 |
|---|---|---|
| 1 | cgroup | `ps -o pid,cgroup`로 `bitget-cron-heavy.slice` 소속 |
| 2 | 정상 완주 | 로그 끝까지·exit 정상, 산출물(DB/후보) 이전과 동일 형식 |
| 3 | 환경 | 실행 중 `/proc/<pid>/environ`의 HOME/USER/PATH/PWD를 **wrapper 없는 ubuntu 잡**과 비교(root cron 전환 부작용 점검) |
| 4 | 로그 소유권 | 리다이렉션이 있으면 셸이 root로 파일을 연다 — 새로 생긴 로그 파일 소유자가 ubuntu 잡·logrotate 쓰기에 지장 없는지 |
| 5 | CPUQuota | 스코프의 실제 CPUQuota가 80%로 적용됐는지(`systemctl show <scope>` — `\%` 이스케이프가 유효한 값으로 전달됐는지) |
| 6 | 슬라이스 속성 | `systemctl show bitget-cron-heavy.slice -p MemoryHigh -p MemoryMax` + 부모 `bitget.slice`/`bitget-cron.slice` 상한 유무 |
| 7 | 정적 점검 | 28줄 전부 wrapper 제거 후 Phase 0 스냅샷과 diff 비어있음(줄별 오타·누락 사전 차단) |
| 8 | watchdog | 다음 watchdog tick 로그에 wrapped 스캔이 LIFECAP 감시 대상으로 잡히는지 1줄(cap 분류 로그) — CAT-A 코드 조사 아님, 로그 확인만 |

## 인접 CAT
CAT-A 비접촉(읽기전용 참조). CAT-N/F 비접촉.

## 롤백
생성기·설치기 변경은 커밋 revert. 라이브 cron.d는 이번 Step에서 재설치하지 않으면 그대로. 라이브 원복은 `snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron`.

## 운영 주의 (디렉터 전달)
Step 3 완료 전까지 서버에서 `install_bitget_cron.sh` · `generate_bitget_crontab.py` 기반 재설치 · `--use-queue` 재생성을 **하지 말 것**. 부득이한 재배포가 끼면 끝난 뒤 Step 2 wrapper를 OUTBOX 절차대로 수동 재적용하고 알릴 것.

## 금지
CAT-A 로직 변경 · (b)/(c) 줄 변경 · 슬라이스 수치 변경(3-way 재관측 전 고정) · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## 완료 정의
A+B 완료, `05_진행로그.md` · `00_전체현황판.md` · `CURSOR_TO_CLAUDE.md` · `NEXT_ACTION.md`(→`WAIT_CLAUDE_OK`) 갱신, 「로컬 구조 스냅샷」에 생성기 변경 diff 요약 포함.

## sub-phase ID
`CAT-L-FENCE-02` (Step 3)

---

# CLAUDE → CURSOR · CAT-L-FENCE-02 · Step 2 (최종 확정 · 적용 승인)

> **작성**: Claude Pro (Architect) · 2026-09-27
> **선행**: Phase 0 완료(28/1/9) · Step 1 완료(3-way 최악 · 2-way 526MB)
> **CAT**: CAT-L 🟡 Medium · also_load: CAT-A(읽기전용)
> **구현**: Cursor only. 게이트/판정 로직 비접촉.

슬라이스 MemoryHigh=1288490188 / MemoryMax=1610612736 **확정 유지**. 역할=가두기(slice 안 킬), 3-way 절대 안 넘김이 아님.

대상: (a) 28줄만. (b)/(c) 비접촉. cron.d 전체 재작성 금지. 3-way 재관측 전 수치 임의 변경 금지.

## sub-phase ID
`CAT-L-FENCE-02` (Step 2 — 최종)

---

# CLAUDE → CURSOR · CAT-L-FENCE-02 · Phase 1

> **작성**: Claude Pro (Architect) · 2026-09-27
> **선행**: Phase 0 완료 — `/etc/cron.d/dual-screener-bitget` 실측 (a)28줄 직행 · (b)1줄 enqueue · (c)9줄 OPS. 롤백 스냅샷: `bitget/docs/work_phases/snapshots/CAT-L-FENCE-02_cron_p0_20260927.cron`
> **디렉터 승인**: 2026-09-27 (옵션 A, 이 Phase 1은 그 승인 범위 내 설계 보강 — 재승인 불요, 단 아래 "설계 변경 사유" 확인 요망)
> **CAT**: CAT-L 🟡 Medium · also_load: CAT-A(읽기전용 참조만) · CAT-MAP §3(L은 deploy scripts만, A pipeline step order 비접촉)
> **구현**: Cursor only. 게이트/판정 로직 비접촉. **Step 2는 Step 1 회신·Claude 확인 후.**

---

## 설계 변경 사유 (원 Phase 1 계획 대비)

Phase 0 RSS: 09-25 OOM 희생자 anon-rss ≈80.7MB. 개별 `--scope`만으로는 다발 겹침 OOM 재발 가능. **보강**: 개별 scope + 공통 `bitget-cron-heavy.slice` 합산 상한. 수치 시작점 = L-3a factory (Max=1610612736 / High=1288490188). Step 1 실측 후 Claude가 1.5G/1.2G vs 하향 확정.

## Step 1 — 확정 전 실측 (코드 변경 없음)

1. (a) 28줄, 동일 5400s 윈도우 동시 실행 가능 개수
2. (a) job 없을 때 `free -h` 1회
3. OUTBOX 회신 → Step 2는 Claude 확정 후

## Step 2 — 적용 (Step 1·Claude 확인 후)

(a) 28줄만. slice + systemd-run --scope. (b)/(c) 비접촉. cron.d 전체 재작성 금지.

## 금지
CAT-A 로직 · (b)/(c) · C-2/MDD5%/live/`ENABLE_REAL_EXECUTION`

## sub-phase ID
`CAT-L-FENCE-02` (Phase 1)

---

# CLAUDE → CURSOR · CAT-L-FENCE-02

> **작성**: Claude Pro (Architect) · 2026-09-27
> **요청 출처**: cron OOM 3회 재발(09-07/09-14/09-25) — L-3 MemoryMax가 systemd 유닛에만 걸리고 cron 직행 scan은 `cron.service` cgroup에 남아 상한 밖
> **디렉터 승인**: 2026-09-27 (옵션 A — systemd-run scope wrapper, 에스컬레이션 해소)
> **CAT**: CAT-L 🟡 Medium · also_load: CAT-A(읽기전용 참조만), CAT-MAP §3(L은 deploy scripts만, A pipeline step order 비접촉)
> **구현**: Cursor only. **Phase 0 결과를 Claude가 확인하기 전 Phase 1 착수 금지.**

---

## Phase 0 — 발견 (읽기전용, 아무것도 바꾸지 않음)

1. 해당 서비스 계정 `crontab -l` 전체 원문을 OUTBOX에 그대로 기재
2. 각 줄을 (a) HEAVY 직행 / (b) 이미 enqueue / (c) OPS·기타로 분류
3. (a)의 기존 OOM RSS 기록 취합만 (신규 측정 금지)
4. Claude가 (a) 목록 확인 후 Phase 1 진행 여부 회신

## Phase 1 — wrapper (Phase 0 확인 후에만)

(a)만. `systemd-run --scope -p MemoryMax=1610612736 -p MemoryHigh=1288490188 -p CPUQuota=80%` (factory L-3a 수치 재사용). (b)/(c) 금지.

## 금지
CAT-A 로직 · crontab 전체 재작성 · 주식 크론 · C-2/MDD5%/live

## sub-phase ID
`CAT-L-FENCE-02`

---

# CLAUDE → CURSOR · A5-EVENTLOG-01

> **작성**: Claude Pro (Architect) · 2026-09-26
> **요청 출처**: A-EFFECTVERIFY-01 결과 — A-1~A-5가 `logger`만 쓰고 `ops_events` 미기록 → 8주째 3단계 판정 불가
> **디렉터 승인**: 2026-09-26 (Critical, 순수 로깅 추가 범위)
> **CAT**: CAT-F/A 🔴 **Critical** (execution_safety.py, tail_risk_gate.py, config_bounds.py 접촉) · also_load: CAT-MAP, CAT-N(A-3 leverage 인접)
> **구현**: Cursor only. 게이트 판정 로직·threshold·반환값 1바이트도 변경 금지. 변경 필요성 발견 시 즉시 중단 + `## Claude 수정 spec`로 Ask.

---

## 목적
A-1~A-5는 tier 전이·debit·clamp·block·reject를 **판정은 하지만 이력을 안 남김**(logger만, 휘발). 그 결과 A-EFFECTVERIFY-01이 8주치 데이터를 못 뽑았다(전부 null). 이번 Handoff는 **이미 일어나고 있는 판정 결과를 ops_events에 추가로 기록**하는 것만 한다 — 판정 자체는 손대지 않는다.

## 수정 범위 (엄수)

| 허용 | 금지 |
|---|---|
| 각 파일에서 **기존 logger.info/warning 호출 직후 지점**에 `ops_events` insert 추가 | 게이트 조건문·threshold 비교식·반환값(bool/tier) 변경 |
| 신규 kill-switch `A1A5_EVENT_LOG_ENABLED`(config_kv, default **true**) | 기존 `logger.*` 호출 제거 (그대로 유지, 이벤트는 **추가**) |
| 신규 테스트 | C-2 · MDD5% · B-2 live · `ENABLE_REAL_EXECUTION` |
| | A-EFFECTVERIFY-01(`a1_a5_effect_verify_bg.py`) 로직 변경 — 이번엔 손대지 않음, 데이터 쌓인 뒤 별도 재실행 |

## Spec 1 — 이벤트 스키마 (5종, `ops_events` 공통 테이블)

| sub | event 이름 | 발생 조건 | payload |
|---|---|---|---|
| A-1 | `portfolio_mdd_tier_transition` | tier 실제 변경 시 | from_tier, to_tier, nav_current, nav_peak, dd_pct |
| A-2 | `tail_fund_debit` | 실제 debit | debit_amount, tail_balance_before, tail_balance_after, trigger_tier |
| A-3 | `leverage_clamped` | requested>cap | symbol, market_type (normalize_market_key), requested_leverage, clamped_to |
| A-4 | `gross_notional_blocked` | gate 7 block | gross_notional, nav_current, gross_pct, cap_pct |
| A-5 | `config_write_rejected` | reject | config_key, attempted_value, bound_min, bound_max |

## Spec 2 — A-1 전이 감지
`PORTFOLIO_MDD_CURRENT_TIER`와 비교 후 변경 시에만. 새 상태 저장소 금지.

## Spec 3 — kill-switch
`A1A5_EVENT_LOG_ENABLED` default true. false면 insert만 skip.

## SPOT/FUT
A-3만 FUT (`normalize_market_key`). 나머지 비분기.

## 테스트
A-1 변경 1건 + 동일 tier 0건 · A-2 debit 1 · A-3 clamp 1/0 · A-4 block 1 · A-5 reject 1 · kill-switch false 전부 0 + 판정 불변 · 기존 A-1~A-5 회귀.

## 금지
게이트/threshold/반환값 · C-2 · MDD5% · live · EFFECTVERIFY 집계 변경 · logger 제거

## 완료 정의
코드+테스트 · 05/00/OUTBOX/NEXT_ACTION WAIT_CLAUDE_OK · 첫 발생 예상 시점 스냅샷

## sub-phase ID
`A5-EVENTLOG-01`

---

# CLAUDE → CURSOR · A-EFFECTVERIFY-01

> **작성**: Claude Pro (Architect) · 2026-09-26
> **요청 출처**: `06_검증체크리스트_및_실패기록.md` 「효과 검증 기록표」 A-1~A-5 8주째 "(대기)" 방치 확인
> **CAT**: CAT-F 인접 / 묶음A (읽기전용) · 🟡 Medium — gate 로직 비접촉, 판정 근거자료 생성만
> **구현**: Cursor only. gate/threshold 변경 필요성 발견 시 CLAUDE_TO_CURSOR 상단 `## Claude 수정 spec`로 별도 요청.

---

## 목적
A-1~A-5(NAV MDD tier·tail fund·leverage clamp·gross notional·config reject)는 2026-08-01~02 배포, Claude OK(1단계) 완료. 8주 경과(2~4주 요건 충족)했으나 3단계(효과검증) 판정이 없어 "유지/롤백/추가조정"을 못 정한다. 이번 Handoff는 **판정 근거 숫자만 뽑는 읽기전용 집계**다. gate 로직·config 기본값은 전혀 안 건드린다.

## 수정 범위 (엄수)

| 허용 | 금지 |
|---|---|
| 신규 읽기전용 파일(예: `bitget/observability/a1_a5_effect_verify_bg.py`) | `execution_safety.py`, `tail_risk_gate.py`, `config_bounds.py` 본체 로직 |
| `ops_events` / `config_kv` 상태 / `bitget_forward_trades` 읽기 | A-1~A-5 threshold·기본값 변경 |
| 신규 테스트 | C-2 · MDD5% · B-2 live · `ENABLE_REAL_EXECUTION` |

## Spec 1 — 지표 정의 (창: 2026-08-01/02 ~ 오늘, 8주)

| sub | 지표 | 소스 우선순위 | 소스 없을 때 |
|---|---|---|---|
| A-1 | tier 전이(NORMAL→REDUCE/BLOCK/HALT) 횟수·시각·NAV, 창 내 관측 최대 NAV MDD % | `PORTFOLIO_MDD_CURRENT_TIER` 상태이력 → 없으면 ops_events tier 변경 로그 | "로그 소스 없음" 그대로 보고. **새 로깅 신설 금지** |
| A-2 | tail fund debit 이벤트 횟수·총액 | `TAIL_FUND_CONSUMPTION_ENABLED` 관련 이벤트 | 상동 |
| A-3 | leverage clamp 발생 횟수(`resolve_max_leverage`에서 requested>cap) | ops_events | 상동, FUT 전용 |
| A-4 | gross notional block 횟수·발생 시 gross/NAV | ops_events | 상동 |
| A-5 | config write reject 횟수·거절 키 목록 | `CONFIG_WRITE_VALIDATION_ENABLED` 관련 로그 | 상동 |

**원칙(D-3a/C-1b 재사용)**: 소스 없으면 추정 금지 — null + 사유. "관측 불가" 자체가 유의미한 결과.

## Spec 2 — 함수 시그니처 (제안)

def collect_a1_a5_effect_snapshot(
window_start: date, window_end: date, config: dict
) -> A1A5EffectSnapshot # 5 sub 지표 + source_availability 플래그

## Spec 3 — 출력
`CURSOR_TO_CLAUDE.md` OUTBOX에 5-sub × {값|null, 소스, 비고} 표. `06` 「효과 검증 기록표」"변경 후(2~4주)" 열에 그대로 옮길 수 있는 형태.

## SPOT/FUT
A-1·A-2·A-4는 포트폴리오 전체(비분기). A-3은 FUT 전용(SPOT 레버리지 없음) — `normalize_market_key` 경유, market_type 하드코딩 금지.

## 안전장치
읽기전용, kill-switch 관례상 `A1A5_EFFECT_VERIFY_ENABLED`(config_kv, default true) 추가 권장.

## 테스트
fixture 3개: (1) 전이 0건 구간 (2) 전이 1건 이상(mock) (3) 로그 소스 없는 sub 최소 1개(null 처리)

## 금지 (재확인)
gate 로직/threshold 변경 · C-2 · MDD5% · B-2 live · `ENABLE_REAL_EXECUTION` · 소스 없는 지표에 새 로그 파이프라인 신설

## 완료 정의
코드+fixture all pass · `05`·`00`·`CURSOR_TO_CLAUDE`·`NEXT_ACTION`(→`WAIT_CLAUDE_OK`) 갱신 · 「로컬 구조 스냅샷」 포함

## sub-phase ID
`A-EFFECTVERIFY-01`

---

# CLAUDE → CURSOR · [CAT-L] L-3b-fix 큐 워커 stale 임계 (A안 · 2026-09-15)
# 워치독 로직 미변경. env 1개. 09-15 15:07 UTC 슬롯에서 Done 판정.

실측(cron 시절 로그, 시작~끝 명확한 최근 4일):
09-09 744s · 09-10 737s · 09-11 735s · 09-12 734s ≈ **12.3분**
1회차 하한: 15:07:03~15:20:59 = 13분+ (킬로 미완료)
×2 ≈ 25분. Handoff 여유 예시(15분→1800~2000)에 맞춰 **1800초**. 50분 상한 미초과.
워치독이 읽는 값임 → .env + watchdog drop-in (queue-worker drop-in만으로는 cron/systemd watchdog에 안 먹음).

---

# CLAUDE → CURSOR · [CAT-L] 디렉터 1번 승인 확정 · L-3a 진행 / L-3b A안 / L-3 보류
# (prepend · 2026-09-14 23:13 KST)

> **판정**: 1번(정원 4곳) **승인** · 2번 **A안**(ema5_r2만 enqueue) · 3번(전체 큐) **나중에**
> **구현 상태**: L-3a drop-in + L-3b crontab **이미 서버 적용**(09-14 00:39 KST). 재적용·Max 축소 금지.
> **queue-worker**: 실측 Max=2G 있었음 → 「없으면 896M」분기 미해당. High=1.5G / Max=2G 유지.
> **L-4**: 이번 확정본 범위 밖(이미 코드만 있음 · 서버 pull 별도).
> **전문 스펙**: 바로 아래 CAT-L-FENCE-01 블록.

---

# CLAUDE → CURSOR · [CAT-L] L-3a/L-3b/L-4 메모리 울타리 커버리지
# (prepend · 덮어쓰지 말 것 · 2026-09-14)

> **레인**: 해당 없음 (Track B 공용 인프라)
> **작성**: 디렉터 확정 spec (진단 실측 09-14) · Cursor 구현
> **CAT**: CAT-L · 🟡 Medium · gate/live/`ENABLE_REAL_EXECUTION` 비접촉
> **sub-phase**: **CAT-L-FENCE-01**
> **L-1/L-2 에스컬레이션**: **취소** (이번 건 무관 · 서버에 이미 설치 실측)

## 원인 (고정)

factory `MemoryMax=1.5G`는 있었음. cron.service 밑 스캔 자식은 cgroup 밖.
09-07 08:44 UTC `global_oom` · RSS~897MB · `task_memcg=/system.slice/cron.service`.
4GB→8GB 증설로 안 고쳐짐. 디스크/L-1 가설 기각.

## Cursor 엔지니어 각주 (구현 반영)

- queue-worker **실측 MemoryMax=2G 이미 있음** → Handoff 「없으면 896M」분기. Max **유지**, `MemoryHigh=1.5G`만 추가 (factory Max 임의 축소 금지와 동일).
- L-3b crontab 플래그는 기존 어댑터 `bitget.sh --enqueue --scan-futures-ema5-r2` (대시 없는 `scan_futures_ema5_r2` 신규 파서 없음).
- 생성기 전면 `--use-queue`(b-3) 켜지 않음. canary 1플래그만.

## L-3a — 상시 서비스 메모리 울타리

SSOT: `bitget/deploy/systemd/*.service.in` + 서버 drop-in (HIST_10 §2.4, 신규 스크립트 없음)

| unit | 적용 |
|------|------|
| factory | MemoryHigh=1.2G · MemoryMax=1.5G 유지 |
| ws | MemoryHigh=200M / MemoryMax=256M |
| async | MemoryHigh=100M / MemoryMax=128M |
| queue-worker | MemoryHigh=1.5G / MemoryMax=**2G 유지** |

완료: 4유닛 `systemctl show -p MemoryMax -p MemoryHigh -p MemoryCurrent` **원문**.

## L-3b — canary 큐 편입

crontab **해당 1줄만**. 다른 scan_* 는 범위 밖.
24–48h 관찰: queue-worker claim→done · dmesg killed 0 · MemoryCurrent 피크.

## L-4 — digest 울타리/크론 (read-only)

`post_deploy_obs_digest_bg.py` · `scan_last_cgroup_by_mode` · `fenced_units_memory_snapshot` · 실패 시 null+unavailable.

---

# CLAUDE → CURSOR · [LANE_FULLBT] FULL-BT-FUT-DEPTH-1 Claude OK + staging FULL-BT Go
# (prepend 보관 · 덮어쓰기 금지 · 2026-08-29)

> **레인**: **LANE_FULLBT** (HIST3FIX 아님)
> **작성**: Claude Pro · 2026-08-29 · [FULL-BT-FUT-DEPTH-1]
> **판정**: **OK — Go** (VPS staging COUNT PASS)
> **Cursor**: WAIT_CURSOR_VPS · staging FULL-BT=1 · MAX_SYMBOLS=3 · futures-only
> **고정**: write_mode=staging · 프로덕션 OHLCV write 금지 · IV L1 참고만 · LIVE/R6/생존 단정 금지 · 3심볼 초과 별도 승인

## Cursor 실행

`ash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
export BITGET_FUT_DEPTH_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
BITGET_FUT_DEPTH_RUN_FULL_BT=1 BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_fut_1d_depth_pilot.sh
`

결과 JSON 핵심(engine_call_total / outcome / trade_count / paper) → lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md

---

# CLAUDE → CURSOR · B0-SAMPLE-CONTRACT Claude OK
# (append 보관 · 덮어쓰기 금지 · 2026-08-28) · LANE_FASTCHECK

> 작성: Claude Pro · [CAT-F] · 판정: **OK** · sub-phase **DONE**
> 추가 diff 없음 · HIST 비접촉 유지 · 원본 이슈(표본 부족) 미해결·관측

---

# CLAUDE → CURSOR · B0-SAMPLE-CONTRACT Handoff
# (append 보관 · 덮어쓰기 금지 · 2026-08-28) · LANE_FASTCHECK

> **작성**: Claude Pro · 2026-08-28 · [CAT-F]  
> **상태**: Cursor 문서 구현 → **WAIT_CLAUDE_OK**  
> **병행**: LANE_HIST3FIX 비접촉

## [CAT-F] 자본배분&리스크 — B0-SAMPLE-CONTRACT (표본 충분성 계약 문서)

### sub-phase ID
B0-SAMPLE-CONTRACT

### SSOT
- `13_B1_신뢰사다리.md` **§7 신설만** · §1~§6 비변경 · config 없음

### Cursor 지시
- Targeted · 문서만 · 09/NEXT_STEP 이번 라운드 미갱신
- `lanes/LANE_FASTCHECK/CLAUDE_TO_CURSOR.md`에도 append 보관

### 위험도
🟢

---

# CLAUDE → CURSOR · B1-LADDER-R1a-FASTCHECK Claude OK
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> 작성: Claude Pro · [CAT-F] · 판정: OK (1단계 스펙 검증 통과)
> 비차단 확인 2건(R6 페이스 포함여부 / SPOT blocked=0 쿼리vs하드코딩) → 다음 보고 1줄씩
> 배포는 디렉터 승인 후 · 2~4주 가상매매(2단계) 관찰 시작 가능
> FULL-BT(A)는 별도 레인 유지, 본 건과 혼합 금지

---

# CLAUDE → CURSOR · B1-LADDER-R1a-FASTCHECK Handoff
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> **작성**: Claude Pro · 2026-08-28 · [CAT-F]
> **상태**: **WAIT_CURSOR_IMPL** → Cursor 구현 후 WAIT_CLAUDE_OK
> **병행**: FULL-BT(A) 좁은 수정 — **별도 트랙 · 비게이팅 · 혼합 금지**

---

## [CAT-F] 자본배분&리스크 — B1-LADDER-R1a-FASTCHECK read-only 주간 집계

### sub-phase ID
B1-LADDER-R1a-FASTCHECK

### SSOT (변경 금지 unless noted)
- 신규: `bitget/observability/b1_ladder_fastcheck_bg.py`
- 수정(추가만): `bitget/docs/work_phases/13_B1_신뢰사다리.md` §6 아래 "R1a FASTCHECK 절차" 소절
- 참조만(비변경): `bitget_forward_trades`, §6 SQL, §3 판정표, `short_funnel_report_bg.py`
- config: `B1_LADDER_FASTCHECK_ENABLED`(config_kv), `B1_LADDER_FASTCHECK_WINDOW_DAYS`(config_kv)

### 변경 Spec
- 함수: `compute_b1_ladder_fastcheck_bg(window_days: int = 7) -> dict[str, dict]`
- market_type(SPOT/FUT)별 open_count·closed_weekly_delta·blocked_short_total·r6_pace_flag → 기존 §3 판정표 대입 → verdict 문자열만
- weekly_evolution 훅 등록(read-only), 기존 훅 순서 비접촉
- 출력: `ops_events` → `b1_ladder_fastcheck_weekly` (mt별 1건)
- SPOT/FUT: 완전 분리, 합산 금지

### Config 변경
| KEY | old | new | default |
|-----|-----|-----|---------|
| `B1_LADDER_FASTCHECK_ENABLED` | — | 신규 | true |
| `B1_LADDER_FASTCHECK_WINDOW_DAYS` | — | 신규 | 7 |

### 인접 CAT 영향
- CAT-C: 없음 (R1b 이름만 예약, 착수 금지 유지)
- CAT-J: 읽기만 (short_funnel 기존 출력)
- CAT-D/N: 비접촉

### 롤백 조건
`B1_LADDER_FASTCHECK_ENABLED=false` → 즉시 비활성, ops_events 신규 기록만 중단

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- FAIL(b) verdict 나와도 R1b 코드 착수 금지 — verdict 산출까지만
- 신규 정책·게이트·Critical·상수 창조 금지
- 테스트: `pytest bitget/tests/test_b1_ladder_fastcheck_bg.py`

### 위험도
🟢 (read-only 집계 · config_kv kill-switch 2개뿐 · gate/Kelly/MDD/live 비접촉)

---

# CLAUDE → CURSOR · FULL-BT-HIST-3-FIX 검증 OK → VPS dry→10×2 + 원문 append
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)
# ⚠️ HIST-3-FIX 좁은 수정 Handoff가 INBOX에 누락됐던 재발(2회째) — 본 검증 응답을 원문으로 소급 append

> **작성**: Claude Pro (Architect) · 2026-08-28 · [CAT-Q]
> **상태**: **FULL-BT-HIST-3-FIX Spec 1~4 = OK** · **VPS dry→10×2 실행 승인** · 전체런 금지
> **caveat**: `_U1_MIN_BARS` 언더스코어 재사용 — 다음 라운드 CAT-CONSTANTS 승격 권고(지금 조치 아님)

---

## [CAT-Q] FULL-BT-HIST-3-FIX 검증 OK → VPS dry→10×2 실행 + 원문 append

### 판정
Spec 1~4 전부 OK. VPS 실행 승인. 전체런 금지 유지.

### Spec 대조 (소급 원문)
1. fetch [start−min_bars, end] Adapter — OK (tail-only 한계 → date-range Adapter, `_load_ohlcv` 비접촉)
2. walk `range(REUSED_MIN_BARS, len)` — OK (walk 1바 단서 대응)
3. warmup 부족 skip(보간 없음) — OK
4. 원본 비접촉 — OK (replay/CAT-C/D/E/batch/report)
- REUSED_MIN_BARS = `bitget.analysis.universe_bt.replay._U1_MIN_BARS` (240)

### 실행 (원문 그대로)
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_full_bt_hist_pilot.sh
BITGET_FULL_BT_MAX_SYMBOLS=10 bash bitget/deploy/run_full_bt_hist_pilot.sh
```

### 보고 (7키)
call_total · outcome_totals · tf_coverage · exception_types · hit/reject ·
fetch_requested vs fetch_loaded · call_total vs walk_bar_expected

### 필수 조치
1. 본 검증 응답 전체를 `CLAUDE_TO_CURSOR.md`에 **append**(덮어쓰기 금지)
2. 롤백: `harness.py` 해당 커밋 revert만 — 결과 스키마·paper·config_kv 무영향

### 세션 종료 의무
- 05: VPS 숫자 + 재판정 · 00 · CURSOR_TO_CLAUDE 7키 · NEXT_ACTION WAIT_CLAUDE_OK · 09/NEXT_STEP

### 위험도
🟢 (read-only 진단 VPS · 실자금/config_kv 미접촉)

---

# CLAUDE → CURSOR · FULL-BT-HIST-3-FIX (warmup fetch-range 교정)
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> **작성**: Claude Pro (Architect) · 2026-08-28 · [CAT-Q]
> **상태**: 디렉터 **(A)** 확정 · **FULL-BT-HIST-3-FIX** Handoff · 🟡
> **금지**: HIST-4 · 열린 lookback · 전체런

---

## [CAT-Q] 진단&레거시 — FULL-BT-HIST-3-FIX: warmup 창 반영 fetch-range 교정

### sub-phase ID
FULL-BT-HIST-3-FIX
(주의: HIST-4 아님 — HIST-3 단일 용의점의 좁은 수정. 디렉터 (A) 확정.)

### SSOT
- 파일: `bitget/full_bt/harness.py` (`run_replay` 내부만)
- `_load_ohlcv` start 오프셋 미지원 시 harness Adapter — `replay.py` 원본 수정 금지
- REUSED_MIN_BARS: 신규 상수 금지 · CAT-C/universe 기존 min-bars import

### Spec
- `fetch_range` / `requested_bar_count = REUSED_MIN_BARS + walk_bar_count`
- walk: `for i in range(REUSED_MIN_BARS, len(window))` — 더 이상 calls=1 고정 아님
- warmup 부족 심볼 skip · 보간 금지

### Cursor
- Targeted diff only · pytest full_bt · dry→10×2 · WAIT_CLAUDE_OK
- 보고: 기존 5키 + requested vs loaded + call vs 기대 walk

---

# CLAUDE → CURSOR · FULL-BT-HIST-3 재판정 = 부분반려 + 디렉터 에스컬레이션
# (append 보관 · 덮어쓰기 금지 · 2026-08-25)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: 원인(1)(3) 배제 **승인** · "에스컬레이션 해당없음" **반려** · **WAIT_DIRECTOR**
> **HOLD**: lookback/HIST-4/신규 코드 착수 금지 · 전체런 금지

---

## 판정 요약
- Ask1: TF·호출경로 배제 OK / 에스컬레이션 해당없음 **반려** (트리거=**미해결 시**)
- Ask2: lookback Handoff **보류** (디렉터 승인 전)
- Ask3: 전체런 금지 **유지**
- 단서 유지: REUSED_MIN_BARS=로더tail(250)→walk 1바 · calls=1

## 디렉터 질문
(A) 용의점만 좁혀 수정 vs (B) FULL-BT 보류·R1a/B1 집중

## Cursor
신규 코드 금지 · NEXT_ACTION=`WAIT_DIRECTOR` · 05에 본 판정 기록

---

# CLAUDE → CURSOR · B0 표본 기아 Ask 답변
# (append 보관 · 덮어쓰기 금지 · 2026-08-28)

> **작성**: Claude Pro · 2026-08-28 · [CAT-F]
> **상태**: Ask 1~5 답변 완료 · 코드 diff 없음 · 코드 Handoff는 디렉터 확정 후
> **병행**: FULL-BT-HIST-3 VPS 유지(비게이팅) · B1-LADDER-R1a OBSERVE 유지

---

## [CAT-F] B0 표본 기아 Ask 답변 — R1a 빠른 판정 경로 제안 (코드 diff 없음)

### SSOT (변경/비변경)
- **비변경**: `13_B1_신뢰사다리.md` §2/§3 렁·Kill 표, `bitget_forward_trades` 스키마, `short_funnel_report_bg.py`, gates/Kelly/execution_safety 전체
- **다음 라운드 추가 예정만(이번엔 미실행)**: `13_B1_신뢰사다리.md` §6에 "R1a FASTCHECK" 절차 소절 — Cursor 확정 후 별도 Handoff

### Spec (Ask 1~5 판정)

**1) 오해/팩트 표 — 수용 OK.** 「누적」= `bitget_forward_trades` CLOSED 사이드별 전 기간 COUNT, 주간 하드쿼터 아님. MAX open positions ~20(CAT-CONSTANTS) 재확인. 배선 생존(CLOSED≈10) + 신규진입 정체(OPEN=0) 동시 성립 인정.

**2) 디렉터 공식 답변 문구** — `09_디렉터_쉬운요약.md` 갱신본 참조.

**3) 우선순위 — (i)도 (ii)도 아님, 제3안.**
`13_` Kill표 R1a는 이미 두 갈래 FAIL 경로: (a) 4주 시간경과, (b) short_funnel 반복 거절 패턴. 4주((a)) 대기도, FAIL 미확정 상태에서 곧장 R1b CAT-C 코드 Handoff((ii))도 Kill표 순서 위반. 대신 **(b) 조건을 기존 계측만으로 지금 판정**. HIST-3(FULL-BT L1)은 다른 트랙(히스토리 리플레이, engine_hit=0 원인규명)이고 R6/B1 근거 사용이 이미 금지(`15_` SSOT) → 완료돼도 디렉터의 진짜 문제(L2 표본 증가 속도)를 풀어주지 않음 → **HIST-3는 병렬·비차단 유지, R1a 판정만 이번 라운드에서 가속.**

**4) 최소 관측 계약** (기존 숫자만 재사용, 신규 상수 없음)

| 신호 | 재사용 출처 | 판정 기여 |
|------|-------------|---------|
| OPEN 재개 | `13_` §6 SQL 그대로 | OPEN>0 → 즉시 PASS 재검토 |
| CLOSED 주간 Δ | 동일 SQL, 기존 timestamp 컬럼 7일 필터만 추가(신규 로직 아님) | 누적치 대신 최근 추이 |
| blocked_short_total 추이 | `short_funnel_report_bg.py` 기존 출력 | 반복되면 FAIL(b) 근거 (LONG 가시성 없음 — 기존 caveat 유지) |
| R6 페이스 환산 | (CLOSED 주간Δ ÷ 7 × 56) vs 기존 30건/56일(`13_` R6, 재발명 아님) | 부족 시 "페이스 부족" flag만, 게이트 변경 아님 |

**5) 다음 Handoff ID 후보:** `B1-LADDER-R1a-FASTCHECK` — 문서(`13_` §6) + 위 4개 값을 조합하는 read-only weekly 집계 1개. 신규 상수·게이트·Critical 없음. R1b(CAT-C)는 FASTCHECK가 FAIL(b) 확정할 때만 별도 라운드 착수.

### SPOT/FUT 분기
공통 — SHORT는 SPOT에서 구조적으로 0(SPOT-FUT 표 기존 각주), FASTCHECK 지표는 market_type별 분리 집계만, 합산 금지.

### 인접 CAT 영향
- CAT-C: 없음 (R1b 미착수, 이번엔 이름만 예약)
- CAT-D/J: 읽기만 (기존 컬럼·기존 short_funnel 출력 재사용)
- CAT-N/F(execution_safety/Kelly/live): 비접촉, 🔴 Critical 아님

### Cursor 지시 (이번 라운드 = 확정 답변 반영만 · 코드 Handoff 아님)
- `09` · `NEXT_STEP` · `NEXT_ACTION`(WAIT_DIRECTOR) · `05` · `00` 현황판 반영
- **다음:** 디렉터가 3)/5) 방향 동의 시, 다음 대화에서 `B1-LADDER-R1a-FASTCHECK` 단일 sub-phase CAT-HANDOFF 발급(CAT-F, 🟢, read-only).
- 그 전까지 HIST-3 VPS dry→10×2 숫자 보고는 별도 트랙 계속.

---

# CLAUDE → CURSOR · FULL-BT-HIST-3 스펙 OK → VPS dry→10×2 실행
# (append 보관 · 덮어쓰기 금지 · 2026-08-25)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: **FULL-BT-HIST-3 검증 = OK (비차단 caveat 1건)** · **VPS dry→10×2 실행 승인** · 코드 diff 종료
> **에스컬레이션**: 3원인 미분리 시 HIST-4 금지 · 디렉터 에스컬레이션만

---

## [CAT-Q] 진단&레거시 — FULL-BT-HIST-3 스펙 OK → VPS dry→10×2 실행

### sub-phase ID
FULL-BT-HIST-3 (VPS 실행 단계)

### SSOT (변경 금지)
- 코드 diff 종료(스펙 OK) — 이번 라운드는 실행만
- full_bt_diag(tf 확장 완료) · CAT-C/B/D 원본 비접촉 유지

### 변경 Spec
- VPS 실행: 회신 원문 명령 그대로(git pull → dry(3) → 10×2), 신규 명령 추가 없음
- 보고 키(5개 숫자만): engine_call_total · engine_call_outcome_totals · tf_ohlcv_coverage · exception_types · HIST-2 hit/reject

### Config 변경
없음

### 인접 CAT 영향
- CAT-C/B/D: 읽기만(재확인) · 🔴 아님

### 롤백 조건
diag 테이블 `tf` 컬럼 무시/삭제만으로 완전 롤백 — 결과 trade 스키마 무영향

### Cursor 지시
- 커밋·푸시 후 VPS: dry(3) → 10×2 순서 그대로. **전체 유니버스 런 금지 유지**
- 보고는 숫자만(해석·재판정은 Claude 몫)
- "call>0 ∧ TF 전부 True ∧ none 지배" 확인되면 → lookback(N bars) 조사는 **별도 Handoff 대기**, 지금 미착수
- 3원인 미분리(여전히 판별 불가) → **HIST-4 설계 금지**, `CURSOR_TO_CLAUDE.md`에 디렉터 에스컬레이션 필요로 명시
- 이번 Handoff 원문 `CLAUDE_TO_CURSOR.md`에 **append 보관**(덮어쓰기 금지, 재발 방지)

### 위험도
🟢 (read-only 진단 VPS 실행 · 실자금/config_kv 미접촉)

### 세션 종료 의무
- 05_진행로그.md HIST-3 VPS 결과 섹션
- 00_전체현황판.md
- CURSOR_TO_CLAUDE.md 4개 숫자 + 3원인 판별 결과
- NEXT_ACTION.md → WAIT_CLAUDE_OK 유지

### caveat (비차단)
HIST-3 원 Handoff 원문 보관 누락 재발 — 이후 append 의무.

---

# CLAUDE → CURSOR · FULL-BT-HIST-3 Handoff (엔진 호출/TF/warmup 분리 계측)
# (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: HIST-2 원인 재판정 **OK**(엔진 미히트) · **FULL-BT-HIST-3** 진단 Handoff · 🟢
> **에스컬레이션**: 동일 trade_count=0 이슈 **3번째** 진단 — HIST-3도 미해결 시 HIST-4 금지·디렉터 에스컬레이션

---

## HIST-2 재판정 요약
- Ask1: 엔진 미히트 재판정 **OK** (hit=0 ∧ reject=0 · dry·10×2 결정론적 0)
- Ask2: **HIST-3** — 후보 (1) TF 갭 (2) warmup/lookback (3) 호출 경로 미실행 — **계측만·수정 아님**
- Ask3: 전체 유니버스 런 **금지 유지**

### Spec (요약)
- `engine_call_count[engine][symbol][mt][tf]` — 함수 진입
- `engine_call_outcome` — candidate / none / exception
- `tf_ohlcv_coverage[mt][tf] -> bool` — 하니스가 로드 가능한 TF
- `full_bt_diag` **확장 우선** · 하니스 wrapper만 · CAT-C/B 원본 금지
- dry(2~3) → 10×2 · pytest full_bt · OUTBOX에 4개 판정 정책 숫자 · WAIT_CLAUDE_OK

---

# CLAUDE → CURSOR · FULL-BT-HIST-2 Claude OK + VPS dry→10×2 실행 승인
# (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: **FULL-BT-HIST-2 검증 = OK (비차단 caveat 2건)** · **VPS dry→10×2 실행 승인** · 추가 코드 변경 없음
> **병행**: B1-LADDER-R1a OBSERVE 유지 (게이팅 없음)

---

## FULL-BT-HIST-2 검증 결과: OK

Spec 1~6 대조 통과. 위험도 🟢 유지. CAT-C/D 원본 비접촉 확인.

**비차단 caveat**
1. 다음 보고 1줄: `bitget_full_bt.sqlite` **FULL-BT 결과 테이블**(`bitget_forward_trades` 클론·report §2) 컬럼 목록 불변 여부 (paper 라이브 원장과 별개로 명시)
2. 기록용: Handoff는 harness-only였으나 batch.py·pilot.sh까지 FULL-BT 스캐폴드 확장 — 🔴 아님. 이후 유사 확장 시 사전 Ask

**실행 승인 (코드 변경 없음)**
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
BITGET_FULL_BT_MAX_SYMBOLS=3 bash bitget/deploy/run_full_bt_hist_pilot.sh
# 통과 시
BITGET_FULL_BT_MAX_SYMBOLS=10 bash bitget/deploy/run_full_bt_hist_pilot.sh
```
**전체 유니버스 런: 금지 유지**

### 결과 보고 (`CURSOR_TO_CLAUDE.md`)
- spot.diag / futures.diag — engine_hit_total · gate_reject_count
- hit=0 vs reject>0 분리 → HIST-1 병기 원인 재판정
- caveat 1 컬럼 불변 1줄 · paper before=after

---

# [CAT-Q] FULL-BT-HIST-1 파일럿 판정 → FULL-BT-HIST-2 진단 Handoff

> 이 파일은 3개 섹션으로 구성. 각 섹션을 해당 대상 파일에 반영.
> 대상: ① `bitget/docs/work_phases/CLAUDE_TO_CURSOR.md` ② `bitget/docs/work_phases/09_디렉터_쉬운요약.md` ③ `bitget/docs/work_phases/NEXT_STEP.md`

---

## ① CLAUDE_TO_CURSOR.md 반영분

### 판정: trade_count=0 → **미통과 (b) 진단 Handoff 필요**

| # | 항목 | 판정 |
|---|------|------|
| 1 | SPOT/FUT 정량 | 참고만 — 값 0 자체는 무해 |
| 2 | caveat | OK — 빈 테이블, 혼입 관측 없음 |
| 3 | 개선단서 | **FAIL** — step1~10 전부 0, side_asymmetry 전부 0 → 계측 무효 |
| 4 | paper | OK — before=after=10 |
| 5 | 배너 | OK |

**이유 (1줄):** gate_bottleneck 전부 0/N/A는 "거절이 없었다"와 "거절 이벤트가 기록 자체가 안 됐다"를 구분 못 함. Cursor 본인 보고에도 원인이 "엔진 hit 없음 또는 try_add 전량 미통과" 2택 병기 — 상호 배타적 원인을 현재 계측으로 특정 불가. 하니스는 완주했지만 "동작 검증"은 안 된 상태이므로 파일럿 목적(FULL-BT-0) 미충족.

**전체 유니버스 런: 금지 유지** (FULL-BT-0 §6 원칙 그대로).

---

### [CAT-Q] FULL-BT-HIST-2 — 원인 분리 계측 (진단 전용, 정책 변경 없음)

**sub-phase ID:** HIST-2

**SSOT (변경 금지)**
- 5종 엔진 / `forward/shared.py` try_add / `master_scanner.py` — 원본 수정 금지 (FULL-BT-0 비접촉 승계)
- 계측은 FULL-BT-1 하니스(read-only 드라이버) 레벨 Adapter로만 삽입

**Spec**
- `engine_hit_count[engine_name][symbol][market_type]` — 5종 엔진 + master_scanner pre-candidate hook이 candidate 생성한 횟수. 원본 콜 전/후 카운트하는 하니스 wrapper만.
- `gate_reject_count[step][market_type]` — `try_add_virtual_position(...)` 진입점 wrapper에서 반환/예외의 거절 사유를 CAT-D §4 step 1~10에 매핑해 카운트 (11=execution_safety real-only이므로 paper 경로는 그대로 N/A)
- 저장: `bitget_full_bt.sqlite` 신규 진단 전용 테이블(`full_bt_diag`) — 기존 결과 스키마 비접촉

**SPOT/FUT 분기**
공통 로직 + market_type 파라미터로 분리 집계, 합산 금지 (SPOT-FUT 표 원칙 승계)

**인접 CAT 영향**
- CAT-D: 읽기만 — `try_add_virtual_position` 반환/예외 관측, 내부 미수정 → Adapter
- CAT-C: 읽기만 — 엔진 호출 지점 wrapper → Adapter
- 🔴 Critical 아님 (진단 read-only, 실행/게이트 정책 변경 없음)

**롤백 조건**
- wrapper가 하니스 실행시간을 유의미하게 늘리면 제거 후 표본 축소 재시도

**Cursor 지시**
- Targeted diff only — 하니스 드라이버(FULL-BT-1 파일) 내부에만 wrapper 추가
- CAT-C/D 원본 파일 수정 금지
- 소표본(SPOT/FUT 각 2~3심볼) dry 확인 → 통과 시 10×2 재실행
- 테스트: `pytest bitget/tests/full_bt`

**세션 종료 의무**
- `05_진행로그.md` HIST-2 섹션
- `00_전체현황판.md` Phase·SSOT
- `CURSOR_TO_CLAUDE.md` 결과 회신
- `NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

**위험도:** 🟢 (읽기 전용 계측, 정책/실행 로직 변경 없음)

---

---

# CLAUDE → CURSOR · FULL-BT-HIST-1 Claude OK + 파일럿 실런 지시
# (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-25 · [CAT-Q]
> **상태**: **FULL-BT-HIST-1 검증 = OK (비차단 caveat 1건)** · 전체런 전 **파일럿 실런 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## FULL-BT-HIST-1 검증 결과: OK

판정 근거:
- 재사용 소스 `bitget.analysis.universe_bt.replay._load_ohlcv` — 신규 근사·로직 창조 없음(룰5)
- entry/exit **캔들축** 전환 — `15_FULL-BT_전체이식가상매매.md` §6 로드맵 목표("`run_replay` 실제 OHLCV 바 워크, 캔들축")와 정확히 일치
- 산출 3파일(`harness.py` 바 워크, `report.py` Adapter, 테스트) — 신규 파일 범위 내. CAT-C 엔진풀·CAT-D try_add 11단계·CAT-E exit 3파일 원본 로직 재작성 없음(호출만) 재확인
- 비접촉 리스트(`forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·paper·batch/checkpoint) 전부 diff 없음 확인 — FULL-BT-1/2/3 헌법 그대로 승계
- SSOT 충돌(entry/exit 축 전환으로 기존 wall-window report 필터 무효화) → 원본 수정이 아닌 **Adapter(`CANDLE_ENTRY_AXIS`)** 제안 — 룰6 준수
- batch 호환 시그니처 유지(`run_replay(market_type, symbol, engine, start, end, db_path, *, market_db=None)`) — FULL-BT-2 오케스트레이터 재작성 불필요
- 테스트 **14 passed**(기존 10 + 신규 4: multi-bar exit·candle≠wall·SPOT/FUT 소스·pilot resume)
- 루트 주식 경로 무접촉, `bitget/full_bt/` 하위만

**참고(비차단 — 다음 보고에서 1줄 확인 요청)**:
`bitget_full_bt.sqlite`는 `paths.py`상 **공유**(run_id 미포함 물리 파일)다. `CANDLE_ENTRY_AXIS=True`로 wall-window 필터를 끄면 집계는 `checkpoint 완료 심볼 ∩ market_type`만으로 좁혀지는데, **결과 테이블 자체에 run_id 컬럼이 없다면** 같은 심볼을 처리한 서로 다른 run_id(예: 이번 파일럿 vs 이후 전체런)의 트레이드가 같은 결과 테이블에 누적될 때 report가 **다른 run의 트레이드까지 합산**할 위험이 있음 — FULL-BT-3 보완 (A)가 원래 막으려던 것과 동일 클래스 문제. **코드 변경 요청 아님.** 아래 파일럿 실행 결과에 "결과 테이블 run_id 컬럼 존재 여부" + "이번 run 트레이드 건수 vs 결과 테이블 전체 건수" 1줄만 같이 보고.

또한 원본 FULL-BT-HIST-1 Handoff 텍스트가 `CLAUDE_TO_CURSOR.md`에서 검색되지 않음(과거 UTF-8 손상 이력과 유사 — 문서 보관 공백 가능). 이번 OUTBOX 내용은 `15_FULL-BT §6` 로드맵과 자체 정합하므로 검증 차단 사유는 아니나, 다음 세션 종료 시 본 파일 자체를 그대로 `CLAUDE_TO_CURSOR.md`에 보관해 재발 방지 권장(비차단).

**다음**: 전체 유니버스×전체 히스토리 런 전, 아래 **파일럿** 먼저 실행.

---

## 파일럿 실런 지시 (신규 코드 없음 · 실행만)

- **market_type**: SPOT, FUTURES 각 1회 (분리 실행, 합산 실행 금지 — §4)
- **max_symbols**: **10** (U3 VPS 파일럿 재사용값 — 신규 상수 아님, 룰5)
- **run_id**: 신규 생성(pilot 접두, 예: `pilot-{ts}`) — 기존 run_id 재사용 금지(체크포인트 오염 방지)
- **resume**: true (기본값 그대로 — 재개 idempotency 실사용 확인 겸함)

### 실행 후 보고 (`CURSOR_TO_CLAUDE.md`에 1줄씩)
1. SPOT/FUT 각 `trade_count`·`total_return_pct`·`mdd_pct` (`report.py` §2 스키마 그대로, 재계산·해석 금지)
2. 위 caveat 확인: 결과 테이블 run_id 컬럼 유무 + (이번 run 트레이드 수) vs (테이블 전체 행 수) 비교
3. `gate_bottleneck_by_step`·`side_asymmetry` 슬롯 값 채워지는지(§2 개선단서, N/A 고정 아님 확인)
4. paper `bitget_forward_trades`(라이브 원장) before=after 재확인 — FULL-BT 공통 헌법(§5) 불변 재확인
5. 상단 고정 배너("IV L1 전체이식 가상매매...") 원문 그대로 출력되는지

### 비접촉 (재확인)
`forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·`bitget_forward_trades`(라이브 paper)·CAT-B/C/D/E/F/G/N 원본 — 파일럿은 **실행만**, diff 없음.

### 인접 CAT 영향
CAT-D/E: 없음(원본 참조·호출만, 변경 아님 — 룰7 Critical 대상 아님) · CAT-B: 읽기만(OHLCV) · CAT-F/G/N: 비접촉.

### 롤백 조건
파일럿 run_id + 해당 checkpoint/결과 행 삭제만으로 완전 롤백. HIST-1 코드·FULL-BT-1/2/3·paper DB·config_kv·원본 CAT 무영향.

### 위험도
🟢 (read-only 파일럿 · 실자금/config_kv 미접촉 · 결과는 L1 참고용 라벨 고정)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-HIST-1 파일럿 섹션(위 1~5 숫자 그대로)
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

---

*버전 2026-08-25 · FULL-BT-HIST-1 OK + 파일럿 지시 · Architect: Claude Pro · Engineer: Cursor*

---
---

# (룰13) 디렉터 문서 갱신본 — 그대로 덮어쓰기

## `09_디렉터_쉬운요약.md`

```markdown
# 디렉터용 쉬운 요약 (비개발자 OK)

> 갱신: 2026-08-25 · FULL-BT-HIST-1(진짜 시세 연결) 통과 · 소규모 시험(파일럿) 지시

## 지금 한 줄
"과거 진짜 시세로 진짜처럼 돌려보는" 마지막 퍼즐 조각(FULL-BT-HIST-1)이 검증을 통과했습니다.
다만 큰 규모로 돌리기 전에, **코인 10개짜리 소규모 시험(파일럿)**을 먼저 한 번 돌려서
숫자가 이상 없이 나오는지 확인하는 단계입니다. 자동차로 치면 "고속도로 타기 전 동네 한 바퀴 시운전"입니다.

신호등: 🟢 FULL-BT-0~3(골격) · 🟢 FULL-BT-HIST-1(진짜 시세 연결) · 🟡 **소규모 시험(파일럿) 대기** · 🟡 R1a 관측 계속

### 당신이 할 일
1. Cursor에게 이 파일 전달 → **코인 10개 시험 실행** 요청 (SPOT 1번, FUTURES 1번)
2. 결과 숫자(수익률/승률 아님, "참고용" 표) 나오면 Claude에게 다시 검증 요청
3. 매일 텔레그램 OPEN/CLOSED(R1a) 계속 확인
4. 이 시험 결과도 **"수익률 확정"이 아닙니다** — 상단에 항상 "참고용, 실전 증명 아님" 배너가 붙습니다. 진짜 합격 판정은 R6(실거래 56일+)에서만 나옵니다.
```

## `NEXT_STEP.md`

```markdown
# NEXT STEP

> 갱신: 2026-08-25 · FULL-BT-HIST-1 Claude 검증 OK(비차단 확인 1건) · 파일럿 실런 지시 발급

## 지금 상태
FULL-BT-HIST-1(실제 OHLCV 캔들축 바 워크) Claude 검증 **OK**. 전체 유니버스 런 전 **파일럿(max_symbols=10, SPOT/FUT 각 1회)** 먼저 실행하도록 지시. 비차단 확인 1건(결과 테이블 run_id 스코프)은 파일럿 결과 보고에 포함.

## 다음 행동
1. Cursor: 위 파일럿 실런 지시대로 SPOT/FUT 각 1회 실행, 5개 항목 `CURSOR_TO_CLAUDE.md`에 보고
2. Cursor: 세션 종료 시 `05_진행로그.md`/`00_전체현황판.md`/`CURSOR_TO_CLAUDE.md`/`NEXT_ACTION.md` 갱신 → `WAIT_CLAUDE_OK`
3. 디렉터: 파일럿 결과 오면 `CURSOR_TO_CLAUDE.md` 검증을 Claude에게 요청(비차단 확인 1건 포함 여부 체크)
4. 파일럿 통과 시에만 전체 유니버스×전체 히스토리 런 Handoff 진행 — 파일럿 생략한 전체런 착수 금지
5. 병행: R1a 매일 관측 유지, 게이팅 없음
6. FULL-BT 산출을 R6 대체·B1「달성」·LIVE 근거로 사용 금지 (전 단계 공통)
```

---

# CLAUDE → CURSOR · FULL-BT-HIST-1 Handoff (bitget/docs/work_phases/CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-24 · [CAT-Q]
> **상태**: Cursor **구현 대기** · `CURSOR_TO_CLAUDE.md` Ask(FULL-BT 실제 히스토리 바 워크) 해소용 Handoff
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## Ask 확인 (CURSOR_TO_CLAUDE.md)
FULL-BT-0~3은 골격(배치·체크포인트·리포트) 완료였으나 `harness.run_replay`가 `_ = (start, end)`로 **미사용**, hist가 **합성 120일 flat**, 청산이 바 워크가 아닌 **즉시 Adapter**라 전 코인×기간 PnL/MDD 방향 검증이 실행 불가하다는 갭 확인. 아래 Handoff로 실데이터 바 워크 전환.

---

## [CAT-Q] 진단&레거시 — FULL-BT-HIST-1 · `run_replay` 실제 OHLCV 바 워크 전환

### sub-phase ID
**FULL-BT-HIST-1** (FULL-BT-0~3 로드맵은 "트랙 종료"로 닫혀 있어 번호 재사용 대신 HIST 접두로 신규 sub-phase 구분 — 15_FULL-BT §6 로드맵 표에 행 추가만, 기존 행 재작성 금지)

### SSOT (변경 금지 unless noted)
- 수정(targeted diff): `bitget/full_bt/harness.py` — `run_replay` 내부만 교체, 시그니처·파일 위치 유지
- 조사 후 재사용(원본 비접촉): `bitget/analysis/universe_bt/` 내 OHLCV 로더(가칭 `_load_ohlcv`) — **정확 경로/시그니처는 Cursor 조사 후 보고**, 신규 로더 발명 금지(룰5)
- 원본 호출만(비변경 — diff 없음): CAT-C 엔진풀(`signal_engines.py`/`master_scanner.py`, FULL-BT-1과 동일 import 경로), CAT-D `forward/shared.py`의 `try_add`(11단계, step11 execution_safety는 real 전용 → paper replay N/A skip 그대로), CAT-E 3파일(`trading/position_manager.py`/`tail_risk_gate.py`/`mega_trend_kill_bg.py`) evaluate
- 변경 없음: `forward/ledger.py`, config_kv, `bitget_forward_trades`(paper), FULL-BT-2 batch/checkpoint 로직, FULL-BT-3 `report.py` 스키마(§2 키 그대로), CAT-B/F/G/N 원본

### 변경 Spec

**함수 시그니처 — 신규 없음, 내부 구현만 실동작화**
```
run_replay(symbol: str, market_type: str, start: int, end: int) -> list[dict]
```

**정책 (Ask 1~6 대응)**
1. **OHLCV 소스**: universe_bt 로더 재사용 우선 조사 → 성공 시 import, 실패/구조 불일치 시 Adapter 제안 후 보고(신규 로더 발명 금지, 룰5). `market_type`별 소스 테이블(`BITGET_SPOT_*`/`BITGET_FUT_*`, `14_UNIVERSE-BT` §1 관례 재사용).
2. **바 워크 루프**: `[start,end]` 구간 각 바에서 — (a) 미보유 심볼 → CAT-C 원본 candidate 생성 → `try_add` 원본 순서 그대로(step11 N/A skip) → 통과 시 격리 DB에 OPEN Adapter write, (b) 보유 심볼 → CAT-E 3파일 evaluate **원본 호출**을 그 바마다 순차 평가 → 트리거 시 CLOSED Adapter write. (현재의 "즉시 Adapter" 방식 폐기 — 실제 경과 바 수만큼 평가)
3. **시간축**: `entry_date`/`exit_date`는 **캔들 타임스탬프**(바 자체 시각) 채택, wall-clock(now()) 아님. 사유 — backtest는 과거 재생이므로 wall-clock을 쓰면 모든 run이 실행 시점 동일 값이 되어 §2 `period_start`/`period_end` 정량표 기간 대조가 무의미해짐. `updated_at`(기록 시각)은 기존처럼 wall-clock 유지, entry_date/exit_date만 분리. Cursor는 FULL-BT-3 `report.py` 공유DB 필터가 현재 어떤 축을 쓰는지 재조사 후 정합 여부 1줄 보고(불일치 시 `report.py` Adapter만, 스키마 재작성 아님).
4. **스코프**: `max_symbols` 파라미터로 소규모 파일럿 먼저(기존 `build_full_bt_shards` 재사용, 신규 파라미터화 최소) → 통과 후 전 유니버스 확장. 메모리 캡은 FULL-BT-2 기 확정값(`TIME_MACHINE_MAX_TABLES`/`TIME_MACHINE_MAX_BARS_PER_TABLE`, `bitget.infra.memory_policy`) 그대로 재사용, 재정의 금지(룰5).
5. **완료 정의**: 격리 DB(`bitget_full_bt.sqlite`)에 실데이터 기반 row 존재 + `generate_full_bt_l1_report(market_type, run_id)` 호출 시 §2 정량표(SPOT/FUT 분리)에 non-flat 실측값 산출. **"합격/달성" 판정 문구 금지**(15_FULL-BT §3 Kill, 배너 원문 그대로 유지).
6. **비접촉**: `forward/ledger.py`·`forward/shared.py`·`signal_engines.py`·exit 3파일 **원본** · config_kv · paper 원장 · CAT-J 미편입(§5 로드맵 그대로).

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지(FULL-BT-0~3과 동일 헌법 승계)

### SPOT/FUT 분기
- `market_type` 파라미터 관통, 공통 바 워크 로직에 하드코딩 금지(`CAT-SPOT-FUT_비대칭표` 인용만)
- OHLCV 소스 테이블만 market_type별 분기(정책1) — 그 외 바 루프/try_add/exit 로직은 공통
- SPOT SHORT는 ledger hard reject로 자연 0건 유지(FULL-BT-1/3 선례 그대로, 신규 분기 불필요)

### 인접 CAT 영향
- **CAT-C**: 없음 — 엔진풀 원본 import·호출만, diff 없음
- **CAT-D**: 없음 — `try_add` 원본 호출만, `forward/shared.py` diff 없음 → **🔴 Critical 아님**(룰7: 원본 "변경" 시에만 대상, 본 Handoff은 읽기/호출 전용)
- **CAT-E**: 없음 — 3파일 evaluate 원본 호출만, diff 없음 → **🔴 Critical 아님**(동일 사유)
- **CAT-B/F/G/N**: 없음, 비접촉
- **CAT-J**: 없음, 미편입 유지(FULL-BT-3 §5 로드맵 그대로)
- **Track B(B1-LADDER)**: 없음, 병렬 독립 · 우선순위 R1a보다 낮음

### 롤백 조건
- `bitget/full_bt/harness.py` diff만 revert(git) — CAT-C/D/E 원본 무영향
- 격리 DB(`bitget_full_bt.sqlite`) 해당 run_id row 삭제만으로 데이터 롤백 — paper/config_kv 무영향
- 파일럿(`max_symbols` 소규모) 단계 이상 발견 시 전체 유니버스 확장 보류(정책4)

### Cursor 지시
- Targeted diff only — `bitget/full_bt/harness.py` 내부 로직만. CAT-C/D/E 신호엔진·try_add·exit 3파일 **원본 diff 금지**, import·호출만.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- universe_bt OHLCV 로더 정확 경로/시그니처 조사 후 `CURSOR_TO_CLAUDE.md`에 `"재사용 소스: {실제 경로/함수명}"` 1줄 보고(임의 명명 금지, 룰5).
- entry_date/exit_date 캔들축 전환과 `report.py` 필터 정합 여부 1줄 보고(정책3).
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/full_bt/`(신규 케이스 — 다중 바 경과 후 exit 트리거 확인 · entry_date≠updated_at 케이스 · SPOT/FUT 소스 분기 · `max_symbols` 파일럿 idempotent resume 필수 포함)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-HIST-1 섹션
- `bitget/docs/work_phases/00_전체현황판.md` SSOT 용어집에 `FULL-BT-HIST-1` 행 추가
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신(재사용 소스 보고 + entry_date 정합 여부)
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `bitget/docs/work_phases/09_디렉터_쉬운요약.md` / `NEXT_STEP.md` — 아래 갱신본 그대로 반영(룰13)

### 위험도
🟡 (신규 실데이터 바 루프 로직 · 결과는 격리 DB만 · CAT-D/E는 원본 호출뿐 diff 없음 · config_kv/paper 무접촉이나 다중바 상태추적 복잡도로 FULL-BT-3 대비 상향)

---

*버전 2026-08-24 · FULL-BT-HIST-1 Handoff · Architect: Claude Pro · Engineer: Cursor*

---
---

# (룰13) 디렉터 문서 갱신본 — 그대로 덮어쓰기

## `09_디렉터_쉬운요약.md`

```markdown
# 디렉터용 쉬운 요약 (비개발자 OK)

> 갱신: 2026-08-24 · FULL-BT 트랙 재가동(진짜 시세 버전) Handoff 발급

## 지금 한 줄
지난번 "과거로 돌려본 결과 보고서"는 사실 진짜 시세가 아니라 평평한 연습용 가짜 데이터로 만든 것이었습니다(모의 훈련용 지도로 길을 그려본 것과 비슷해요). 이번엔 **진짜 과거 시세**로 다시 돌리는 작업을 Cursor에게 요청했습니다.

신호등: 🟡 **FULL-BT-HIST-1 = Cursor 구현 대기(진짜 시세 연결)** · 🟢 이전 골격(FULL-BT-0~3, 배치·저장·보고서 틀)은 그대로 재사용 · 🟡 R1a 관측 계속

## 비유로 설명
- 이전 결과 = 종이 위에 그려본 시뮬레이션 지도
- 이번 결과 = 실제 GPS 기록(진짜 과거 캔들)으로 같은 지도를 다시 그리는 것
- "진짜 참고용" 숫자는 이번 작업이 끝나야 나옵니다. 이전 숫자는 참고조차 되지 않습니다(전부 가짜 평지 데이터였음).

### 당신이 할 일
1. Cursor에게 이 Handoff(FULL-BT-HIST-1) 전달 → 진짜 시세 연결 구현 요청
2. 먼저 코인 몇 개만(파일럿) 돌려보고 이상 없으면 전체로 확대 — 처음부터 전체를 돌리지 않습니다
3. 완료되면 Claude에게 다시 검증 요청 (지금과 같은 방식)
4. 결과가 나와도 여전히 **"수익률 확정 아님"** — 진짜 합격 판정은 실거래 56일+(R6)에서만 나옵니다
5. 매일 텔레그램 OPEN/CLOSED(R1a) 계속 확인
```

## `NEXT_STEP.md`

```markdown
# NEXT STEP

> 갱신: 2026-08-24 · FULL-BT-HIST-1 Handoff 발급(실제 OHLCV 바 워크)

## 지금 상태
FULL-BT-0~3(골격·배치·체크포인트·리포트)은 완료였으나, `harness.run_replay`가 합성(가짜) OHLCV 스모크였음이 확인됨. 실제 히스토리 바 워크로 전환하는 **FULL-BT-HIST-1** Handoff 발급, Cursor 구현 대기.

## 다음 행동
1. Cursor: 위 FULL-BT-HIST-1 Handoff 기준 `bitget/full_bt/harness.py` 실데이터 바 워크 구현
2. Cursor: universe_bt OHLCV 로더 재사용 경로 조사 후 `CURSOR_TO_CLAUDE.md`에 1줄 보고
3. Cursor: `max_symbols` 소규모 파일럿 먼저 → 통과 후 전 유니버스 확장
4. Cursor: 세션 종료 시 `05_진행로그.md`/`00_전체현황판.md`/`CURSOR_TO_CLAUDE.md`/`NEXT_ACTION.md` 갱신 → `WAIT_CLAUDE_OK`
5. 디렉터: Cursor 완료 보고 오면 `CURSOR_TO_CLAUDE.md` 검증을 Claude에게 요청
6. 병행: R1a 매일 관측 유지, 게이팅 없음
7. FULL-BT 산출을 R6 대체·B1「달성」·LIVE 근거로 사용 금지(전 단계 공통, 실데이터 전환 후에도 동일)
```

---
---

*본 파일은 `bitget/docs/work_phases/CLAUDE_TO_CURSOR.md`의 기존 최상단(현재 "FULL-BT-3 보완 검증 결과 + 트랙 종료" 블록) 바로 위에 그대로 붙여넣기 위한 prepend 블록입니다. Claude Pro는 실제 리포지토리 파일에 직접 쓰기 권한이 없어(프로젝트 지식은 읽기 전용 미러) 이 파일로 대신합니다 — 디렉터 또는 Cursor가 붙여넣기.*
# CLAUDE → CURSOR · FULL-BT-3 보완 검증 결과 + 트랙 종료

> **작성**: Claude Pro (Architect) · 2026-08-24 · [CAT-Q]
> **상태**: **FULL-BT-3 보완 검증 = OK** · FULL-BT-0~3 로드맵 전체 완료 → **트랙 종료**

## 판정: OK
- (A) 공유 DB 시간창 필터, (B) 미측정 각주 — 원 스펙 항목4/항목3과 상충 없음
- 비접촉 리스트 전부 확인 · 테스트 10 passed
- 신규 코드 요청 없음 · 잔여 하드 격리는 디렉터 별도 Handoff만

## 비차단 확인 (Cursor 이행)
updated_at/entry_date/exit_date wall-clock 동일 축 — OUTBOX 1줄 보고

## 다음 상태
FULL-BT 트랙 **종료** · 우선순위 B1-LADDER R1a OBSERVE 복귀

---
---

# CLAUDE → CURSOR · FULL-BT-3 Handoff (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **FULL-BT-2 검증 = OK** · **FULL-BT-3 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## FULL-BT-2 검증 결과: OK

판정 근거:
- 산출물(`batch.py`/`checkpoint.py`) — Handoff 함수 시그니처 골격(run_full_bt_batch·shards·window batches·checkpoint)과 일치
- 재사용값 `TIME_MACHINE_MAX_TABLES=300`·`TIME_MACHINE_MAX_BARS_PER_TABLE=5000`, 출처 `bitget.infra.memory_policy` — U2 재사용 확인값과 동일, 신규 상수 없음(룰5)
- 정책 승계: `harness.run_replay` 재사용만·TF `['1D','4H','2H','1H']`·funding 미추적·국면 UNKNOWN·step11 N/A skip — FULL-BT-1 OK 판정과 100% 일치, 재조사·확장 없음
- 엔진 5종 확인(`EMA5`/`MASTER`/`NULRIM`/`TV_SHORT_V1`/`TV_SHORT_V2`, `_build_engine_pool` 원본 import) — FULL-BT-1 OK 시 권장했던 선택 보고 항목 이행
- resume idempotency: 최초 `paper_count=2` 유지 → 재실행(동일 run_id) 시 `batches_run=0`·`batches_skipped=n1` — 완료분 skip·중복 삽입 없음 확인(Handoff 정책 5항)
- 비접촉: `forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·paper 원장·FULL-BT-1 harness 로직 재작성 없음 — 전부 확인
- 테스트: resume + paper 케이스 포함 **4 passed** — Handoff 필수 케이스 충족
- 루트 주식 경로 무접촉, `bitget/full_bt/` 하위만

**참고(비차단)**: 결과 격리 테이블의 실제 컬럼명(FULL-BT-1 `paths.py`/`harness.py` 확정본)은 OUTBOX에 별도 명시 안 됨 — FULL-BT-2는 체크포인트 테이블만 다루므로 이번 단계엔 불필요. FULL-BT-3 착수 시 재확인 필요(아래 Handoff에 반영).

**다음:** `05_진행로그.md`에 이 OK 기록 · 아래 FULL-BT-3 Handoff로 진행.

---

## [CAT-Q] 진단&레거시 — FULL-BT-3 리포트 (§2 스키마 · CAT-J 비편입)

### sub-phase ID
FULL-BT-3

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/full_bt/report.py` (기존 `full_bt/` 컨벤션에 맞춰 Cursor 배치)
- 참조만(원본 비접촉): FULL-BT-1 격리 결과 테이블(`bitget_full_bt.sqlite` 내부 — 정확 테이블/컬럼명은 `paths.py`/`harness.py` 확정본 기준, 재조사 후 보고, 임의 명명 금지 룰5) + `bitget_full_bt_checkpoint`(FULL-BT-2, 완료 batch만 집계 대상 판별용), `13_B1_신뢰사다리.md` §1(인용만), `CAT-SPOT-FUT_비대칭표.md`(인용만)
- 변경 없음: `forward/ledger.py`, `forward/shared.py`, `signal_engines.py`, `master_scanner.py`, exit 3파일, config_kv, `bitget_forward_trades`, FULL-BT-1/2 코드 전체, CAT-B/C/D/E/F/G/N 원본

### 변경 Spec

**함수 시그니처 (골격만)**
```
generate_full_bt_l1_report(market_type: str, run_id: str) -> dict
render_full_bt_l1_report_md(report: dict) -> str   # 배너 고정 + §2 정량표/개선단서만
```

**정책**
1. 상단 고정 배너를 `render_full_bt_l1_report_md` 최상단에 원문 그대로 삽입(15_FULL-BT §3, 요약·재작성 금지):
   `"IV L1 전체이식 가상매매 — 격리 리플레이 결과, LIVE 승격·R6 대체·B1「달성」 판정 금지. 공식 B1 판정은 R6(L2 forward 56일+)만."`
2. PnL/MDD 정량표 키(§2) 그대로: `run_id, market_type, symbol_or_agg, period_start, period_end, total_return_pct, mdd_pct, trade_count, b1_reference_band`. `b1_reference_band`는 고정 문자열 `"12~18%/≤5%, 참고용 — 판정 아님"`(13_B1 §1 인용만, 수치 재계산·재해석 금지).
3. 개선 단서 슬롯(§2) 키 그대로: `gate_bottleneck_by_step`(try_add 11단계별 거절 카운트, step11은 N/A 고정), `side_asymmetry`(LONG/SHORT 진입·거절), `symbol_breakdown`(top rejected/entered), `tf_note`(재사용 TF 1줄).
4. 집계 대상은 `bitget_full_bt_checkpoint`상 완료 표시된 (symbol×batch)만 — 미완료 run_id 부분 집계 시 report에 "미완료 run — 부분 결과" 경고 문자열 포함(체크포인트 완료 플래그 그대로 필터, 신규 판정 로직 발명 아님).
5. Kill(§2) 준수: CAGR 과신·승률 단정·연복리 환산 과대표현 금지 — 정량표·개선단서 슬롯 값 그대로만 출력, 해석성 자유서술 삽입 금지.
6. CAT-J 비편입: `reports/` 등 독립 디렉터리 산출물로만 존재, 리포팅 파이프라인 등록·자동 트리거 연결 금지(U3와 동일 원칙, §5 로드맵 그대로).

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지(FULL-BT-1/2와 동일)

### SPOT/FUT 분기
- `market_type` 파라미터 관통, 하드코딩 금지(§4)
- 리포트는 SPOT·FUT **분리 집계 후 나란히 제시** — 합산 금지(§4)
- SPOT: SHORT는 ledger hard reject로 `trade_count=0` 자연 발생(특수분기 불필요), `side_asymmetry`는 U3 `side_asymmetry_ratio` null 처리 선례 준용(신규 정의 금지)

### 인접 CAT 영향
- **CAT-J**: 없음 — 읽기도 아님, 파이프라인 미등록(§5 로드맵 "CAT-J 비편입" 그대로)
- **CAT-B/C/D/E/F/G/N**: 없음 — FULL-BT-1/2와 동일 비접촉 헌법 유지, 결과 테이블 읽기 전용
- **Track B(B1-LADDER)**: 없음, 병렬 독립. `b1_reference_band`는 13_B1 §1 인용 표기일 뿐 B1 판정에 미반영(§3 Kill: R6 대체 금지)

CAT-F/G/N/B/D 관련 — 본 Handoff은 위 CAT들을 변경하지 않고 원본/결과 참조만 하므로 🔴 Critical 아님(룰7: 변경 시에만 Critical 판정 대상).

### 롤백 조건
신규 `report.py` 파일 삭제만으로 완전 롤백. FULL-BT-1/2 하니스·배치·체크포인트·paper DB·config_kv·원본 CAT 코드 무영향.

### Cursor 지시
- Targeted 신규 파일만. FULL-BT-1/2/CAT-B/C/D/E 원본 파일 diff 금지 — 읽기 전용 쿼리만.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- FULL-BT-1 격리 결과 테이블 실제 컬럼명(`paths.py`/`harness.py` 확정본) 재조사 후 `CURSOR_TO_CLAUDE.md`에 "결과 테이블: {실제명}, 컬럼 매핑: {...}" 1줄 보고(임의 컬럼명 금지, 룰5)
- 상단 배너·`b1_reference_band` 문자열은 원문 그대로 복사 — 요약·재계산 금지
- 충돌 시 Adapter 제안 후 디렉터 Ask
- 테스트: `pytest bitget/tests/full_bt/` (신규 — 정량표 키 존재 확인 + SPOT/FUT 분리집계 케이스 + 배너/Kill 문구 고정 확인 + 미완료 run 부분결과 경고 케이스 필수 포함)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-3 섹션
- `bitget/docs/work_phases/00_전체현황판.md` Phase·SSOT
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `bitget/docs/work_phases/09_디렉터_쉬운요약.md` / `NEXT_STEP.md` — 아래 갱신본 그대로 반영(룰13)

### 위험도
🟢 (read-only 리포트 생성 · 원본 코드/DB 쓰기 없음 · CAT-F/G/N Critical 코드 비접촉)

---

*버전 2026-08-23 · FULL-BT-3 Handoff · Architect: Claude Pro · Engineer: Cursor*

---
---

# (룰13) 디렉터 문서 갱신본 — 그대로 덮어쓰기

## `09_디렉터_쉬운요약.md`

```markdown
# 디렉터용 쉬운 요약 (비개발자 OK)

> 갱신: 2026-08-23 · FULL-BT-2 통과 · FULL-BT-3(결과 보고서) Handoff 발급

## 지금 한 줄
전체 코인×기간을 나눠 돌리고 끊기면 이어서 하는 배치(FULL-BT-2)가 검증을 통과했습니다.
이제 마지막 단계, **"과거로 돌려본 결과를 숫자표로 정리하는 보고서(FULL-BT-3)"**를 Cursor가 만들 차례입니다.

신호등: 🟢 FULL-BT-1 통과 · 🟢 FULL-BT-2 통과 · 🟡 **FULL-BT-3 = Cursor 구현 대기** · 🟡 R1a 관측 계속

### 당신이 할 일
1. Cursor에게 이 Handoff 파일 전달 → FULL-BT-3(보고서) 구현 요청
2. 완료되면 Claude에게 다시 검증 요청 (지금과 같은 방식)
3. 매일 텔레그램 OPEN/CLOSED(R1a) 계속 확인
4. 보고서가 나와도 **"수익률 확정"이 아닙니다** — 상단에 항상 "참고용, 실전 증명 아님" 배너가 붙습니다. 진짜 합격 판정은 R6(실거래 56일+)에서만 나옵니다.
```

## `NEXT_STEP.md`

```markdown
# NEXT STEP

> 갱신: 2026-08-23 · FULL-BT-2 Claude 검증 OK · FULL-BT-3 Handoff 발급

## 지금 상태
FULL-BT-2(배치+체크포인트) Claude 검증 **OK**. FULL-BT-3(§2 스키마 리포트 · CAT-J 비편입) Handoff 발급 완료, Cursor 구현 대기.

## 다음 행동
1. Cursor: 위 FULL-BT-3 Handoff 기준 `bitget/full_bt/report.py` 구현
2. Cursor: 세션 종료 시 `05_진행로그.md`/`00_전체현황판.md`/`CURSOR_TO_CLAUDE.md`/`NEXT_ACTION.md` 갱신 → `WAIT_CLAUDE_OK`
3. 디렉터: Cursor 완료 보고 오면 `CURSOR_TO_CLAUDE.md` FULL-BT-3 검증을 Claude에게 요청
4. 병행: R1a 매일 관측 유지, 게이팅 없음
5. FULL-BT 산출을 R6 대체·B1「달성」·LIVE 근거로 사용 금지 (전 단계 공통)
```

---

﻿# CLAUDE → CURSOR · FULL-BT-2 Handoff (기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **FULL-BT-1 검증 = OK** · **FULL-BT-2 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음, 우선순위 R1a보다 낮음)

---

## FULL-BT-1 검증 결과: OK

판정 근거:
- 엔진 풀: CAT-C `_build_engine_pool` **원본 import** 확인, diff 없음 — 스펙 "signal_engines 원본 그대로" 준수
- candidate→진입: `try_add` **원본 호출**, step11(`execution_safety`, real 전용)은 paper replay에서 자연 N/A skip — 디렉터 원문 "게이트13" 용어 정정(=CAT-N execution_safety, paper 범위 아님) 정확 반영
- 청산: CAT-E 3파일(`position_manager`/`tail_risk_gate`/`mega_trend_kill_bg`) evaluate **원본 import**, CLOSED write만 격리 Adapter — 스펙 일치
- TF 조사: `재사용 TF: ['1D','4H','2H','1H']` — 출처 `master_scanner.TIMEFRAMES` 명시, 임의 확장 없음(룰5)
- funding 조사: (a)/(b)/(c) 모두 확인 후 "추적 없이 진행" 채택, 근사치 창조 없음(룰5), P1-3 미차감 라이브와 동일 승계
- 격리 검증: 격리 DB row 증가 · paper `bitget_forward_trades` **before=after** · config_kv 비접촉 — U1과 동일한 물리적 격리 원칙 충족
- 비접촉 리스트(`forward/ledger.py`·`shared.py`·`signal_engines`·exit 3파일·config_kv·paper 원장) 전부 원본 diff 없음 확인
- 테스트 1 passed(smoke) — FULL-BT-1 범위(read-only 하니스 존재 확인)에 충분

**참고(비차단, 기록용)**: OUTBOX가 "5종 엔진 + master_scanner C-1 pre-candidate hook" 커버리지를 항목별로 명시하진 않았음(`_build_engine_pool` import라고만 보고). diff 없음이 확인된 이상 안전성 문제는 아니며, 실제 엔진 커버리지는 FULL-BT-3 리포트의 `symbol_breakdown`/엔진별 집계에서 자연히 드러날 사항이라 이번 read-only 검증 단계의 필수 차단 요건은 아님. FULL-BT-2 세션 종료 보고 시 "관여 엔진 5종 확인" 1줄 추가를 권장(선택).

---

## [CAT-Q] 진단&레거시 — FULL-BT-2 배치·체크포인트 (전체 유니버스×전체 히스토리 확장)

### sub-phase ID
FULL-BT-2

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/full_bt/` 하위 (배치 오케스트레이터 — 기존 디렉토리 컨벤션에 맞춰 Cursor 배치)
- 신규 테이블(격리 DB 내부): `bitget_full_bt_checkpoint` (`bitget_full_bt.sqlite` 전용, paper DB와 무관)
- 참조만(원본 비접촉): FULL-BT-1 `harness.run_replay`(및 그 내부 CAT-C 엔진풀·CAT-D try_add 11단계·CAT-E exit 3파일 import 경로 그대로), `bitget.infra.memory_policy`의 `TIME_MACHINE_MAX_TABLES`/`TIME_MACHINE_MAX_BARS_PER_TABLE`(U2에서 재사용 확인된 값 — **재사용만, 재정의 금지**, 룰5)
- 변경 없음: `forward/ledger.py`, `forward/shared.py`, `signal_engines.py`, `master_scanner.py`, `trading/position_manager.py`/`tail_risk_gate.py`/`mega_trend_kill_bg.py`, config_kv, `bitget_forward_trades`(paper ledger), CAT-B/C/D/E/F/G/N 원본 코드 전체, FULL-BT-1의 국면 처리(UNKNOWN 고정)·funding 미추적 정책

### 변경 Spec

**함수 시그니처 (골격만)**
```
run_full_bt_batch(market_type: str, run_id: str, resume: bool = True) -> None
build_full_bt_shards(symbols: list[str], shard_size: int) -> list[list[str]]
get_full_bt_window_batches(symbol: str, market_type: str, batch_size: int) -> list[tuple[int, int]]
load_full_bt_checkpoint(run_id: str, market_type: str) -> dict | None
save_full_bt_checkpoint(run_id: str, market_type: str, symbol: str, batch_idx: int) -> None
```

**정책**
1. `run_full_bt_batch`는 FULL-BT-1 `harness.run_replay`를 **원본 그대로 재사용**하는 상위 오케스트레이터. 내부 로직(엔진풀 호출·try_add 11단계·exit 3파일 evaluate·CLOSED Adapter write) 재작성·복제 금지 — 배치/체크포인트 Adapter만 추가.
2. 유니버스 스냅샷을 `build_full_bt_shards`로 분할 — `shard_size`는 `TIME_MACHINE_MAX_TABLES`(U2 재사용값=300) 그대로. Cursor가 codebase 재확인 후 "재사용값: {실제값}" 1줄 보고(임의 값 금지, 룰5).
3. 심볼별 `get_full_bt_window_batches`로 시간축 배치 분할 — `batch_size`는 `TIME_MACHINE_MAX_BARS_PER_TABLE`(U2 재사용값=5000) 그대로.
4. 배치(심볼×윈도우) 완료마다 `save_full_bt_checkpoint` 기록 — 대상은 `bitget_full_bt.sqlite` 내부 `bitget_full_bt_checkpoint` 테이블만. config_kv·paper DB(`bitget_forward_trades`) 접촉 금지(FULL-BT-1 원칙 승계).
5. `resume=True` + 동일 `run_id` 체크포인트 존재 시 완료분 skip, 중단 지점부터 재개. 격리 결과 테이블 중복 삽입 방지(unique 제약 또는 사전 skip 로직).
6. 엔진풀(5종+`master_scanner` hook)·try_add 11단계(step11 N/A skip)·exit 3파일·funding 미추적·국면 UNKNOWN 고정 — FULL-BT-1과 **동일 유지**, 본 Handoff 범위 밖(재조사·확장 금지).
7. TF: FULL-BT-1 조사값 `['1D','4H','2H','1H']` 그대로 재사용, 확장 금지.
8. 실행 규모 확대(전체 유니버스×전체 히스토리)에 따라 paper DB(`bitget_forward_trades`) row count 불변 검증을 **배치 실행 전/후 + 샤드마다** 재확인(FULL-BT-1은 1회성 smoke, FULL-BT-2는 노출 시간 증가로 반복 확인 필요 — U2 원칙과 동일).

**신규 테이블 스키마 (키만)**
`bitget_full_bt_checkpoint`: `run_id, market_type, shard_index, completed_symbol, completed_batch_idx, updated_at`

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지 (FULL-BT-1과 동일)

### SPOT/FUT 분기
- `market_type` 파라미터 FULL-BT-1과 동일하게 관통 (하드코딩 금지)
- SPOT: SHORT는 ledger hard reject로 자연 0건(특수분기 불필요)
- FUTURES: LONG/SHORT 모두 기록
- SPOT·FUT 분리 집계 리포트는 FULL-BT-3 범위 — 본 phase는 실행·저장만

### 인접 CAT 영향
- **CAT-B**: 읽기만(OHLCV) — 규모 확대로 읽기량 증가, 쓰기 없음 불변
- **CAT-C**: 읽기만(엔진풀 원본 import 승계), 원본 수정 금지
- **CAT-D**: 참조만 — try_add 11단계 원본 호출 승계, 실 write(`bitget_forward_trades`) 절대 금지, 검증 빈도 상향(위 8항)
- **CAT-E**: 참조만 — exit 3파일 원본 evaluate 승계, CLOSED write는 격리 Adapter만
- **CAT-F/G/N**: 비접촉 — Kelly UNKNOWN cap·regime UNKNOWN·execution_safety step11 N/A skip 그대로 승계(재조사 없음)
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

CAT-F/G/N/B/D 관련 — 본 Handoff은 위 CAT들을 **변경하지 않고 원본 참조만** 하므로 🔴 Critical 아님(룰7: 변경 시에만 Critical 판정 대상).

### 롤백 조건
신규 오케스트레이터 파일 + `bitget_full_bt_checkpoint` 테이블 삭제만으로 완전 롤백. FULL-BT-1 하니스·paper DB·config_kv·원본 CAT 코드 무영향.

### Cursor 지시
- Targeted 신규 파일만. FULL-BT-1/CAT-B/C/D/E 원본 파일 diff 금지 — import만.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- `TIME_MACHINE_MAX_*` 정확 값·위치는 codebase 재조사 후 `CURSOR_TO_CLAUDE.md`에 "재사용값: {실제값}" 1줄 보고(임의 값 사용 금지).
- 하니스 실행 전후 + 샤드마다 paper DB(`bitget_forward_trades`) row count 대조, 세션 종료 보고에 숫자로 기록.
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/full_bt/` (신규 — 체크포인트 재개(resume) idempotency 케이스 + paper DB 불변 케이스 필수 포함)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` FULL-BT-2 섹션
- `bitget/docs/work_phases/00_전체현황판.md` Phase·SSOT
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md` 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`

### 위험도
🟡 Medium (원본 CAT 코드 비접촉·paper 격리 유지되나, 실행 규모·노출 시간 증가로 격리 실패 리스크 누적 — 위 8항 반복 검증 필수. 실자금·config_kv 미접촉이라 🔴 Critical 아님)

---

> **NOTE (Cursor 2026-08-23)**: PowerShell 붙여넣기 중 기존 INBOX UTF-8이 손상됨.
> 아래는 `git HEAD`의 직전 커밋본을 복구한 이력 스택이다.
> 미커밋이던 FULL-BT-0/1 Handoff 본문이 필요하면 Downloads에서 재붙여넣기 요청.

---

# CLAUDE → CURSOR · UNIVERSE-BT-U3 OK (상단)

## UNIVERSE-BT-U3 검증 결과: OK

판정 근거:
- 지표 범위 4종(hit/gate_pass/virtual_entry/side_asymmetry) 정확 승계, 지표4는 고정 N/A 문자열로만 표기 — 근사 대입 없음(룰5)
- 고정 배너 + 정량표만, 자유서술 없음 — §3 Kill 준수
- 분모0→null 기존 정책 재사용, 신규 정의 없음
- SPOT/FUT 분리 집계 나란히 제시, side_asymmetry_ratio SPOT=null 각주 준수(§4)
- CAT-J 비편입 확인 — reports/ 별도 디렉터리, 파이프라인 미등록(§5 로드맵 그대로)
- U1/U2·config_kv·paper DB·CAT-B/C/G/F/N/D 원본 무접촉
- 테스트 11 passed(U1 4 + U2 3 + U3 4) — 기존분 보존, 회귀 없음

**다음:** 지표4(`crash_window_forced_exit_rate`)는 규명 미확정 리서치형 문제로 범위 밖 유지. 착수는 디렉터 판단 후 별도 Handoff에서만 진행 — 이번 라운드 미승인.

**U0~U3 로드맵 완료.** UNIVERSE-BT L0 트랙은 지표4 제외 상태로 현재 라운드 종료.

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U2 검증 OK + U3 Handoff · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **U2 검증 = OK** · **U3 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음)
> **범위 밖**: 지표4(`crash_window_forced_exit_rate`) — 별도 Handoff 대기 (regime=UNKNOWN 구간 해석 미확정, 임의 대입 금지)

---

## UNIVERSE-BT-U2 검증 결과: **OK**

판정 근거:
- 재사용 상수 `TIME_MACHINE_MAX_TABLES=300` / `TIME_MACHINE_MAX_BARS_PER_TABLE=5000` — 출처 `bitget.infra.memory_policy` 확인, 신규 상수 없음(룰5)
- U1 원본(`replay_symbol_window` 등) 로직 재작성 없음 확인 — window≤5 우회는 바 단위 재호출(호출 패턴 변경)일 뿐 원본 비접촉
- 테스트 7 passed (U1 4 + U2 3) — 회귀 없음
- 정책 승계 일치: C3 regime=`UNKNOWN` · `exit_trigger=NULL` · 지표4 미재개 — U1 SSOT(§5 로드맵 C3 조건)와 동일
- paper DB before=after=**3**, resume 2회차 `rows_written=0`, result COUNT 불변 — 격리·재개 안전성 확인, U0~U3 공통 비접촉 헌법(paper/config_kv) 위반 없음
- market_type 하드코딩 신규 유입 없음 확인 (§4)

**다음: 아래 U3 Handoff.**

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U3 리포트 (L0 정량표, 지표4 제외)

### sub-phase ID
UNIVERSE-BT-U3

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/analysis/universe_bt/u3_report.py`
- 참조만(읽기 전용, 원본 비접촉): `bitget_universe_bt.sqlite`(U1/U2 산출 — `bitget_universe_bt_results`, `bitget_universe_bt_checkpoint`), `14_UNIVERSE-BT_구조생존검증.md` §2·§3·§4(인용만, 표 재작성 금지)
- 변경 없음: config_kv, paper DB(`bitget_forward_trades`), CAT-C/B/G/F/N/D 원본, CAT-J 리포팅 파이프라인(§5 로드맵 "CAT-J 인접·편입 아님" 그대로)

### 변경 Spec

**함수 시그니처 (골격만)**
```
generate_universe_bt_u3_report(market_type: str, run_id: str) -> dict
render_u3_report_md(report: dict) -> str   # L0 배너 고정 + 정량표만, 자유서술 금지
```

**지표 범위 — §2 5종 중 4종만 (지표4 제외)**
- `hit_rate`
- `gate_pass_rate`
- `virtual_entry_rate`
- `side_asymmetry_ratio` — FUTURES만 산출, SPOT은 §2 각주에 따라 `null` 고정

**지표4(`crash_window_forced_exit_rate`) 범위 제외 (이번 sub-phase 미착수)**
U1/U2 정책 승계상 regime=`UNKNOWN`·`exit_trigger=NULL` 구간이 존재해 분자(SL/MDD 트리거)·분모(해당 구간 가상포지션 수) 모두 신뢰 불가. U3는 이 지표를 계산하지 않고 리포트 내 고정 문자열 `"N/A — 별도 Handoff 대기"`로만 표기. 근사치 대입·임의 재정의 금지(룰5).

**분모 0 처리**: §2 정책 그대로 — 0으로 나누지 않고 `null` (신규 정의 아님, 기존 재사용).

**출력 상단 고정 배너 (그대로 삽입)**
```
L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지
```
배너 + 정량표 외 자유서술 금지 (§3 Kill 준수).

### Config 변경 (있으면)
없음

### SPOT/FUT 분기
- `market_type` 파라미터 관통, 하드코딩 금지 (§4)
- 리포트는 SPOT·FUT **분리 집계 후 나란히 제시** — 합산 금지 (§4)
- `side_asymmetry_ratio`: SPOT은 항상 `null`(각주), FUTURES만 국면별 값 산출

### 인접 CAT 영향
- **CAT-J**: 없음 — 읽기도 아님. §5 로드맵 "CAT-J 인접·편입 아님" 그대로, 리포팅 파이프라인 등록·자동 트리거 연결 금지, 독립 산출물로만 존재
- **CAT-B/C/G/F/N/D**: 없음 (U1/U2와 동일 비접촉 헌법 유지)
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
`u3_report.py` + 산출 리포트 파일 삭제만으로 완전 롤백. `bitget_universe_bt.sqlite`(U1/U2 산출물)·paper DB·config_kv·CAT-C/B/G/F/N/D 원본 무영향.

### Cursor 지시
- Targeted 신규 파일만(`u3_report.py`). U1/U2 파일 diff 금지 — 전체 파일 rewrite 금지.
- **루트 주식 경로 무접촉**, `bitget/` 하위만.
- 리포트 산출 파일은 CAT-J 리포팅 디렉터리 밖에 저장 (예: `bitget/analysis/universe_bt/reports/`) — CAT-J 파이프라인 등록·자동 트리거 연결 금지.
- 지표4는 이번 sub-phase에서 코드·수치 모두 다루지 않음 — `"N/A"` 고정 문자열만 출력.
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/universe_bt/` (신규 `test_u3` — denominator=0→null, SPOT `side_asymmetry_ratio`=null, 배너 존재, 지표4 N/A 고정 케이스 포함)

### 위험도
🟢 (읽기 전용 리포트 · 격리 DB만 참조 · 코드/config/paper 비접촉)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U3 착수 + 지표4 제외 사유 1줄
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: **룰13 대상 — 이번 턴은 디렉터 지시(U3 Handoff만 파일로)에 따라 범위 밖. U3 Claude OK 수신 시 별도로 갱신 예정.**

---

*버전 2026-08-23 · UNIVERSE-BT-U3 · Architect: Claude Pro · Engineer: Cursor*

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U1 Claude OK + U2 Handoff · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **UNIVERSE-BT-U1(C3) = OK** · **U2 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음)

---

## UNIVERSE-BT-U1(C3) 검증 결과: **OK**

판정 근거:
- `resolve_historical_regime`를 항상 `UNKNOWN`으로 고정한 선택은 원 spec (c) 문언("현재 라이브 국면 스냅샷 적용")보다 **보수적**임 — 오늘 국면 라벨을 과거 구간에 역투영하는 쪽이 오히려 오정보 위험이 크므로, UNKNOWN + 지표4 null 처리가 U0 §2 "분모 0 → null(가짜 100% 금지)" 원칙에 더 부합. 문언 이탈이나 Kill 위반 아님 — **승인**.
- Adapter 방식: `try_add_virtual_position` 원본 호출 그대로 두고 `DB_PATH`만 scratch로 패치, `save_system_config`/telegram no-op — 원본 미수정 확인(룰6 Adapter 원칙 준수).
- CAT-C/G/D/N/F 원본 파일 diff 없음 · config_kv 쓰기 없음 — spec "변경 없음" 항목과 일치.
- paper DB(`bitget_forward_trades`) row count **before=after=3** 확인 — 물리적 파일 분리(1차 안전장치) 유효 입증.
- SPOT SHORT dry → ledger hard reject(SHORT-DANTE-FUT-01) 확인 — 하니스 특수분기 없이 자연 동작, spec 일치.
- 테스트 4 passed, paper DB 불변 케이스 포함 확인 — spec 필수요건 충족.
- U1 축소 범위(TF=1D·engines=master+ema5·window≤5)는 OUTBOX에 명시적으로 disclosure됨 — 은닉 축소 아님, 승인.

**참고 (수정 요구 아님):** UNKNOWN 고정을 택한 근거를 `05_진행로그.md` 또는 U3 배너 제약문에 한 줄 남겨두면 추후 U1.1/U3에서 판단 근거 추적이 쉬움.

**다음 Handoff 선택 사유:** 지표4(과거 국면 재구성)는 (a)/(b) 조사 모두 불가로 판명된 **리서치형 미해결 문제**라 임의 설계 시 상수/라벨 창조 리스크가 큼(룰5) — 별도 라운드로 분리. 이미 안전성이 검증된 U1 골격을 그대로 재사용해 **규모만 확장**하는 U2가 리스크 대비 진행 가치가 높아 다음 Handoff로 선택.

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U2 배치·샤드·체크포인트

### sub-phase ID
UNIVERSE-BT-U2

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/analysis/universe_bt/` 하위 (U2 오케스트레이터 — 기존 디렉토리 컨벤션에 맞춰 Cursor 배치)
- 신규 테이블(격리 DB 내부): `bitget_universe_bt_checkpoint` (`bitget_universe_bt.sqlite` 전용, paper DB와 무관)
- 참조만(원본 비접촉): U1 `run_universe_bt_u1` / `replay_symbol_window` / `resolve_historical_regime` / `write_bt_results`, 기존 `TIME_MACHINE_MAX_*` 상수(정확 위치는 Cursor 조사 — **재사용만, 재정의 금지**)
- 변경 없음: config_kv, `bitget_forward_trades`(paper ledger), execution_safety, Kelly/Treasury, CAT-C/G/N/D 원본 코드 전체, U1의 국면 처리(UNKNOWN 고정)·exit_trigger(NULL 고정)

### 변경 Spec

**함수 시그니처 (골격만)**
```
run_universe_bt_u2(market_type: str, run_id: str, resume: bool = True) -> None
build_universe_shards(symbols: list[str], shard_size: int) -> list[list[str]]
get_symbol_window_batches(symbol: str, market_type: str, batch_size: int) -> list[tuple[int, int]]
load_checkpoint(run_id: str, market_type: str) -> dict | None
save_checkpoint(run_id: str, market_type: str, symbol: str, batch_idx: int) -> None
```

**정책**
1. `run_universe_bt_u2`는 U1 함수(`replay_symbol_window`/`resolve_historical_regime`/`write_bt_results`)를 **원본 그대로 재사용**하는 상위 오케스트레이터. U1 내부 로직 재작성·복제 금지(Adapter만 추가).
2. 유니버스 스냅샷 `U`(U0 §1 정의 그대로)를 `build_universe_shards`로 분할 — `shard_size`는 기존 `TIME_MACHINE_MAX_*` 값 그대로. Cursor가 codebase에서 정확한 상수명·값을 확인 후 **재사용만, 신규 값 창조 금지**(룰5).
3. 심볼별 `get_symbol_window_batches`로 전체 보유 히스토리를 시간축 배치 분할 — U1의 "심볼당 window ≤5" 임시 상한 제거. 배치 크기도 기존 `TIME_MACHINE_MAX_*` 재사용.
4. 배치(심볼×윈도우) 완료마다 `save_checkpoint` 기록 — 대상은 `bitget_universe_bt.sqlite` 내부 `bitget_universe_bt_checkpoint` 테이블만. config_kv·paper DB 접촉 금지(U1 원칙 승계).
5. `resume=True` + 동일 `run_id` 체크포인트 존재 시 완료분 skip, 중단 지점부터 재개. `write_bt_results`가 `(run_id, market_type, symbol, bar_ts)` 중복 삽입 안 하도록 unique 제약 또는 사전 skip 로직 확인.
6. 국면·지표4: U1 C3 결과 그대로 승계 — `resolve_historical_regime` 항상 `UNKNOWN`, `exit_trigger` 항상 `NULL` 유지. **본 Handoff에서 재개하지 않음**(별도 라운드).
7. engines 풀(`master`+`ema5`)·TF(`1D`)는 U1과 **동일 유지** — 확장은 본 Handoff 범위 밖.
8. 실행 규모 확대(전체 유니버스 × 전체 히스토리)에 따라 paper DB(`bitget_forward_trades`) row count 불변 검증을 **배치 실행 전/후 + 샤드마다** 재확인(U1은 1회 확인, U2는 노출 시간이 길어 반복 확인 필요).

**신규 테이블 스키마 (키만)**
`bitget_universe_bt_checkpoint`: `run_id, market_type, shard_index, completed_symbol, completed_batch_idx, updated_at`

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지 (U1과 동일)

### SPOT/FUT 분기
- `market_type` 파라미터 U1과 동일하게 관통 (하드코딩 금지)
- SPOT: SHORT 자연 0건(U1과 동일, 특수분기 불필요)
- FUTURES: LONG/SHORT 모두 기록

### 인접 CAT 영향
- **CAT-B**: 읽기만(OHLCV) — 규모 확대로 읽기량 증가, 쓰기 없음 불변
- **CAT-C**: 읽기만(원본 import 승계), 원본 수정 금지
- **CAT-G**: 읽기만, UNKNOWN 고정 승계(신규 조사 없음)
- **CAT-D**: 참조만 — 실 write(`bitget_forward_trades`) 절대 금지, 검증 빈도 상향(위 8항)
- **CAT-F/N**: 비접촉
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
신규 파일(U2 오케스트레이터) + `bitget_universe_bt_checkpoint` 테이블 삭제만으로 완전 롤백. U1 하니스·paper DB·config_kv·원본 CAT 코드 무영향.

### Cursor 지시
- Targeted 신규 파일만. U1/CAT-C/G/D 원본 파일 diff 금지 — import만.
- **루트 주식 경로 무접촉**, `bitget/` 하위만.
- `TIME_MACHINE_MAX_*` 정확 값·위치는 codebase 조사 후 `CURSOR_TO_CLAUDE.md`에 "재사용값: {실제값}" 1줄 보고(임의 값 사용 금지).
- 하니스 실행 전후 + 샤드마다 paper DB(`bitget_forward_trades`) row count 대조, 세션 종료 보고에 숫자로 기록.
- 테스트: `pytest bitget/tests/universe_bt/` (신규 — 체크포인트 재개(resume) idempotency 케이스 + paper DB 불변 케이스 필수 포함)

### 위험도
🟡 Medium (원본 CAT 코드 비접촉·paper 격리 유지되나, 실행 규모·노출 시간 증가로 격리 실패 리스크 누적 — 위 8항 반복 검증 필수. CAT-F/G/I/N/B/D **코드 변경 없음**이므로 🔴 Critical 미해당)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U2 착수 + 실사용 `TIME_MACHINE_MAX_*` 값
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: 첨부 갱신본 반영(룰13, 별첨 참고)

---

*버전 2026-08-23 · UNIVERSE-BT-U2 · Architect: Claude Pro · Engineer: Cursor*

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U0 재검증 OK + U1 Handoff · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **U0 재검증 = OK** · **U1 착수 승인**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 트랙과 게이팅 없음)

---

## UNIVERSE-BT-U0 재검증 결과: **OK**

판정 근거:
- `14_UNIVERSE-BT_구조생존검증.md` §2 `crash_window_forced_exit_rate` — `CRASH` 라벨 삭제, `(BEAR ∪ HIGH_VOL)`로 정정 확인
- CAT-CONSTANTS Regime Kelly cap 표 대조: BEAR(~0.010)·HIGH_VOL(~0.012)이 5개 국면 중 최저 리스크 허용치 — "위험 국면" 취지와 일치, 신규 상수 창조 아님(룰5 준수)
- CAT-G SSOT 값 집합 `{BULL,BEAR,CHOP,HIGH_VOL,SIDEWAYS,UNKNOWN}` 재확인 — `CRASH` 미존재 확정
- `gate_pass_rate`/`virtual_entry_rate` 변수명 `gate_passed_candidates`로 통일 확인
- §1·§3·§4·§5, `00_마스터_로드맵.md` 포인터(단일 라인), Track B 병렬 — 비변경 확인
- 코드·config_kv 비접촉 확인 (문서 전용 정정)

**다음: 아래 U1 Handoff.**

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U1 read-only 리플레이 하니스

### sub-phase ID
UNIVERSE-BT-U1

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/analysis/universe_bt/` 하위 (정확 위치는 기존 디렉토리 컨벤션에 맞춰 Cursor 배치)
- 신규 격리 DB: `bitget_universe_bt.sqlite` — **신규 SQLite 파일**, paper DB(`bitget_forward_trades`)와 물리적으로 분리, 커넥션 공유 금지
- 참조만(import/read only, 원본 비접촉): `signal_engines.py`, `master_scanner.py`, `forward/ledger` try_add 게이트 로직, `governance/meta_sync.py`/`meta_consumer.py`, OHLCV `BITGET_SPOT_*`/`BITGET_FUT_*`
- 변경 없음: config_kv, `bitget_forward_trades`(paper ledger), execution_safety, Kelly/Treasury, CAT-C/G/N/D 원본 코드 전체

### 변경 Spec

**함수 시그니처 (골격만)**
```
run_universe_bt_u1(market_type: str) -> None
replay_symbol_window(symbol: str, market_type: str, start_ts: int, end_ts: int) -> list[dict]
resolve_historical_regime(symbol: str, market_type: str, bar_ts: int) -> str
write_bt_results(rows: list[dict]) -> None   # bitget_universe_bt.sqlite 전용, 타 DB 접촉 금지
```

**정책**
1. `run_universe_bt_u1`은 §1 스냅샷(`U = load_dynamic_universe(market_type) ∩ 보유 OHLCV`)을 순회하며 심볼별 `replay_symbol_window` 호출 → `write_bt_results`. 단일 프로세스 순차 실행만 (배치·샤드·체크포인트는 U2 범위 — `TIME_MACHINE_MAX_*` 재사용은 U2에서).
2. `replay_symbol_window`는 CAT-C 후보생성·게이트 로직을 **원본 그대로 import**해 호출 — 로직 재작성·복제 금지. 게이트 판정이 `try_add_virtual_position` 내부에서 DB write와 결합되어 분리 호출이 불가능하면: 원본 함수 **수정 금지**, write 인자만 격리 DB로 주입하는 Adapter로 감싸는 방식을 조사. Adapter로도 안전한 분리가 불가능하면 CURSOR_TO_CLAUDE에 충돌 보고(템플릿 §"구현 충돌") 후 디렉터 Ask.
3. `resolve_historical_regime` — 과거 시점 국면 라벨 소스가 현재 spec상 불명확. Cursor 조사 후 3갈래 중 보고:
   - (a) 국면 이력 로그(`validation/regime_audit.py` 또는 유사)에 시점별 스냅샷이 존재 → 읽기 전용 사용
   - (b) 이력 로그 없음 + `meta_sync.py` 판정이 가격/거래량 히스토리만의 결정적 함수 → 읽기 전용 Adapter로 과거 구간에 재적용 (`meta_sync.py` 원본 수정·config_kv 쓰기 금지)
   - (c) 둘 다 불가 → U1을 "현재 라이브 국면 스냅샷 기준" 한정판으로 축소하고 U3 배너에 제약 명시 — **착수 전 디렉터 Ask 필요**
4. `write_bt_results`는 `bitget_universe_bt.sqlite`에만 연결. paper DB 커넥션과 절대 공유 금지(물리적 파일 분리가 1차 안전장치).

**신규 테이블 스키마 (키만)**
`bitget_universe_bt_results`: `run_id, market_type, symbol, bar_ts, regime_label, candidate_generated, gate_passed, virtual_entry, side, exit_trigger, created_at`

### Config 변경 (있으면)
없음 — config_kv 쓰기 전면 금지

### SPOT/FUT 분기
- `market_type` 파라미터 전체 관통 (하드코딩 금지)
- SPOT: SHORT는 기존 ledger hard reject(SHORT-DANTE-FUT-01)로 자연 0건 — 하니스 내 특수분기 불필요, 있는 그대로 기록
- FUTURES: LONG/SHORT 모두 기록

### 인접 CAT 영향
- **CAT-B**: 읽기만 (OHLCV), 쓰기 없음
- **CAT-C**: 읽기만(원본 import), 원본 수정 금지 — 위 2항 Adapter 조사 필요 시 신규 Adapter 파일만 추가
- **CAT-G**: 읽기만, 이력 소스 불명확 시 Adapter 조사(원본·config_kv 비접촉)
- **CAT-D**: 참조만 — 게이트 로직 재사용하되 실 write(`bitget_forward_trades`) 절대 금지
- **CAT-F/N**: 비접촉 (Kelly/execution_safety 관여 없음 — 가상 리서치, 주문 경로 아님)
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
신규 파일(`bitget/analysis/universe_bt/*`) + `bitget_universe_bt.sqlite` 삭제만으로 완전 롤백. paper DB·config_kv·CAT-C/G/N/D/B 원본 무영향.

### Cursor 지시
- Targeted 신규 파일만. 기존 CAT-C/G/D 원본 파일 diff 금지 — import만.
- **루트 주식 경로 무접촉**, `bitget/` 하위만.
- 위 3항 (a)/(b)/(c) 중 어느 경로로 갔는지, 무슨 이력 소스를 썼는지 `CURSOR_TO_CLAUDE.md`에 먼저 보고 — (c)면 착수 전 디렉터 Ask.
- 하니스 실행 전후 paper DB(`bitget_forward_trades`) row count 불변을 직접 대조해 세션 종료 보고에 숫자로 기록.
- 테스트: `pytest bitget/tests/universe_bt/` (신규 — paper DB 불변 검증 케이스 필수 포함)

### 위험도
🟡 Medium (CAT-C/D 로직 재사용·실거래 경로 비접촉이나, 격리 실패 시 paper 오염 리스크 — 위 격리 검증 필수)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U1 착수 + 국면이력 (a)/(b)/(c) 판단 결과
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: 본 Handoff와 함께 Claude가 갱신(룰13, 별첨 파일 참고)

---

*버전 2026-08-23 · UNIVERSE-BT-U1 · Architect: Claude Pro · Engineer: Cursor*

---

# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U0 수정 spec · 기존 CLAUDE_TO_CURSOR.md 최상단에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]
> **상태**: **U0 검증 결과 = 수정 필요** (OK 아님) · **U1 착수 계속 금지**
> **병행**: B1-LADDER-R1a OBSERVE 유지 (본 정정과 무관)

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT-U0 §2 지표4 정정 (CAT-G 미존재 라벨 "CRASH" 교정)

### sub-phase ID
UNIVERSE-BT-U0 (수정 라운드 · 재검증 대기)

### SSOT (변경 금지 unless noted)
- 수정(타겟 diff만): `bitget/docs/work_phases/14_UNIVERSE-BT_구조생존검증.md` §2 표 — `crash_window_forced_exit_rate` 행 + 변수명 통일 2곳
- 참조만(비변경): `governance/meta_sync.py` `CURRENT_REGIME_KEY` 값 집합, CAT-CONSTANTS Regime Kelly cap 표
- 변경 없음: `00_마스터_로드맵.md` 포인터, config_kv, CAT-C/B/G/F/N/D 코드 전체

### 변경 Spec

**문제**
§2 표 4번째 지표 `crash_window_forced_exit_rate`가 "CAT-G 국면 라벨 **CRASH**/BEAR 한정"으로 정의되어 있음. 그러나 CAT-G SSOT(`CURRENT_REGIME_KEY`)의 실제 값 집합은:

```
{BULL, BEAR, CHOP, HIGH_VOL, SIDEWAYS, UNKNOWN}
```

`CRASH`는 CAT-G 문서·CAT-CONSTANTS Regime Kelly cap 표 어디에도 없는 값 — 임의 라벨 창조 금지(룰5)에 해당. U1에서 이대로 구현 시 필터가 항상 공집합이 되거나 Cursor가 임의로 재해석하게 됨.

**정정 정의**
```
crash_window_forced_exit_rate =
  (BEAR ∪ HIGH_VOL 구간) SL 또는 MDD 트리거 횟수 / 해당 구간 가상포지션 수
```
지표 ID·의미(하락·고변동 구간 강제청산 비율)는 유지, **라벨만** 실제 SSOT 값(BEAR, HIGH_VOL)으로 교체. 근거: CAT-CONSTANTS Regime Kelly cap 표에서 BEAR(~0.010)·HIGH_VOL(~0.012)이 나머지 국면(BULL 0.028, SIDEWAYS 0.018, CHOP/UNKNOWN 0.015) 대비 가장 낮은 리스크 허용치 — "위험 국면" 취지에 부합하는 실제 라벨 쌍.

**부수 정정 (변수명 통일)**
§2 표에서 `gate_pass_rate` 분자 `gate_passed_candidates`와 `virtual_entry_rate` 분모 `gate_pass_candidates`가 동일 대상인데 표기가 다름 → **`gate_passed_candidates`로 통일**.

### SPOT/FUT 분기
공통 (§4 비변경 — market_type 하드코딩 없음, 정정과 무관)

### 인접 CAT 영향
- **CAT-G**: 없음 — 코드·config_kv 비접촉, 문서 내 라벨 표기만 실제 SSOT 값으로 정정
- **CAT-C/B/F/N/D**: 없음
- **Track B (B1-LADDER)**: 없음, 병렬 독립 유지

### 롤백 조건
문서 표 1개 행 + 변수명 2곳 재수정만 — 코드·config 영향 없음

### Cursor 지시
- Targeted diff only — `14_UNIVERSE-BT_구조생존검증.md` §2의 `crash_window_forced_exit_rate` 행과 변수명 통일 2곳만 수정. **문서 전체 재작성 금지**
- `CLAUDE_TO_CURSOR.md` 기존 U0 prepend 블록도 동일하게 §2 부분만 수정(전체 재작성 금지)
- 루트 주식 경로 무접촉, **U1 코드 착수 계속 금지** (U0 SSOT 미확정 상태)
- 테스트: 해당 없음 (문서)

### 위험도
🟢 (문서 전용 · CAT-G Critical 코드 비접촉 — 라벨 표기 정정뿐, 코드측 Critical 파급 없음)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md`: UNIVERSE-BT-U0 정정 사유 1줄 (CRASH → BEAR/HIGH_VOL)
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`: "UNIVERSE-BT-U0 정정 완료 → 재검증 요청"으로 갱신
- `bitget/docs/work_phases/NEXT_ACTION.md`: `WAIT_CLAUDE_OK` 유지 (변경 없음)
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: **이번 라운드는 갱신 대상 아님** (룰13 — OK+Handoff 확정 후에만 갱신, 재검증 OK 시 진행)

---

*버전 2026-08-23 · UNIVERSE-BT-U0 수정 라운드 · Architect: Claude Pro · Engineer: Cursor*

---

﻿# CLAUDE → CURSOR · 상단 추가분 (UNIVERSE-BT-U0 · 기존 B1 Handoff 위에 붙여넣기)

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-Q]  
> **상태**: Cursor **구현 대기** · U0 문서만 · **U1 착수 금지**  
> **병행**: B1-LADDER-R1a OBSERVE **유지** (상호 게이팅 없음)

---

## [CAT-Q] 진단&레거시 — UNIVERSE-BT 로드맵 배치 + U0 구조생존검증 정의문서

### sub-phase ID
UNIVERSE-BT-U0

### SSOT (변경 금지 unless noted)
- 신규: `bitget/docs/work_phases/14_UNIVERSE-BT_구조생존검증.md`
- 포인터 1줄만 추가: `00_마스터_로드맵.md` 말미 (표 재작성 금지, 13_B1_신뢰사다리 방식 재사용)
- 변경 없음: `forward/`, `factory_pipelines.py`, config_kv, CAT-C/B/G/F/N/D 코드 전체

### 변경 Spec

**문서 목차 (14_UNIVERSE-BT_구조생존검증.md)**
- §1 유니버스 스냅샷 정의 — `load_dynamic_universe()` 출력(거래량 floor 통과분) ∩ 보유 OHLCV(`BITGET_SPOT_*`/`BITGET_FUT_*`) 커버리지. "상장 전부" 제외 사유 1문단(라이브 스캐너와 동일 필터 사용 — 외부 타당도).
- §2 지표 5종 (수식)
  - `hit_rate = raw_signal_hit / total_bars_scanned`
  - `gate_pass_rate = gate_passed_candidates / candidates_generated`
  - `virtual_entry_rate = virtual_entries / gate_passed_candidates`
  - `crash_window_forced_exit_rate` (CAT-G `CURRENT_REGIME_KEY` **BEAR ∪ HIGH_VOL** 구간 한정, SL/MDD 트리거 비율 — CRASH 라벨 없음)
  - `side_asymmetry_ratio = LONG_virtual_entries / SHORT_virtual_entries` (국면별, SPOT SHORT=0 각주)
- §3 L0 라벨 · Kill(과신 표현) — 모든 산출물 상단 고정 배너: **"L0 구조단서 — 수익률/승률 아님, LIVE·B1「달성」·CAGR 단정 금지"**. 지표를 R6 판정이나 B1 성공계약(§1)에 대입 시 즉시 정정 대상.
- §4 SPOT/FUT 분리 원칙 — SPOT-FUT 비대칭표 인용(재작성 금지), `market_type` 파라미터화, 하드코딩 금지.
- §5 로드맵 표 — U0(본 Handoff)→U1(하니스, 🟡)→U2(배치, 🟢)→U3(리포트, 🟢). Track B(R1a~R6) 게이팅과 무관, 병렬 독립.

### Config 변경 (있으면)
없음

### 인접 CAT 영향
- CAT-C, B, G, F, N, D: **없음** (문서 전용, 코드 비접촉)
- Track B (B1-LADDER R0~R6): **없음** — 병렬 독립, 상호 게이팅 없음

### 롤백 조건
- 문서 삭제만으로 완전 롤백 (코드·config 영향 없음)

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- 이번 sub-phase는 **문서 작성만** — U1 코드 착수 금지(별도 Handoff 대기).
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: 해당 없음 (문서)

### 세션 종료 의무
- `bitget/docs/work_phases/05_진행로그.md` UNIVERSE-BT-U0 섹션
- `bitget/docs/work_phases/00_전체현황판.md`
- `bitget/docs/work_phases/CURSOR_TO_CLAUDE.md`
- `bitget/docs/work_phases/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `bitget/docs/work_phases/NEXT_STEP.md`
- `bitget/docs/work_phases/09_디렉터_쉬운요약.md` (쉬운 말·비유로 — "과거로 돌려본 결과는 참고용, 실전 증명 아님" 톤 유지)

### 위험도
🟢 (문서 전용 · Critical 코드 비접촉)

---

# CLAUDE → CURSOR · 상단 추가분 (기존 B1-LADDER-R0 Handoff 위에 붙여넣기)

---

## Claude OK — B1-LADDER-R0 (2026-08-23)

**판정: OK.**

검증 근거:
- `13_B1_신뢰사다리.md` 성공계약(B1 12~18% / MDD≤5% / 6~12개월) = `00` §0.4 표 그대로 재사용, **신규 상수 없음** (룰5)
- 렁 R0→R1→R2→(A06)→R3∥R4→R5→R6 순서, Critical 표기(🔴 R3/R4/R5), 승인문구 템플릿 — Ask 스펙과 일치
- `00` §0.4 말미 **1줄 포인터만** 확인, 표 비변경 확인
- 코드 · config_kv · execution_safety · gates · Kelly · deathmatch live **비접촉** 확인
- SPOT/FUT 공통 (market_type 분기 없음) 확인
- 인접 CAT: CAT-J 읽기만, CAT-H/D/N/G 비접촉 — 스펙과 일치
- 세션 종료 문서(`05`, `00` 용어집=전체현황판, `09`, `NEXT_STEP`, `NEXT_ACTION`) 갱신 확인
- 위험도 🟢 문서 전용 — 표기 적절

**참고 지적 (수정 요구 아님, R1b 설계 시 참고):**
R1a에서 "냉시동 vs 구조막힘"을 가르려면 후보 생성 여부(스캔 히트) 대비 진입 거절 여부가 필요한데, LONG 쪽은 `blocked_today` 텔레메트리가 아직 없음 (LS-GOAL-UX-01 기록: SHORT만 `short_funnel.blocked_short_total` 보유). 지금 코드 변경 요청 아님 — R1a 판정에서 LONG 증거가 약할 수 있다는 점만 인지.

**다음:** 05에 이 OK 기록 · 아래 R1a Handoff로 진행.

---

## [CAT-F] B1 신뢰사다리 — R1a 관측 마감 판정 기준

### sub-phase ID
**B1-LADDER-R1a**

### SSOT (변경 금지 unless noted)
- 수정(추가만): `bitget/docs/work_phases/13_B1_신뢰사다리.md` §3 Kill 표 아래에 "R1a 판정 절차" 소절 추가
- 참조만(비변경): §6 SQL, `short_funnel_report_bg.py`, `post_deploy_obs_digest_bg.py`
- config: **없음**

### 변경 Spec (문서 전용 · 신규 코드 없음)
R1a는 **3갈래 판정**, 매주 재관측:

| 판정 | 조건 | 행동 |
|------|------|------|
| **PASS** | 신선 실측에서 OPEN>0 신규 진입 확인 | R2 착수 (효과표 채우기 시작) |
| **관측 유지** | OPEN=0 지속 **AND** R0 확정일(2026-08-23)로부터 **4주 미만** 경과 **AND** 구조적 거절 증거 없음 | Kill 미발동 · 주간 재실측 반복 |
| **FAIL (구조막힘)** | 아래 (a) 또는 (b) **하나만 충족해도** 확정 — 4주 대기 불필요 | R2 착수 금지 유지 · R1b(CAT-C) 디렉터 승인 후 별도 대화 |

FAIL 근거 (a)/(b):
- (a) **4주 경과 후**에도 OPEN=0 지속 (기존 Kill 표 §3 그대로, 신규 상수 아님)
- (b) `short_funnel`에서 후보는 생성되나(`blocked_short_total`>0 등 진입 직전 거절 이벤트 존재) OPEN으로 이어지지 않는 패턴이 반복 관측됨 (SHORT만 현재 가시성 있음, 위 참고 지적 참고)

판정 근거는 매주 `05_진행로그.md`와 `CURSOR_TO_CLAUDE.md`에 **숫자만** 기록 (OPEN count, CLOSED count, short_funnel blocked count 있으면 같이). 코드 diff 없음.

### SPOT/FUT 분기
공통 (market_type 하드코딩 없음)

### 인접 CAT 영향
- **CAT-C**: 없음 (이번 R1a는 미착수 · R1b FAIL 확정 시에만 별도 Handoff)
- **CAT-J**: 읽기만 (`short_funnel_report_bg.py`, `post_deploy_obs_digest_bg.py` 기존 값 참조만, 재계산 없음)
- **CAT-H/D/N/F(Kelly/live/execution_safety)**: 비접촉

### 롤백 조건
문서만 (판정 절차 소절 삭제/재작성) — 코드 영향 없음

### Cursor 지시
- Targeted: `13_B1_신뢰사다리.md`에 위 판정표만 추가. **전체 재작성 금지**
- 디렉터가 §6 SQL 신선 실측값(+ 가능하면 short_funnel blocked count)을 전달하면, 그 숫자를 위 3갈래 판정표에 대입해 PASS/관측유지/FAIL만 표기
- 신규 코드 · 신규 테스트 없음
- 루트 주식 경로 무접촉

### 위험도
🟢 문서 전용

### 세션 종료 의무
- `05_진행로그.md`: B1-LADDER-R1a 섹션 + 매주 실측 숫자
- `00_전체현황판.md`: 다음 Handoff 필드 갱신
- `CURSOR_TO_CLAUDE.md`: 실측 숫자 + 판정 결과 보고
- `NEXT_ACTION.md`: 판정 결과에 따라 `WAIT_CLAUDE_OK`(FAIL 시 R1b 승인 대기) 또는 관측 유지 표기
- `09_디렉터_쉬운요약.md` / `NEXT_STEP.md`: Claude가 판정 결과 확인 후 직접 갱신 (룰13)

# CLAUDE → CURSOR · B1-LADDER-R0

> **작성**: Claude Pro (Architect) · 2026-08-23 · [CAT-F]  
> **상태**: Cursor 구현 완료 · **Claude OK 2026-08-23**  
> **금지**: config_kv · live · Kelly · MDD 코드 · execution_safety · gates · deathmatch live

---

## [CAT-F] 자본배분&리스크 — B1 성공계약·신뢰사다리 R0 문서화

### sub-phase ID
**B1-LADDER-R0**

### SSOT (변경 금지 unless noted)
- 신규 파일: `bitget/docs/work_phases/13_B1_신뢰사다리.md`
- 참조만(비변경): `00_마스터_로드맵.md` §0.4, `05`/`06`, `12_듀얼북극성…`
- config: **없음**

### 변경 Spec
- 코드 변경 **없음**
- `13_B1_신뢰사다리.md`: (1) 성공계약 B1 (2) 렁 R0~R6 (3) Kill 표 (4) 신뢰밴드
- `00` §0.4 말미 **1줄 포인터만** (표 재작성 금지)
- SPOT/FUT: 공통

### Config 변경
없음

### 인접 CAT
- CAT-J: 읽기만 · CAT-H/D/N: 없음

### 롤백
문서만 재작성 (코드 영향 없음)

### Cursor 지시
- Targeted · `bitget/` only · R1a는 **관측만** · 테스트 없음

### 위험도
🟢 문서 전용

### 세션 종료 의무
- `05` · `00` 용어집 · `CURSOR_TO_CLAUDE` · `NEXT_ACTION`→WAIT_CLAUDE_OK · `09` · `NEXT_STEP`

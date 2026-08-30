# VPS Cursor 격리 Draft PR 작업기

이 단계는 Cursor가 운영 저장소를 직접 수정하지 않도록 별도 Git
worktree에서만 코드를 고치고, 안전 검증이 모두 통과한 경우 Draft PR만
생성한다. 자동 병합·배포·SSH·실전 주문·위험도 변경 권한은 없다.

## 실행 흐름

1. 기존 관제기가 `CURSOR_IMPLEMENT` 후보 JSON을 outbox에 기록한다.
2. 현재 주간 envelope가 코드 수정·브랜치 push·Draft PR을 명시적으로
   허용하는지 다시 확인한다.
3. 현재 SSOT가 정확히 `WAIT_CURSOR_IMPL`이고 Handoff에 machine-readable
   `Allowed files:`와 pytest 명령이 있는지 확인한다.
4. `main == origin/main`, 운영 작업트리 clean을 확인한다.
5. `/var/lib/quant-dev-pr-worker/<role>/worktrees/`에 격리 worktree를 만든다.
6. Cursor는 그 worktree에서만 작업하며 Git·네트워크·secret·deploy는
   `.cursor/cli.json`으로 차단된다.
7. 변경 파일 allowlist·위험 경로·위험 내용·pytest를 검사한다.
8. Claude Code가 읽기 전용으로 검토해 `OK`를 반환해야 한다.
9. 컨트롤러가 고정 메시지로 commit하고 새 브랜치를 push한 뒤
   `gh pr create --draft`를 실행한다.
10. 텔레그램으로 Draft PR 주소를 보고한다. 병합은 사람이 결정한다.

## 1. 주간 envelope 준비

`dev_autonomy/autonomy_envelope.pr_worker.example.json`을 검토해 날짜와
트랙을 현재 주간 범위로 바꾼 뒤 각 VPS의 아래 경로에 둔다.

```text
/etc/quant-dev-autonomy/envelope.json
```

필수 권한은 아래 세 가지뿐이다.

```json
{
  "allow_cursor_write": true,
  "allow_branch_push": true,
  "allow_draft_pr": true,
  "allow_deploy": false,
  "allow_live": false,
  "allow_merge": false
}
```

`max_tasks_per_day`는 1을 권장하고 최대 3만 허용된다. 기존 envelope처럼
세 새 필드가 없거나 false이면 작업기는 fail-closed로 정지한다.

## 2. GitHub CLI 설치와 브라우저 로그인

각 VPS에서 먼저 설치 파일을 배치하되 timer는 켜지 않는다.

주식 VPS:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
git pull --ff-only
sudo INSTALL_ROOT="$PWD" ./deploy/install_dev_pr_worker.sh \
  --role stock --install-github-cli
sudo -u ubuntu -H bash deploy/auth_dev_pr_worker_github.sh
```

Bitget VPS는 `stock` 대신 `bitget`을 사용한다. 화면에 표시되는 GitHub
로그인 URL을 노트북에서 열어 승인한다. 토큰·SSH 키를 채팅이나 저장소에
붙여 넣지 않는다.

## 3. 활성화

Claude·Cursor·GitHub 로그인이 모두 끝나고 envelope가 설치된 뒤 실행한다.

```bash
sudo INSTALL_ROOT="$PWD" ./deploy/install_dev_pr_worker.sh \
  --role stock --enable-timer --start-now
```

Bitget VPS에서는 `stock`을 `bitget`으로 바꾼다. 평일에는 기존 관제 실행
5분 뒤인 매시 05분·35분, 주말에는 한국 시간 10시 20분에 실행된다.
한 번에 최대 한 작업만 처리한다.

## 4. 감사

```bash
sudo bash deploy/audit_dev_pr_worker.sh stock
systemctl list-timers 'quant-dev-pr-worker@*'
journalctl -u quant-dev-pr-worker@stock.service -n 100 --no-pager
```

정상 상태는 `[READY] stock Cursor draft-PR worker`다. Bitget 서버에서는
`stock`을 `bitget`으로 바꾼다.

## Fail-closed 조건

- 운영 저장소가 clean main이 아니거나 `origin/main`과 다름
- envelope 만료·일일 한도 초과·세 권한 중 하나라도 false
- `WAIT_CURSOR_IMPL`이 아님
- Handoff allowed files 또는 pytest 명령 없음
- secret·deploy·risk·교차 트랙 파일 포함
- Cursor 실패, pytest 실패, Claude `MODIFY`/`REJECT`
- GitHub 인증 없음, 브랜치 중복, push/PR 생성 실패

실패한 worktree는 사람이 확인할 수 있도록 보존하며 자동 재시도하지 않는다.

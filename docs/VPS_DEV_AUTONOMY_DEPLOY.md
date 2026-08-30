# VPS 개발 자동화 설치

이 구성은 퀀트 결과를 읽어 Claude 검토·Cursor 작업 후보·주말 대기로
분류하고 텔레그램으로 보고한다. 실전 주문, 위험도 변경, SSH, 배포,
Git 병합 권한은 주지 않는다.

## 역할 분리

| 서버 | 설치 역할 | 읽는 자료 |
|---|---|---|
| 주식 VPS | `stock` | North Star 원장 + 저장소 SSOT |
| Bitget VPS | `bitget` | Bitget ops DB만 (`--no-ssot`) |

양쪽을 분리하면 같은 SSOT를 두 서버가 중복 보고하지 않는다. 결과 ID는
각 서버의 `/var/lib/quant-dev-autonomy/<role>/control_plane.sqlite`에서
중복 방지된다.

## 1. 코드 갱신과 CLI 설치

각 VPS에서 해당 역할만 실행한다.

주식 VPS:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
git pull --ff-only
sudo INSTALL_ROOT="$PWD" ./deploy/install_dev_autonomy.sh \
  --role stock --install-ai-clis
```

Bitget VPS:

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
git pull --ff-only
sudo INSTALL_ROOT="$PWD" ./deploy/install_dev_autonomy.sh \
  --role bitget --install-ai-clis
```

첫 설치는 unit과 timer를 만들고 dry-run만 수행한다. 인증 전에는 timer를
자동으로 켜지 않는다.

## 2. Claude Pro와 Cursor Pro 로그인

각 VPS에서 `ubuntu` 사용자로 한 번씩 실행한다.

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
sudo -u ubuntu -H bash deploy/auth_dev_autonomy_ai.sh
```

표시되는 로그인 URL을 노트북 브라우저에서 열어 승인한다. Claude Pro
구독 로그인을 사용할 때는 `claude auth login`을 사용한다. 이 스크립트는
별도 API 과금 로그인을 뜻하는 `--console`을 사용하지 않는다.

Cursor는 브라우저 로그인을 사용하며, API 키·토큰·SSH 키를 채팅이나
저장소에 붙여 넣지 않는다.

## 3. Telegram 연결

주식 VPS는 기존 루트 `.env`의 아래 이름 중 현재 사용 중인 값을 재사용한다.

- `REPORT_BOT_TOKEN` 또는 기존 Telegram token 폴백
- `REPORT_BOT_CHAT_ID` 또는 `TELEGRAM_CHAT_ID`

Bitget VPS는 `bitget/.env`의 `BITGET_BOT_TOKEN`, `BITGET_BOT_CHAT_ID`도
프로세스 안에서 report 이름으로 안전하게 매핑한다. 실제 값은 출력하지
않는다. 두 `.env` 파일은 설치 시 권한을 `600`으로 유지한다.

## 4. 활성화와 첫 보고

인증을 마친 뒤 역할별로 실행한다.

```bash
sudo INSTALL_ROOT="$PWD" ./deploy/install_dev_autonomy.sh \
  --role stock --enable-timer --start-now
```

Bitget 서버에서는 `stock` 대신 `bitget`을 사용한다. 첫 실행은 새 결과가
있을 때만 텔레그램을 보낸다. 이후 평일에는 30분 간격, 주말에는 오전
10시 15분(한국 시간)에 실행된다.

## 5. 상태 점검

```bash
sudo bash deploy/audit_dev_autonomy.sh stock
systemctl list-timers 'quant-dev-autonomy@*'
journalctl -u quant-dev-autonomy@stock.service -n 100 --no-pager
```

Bitget 서버에서는 `stock`을 `bitget`으로 바꾼다. 감사 스크립트는 인증값의
존재 여부만 검사하고 실제 값을 출력하지 않는다.

## 현재 Cursor 경계

Cursor CLI 설치·인증과 작업 JSON 생성까지 연결된다. 그러나 코드 변경
스위치는 아직 기본 OFF다. 거래 VPS의 운영 작업트리를 직접 수정하지 않고,
별도 격리 worktree 또는 Cursor Cloud Agent가 PR만 만드는 다음 단계가
완료돼야 자동 코드 수정이 켜진다.

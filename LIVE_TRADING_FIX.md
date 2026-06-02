# LIVE TRADING ARCHITECTURE FIX

## 1) 정규 장중 실시간 스케줄링 변경

대상 파일: `deploy/factory.crontab.example`

- KR 스캔을 장마감 후 단발성 패턴에서 장중 30분 주기로 변경:
  - `0,30 9-15 * * 1-5  ... ./factory.sh --scan-kr`
  - KST 09:00~15:30 구간을 30분 간격으로 커버.
- US 스캔을 장중 30분 주기로 변경(자정 경계 + DST/비DST 대응):
  - `30 22 * * 2-6 ... ./factory.sh --scan-us` (DST 시작 구간 커버)
  - `0,30 23 * * 2-6 ... ./factory.sh --scan-us`
  - `0,30 0-6 * * 2-6 ... ./factory.sh --scan-us`
  - KST 기준 22:30~06:30 범위를 30분 간격 커버하여
    미국 정규장(서머타임 22:30~05:00 / 비서머타임 23:30~06:00)을 포함.

## 2) 당일 중복 텔레그램 발송 방지 (Daily Dedup Cache)

대상 파일:
- `daily_dispatch_cache.py` (신규)
- `supernova_hunter.py`

구현 내용:
- 신규 캐시 파일 `daily_dispatched_tickers.json`를 `CONFIG_DIR` 하위에 저장.
- 저장 구조:
  - `date`: KST 기준 YYYY-MM-DD
  - `tickers`: `{ "KR": [...], "US": [...] }`
- 동작:
  - 메시지 발송 전 `was_dispatched_today(market, ticker)`로 당일 발송 여부 확인.
  - 이미 발송한 티커면 텔레그램 발송 Drop.
  - 신규 발송 성공 시 `mark_dispatched_today(market, ticker)`로 즉시 기록.
  - 날짜가 바뀌면(자정) 자동으로 새 날짜 버킷으로 초기화.

적용 지점:
- `supernova_hunter.py`의 `execute_supernova_live_scan()` 내
  `try_add_virtual_position` 성공 후 텔레그램 전송 직전에 중복 필터 적용.

## 3) 가상매매 당일 중복 진입(Pyramiding) 원천 차단

대상 파일: `forward/shared.py`

`try_add_virtual_position()` 방어벽 추가:
- OPEN 중복 방지 강화:
  - 같은 `market + code`가 `status='OPEN'`이면 즉시 차단.
- 당일 재진입 차단:
  - 같은 `market + code`가 `entry_date=today`로 이미 존재하면
    (OPEN/CLOSED 상태 무관) 신규 진입 차단.

결과:
- 장중 반복 스캔 환경에서도 동일 티커의 당일 중복 편입(피라미딩) 방지.
- 텔레그램 중복 발송 방지와 함께 실전 운용 시 과대 배분 리스크를 구조적으로 차단.


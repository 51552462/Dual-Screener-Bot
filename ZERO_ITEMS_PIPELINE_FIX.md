# ZERO_ITEMS_PIPELINE_FIX — 2~9번 전역 0건 병목 수술 보고

## 원인 진단 (데이터 증발 지점)

핵심 병목은 **리포트 공통 슬라이스 단계**였습니다.

1. `reports/daily_report_context.py`의 `load_market_slice()`가 SQL에서
   `status LIKE 'CLOSED%' AND substr(exit_date,1,10) BETWEEN cutoff AND anchor`
   로 **`exit_date`만 강제 필터**하고 있었습니다.
2. 실제 장부에는 행마다 날짜 포맷 편차(문자열/공백/레거시) 및 `entry_date`만 신뢰 가능한 케이스가 섞여 있어,
   세션 앵커(KST/ET) 기준 윈도우에서 CLOSED 표본이 과도하게 탈락했습니다.
3. 이 공통 슬라이스(`df_closed`, `df_real`)를 [2/9]~[9/9]가 재사용하므로,
   결과적으로 **2~9번이 연쇄적으로 0건**처럼 보이는 증상이 발생했습니다.

## 적용한 수정

## Action 1 — Timezone/날짜 필터 충돌 해결

### `reports/daily_report_context.py`
- SQL 1차 필터를 `market + INCUBATOR 제외`로 완화
- CLOSED 윈도우 판정은 Pandas에서 재평가:
  - 기준일: `exit_date` 우선, 비어 있으면 `entry_date` 폴백
  - 조건: `rolling_cutoff <= closed_day <= session_anchor`
- OPEN/ACTIVE는 별도 유지 후 유효 OPEN 마스크 적용

효과:
- timezone-aware/naive 포맷 편차·날짜 문자열 편차로 인한 조기 탈락 방지
- 공통 슬라이스 자체가 비는 현상 완화

## Action 2 — 딥다이브 공통 필터 붕괴 보정

### `forward/deep_dive.py`
- 딥다이브 CLOSED 조회 SQL의 날짜 기준을 `exit_date` 단일 의존에서
  `exit_date` → `entry_date` 폴백으로 변경:
  - `substr(IFNULL(NULLIF(TRIM(exit_date),''), NULLIF(TRIM(entry_date),'')),1,10)`

효과:
- 딥다이브 윈도우가 `exit_date` 결측/포맷 이상으로 0건이 되는 현상 방지

## Action 3 — 파이프라인 매핑 확인

현재 `factory_pipelines.py`의 `scan_kr`/`scan_us`는
`supernova + nulrim + 5ema + bowl` 스텝이 모두 등록되어 동작합니다.
이번 수정은 큐 변수 덮어쓰기 이슈보다는 **공통 데이터 슬라이스 병목**을 해소하는 데 집중했습니다.

## 변경 파일

- `reports/daily_report_context.py`
- `forward/deep_dive.py`

## 배포

로컬에서:

```bash
git add . && git commit -m "Fix 0 items bottleneck pipeline" && git push origin main
```

서버에서:

```bash
sudo bash update_bot.sh
```

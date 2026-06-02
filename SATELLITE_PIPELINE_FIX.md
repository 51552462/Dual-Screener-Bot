# SATELLITE_PIPELINE_FIX — 위성망 실종·0건 증발 병목 수술 보고

## 원인 요약

이번 0건 증발 병목은 단일 원인이 아니라 다음 3개가 겹쳤습니다.

1. **`alt_data.sqlite` 경로/초기화 취약**
   - 위성 매크로 프로바이더가 DB 파일 미존재 시 곧바로 `"매크로: 데이터 없음 (alt_data.sqlite 없음)"`으로 고착.
   - 초기 실행 또는 경로 이관 후 DB touch/DDL이 보장되지 않아 위성 입력이 비어버림.

2. **보조 데이터 결측 시 교집합 드랍 성격의 처리**
   - `supernova_hunter.py`의 벤치 정렬 구간에서 결측 처리 방식이 공격적이어서
     보조 시계열 결측 시 종목 특징 계산이 쉽게 `NaN`으로 흘러감.
   - 보조(위성/벤치) 데이터가 비어도 **종목 자체는 유지**하도록 안전 폴백이 필요.

3. **스필오버/하이드레이트 stale 고착**
   - `cross_market_theme_snapshot` 단계에서 중간 예외 발생 시 publish/hydrate 체인이 끊어질 수 있음.
   - `kr_cross_market_hydrate` 시점에서 stale standalone 상태를 적극 재산출하지 않아
     산출일이 오래된 상태로 머무를 수 있음.

---

## 적용한 수정

### Action 1) `alt_data.sqlite` 경로 실종 복구

수정 파일:
- `factory_data_paths.py`
- `satellite_intel_brief.py`

변경:
- `factory_data_paths.alt_data_db_path()`가 호출될 때
  `ensure_alt_data_db_initialized()`를 통해 DB 파일/최소 스키마를 자동 보장.
- 최소 스키마:
  - `macro_daily(date, usd_krw, us_10y_yield, vix_index)`
- `satellite_intel_brief._alt_data_db_path()`도 후보 경로 선택 후
  즉시 초기화 함수를 호출하도록 변경.

효과:
- 파일이 없어도 즉시 생성되어 경로 실종으로 인한 고착 크래시 차단.

### Action 2) 보조 데이터 결측 시 종목 증발 방지

수정 파일:
- `supernova_hunter.py`

변경:
- `_approx_dyn_rs_vs_benchmark()`에서 벤치 결측 처리 강화:
  - 기존: concat 후 사실상 교집합 성격의 결측 드랍
  - 변경: `Close`는 유지, 벤치(`__ix`)가 비면 종가로 폴백
- 즉, 위성/벤치가 비어도 종목 자체가 파이프라인에서 탈락하지 않도록 방어.

효과:
- 보조 데이터 공백으로 스캐너 본체가 연쇄 0건으로 무너지는 병목 완화.

### Action 3) 스필오버 강제 최신화(stale 갱신)

수정 파일:
- `factory_pipelines.py`
- `cross_market_ssot.py`

변경:
- `factory_pipelines._step_cross_market_theme_snapshot()`:
  - `refresh -> publish -> hydrate`를 각각 방어적으로 분리
  - 중간 실패가 나도 다음 단계를 계속 수행
  - publish 실패 시에도 hydrate 수행하여 런타임 stale 고착 완화
- `cross_market_ssot.hydrate_kr_runtime_from_ssot()`:
  - `KR_STANDALONE` + `us_sector_raw` 공백이면
    `publish_us_market_snapshot(source="kr_hydrate_republish")` 1회 재시도 후 재로드

효과:
- 산출일 정체(예: 5/28 고착) 상태를 hydrate 진입점에서 자동 풀어내는 복원력 확보.

---

## 수정 파일 목록

- `factory_data_paths.py`
- `satellite_intel_brief.py`
- `supernova_hunter.py`
- `factory_pipelines.py`
- `cross_market_ssot.py`

---

## 배포

로컬에서:

```bash
git add . && git commit -m "Fix satellite paths and Left Join trap" && git push origin main
```

서버에서:

```bash
sudo bash update_bot.sh
```

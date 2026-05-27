# DATA_RECOVERY_AUDIT_PROPOSAL

작성 목적: "데이터 영구 유실" 판단의 타당성을 재검증하고, 과거 데이터 연속성을 최대한 회복하는 복구 시나리오를 제안한다.  
범위: 코드 수정 없이 파일 시스템 스캔 + 기존 운영 문서(`DATA_RECOVERY_AND_ENVIRONMENT_CHECK.md`) 분석.

---

## 1) 물리적 데이터 생존 여부 스캔 결과

요청 범위 기준(워크스페이스 + `~/dante_bots/Dual-Screener-Bot` 계열)으로 `.sqlite`, `.db`, `.bak`, `.snapshot` 파일을 확인했다.

## 1-1. 파일 목록 (크기/수정시각)

| path | size_MB | modified_local_time |
|---|---:|---|
| `C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\market_data.sqlite` | 0.043 | 2026-05-19 01:02:33 |
| `C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\message_queue.sqlite` | 0.016 | 2026-05-27 13:56:14 |
| `C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\ops_events.sqlite` | 0.039 | 2026-05-27 14:07:22 |
| `C:\Users\GoodLife\dante_bots\Dual-Screener-Bot\system_config.sqlite` | 0.012 | 2026-05-18 14:33:53 |

## 1-2. 루트별 집계

- `C:/Users/GoodLife/Desktop/quant/Dual-Screener-Bot`: `0` files / `0.000 MB`
- `C:/Users/GoodLife/dante_bots/Dual-Screener-Bot`: `4` files / `0.109 MB`
- `C:/Users/GoodLife/Desktop/dante_bots/Dual-Screener-Bot`: root not found

## 1-3. 팩트 체크 결론

- "파일이 0건"은 아니며, 물리 파일은 **생존**해 있다.
- 다만 총 용량이 `0.109 MB` 수준으로 매우 작아, **의미 있는 과거 거래 히스토리(대량 `forward_trades`)가 들어 있는 상태로 보긴 어렵다.**
- 즉 "완전 무(0) 상태는 아님" + "실질적 과거 운용 데이터는 거의 없는 상태"가 동시 성립 가능성이 높다.

---

## 2) 'Day 1 전환' 근본 원인 분석

분석 대상: `DATA_RECOVERY_AND_ENVIRONMENT_CHECK.md`

## 2-1. 문서상 선언된 직접 원인

문서의 핵심 조건은 아래 3개 중 하나 충족 시 Day1 확정:
1. `/var/backups/dante-dr/`에 `dante-sqlite-*.tar.gz` 없음
2. 복원 후보를 찾아도 `forward_trades` 표본 기준 미달
3. 복원 시도 실패/지연으로 운영 안정성 우선

즉, **기술적 불가능**이라기보다 **백업 발견 실패 + 운영 판단(시간/안정성 우선)**으로 Day1을 선언한 구조다.

## 2-2. 치명 에러의 본질 (아키텍처 관점)

이번 케이스의 핵심은 "DB 엔진이 복구 불가능하게 깨졌다"가 아니라 아래의 운영/탐색 한계다.

- 백업 소스 탐색 스코프가 충분히 체계적이지 않았을 가능성
- `DB_STORAGE_PATH`와 실제 운용 경로(서버/로컬)가 다를 때 탐색 누락 가능성
- "빠른 서비스 복구"를 위해 Day1을 운영 우선안으로 채택

따라서 본 사건은 **물리 데이터 완전 소실 확정 사건**이라기보다,  
**백업/경로 추적 체계 미흡 + 시간 제약 하 의사결정**에 가깝다.

---

## 3) 복구 및 파이프라인 재연결 역제안

목표: Day1 고정이 아니라, 가능한 경우 과거 연속성을 복원.

## 3-1. 복구 전략 의사결정 트리

### A안: "대용량 원본 DB"가 서버/외부볼륨에 존재하는 경우 (최우선)

1. 원본 DB를 **읽기전용**으로 먼저 점검:
   - `forward_trades` row count
   - 가장 최근 `exit_date`
   - 핵심 테이블 스키마 유효성
2. 현재 운영 DB를 백업 후 원본을 `DB_STORAGE_PATH`로 교체 또는 병합
3. `sqlite_schema_guard` + `factory_artifact_guard` 실행
4. `scan-kr/us` -> `daily-kr/us` 순으로 재개

### B안: 원본이 여러 조각 DB로 흩어진 경우 (병합)

`ATTACH DATABASE` 기반으로 테이블 단위 병합:
- `forward_trades`는 PK/unique 기준 dedupe merge
- `ops_events`는 `id` 충돌 방지하여 append
- `system_config`는 최신 timestamp 우선 merge + 민감키는 env SSOT 유지

### C안: 현재처럼 소용량 DB만 있는 경우 (준-Day1 + 최소 연속성)

- `market_data.sqlite` 안에 남은 OHLCV/구성 테이블은 유지
- `system_config.sqlite`의 전략/파라미터는 최대 보존
- `ops_events.sqlite`로 최근 운영 상태/오류 fingerprint 복구
- 결과적으로 "완전 초기화"가 아니라 "부분 연속성 유지 재시작"으로 정의

---

## 4) 실행 가능한 구체 시나리오 (권장)

## Phase 0. 포렌식 스냅샷 고정

- 현재 발견된 4개 파일을 즉시 별도 보관(읽기전용 복사)
- 해시(SHA256) 생성해 증거 체인 유지

## Phase 1. 서버 측 재탐색 강화

단순 `/var/backups/dante-dr`만 보지 말고 아래를 포함:
- 과거 `DB_STORAGE_PATH` 이력(쉘 히스토리, systemd EnvironmentFile, 이전 `.env`)
- `rsync/scp` 대상 경로 로그
- 클라우드 백업 버킷 prefix 전체 탐색 (날짜 prefix 외 전체)
- 디스크/볼륨 마운트 이력 경로

## Phase 2. "발견 시 즉시 병합" 자동화

복구 스크립트를 2단계로 분리:
1. `audit` 모드: 후보 DB 인덱싱/스키마/카운트만 출력
2. `recover` 모드: dry-run diff -> 승인 -> 병합/교체 실행

이렇게 해야 운영자가 Day1 선언 전에 "복원 가능성 수치"를 확인 가능.

## Phase 3. 운용 재개 가드레일

- `forward_trades`가 0이면 서비스는 올라가되 상태를 `DEGRADED`로 표기
- 최소 표본 미달 시 텔레그램으로 "복구 모드" 알림
- 자동 Day1 전환은 금지하고, 승인 플래그가 있어야만 전환

---

## 5) 재발 방지 고도화 제안

## 5-1. 백업 정책

- `DB_STORAGE_PATH` 전체 스냅샷 백업을 기본값으로 고정
- 단일 파일(`market_data.sqlite`) 백업이 아니라 세트 백업:
  - `market_data.sqlite`
  - `ops_events.sqlite`
  - `system_config.sqlite`
  - 스냅샷/파생물

## 5-2. 복구 정책

- "백업 미발견 = 즉시 Day1" 금지
- 최소 2회 탐색(로컬 + 서버 + 원격 백업) 후에만 전환
- 전환 직전 자동 보고서 생성(후보 경로/용량/row count 포함)

## 5-3. 파이프라인 정책

- 데이터 결손 상태를 런타임 메타로 명시 (`RECOVERY_MODE`, `HISTORICAL_COVERAGE_PCT`)
- 리포트에 "히스토리 커버리지"를 출력해 의사결정 오판 방지

---

## 6) 최종 판단

1. 물리 DB 파일은 남아 있다(0건 아님).  
2. 그러나 현재 발견 용량 기준으로는 과거 실거래 히스토리가 충분히 남아 있다고 보기 어렵다.  
3. 따라서 지금 가장 중요한 것은:
   - 서버/원격 백업 경로 재탐색을 체계화하고,
   - 발견 시 병합 가능한 복구 파이프라인을 준비해,
   - Day1 단정 대신 "연속성 복구 가능성"을 수치로 판단하는 운영 체계로 바꾸는 것.


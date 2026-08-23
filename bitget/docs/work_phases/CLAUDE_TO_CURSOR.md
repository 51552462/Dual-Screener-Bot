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

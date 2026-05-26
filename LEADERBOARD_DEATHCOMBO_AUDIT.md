# LEADERBOARD_DEATHCOMBO_AUDIT — [2/9] 리더보드 · [5/9] 데스콤보 데이터 질식 전수 감사

**작성일:** 2026-05-26  
**증상:** 일일 통합 리포트(`send_comprehensive_daily_report`)에서 KR/US 모두  
- `[2/9] 로직별 복리 생존 리더보드` → `↳ 매매 데이터 없음`  
- `[5/9] 티어 및 데스콤보 검증` → `↳ 검증 표본 부족`  
**핵심 파일:** `forward/deep_dive.py` (통합 9분할), `forward/shared.py`, `report_collectors.py`  
**참고:** `run_deep_dive_analysis()`는 **별도** 단일 메시지 리포트이며 **[2/9]/[5/9] 섹션 번호 없음**

---

## Executive Summary

| 섹션 | 직접 트리거 조건 | Timekeeper 사용 | 1차 근본 원인 |
|------|------------------|-----------------|---------------|
| [2/9] | `df_real.empty` | **없음** | 시장 필터 후 **행 0건** (DB·`market`·`code` 정규화) — Timekeeper와 **무관** |
| [5/9] | `t1_df.empty` **且** `dc_df.empty` | **없음** | `tier=='80점대'` 표본 없음 + **`is_death_combo` DB에 미저장(항상 0)** |

Timekeeper SSOT 이식은 **`run_deep_dive_analysis`** 에만 적용되어 있고, **[2/9]/[5/9] 경로와 분리**되어 있다.  
다만 동일 factory 실행 직후 DB 워터마크·표본 부족이면 **체감상 “시계 붕괴 후 전부 죽었다”**로 보일 수 있다.

---

## 1. 데이터 질식(Empty Data)의 근본 원인 규명

### 1.1 [2/9] 로직별 복리 생존 리더보드 — 코드 경로

**유일 송출 위치:** `send_comprehensive_daily_report()` (`forward/deep_dive.py`)

```136:145:forward/deep_dive.py
            conn = _open_market_db_ro()
            df_all_raw = pd.read_sql("SELECT * FROM forward_trades", conn)
            df_all = _daily_report_trades_for_market(df_all_raw, market)
            df_real = _df_long_only(df_all)
            df_closed = df_real[df_real['status'].str.contains('CLOSED', na=False)]
```

```197:230:forward/deep_dive.py
            msg2 = f"{market_icon} <b>[2/9] 로직별 복리 생존 리더보드</b>\n"
            if not df_real.empty:
                ...
            else: msg2 += " ↳ 매매 데이터 없음\n"
```

| 단계 | 쿼리/필터 | 날짜 조건 |
|------|-----------|-----------|
| SQL | `SELECT * FROM forward_trades` | **없음** (전기간) |
| 시장 | `_daily_report_trades_for_market` → `_normalize_trade_market(code, market)` | **없음** |
| 롱만 | `_df_long_only` — `INCUBATOR`·`INVERSE` sig 제외 | **없음** |
| 리더보드 | `sig_type` 그룹별 CLOSED + `sim_kelly_invest` PnL | **없음** |

**결론:** [2/9]는 `ReportTimekeeper`·`session_anchor`·`rolling_cutoff`를 **전혀 사용하지 않는다.**  
`datetime.now()`도 **리더보드 슬라이스에 쓰이지 않는다** (섹션 [7/9] 순환매 `entry_date >= now-60d` 만 별도 사용).

`↳ 매매 데이터 없음` = **`df_real` 행 수 0** (OPEN+CLOSED 합쳐서 한 건도 없음).

### 1.2 [5/9] 티어 및 데스콤보 검증 — 코드 경로

```314:322:forward/deep_dive.py
            msg5 = f"{market_icon} <b>[5/9] 티어 및 데스콤보 검증</b>\n"
            t1_df = df_closed[df_closed['tier'] == '80점대']
            dc_df = df_closed[df_closed['is_death_combo'] == 1]
            ...
            if t1_df.empty and dc_df.empty: msg5 += " ↳ 검증 표본 부족\n"
```

| 서브블록 | 필터 | Timekeeper |
|--------|------|------------|
| 80점대 티어 | `tier == '80점대'` (문자열 **완전 일치**) | 없음 |
| 데스콤보 | `is_death_combo == 1` | 없음 |
| `df_closed` | `df_real` + `status` contains `CLOSED` | 없음 |

**`df_closed`가 비어 있지 않아도** [5/9]는 “표본 부족” 가능 — `t1`·`dc` 둘 다 비면 됨.

### 1.3 Timekeeper가 있는 경로와의 대비 (혼동 지점)

`run_deep_dive_analysis(market)` (`forward/deep_dive.py` L696~):

```731:741:forward/deep_dive.py
            df = pd.read_sql(
                """
                SELECT * FROM forward_trades
                WHERE market=? AND status LIKE 'CLOSED%'
                  AND substr(IFNULL(exit_date,''),1,10) >= ?
                  AND substr(IFNULL(exit_date,''),1,10) <= ?
                  ...
                """,
                params=(market, tk.rolling_cutoff, tk.session_anchor),
            )
```

| 항목 | `run_deep_dive_analysis` | `send_comprehensive` [2/9]/[5/9] |
|------|--------------------------|----------------------------------|
| ReportTimekeeper | ✅ | ❌ |
| 롤링 윈도우 SQL | ✅ `[cutoff, anchor]` | ❌ 전기간 |
| 표본 &lt; 10 | 전체 딥다이브 **조기 return** | 해당 없음 |
| 섹션 번호 | 없음 (통합 메시지) | [2/9], [5/9] |

→ Timekeeper 이식 **이후** 포워드 딥다이브만 앵커 윈도우로 좁아지고, **통합 9분할 [2/9]/[5/9]는 예전 그대로**라 “한쪽만 살아 있고 한쪽은 질식” 비대칭이 발생할 수 있다.

### 1.4 [2/9] 질식 가능 시나리오 (우선순위)

1. **`forward_trades` 자체가 비었거나 해당 `market` 행 0** — `track`/`scan` 미실행.  
2. **`_daily_report_trades_for_market` 전부 탈락** — DB `market` 열과 `code` 정규화 불일치 (예: US 티커인데 `market='KR'`만 있으면 US 리포트 0건).  
3. **`_df_long_only` 전부 제외** — 모든 `sig_type`이 `INCUBATOR` 또는 `INVERSE` (드묾).  
4. **(오해)** Timekeeper `rolling_cutoff` — **[2/9] 미적용**이므로 직접 원인 아님.

### 1.5 [5/9] 질식 가능 시나리오 (우선순위)

1. **`is_death_combo` 미저장 (구조적 버그)**  
   - `try_add_virtual_position` → `_insert_forward_trade_row` 컬럼 목록에 **`is_death_combo` 없음** (`forward/shared.py` `_FORWARD_TRADE_INSERT_COLS`).  
   - 스캐너(`master.py` 등)는 `dbg['is_death_combo']`를 계산하지만 **`try_add_virtual_position` 인자로 전달하지 않음** → DB 기본값 **0**.  
   - → `dc_df`는 **사실상 항상 empty**.

2. **`tier == '80점대'` 표본 희소**  
   - 적재 시: `score_bucket = int(score // 10) * 10` → `tier_label = f"{score_bucket}점대"` (80~89점만 `80점대`).  
   - 70점대·90점대만 있으면 `t1_df` empty.

3. **`df_closed` empty** — [2/9]도 empty면 동시 발생.

4. **`_tier80_sync_effective_and_report_line`이 빈 문자열** — `t1_df` empty 시 DB만 NULL 갱신, 메시지 없음 → 사용자는 “표본 부족” 한 줄만 봄.

### 1.6 하드코딩·레거시 시계 (통합 리포트 내)

| 위치 | 시계 | 비고 |
|------|------|------|
| [1/9] `today_str` | `datetime.now(tz_kr)` L53 | Timekeeper 아님 |
| [7/9] `rot_df` | `datetime.now() - 60d` KST | Timekeeper 아님 |
| [2/9],[5/9] | 날짜 필터 없음 | 전기간 집계 |

---

## 2. KR / US 양방향 대칭성 및 파이프라인 검증

### 2.1 동일 코드·동일 조건

```133:134:forward/deep_dive.py
    for market in ['KR', 'US']:
```

KR/US **동일 루프** · 동일 SQL · 동일 `_daily_report_trades_for_market` · 동일 [2/9]/[5/9] 로직.

| 검증 | KR | US |
|------|----|----|
| [2/9] empty 조건 | `df_real.empty` | 동일 |
| [5/9] empty 조건 | `t1`∧`dc` empty | 동일 |
| Timekeeper | 미사용 | 미사용 |
| `is_death_combo` INSERT | 미포함 | 미포함 |
| `tier` 80점대 | 80~89점 버킷 | 동일 |

**양쪽 모두 “데이터 없음”** → 단일 시장만의 쿼리 버그보다 **공통 상위 원인** 가능성이 큼:
- DB 공통(empty / market 정규화)
- 또는 [5/9]는 **전 시장 공통**으로 `is_death_combo`·`80점대` 부재

### 2.2 팩토리 파이프라인 순서

```text
track_kr/us → deep_dive_kr/us → comprehensive_daily_report → ...
```

| 질문 | 답 |
|------|-----|
| track 전에 comprehensive? | **아니오** — track 후 |
| deep_dive가 df를 비워 내려줌? | **아니오** — comprehensive는 **DB 재조회** |
| deep_dive &lt;10건 skip이 [2/9]에 영향? | **간접 없음** — comprehensive는 별도 로드 |

**단,** `track` 미실행·청산 0이면 `df_closed` 빈약 → [5/9] 악화. [2/9]는 OPEN만 있어도 `df_real` non-empty여야 함.

### 2.3 상위에서 “빈 df”가 내려오는가?

**아니오.** comprehensive는 팩토리가 **DataFrame을 인자로 넘기지 않음**. 매 시장마다 `_open_market_db_ro()`로 **자체 로드**.  
질식은 **팩토리가 빈 df를 전달**한 것이 아니라 **로컬 필터 결과가 빈 것**.

### 2.4 KR/US 시장 정규화 리스크

`_normalize_trade_market` (`forward/shared.py` / `forward_market_guard.normalize_trade_market`):

- 숫자 5~6자리 코드 → KR  
- `[A-Z]…` 티커 → US  

DB에 `market` 열이 잘못되어 있어도 **code 기준으로 재분류**하므로, 극단적 오태깅은 줄지만 **양 시장 모두 표본이 적으면** 동시에 빈 메시지 가능.

---

## 3. 상태(Status) 필터링 및 표본 조건 오류 점검

### 3.1 [2/9] Status·투자금

```204:207:forward/deep_dive.py
                    g_closed = g_df[g_df['status'].str.contains('CLOSED', na=False)]
                    valid_invest = g_closed['sim_kelly_invest'].replace(0, 400000)
```

| 항목 | 설정 | 문제 |
|------|------|------|
| CLOSED | `str.contains('CLOSED')` | `CLOSED_WIN`/`CLOSED_LOSS` 포함 — 정상 |
| OPEN | 리더보드 `op` 집계에 사용 | 정상 |
| `sim_kelly_invest` 0 | 400000 치환 | PnL 왜곡 가능, **empty 원인 아님** |
| 최소 그룹 수 | **없음** | 그룹 0개면 for 루프 0회이나, `df_real` non-empty면 “매매 데이터 없음” **아님** |

`df_real` non-empty인데 리더보드 줄이 없는 경우: `group` unique 0 (sig_type 전부 빈) — 드묾.

### 3.2 [5/9] 80점대 · min_samples

**티어 동적 강등 (`_tier80_sync_effective_and_report_line`):**

```1117:1120:forward/shared.py
    elif n < 5:
        db_val = "UNCONFIRMED"
```

| 조건 | 값 | 영향 |
|------|-----|------|
| `t1_df` empty | n=0 | sync만 수행, **텔레그램 티어 줄 없음** |
| n &lt; 5 | UNCONFIRMED | 1~4건이어도 **표시는 가능** (빈 t1이면 표시 없음) |
| `tier == '80점대'` | 정확 일치 | `80` / `RANK_A` / NULL → **미매칭** |

**min_samples가 비정상적으로 높음?** → **아니오.** 리포트 게이트는 **“t1∧dc 둘 다 empty”** 단순 OR. n≥5는 DB `tier_effective` 산출용.

### 3.3 [5/9] 데스콤보 — 구조적 0건

스캐너는 진입 시 데스콤보를 계산:

```595:595:master.py
    is_death_combo = (cur_cpv > 0.85) and (cur_rs < 0)
```

DB INSERT SSOT:

```760:788:forward/shared.py
_FORWARD_TRADE_INSERT_COLS: tuple[str, ...] = (
    "entry_date", "market", "code", ... "entry_regime",
)
# is_death_combo 없음
```

**리포트는 DB 플래그만 조회** → **실전 필터 작동 여부와 리포트가 단절**.

### 3.4 [5/9] 메시지 로직 결함

```321:322:forward/deep_dive.py
            if not dc_df.empty: msg5 += f"💀 데스콤보 승률: ..."
            if t1_df.empty and dc_df.empty: msg5 += " ↳ 검증 표본 부족\n"
```

- `t1` only / `dc` only 인 경우: **둘 중 하나만 있어도** “표본 부족” 미출력 — OK.  
- `t1` empty, `dc` empty, **`df_closed` 수백 건**: 사용자에게 **“표본 부족”** — **오해 유발** (실제로는 티어·데스콤보 **컬럼/적재** 문제).

---

## 4. [역제안] Timekeeper 100% 강제 종속 아키텍처

### 4.1 `DailyReportContext` (신규 SSOT)

`ColosseumReportContext` / `PractitionerReportContext`와 동일 패턴으로 **일일 9분할 전용** 컨텍스트:

```python
@dataclass(frozen=True)
class DailyReportContext:
    tk_kr: ReportTimekeeper
    tk_us: ReportTimekeeper
    window_days: int

    @classmethod
    def build(cls, *, rolling_days: int = 90) -> "DailyReportContext": ...

    def timekeeper_for(self, market: str) -> ReportTimekeeper: ...

    def load_market_trades(self, conn, market: str) -> pd.DataFrame:
        """CLOSED+OPEN, [rolling_cutoff, session_anchor], INCUBATOR 제외."""
        tk = self.timekeeper_for(market)
        ...

    def header_html(self) -> str:
        """KR/US 앵커 · 워터마크 · lag · 읽기소스."""
```

**강제 규칙:**

- `send_comprehensive_daily_report(ctx: DailyReportContext | None = None)`  
  - `ctx is None` → `DailyReportContext.build()`  
  - **금지:** 모듈 내부 `datetime.now()-N` 로 `df_closed` 슬라이스 ([7/9] 포함 전부 `ctx` 경유)

### 4.2 [2/9] 리더보드 듀얼 트랙 (선택 P0)

Timekeeper 윈도우 내 집계 + 헤더:

```text
📎 KR앵커 · US앵커 · lag · 롤링 N일
[2/9] 로직별 복리 생존 리더보드 (롤링 윈도우 내)
 🟢 당일 청산 포함 그룹 …  ← anchor일 CLOSED
 🏛️ 윈도우 전체 …         ← [cutoff, anchor] 전체
```

`df_real.empty` 시:

```text
↳ 매매 데이터 없음 (KR 표본 0 · 앵커 {anchor} · 워터마크 {wm} · lag {n}일)
```

→ **허공 메시지 제거**.

### 4.3 [5/9] 데스콤보 · 티어 복구 (P0)

**A. 적재 SSOT**

```python
# try_add_virtual_position insert_row 확장
"is_death_combo": int(facts.get("is_death_combo") or 0),
"is_tenbagger": int(facts.get("is_tenbagger") or 0),
...
```

스캐너 → `facts` dict에 플래그 전달 (또는 `normalize_sector_for_db`와 같이 중앙 관문).

**B. 리포트 시 재계산 (백필 없이도 동작)**

```python
def compute_death_combo_row(row) -> bool:
    cpv = float(row.get("dyn_cpv") or row.get("v_cpv") or 0)
    rs = float(row.get("dyn_rs") or row.get("v_rs") or 0)
    return cpv > 0.85 and rs < 0  # market별 threshold config
```

`dc_df = df_closed[df_closed.apply(compute_death_combo_row, axis=1)]`

**C. 티어 폴백**

```python
def effective_tier_bucket(row):
    t = str(row.get("tier") or "")
    if t.endswith("점대"):
        return t
    sc = pd.to_numeric(row.get("total_score"), errors="coerce")
    if pd.notna(sc):
        return f"{int(sc // 10) * 10}점대"
    return ""
```

**D. [5/9] 메시지 분리**

```text
↳ 80점대 청산 0건 (윈도우 N일)
↳ 데스콤보 0건 (플래그 미저장 시 재계산 사용)
```

`t1`∧`dc` empty **且** `df_closed` non-empty → **“컬럼/버킷 불일치”** 명시.

### 4.4 `run_deep_dive`와 컨텍스트 공유

```python
def run_deep_dive_analysis(market="KR", *, ctx: DailyReportContext | None = None):
    ctx = ctx or DailyReportContext.build()
    tk = ctx.timekeeper_for(market)
    ...
```

factory `comprehensive` 직전 1회 `ctx = DailyReportContext.build()` 생성 → deep_dive + comprehensive **동일 앵커** 공유.

### 4.5 Fail-safe · 관측

| lag | 동작 |
|-----|------|
| ≥2 | [2/9] 롤링만 표시 + RED 배너 |
| live_closed=0 | “당일 청산 0 — 윈도우 내 과거만” |

`ops_events`: `leaderboard.empty`, `deathcombo.column_missing`.

---

## 5. 통합 역제안 로드맵

| 순위 | 작업 | 파일 | 효과 |
|:----:|------|------|------|
| P0 | `DailyReportContext` + `load_market_trades` | 신규 + `forward/deep_dive.py` | Timekeeper 9분할 SSOT |
| P0 | `is_death_combo` INSERT + 리포트 재계산 폴백 | `forward/shared.py`, [5/9] | 데스콤보 질식 해소 |
| P0 | [2/9]/[5/9] 헤더에 앵커·wm·lag·표본 수 | `forward/deep_dive.py` | 질식 vs 무표본 구분 |
| P1 | `tier` NULL 시 `total_score` 버킷 | [5/9] | 80점대 표본 복구 |
| P1 | [7/9] `rot_df` → `ctx.rolling_cutoff` | `forward/deep_dive.py` | 잔여 `datetime.now` 제거 |
| P2 | factory에서 `ctx` 1회 생성·공유 | `factory_pipelines.py` | deep_dive·comprehensive 정합 |

---

## 6. 검증 체크리스트 (수정 후)

1. [2/9] 상단: `KR앵커` · `US앵커` · `DB워터마크` · `lag` · `롤링 N일` · `표본 M건`.  
2. `df_closed` ≥ 1 인데 [5/9] “표본 부족” **금지** — 원인별 분리 메시지.  
3. SQL: `SELECT COUNT(*) ... is_death_combo=1` &gt; 0 (적재 후) 또는 재계산 `dc_df` &gt; 0.  
4. `tier='80점대'` COUNT + `total_score` 80~89 COUNT 비교 로그.  
5. KR/US **동일 ctx**로 deep_dive·comprehensive 실행 시 앵커 문자열 일치.

---

## 7. 참고 코드 인덱스

| 심볼 | 파일 |
|------|------|
| `send_comprehensive_daily_report` | `forward/deep_dive.py` L6~509 |
| [2/9] 리더보드 | `forward/deep_dive.py` L188~231 |
| [5/9] 데스콤보 | `forward/deep_dive.py` L311~323 |
| `_daily_report_trades_for_market` | `forward/shared.py` L956~969 |
| `_df_long_only` | `report_collectors.py` L25~32 |
| `_tier80_sync_effective_and_report_line` | `forward/shared.py` L1090~1151 |
| `_FORWARD_TRADE_INSERT_COLS` | `forward/shared.py` L760~788 |
| `run_deep_dive_analysis` (Timekeeper O) | `forward/deep_dive.py` L696~769 |
| `load_dual_track_frames` | `forward_dual_track_queries.py` |
| `ColosseumReportContext` | `colosseum_report_context.py` (참고 패턴) |

---

## 8. 운영 DB 확인 쿼리 예

```sql
-- 시장별 전체 / 청산 / OPEN
SELECT market, status, COUNT(*) FROM forward_trades
GROUP BY market, status;

-- [5/9] 80점대 · 데스콤보
SELECT market,
  SUM(CASE WHEN tier='80점대' THEN 1 ELSE 0 END) AS n_tier80,
  SUM(CASE WHEN is_death_combo=1 THEN 1 ELSE 0 END) AS n_dc,
  COUNT(*) AS n_closed
FROM forward_trades
WHERE status LIKE 'CLOSED%'
GROUP BY market;

-- 데스콤보 재계산 후보 (KR, dyn 컬럼)
SELECT COUNT(*) FROM forward_trades
WHERE market='KR' AND status LIKE 'CLOSED%'
  AND COALESCE(dyn_cpv, v_cpv, 0) > 0.85
  AND COALESCE(dyn_rs, v_rs, 0) < 0;

-- 최신 청산일 (Timekeeper lag 진단)
SELECT market, MAX(substr(exit_date,1,10)) FROM forward_trades
WHERE status LIKE 'CLOSED%' GROUP BY market;
```

---

*정적 코드 분석 기준. [2/9] “매매 데이터 없음”은 Timekeeper 미적용 경로에서 `df_real` 0건일 때만 발생한다. [5/9] “검증 표본 부족”은 Timekeeper와 독립적으로 `tier`/`is_death_combo` 필터 결과가 동시에 비었을 때 발생하며, `is_death_combo` 미저장이 구조적으로 가장 유력하다.*

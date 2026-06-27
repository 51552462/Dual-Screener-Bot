# 주간 Flow 총결산 — KR/US Flow 태그 "표본 없음" 원인 검토

> **[2026-06-27 업데이트] FALSE ZERO 입증 → 패치 적용 완료.**
> 서버 원천 쿼리 결과 최근 7일 KR 40건·US 20건 청산 + 유효 flow_tags 가 존재함에도
> 리포트가 "표본 없음"으로 마스킹된 **FALSE ZERO** 가 확정됐다. 아래 4개 결함을 모두 패치했다.
> - M1 `scripts/weekly_zero_diag.py` 경로 주입(`sys.path.append(...)`) — ModuleNotFound 해소.
> - M2 주간 롤업이 `build_flow_tag_snapshot(force_aggregate=True)` 로 RED 게이트 바이패스(과거 확정 청산 강제 집계).
> - M3 `build_flow_tag_snapshot(weekly_window=True)` → `FLOW_TAG_WEEKLY_MIN_N`(기본 2)로 동적 다운스케일.
> - M4 `WeeklyFlowTagRollup.empty_reason_html` 신설 → 렌더러가 실제 사유 출력(하드코딩 "표본 없음" 제거).
> 검증: RED 강제 상태에서 OLD blocks=0(skip) → NEW blocks=3(독성 포함) 회복 확인.


> 결론 요약: **데이터 `df` 자체는 정상적으로 연결되어 있다.** 주간 PnL 섹션과 Flow 태그 롤업은
> 동일한 `forward_trades` 조회 결과(`flow_tags` 컬럼 포함)를 공유한다. 따라서 "df가 태그
> 모듈로 안 넘어가서" 비는 1차적 배선 누락은 **아니다.**
>
> 진짜 문제는 두 갈래다.
> 1. **사유 전달 배선 누락(확정·코드 근거 있음)** — 렌더러가 RED 스킵 / flow_tags 없음 /
>    min_n 미달 / 진짜 0건이라는 **4가지 다른 원인을 "표본 없음" 한 줄로 뭉개고** 실제
>    사유(`synergy_action_html`)를 버린다. 그래서 "왜 없는지"가 사용자에게 전달되지 않는다.
> 2. **숨은 억제 게이트(확정·코드 근거 있음)** — `RED` 정체 시 태그 집계를 무조건 스킵하는데
>    해제 플래그 `allow_flow_tag` 가 **코드 어디에서도 set되지 않아 항상 False**다. 여기에
>    7일 창에 90일용 `min_n=3` 임계가 그대로 적용된다.
>
> "진짜 데이터 없음(True Zero)" vs "데이터 있는데 안 뜸(False Zero)"의 최종 판정은
> 서버에서 `scripts/weekly_zero_diag.py` 1회 실행으로 확정한다(맨 아래 절차).

---

## 1. 파이프라인 배선 추적 (df가 어디서 와서 어디로 가는가)

```
build_weekly_flow_snapshot()                      # weekly_flow_report.py
  ├─ df_kr / df_us = _load_week_closed_df(...)     # SELECT * FROM forward_trades ... CLOSED%
  │        (flow_tags 컬럼 포함, 7일 창으로 필터)
  ├─ kr_snap = build_weekly_market_snapshot(df_kr) # ← 같은 df (PnL/섹터/MVP)
  ├─ us_snap = build_weekly_market_snapshot(df_us) # ← 같은 df
  ├─ tags_kr = build_weekly_flow_tag_rollup(df_kr) # ← 같은 df (Flow 태그)
  └─ tags_us = build_weekly_flow_tag_rollup(df_us) # ← 같은 df
```

`_load_week_closed_df` 는 `SELECT *` 이므로 `flow_tags` 컬럼이 그대로 실려서 태그 롤업으로
전달된다. **즉, PnL 섹션에 청산 건수가 찍히는데 태그만 "표본 없음"이면, df 누락이 아니라
태그 집계 단계의 게이트/임계에서 걸러진 것이다.**

`flow_tags` 는 **청산(CLOSE) 시점에만** 기록된다:

```445:445:forward/ledger.py
                ''', ('CLOSED_WIN' if ret > 0 else 'CLOSED_LOSS', exit_date, exit_rsn, flow_tags, ret, mfe, ...
```

→ 따라서 그 주에 **청산이 0건이면 `flow_tags` 도 당연히 0건**이다(True Zero). 이는 앞서 고쳤던
`db_watermark` 정체(데이터 갱신 누락 → 신규 진입 0 → 청산 0) 이슈와 직접 연결된다.

---

## 2. "표본 없음" 이 출력되는 정확한 조건

렌더러는 `toxic_tag` 도 없고 `top_tags_lines` 도 비었을 때만 "표본 없음"을 찍는다:

```260:272:weekly_flow_rollup.py
    if rollup.toxic_tag:
        ...
    elif rollup.top_tags_lines:
        out += " ☠️ <b>독성:</b> <i>주간 임계 미충족</i>\n"
    else:
        out += " ↳ flow_tags 표본 없음\n"
```

`top_tags_lines` 는 `blocks`(태그별 집계 결과)가 있어야 채워진다. 즉 **`blocks` 가 비면
"표본 없음"** 이다. `blocks` 가 비는 경로는 `build_flow_tag_snapshot` 안에 **네 군데**가 있고,
모두 `_empty_snapshot(blocks=())` 으로 빠진다.

| # | 게이트 | 코드 위치 | 트리거 조건 | 성격 |
|---|--------|-----------|-------------|------|
| G1 | **RED 정체 스킵** | `forward_flow_tag_deep_dive.py:340` | `staleness.grade=="RED"` and **`allow_flow_tag` 미설정(항상 False)** | False Zero 유발 가능 |
| G2 | **flow_tags 컬럼/0건** | `:356` | `flow_tags` 컬럼 없음 또는 `final_ret` 전부 NaN | 데이터 결손 |
| G3 | **유효 태그 0** | `:384` | 태그가 전부 `nan/none/null/''` (invalid) | 데이터 결손 |
| G4 | **태그별 min_n 미달** | `:412` + `:452` | 어떤 태그도 7일 내 `n ≥ min_n(기본 3)` 미달 | 임계 과다 |

```340:363:forward_flow_tag_deep_dive.py
    if staleness.grade == "RED" and not getattr(staleness, "allow_flow_tag", False):
        return _empty_snapshot(
            timekeeper, staleness,
            synergy=("<i>데이터 정체 RED — flow 태그 집계를 생략합니다. ...</i>"),
            ...
            skipped_red=True,
        )
    empty_synergy = "<i>표본 부족 (flow_tags 컬럼 없음 또는 0건)으로 flow 태그 딥다이브 생략</i>"
    if work is None or work.empty or "flow_tags" not in work.columns:
        return _empty_snapshot(timekeeper, staleness, synergy=empty_synergy, ...)
```

---

## 3. 핵심 결함 #1 — "사유"가 롤업으로 연결되지 않는다 (확정된 배선 누락)

`build_flow_tag_snapshot` 은 빈 경우에도 **왜 비었는지**를 `synergy_action_html` /
`skipped_red` / `staleness_grade` 에 정확히 담아서 반환한다. 그러나 주간 롤업 변환기
`_build_weekly_flow_tag_rollup_inner` 는 **이 사유 필드들을 하나도 읽지 않고 버린다.**

```207:221:weekly_flow_rollup.py
    toxic = snap.toxic
    return WeeklyFlowTagRollup(
        market=str(market).upper(),
        dominant_tag=dominant_tag,        # blocks 없으면 None
        ...
        top_tags_lines=tuple(lines),      # blocks 없으면 ()
        # ← snap.synergy_action_html / snap.skipped_red / snap.staleness_grade 미전달!
    )
```

그 결과 `WeeklyFlowTagRollup` 에는 **"왜 비었는지"를 담는 필드가 아예 없다.** 렌더러는
어쩔 수 없이 G1~G4를 구분 못 하고 전부 **"flow_tags 표본 없음"** 한 줄로 출력한다.

> **사용자가 체감한 "누락되고 연결되지 않았다"는 정확히 이 지점이다.** 데이터가 RED로 스킵된
> 것인지(G1), 컬럼이 빈 것인지(G2/G3), 단지 표본이 임계 미달인지(G4)를 리포트만 봐서는
> 절대 알 수 없다. 사유 배선이 끊겨 있다.

---

## 4. 핵심 결함 #2 — RED 게이트가 영구 활성 (해제 스위치가 죽어 있음)

G1의 해제 플래그 `allow_flow_tag` 는 **읽기만 하고(`getattr(..., False)`) 어디에서도 set
하지 않는다.** 전 코드베이스 grep 결과 set 지점이 없다. 즉 `StalenessVerdict.grade` 가 한 번
`RED` 가 되면 주간 태그 집계는 **무조건** 스킵된다.

주간 롤업의 staleness 는 다음처럼 계산된다:

```166:175:weekly_flow_rollup.py
    tk = _timekeeper_for_weekly_rollup(mkt, today_str)     # rolling_days=90
    staleness = evaluate_staleness(tk, live_row_count=0)   # ← 라이브 0 가정
    snap = build_flow_tag_snapshot(scrubbed, timekeeper=tk, staleness=staleness, ...)
```

`db_watermark`(최근 청산일) 가 며칠 이상 밀리면 `grade=RED` → G1 발동. 이는 **주간 회고
리포트의 성격과 모순**이다. 그 주에 실제로 청산된 거래는 *이미 확정된 과거 사실*인데,
"오늘 기준 데이터가 신선하지 않다"는 이유로 과거 집계를 통째로 가린다.

→ 앞서 `data_refresh`(08:00 KST bulk OHLCV) 크론을 복원하기 **전 기간**에는 청산이 멈춰
`db_watermark` 가 RED 였고, 그 영향으로 KR/US **둘 다** G1에 걸려 태그가 빈 것으로
설명된다(둘 다 동시에 비는 현상과 정합).

---

## 5. 핵심 결함 #3 — 7일 창에 90일용 임계(min_n=3)

`min_n` 기본값은 3이고(`FLOW_TAG_MIN_N`), 이는 원래 90일 롤링 딥다이브용 임계다. 그런데 주간
롤업은 **7일 창** 데이터에 같은 임계를 적용한다(`:412`, `:452`). 한 주에 청산이 태그별로
얇게 흩어지면(예: 8건이 7개 서로 다른 태그) **모든 태그가 n<3 으로 탈락 → G4 → "표본 없음"**.

이 경우는 "데이터가 있는데도" 빈다는 점에서 사용자 우려(False Zero)에 해당한다.

---

## 6. 종합 판정 (Decision Tree)

```
PnL 섹션 청산건수(week_n_closed) == 0 ?
 ├─ 예  → TRUE ZERO. 그 주 실제 청산 0건. (data_refresh 크론 복원 전 정체 영향)
 │         → flow_tags 도 당연히 0. 정상 동작이나, 사유 메시지로 명시돼야 함(결함#1).
 └─ 아니오(청산 > 0인데 태그만 빔) → FALSE ZERO. 아래 중 하나:
      ├─ staleness=RED  → G1(결함#2). allow_flow_tag 죽어서 영구 스킵.
      ├─ flow_tags 전부 공란/invalid → G2/G3. 청산 시 태그 join 실패 점검.
      └─ 태그별 n<3 → G4(결함#3). 7일 창에 과도한 임계.
```

현재 정황(KR·US 동시 공백 + 직전 watermark 정체 이력)상 **결함#2(RED 영구 스킵)** 가
주원인일 가능성이 가장 높고, **결함#1(사유 미전달)** 이 그 진단을 불가능하게 만들고 있다.
단, 최종 확정은 서버 실데이터 1회 점검이 필요하다.

---

## 7. 확정 절차 — 서버에서 1회 실행 (True/False Zero 판별)

이미 준비된 진단 스크립트가 있다. 우분투 서버에서:

```bash
cd ~/dante_bots/Dual-Screener-Bot
python3 scripts/weekly_zero_diag.py
```

출력의 `KR=... US=...`(week_n_closed)와 `VERDICT` 를 본다.
- `TRUE_ZERO` → 그 주 청산이 실제 0건(데이터 없음). 결함#1만 손보면 됨(사유 명시).
- `FALSE_ZERO` / `DATA_PRESENT` 인데 태그가 비면 → 결함#2·#3 패치 필요.

추가로 태그 원천을 직접 확인:

```bash
sqlite3 ~/dante_bots/Dual-Screener-Bot/market_data.sqlite \
  "SELECT market, COUNT(*) AS closed,
          SUM(CASE WHEN IFNULL(TRIM(flow_tags),'')='' THEN 1 ELSE 0 END) AS blank_tags
   FROM forward_trades
   WHERE status LIKE 'CLOSED%' AND exit_date >= date('now','-7 day')
   GROUP BY market;"
```

- `closed>0` 인데 `blank_tags==closed` → 청산 시 태그 join 실패(G2/G3 = 진짜 결손).
- `closed>0` 이고 `blank_tags` 가 작음 → 태그는 있는데 G1(RED) 또는 G4(min_n)로 가려진 것.

---

## 8. 권장 패치 (검토용 제안 — 적용은 별도 지시 시)

### P1. 사유 배선 복구 (결함#1) — 최우선·저위험
`WeeklyFlowTagRollup` 에 사유 필드를 추가하고 렌더러가 구분 출력하도록 연결한다.

- `weekly_flow_rollup.py`
  - 데이터클래스에 `empty_reason_html: str = ""`, `skipped_red: bool = False`,
    `staleness_grade: str = "GREEN"` 추가.
  - `_build_weekly_flow_tag_rollup_inner` 반환 시
    `empty_reason_html=snap.synergy_action_html`, `skipped_red=snap.skipped_red`,
    `staleness_grade=snap.staleness_grade` 전달.
  - `format_weekly_flow_tag_rollup_html` 의 `else` 분기에서 `"표본 없음"` 대신
    `rollup.empty_reason_html`(RED/컬럼없음/임계미달)을 그대로 출력.

### P2. 주간 회고는 RED여도 과거 집계 허용 (결함#2)
주간 롤업 한정으로 `allow_flow_tag=True` 를 부여(또는 `build_flow_tag_snapshot` 에
`force_aggregate=True` 인자 신설)하여, 확정된 과거 청산은 staleness와 무관하게 집계.
스냅샷 헤더에는 "데이터 신선도 주의" 배지를 별도 표기.

### P3. 주간용 min_n 분리 (결함#3)
7일 창에는 `FLOW_TAG_WEEKLY_MIN_N`(예: 2) 같은 별도 임계를 두거나, `min_n` 을
`max(2, round(min_n * 7/lookback_days))` 로 스케일링.

> P1은 진단 가시성만 높이는 무해 패치라 단독 선적용을 권장한다. P2·P3은 집계 정책 변경이라
> 7절 서버 판정 결과(True/False Zero)를 본 뒤 적용 여부를 결정하는 것이 안전하다.

---

## 9. 한 줄 답변

> "연결이 안 돼서" 비는 것 — **부분적으로 맞다.** 단, df 연결이 끊긴 게 아니라
> **(1) 빈 사유가 롤업/리포트로 연결되지 않아 전부 "표본 없음"으로 보이고,
> (2) RED 정체 해제 스위치(`allow_flow_tag`)가 죽어 있어 과거 청산까지 영구 스킵되며,
> (3) 7일 창에 90일용 min_n=3 임계가 과하게 적용**되는 3중 구조 때문이다.
> True Zero(그 주 청산 0건) 여부는 `scripts/weekly_zero_diag.py` 로 즉시 확정 가능하다.

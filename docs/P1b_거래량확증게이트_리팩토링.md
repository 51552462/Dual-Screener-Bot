# P1b(RUNNER_TRAIL) 초효율 거래량 확증(Volume Confirmation) 게이트 — 리팩토링 리포트

> 대상: `forward/ledger.py` 의 P1b 프리러너 볼록 트레일 블록(기존 542~553 라인 부근)만 수정.
> 원칙: **DB 추가 쿼리 0건**(이미 로드된 변수만 재사용), **RL 엔진·타임스탑·1순위 MAE/MFE 일절 불변**.
> 상태: 적용 완료 · `py_compile` 통과 · 게이트 결정 로직 5케이스 단위검증 통과.

---

## 1. 설계 요약

기존 P1b는 `l ≤ trail_px`(장중 저가가 트레일선 하향 돌파)면 **무조건 즉시 `RUNNER_TRAIL` 청산**이었습니다. 여기에 "거래량 없는 페이크 하락이면 청산을 유예"하는 확증 게이트를 끼웠습니다.

판정 흐름(첫 매칭 승):

```
l ≤ trail_px ?
  ├─ NO  → (기존과 동일) 아무 일도 안 함, 러너 유지
  └─ YES → 거래량 확증 게이트
            ├─ c >= o (양봉/도지) ───────────────→ 유예(HOLD) + #RUNNER_유예_거래량페이크
            └─ c <  o (음봉)
                 ├─ ICR ≥ 0.25  OR  RVOL ≥ 1.5 → 확증 → RUNNER_TRAIL 청산(기존 집행)
                 └─ 둘 다 미달               → 유예(HOLD) + #RUNNER_유예_거래량페이크
```

지표 정의(요청 사양 그대로):
- **ICR(기관 이탈률)** = `당일거래량 v / max(1.0, up_vol_sum)`
  → 진입 후 누적된 "상승 에너지(양봉 거래량)" 대비 당일 출회량 비율. 0.25면 기존 매집의 25% 이탈.
- **RVOL(상대 거래량)** = `당일거래량 v / max(1.0, (up_vol_sum + down_vol_sum) / new_bars)`
  → 보유기간 평균 일거래량 대비 당일 거래량 배수. 1.5면 평균의 1.5배 폭증.

> ICR/RVOL 모두 `row_scalar(r, ...)`로 **이번 사이클에 이미 읽은 행 값**과 당일 OHLCV의 `v`만 사용 → 추가 SELECT 없음(O(1)).

---

## 2. 리팩토링된 P1b 블록 (실제 적용 코드)

```542:594:forward/ledger.py
            # [M2] 프리러너 볼록 트레일링 래칫 — 부분익절 후 잔여 물량을 MaxHigh×(1-κ)로 끝까지 추적.
            if not do_exit and _is_free_runner and _xdyn is not None:
                _run_ret = ((new_max - ep) / ep) * 100.0
                _kappa = _xdyn.convex_ratchet_kappa(_run_ret, _ratchet_state)
                _trail_px = _xdyn.trail_stop_price(new_max, _kappa)
                if l <= _trail_px:
                    # [거래량 확증 게이트] 트레일 하향 돌파라도 '거래량 없는 페이크 하락'이면 청산 유예.
                    #   · DB 무접근 — 이미 로드된 v·up/down_vol_sum·new_bars 재사용(O(1), 추가 쿼리 0).
                    #   · 양봉/도지(c>=o)는 추세 이탈로 보지 않고 즉시 유예(홀드).
                    #   · 음봉(c<o)일 때만 ICR/RVOL 평가 → 하나라도 충족이면 확증 청산, 둘 다 미달이면 유예.
                    #   · 1순위 MAE/MFE·2순위 타임스탑·κ RL 엔진은 일절 불변(P1b 내부에서만 분기).
                    if c < o:
                        _icr = v / max(1.0, row_scalar(r, 'up_vol_sum', 1.0))
                        _avg_vol = (
                            row_scalar(r, 'up_vol_sum', 0.0) + row_scalar(r, 'down_vol_sum', 0.0)
                        ) / max(1, int(new_bars))
                        _rvol = v / max(1.0, _avg_vol)
                        _vol_confirmed = (_icr >= 0.25) or (_rvol >= 1.5)
                    else:
                        # 양봉·도지에서 트레일 터치 = 거래량 동반 투매로 보기 어려움 → 유예
                        _icr = _rvol = 0.0
                        _vol_confirmed = False

                    if _vol_confirmed:
                        do_exit, exit_rsn, actual_exit_type = (
                            True,
                            f"프리러너 볼록 트레일 청산 (κ={_kappa:.3f} · 고점 {_run_ret:.0f}% "
                            f"· ICR {_icr:.2f}/RVOL {_rvol:.2f})",
                            "RUNNER_TRAIL",
                        )
                        actual_exit_price = _trail_px
                    else:
                        # 거래량 없는 페이크 하락 — 청산 유예, 러너 보존(RL 표본은 자연 연장돼 자가학습).
                        try:
                            _prev_ft = r.get('flow_tags')
                            _prev_ft = (
                                '' if _prev_ft is None
                                or (isinstance(_prev_ft, float) and pd.isna(_prev_ft))
                                else str(_prev_ft)
                            )
                            _gtag = "#RUNNER_유예_거래량페이크"
                            if _gtag not in _prev_ft:
                                conn.execute(
                                    "UPDATE forward_trades SET flow_tags=? WHERE id=?",
                                    ((f"{_prev_ft} {_gtag}").strip(), r['id']),
                                )
                        except Exception:
                            pass
                        print(
                            f"⏸️ [RUNNER 유예] {code} 거래량 페이크 하락 "
                            f"(l={l:.2f}≤trail={_trail_px:.2f} · ICR {_icr:.2f}/RVOL {_rvol:.2f} 미달) — 러너 유지"
                        )
```

### 변경 전 → 변경 후 핵심 diff
| 항목 | 변경 전 | 변경 후 |
|---|---|---|
| `l ≤ trail_px` 충족 시 | 무조건 `RUNNER_TRAIL` 청산 | 거래량 확증 게이트 통과 시에만 청산 |
| 음봉 평가 | 없음 | ICR≥0.25 OR RVOL≥1.5 → 확증 |
| 양봉/도지 트레일 터치 | (해당 없음, 즉시 청산) | 유예(HOLD) |
| 페이크 하락 | 청산됨 | `do_exit` 유지(False) + `#RUNNER_유예_거래량페이크` 태그 |
| DB 부하 | — | **추가 쿼리 0** (유예 시 태그 1회 UPDATE만, 중복 가드) |

---

## 3. 요건별 충족 점검

### ✅ 요건 1 — DB 부하 없는 실시간 거래량 지표
- `l ≤ _trail_px` 충족 후 곧장 하위 검증으로 진입(즉시 청산 제거).
- 음봉(`c < o`)일 때만 평가, 양봉/도지는 홀드.
- ICR = `v / max(1.0, row_scalar(r,'up_vol_sum',1.0))` (사양 일치).
- RVOL = `v / max(1.0, (up_vol_sum + down_vol_sum)/max(1,new_bars))` (사양 일치).
- 모든 입력은 **이미 메모리에 있는 값**(`v`는 당일 OHLCV, 나머지는 이번 행) → SELECT 추가 없음.

### ✅ 요건 2 — 페이크 하락 방어(Volume Grace Gate)
- `ICR ≥ 0.25` 또는 `RVOL ≥ 1.5` 중 하나라도 충족 → 진짜 이탈로 보고 기존 `RUNNER_TRAIL` 그대로 집행.
- 둘 다 미달(또는 양봉/도지) → `do_exit = False`로 **청산 유예**, `#RUNNER_유예_거래량페이크` 기록 후 포지션 유지.
- 태그는 OPEN 행의 `flow_tags`에 즉시 영속화(유지 분기는 flow_tags를 안 건드리므로 P1b 내부에서 직접 기록). 중복 방지 가드 포함.

### ✅ 요건 3 — RL 엔진 시너지 (무수정)
- `exit_ratchet_rl.py`는 **한 글자도 수정하지 않음**.
- 유예로 청산이 방어되면 해당 러너의 `bars_held`·`final_ret`이 자연 연장됩니다. 이후 실제 청산 시 그 표본이 `evolve_ratchet_kappa`의 `whipsaw_rate`/`giveback_rate` 계산에 자연 반영되어, κ 곡선이 **바뀐 표본으로 스스로 재학습**합니다(개입 없음).
- 1순위 MAE/MFE, 2순위 타임스탑/ATR/기술, 3순위 좀비 로직 모두 불변 — 분기는 오직 P1b의 `l ≤ trail_px` 내부에서만 발생.

---

## 4. 검증

### 4-1. 컴파일
```
python -m py_compile forward/ledger.py  → EXIT=0
```

### 4-2. 게이트 결정 로직 단위검증 (격리 복제)
| 케이스 | 입력 | ICR | RVOL | 결과 |
|---|---|---|---|---|
| 음봉·대량투매 | c<o, v=5000, up=10000 | 0.50 | 2.08 | **청산(확증)** |
| 음봉·거래량없음 | c<o, v=300, up=10000 | 0.03 | 0.13 | **유예** |
| 음봉·RVOL폭증 | c<o, v=4000, avg≈2000 | 0.50 | 2.00 | **청산(확증)** |
| 양봉 트레일터치 | c>o | — | — | **유예** |
| 도지 | c==o | — | — | **유예** |

모두 기대대로 동작(확증=청산, 미달/양봉/도지=유예).

---

## 5. 부수 효과 · 주의

- **유예 누적**: 페이크 하락이 며칠 연속이면 `#RUNNER_유예_거래량페이크`는 중복 없이 1회만 기록(가드). `bars_held`는 계속 증가하므로, 페이크가 길어지면 결국 **2순위 타임스탑/3순위 좀비**가 자연 종료시킵니다(무한 보유 방지). 즉 P1b 유예는 "거래량 없는 단발 흔들기"만 흡수하고, 진짜 시간 만료 방어선은 그대로 살아있습니다.
- **κ는 그대로**: 트레일 가격 `trail_px = new_max×(1−κ)` 산식은 불변. 게이트는 "그 선을 깼을 때 집행할지 말지"만 판단합니다.
- **OPEN 행 flow_tags 사용**: 유예 태그가 OPEN 단계에서 기록되므로, 청산 시점 `flow_tags`에 페이크 유예 이력이 함께 남아 사후 부검(거래량 페이크를 몇 번 흡수했는지) 분석이 가능합니다.

### 변경 파일
- `forward/ledger.py` — P1b 블록(거래량 확증 게이트) 1곳만 수정. 그 외 파일 무변경.

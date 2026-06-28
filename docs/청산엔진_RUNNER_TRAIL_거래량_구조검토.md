# 청산 엔진 구조 정밀 검토 — RUNNER_TRAIL · 거래량 누적 · 거래량 필터 · RL 의존성

> 범위: `forward/ledger.py`(`track_daily_positions`) 중심, `exit_dynamics.py` / `exit_ratchet_rl.py` 연동.
> 본 문서는 **읽기 전용 추적 리포트**입니다. 코드는 일절 수정하지 않았습니다.
> 작성 시점 기준 라인 번호 인용 (인용 블록은 실제 코드).

---

## 0. 청산 사다리 한눈에 (첫 매칭 승)

`track_daily_positions(market)` 루프(`forward/ledger.py:276` ~)는 OPEN 행마다 OHLCV를 받아 아래 순서로 판정합니다. `do_exit`가 True가 되는 즉시 이후 단계는 평가하지 않습니다.

| 단계 | 트리거 변수 | 조건 | exit_type | 청산가 |
|---|---|---|---|---|
| **P1 MAE** | `low_ret_pct` | `≤ dyn_mae_sl` (기본 −3.5%, breadth붕괴 ×0.5) | `STAT_MAE` | `ep×(1+SL/100)` |
| **P1 MFE/분할** | `high_ret_pct` | `≥ dyn_mfe_tp` & 미분할 | `STAT_MFE_FULL`(f_out≥0.999) 또는 부분실현+`free_runner=1` | TP 지정가 |
| **P1b RUNNER_TRAIL(M2)** | `l` (장중 저가) | `l ≤ trail_px` (러너 한정) | `RUNNER_TRAIL` | `trail_px` |
| **P1c 피라미딩(M4)** | `edge_now` | 러너&엣지폭발 → 추가매수(청산 아님) | — | — |
| **P2 모드별** | `bars`, `l`, tech | 타임스탑 / ATR스탑 / 기술이탈 | `*_TIME`/`*_ATR`/`*_TECH` | 종가 또는 `sl_price` |
| **P3 좀비** | `bars` | `≥ time_stop×2` | `ZOMBIE_FORCE_CLOSE` | `ep`(원금) |

핵심 결론을 먼저 요약하면:
- **P1b(RUNNER_TRAIL)는 "부분 익절을 거친 프리러너"에게만** 적용됩니다. 일반 포지션은 절대 진입하지 않습니다.
- 모든 청산 게이트는 **가격(MAE/MFE/ATR/트레일/추세)과 시간(bars)** 으로만 작동합니다. **거래량을 청산 조건으로 쓰는 게이트는 단 하나도 없습니다.**
- `trail_px`는 `exit_dynamics`의 순수 수학(κ)이 계산하며, κ 곡선은 `exit_ratchet_rl`이 주간 RL로 자가 진화시킨 `EXIT_RATCHET_STATE`에서 나옵니다.

---

## 1. RUNNER_TRAIL(P1b) 발동 조건 정밀 추적

### 1-1. 선행 관문 — "프리러너"가 되어야만 평가된다

RUNNER_TRAIL은 P1의 MFE 분할익절 분기에서 `free_runner=1`이 찍힌 포지션만 대상입니다. 즉 **1차 목표가(`dyn_mfe_tp`)를 한 번 터치 → 일부만 팔고(`f_out<0.999`) 잔여를 러너로 전환**한 상태가 전제입니다.

```518:540:forward/ledger.py
            elif high_ret_pct >= dyn_mfe_tp and _scaled_done < 1e-6:
                # [M1] 고정 상한 캡 폐기 — 1차 목표가 도달 시 유동 비율(F_out)만 부분 실현,
                # 나머지는 캡 없는 '프리러너'로 전환하여 우측 꼬리를 끝까지 추적한다.
                _vol_pct = (cur_atr / ep * 100.0) if (ep > 0 and np.isfinite(cur_atr)) else 5.0
                _edge_pre = (current_ret_pct / max(1, int(new_bars))) * (row_scalar(r, 'v_energy', 1.0) / 10.0)
                if _xdyn is not None:
                    f_out = _xdyn.fluid_scale_out_fraction(_meta_regime, _vol_pct, _edge_pre)
                else:
                    f_out = 0.5
                if f_out >= 0.999:
                    # 완전 방어 국면 — 전량 실현(단, 종가가 아닌 TP 지정가 체결)
                    do_exit, exit_rsn, actual_exit_type = True, f"유동 전량익절 (방어국면 F_out={f_out:.0%})", "STAT_MFE_FULL"
                    actual_exit_price = ep * (1 + (dyn_mfe_tp / 100.0))
                else:
                    # 부분 실현분(F_out)은 TP 지정가에서 체결로 적립, 잔여는 러너로 계속 보유
                    _partial_locked = round(f_out * dyn_mfe_tp, 4)
                    conn.execute(
                        "UPDATE forward_trades SET scaled_out_frac=?, realized_partial_ret=?, free_runner=1, max_high=? WHERE id=?",
                        (round(f_out, 4), _partial_locked, new_max, r['id']),
                    )
                    _scaled_done = f_out
                    _is_free_runner = True
                    _realized_partial = _partial_locked
```

- `_is_free_runner`는 위에서 방금 전환됐거나(`free_runner=1` UPDATE) 혹은 이전 사이클에 이미 찍혀 있던 값(`row_scalar(r, 'free_runner', 0.0) >= 1`, `forward/ledger.py:513`)으로 결정됩니다.
- 따라서 **"MFE 한 번도 안 찍은 일반 포지션"은 P1b를 영원히 건너뜁니다.**

### 1-2. RUNNER_TRAIL 본체 — 조건문 그대로

```542:553:forward/ledger.py
            # [M2] 프리러너 볼록 트레일링 래칫 — 부분익절 후 잔여 물량을 MaxHigh×(1-κ)로 끝까지 추적.
            if not do_exit and _is_free_runner and _xdyn is not None:
                _run_ret = ((new_max - ep) / ep) * 100.0
                _kappa = _xdyn.convex_ratchet_kappa(_run_ret, _ratchet_state)
                _trail_px = _xdyn.trail_stop_price(new_max, _kappa)
                if l <= _trail_px:
                    do_exit, exit_rsn, actual_exit_type = (
                        True,
                        f"프리러너 볼록 트레일 청산 (κ={_kappa:.3f} · 고점 {_run_ret:.0f}%)",
                        "RUNNER_TRAIL",
                    )
                    actual_exit_price = _trail_px
```

발동 조건을 단계별로 분해하면:

1. **진입 가드**: `not do_exit`(앞선 P1 MAE/MFE에서 안 털림) **그리고** `_is_free_runner`(프리러너) **그리고** `_xdyn is not None`(`exit_dynamics` 임포트 성공).
2. **고점 수익률 산출**: `_run_ret = (new_max − ep) / ep × 100`.
   - `new_max = max(직전 max_high, 당일 고가 h)` (`forward/ledger.py:351`) — 진입 후 갱신된 **역대 최고가** 기준.
3. **κ(트레일 계수) 계산**: `_kappa = convex_ratchet_kappa(_run_ret, _ratchet_state)` → 1-3 참조.
4. **트레일 컷오프 가격**: `_trail_px = trail_stop_price(new_max, _kappa) = new_max × (1 − κ)`.
5. **하향 돌파 판정**: **`l ≤ _trail_px`** — 종가(c)가 아니라 **장중 저가 `l`** 이 트레일선을 건드리면 즉시 청산. 즉 "장중에 한 번이라도 닿으면 체결"되는 실전형 슬리피지 모델.
6. **청산가**: `actual_exit_price = _trail_px`(종가가 아닌 트레일 지정가에서 체결로 간주).

### 1-3. κ 적용 방식 — 볼록 래칫 곡선

`exit_dynamics.convex_ratchet_kappa`가 고점 수익률에 따라 트레일 폭을 좁혀갑니다(초반 넓게 → 수익 팽창 시 조임).

```92:112:exit_dynamics.py
def convex_ratchet_kappa(run_ret_pct: float, state: Optional[Dict[str, Any]] = None) -> float:
    st = state or DEFAULT_RATCHET_STATE
    k_max = float(st.get("kappa_max", 0.12))
    k_min = float(st.get("kappa_min", 0.05))
    anchor = max(1.0, float(st.get("anchor_ret", 40.0)))
    p = max(0.1, float(st.get("convexity", 1.0)))

    prog = _clamp(float(run_ret_pct) / anchor, 0.0, 1.0)
    shape = prog ** p  # convex(p>1): 초반 작게 → κ가 천천히 줄어 더 오래 넓다
    kappa = k_max - (k_max - k_min) * shape
    return _clamp(kappa, min(k_min, k_max), max(k_min, k_max))


def trail_stop_price(max_high_price: float, kappa: float) -> float:
    """TrailStop = MaxHigh × (1 - κ)."""
    return float(max_high_price) * (1.0 - _clamp(kappa, 0.0, 0.95))
```

- **진행도** `prog = run_ret / anchor_ret`(기본 anchor 40%)를 0~1로 클램프.
- **곡선** `shape = prog ** convexity`. convexity>1이면 초반 진행도에서 shape가 작게 유지돼 κ가 천천히 줄어듦 → **초반엔 넓은 트레일(숨통)**, 고점 40%에 근접할수록 `kappa_min`으로 수렴 → **이익 보호 조임**.
- 기본값(`DEFAULT_RATCHET_STATE`, `exit_dynamics.py:74`): `kappa_max=0.12`(러너 직후 −12% 트레일), `kappa_min=0.05`(고수익 후 −5%), `anchor_ret=40`, `convexity=1.0(linear)`.
- 예) 러너 직후 `run_ret≈10%`, convexity=1 → `prog=0.25`, `κ=0.12−0.07×0.25≈0.1025` → `trail_px≈new_max×0.8975`. `run_ret≈40%` → `κ=kappa_min=0.05` → `trail_px≈new_max×0.95`.

> **요약**: P1b는 "이미 1차 익절한 러너"의 잔여 물량을 *역대 최고가 × (1−κ)* 로 추적하다가, **장중 저가가 그 선을 깨면(`l ≤ trail_px`)** `RUNNER_TRAIL`로 청산합니다. κ는 고점 수익률이 커질수록 작아져(트레일이 타이트해져) 이익을 잠급니다.

---

## 2. 거래량 데이터(`up_vol_sum` / `down_vol_sum` / `v_energy`) 누적 방식

### 2-1. `up_vol_sum` / `down_vol_sum` — 봉 색깔 기준 전량 가산

매 추적 사이클마다 당일 거래량 `v`를 **양봉/음봉 여부에 따라 통째로** 한쪽 버킷에 더합니다.

```357:358:forward/ledger.py
            new_up_vol = row_scalar(r, 'up_vol_sum', 0.0) + (v if c > o else 0)
            new_down_vol = row_scalar(r, 'down_vol_sum', 0.0) + (v if c < o else 0)
```

- `c > o`(종가>시가, **양봉**) → 당일 거래량 `v` 전부를 `up_vol_sum`에 가산.
- `c < o`(종가<시가, **음봉**) → 당일 거래량 `v` 전부를 `down_vol_sum`에 가산.
- `c == o`(도지) → **어느 쪽에도 더하지 않음**.
- **등락률에 비례하지 않습니다.** 등락 폭이 0.1%든 9%든, 양봉이면 거래량 전량이 up으로 들어갑니다(이진 분류 + 전량 가산).
- `v`는 그날 마지막 캔들 거래량(`ohlcv_last_floats(df)`, `forward/ledger.py:324`).

누적값은 청산/유지 양쪽 UPDATE에서 DB에 영속화됩니다.

```711:717:forward/ledger.py
            else:
                # DB 업데이트 (유지)
                conn.execute('''
                    UPDATE forward_trades 
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?
                    WHERE id=?
                ''', (new_max, new_min, new_bars, new_up_vol, new_down_vol, r['id']))
```

청산 시에는 `up_vol_sum`/`down_vol_sum`이 **사후 분석 태그**로만 쓰입니다(청산 여부엔 무관).

```627:629:forward/ledger.py
                vol_ratio = new_up_vol / (new_down_vol + 1)
                if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")
```

### 2-2. `v_energy` — 누적되지 않음 (진입 시점 스냅샷 값)

`v_energy`는 `track_daily_positions` 루프 안에서 **재계산·누적되지 않습니다.** 항상 원장 행(진입 시 기록된 수급 에너지)에서 읽어옵니다.

- 오버드라이브 판정: `is_overdrive_on = row_scalar(r, 'v_energy', 0.0) >= od_hurdle`(`forward/ledger.py:455`), True면 `dyn_mfe_tp ×= 1.10`(`:457`).
- 엣지 스코어: `holding_edge_score = (current_ret_pct / max(1, bars)) × (v_energy/10)`(`forward/ledger.py:571-573`) — 타임스탑 +2일 연장(`>1.5`), 피라미딩 트리거(`_edge_now`, `:558-560`)에 사용.

> **요약**: `up/down_vol_sum`은 "양봉이면 +v, 음봉이면 +v"의 **봉 색깔 기준 전량 누적**(등락률 무관, 도지 제외). `v_energy`는 누적 변수가 아니라 진입 스냅샷을 읽어 TP 보너스·엣지스코어에 곱해지는 상수입니다.

---

## 3. 청산 사다리 내 '거래량 필터' 유무 진단 → **없음**

P1~P3 전 구간을 추적한 결과, **거래량을 조건으로 청산을 유예(defer)하거나 확정(confirm)하는 게이트는 존재하지 않습니다.** 모든 `do_exit` 분기의 판정 변수는 다음과 같습니다.

| 게이트 | 판정 변수 | 거래량 사용? |
|---|---|---|
| P1 MAE (`STAT_MAE`) | `low_ret_pct ≤ dyn_mae_sl` | ❌ 가격 |
| P1 MFE 분할 | `high_ret_pct ≥ dyn_mfe_tp` | ❌ 가격(단, `v_energy`로 TP 1.10배·`f_out` 보정) |
| P1b 트레일 (`RUNNER_TRAIL`) | `l ≤ new_max×(1−κ)` | ❌ 가격 |
| P2 타임스탑 (`*_TIME`) | `bars ≥ time_stop_eff` & `ret<3%` | ❌ 시간 |
| P2 ATR 스탑 (`*_ATR`) | `l ≤ sl_price` | ❌ 가격(ATR) |
| P2 기술이탈 (`*_TECH`) | `c<ZLEMA` 또는 EMA10<EMA20 데드크로스 | ❌ 가격/추세 |
| P3 좀비 (`ZOMBIE_FORCE_CLOSE`) | `bars ≥ time_stop×2` | ❌ 시간 |

거래량이 등장하는 지점은 **청산 결정 이후 또는 보조 파라미터**뿐입니다:

1. **사후 태그**: `vol_ratio = up_vol/(down_vol+1)` → `#건전한조정_매집우위`/`#음봉대량거래_세력이탈`/`#세력_엑시트_투매출회` 등 (이미 `do_exit=True`인 블록 내부, `forward/ledger.py:627-663`).
2. **`v_energy` 간접 영향**: MFE TP를 1.10배 키우거나(`:455-457`), 엣지스코어로 **타임스탑을 2일 연장**(`:574-575`)하거나, 피라미딩을 유발(`:556-561`). → 이는 "거래량 기반 청산 유예"에 가장 근접하지만, **엄밀히는 진입 시점 `v_energy` 스냅샷**이며, 효과도 "타임스탑만 +2일"로 1순위 MAE/MFE/트레일을 막지는 못합니다(`forward/ledger.py:565` 주석이 명시: *"1순위 MAE/MFE 불변"*).

> **진단**: "장중 음봉 대량거래(투매)면 손절을 굳힌다" 또는 "양봉 대량거래(매집)면 타임스탑을 유예한다" 같은 **거래량 조건 청산 방어 로직은 부재**. 거래량은 라벨링과 `v_energy`(진입값)를 통한 간접 보정에만 관여합니다. → 고도화 시 P2 타임스탑 직전에 `vol_ratio`/당일 상대거래량을 confirm 게이트로 끼워 넣을 여지가 있습니다.

---

## 4. RL 및 파라미터 의존성 — `trail_px` 계산 연결 고리

`trail_px`는 세 모듈의 협업으로 산출됩니다: **ledger(소비) ← exit_dynamics(수학) ← exit_ratchet_rl(주간 RL 진화)**.

### 4-1. 상태 로드 (사이클당 1회)

`track_daily_positions` 시작부에서 `EXIT_RATCHET_STATE`를 **행별 재로딩 없이 1회만** 로드합니다.

```269:274:forward/ledger.py
    try:
        import exit_dynamics as _xdyn
        _ratchet_state = _xdyn.load_ratchet_state(sys_config)
    except Exception:
        _xdyn = None
        _ratchet_state = None
```

```83:89:exit_dynamics.py
def load_ratchet_state(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = dict(DEFAULT_RATCHET_STATE)
    if isinstance(cfg, dict):
        st = cfg.get(RATCHET_STATE_KEY)
        if isinstance(st, dict):
            base.update({k: st[k] for k in st if k in DEFAULT_RATCHET_STATE})
    return base
```

- `RATCHET_STATE_KEY = "EXIT_RATCHET_STATE"`. config에 키가 없으면 `DEFAULT_RATCHET_STATE`(κ_max 0.12 / κ_min 0.05 / anchor 40 / convexity 1.0)로 폴백.
- `_xdyn` 임포트 실패 시 `_xdyn=None` → P1b 자체가 비활성(가드 `_xdyn is not None`).

### 4-2. 호출 관계 (런타임, 행별)

```
track_daily_positions (forward/ledger.py:543-546)
   └─ _run_ret = (new_max - ep)/ep*100
   └─ _kappa   = exit_dynamics.convex_ratchet_kappa(_run_ret, _ratchet_state)
   └─ _trail_px= exit_dynamics.trail_stop_price(new_max, _kappa)   # = new_max*(1-κ)
   └─ if l <= _trail_px:  → RUNNER_TRAIL, actual_exit_price=_trail_px
```

즉 `trail_px`에 들어가는 가변 입력은 **(a) `new_max`(역대 고가, 가격 데이터)** 와 **(b) `_ratchet_state`(RL 진화 파라미터)** 두 가지입니다. `exit_dynamics`는 순수 함수(무 I/O)라 그 자체는 학습하지 않고, **상태값을 외부에서 주입**받습니다.

### 4-3. 주간 RL 진화 (`exit_ratchet_rl.evolve_ratchet_kappa`)

`_ratchet_state`의 κ 곡선을 매주 실측 PnL로 갱신하는 주체입니다.

- **입력 수집**: 최근 `lookback_days`(기본 7일) CLOSED **러너 트레이드**(`free_runner=1 OR scaled_out_frac>0`)의 `mfe, final_ret, exit_type, bars_held`를 RO로 조회(`exit_ratchet_rl.py:18-35`).
- **지표 계산**(`compute_runner_rates`, `:38-79`):
  - `giveback_rate = mean((mfe − final_ret)/max(mfe,1))` — 고점 대비 이익 반납 평균.
  - `whipsaw_rate = (exit_type=="RUNNER_TRAIL" 이면서 mfe ≤ p40_mfe 인 건수) / n` — 추세 발달 전 조기 절단 비율.
- **그래디언트 업데이트**(`exit_dynamics.update_ratchet_kappa_rl`, `:115-147`):
  - `Δ = eta(0.04) × (whipsaw_rate − giveback_rate)`.
  - 조기청산↑(w>g) → `kappa_max/min ↑`(트레일 확대) + `convexity ↑`(초반 더 넓게, 볼록화).
  - 이익반납↑(g>w) → `kappa ↓`(조임) + `convexity ↓`(선형/오목화).
  - 클램프: `kappa_max∈[0.04,0.30]`, `kappa_min∈[0.02,kappa_max]`, `convexity∈[0.5,3.0]`.
- **표본 가드**: 러너 표본 `n<3`이면 갱신 스킵(`exit_ratchet_rl.py:117-118`).
- **영속화**: `cfg["EXIT_RATCHET_STATE"] = new_state` 후 `update_system_config({...})`(`:126-133`)로 SQLite config_kv에 저장 → 다음 `track_daily_positions`가 `load_ratchet_state`로 읽어 `trail_px`에 반영.

### 4-4. RL 호출 스케줄 (어디서 매주 도는가)

- `system_auto_pilot.py:1921-1923` — 주간 마스터 사이클(둠스데이-γ 진화 직후, 주간 플로우 리포트 직전).
- `factory_pipelines.py:994-996` — 팩토리 파이프라인 경로.

```1919:1926:system_auto_pilot.py
    # [M2] 프리러너 볼록 래칫 κ 곡선의 주간 RL 자율 갱신(조기청산·이익반납 학습).
    try:
        from exit_ratchet_rl import evolve_ratchet_kappa

        _kv = evolve_ratchet_kappa()
        print(f"🪝 [Ratchet-κ] {_kv.get('rates')} → {_kv.get('state')}")
    except Exception as _kv_ex:
        print(f"⚠️ [Ratchet-κ] skip: {_kv_ex}")
```

### 4-5. 의존성 다이어그램

```
[주간] system_auto_pilot / factory_pipelines
        └─ exit_ratchet_rl.evolve_ratchet_kappa()
              ├─ _read_runner_trades(forward_trades RO)  → mfe/final_ret/exit_type
              ├─ compute_runner_rates → whipsaw_rate, giveback_rate
              ├─ exit_dynamics.update_ratchet_kappa_rl(Δ=0.04·(w−g))
              └─ update_system_config({EXIT_RATCHET_STATE: new_state})   ←─┐
                                                                          │ 영속
[매일] track_daily_positions                                              │
        ├─ _ratchet_state = exit_dynamics.load_ratchet_state(sys_config) ─┘
        └─ (러너 행) convex_ratchet_kappa → trail_stop_price → l≤trail_px? → RUNNER_TRAIL
```

> **요약**: `exit_dynamics.py`는 `trail_px = new_max×(1−κ)`를 계산하는 **순수 수학 계층**이고, κ 곡선 파라미터(`EXIT_RATCHET_STATE`)는 `exit_ratchet_rl.py`가 **주간 RL(조기청산 vs 이익반납 그래디언트)** 로 진화시켜 config에 저장합니다. ledger는 매 사이클 이 상태를 1회 로드해 러너 행에 적용합니다. 즉 트레일 컷오프는 "RL이 학습한 폭(κ)"과 "실시간 최고가(new_max)"의 곱으로 결정됩니다.

---

## 5. 고도화 착수 전 체크포인트 (관찰 기반, 변경 없음)

1. **P1b는 러너 전용** — 거래량 기반 트레일을 넣고 싶다면 `free_runner` 게이트 밖(일반 포지션)까지 확장할지 먼저 결정 필요.
2. **거래량 confirm 게이트 부재** — P2 타임스탑/ATR 직전에 `vol_ratio` 또는 당일 상대거래량으로 "유예 1봉" 같은 로직을 끼울 자연스러운 자리는 `forward/ledger.py:584`(2순위 진입 직전).
3. **`v_energy`는 진입 스냅샷** — "장중 실시간 거래량 폭발"을 쓰려면 루프 안에서 당일 거래량/평균거래량 비율을 새로 계산해야 함(현재는 미계산).
4. **RL 표본 의존** — 러너 청산이 주 3건 미만이면 κ가 갱신되지 않아 기본값에 고정됨(`n<3` 스킵). 거래량 필터를 추가하면 러너 표본 분포가 바뀌어 κ RL에도 2차 영향.

---

### 인용 파일 인덱스
- `forward/ledger.py`: `track_daily_positions`(189), 거래량 누적(357-358), P1 MFE 분할(518-540), **P1b RUNNER_TRAIL(542-553)**, 엣지/타임스탑(565-575), 사후 태그(627-663), 유지 UPDATE(711-717), 래칫 상태 로드(269-274).
- `exit_dynamics.py`: `convex_ratchet_kappa`(92-107), `trail_stop_price`(110-112), `load_ratchet_state`(83-89), `update_ratchet_kappa_rl`(115-147), `DEFAULT_RATCHET_STATE`(74-80).
- `exit_ratchet_rl.py`: `_read_runner_trades`(18-35), `compute_runner_rates`(38-79), `evolve_ratchet_kappa`(82-135).
- 스케줄: `system_auto_pilot.py`(1919-1926), `factory_pipelines.py`(994-996).

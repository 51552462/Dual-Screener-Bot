"""Regime Time-Space Dual Memory — 블랙스완 예약, 능동형 스로틀, 심층탐색, 크로스 진화."""
import regime_memory as rm


# ---------------------------------------------------------------------------
# Mission 4: 블랙스완 감지
# ---------------------------------------------------------------------------
def test_is_black_swan_low_score_complete():
    res = {"score": 0.40, "data_completeness": 1.0}
    assert rm.is_black_swan(res, cfg={}) is True


def test_is_black_swan_high_score_false():
    res = {"score": 0.90, "data_completeness": 1.0}
    assert rm.is_black_swan(res, cfg={}) is False


def test_is_black_swan_incomplete_data_false():
    res = {"score": 0.10, "data_completeness": 0.4}
    assert rm.is_black_swan(res, cfg={}) is False


# ---------------------------------------------------------------------------
# Mission 6: 코인 스캔 감지 + 능동형 스로틀
# ---------------------------------------------------------------------------
def test_coin_scan_active_detects_bitget(tmp_path):
    orch = rm._orch()
    assert orch is not None
    db = str(tmp_path / "main_q.sqlite")
    orch.enqueue("BITGET", "scan", db_path=db)
    assert rm.coin_scan_active(db_path=db) is True


def test_coin_scan_inactive_when_empty(tmp_path):
    orch = rm._orch()
    db = str(tmp_path / "empty_q.sqlite")
    orch.init_queue(db_path=db)
    assert rm.coin_scan_active(db_path=db) is False


def test_active_throttle_yields_until_max_wait(tmp_path):
    orch = rm._orch()
    db = str(tmp_path / "busy_q.sqlite")
    orch.enqueue("BITGET", "scan", db_path=db)  # 코인 상시 활성 → 절대 안 풀림
    ticks = []
    waited = rm.active_throttle_yield(
        max_wait_sec=6.0, poll_sec=2.0, db_path=db, sleeper=lambda s: ticks.append(s)
    )
    assert waited >= 6.0
    assert ticks  # sleep(양보) 가 실제로 호출됨


def test_active_throttle_no_wait_when_clear(tmp_path):
    orch = rm._orch()
    db = str(tmp_path / "clear_q.sqlite")
    orch.init_queue(db_path=db)
    waited = rm.active_throttle_yield(
        max_wait_sec=6.0, poll_sec=2.0, db_path=db, sleeper=lambda s: None
    )
    assert waited == 0.0


# ---------------------------------------------------------------------------
# Mission 4/6: 예약 enqueue → 전용 큐 직렬 처리
# ---------------------------------------------------------------------------
def test_enqueue_and_drain_priority3(tmp_path, monkeypatch):
    db = str(tmp_path / "regime_q.sqlite")
    tid = rm.enqueue_deep_analysis({"market": "KR", "current_vector": [0.0] * 6}, db_path=db)
    assert tid is not None

    seen = []
    monkeypatch.setattr(rm, "_deep_executor", lambda task: seen.append(task))
    out = rm.run_deep_archive_worker(db_path=db)
    assert out["processed"] == 1
    assert len(seen) == 1
    assert seen[0].mode == rm.DEEP_ARCHIVE_MODE
    assert seen[0].payload.get("reason") == "black_swan"


def test_enqueue_dedupe(tmp_path):
    db = str(tmp_path / "dedupe_q.sqlite")
    t1 = rm.enqueue_deep_analysis({"market": "US"}, db_path=db)
    t2 = rm.enqueue_deep_analysis({"market": "US"}, db_path=db)
    assert t1 is not None
    assert t2 is None  # 동일 (engine,mode) PENDING 중복 차단


# ---------------------------------------------------------------------------
# 장기 기억 DTW 심층 탐색
# ---------------------------------------------------------------------------
def test_deep_archive_dtw_search_structure(monkeypatch):
    monkeypatch.setattr(rm, "_load_cfg", lambda *a, **k: {})
    import regime_analog_engine as rae

    monkeypatch.setattr(
        rae,
        "build_regime_archetype_dna",
        lambda persist=True: {"store": dict(rae.DEFAULT_ARCHETYPE_DNA)},
    )
    payload = {"current_vector": [0.02, 0.02, 0.8, 1.0, 1.1, 0.8]}  # V_RECOVERY 형
    out = rm.deep_archive_dtw_search(payload, throttle=False)
    assert out["best_episode"] in rae.HISTORICAL_EPISODES
    assert set(out["dtw_by_episode"].keys()) == set(rae.HISTORICAL_EPISODES.keys())


# ---------------------------------------------------------------------------
# Mission 5: 크로스 진화 (단기 캐시 + 실전 템플릿 병합)
# ---------------------------------------------------------------------------
def test_merge_appends_parallel_template_without_overwriting_base():
    import deep_evolution_deploy as dep

    cfg = {}
    deep = {
        "best_episode": "V_RECOVERY",
        "best_regime": "UP",
        "front_run_favorable": True,
        "best_dtw_dist": 0.2,
        "archetype_dna": {
            "KR": {"cpv": 0.72, "tb": 13.5, "bbe": 31.0},
            "US": {"cpv": 0.68, "tb": 12.0, "bbe": 27.5},
        },
    }
    out = rm.merge_deep_result_into_working_memory(deep, cfg=cfg, persist=False)
    assert out["applied"] is True
    # 🧬 Mission 3 — Base 템플릿은 손대지 않고(DNA_BASE_TEMPLATES 미생성), 신형만 병렬 추가
    assert "DNA_BASE_TEMPLATES" not in cfg
    kr_name = dep.deployed_template_name("KR", 1)
    assert kr_name in out["deployed"]
    assert cfg[kr_name]["cpv"] == 0.72
    assert out["working_memory_cache"]["episode"] == "V_RECOVERY"


def test_merge_skips_when_unfavorable():
    cfg = {}
    deep = {
        "best_episode": "EXTREME_CRASH",
        "front_run_favorable": False,
        "archetype_dna": None,
    }
    out = rm.merge_deep_result_into_working_memory(deep, cfg=cfg, persist=False)
    assert out["applied"] is False
    assert out["reason"] == "no_favorable_archetype"

"""Mission 7/8 — 자율 배포 + 영향도 추적 + Capital Strangle + RL 면역 + 긴급 재진화."""
import sqlite3

import deep_evolution_deploy as dep
import immune_evolution as ie
import regime_analog_engine as rae
import regime_memory as rm
import template_bandit as tb


def _favorable_deep():
    return {
        "best_episode": "V_RECOVERY",
        "best_regime": "UP",
        "front_run_favorable": True,
        "best_dtw_dist": 0.2,
        "archetype_dna": {
            "KR": {"cpv": 0.72, "tb": 13.5, "bbe": 31.0},
            "US": {"cpv": 0.68, "tb": 12.0, "bbe": 27.5},
        },
    }


# ---------------------------------------------------------------------------
# Mission 7: 자율 배포
# ---------------------------------------------------------------------------
def test_deployed_name_contains_tag():
    assert dep.DEEP_EVOLVED_TAG in dep.deployed_template_name("KR")
    assert dep.deployed_template_name("KR").startswith(dep.DEPLOY_PREFIX)


def test_auto_deploy_writes_live_templates_and_registry():
    cfg = {}
    out = dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    assert out["applied"] is True
    kr_name = dep.deployed_template_name("KR")
    assert kr_name in out["deployed"]
    # supernova_hunter 가 NEW_EVOLUTION_ 접두사로 ideal_templates 에 자동 로드.
    assert cfg[kr_name]["cpv"] == 0.72
    assert cfg[dep.DEPLOY_REGISTRY_KEY][kr_name]["episode"] == "V_RECOVERY"
    # 밴딧에도 자본 추적용으로 등록됨
    assert kr_name in cfg[tb.BANDIT_KEY]


def test_auto_deploy_versions_increment_parallel():
    cfg = {}
    out1 = dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    out2 = dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    assert out1["version"] == 1 and out2["version"] == 2
    # v1, v2 가 병렬로 공존(덮어쓰기 아님)
    assert dep.deployed_template_name("KR", 1) in cfg
    assert dep.deployed_template_name("KR", 2) in cfg


def test_auto_deploy_retires_beyond_cap():
    cfg = {dep.MAX_VERSIONS_KEY: 2}
    for _ in range(3):
        dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    # 상한 2 → v1 은퇴, v2/v3 만 생존
    assert dep.deployed_template_name("KR", 1) not in cfg
    assert dep.deployed_template_name("KR", 2) in cfg
    assert dep.deployed_template_name("KR", 3) in cfg


def test_mab_recognizes_deep_arms_independently():
    import mab_capital_allocator as mab

    # 버전별 마커가 보존되어 독립 Arm 으로 구분됨
    gk1 = mab._parse_group_key(f"[SUPERNOVA] [{dep.version_tag(1)}]_KR")
    gk2 = mab._parse_group_key(f"[SUPERNOVA] [{dep.version_tag(2)}]_KR")
    assert gk1 != gk2
    assert "DEEP_EVOLVED_v1" in gk1 and "DEEP_EVOLVED_v2" in gk2

    # 거래 표본이 없어도 deep 레지스트리가 탐험 Arm 으로 시드됨
    cfg = {}
    dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    arms: dict = {}
    mab._seed_explore_arms(arms, cfg, "KR")
    assert any("DEEP_EVOLVED_v1" in k for k in arms)


def test_track_impact_per_template(tmp_path):
    db = str(tmp_path / "fwd_tpl.sqlite")
    _seed_forward_db(db)
    cfg = {}
    dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    out = dep.track_deep_evolution_impact(db_path=db, cfg=cfg, persist=False)
    assert "by_template" in out
    # 레지스트리의 각 배포 템플릿이 키로 존재
    assert dep.deployed_template_name("KR", 1) in out["by_template"]


def test_auto_deploy_skips_unfavorable():
    cfg = {}
    out = dep.auto_deploy({"front_run_favorable": False}, cfg, persist=False)
    assert out["applied"] is False


def test_ensure_deep_tag_forces_tag():
    cfg = {}
    dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    # 마커 없는 이름이 레지스트리에 있을 때, 참조 sig 에 해당 버전 태그를 강제 부착
    cfg[dep.DEPLOY_REGISTRY_KEY]["LEGACY_ARM_KR"] = {"market": "KR", "tag": dep.version_tag(1)}
    out = dep.ensure_deep_tag("[SUPERNOVA] LEGACY_ARM_KR", cfg)
    assert dep.DEEP_EVOLVED_MARKER in out


# ---------------------------------------------------------------------------
# Mission 7: 영향도 추적 (Impact Attribution)
# ---------------------------------------------------------------------------
def _seed_forward_db(path):
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE forward_trades (id INTEGER PRIMARY KEY, market TEXT, sig_type TEXT, "
        "status TEXT, final_ret REAL, mfe REAL)"
    )
    tag = dep.DEEP_EVOLVED_TAG
    rows = [
        ("KR", f"[SUPERNOVA] [{tag}]_KR", "CLOSED_WIN", 8.0, 12.0),
        ("KR", f"[SUPERNOVA] [{tag}]_KR", "CLOSED_LOSS", -4.0, 1.0),
        ("KR", "[SUPERNOVA] PLAIN", "CLOSED_WIN", 5.0, 6.0),  # 태그 없음 → 제외
        ("US", f"[SUPERNOVA] [{tag}]_US", "CLOSED_WIN", 10.0, 15.0),
    ]
    conn.executemany(
        "INSERT INTO forward_trades (market,sig_type,status,final_ret,mfe) VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_track_impact_isolates_tagged_trades(tmp_path):
    db = str(tmp_path / "fwd.sqlite")
    _seed_forward_db(db)
    out = dep.track_deep_evolution_impact(db_path=db, persist=False)
    assert out["total"]["n"] == 3  # 태그 3건만(PLAIN 제외)
    assert out["total"]["wins"] == 2
    assert out["by_market"]["KR"]["n"] == 2
    assert out["by_market"]["US"]["n"] == 1
    assert out["by_market"]["US"]["max_mfe"] == 15.0


# ---------------------------------------------------------------------------
# Mission 8: Capital Strangle
# ---------------------------------------------------------------------------
def test_enforce_strangle_locks_failing_template():
    cfg = {tb.BANDIT_KEY: {"T1": {"alpha": 2, "beta": 8, "mult": 1.5, "n": 10}}}
    rec = tb.enforce_capital_strangle(cfg, "T1", n=10, wins=3, profit_factor=0.7)
    assert rec is not None
    assert rec["mult"] == tb.MULT_MIN
    assert rec["strangled"] is True


def test_enforce_strangle_spares_winner():
    cfg = {tb.BANDIT_KEY: {"T2": {"alpha": 8, "beta": 2, "mult": 1.5, "n": 10}}}
    rec = tb.enforce_capital_strangle(cfg, "T2", n=10, wins=7, profit_factor=2.5)
    assert rec is None


def test_enforce_strangle_needs_min_samples():
    cfg = {tb.BANDIT_KEY: {}}
    assert tb.enforce_capital_strangle(cfg, "T3", n=3, wins=0, profit_factor=0.0) is None


# ---------------------------------------------------------------------------
# Mission 8: RL 면역 (페널티 + 오답노트 bbox)
# ---------------------------------------------------------------------------
def test_penalize_episode_decays_weight():
    cfg = {}
    out = rae.penalize_episode(cfg, "EXTREME_CRASH", persist=False)
    assert out["q_after"] < 0.0
    assert out["weight"] < 1.0
    # 누적 처벌 → 가중치 더 감소
    out2 = rae.penalize_episode(cfg, "EXTREME_CRASH", persist=False)
    assert out2["weight"] < out["weight"]


def test_anti_pattern_bbox_blocks_failed_vector():
    import toxic_antipattern_core as tac

    bbox = rae.build_anti_pattern_bbox([0.6, 8.0, 20.0], "FAILED", market="KR")
    assert bbox["dyn_cpv_min"] < 0.6 < bbox["dyn_cpv_max"]
    # 실패 벡터 중심값은 차단(매칭)
    assert tac.evaluate_toxic_bbox_match(bbox, 0.6, 8.0, 20.0, float("nan"), "ANY") is True
    # 동떨어진 벡터는 통과(미매칭)
    assert tac.evaluate_toxic_bbox_match(bbox, 0.9, 20.0, 50.0, float("nan"), "ANY") is False


# ---------------------------------------------------------------------------
# Mission 8: 통합 자가 교정 드라이버
# ---------------------------------------------------------------------------
def test_run_immune_self_correction_full(monkeypatch):
    cfg = {}
    dep.auto_deploy(_favorable_deep(), cfg, persist=False)
    kr_name = dep.deployed_template_name("KR")

    # 실전에서 KR 진화템플릿이 망함(표본 6, 승률 33%, PF 0.5)
    fake_impact = {
        "by_market": {
            "KR": {"n": 6, "wins": 2, "wr": 0.333, "avg_mfe": 1.0, "max_mfe": 3.0,
                   "cum_pnl": -12.0, "profit_factor": 0.5},
            "US": {"n": 1, "wins": 1, "wr": 1.0, "avg_mfe": 9.0, "max_mfe": 9.0,
                   "cum_pnl": 9.0, "profit_factor": 5.0},
        },
        "total": {"n": 7, "wins": 3, "wr": 0.43, "avg_mfe": 2.0, "max_mfe": 9.0,
                  "cum_pnl": -3.0, "profit_factor": 0.9},
    }
    monkeypatch.setattr(dep, "track_deep_evolution_impact", lambda **k: fake_impact)
    enq = {}
    monkeypatch.setattr(
        rm, "enqueue_emergency_remorph", lambda **k: enq.update(k) or 777
    )

    out = ie.run_immune_self_correction(cfg, persist=False)
    actions = out["actions"]
    assert len(actions) == 1
    act = actions[0]
    assert act["template"] == kr_name
    # 1) 즉각 처벌
    assert cfg[tb.BANDIT_KEY][kr_name]["mult"] == tb.MULT_MIN
    # 2) 면역: anti-pattern + RL penalty
    assert act.get("anti_pattern_key") in cfg["ANTI_PATTERNS"]
    assert cfg[rae.EPISODE_PENALTY_KEY]["V_RECOVERY"] < 0.0
    # 3) 재진화: Plan B 긴급 재탐색 (실패 국면 제외)
    assert act["emergency_task_id"] == 777
    assert enq["failed_episode"] == "V_RECOVERY"


# ---------------------------------------------------------------------------
# 긴급 재진화 큐 + Plan B 제외 탐색
# ---------------------------------------------------------------------------
def test_emergency_remorph_enqueue_no_dedupe(tmp_path):
    db = str(tmp_path / "emg_q.sqlite")
    t1 = rm.enqueue_emergency_remorph(failed_episode="V_RECOVERY", market="KR", db_path=db)
    t2 = rm.enqueue_emergency_remorph(failed_episode="V_RECOVERY", market="KR", db_path=db)
    assert t1 is not None and t2 is not None and t1 != t2  # 긴급은 dedupe 안 함


def test_deep_search_excludes_failed_episode(monkeypatch):
    monkeypatch.setattr(rm, "_load_cfg", lambda *a, **k: {})
    monkeypatch.setattr(
        rae, "build_regime_archetype_dna",
        lambda persist=True: {"store": dict(rae.DEFAULT_ARCHETYPE_DNA)},
    )
    payload = {"current_vector": [0.0] * 6, "exclude_episodes": ["EXTREME_CRASH"]}
    out = rm.deep_archive_dtw_search(payload, throttle=False)
    assert "EXTREME_CRASH" not in out["dtw_by_episode"]
    assert out["best_episode"] != "EXTREME_CRASH"
    assert "EXTREME_CRASH" in out["excluded_episodes"]

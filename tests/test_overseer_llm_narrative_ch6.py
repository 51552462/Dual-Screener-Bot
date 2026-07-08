"""Ch.6 — Overseer LLM 해석 문구 정합성 테스트."""
from __future__ import annotations

import unittest
from types import SimpleNamespace

from overseer_audit_binder import AuditAnomaly
from overseer_llm_narrative import (
    build_canonical_narrative_facts,
    build_deterministic_narrative,
    build_llm_narrative_user_prompt,
    build_overseer_llm_system_prompt,
    process_overseer_llm_narrative,
    sanitize_overseer_narrative_html,
    validate_llm_narrative,
)


def _dossier(**kw) -> SimpleNamespace:
    base = dict(
        as_of_kst="2026-07-08",
        meta_regime_key="BEAR",
        config_regime_key="BEAR",
        meta_treasury_mode="NORMAL",
        meta_global_kelly_mult=1.0,
        meta_governor_last_run_at="2026-07-08T10:00:00+00:00",
        meta_governor_last_run_status="OK",
        vix_summary="VIX p75",
        trades_closed_today=13,
        trades_entry_today=5,
        trades_open=2,
        win_rate_today_pct=0.0,
        overdrive_hurdle=10.0,
        overdrive_eligible_today=0,
        overdrive_logged_today=0,
        overdrive_loss_target_today=13,
        overdrive_v_energy_max_today=8.5,
        overdrive_supernova_closed_today=10,
        overdrive_all_loss_sl_day=True,
        toxic_tag_entry_hits_today=0,
        toxic_tag_exit_echo_hits_today=19,
        regime_mismatch_entry_hits_today=3,
        regime_mismatch_closed_hits_today=5,
        catastrophic_clutch_active=True,
        catastrophic_clutch_mult=0.15,
        effective_kelly_risk=0.001,
        effective_kelly_pre_overlay=0.01,
        kelly_day_clutch_mult=0.15,
        kelly_nav_dd_mult=0.85,
        kelly_elasticity_mult=0.1275,
        nav_drawdown_pct=2.65,
        kill_switch_active=False,
        treasury_zeroed_groups=0,
        treasury_actionable_groups=8,
        governor_is_stale=False,
        governor_stale_hours=2.0,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _anom(code: str, severity: str = "CRITICAL", headline: str = "") -> AuditAnomaly:
    return AuditAnomaly(
        code=code,
        severity=severity,
        headline=headline or code,
        evidence="test",
    )


class TestCanonicalFacts(unittest.TestCase):
    def test_7_8_scenario_fields(self):
        d = _dossier()
        anoms = [_anom("CATASTROPHIC_LOSS_DAY", headline="당일 승률 붕괴")]
        facts = build_canonical_narrative_facts(d, anoms)
        self.assertEqual(facts["meta_regime_key"], "BEAR")
        self.assertIn("overdrive_idle_expected", facts["overdrive"]["note"])
        self.assertIn("exit_echo", facts["toxic_tags"])
        self.assertEqual(facts["anomalies"]["top_code"], "CATASTROPHIC_LOSS_DAY")


class TestValidateNarrative(unittest.TestCase):
    def test_rejects_praise_with_critical(self):
        d = _dossier()
        anoms = [_anom("CATASTROPHIC_LOSS_DAY")]
        bad = "오늘도 탁월한 방어 상태로 Kelly가 잘 유지되었습니다."
        v = validate_llm_narrative(bad, d, anoms)
        self.assertIn("forbidden_praise_with_critical", v)

    def test_accepts_aligned_narrative(self):
        d = _dossier()
        anoms = [_anom("CATASTROPHIC_LOSS_DAY", headline="당일 승률 붕괴")]
        good = (
            "CATASTROPHIC_LOSS_DAY: 청산 13건 승률 0% — META_REGIME=BEAR, "
            "Kelly 탄력성 ×0.128 적용. 내일 regime_tag 격리 점검."
        )
        v = validate_llm_narrative(good, d, anoms)
        self.assertEqual(v, [])

    def test_false_overdrive_claim(self):
        d = _dossier()
        anoms = []
        bad = "오버드라이브가 발동하여 익절 가속이 작동했습니다."
        v = validate_llm_narrative(bad, d, anoms)
        self.assertIn("false_overdrive_trigger_claim", v)

    def test_treasury_mode_contradiction(self):
        d = _dossier(meta_treasury_mode="NORMAL")
        anoms = []
        bad = "Treasury 모드는 DEFENSE로 정상 방어 중입니다."
        v = validate_llm_narrative(bad, d, anoms)
        self.assertTrue(any(x.startswith("treasury_mode_contradiction") for x in v))


class TestDeterministicFallback(unittest.TestCase):
    def test_includes_top_anomaly(self):
        d = _dossier()
        anoms = [
            _anom("TOXIC_TAG_LEAK", headline="독성 태그 진입 누출"),
            _anom("CATASTROPHIC_LOSS_DAY", headline="당일 승률 붕괴"),
        ]
        text = build_deterministic_narrative(d, anoms)
        self.assertIn("CATASTROPHIC_LOSS_DAY", text)
        self.assertIn("승률 붕괴", text)

    def test_zero_anomaly_neutral(self):
        d = _dossier(trades_entry_today=0, trades_closed_today=0)
        text = build_deterministic_narrative(d, [])
        self.assertIn("META_REGIME", text)


class TestProcessPipeline(unittest.TestCase):
    def test_bad_llm_replaced(self):
        d = _dossier()
        anoms = [_anom("CATASTROPHIC_LOSS_DAY")]
        bad_llm = "완벽히 동기화된 훌륭한 방어 상태입니다."
        res = process_overseer_llm_narrative(d, anoms, bad_llm)
        self.assertEqual(res.source, "deterministic")
        self.assertFalse(res.valid)
        self.assertIn("CATASTROPHIC_LOSS_DAY", res.text)

    def test_good_llm_kept(self):
        d = _dossier()
        anoms = [_anom("CATASTROPHIC_LOSS_DAY", headline="당일 승률 붕괴")]
        good = "CATASTROPHIC_LOSS_DAY 당일 승률 붕괴 — 청산 13건, Kelly ×0.128."
        res = process_overseer_llm_narrative(d, anoms, good)
        self.assertEqual(res.source, "llm")
        self.assertTrue(res.valid)


class TestPromptBuilder(unittest.TestCase):
    def test_user_prompt_has_canonical_block(self):
        d = _dossier()
        anoms = [_anom("KELLY_INELASTIC", "WARN")]
        p = build_llm_narrative_user_prompt(d, anoms, dossier_json={"x": 1})
        self.assertIn("[CANONICAL_NARRATIVE_FACTS]", p)
        self.assertIn("[ANOMALIES_JSON]", p)
        self.assertIn("KELLY_INELASTIC", p)

    def test_system_prompt_has_anomaly_guide(self):
        sp = build_overseer_llm_system_prompt()
        self.assertIn("TOXIC_TAG_LEAK", sp)
        self.assertIn("CANONICAL_NARRATIVE_FACTS", sp)


class TestSanitize(unittest.TestCase):
    def test_strips_markdown_headers(self):
        raw = "## 헤더\n**강조** 내용"
        out = sanitize_overseer_narrative_html(raw)
        self.assertNotIn("##", out)
        self.assertIn("<b>강조</b>", out)


if __name__ == "__main__":
    unittest.main()

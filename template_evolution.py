"""
Transcendent Template Evolution — 초신성 템플릿의 완전 유동화 + 적응형 승격.

Mission 1 (유전적 모핑): 고정 RANK 템플릿을 system_config 로 옮겨, 실전 승자 DNA 쪽으로
    상시 EMA 미세조정한다.  T_{t+1} = (1-α)·T_t + α·D^real_t   (α=0.2)
Mission 2 (적응형 승격 게이트): 섀도우 포렌식(LIMIT_UP_DNA/forensics_pioneer)의 성과가
    실전 메인 템플릿 14일 롤링 평균 승률을 '통계적으로(윌슨 하한)' 뛰어넘을 때만,
    실전 승자 DNA 센트로이드를 [🚀차세대_포렌식_황금타점] 으로 복사 승격 + 밴딧 초기화.

DNA 3축: cpv(=dyn_cpv), tb(=dyn_tb), bbe(=v_energy).
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

BASE_TEMPLATES_KEY = "DNA_BASE_TEMPLATES"
GRADUATED_TEMPLATE_NAME = "🚀차세대_포렌식_황금타점"
MORPH_ALPHA = 0.2
MORPH_MIN_N = 3
LIVE_WR_WINDOW_DAYS = 14
GRAD_MIN_SHADOW_N = 8
GRAD_MIN_LIVE_N = 10

DEFAULT_BASE_TEMPLATES: Dict[str, Dict[str, List[float]]] = {
    "KR": {
        "RANK_A_장기매집": [0.75, 11.8, 27.15],
        "RANK_B_중기스윙": [0.75, 10.0, 27.35],
        "RANK_C_단기테마": [0.60, 8.0, 19.70],
        "RANK_D_초단기밈": [0.60, 8.0, 24.45],
    },
    "US": {
        "US_RANK_A_장기매집": [0.70, 10.5, 25.0],
        "US_RANK_B_중기스윙": [0.66, 9.2, 21.5],
        "US_RANK_C_단기테마": [0.60, 8.1, 17.0],
        "US_RANK_D_초단기밈": [0.55, 7.5, 13.5],
        "US_MEME_슈팅": [0.55, 8.8, 12.80],
    },
}


# ---------------------------------------------------------------------------
# 통계 유틸
# ---------------------------------------------------------------------------
def wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    """승률의 윌슨 하한(보수적 추정). n=0이면 0."""
    if n <= 0:
        return 0.0
    p = wins / n
    z2 = z * z
    denom = 1.0 + z2 / n
    centre = p + z2 / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


# ---------------------------------------------------------------------------
# Mission 1: 베이스 템플릿 유동화 + EMA 모핑
# ---------------------------------------------------------------------------
def seed_base_templates(cfg: Dict[str, Any], market: str) -> Dict[str, List[float]]:
    """system_config 에 베이스 템플릿을 시드(없으면 DEFAULT). 신규 기본키도 보강."""
    mk = str(market).upper()
    store = cfg.get(BASE_TEMPLATES_KEY)
    if not isinstance(store, dict):
        store = {}
        cfg[BASE_TEMPLATES_KEY] = store
    region = store.get(mk)
    if not isinstance(region, dict) or not region:
        region = {k: list(v) for k, v in DEFAULT_BASE_TEMPLATES.get(mk, {}).items()}
        store[mk] = region
    for k, v in DEFAULT_BASE_TEMPLATES.get(mk, {}).items():
        if k not in region:
            region[k] = list(v)
    return region


def load_base_templates(cfg: Dict[str, Any], market: str) -> Dict[str, List[float]]:
    """스캐너 소비용 — 현재 (모핑된) 베이스 템플릿 벡터."""
    return seed_base_templates(cfg, market)


def _winner_centroid(df_rows) -> Optional[Tuple[float, float, float, int]]:
    """승리 청산행들의 (cpv, tb, bbe) 평균 + 건수. 컬럼 부재/표본0이면 None."""
    if df_rows is None or len(df_rows) == 0:
        return None
    needed = ("dyn_cpv", "dyn_tb", "v_energy")
    if not all(c in df_rows.columns for c in needed):
        return None
    try:
        import pandas as pd

        cpv = pd.to_numeric(df_rows["dyn_cpv"], errors="coerce").dropna()
        tb = pd.to_numeric(df_rows["dyn_tb"], errors="coerce").dropna()
        bbe = pd.to_numeric(df_rows["v_energy"], errors="coerce").dropna()
        n = min(len(cpv), len(tb), len(bbe))
        if n <= 0:
            return None
        return float(cpv.mean()), float(tb.mean()), float(bbe.mean()), int(n)
    except Exception:
        return None


def _market_winners(df, market: str):
    """해당 시장의 승리(final_ret>0) 청산 SUPERNOVA 행."""
    if df is None or len(df) == 0:
        return None
    try:
        d = df.copy()
        sig = d["sig_type"].astype(str)
        status = d["status"].astype(str)
        m = (
            sig.str.contains("SUPERNOVA", na=False)
            & status.str.contains("CLOSED", na=False)
        )
        import pandas as pd

        ret = pd.to_numeric(d.get("final_ret"), errors="coerce")
        m = m & (ret > 0)
        if "market" in d.columns:
            m = m & (d["market"].astype(str).str.upper() == str(market).upper())
        return d.loc[m]
    except Exception:
        return None


def _morph_toward_archetype(
    cfg: Dict[str, Any],
    base: Dict[str, List[float]],
    market: str,
    target: Dict[str, Any],
    *,
    alpha: float,
) -> List[str]:
    """
    🕰️ [타임머신 모핑] 모든 베이스 템플릿을 '현재 장세와 가장 닮았던 과거 국면의
    전설적 승자 DNA' 쪽으로 EMA 미세조정한다. (최근 며칠 승자가 아니라 과거의 '정답')
    """
    real = target.get("dna") or []
    if len(real) < 3:
        return []
    episode = str(target.get("episode", "?"))
    score_pct = round(float(target.get("score", 0.0)) * 100.0, 0)
    logs: List[str] = []
    for name, vec in list(base.items()):
        new = [
            round((1 - alpha) * float(vec[i]) + alpha * float(real[i]), 4)
            for i in range(3)
        ]
        base[name] = new
        logs.append(
            f"  ↳ [🕰️{episode} {score_pct:.0f}%] {name}: "
            f"cpv {vec[0]:.2f}→{new[0]:.2f} · tb {vec[1]:.1f}→{new[1]:.1f} "
            f"· bbe {vec[2]:.1f}→{new[2]:.1f}"
        )
    cfg.setdefault(BASE_TEMPLATES_KEY, {})[str(market).upper()] = base
    return logs


def morph_templates(
    cfg: Dict[str, Any],
    df,
    market: str,
    *,
    alpha: float = MORPH_ALPHA,
    min_n: int = MORPH_MIN_N,
) -> List[str]:
    """각 베이스 템플릿을 그 템플릿의 실전 승자 DNA 센트로이드로 EMA 미세조정.

    단, 현재 장세가 '선취매 유리 과거 국면' 과 고유사도(REGIME_ANALOG_MORPH_MIN_SCORE)면
    최근 승자 대신 그 과거 국면의 전설적 승자 DNA 쪽으로 타깃을 동적 전환한다(타임머신 모핑).
    """
    base = seed_base_templates(cfg, market)

    # 🕰️ 타임머신 모핑 우선 판정 (미래형 자가진화 폐루프)
    try:
        from regime_analog_engine import resolve_morph_target_dna

        tm_target = resolve_morph_target_dna(cfg, market)
    except Exception:
        tm_target = None
    if tm_target is not None:
        tm_alpha = MORPH_ALPHA
        try:
            tm_alpha = float(cfg.get("REGIME_ANALOG_MORPH_ALPHA", 0.3))
        except (TypeError, ValueError):
            tm_alpha = 0.3
        tm_logs = _morph_toward_archetype(cfg, base, market, tm_target, alpha=tm_alpha)
        if tm_logs:
            return tm_logs

    winners = _market_winners(df, market)
    logs: List[str] = []
    if winners is None or len(winners) == 0:
        return logs
    sig_all = winners["sig_type"].astype(str)
    for name, vec in list(base.items()):
        sub = winners.loc[sig_all.str.contains(name, na=False, regex=False)]
        cen = _winner_centroid(sub)
        if cen is None or cen[3] < min_n:
            continue
        real = [cen[0], cen[1], cen[2]]
        new = [
            round((1 - alpha) * float(vec[i]) + alpha * float(real[i]), 4)
            for i in range(3)
        ]
        base[name] = new
        logs.append(
            f"  ↳ {name}: cpv {vec[0]:.2f}→{new[0]:.2f} · tb {vec[1]:.1f}→{new[1]:.1f} "
            f"· bbe {vec[2]:.1f}→{new[2]:.1f} (실전승자 n={cen[3]})"
        )
    cfg.setdefault(BASE_TEMPLATES_KEY, {})[str(market).upper()] = base
    return logs


# ---------------------------------------------------------------------------
# Mission 2: 적응형 승격 게이트
# ---------------------------------------------------------------------------
def live_rolling_win_rate(
    df,
    market: str,
    *,
    days: int = LIVE_WR_WINDOW_DAYS,
    now: Optional[datetime] = None,
) -> Tuple[float, int]:
    """실전 메인 SUPERNOVA 템플릿의 최근 `days`일 청산 승률(0~1)과 표본수."""
    if df is None or len(df) == 0:
        return 0.0, 0
    try:
        import pandas as pd

        d = df.copy()
        sig = d["sig_type"].astype(str)
        status = d["status"].astype(str)
        m = sig.str.contains("SUPERNOVA", na=False) & status.str.contains("CLOSED", na=False)
        if "market" in d.columns:
            m = m & (d["market"].astype(str).str.upper() == str(market).upper())
        d = d.loc[m].copy()
        if d.empty:
            return 0.0, 0
        now = now or datetime.now()
        cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d")
        exit_col = "exit_date" if "exit_date" in d.columns else "entry_date"
        d["_ed"] = d[exit_col].astype(str).str[:10]
        d = d[d["_ed"] >= cutoff]
        if d.empty:
            return 0.0, 0
        ret = pd.to_numeric(d["final_ret"], errors="coerce").dropna()
        if ret.empty:
            return 0.0, 0
        wins = int((ret > 0).sum())
        n = int(len(ret))
        return (wins / n if n else 0.0), n
    except Exception:
        return 0.0, 0


def maybe_graduate_forensics(
    cfg: Dict[str, Any],
    df,
    market: str,
    *,
    shadow_wins: int,
    shadow_losses: int,
    now: Optional[datetime] = None,
    min_shadow_n: int = GRAD_MIN_SHADOW_N,
    min_live_n: int = GRAD_MIN_LIVE_N,
) -> Dict[str, Any]:
    """
    섀도우 포렌식 승률(윌슨 하한)이 실전 14일 평균 승률을 통계적으로 능가하면 승격.
    승격 시: 실전 승자 DNA 센트로이드를 [🚀차세대_포렌식_황금타점] 으로 복사 + 밴딧 초기화.
    """
    mk = str(market).upper()
    n_sh = int(shadow_wins) + int(shadow_losses)
    live_wr, live_n = live_rolling_win_rate(df, mk, now=now)

    verdict: Dict[str, Any] = {
        "market": mk,
        "graduated": False,
        "shadow_wins": int(shadow_wins),
        "shadow_losses": int(shadow_losses),
        "shadow_n": n_sh,
        "live_wr": round(live_wr, 4),
        "live_n": live_n,
    }
    if n_sh < min_shadow_n or live_n < min_live_n:
        verdict["reason"] = "insufficient_sample"
        return verdict

    sh_lb = wilson_lower_bound(int(shadow_wins), n_sh)
    verdict["shadow_wilson_lb"] = round(sh_lb, 4)
    if sh_lb <= live_wr:
        verdict["reason"] = "no_statistical_edge"
        return verdict

    winners = _market_winners(df, mk)
    cen = _winner_centroid(winners) if winners is not None else None
    if cen is None:
        verdict["reason"] = "no_winner_dna_for_promotion"
        return verdict

    multi_key = f"DNA_SUPERNOVA_{mk}_MULTI"
    multi = cfg.get(multi_key)
    if not isinstance(multi, dict):
        multi = {}
        cfg[multi_key] = multi
    multi[GRADUATED_TEMPLATE_NAME] = {
        "cpv": round(cen[0], 4),
        "tb": round(cen[1], 4),
        "bbe": round(cen[2], 4),
        "graduated_at": (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
        "shadow_wr": round(shadow_wins / n_sh, 4),
        "shadow_wilson_lb": round(sh_lb, 4),
        "live_wr_beaten": round(live_wr, 4),
        "winner_dna_n": cen[3],
    }
    cfg[multi_key] = multi

    try:
        from template_bandit import init_bandit

        bandit = init_bandit(
            cfg, GRADUATED_TEMPLATE_NAME, shadow_wins=int(shadow_wins), shadow_losses=int(shadow_losses)
        )
        verdict["bandit_init"] = {"alpha": bandit["alpha"], "beta": bandit["beta"], "mult": bandit["mult"]}
    except Exception as ex:
        verdict["bandit_error"] = str(ex)

    verdict["graduated"] = True
    verdict["reason"] = "shadow_beats_live"
    verdict["promoted_dna"] = {"cpv": round(cen[0], 4), "tb": round(cen[1], 4), "bbe": round(cen[2], 4)}
    return verdict


# ---------------------------------------------------------------------------
# 섀도우 포렌식 성과 평가 (forensics_pioneer virtual_trade_history)
# ---------------------------------------------------------------------------
def evaluate_forensics_shadow(
    market: str,
    *,
    price_fetcher: Optional[Callable[[str, str], Optional[float]]] = None,
    db_path: Optional[str] = None,
    min_hold_days: int = 3,
    max_eval: int = 30,
    now: Optional[datetime] = None,
) -> Tuple[int, int]:
    """
    forensics_pioneer 섀도우 진입의 사후 수익을 가격조회로 채점 → (wins, losses).
    price_fetcher(code, market)->최근가. 미지정 시 shadow_performance_tracker 사용.
    """
    import sqlite3

    mk = str(market).upper()
    now = now or datetime.now()
    if price_fetcher is None:
        try:
            from shadow_performance_tracker import fetch_last_close_stealth as price_fetcher  # type: ignore
        except Exception:
            return 0, 0
    if db_path is None:
        try:
            import shadow_tracking

            db_path = shadow_tracking.DB_PATH
        except Exception:
            return 0, 0

    cutoff = (now - timedelta(days=min_hold_days)).strftime("%Y-%m-%d")
    wins = losses = 0
    try:
        uri = str(db_path).replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=30)
        try:
            rows = conn.execute(
                """
                SELECT code, entry_price FROM virtual_trade_history
                WHERE market=? AND sig_type LIKE '%forensics_pioneer%'
                  AND substr(logged_at,1,10) <= ?
                ORDER BY id DESC LIMIT ?
                """,
                (mk, cutoff, int(max_eval)),
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return 0, 0

    for code, entry_price in rows:
        try:
            ep = float(entry_price)
            if ep <= 0:
                continue
            px = price_fetcher(str(code), mk)
            if px is None:
                continue
            if float(px) > ep:
                wins += 1
            else:
                losses += 1
        except Exception:
            continue
    return wins, losses


# ---------------------------------------------------------------------------
# 통합 엔트리 (system_auto_pilot 주말/일일 자율조율에서 호출)
# ---------------------------------------------------------------------------
def run_transcendent_template_evolution(
    cfg: Dict[str, Any],
    df,
    *,
    markets: Tuple[str, ...] = ("KR", "US"),
    do_graduation: bool = True,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Mission 1(모핑) + Mission 2(승격)를 KR/US에 적용하고 cfg 를 변이시킨다."""
    report_lines: List[str] = ["\n🧬 <b>[초월적 진화: 템플릿 유동화·적응형 승격]</b>"]
    out: Dict[str, Any] = {"morph": {}, "graduation": {}}

    for mk in markets:
        logs = morph_templates(cfg, df, mk)
        out["morph"][mk] = len(logs)
        if logs:
            report_lines.append(f"▪️ <b>{mk} 템플릿 EMA 모핑</b> (실전 승자 DNA 추종)")
            report_lines.extend(logs[:6])

    if do_graduation:
        for mk in markets:
            try:
                sw, sl = evaluate_forensics_shadow(mk, now=now)
                verdict = maybe_graduate_forensics(
                    cfg, df, mk, shadow_wins=sw, shadow_losses=sl, now=now
                )
                out["graduation"][mk] = verdict
                if verdict.get("graduated"):
                    report_lines.append(
                        f"🚀 <b>[{mk} 포렌식 승격]</b> 섀도우 윌슨하한 "
                        f"{verdict.get('shadow_wilson_lb', 0)*100:.0f}% > 실전 "
                        f"{verdict.get('live_wr', 0)*100:.0f}% → 차세대 황금타점 편입 "
                        f"(밴딧 배수 {verdict.get('bandit_init', {}).get('mult', '—')})"
                    )
                elif verdict.get("reason") not in ("insufficient_sample",):
                    report_lines.append(
                        f"▪️ {mk} 포렌식 승격 보류: {verdict.get('reason')} "
                        f"(섀도우 {verdict.get('shadow_n')}건/실전WR {verdict.get('live_wr',0)*100:.0f}%)"
                    )
            except Exception as ex:
                report_lines.append(f"⚠️ {mk} 승격 평가 스킵: {ex}")

    out["report_lines"] = report_lines
    return out

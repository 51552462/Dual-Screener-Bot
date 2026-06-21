"""
Offline R&D Sandbox — 내부 DB·장부·meta_governor 만 사용 (외부 API 금지).

A. Proprietary stress-test: LIVE DNA × MDD 상위 구간 생존율 → DEFCON Weight
B. Toxic-inverted mining: 안티패턴 최원거리 교집합 → Hidden Spillover Theme
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from market_db_paths import MARKET_DATA_DB_PATH
from toxic_antipattern_core import collect_merged_antipattern_rules, toxic_ml_antipatterns_rule_map


def _artifact_dir() -> str:
    root = os.path.dirname(os.path.abspath(__file__))
    d = os.path.join(root, "artifacts", "proprietary_rnd")
    os.makedirs(d, exist_ok=True)
    return d


def _benchmark_table(market: str) -> str:
    return "US_SPY" if str(market).upper() == "US" else "KR_KOSPI_IDX"


def _market_toxic_rules(cfg: Dict[str, Any], market: str) -> Dict[str, Any]:
    merged = collect_merged_antipattern_rules(cfg)
    mk = str(market or "KR").upper()
    if mk == "US":
        us_ml = toxic_ml_antipatterns_rule_map(cfg.get("US_TOXIC_ML_ANTIPATTERNS"))
        if us_ml:
            merged = {**merged, **us_ml}
    return merged


def _bbox_centroid(bounds: Dict[str, Any]) -> Optional[np.ndarray]:
    """dyn_cpv / dyn_tb / v_energy (또는 cpv/tb/bbe) bbox 중심."""
    if not isinstance(bounds, dict):
        return None
    mapping = (
        ("dyn_cpv", "cpv"),
        ("dyn_tb", "tb"),
        ("v_energy", "bbe"),
    )
    vals: List[float] = []
    for primary, alt in mapping:
        raw = bounds.get(primary)
        if raw is None:
            raw = bounds.get(alt)
        if isinstance(raw, dict):
            lo = raw.get("min", raw.get("lo"))
            hi = raw.get("max", raw.get("hi"))
            try:
                lo_f = float(lo) if lo is not None else None
                hi_f = float(hi) if hi is not None else None
            except (TypeError, ValueError):
                lo_f = hi_f = None
            if lo_f is not None and hi_f is not None:
                vals.append((lo_f + hi_f) / 2.0)
            elif lo_f is not None:
                vals.append(lo_f)
            elif hi_f is not None:
                vals.append(hi_f)
        else:
            try:
                vals.append(float(raw))
            except (TypeError, ValueError):
                pass
    if len(vals) < 3:
        return None
    return np.array(vals[:3], dtype=float)


def _cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na < 1e-9 or nb < 1e-9:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def _clone_live_dna_templates(
    market: str,
    cfg: Dict[str, Any],
    meta: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """meta LIVE 등급 + health 상위 그룹 DNA 복제."""
    mk = str(market or "KR").upper()
    live_ids: Set[str] = set()
    for sid in meta.get("META_LIVE_STRATEGY_IDS") or []:
        if sid:
            live_ids.add(str(sid))
    for row in meta.get("META_STRATEGY_REGISTRY") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("state") or "").upper() == "LIVE" and row.get("strategy_id"):
            live_ids.add(str(row["strategy_id"]))

    health = meta.get("META_STRATEGY_HEALTH") or {}
    top_groups: Set[str] = set()
    for key, hv in health.items():
        if key == "__meta__" or not isinstance(hv, dict):
            continue
        try:
            mult = float(hv.get("mult", 1.0) or 1.0)
        except (TypeError, ValueError):
            mult = 1.0
        if mult >= 0.35:
            _, _, gk = str(key).rpartition("|")
            top_groups.add(gk or str(key))

    multi: Dict[str, Any] = {}
    multi.update(cfg.get(f"DNA_SUPERNOVA_{mk}_MULTI") or {})
    multi.update(cfg.get("LIVE_CLUSTER_TEMPLATES") or {})

    selected: Dict[str, Dict[str, Any]] = {}
    for name, dna in multi.items():
        if not isinstance(dna, dict):
            continue
        nm = str(name)
        if any(lid and lid in nm for lid in live_ids):
            selected[nm] = dna
            continue
        if any(g and g in nm for g in top_groups if g):
            selected[nm] = dna

    if not selected and multi:
        for name, dna in list(multi.items())[:12]:
            if isinstance(dna, dict):
                selected[str(name)] = dna
    return selected


def _volatile_session_dates(
    conn: sqlite3.Connection,
    market: str,
    *,
    window: int = 20,
    top_pct: float = 0.05,
    lookback_days: int = 504,
) -> Set[str]:
    table = _benchmark_table(market)
    try:
        df = pd.read_sql(
            f'SELECT Date, Close FROM "{table}" ORDER BY Date DESC LIMIT ?',
            conn,
            params=(lookback_days + window + 5,),
        )
    except Exception:
        return set()
    if df is None or len(df) < window + 5:
        return set()
    df = df.sort_values("Date")
    close = pd.to_numeric(df["Close"], errors="coerce").ffill()
    roll_max = close.rolling(window, min_periods=window).max()
    dd = (close - roll_max) / roll_max.replace(0, np.nan)
    n = max(1, int(len(dd.dropna()) * top_pct))
    worst = dd.nsmallest(n)
    dates = df.loc[worst.index, "Date"].astype(str).str[:10]
    return set(dates.tolist())


def _load_market_trades(conn: sqlite3.Connection, market: str) -> pd.DataFrame:
    mk = str(market or "KR").upper()
    try:
        return pd.read_sql(
            """
            SELECT ticker, entry_date, dyn_cpv, dyn_tb, v_energy,
                   COALESCE(ret_pct, pnl_pct, 0) AS ret_pct,
                   sector, sig_type
            FROM forward_trades
            WHERE UPPER(TRIM(COALESCE(market, ''))) = ?
              AND entry_date IS NOT NULL
            ORDER BY entry_date DESC
            LIMIT 800
            """,
            conn,
            params=(mk,),
        )
    except Exception:
        try:
            return pd.read_sql(
                """
                SELECT ticker, entry_date, dyn_cpv, dyn_tb, v_energy, sector, sig_type
                FROM forward_trades
                WHERE UPPER(TRIM(COALESCE(market, ''))) = ?
                  AND entry_date IS NOT NULL
                ORDER BY entry_date DESC
                LIMIT 800
                """,
                conn,
                params=(mk,),
            )
        except Exception:
            return pd.DataFrame()


def run_proprietary_stress_test(
    market: str,
    *,
    cfg: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """LIVE DNA 템플릿 × 내부 벤치마크 MDD 구간 생존율 → DEFCON Weight."""
    mk = str(market or "KR").upper()
    out: Dict[str, Any] = {
        "ok": False,
        "market": mk,
        "templates_tested": 0,
        "volatile_dates": 0,
        "defcon_weights": {},
        "artifact": "",
        "reason": "",
    }
    try:
        from config_manager import load_system_config

        cfg = cfg if isinstance(cfg, dict) else (load_system_config() or {})
    except Exception:
        cfg = cfg or {}
    try:
        from meta_governor import load_meta_governor_state

        meta = meta if isinstance(meta, dict) else load_meta_governor_state()
    except Exception:
        meta = meta or {}

    templates = _clone_live_dna_templates(mk, cfg, meta)
    if not templates:
        out["reason"] = "no_live_dna_templates"
        return out

    path = db_path or MARKET_DATA_DB_PATH
    if not path or not os.path.isfile(path):
        out["reason"] = "no_market_db"
        return out

    try:
        conn = sqlite3.connect(path, timeout=20)
    except sqlite3.Error as ex:
        out["reason"] = f"db_open_failed:{ex}"
        return out

    try:
        volatile = _volatile_session_dates(conn, mk)
        trades = _load_market_trades(conn, mk)
    finally:
        conn.close()

    out["volatile_dates"] = len(volatile)
    if trades is None or trades.empty:
        out["reason"] = "no_forward_trades"
        return out

    trades = trades.copy()
    trades["entry_d"] = trades["entry_date"].astype(str).str[:10]
    if volatile:
        stress_pool = trades[trades["entry_d"].isin(volatile)]
        if stress_pool.empty:
            stress_pool = trades.head(min(120, len(trades)))
    else:
        stress_pool = trades.head(min(120, len(trades)))

    weights: Dict[str, float] = {}
    for name, dna in templates.items():
        try:
            vec = np.array(
                [float(dna.get("cpv", 0)), float(dna.get("tb", 0)), float(dna.get("bbe", 0))],
                dtype=float,
            )
        except (TypeError, ValueError):
            continue
        survives = 0
        total = 0
        for _, row in stress_pool.iterrows():
            try:
                feat = np.array(
                    [
                        float(row.get("dyn_cpv") or 0),
                        float(row.get("dyn_tb") or 0),
                        float(row.get("v_energy") or 0),
                    ],
                    dtype=float,
                )
            except (TypeError, ValueError):
                continue
            if np.linalg.norm(feat) < 1e-6:
                continue
            total += 1
            cos = _cosine_sim(feat, vec)
            try:
                ret = float(row.get("ret_pct") or 0)
            except (TypeError, ValueError):
                ret = 0.0
            if cos >= 0.45 and ret >= -5.0:
                survives += 1
        rate = (survives / total) if total else 0.5
        weights[str(name)] = round(0.35 + 0.65 * rate, 4)

    if not weights:
        out["reason"] = "no_survival_scores"
        return out

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact = os.path.join(_artifact_dir(), f"defcon_weights_{mk}_{stamp}.json")
    payload = {
        "market": mk,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "volatile_window_count": len(volatile),
        "stress_pool_rows": int(len(stress_pool)),
        "defcon_weights": weights,
    }
    with open(artifact, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    cfg_key = f"PROPRIETARY_DEFCON_WEIGHTS_{mk}"
    cfg[cfg_key] = {
        "weights": weights,
        "as_of": payload["generated_at"],
        "artifact": artifact,
    }
    try:
        from config_manager import save_system_config

        save_system_config(cfg)
    except Exception:
        pass

    out.update(
        ok=True,
        templates_tested=len(weights),
        defcon_weights=weights,
        artifact=artifact,
        reason="stress_complete",
    )
    return out


def _dbscan_labels(X: np.ndarray) -> np.ndarray:
    try:
        from sklearn.cluster import DBSCAN
        from sklearn.preprocessing import StandardScaler

        if len(X) < 6:
            return np.zeros(len(X), dtype=int)
        Xs = StandardScaler().fit_transform(X)
        return DBSCAN(eps=0.9, min_samples=3).fit_predict(Xs)
    except Exception:
        return np.zeros(len(X), dtype=int)


def _internal_ohlcv_features(
    conn: sqlite3.Connection,
    market: str,
    *,
    max_tickers: int = 80,
) -> pd.DataFrame:
    """market_data.sqlite 테이블만 스캔 — 외부 API 없음."""
    mk = str(market or "KR").upper()
    prefix = "US_" if mk == "US" else "KR_"
    skip = {
        "US_SPY",
        "US_QQQ",
        "US_VIX",
        "KR_KOSPI_IDX",
        "KR_KOSDAQ_IDX",
    }
    rows: List[Dict[str, Any]] = []
    try:
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
                (f"{prefix}%",),
            ).fetchall()
        ]
    except sqlite3.Error:
        return pd.DataFrame()

    for tbl in tables[: max_tickers * 3]:
        if tbl in skip or len(rows) >= max_tickers:
            break
        try:
            df = pd.read_sql(
                f'SELECT Date, Close, Volume FROM "{tbl}" ORDER BY Date DESC LIMIT 8',
                conn,
            )
        except Exception:
            continue
        if df is None or len(df) < 3:
            continue
        df = df.sort_values("Date")
        c0 = float(pd.to_numeric(df["Close"].iloc[-1], errors="coerce") or 0)
        c1 = float(pd.to_numeric(df["Close"].iloc[-2], errors="coerce") or 0)
        if c0 <= 0 or c1 <= 0:
            continue
        ret = (c0 / c1) - 1.0
        vol = float(pd.to_numeric(df["Volume"].iloc[-1], errors="coerce") or 0)
        vol_z = vol / max(1.0, float(pd.to_numeric(df["Volume"], errors="coerce").mean() or 1))
        sym = tbl.replace(prefix, "", 1)
        rows.append(
            {
                "ticker": sym,
                "ret_1d": ret,
                "vol_z": vol_z,
                "dv_proxy": c0 * vol,
                "source": "ohlcv",
            }
        )
    return pd.DataFrame(rows)


def run_toxic_inverted_mining(
    market: str,
    *,
    cfg: Optional[Dict[str, Any]] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """안티패턴 최원거리 × DBSCAN → Hidden Spillover Theme."""
    mk = str(market or "KR").upper()
    out: Dict[str, Any] = {
        "ok": False,
        "market": mk,
        "theme": {},
        "artifact": "",
        "reason": "",
    }
    try:
        from config_manager import load_system_config

        cfg = cfg if isinstance(cfg, dict) else (load_system_config() or {})
    except Exception:
        cfg = cfg or {}

    toxic_rules = _market_toxic_rules(cfg, mk)
    centroids: List[np.ndarray] = []
    for bounds in toxic_rules.values():
        c = _bbox_centroid(bounds if isinstance(bounds, dict) else {})
        if c is not None:
            centroids.append(c)
    if not centroids:
        out["reason"] = "no_toxic_centroids"
        return out

    path = db_path or MARKET_DATA_DB_PATH
    if not path or not os.path.isfile(path):
        out["reason"] = "no_market_db"
        return out

    candidates: List[Dict[str, Any]] = []
    try:
        conn = sqlite3.connect(path, timeout=20)
        try:
            trades = _load_market_trades(conn, mk)
            if trades is not None and not trades.empty:
                for _, row in trades.head(200).iterrows():
                    try:
                        feat = np.array(
                            [
                                float(row.get("dyn_cpv") or 0),
                                float(row.get("dyn_tb") or 0),
                                float(row.get("v_energy") or 0),
                            ],
                            dtype=float,
                        )
                    except (TypeError, ValueError):
                        continue
                    if np.linalg.norm(feat) < 1e-6:
                        continue
                    candidates.append(
                        {
                            "ticker": str(row.get("ticker") or ""),
                            "sector": str(row.get("sector") or ""),
                            "feat": feat,
                            "source": "forward_trades",
                        }
                    )
            ohlcv_df = _internal_ohlcv_features(conn, mk)
            for _, row in ohlcv_df.iterrows():
                feat = np.array(
                    [
                        float(row.get("ret_1d") or 0) * 10.0,
                        float(row.get("vol_z") or 0),
                        np.log1p(max(0.0, float(row.get("dv_proxy") or 0))) / 20.0,
                    ],
                    dtype=float,
                )
                candidates.append(
                    {
                        "ticker": str(row.get("ticker") or ""),
                        "sector": "",
                        "feat": feat,
                        "source": "ohlcv",
                    }
                )
        finally:
            conn.close()
    except sqlite3.Error as ex:
        out["reason"] = f"db_error:{ex}"
        return out

    if len(candidates) < 8:
        out["reason"] = "insufficient_candidates"
        return out

    toxic_stack = np.stack(centroids)
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for cand in candidates:
        feat = cand["feat"]
        dists = [float(np.linalg.norm(feat - tc)) for tc in toxic_stack]
        min_d = min(dists) if dists else 0.0
        scored.append((min_d, cand))

    scored.sort(key=lambda x: x[0], reverse=True)
    top_n = max(8, int(len(scored) * 0.20))
    furthest = [c for _, c in scored[:top_n]]
    X = np.stack([c["feat"] for c in furthest])
    labels = _dbscan_labels(X)

    best_label = -1
    best_count = 0
    for lbl in set(labels.tolist()):
        if lbl < 0:
            continue
        cnt = int((labels == lbl).sum())
        if cnt > best_count:
            best_count = cnt
            best_label = int(lbl)

    if best_label < 0:
        cluster_members = furthest[: min(6, len(furthest))]
        method = "furthest_rank"
    else:
        cluster_members = [furthest[i] for i, lb in enumerate(labels) if lb == best_label]
        method = "toxic_inverted_dbscan"

    tickers = [str(c.get("ticker") or "") for c in cluster_members if c.get("ticker")]
    sectors = [str(c.get("sector") or "") for c in cluster_members if c.get("sector")]
    sector_mode = ""
    if sectors:
        sector_mode = max(set(sectors), key=sectors.count)

    theme = {
        "market": mk,
        "method": method,
        "tickers": tickers[:12],
        "sector_hint": sector_mode,
        "n_candidates": len(candidates),
        "n_furthest": len(furthest),
        "n_cluster": len(cluster_members),
        "confidence": round(min(0.95, 0.4 + 0.05 * len(cluster_members)), 3),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "toxic_rule_count": len(toxic_rules),
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact = os.path.join(_artifact_dir(), f"hidden_theme_{mk}_{stamp}.json")
    with open(artifact, "w", encoding="utf-8") as fh:
        json.dump(theme, fh, ensure_ascii=False, indent=2)

    theme_key = f"HIDDEN_SPILLOVER_THEME_{mk}"
    cfg[theme_key] = theme
    if mk == "US":
        cfg["US_ZERO_SAMPLE_SPILLOVER"] = {
            **(cfg.get("US_ZERO_SAMPLE_SPILLOVER") or {}),
            "sector_std": sector_mode or (cfg.get("US_ZERO_SAMPLE_SPILLOVER") or {}).get("sector_std", ""),
            "method": f"hidden_theme::{method}",
            "confidence": theme["confidence"],
            "hidden_tickers": tickers[:8],
        }
    try:
        from config_manager import save_system_config

        save_system_config(cfg)
    except Exception:
        pass

    out.update(ok=True, theme=theme, artifact=artifact, reason="mining_complete")
    return out


class OfflineRnDSandbox:
    """내부 전용 듀얼 R&D 엔진."""

    def run(self, market: str, *, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        mk = str(market or "KR").upper()
        stress = run_proprietary_stress_test(mk, cfg=cfg)
        mining = run_toxic_inverted_mining(mk, cfg=cfg)
        return {
            "market": mk,
            "stress": stress,
            "mining": mining,
            "ok": bool(stress.get("ok") or mining.get("ok")),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
        }


def run_offline_rnd_sandbox(market: str) -> Dict[str, Any]:
    return OfflineRnDSandbox().run(market)

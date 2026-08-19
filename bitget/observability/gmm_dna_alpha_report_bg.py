"""
I-GMM-DNA-01b — weekly GMM/DNA alpha observability (read-only).

Reads forward trades + CRYPTO_DNA_ALPHA_RANK* + Cos_eff log samples.
Writes one ``gmm_dna_alpha_report_weekly`` ops_events row.
Does NOT touch gates.py or gmm_dna_alpha_sync.py.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import subprocess
from collections import Counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from bitget.infra.clock import utc_hours_ago_iso, utc_now_iso

logger = logging.getLogger(__name__)

_SUMMARY_EVENT = "gmm_dna_alpha_report_weekly"
_SUMMARY_COMPONENT = "observability.dna"
_FORWARD_TABLE = "bitget_forward_trades"
_COS_EFF_RE = re.compile(r"Cos_eff=([\d.]+)")
_RANK_KEYS = ("CRYPTO_DNA_ALPHA_RANK1", "CRYPTO_DNA_ALPHA_RANK2", "CRYPTO_DNA_ALPHA_RANK3")


def gmm_dna_alpha_report_enabled() -> bool:
    env = os.environ.get("GMM_DNA_ALPHA_REPORT_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("GMM_DNA_ALPHA_REPORT_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import GMM_DNA_ALPHA_REPORT_ENABLED

    return bool(GMM_DNA_ALPHA_REPORT_ENABLED)


def _resolve_window_days(window_days: Optional[int] = None) -> int:
    if window_days is not None:
        return max(1, int(window_days))
    env = os.environ.get("GMM_DNA_ALPHA_REPORT_WINDOW_DAYS")
    if env is not None and str(env).strip():
        try:
            return max(1, int(float(env)))
        except (TypeError, ValueError):
            pass
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("GMM_DNA_ALPHA_REPORT_WINDOW_DAYS", None)
        if raw is not None:
            return max(1, int(float(raw)))
    except Exception:
        pass
    from bitget.infra.memory_policy import GMM_DNA_ALPHA_REPORT_WINDOW_DAYS

    return max(1, int(GMM_DNA_ALPHA_REPORT_WINDOW_DAYS))


def _resolve_log_source_pref() -> str:
    env = os.environ.get("GMM_DNA_ALPHA_REPORT_LOG_SOURCE")
    if env is not None and str(env).strip():
        return str(env).strip().lower()
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("GMM_DNA_ALPHA_REPORT_LOG_SOURCE", None)
        if raw is not None and str(raw).strip():
            return str(raw).strip().lower()
    except Exception:
        pass
    from bitget.infra.memory_policy import GMM_DNA_ALPHA_REPORT_LOG_SOURCE

    return str(GMM_DNA_ALPHA_REPORT_LOG_SOURCE or "journal").strip().lower()


def _forward_db_path() -> str:
    from bitget.forward.shared import DB_PATH

    return str(DB_PATH or "")


def _parse_cos_eff_values(text: str) -> List[float]:
    out: List[float] = []
    for m in _COS_EFF_RE.finditer(text or ""):
        try:
            out.append(float(m.group(1)))
        except (TypeError, ValueError):
            continue
    return out


def _read_journal_text(window_days: int) -> Optional[str]:
    since = f"{int(window_days)} days ago"
    try:
        proc = subprocess.run(
            [
                "journalctl",
                "-u",
                "dante-bitget*",
                "--since",
                since,
                "--no-pager",
                "-o",
                "cat",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if proc.returncode != 0:
            return None
        text = proc.stdout or ""
        return text if text.strip() else None
    except (OSError, subprocess.SubprocessError) as ex:
        logger.info("gmm dna report journalctl unavailable: %s", ex)
        return None


def _read_file_log_text(log_dir: Optional[str] = None) -> Optional[str]:
    try:
        from bitget.infra.data_paths import logs_dir

        base = log_dir or logs_dir()
    except Exception:
        base = log_dir or ""
    if not base or not os.path.isdir(base):
        return None
    chunks: List[str] = []
    try:
        names = sorted(
            n
            for n in os.listdir(base)
            if n.startswith("bitget") and (n.endswith(".log") or ".log." in n)
        )
    except OSError as ex:
        logger.info("gmm dna report log dir list failed: %s", ex)
        return None
    for name in names[-20:]:
        path = os.path.join(base, name)
        try:
            with open(path, encoding="utf-8", errors="replace") as fh:
                chunks.append(fh.read())
        except OSError:
            continue
    text = "\n".join(chunks)
    return text if text.strip() else None


def _collect_cos_eff_samples(
    *,
    window_days: int,
    log_text: Optional[str] = None,
    log_dir: Optional[str] = None,
) -> Tuple[Optional[List[float]], str]:
    """Return (samples|None, log_source_used)."""
    if log_text is not None:
        vals = _parse_cos_eff_values(log_text)
        return vals, "file" if vals or log_text.strip() else "unavailable"

    pref = _resolve_log_source_pref()
    order: Sequence[str]
    if pref == "file":
        order = ("file", "journal")
    else:
        order = ("journal", "file")

    for src in order:
        if src == "journal":
            text = _read_journal_text(window_days)
            if text is None:
                continue
            return _parse_cos_eff_values(text), "journal"
        text = _read_file_log_text(log_dir=log_dir)
        if text is None:
            continue
        return _parse_cos_eff_values(text), "file"

    return None, "unavailable"


def _trade_counts_by_market(
    *,
    forward_db_path: Optional[str] = None,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    from bitget.evolution.market_key_normalize import normalize_market_key

    open_by: Counter[str] = Counter()
    closed_by: Counter[str] = Counter()
    path = forward_db_path or _forward_db_path()
    if not path or not os.path.isfile(path):
        return dict(open_by), dict(closed_by)
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            table = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (_FORWARD_TABLE,),
            ).fetchone()
            if not table:
                return dict(open_by), dict(closed_by)
            rows = conn.execute(
                f"""
                SELECT market_type, status, COUNT(*)
                FROM {_FORWARD_TABLE}
                GROUP BY market_type, status
                """
            ).fetchall()
            for market_type, status, cnt in rows:
                mk = normalize_market_key(str(market_type or "spot"))
                st = str(status or "").upper()
                n = int(cnt or 0)
                if st == "OPEN":
                    open_by[mk] += n
                elif st.startswith("CLOSED"):
                    closed_by[mk] += n
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("gmm dna report trade counts failed: %s", ex)
    return dict(open_by), dict(closed_by)


def _as_dna_dict(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    return None


def _dna_rank_and_shape() -> Tuple[Dict[str, bool], Dict[str, int]]:
    present = {f"RANK{i}": False for i in (1, 2, 3)}
    shape_dist: Counter[str] = Counter()
    try:
        from bitget.infra import config_manager as cm
    except Exception:
        return present, dict(shape_dist)

    for i, key in enumerate(_RANK_KEYS, start=1):
        try:
            raw = cm.get_config_value(key, None)
        except Exception:
            raw = None
        dna = _as_dna_dict(raw)
        if dna is None:
            continue
        present[f"RANK{i}"] = True
        src = dna.get("shape_source")
        if src is not None and str(src).strip():
            shape_dist[str(src).strip()] += 1
    return present, dict(shape_dist)


# mining floor used by data_miner._fit_gmm_templates (len(xdf) < 12 → {})
GMM_FIT_MIN_ROWS_OBSERVED = 12
_DNA_DIAG_TFS = ("1D", "4H", "2H", "1H")


def resolve_bitget_min_mfe_for_mining(cfg: Optional[Dict[str, Any]] = None) -> float:
    if isinstance(cfg, dict) and cfg.get("BITGET_MIN_MFE_FOR_MINING") is not None:
        try:
            return float(cfg.get("BITGET_MIN_MFE_FOR_MINING"))
        except (TypeError, ValueError):
            pass
    return 8.0


def count_gmm_template_clusters(gmm: Any) -> int:
    """Count rankable-ish cluster dicts under BITGET_GMM_DNA_TEMPLATES (read-only)."""
    if not isinstance(gmm, dict) or not gmm:
        return 0
    n = 0
    for v in gmm.values():
        if not isinstance(v, dict):
            continue
        inner = v.get("templates")
        if isinstance(inner, dict):
            n += sum(1 for c in inner.values() if isinstance(c, dict) and c)
            continue
        # legacy flat cluster map under TF blob
        n += sum(
            1
            for k, c in v.items()
            if isinstance(c, dict)
            and c
            and str(k).upper().startswith(("GMM_CLUSTER", "CLUSTER"))
        )
    return int(n)


def collect_closed_mfe_counts_by_tf(
    *,
    forward_db_path: Optional[str] = None,
    mfe_min: float = 8.0,
) -> Tuple[Optional[Dict[str, int]], Optional[Dict[str, int]], Optional[str]]:
    """
    CLOSED counts + mining-eligible (mfe≥min + feature non-null) by TF.
    Feature gate mirrors data_miner._fit_gmm_templates dropna subset
    (dyn_cpv, dyn_tb, v_energy, dyn_rs|v_rs).
    Returns (n_closed_by_tf, n_mfe8_by_tf, error) — error set on DB failure.
    """
    from bitget.infra.data_paths import market_data_db_path
    from bitget.infra.shared_db_connector import get_connection

    path = forward_db_path or market_data_db_path()
    if not path or not os.path.isfile(path):
        return None, None, f"db_missing:{path}"
    n_closed = {tf: 0 for tf in _DNA_DIAG_TFS}
    n_mfe = {tf: 0 for tf in _DNA_DIAG_TFS}
    try:
        conn = get_connection(path, read_only=True)
        try:
            sql = f"""
                SELECT UPPER(timeframe) AS tf,
                       COUNT(*) AS n_closed,
                       SUM(
                         CASE
                           WHEN COALESCE(mfe, 0) >= ?
                            AND dyn_cpv IS NOT NULL
                            AND dyn_tb IS NOT NULL
                            AND v_energy IS NOT NULL
                            AND (dyn_rs IS NOT NULL OR v_rs IS NOT NULL)
                           THEN 1 ELSE 0
                         END
                       ) AS n_mfe
                FROM {_FORWARD_TABLE}
                WHERE status LIKE 'CLOSED%'
                GROUP BY UPPER(timeframe)
            """
            rows = conn.execute(sql, (float(mfe_min),)).fetchall()
            for tf, c, m in rows:
                key = str(tf or "").strip().upper()
                if not key:
                    continue
                n_closed[key] = int(c or 0)
                n_mfe[key] = int(m or 0)
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("dna diag TF counts failed: %s", ex)
        return None, None, str(ex)[:200]
    return n_closed, n_mfe, None


def compute_weekly_gmm_dna_alpha_report_bg(
    window_days: int = 7,
    *,
    forward_db_path: Optional[str] = None,
    log_text: Optional[str] = None,
    log_dir: Optional[str] = None,
) -> Dict[str, Any]:
    days = _resolve_window_days(window_days)
    samples, log_source = _collect_cos_eff_samples(
        window_days=days,
        log_text=log_text,
        log_dir=log_dir,
    )
    if samples is None:
        sample_count: Optional[int] = None
        zero_ratio: Optional[float] = None
        mean_nonzero: Optional[float] = None
    else:
        sample_count = len(samples)
        if sample_count == 0:
            zero_ratio = None
            mean_nonzero = None
        else:
            zeros = sum(1 for v in samples if abs(v) < 1e-12)
            zero_ratio = round(zeros / float(sample_count), 6)
            nonzero = [v for v in samples if abs(v) >= 1e-12]
            mean_nonzero = (
                round(sum(nonzero) / float(len(nonzero)), 6) if nonzero else None
            )

    open_by, closed_by = _trade_counts_by_market(forward_db_path=forward_db_path)
    rank_present, shape_dist = _dna_rank_and_shape()
    return {
        "window_days": days,
        "cos_eff_sample_count": sample_count,
        "cos_eff_zero_ratio": zero_ratio,
        "cos_eff_mean_nonzero": mean_nonzero,
        "open_count_by_market": open_by,
        "closed_count_by_market": closed_by,
        "dna_rank_keys_present": rank_present,
        "shape_source_distribution": shape_dist,
        "log_source_used": log_source,
        "since_iso": utc_hours_ago_iso(float(days) * 24.0),
    }


def persist_gmm_dna_alpha_report_weekly(
    summary: Dict[str, Any],
    *,
    ops_db_path: Optional[str] = None,
) -> bool:
    from bitget.infra.ops_logger import insert_ops_event

    payload = dict(summary)
    payload["recorded_at"] = utc_now_iso()
    _ = ops_db_path
    return bool(
        insert_ops_event(
            component=_SUMMARY_COMPONENT,
            severity="INFO",
            event=_SUMMARY_EVENT,
            payload=payload,
        )
    )


def run_gmm_dna_alpha_report_job(
    *,
    window_days: Optional[int] = None,
    forward_db_path: Optional[str] = None,
    ops_db_path: Optional[str] = None,
    log_text: Optional[str] = None,
    log_dir: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """weekly_evolution pipeline hook — read-only aggregate."""
    if not gmm_dna_alpha_report_enabled():
        logger.info("gmm dna alpha report disabled — skip batch")
        return None

    summary = compute_weekly_gmm_dna_alpha_report_bg(
        window_days=_resolve_window_days(window_days),
        forward_db_path=forward_db_path,
        log_text=log_text,
        log_dir=log_dir,
    )
    ok = persist_gmm_dna_alpha_report_weekly(summary, ops_db_path=ops_db_path)
    logger.info(
        "gmm dna report window=%dd cos_n=%s zero_ratio=%s log=%s inserted=%s",
        summary["window_days"],
        summary["cos_eff_sample_count"],
        summary["cos_eff_zero_ratio"],
        summary["log_source_used"],
        ok,
    )
    return {
        "inserted": ok,
        "summary": summary,
    }

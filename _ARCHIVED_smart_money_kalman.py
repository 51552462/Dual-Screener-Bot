"""
[ARCHIVED] 실험용 Smart Money Kalman — 운영에서 제외.

SSOT: smart_money_tracker.py → SMART_MONEY_RADAR.picks[*].avg_price (system_config.json)
본 파일은 참고·재현용. smart_money_targets.json 은 어떤 스크리너도 읽지 않음.

(구 smart_money_kalman.py 전체 로직 보관)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date

import numpy as np

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_JSON = os.path.join(_THIS_DIR, "smart_money_targets.json")

DEFAULT_TICKERS = ("005930", "000660", "035420", "051910", "006400")


@dataclass
class DailyRow:
    o: float
    h: float
    l: float
    c: float
    v: float
    inst_net_shares: float
    inst_net_value_krw: float


def _atomic_write_json(path: str, obj: dict) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def dummy_fetch_kr_institutional_panel(
    ticker: str,
    n_days: int = 420,
    seed: int | None = None,
) -> list[DailyRow]:
    rng = np.random.default_rng(seed if seed is not None else sum(ord(c) for c in ticker) % 10_007)
    base = 50_000.0 + (int(ticker[-3:]) % 100) * 800.0
    rows: list[DailyRow] = []
    px = float(base)

    for i in range(n_days):
        shock = rng.normal(0, 0.012)
        if rng.random() < 0.03:
            shock += rng.choice([-1, 1]) * rng.uniform(0.02, 0.05)
        px = max(1_000.0, px * (1.0 + shock))
        o = px * (1.0 + rng.normal(0, 0.002))
        c = px * (1.0 + rng.normal(0, 0.003))
        h = max(o, c) * (1.0 + abs(rng.normal(0, 0.004)))
        l = min(o, c) * (1.0 - abs(rng.normal(0, 0.004)))
        v = float(rng.lognormal(16.0, 0.35))

        tilt = np.tanh((c - o) / max(o, 1e-9) * 5.0)
        inst_intensity = rng.lognormal(0.0, 0.55) * (1.0 + 0.35 * tilt + 0.25 * (v / 1e6))
        inst_net_shares = float(rng.normal(25_000.0, 120_000.0) * inst_intensity)
        inst_net_value_krw = inst_net_shares * float((o + h + l + c) / 4.0) * (1.0 + rng.normal(0, 0.01))

        rows.append(
            DailyRow(
                o=float(o),
                h=float(h),
                l=float(l),
                c=float(c),
                v=float(v),
                inst_net_shares=inst_net_shares,
                inst_net_value_krw=float(inst_net_value_krw),
            )
        )
        px = c
    return rows


def _median_abs(x: np.ndarray) -> float:
    x = np.abs(x[np.isfinite(x)])
    if x.size == 0:
        return 1.0
    return float(np.median(x)) if np.median(x) > 1e-9 else 1.0


def run_kalman_smart_money(
    rows: list[DailyRow],
    obs_noise_seed: int | None = None,
) -> tuple[float, float, np.ndarray]:
    n = len(rows)
    u = np.array([r.inst_net_shares for r in rows], dtype=np.float64)
    vwap = np.array(
        [
            (r.inst_net_value_krw / r.inst_net_shares)
            if abs(r.inst_net_shares) > 1.0
            else (r.h + r.l + r.c) / 3.0
            for r in rows
        ],
        dtype=np.float64,
    )
    cum_q_obs = np.cumsum(u)
    med_u = _median_abs(u)
    med_v = _median_abs(np.array([r.v for r in rows], dtype=np.float64))
    if obs_noise_seed is not None:
        rng_z = np.random.default_rng(int(obs_noise_seed) & 0xFFFFFFFF)
        z2_noise = rng_z.normal(0.0, med_u * 0.012 + 200.0, size=n)
    else:
        z2_noise = np.zeros(n, dtype=np.float64)
    z = np.stack([vwap, cum_q_obs + z2_noise], axis=0)

    x = np.array([[float(vwap[0])], [0.0]]], dtype=np.float64)
    P = np.diag([200.0**2, (500_000.0) ** 2]).astype(np.float64)

    F = np.eye(2, dtype=np.float64)
    B = np.array([[0.0], [1.0]], dtype=np.float64)
    H = np.eye(2, dtype=np.float64)

    q_mu = (8.0) ** 2
    q_q = (25_000.0) ** 2
    Q = np.diag([q_mu, q_q]).astype(np.float64)

    I2 = np.eye(2, dtype=np.float64)

    for t in range(n):
        u_t = float(u[t])
        x_pred = F @ x + B * u_t
        P_pred = F @ P @ F.T + Q

        vol_t = float(rows[t].v)
        mag = float(abs(u_t))
        scale_r = 1.0 / (1.0 + 0.35 * (mag / med_u) + 0.12 * (vol_t / med_v))
        r0 = (max(80.0, 0.02 * abs(x_pred[0, 0])) ** 2) * (1.0 / max(scale_r, 0.25))
        r1 = ((max(5000.0, 0.05 * abs(x_pred[1, 0]))) ** 2) * (1.0 / max(scale_r, 0.25))
        R = np.diag([r0, r1]).astype(np.float64)

        z_t = z[:, t : t + 1]
        y = z_t - H @ x_pred
        S = H @ P_pred @ H.T + R
        try:
            S_inv = np.linalg.inv(S)
        except np.linalg.LinAlgError:
            S_inv = np.linalg.pinv(S)
        K = P_pred @ H.T @ S_inv
        x = x_pred + K @ y
        P = (I2 - K @ H) @ P_pred

    mu_hat = float(x[0, 0])
    sig_mu = float(max(np.sqrt(max(P[0, 0], 1e-12)), 1e-9))
    conf = float(np.clip(1.0 - sig_mu / max(abs(mu_hat) * 0.02, 500.0), 0.0, 1.0))
    return mu_hat, conf, P


def build_targets(tickers: tuple[str, ...] = DEFAULT_TICKERS) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for code in tickers:
        seed = 10_007 + int(code) * 13 if code.isdigit() else hash(code) % 10_009
        rows = dummy_fetch_kr_institutional_panel(code, seed=seed)
        try:
            kalman_avg, confidence, _P = run_kalman_smart_money(
                rows,
                obs_noise_seed=(seed * 31_337 + int(code)) % (2**31 - 1)
                if code.isdigit()
                else abs(hash(code)) % (2**31 - 1),
            )
        except Exception as e:
            out[code] = {"kalman_avg_price": None, "confidence": 0.0, "error": str(e)}
            continue
        out[code] = {
            "kalman_avg_price": int(round(kalman_avg)),
            "confidence": round(confidence, 4),
        }
    return out


def main() -> None:
    payload = {
        "generated_at": date.today().isoformat(),
        "model": "ARCHIVED Kalman experiment — not SSOT",
        "tickers": build_targets(),
    }
    _atomic_write_json(OUTPUT_JSON, payload)
    print(f"✅ 저장: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()

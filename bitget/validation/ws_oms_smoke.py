"""
Live WS / OMS smoke checklist — observational readiness only.

Institutional rules:
  - Never opens WebSockets, never places orders, never mutates trading state
  - Opt-in WS flags: when OFF, related checks are informational (not FAIL)
  - When ON: require fresh ``bitget_auto_pilot`` heartbeat + started WS payload
  - Credentials: presence bool only — never log secret values
  - OMS REST-heavy is WARN per plane (FAIL only under strict + enough samples)
  - Private plane ignores public ticker (tk) contamination
"""
from __future__ import annotations

import os
from typing import Any, Optional

from bitget.infra.clock import parse_utc_iso, utc_now
from bitget.infra.memory_policy import (
    WS_OMS_SMOKE_BUF_STALE_SEC,
    WS_OMS_SMOKE_HB_LOOKBACK_HOURS,
    WS_OMS_SMOKE_HEARTBEAT_MAX_AGE_SEC,
)
from bitget.trading.oms_source_stats import analyze_oms_book

# Must match bitget.pipelines.bitget_auto_pilot.HEARTBEAT_COMPONENT / watchdog SSOT
HEARTBEAT_COMPONENT = "bitget_auto_pilot"

_HEALTHY_WS_STATES = frozenset({"connected", "authenticated"})
_TRANSITIONAL_WS_STATES = frozenset({"connecting", "reconnecting"})


def _env_truthy(name: str, default: str = "0") -> bool:
    return str(os.environ.get(name, default)).strip().lower() in ("1", "true", "yes", "on")


def _item(
    *,
    ok: bool,
    severity: str,
    message: str,
    **extra: Any,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ok": bool(ok),
        "severity": str(severity),
        "message": str(message)[:240],
    }
    for k, v in extra.items():
        if k and str(k)[:48]:
            out[str(k)[:48]] = v
    return out


def _latest_auto_pilot_heartbeat(
    *,
    hours: float | None = None,
    limit: int = 800,
) -> Optional[dict[str, Any]]:
    from bitget.infra import ops_logger

    lookback = float(hours if hours is not None else WS_OMS_SMOKE_HB_LOOKBACK_HOURS)
    ticks = ops_logger.fetch_heartbeat_ticks(hours=lookback, limit=int(limit))
    for row in ticks or []:
        if str(row.get("component") or "") == HEARTBEAT_COMPONENT:
            return row
    return None


def _heartbeat_age_sec(row: Optional[dict[str, Any]]) -> Optional[float]:
    if not row:
        return None
    ts = parse_utc_iso(str(row.get("ts_utc") or ""))
    if ts is None:
        return None
    return max(0.0, (utc_now() - ts).total_seconds())


def _eval_ws_payload(
    name: str,
    *,
    env_enabled: bool,
    payload: Optional[dict[str, Any]],
) -> dict[str, Any]:
    """Evaluate public_ws / private_ws heartbeat attaché."""
    if not env_enabled:
        return _item(
            ok=True,
            severity="info",
            message=f"{name} opt-in off — skip live assert",
            enabled=False,
        )
    if not isinstance(payload, dict):
        return _item(
            ok=False,
            severity="hard",
            message=f"{name} missing on auto_pilot heartbeat (daemon may lack env)",
            enabled=True,
        )
    if payload.get("error"):
        return _item(
            ok=False,
            severity="hard",
            message=f"{name} heartbeat error: {str(payload.get('error'))[:120]}",
            enabled=True,
            payload_enabled=bool(payload.get("enabled")),
        )
    if not bool(payload.get("enabled")):
        return _item(
            ok=False,
            severity="hard",
            message=f"{name} env on but heartbeat reports enabled=False",
            enabled=True,
            payload_enabled=False,
        )
    if "started" in payload and not bool(payload.get("started")):
        return _item(
            ok=False,
            severity="hard",
            message=f"{name} not started (soft-disable: deps/creds/transport?)",
            enabled=True,
            started=False,
            ws_state=str(payload.get("ws_state") or "")[:32],
        )

    state = str(payload.get("ws_state") or "").strip().lower()
    if state in _HEALTHY_WS_STATES or state == "":
        # empty state tolerated only if started True (legacy ticks)
        ok_state = True
        sev = "info"
        msg = f"{name} live ok"
    elif state in _TRANSITIONAL_WS_STATES:
        ok_state = True
        sev = "warn"
        msg = f"{name} transitional state={state}"
    else:
        ok_state = False
        sev = "hard"
        msg = f"{name} unhealthy ws_state={state}"

    try:
        buf_age = float(payload.get("buf_age_sec")) if payload.get("buf_age_sec") is not None else None
    except (TypeError, ValueError):
        buf_age = None
    if ok_state and buf_age is not None and buf_age > float(WS_OMS_SMOKE_BUF_STALE_SEC):
        sev = "warn"
        msg = f"{name} buffer stale buf_age_sec={buf_age:.1f}"

    return _item(
        ok=ok_state,
        severity=sev if ok_state else "hard",
        message=msg,
        enabled=True,
        started=bool(payload.get("started", True)),
        ws_state=state[:32],
        buf_age_sec=buf_age,
        frames=payload.get("frames"),
        updates=payload.get("updates"),
    )


def check_ws_oms_smoke(
    *,
    strict: bool | None = None,
    heartbeat_row: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Pure-ish observational checklist. Never raises for soft failures."""
    if strict is None:
        strict = _env_truthy("BITGET_WS_OMS_SMOKE_STRICT", "0")

    public_env = _env_truthy("BITGET_DAEMON_PUBLIC_WS", "0")
    private_env = _env_truthy("BITGET_DAEMON_PRIVATE_WS", "0")
    either_ws = public_env or private_env

    checks: dict[str, Any] = {}

    # --- deps ---
    try:
        from bitget.data.ws_market_service import live_ws_transport_available

        ws_ok = bool(live_ws_transport_available())
    except Exception:
        ws_ok = False
    if either_ws:
        checks["websocket_client"] = _item(
            ok=ws_ok,
            severity="hard",
            message="websocket-client available" if ws_ok else "install websocket-client",
        )
    else:
        checks["websocket_client"] = _item(
            ok=True,
            severity="info",
            message="websocket-client not required (WS opt-in off)",
            available=ws_ok,
        )

    # --- credentials (bool only) ---
    try:
        from bitget.data.ws_private_service import credentials_available

        creds = bool(credentials_available())
    except Exception:
        creds = False
    if private_env:
        checks["credentials"] = _item(
            ok=creds,
            severity="hard",
            message=(
                "BITGET_ACCESS_KEY/SECRET_KEY/PASSPHRASE present"
                if creds
                else "private WS on but API credentials missing"
            ),
            present=creds,
        )
    else:
        checks["credentials"] = _item(
            ok=True,
            severity="info",
            message="credentials not required (private WS opt-in off)",
            present=creds,
        )

    # --- static daemon policy (code drift) ---
    try:
        from bitget.validation.architecture_checks import (
            check_daemon_private_ws_policy,
            check_daemon_public_ws_policy,
            check_oms_book_consumer_ssot,
        )

        pub_pol = check_daemon_public_ws_policy()
        priv_pol = check_daemon_private_ws_policy()
        oms_pol = check_oms_book_consumer_ssot()
        checks["daemon_public_ws_policy"] = _item(
            ok=bool(pub_pol.get("ok")),
            severity="hard",
            message=str(pub_pol.get("message") or ""),
        )
        checks["daemon_private_ws_policy"] = _item(
            ok=bool(priv_pol.get("ok")),
            severity="hard",
            message=str(priv_pol.get("message") or ""),
        )
        checks["oms_book_consumer_ssot"] = _item(
            ok=bool(oms_pol.get("ok")),
            severity="hard",
            message=str(oms_pol.get("message") or ""),
            failed=list(oms_pol.get("failed") or [])[:12],
        )
    except Exception as e:
        checks["daemon_public_ws_policy"] = _item(
            ok=False, severity="hard", message=f"policy check failed: {e}"
        )
        checks["daemon_private_ws_policy"] = _item(
            ok=False, severity="hard", message=f"policy check failed: {e}"
        )
        checks["oms_book_consumer_ssot"] = _item(
            ok=False, severity="hard", message=f"oms consumer policy failed: {e}"
        )

    # --- env snapshot ---
    checks["public_ws_env"] = _item(
        ok=True,
        severity="info",
        message="BITGET_DAEMON_PUBLIC_WS",
        enabled=public_env,
    )
    checks["private_ws_env"] = _item(
        ok=True,
        severity="info",
        message="BITGET_DAEMON_PRIVATE_WS",
        enabled=private_env,
    )

    # --- live heartbeat ---
    row = heartbeat_row if heartbeat_row is not None else _latest_auto_pilot_heartbeat()
    age = _heartbeat_age_sec(row)
    payload = (row or {}).get("payload") if isinstance(row, dict) else None
    if not isinstance(payload, dict):
        payload = {}

    max_age = float(WS_OMS_SMOKE_HEARTBEAT_MAX_AGE_SEC)
    if either_ws:
        if row is None or age is None:
            checks["auto_pilot_heartbeat"] = _item(
                ok=False,
                severity="hard",
                message=f"no {HEARTBEAT_COMPONENT} heartbeat in lookback window",
            )
        elif age > max_age:
            checks["auto_pilot_heartbeat"] = _item(
                ok=False,
                severity="hard",
                message=f"auto_pilot heartbeat stale age_sec={age:.1f} max={max_age}",
                age_sec=round(age, 1),
                ts_utc=str(row.get("ts_utc") or "")[:32],
            )
        else:
            checks["auto_pilot_heartbeat"] = _item(
                ok=True,
                severity="info",
                message="auto_pilot heartbeat fresh",
                age_sec=round(age, 1),
                ts_utc=str(row.get("ts_utc") or "")[:32],
            )
    else:
        checks["auto_pilot_heartbeat"] = _item(
            ok=True,
            severity="info",
            message="WS opt-in off — heartbeat freshness not required",
            age_sec=round(age, 1) if age is not None else None,
            present=row is not None,
        )

    pub_payload = payload.get("public_ws") if isinstance(payload.get("public_ws"), dict) else None
    priv_payload = payload.get("private_ws") if isinstance(payload.get("private_ws"), dict) else None
    checks["public_ws_live"] = _eval_ws_payload(
        "public_ws", env_enabled=public_env, payload=pub_payload
    )
    checks["private_ws_live"] = _eval_ws_payload(
        "private_ws", env_enabled=private_env, payload=priv_payload
    )

    # --- OMS book (dual-plane) ---
    oms = payload.get("oms_book") if isinstance(payload.get("oms_book"), dict) else None
    analysis = analyze_oms_book(oms)

    def _plane_check(
        *,
        key: str,
        enabled: bool,
        plane_status: str,
        rest_share: Any,
        n_total: int,
        label: str,
    ) -> dict[str, Any]:
        if not enabled:
            return _item(
                ok=True,
                severity="info",
                message=f"{label} plane skip (WS opt-in off)",
                status=plane_status,
                rest_share=rest_share,
                n_total=n_total,
            )
        ok = True
        sev = "info"
        msg = f"{label} plane status={plane_status}"
        if plane_status == "rest_heavy":
            if int(n_total) >= int(analysis.get("min_samples") or 0):
                if strict:
                    ok = False
                    sev = "hard"
                    msg = f"{label} plane REST-heavy under strict smoke"
                else:
                    sev = "warn"
                    msg = f"{label} plane REST-heavy while WS enabled"
            else:
                msg = f"{label} plane REST-heavy but below min_samples"
        return _item(
            ok=ok,
            severity=sev,
            message=msg,
            status=plane_status,
            rest_share=rest_share,
            n_total=n_total,
        )

    checks["oms_book_private"] = _plane_check(
        key="oms_book_private",
        enabled=private_env,
        plane_status=str(analysis.get("private_status") or "no_data"),
        rest_share=analysis.get("private_rest_share"),
        n_total=int(analysis.get("private_n_total") or 0),
        label="private",
    )
    checks["oms_book_public"] = _plane_check(
        key="oms_book_public",
        enabled=public_env,
        plane_status=str(analysis.get("public_status") or "no_data"),
        rest_share=analysis.get("public_rest_share"),
        n_total=int(analysis.get("public_n_total") or 0),
        label="public",
    )
    # Compact summary (extras for ops) — never invents WS hits
    checks["oms_book"] = _item(
        ok=True,
        severity="info",
        message=(
            f"private={analysis.get('private_status')} "
            f"public={analysis.get('public_status')}"
        ),
        private_status=analysis.get("private_status"),
        public_status=analysis.get("public_status"),
        bal_ws=analysis.get("bal_ws"),
        bal_rest=analysis.get("bal_rest"),
        mm_ws=analysis.get("mm_ws"),
        mm_rest=analysis.get("mm_rest"),
        tk_ws=analysis.get("tk_ws"),
        tk_rest=analysis.get("tk_rest"),
        pos_ws=analysis.get("pos_ws"),
        pos_rest=analysis.get("pos_rest"),
    )

    hard_fail = [
        k
        for k, v in checks.items()
        if isinstance(v, dict) and v.get("severity") == "hard" and not v.get("ok")
    ]
    warns = [
        k
        for k, v in checks.items()
        if isinstance(v, dict) and v.get("severity") == "warn"
    ]
    passed = len(hard_fail) == 0
    return {
        "ok": True,
        "passed": passed,
        "strict": bool(strict),
        "public_ws_env": public_env,
        "private_ws_env": private_env,
        "hard_failures": hard_fail,
        "warnings": warns,
        "checks": checks,
        "oms_analysis": analysis,
        "message": (
            "ws/oms smoke PASS"
            if passed
            else f"ws/oms smoke FAIL: {', '.join(hard_fail)}"
        ),
    }


def run_ws_oms_smoke(
    *,
    strict: bool | None = None,
    raise_on_fail: bool = False,
) -> dict[str, Any]:
    """CLI / pipeline entry — records gauge.

    ``raise_on_fail``: dedicated ``ws_oms_smoke`` mode exits non-zero on hard FAIL.
    Health attaché keeps ``raise_on_fail=False`` (informational unless STRICT).
    ``strict`` (or env): escalates OMS REST-heavy to hard + also raises on FAIL.
    """
    from bitget.infra import ops_logger
    from bitget.infra.logging_setup import get_logger

    log = get_logger("bitget.validation.ws_oms_smoke")
    report = check_ws_oms_smoke(strict=strict)
    try:
        ops_logger.record_gauge_snapshot(
            "bitget.ws_oms_smoke",
            {
                "passed": bool(report.get("passed")),
                "strict": bool(report.get("strict")),
                "public_ws_env": bool(report.get("public_ws_env")),
                "private_ws_env": bool(report.get("private_ws_env")),
                "hard_failures": list(report.get("hard_failures") or [])[:12],
                "warnings": list(report.get("warnings") or [])[:12],
                "message": str(report.get("message") or "")[:200],
            },
        )
    except Exception:
        pass

    log.info(
        "[ws_oms_smoke] passed=%s strict=%s pub=%s priv=%s fails=%s warns=%s",
        report.get("passed"),
        report.get("strict"),
        report.get("public_ws_env"),
        report.get("private_ws_env"),
        report.get("hard_failures"),
        report.get("warnings"),
    )
    for name, item in (report.get("checks") or {}).items():
        if not isinstance(item, dict):
            continue
        log.info(
            "[ws_oms_smoke] %s ok=%s sev=%s %s",
            name,
            item.get("ok"),
            item.get("severity"),
            item.get("message"),
        )

    should_raise = bool(raise_on_fail) or bool(report.get("strict"))
    if should_raise and not report.get("passed"):
        raise RuntimeError(str(report.get("message") or "ws_oms_smoke failed"))
    return report

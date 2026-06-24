"""
MetaGovernor / Bitget pipeline CRITICAL 텔레그램 알림 (주식 factory_meta_alerts 패리티).
"""
from __future__ import annotations

from html import escape as html_escape


def send_meta_critical_alert(title: str, body: str, *, prefix: str = "CRITICAL") -> bool:
    """Meta 뇌사·파이프라인 치명 실패 — parse_mode=HTML 안전 발송."""
    t = str(title or "MetaGovernor").strip()[:120]
    b = str(body or "").strip()[:3500]
    if not b:
        b = "(no detail)"
    msg = (
        f"🚨🚨 <b>[{html_escape(prefix, quote=False)}] "
        f"{html_escape(t, quote=False)}</b>\n"
        f"<code>{html_escape(b, quote=False)}</code>"
    )
    try:
        from bitget.forward.shared import send_telegram_msg

        send_telegram_msg(msg)
        return True
    except Exception as e:
        try:
            print(f"[CRITICAL] telegram skip: {e}")
        except Exception:
            pass
        return False

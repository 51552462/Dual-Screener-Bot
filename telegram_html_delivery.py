"""
Telegram HTML delivery SSOT — 400 Bad Request 시 태그 완전 제거 후 plain 재전송.
"""
from __future__ import annotations

import re
from typing import Any, Optional

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def sanitize_telegram_plain_text(text: str) -> str:
    """parse_mode=HTML 실패 시 재전송용 — 모든 HTML 태그 제거."""
    if not text:
        return ""
    plain = _HTML_TAG_RE.sub("", str(text))
    for ent, ch in (
        ("&nbsp;", " "),
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", '"'),
    ):
        plain = plain.replace(ent, ch)
    return plain


def post_telegram_message(
    *,
    url: str,
    chat_id: str,
    text: str,
    parse_mode: Optional[str] = "HTML",
    timeout: float = 10.0,
    session: Any = None,
) -> Any:
    """
    sendMessage 1회 — HTML 실패(400) 시 sanitize 후 plain 재전송.
    session: requests 모듈 또는 requests.Session (post 메서드 필요).
    """
    import requests as _requests

    http = session if session is not None else _requests
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    use_html = str(parse_mode or "").upper() == "HTML"
    if use_html:
        payload["parse_mode"] = "HTML"
    res = http.post(url, json=payload, timeout=timeout)
    if use_html and getattr(res, "status_code", None) == 400:
        plain = sanitize_telegram_plain_text(text)
        res = http.post(
            url,
            json={"chat_id": chat_id, "text": plain},
            timeout=timeout,
        )
    return res

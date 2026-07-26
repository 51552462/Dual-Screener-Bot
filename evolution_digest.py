"""
[Δ] 진화·튜닝 요약 — META_CHANGELOG + config 스냅샷 diff (글로벌 · 1회 송출).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from config_manager import CONFIG_SNAPSHOTS_DIR, find_latest_config_snapshot_on_or_before
from tuning_digest_formatter import format_meta_changelog_telegram


def _snapshot_diff_line() -> Optional[str]:
    if not os.path.isdir(CONFIG_SNAPSHOTS_DIR):
        return None
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    p_today = find_latest_config_snapshot_on_or_before(today)
    p_yday = find_latest_config_snapshot_on_or_before(yesterday)
    if not p_today or not p_yday or p_today == p_yday:
        return None
    try:
        with open(p_today, encoding="utf-8") as f:
            cur = json.load(f)
        with open(p_yday, encoding="utf-8") as f:
            prev = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    keys = (
        "DYNAMIC_SUPERNOVA_CUTOFF",
        "DYNAMIC_ML_BOX_CUTOFF",
        "INVERSE_MODE_ACTIVE",
        "DOOMSDAY_DEFCON",
        "META_GLOBAL_KELLY_MULT",
    )
    parts: list[str] = []
    for k in keys:
        if cur.get(k) != prev.get(k):
            parts.append(k)
    if not parts:
        return None
    return f"스냅샷 Δ: {', '.join(parts[:5])}" + ("…" if len(parts) > 5 else "")


def build_global_evolution_digest_messages(meta: dict[str, Any]) -> list[str]:
    """[Δ] 글로벌 블록 — KR/US 루프 밖 1회, 켈리 Δ 많으면 다통."""
    page_groups = format_meta_changelog_telegram(meta, max_entries=5)
    snap = _snapshot_diff_line()
    if not page_groups and not snap:
        return []

    messages: list[str] = []
    for i, lines in enumerate(page_groups):
        header = (
            f"\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📐 <b>[Δ] 진화·튜닝</b> <i>(글로벌 · MetaGovernor)</i>\n"
        )
        if i > 0:
            header = (
                f"\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📐 <b>[Δ] 진화·튜닝</b> <i>(글로벌 · 계속 {i + 1}/{len(page_groups)})</i>\n"
            )
        body = "\n".join(lines)
        messages.append(f"{header}{body}\n")

    if snap:
        if messages:
            messages[-1] = messages[-1].rstrip() + f"\n{snap}\n"
        else:
            messages.append(
                f"\n━━━━━━━━━━━━━━━━━━━━\n"
                f"📐 <b>[Δ] 진화·튜닝</b> <i>(글로벌 · MetaGovernor)</i>\n"
                f"{snap}\n"
            )
    return messages


def build_global_evolution_digest_html(meta: dict[str, Any]) -> str:
    """[Δ] 글로벌 블록 — 단일 문자열 (레거시·첫 통만)."""
    msgs = build_global_evolution_digest_messages(meta)
    return msgs[0] if msgs else ""


def build_evolution_digest_html(
    meta: dict[str, Any],
    *,
    market: Optional[str] = None,
) -> str:
    """레거시 호환 — 시장 태그 없이 글로벌과 동일 본문."""
    _ = market
    return build_global_evolution_digest_html(meta)


def _esc(s: Any) -> str:
    import html

    return html.escape(str(s) if s is not None else "", quote=False)


def _truncate_names(names: list[str], *, limit: int = 8) -> str:
    if not names:
        return "—"
    shown = names[:limit]
    body = ", ".join(_esc(n) for n in shown)
    if len(names) > limit:
        body += f" …(+{len(names) - limit})"
    return body


def build_weekend_self_evolution_digest_messages(
    pipeline_result: dict[str, Any],
) -> list[str]:
    """
    주말 자가진화 파이프라인 결과 → 텔레그램 HTML 브리핑.
    퇴출·백신·신규 엘리트 DNA 목록을 한 통(또는 분할)으로 구성.
    """
    steps = pipeline_result.get("steps") or {}
    if not isinstance(steps, dict):
        return []

    bandit = steps.get("feed_rewards_to_bandit") or {}
    apoptosis = steps.get("check_apoptosis") or {}
    vaccines = steps.get("register_failed_template") or {}
    dna = steps.get("run_weekend_dna_mutation_cycle") or {}

    if isinstance(bandit, str) or isinstance(dna, str):
        err_lines = ["⚠️ 자가진화 파이프라인 일부 실패"]
        for key, label in (
            ("feed_rewards_to_bandit", "보상 피드백"),
            ("run_weekend_dna_mutation_cycle", "DNA 변이"),
        ):
            val = steps.get(key)
            if isinstance(val, str) and val.startswith("error:"):
                err_lines.append(f"▪️ {label}: {_esc(val[6:])}")
        if len(err_lines) <= 1:
            return []
        header = (
            "\n━━━━━━━━━━━━━━━━━━━━\n"
            "🧬 <b>[주말 자가진화]</b> <i>(Alpha Mining · Self-Evolution)</i>\n"
        )
        return [header + "\n".join(err_lines) + "\n"]

    removed_total = int(apoptosis.get("removed_total") or 0)
    removed_names = list(apoptosis.get("removed_names") or [])
    vaccines_reg = int(vaccines.get("registered") or apoptosis.get("vaccines_registered") or 0)
    vaccines_fail = int(vaccines.get("failed") or apoptosis.get("vaccines_failed") or 0)
    bandit_updated = int(bandit.get("updated") or 0)

    mutants = list(dna.get("mutants_created") or [])
    elites = list(dna.get("elite_spinoffs") or [])
    mut_logs = list(dna.get("logs") or [])

    if (
        removed_total == 0
        and vaccines_reg == 0
        and not mutants
        and not elites
        and bandit_updated == 0
        and not mut_logs
    ):
        return []

    started = _esc(pipeline_result.get("started_at") or "")
    ok_flag = "✅" if pipeline_result.get("ok") else "⚠️"

    lines: list[str] = [
        f"\n━━━━━━━━━━━━━━━━━━━━",
        f"🧬 <b>[주말 자가진화]</b> <i>(Alpha Mining · Self-Evolution)</i>",
        f"{ok_flag} 완료 {started}",
        "",
        f"📊 <b>LinUCB 보상 피드백</b>: {bandit_updated}건 갱신",
        f"💀 <b>세포 자멸사 (Apoptosis)</b>: {removed_total}개 퇴출",
    ]
    if removed_names:
        lines.append(f"   └ {_truncate_names(removed_names)}")
    lines.append(
        f"🦠 <b>면역 백신 등록</b>: {vaccines_reg}건"
        + (f" (미등록 {vaccines_fail})" if vaccines_fail else "")
    )

    by_market = apoptosis.get("by_market") or {}
    if isinstance(by_market, dict):
        for mk in sorted(by_market.keys()):
            block = by_market.get(mk) or {}
            if not isinstance(block, dict):
                continue
            mk_removed = block.get("removed") or []
            mk_vac = block.get("vaccines_registered", 0)
            if mk_removed or mk_vac:
                lines.append(
                    f"   ▪️ { _esc(mk) }: 퇴출 {len(mk_removed)} · 백신 {mk_vac}"
                )

    lines.extend(["", f"🧬 <b>신규 돌연변이 DNA</b>: {len(mutants)}개"])
    if mutants:
        lines.append(f"   └ {_truncate_names(mutants)}")

    lines.extend([f"🌟 <b>엘리트 스핀오프 DNA</b>: {len(elites)}개"])
    if elites:
        lines.append(f"   └ {_truncate_names(elites)}")

    if mut_logs:
        lines.append("")
        lines.append("<b>로그 하이라이트</b>")
        for log_line in mut_logs[:6]:
            lines.append(f"▪️ {_esc(log_line)}")
        if len(mut_logs) > 6:
            lines.append(f"▪️ …(+{len(mut_logs) - 6} lines)")

    body = "\n".join(lines) + "\n"
    if len(body) <= 3800:
        return [body]

    # 긴 본문은 헤더 + 상세 2통 분할
    summary = "\n".join(lines[:12]) + "\n"
    detail_lines = lines[12:]
    return [
        summary,
        "\n━━━━━━━━━━━━━━━━━━━━\n"
        "🧬 <b>[주말 자가진화 · 계속]</b>\n"
        + "\n".join(detail_lines)
        + "\n",
    ]


def build_weekend_self_evolution_digest_html(
    pipeline_result: dict[str, Any],
) -> str:
    msgs = build_weekend_self_evolution_digest_messages(pipeline_result)
    return msgs[0] if msgs else ""


def send_weekend_self_evolution_digest(
    pipeline_result: dict[str, Any],
    *,
    send_fn: Optional[Callable[[str], Any]] = None,
) -> bool:
    """주말 자가진화 브리핑 텔레그램 송출. send_fn 미지정 시 system_auto_pilot 사용."""
    messages = build_weekend_self_evolution_digest_messages(pipeline_result)
    if not messages:
        return False

    if send_fn is None:
        try:
            from system_auto_pilot import send_telegram_report as send_fn  # type: ignore
        except Exception:
            send_fn = None

    if send_fn is None:
        return False

    sent_any = False
    for msg in messages:
        try:
            if send_fn(msg):
                sent_any = True
        except TypeError:
            send_fn(msg)
            sent_any = True
        except Exception:
            continue
    return sent_any

#!/usr/bin/env python3
"""META_CHANGELOG · META_GROUP_KELLY_MULT 전체를 Markdown으로 덤프 (텔레그램 잘림 없음)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> int:
    from meta_governor import load_meta_governor_state
    from tuning_digest_formatter import format_group_kelly_mult_diff

    meta = load_meta_governor_state()
    log = meta.get("META_CHANGELOG") or []
    mult_now = meta.get("META_GROUP_KELLY_MULT") or {}

    lines = [
        "# 진화·튜닝 전체 덤프 (MetaGovernor)",
        "",
        f"- 생성: 서버 `meta_governor_state` 기준",
        f"- META_CHANGELOG 항목 수: {len(log) if isinstance(log, list) else 0}",
        f"- 현재 META_GROUP_KELLY_MULT 그룹 수: {len(mult_now) if isinstance(mult_now, dict) else 0}",
        "",
        "---",
        "",
        "## 1. 현재 그룹별 켈리 배율 (META_GROUP_KELLY_MULT)",
        "",
    ]

    if isinstance(mult_now, dict) and mult_now:
        for k in sorted(mult_now.keys(), key=str):
            try:
                v = float(mult_now[k])
            except (TypeError, ValueError):
                v = 1.0
            tag = ""
            if v < 1.0:
                tag = " ← 방어(비중 축소)"
            elif v > 1.0:
                tag = " ← 가속(비중 확대)"
            lines.append(f"- **{k}**: `{v:.4f}`{tag}")
    else:
        lines.append("_비어 있음 (전 그룹 1.0 취급)_")

    lines.extend(["", "---", "", "## 2. META_CHANGELOG (최근→과거)", ""])

    if not isinstance(log, list) or not log:
        lines.append("_CHANGELOG 없음_")
    else:
        for i, entry in enumerate(reversed(log[-20:]), 1):
            if not isinstance(entry, dict):
                continue
            key = entry.get("key", "?")
            reason = entry.get("reason", "")
            at = str(entry.get("at", ""))[:19]
            lines.append(f"### 2.{i} `{key}` ({reason}) — {at}")
            lines.append("")
            if key == "META_GROUP_KELLY_MULT":
                body = format_group_kelly_mult_diff(
                    entry.get("old"),
                    entry.get("new"),
                    max_show=9999,
                )
                for b in body:
                    lines.append(b.replace("<b>", "**").replace("</b>", "**").replace("<i>", "_").replace("</i>", "_").replace("<code>", "`").replace("</code>", "`"))
            else:
                lines.append(f"- old: `{entry.get('old')}`")
                lines.append(f"- new: `{entry.get('new')}`")
            lines.append("")

    lines.extend([
        "---",
        "",
        "## 3. Treasury 헬스 스냅샷 (META_STRATEGY_HEALTH)",
        "",
    ])
    health = meta.get("META_STRATEGY_HEALTH") or {}
    if isinstance(health, dict):
        meta_block = health.get("__meta__")
        if isinstance(meta_block, dict):
            lines.append(f"- lookback_days: `{meta_block.get('window_days_kst')}`")
            lines.append(f"- cutoff: `{meta_block.get('cutoff_exit_date_gte')}`")
            lines.append(f"- n_rows: `{meta_block.get('n_rows')}`")
            lines.append("")
        for hk in sorted(k for k in health if k != "__meta__"):
            hv = health.get(hk)
            if not isinstance(hv, dict):
                continue
            lines.append(
                f"- **{hk}**: n={hv.get('n')} WR={hv.get('rolling_wr')} "
                f"PF={hv.get('rolling_pf')} mult=`{hv.get('mult')}` reason=`{hv.get('reason')}`"
            )
    else:
        lines.append("_없음_")

    out_path = _ROOT / "docs" / "진화튜닝_런타임_전체덤프.md"
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())

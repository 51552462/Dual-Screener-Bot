"""tuning_digest_formatter — 유니코드·1.0 숨김."""
from tuning_digest_formatter import (
    format_group_kelly_mult_diff,
    format_meta_changelog_telegram_flat,
)


def test_group_kelly_hides_neutral_shows_delta():
    old = {"alpha": 1.0, "beta": 1.0}
    new = {"alpha": 1.0, "beta": 0.85, "gamma": 1.0}
    lines = format_group_kelly_mult_diff(old, new)
    text = "\n".join(lines)
    assert "beta" in text or "0.85" in text
    assert "\\u" not in text
    assert "alpha" not in text or "1.00 ➔ 1.00" not in text


def test_legacy_json_string_changelog():
    old_s = '{"\\ud83d\\udd25\\uc8fc\\ub3c4": 1.0, "grp": 0.9}'
    new_s = '{"\\ud83d\\udd25\\uc8fc\\ub3c4": 0.85, "grp": 0.9}'
    lines = format_group_kelly_mult_diff(old_s, new_s)
    joined = "\n".join(lines)
    assert "\\ud83" not in joined


def test_meta_changelog_dict_entry():
    meta = {
        "META_CHANGELOG": [
            {
                "key": "META_GROUP_KELLY_MULT",
                "old": {"로직A": 1.0, "로직B": 1.0},
                "new": {"로직A": 0.85, "로직B": 1.0},
                "reason": "treasury_groups",
                "at": "2026-05-26T00:00:00",
            }
        ]
    }
    lines = format_meta_changelog_telegram_flat(meta)
    text = "\n".join(lines)
    assert "로직A" in text
    assert "0.85" in text
    assert "\\u" not in text


def test_group_kelly_mult_splits_pages_no_ellipsis():
    old = {f"grp_{i}": 1.07 for i in range(20)}
    new = {f"grp_{i}": 1.0 for i in range(20)}
    from tuning_digest_formatter import format_group_kelly_mult_diff_pages

    pages = format_group_kelly_mult_diff_pages(old, new, page_size=15)
    joined = "\n".join("\n".join(p) for p in pages)
    assert "외 " not in joined
    assert "grp_0" in joined and "grp_19" in joined
    assert len(pages) == 2

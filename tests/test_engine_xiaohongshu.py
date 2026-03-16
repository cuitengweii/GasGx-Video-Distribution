from __future__ import annotations

from cybercar import engine


def test_trim_text_by_utf16_units_counts_emoji_like_browser() -> None:
    text = "1234567890123456789🎯"

    trimmed = engine._trim_text_by_utf16_units(text, 20)

    assert trimmed == "1234567890123456789"
    assert engine._utf16_code_unit_length(trimmed) == 19


def test_build_xiaohongshu_title_from_caption_limits_utf16_length() -> None:
    caption = "你看到的下一辆赛博卡车有可能就是你的票💯🎯\nThe next Cybertruck you see"

    title = engine._build_xiaohongshu_title_from_caption(caption, limit=20)

    assert title == "你看到的下一辆赛博卡车有可能就是你的票"
    assert engine._utf16_code_unit_length(title) <= 20

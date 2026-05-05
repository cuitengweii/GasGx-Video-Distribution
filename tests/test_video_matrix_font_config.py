from __future__ import annotations

from pathlib import Path

from gasgx_distribution.video_matrix import font_config


def test_build_font_candidates_uses_configured_dirs(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "defaults.json"
    config_path.write_text('{"font_dirs": ["C:/Fonts", "/opt/fonts"]}', encoding="utf-8")
    monkeypatch.setattr(font_config, "DEFAULTS_PATH", config_path)
    font_config.load_font_dirs.cache_clear()

    candidates = font_config.build_font_candidates()

    assert candidates[0] == Path("C:/Fonts") / "msyh.ttc"
    assert Path("/opt/fonts") / "arial.ttf" in candidates


def test_build_font_candidates_uses_env_override(monkeypatch) -> None:
    monkeypatch.setenv("FONT_DIRS", "D:/fonts;E:/more-fonts")
    font_config.load_font_dirs.cache_clear()

    dirs = font_config.load_font_dirs()

    assert dirs == (Path("D:/fonts"), Path("E:/more-fonts"))

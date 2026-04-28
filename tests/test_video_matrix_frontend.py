from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_video_matrix_bgm_uses_local_library_with_visible_directory_hint() -> None:
    app = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_app.js").read_text(encoding="utf-8")
    css = (ROOT / "src" / "gasgx_distribution" / "web" / "static" / "video_matrix_styles.css").read_text(encoding="utf-8")

    assert "本地背景音乐" in app
    assert "暂无 MP3 文件" in app
    assert "local_bgm_dir" in app
    assert 'bgm_source: "Local library"' in app
    assert "bgmUpload" not in app
    assert "上传文件" not in app
    assert "bgmLibraryHint" in app
    assert '$("bgmLibrary").disabled = localBgm.length === 0' in app
    assert ".help-dot" in css
    assert 'classList.toggle("error-message", isError)' in app
    assert ".job-status-card.error" in css
    assert "#jobMessage.error-message" in css

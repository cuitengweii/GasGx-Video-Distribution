from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient

import gasgx_distribution.video_matrix_api as video_matrix_api
from gasgx_distribution.web import create_app


def test_video_matrix_api_state_and_preview() -> None:
    client = TestClient(create_app())

    state = client.get("/api/video-matrix/state")
    assert state.status_code == 200
    payload = state.json()
    assert payload["cover_templates"]
    assert "industrial_engine_hook" in payload["cover_templates"]
    assert payload["local_bgm_dir"].endswith("runtime\\video_matrix\\bgm") or payload["local_bgm_dir"].endswith("runtime/video_matrix/bgm")
    labels = [item["label"] for item in payload["settings"]["material_categories"]]
    for expected in [
        "矿机部分",
        "集装箱部分",
        "发电机部分",
        "各显示器部分",
        "传感器部分",
        "施工过程",
        "测试动线",
        "工厂全貌",
    ]:
        assert expected in labels
    assert "category_H" in payload["source_dirs"]
    assert "category_H" in payload["category_counts"]
    assert payload["settings"]["composition_sequence"][0]["category_id"] == "category_A"
    assert payload["settings"]["composition_sequence"][1]["duration"] == 3.4
    assert payload["settings"]["video_duration_max"] == 12.0
    assert payload["settings"]["beat_detection"]["fallback_spacing"] == 0.48
    assert payload["settings"]["max_variant_attempts"] == 20
    assert payload["settings"]["variant_history_enabled"] is True

    preview = client.post(
        "/api/video-matrix/cover-preview",
        json={
            "template_id": "industrial_engine_hook",
            "headline": "Gas Engines That Turn Field Gas Into Power",
            "subhead": "Generator sets for onsite Bitcoin and industrial load",
            "cta": "Learn more at gasgx.com/roi",
            "hud_text": "Gas Engine -> Generator Set -> Power Output",
        },
    )
    assert preview.status_code == 200
    assert preview.json()["data_url"].startswith("data:image/png;base64,")


def test_video_matrix_generate_passes_composition_sequence(monkeypatch, tmp_path) -> None:
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["test-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request = {
        "output_count": 1,
        "output_options": ["mp4"],
        "copy_language": "zh",
        "source_mode": "Category folders",
        "video_duration_max": 24,
        "composition_sequence": [{"category_id": "category_D", "duration": 2.2}],
    }

    video_matrix_api._run_generate_job("test-job", request, tmp_path / "bgm.mp3", None)

    assert captured["composition_sequence"] == [{"category_id": "category_D", "duration": 2.2}]
    assert captured["settings"].video_duration_max == 24
    assert isinstance(captured["existing_signatures"], set)


def test_video_matrix_generate_persists_full_ui_state(monkeypatch, tmp_path) -> None:
    def fake_run_pipeline(**kwargs):
        return []

    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["persist-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request = {
        "output_count": 2,
        "output_options": ["mp4"],
        "output_root": str(tmp_path / "exports"),
        "copy_language": "zh",
        "source_mode": "Category folders",
        "recent_limits": {"category_A": 12},
        "active_category_ids": ["category_A"],
        "video_duration_max": 18,
        "transcript_text": "field gas project notes",
    }

    video_matrix_api._run_generate_job("persist-job", request, tmp_path / "bgm.mp3", None)

    state = video_matrix_api.load_ui_state(tmp_path / "ui_state.json")
    assert state["output_root"].endswith("exports")
    assert state["recent_limits"] == {"category_A": 12}
    assert state["video_duration_max"] == 18
    assert state["transcript_text"] == "field gas project notes"


def test_video_matrix_generate_falls_back_to_active_categories(monkeypatch, tmp_path) -> None:
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["active-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request = {
        "output_count": 1,
        "output_options": ["mp4"],
        "copy_language": "zh",
        "source_mode": "Category folders",
        "active_category_ids": ["category_A", "category_D", "category_E"],
    }

    video_matrix_api._run_generate_job("active-job", request, tmp_path / "bgm.mp3", None)

    assert captured["composition_sequence"] == [
        {"category_id": "category_A", "duration": 1.5},
        {"category_id": "category_D", "duration": 2.0},
        {"category_id": "category_E", "duration": 2.0},
    ]


def test_video_matrix_static_entry_exists() -> None:
    client = TestClient(create_app())

    response = client.get("/")

    assert response.status_code == 200
    html = response.text
    assert 'data-view="video-matrix"' in html
    assert 'class="video-matrix-frame"' in html
    assert 'src="/static/video_matrix.html?embed=1"' in html

    script = client.get("/static/app.js")
    assert script.status_code == 200
    assert 'src="/static/video_matrix.html?embed=1"' in script.text


def test_video_matrix_full_clone_page_exists() -> None:
    client = TestClient(create_app())

    response = client.get("/static/video_matrix.html")

    assert response.status_code == 200
    html = response.text
    assert "GasGx 短视频矩阵批量生成工具" in html
    assert "/static/video_matrix_app.js" in html
    assert "/static/video_matrix_styles.css" in html


def test_video_matrix_can_add_named_material_category(monkeypatch, tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "defaults.json"
    config_path.write_text(video_matrix_api.CONFIG_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setattr(video_matrix_api, "CONFIG_PATH", config_path)

    client = TestClient(create_app())

    response = client.post("/api/video-matrix/material-categories", json={"label": "泵站细节"})

    assert response.status_code == 200
    state = client.get("/api/video-matrix/state").json()
    assert any(item["label"] == "泵站细节" for item in state["settings"]["material_categories"])
    assert "category_custom_1" in state["source_dirs"]


def test_video_matrix_uses_random_local_bgm_when_no_track_is_selected(monkeypatch, tmp_path) -> None:
    bgm_dir = tmp_path / "bgm"
    bgm_dir.mkdir()
    first = bgm_dir / "one.mp3"
    second = bgm_dir / "two.mp3"
    first.write_bytes(b"one")
    second.write_bytes(b"two")
    monkeypatch.setattr(video_matrix_api, "BGM_DIR", bgm_dir)
    monkeypatch.setattr(video_matrix_api.random, "choice", lambda items: items[-1])

    selected = asyncio.run(
        video_matrix_api._resolve_bgm_path(
            {"bgm_source": "Local library", "bgm_library_id": ""},
            tmp_path,
            None,
        )
    )

    assert selected == second.resolve()


def test_video_matrix_local_bgm_file_endpoint(monkeypatch, tmp_path) -> None:
    bgm_dir = tmp_path / "bgm"
    bgm_dir.mkdir()
    (bgm_dir / "track.mp3").write_bytes(b"audio")
    monkeypatch.setattr(video_matrix_api, "BGM_DIR", bgm_dir)
    client = TestClient(create_app())

    response = client.get("/api/video-matrix/bgm/track.mp3")

    assert response.status_code == 200
    assert response.content == b"audio"


def test_video_matrix_pixabay_industry_tracks_endpoint() -> None:
    client = TestClient(create_app())

    response = client.get("/api/video-matrix/pixabay/industry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_url"] == "https://pixabay.com/music/search/industry/"
    assert len(payload["tracks"]) == 10
    assert payload["tracks"][0]["title"] == "Corporate Industry"
    assert payload["tracks"][0]["source_url"].startswith("https://pixabay.com/music/")

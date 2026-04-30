from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.testclient import TestClient

import gasgx_distribution.video_matrix_api as video_matrix_api
from gasgx_distribution.video_matrix.models import ClipMetadata, RenderedAsset, SegmentPlan, VideoVariant
from gasgx_distribution.web import create_app


class FakeHistorySupabase:
    def __init__(self) -> None:
        self.tables: dict[str, list[dict]] = {}
        self.select_calls: list[tuple[str, dict[str, str]]] = []

    def insert(self, table: str, payload: dict) -> dict:
        row = dict(payload)
        row.setdefault("id", len(self.tables.setdefault(table, [])) + 1)
        self.tables.setdefault(table, []).append(row)
        return row

    def update(self, table: str, payload: dict, *, filters: dict) -> dict:
        self.tables.setdefault(table, []).append({"update": payload, "filters": filters})
        return payload

    def select_where(self, table: str, *, params: dict[str, str], order: str = "") -> list[dict]:
        self.select_calls.append((table, params))
        return list(self.tables.get(table, []))

    def select_one(self, table: str, *, filters: dict) -> dict | None:
        for row in self.tables.get(table, []):
            if all(row.get(key) == value for key, value in filters.items()):
                return row
        return None


def _fake_rendered_asset(signature: str = "sig-1") -> RenderedAsset:
    clip = ClipMetadata(
        clip_id="clip-1",
        source_path=Path("source.mp4"),
        normalized_path=Path("normalized.mp4"),
        category="category_A",
        duration=5,
        width=1080,
        height=1920,
        fps=60,
        brightness_score=1,
        contrast_score=1,
        tags=["category_A"],
    )
    segment = SegmentPlan(category="category_A", clip=clip, start_time=0.5, duration=1.0, index=0)
    variant = VideoVariant(
        sequence_number=1,
        title="Title",
        slogan="Slogan",
        hud_lines=["HUD"],
        lut_strength=1,
        zoom=1,
        mirror=False,
        x_offset=0,
        y_offset=0,
        segments=[segment],
        signature=signature,
    )
    return RenderedAsset(variant, Path("video.mp4"), Path("cover.png"), Path("copy.txt"), Path("manifest.json"))


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


def test_video_matrix_state_falls_back_when_supabase_settings_are_unavailable(monkeypatch) -> None:
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")
    monkeypatch.delenv("BRAND_SUPABASE_SERVICE_KEY", raising=False)
    monkeypatch.delenv("BRAND_SUPABASE_URL", raising=False)

    client = TestClient(create_app())

    state = client.get("/api/video-matrix/state")

    assert state.status_code == 200
    payload = state.json()
    assert payload["templates"]
    assert payload["cover_templates"]


def test_video_matrix_state_uses_local_templates_when_remote_state_is_empty(monkeypatch) -> None:
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "supabase")
    monkeypatch.setattr(video_matrix_api.service, "_app_setting", lambda key, default=None: {"templates": {}, "cover_templates": {}})

    client = TestClient(create_app())

    state = client.get("/api/video-matrix/state")

    assert state.status_code == 200
    payload = state.json()
    assert "impact_hud" in payload["templates"]
    assert "industrial_engine_hook" in payload["cover_templates"]


def test_video_matrix_template_save_persists_to_supabase_state(monkeypatch) -> None:
    saved = {}

    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api, "_video_matrix_app_setting", lambda default=None: {"templates": {}, "cover_templates": {}})
    monkeypatch.setattr(video_matrix_api.service, "_save_app_setting", lambda key, payload: saved.update({"key": key, "payload": payload}))

    client = TestClient(create_app())
    response = client.post("/api/video-matrix/templates/impact_hud", json={"name": "Saved Name"})

    assert response.status_code == 200
    assert response.json()["storage"] == "database"
    assert saved["key"] == "video_matrix_state"
    assert saved["payload"]["templates"]["impact_hud"]["name"] == "Saved Name"


def test_video_matrix_template_save_reports_database_failure(monkeypatch) -> None:
    def fail_save(_key, _payload):
        raise video_matrix_api.service.SupabaseError("offline")

    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api, "_video_matrix_app_setting", lambda default=None: {"templates": {}, "cover_templates": {}})
    monkeypatch.setattr(video_matrix_api.service, "_save_app_setting", fail_save)

    client = TestClient(create_app())
    response = client.post("/api/video-matrix/templates/impact_hud", json={"name": "Lost Name"})

    assert response.status_code == 503
    assert "Failed to save video matrix state to database" in response.text


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


def test_video_matrix_generation_history_is_loaded_from_structured_tables(monkeypatch) -> None:
    fake = FakeHistorySupabase()
    fake.tables["video_matrix_generation_assets"] = [{"signature": "sig-old"}]
    fake.tables["video_matrix_generation_segments"] = [{"clip_id": "clip-old", "start_time": 0.5, "duration": 1.0}]
    fake.tables["video_matrix_generation_runs"] = [{"bgm_filename": "used.mp3"}]
    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api.service, "_brand_supabase", lambda: fake)

    history = video_matrix_api._load_generation_history(5000)

    assert history["signatures"] == {"sig-old"}
    assert history["clip_ids"] == {"clip-old"}
    assert history["segment_keys"] == {"clip-old:0.5:1.0"}
    assert history["bgm_names"] == {"used.mp3"}
    assert all(params["limit"] == "5000" for _table, params in fake.select_calls)


def test_video_matrix_generation_history_is_saved_after_success(monkeypatch, tmp_path) -> None:
    fake = FakeHistorySupabase()
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return [_fake_rendered_asset()]

    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api.service, "_brand_supabase", lambda: fake)
    monkeypatch.setattr(video_matrix_api, "_video_matrix_app_setting", lambda default=None: {})
    monkeypatch.setattr(video_matrix_api, "_save_video_matrix_app_setting", lambda payload: True)
    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    video_matrix_api._jobs["history-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}

    video_matrix_api._run_generate_job(
        "history-job",
        {
            "output_count": 1,
            "output_options": ["mp4"],
            "copy_language": "zh",
            "source_mode": "Category folders",
            "composition_sequence": [{"category_id": "category_A", "duration": 1.0}],
        },
        tmp_path / "fresh.mp3",
        None,
    )

    assert fake.tables["video_matrix_generation_runs"][0]["bgm_filename"] == "fresh.mp3"
    assert fake.tables["video_matrix_generation_assets"][0]["signature"] == "sig-1"
    assert fake.tables["video_matrix_generation_segments"][0]["clip_id"] == "clip-1"
    assert captured["recent_clip_ids"] == set()


def test_video_matrix_generation_failure_does_not_write_history(monkeypatch, tmp_path) -> None:
    fake = FakeHistorySupabase()

    def fail_pipeline(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api.service, "_brand_supabase", lambda: fake)
    monkeypatch.setattr(video_matrix_api, "_video_matrix_app_setting", lambda default=None: {})
    monkeypatch.setattr(video_matrix_api, "_save_video_matrix_app_setting", lambda payload: True)
    monkeypatch.setattr(video_matrix_api, "run_pipeline", fail_pipeline)
    video_matrix_api._jobs["failed-history-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}

    video_matrix_api._run_generate_job("failed-history-job", {"output_count": 1, "output_options": ["mp4"]}, tmp_path / "bgm.mp3", None)

    assert "video_matrix_generation_runs" not in fake.tables


def test_video_matrix_random_bgm_prefers_unused_local_track(monkeypatch, tmp_path) -> None:
    bgm_dir = tmp_path / "bgm"
    bgm_dir.mkdir()
    used = bgm_dir / "used.mp3"
    fresh = bgm_dir / "fresh.mp3"
    used.write_bytes(b"used")
    fresh.write_bytes(b"fresh")
    monkeypatch.setattr(video_matrix_api, "BGM_DIR", bgm_dir)
    monkeypatch.setattr(video_matrix_api, "_recent_bgm_names", lambda: {"used.mp3"})
    monkeypatch.setattr(video_matrix_api.random, "choice", lambda items: items[0])

    selected = asyncio.run(video_matrix_api._resolve_bgm_path({"bgm_source": "Local library", "bgm_library_id": ""}, tmp_path, None))

    assert selected == fresh.resolve()


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


def test_video_matrix_model_images_endpoint(monkeypatch, tmp_path) -> None:
    image_dir = tmp_path / "modelimg"
    image_dir.mkdir()
    (image_dir / "sample.png").write_bytes(b"png")
    (image_dir / "skip.txt").write_text("no", encoding="utf-8")
    monkeypatch.setattr(video_matrix_api, "MODEL_IMAGE_DIR", image_dir)
    client = TestClient(create_app())

    response = client.get("/api/video-matrix/model-images")

    assert response.status_code == 200
    payload = response.json()
    assert payload["directory"] == str(image_dir)
    assert payload["images"] == [{"name": "sample.png", "url": "/api/video-matrix/model-images/sample.png"}]

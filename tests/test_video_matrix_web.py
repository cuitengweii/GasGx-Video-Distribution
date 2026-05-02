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


class FailingJobSupabase:
    def update(self, table: str, payload: dict, *, filters: dict) -> dict:
        raise RuntimeError("supabase timeout")

    def select_one(self, table: str, *, filters: dict) -> dict | None:
        raise RuntimeError("supabase timeout")


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
            "cta": "",
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


def test_video_matrix_job_status_prefers_local_active_job_when_supabase_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api.service, "_brand_supabase", lambda: FailingJobSupabase())
    video_matrix_api._jobs["local-complete-job"] = {
        "status": "complete",
        "progress": 1,
        "message": "Completed 3 exports",
        "assets": [{"video_path": "runtime/materials/videos/a.mp4"}],
        "error": "",
    }

    try:
        response = video_matrix_api.job_status("local-complete-job")
    finally:
        video_matrix_api._jobs.pop("local-complete-job", None)

    assert response["status"] == "complete"
    assert response["progress"] == 1
    assert response["assets"][0]["video_path"].endswith("a.mp4")


def test_video_matrix_progress_sync_failure_does_not_abort_render(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api.service, "_brand_supabase", lambda: FailingJobSupabase())
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")

    def fake_run_pipeline(**kwargs):
        kwargs["progress_callback"]("ingestion", 0.05, "Scanning source assets")
        return [_fake_rendered_asset()]

    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    video_matrix_api._jobs["sync-failure-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}

    try:
        video_matrix_api._run_generate_job(
            "sync-failure-job",
            {"output_count": 1, "output_options": ["mp4"], "copy_language": "zh", "source_mode": "Category folders"},
            tmp_path / "bgm.mp3",
            None,
        )
        response = video_matrix_api._jobs["sync-failure-job"]
    finally:
        video_matrix_api._jobs.pop("sync-failure-job", None)

    assert response["status"] == "complete"
    assert response["progress"] == 1.0
    assert response["assets"][0]["video_path"] == "video.mp4"


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
        "video_duration_min": 8,
        "video_duration_max": 24,
        "target_fps": 30,
        "composition_sequence": [{"category_id": "category_D", "duration": 2.2}],
    }

    video_matrix_api._run_generate_job("test-job", request, tmp_path / "bgm.mp3", None)

    assert captured["composition_sequence"] == [{"category_id": "category_D", "duration": 2.2}]
    assert captured["settings"].video_duration_min == 8
    assert captured["settings"].video_duration_max == 24
    assert captured["settings"].target_fps == 30
    assert isinstance(captured["existing_signatures"], set)


def test_video_matrix_generate_prefers_current_request_template_snapshot(monkeypatch, tmp_path) -> None:
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["snapshot-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request_template = {
        "name": "Current Screen Template",
        "show_slogan": True,
        "slogan_bg_opacity": 0,
        "slogan_text_effect": "none",
    }
    request_cover = {"name": "Current Cover", "brand": "GasGx"}

    video_matrix_api._run_generate_job(
        "snapshot-job",
        {
            "output_count": 1,
            "output_options": ["mp4"],
            "copy_language": "zh",
            "source_mode": "Category folders",
            "template_id": "impact_hud",
            "cover_template_id": "industrial_engine_hook",
            "template_config": request_template,
            "cover_template_config": request_cover,
        },
        tmp_path / "bgm.mp3",
        None,
    )

    assert captured["template_config"] == request_template
    assert captured["cover_template_config"]["name"] == "Current Cover"
    assert captured["cover_template_config"]["brand"] == "GasGx"


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
        "video_duration_min": 7,
        "video_duration_max": 18,
        "target_fps": 30,
        "bgm_library_id": "selected.mp3",
    }

    video_matrix_api._run_generate_job("persist-job", request, tmp_path / "bgm.mp3", None)

    state = video_matrix_api.load_ui_state(tmp_path / "ui_state.json")
    assert state["output_root"].endswith("exports")
    assert state["recent_limits"] == {"category_A": 12}
    assert state["video_duration_min"] == 7
    assert state["video_duration_max"] == 18
    assert state["target_fps"] == 30
    assert state["bgm_library_id"] == "selected.mp3"
    assert "transcript_text" not in state


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


def test_video_matrix_selected_bgm_uses_requested_local_track(monkeypatch, tmp_path) -> None:
    bgm_dir = tmp_path / "bgm"
    bgm_dir.mkdir()
    selected_file = bgm_dir / "selected.mp3"
    selected_file.write_bytes(b"selected")
    (bgm_dir / "other.mp3").write_bytes(b"other")
    monkeypatch.setattr(video_matrix_api, "BGM_DIR", bgm_dir)
    monkeypatch.setattr(video_matrix_api, "_recent_bgm_names", lambda: set())

    selected = asyncio.run(video_matrix_api._resolve_bgm_path({"bgm_source": "Local library", "bgm_library_id": "selected.mp3"}, tmp_path, None))

    assert selected == selected_file.resolve()


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
    assert "category_custom_1" not in state["source_dirs"]
    assert "category_I" in state["source_dirs"]
    assert "category_J" in state["source_dirs"]
    assert "category_K" in state["source_dirs"]


def test_video_matrix_state_migrates_legacy_custom_category_refs(monkeypatch) -> None:
    stored = {
        "settings": {
            "material_categories": [
                {"id": "category_I", "label": "测试"},
                {"id": "category_J", "label": "电控"},
            ],
            "recent_limits": {"category_custom_1": 6, "category_custom_2": 5},
            "composition_sequence": [{"category_id": "category_custom_1", "duration": 2.0}],
        },
        "ui_state": {
            "active_category_ids": ["category_custom_1", "category_custom_2"],
            "recent_limits": {"category_custom_1": 6},
            "composition_sequence": [{"category_id": "category_custom_2", "duration": 1.5}],
        },
    }
    saved = {}
    monkeypatch.setattr(video_matrix_api.service, "brand_database_backend", lambda: "supabase")
    monkeypatch.setattr(video_matrix_api, "_video_matrix_app_setting", lambda default=None: stored)
    monkeypatch.setattr(video_matrix_api, "_save_video_matrix_app_setting", lambda payload: saved.update(payload) or True)

    client = TestClient(create_app())
    response = client.get("/api/video-matrix/state")

    assert response.status_code == 200
    payload = response.json()
    assert "category_custom_1" not in str(payload["ui_state"])
    assert "category_custom_2" not in str(payload["ui_state"])
    assert payload["ui_state"]["active_category_ids"] == ["category_I", "category_J"]
    assert payload["ui_state"]["composition_sequence"] == [{"category_id": "category_J", "duration": 1.5}]
    assert payload["ui_state"]["recent_limits"] == {"category_I": 6}
    assert saved["ui_state"]["active_category_ids"] == ["category_I", "category_J"]


def test_video_matrix_generate_normalizes_legacy_custom_category_refs(monkeypatch, tmp_path) -> None:
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["legacy-category-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request = {
        "output_count": 1,
        "output_options": ["mp4"],
        "source_mode": "Category folders",
        "active_category_ids": ["category_custom_1", "category_custom_2"],
        "recent_limits": {"category_custom_1": 6, "category_custom_2": 5},
        "composition_sequence": [{"category_id": "category_custom_2", "duration": 1.5}],
    }

    video_matrix_api._run_generate_job("legacy-category-job", request, tmp_path / "bgm.mp3", None)

    assert captured["active_category_ids"] == ["category_I", "category_J"]
    assert captured["recent_limits"] == {"category_I": 6, "category_J": 5}
    assert captured["composition_sequence"] == [{"category_id": "category_J", "duration": 1.5}]
    state = video_matrix_api.load_ui_state(tmp_path / "ui_state.json")
    assert "category_custom" not in str(state)


def test_video_matrix_can_rename_material_category(monkeypatch, tmp_path) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_path = config_dir / "defaults.json"
    config_path.write_text(video_matrix_api.CONFIG_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.setattr(video_matrix_api, "CONFIG_PATH", config_path)

    client = TestClient(create_app())

    response = client.post("/api/video-matrix/material-categories/category_A", json={"label": "矿机改名"})

    assert response.status_code == 200
    state = client.get("/api/video-matrix/state").json()
    categories = state["settings"]["material_categories"]
    assert any(item["id"] == "category_A" and item["label"] == "矿机改名" for item in categories)


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


def test_video_matrix_state_lists_ending_templates(monkeypatch, tmp_path) -> None:
    ending_dir = tmp_path / "ending_template"
    ending_dir.mkdir()
    (ending_dir / "tail.mp4").write_bytes(b"video")
    (ending_dir / "tail.png").write_bytes(b"png")
    (ending_dir / "skip.txt").write_text("no", encoding="utf-8")
    monkeypatch.setattr(video_matrix_api, "ENDING_TEMPLATE_DIR", ending_dir)
    client = TestClient(create_app())

    response = client.get("/api/video-matrix/state")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ending_template_dir"] == str(ending_dir)
    names = {item["name"] for item in payload["ending_templates"]}
    assert names == {"tail.mp4", "tail.png"}
    assert {item["type"] for item in payload["ending_templates"]} == {"video", "image"}
    assert any(item["url"] == "/api/video-matrix/ending-templates/tail.mp4" for item in payload["ending_templates"])

    file_response = client.get("/api/video-matrix/ending-templates/tail.mp4")
    assert file_response.status_code == 200
    assert file_response.content == b"video"


def test_video_matrix_generate_passes_specific_ending_template(monkeypatch, tmp_path) -> None:
    ending_dir = tmp_path / "ending_template"
    ending_dir.mkdir()
    selected = ending_dir / "brand_tail.mp4"
    selected.write_bytes(b"video")
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(video_matrix_api, "ENDING_TEMPLATE_DIR", ending_dir)
    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["ending-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request = {
        "output_count": 1,
        "output_options": ["mp4"],
        "copy_language": "zh",
        "source_mode": "Category folders",
        "ending_template_mode": "specific",
        "ending_template_id": selected.name,
    }

    video_matrix_api._run_generate_job("ending-job", request, tmp_path / "bgm.mp3", None)

    assert video_matrix_api._jobs["ending-job"]["status"] == "complete"
    assert captured["ending_template_path"] == selected.resolve()
    state = video_matrix_api.load_ui_state(tmp_path / "ui_state.json")
    assert state["ending_template_mode"] == "specific"
    assert state["ending_template_id"] == selected.name


def test_video_matrix_generate_random_ending_template_uses_checked_names(monkeypatch, tmp_path) -> None:
    ending_dir = tmp_path / "ending_template"
    ending_dir.mkdir()
    selected = ending_dir / "selected_tail.mp4"
    skipped = ending_dir / "skipped_tail.mp4"
    selected.write_bytes(b"video")
    skipped.write_bytes(b"video")
    captured = {}

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(video_matrix_api, "ENDING_TEMPLATE_DIR", ending_dir)
    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["ending-random-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request = {
        "output_count": 1,
        "output_options": ["mp4"],
        "copy_language": "zh",
        "source_mode": "Category folders",
        "ending_template_mode": "random",
        "ending_template_ids": [selected.name],
    }

    video_matrix_api._run_generate_job("ending-random-job", request, tmp_path / "bgm.mp3", None)

    assert video_matrix_api._jobs["ending-random-job"]["status"] == "complete"
    assert captured["ending_template_path"] == selected.resolve()
    state = video_matrix_api.load_ui_state(tmp_path / "ui_state.json")
    assert state["ending_template_mode"] == "random"
    assert state["ending_template_ids"] == [selected.name]


def test_video_matrix_generate_persists_and_passes_dynamic_ending_cover_template(monkeypatch, tmp_path) -> None:
    captured = {}
    ending_cover = {
        "name": "Tail Card",
        "single_cover_logo_text": "Tail Logo",
        "single_cover_logo_y": 240,
        "mask_mode": "full",
        "mask_opacity": 0.55,
    }

    def fake_run_pipeline(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(video_matrix_api, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(video_matrix_api, "SIGNATURE_HISTORY_PATH", tmp_path / "signature_history.json")
    monkeypatch.setattr(video_matrix_api, "UI_STATE_PATH", tmp_path / "ui_state.json")
    video_matrix_api._jobs["ending-cover-job"] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    request = {
        "output_count": 1,
        "output_options": ["mp4"],
        "copy_language": "zh",
        "source_mode": "Category folders",
        "ending_template_mode": "dynamic",
        "ending_template_dir": str(tmp_path / "ending_template"),
        "ending_template_ids": ["tail-a.mp4", "tail-b.png"],
        "ending_cover_template": ending_cover,
    }

    video_matrix_api._run_generate_job("ending-cover-job", request, tmp_path / "bgm.mp3", None)

    assert video_matrix_api._jobs["ending-cover-job"]["status"] == "complete"
    assert captured["ending_cover_template_config"] == ending_cover
    assert captured["ending_template_path"] is None
    state = video_matrix_api.load_ui_state(tmp_path / "ui_state.json")
    assert state["ending_template_mode"] == "dynamic"
    assert state["ending_template_dir"] == str(tmp_path / "ending_template")
    assert state["ending_template_ids"] == ["tail-a.mp4", "tail-b.png"]
    assert state["ending_cover_template"] == ending_cover


def test_video_matrix_pixabay_industry_tracks_endpoint() -> None:
    client = TestClient(create_app())

    response = client.get("/api/video-matrix/pixabay/industry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_url"] == "https://pixabay.com/music/search/industry/"
    assert payload["query"] == "industry"
    assert "api_status" in payload
    assert len(payload["tracks"]) == 10
    assert payload["tracks"][0]["title"] == "Corporate Industry"
    assert payload["tracks"][0]["source_url"].startswith("https://pixabay.com/music/")
    assert payload["tracks"][0]["audio_url"]

    filtered = client.get("/api/video-matrix/pixabay/industry?q=corporate").json()
    assert filtered["source_url"] == "https://pixabay.com/music/search/corporate/"
    assert filtered["query"] == "corporate"
    assert "api_error" in filtered
    assert filtered["tracks"][0]["title"] == "Corporate Industry"
    assert filtered["tracks"][0]["audio_url"]


def test_video_matrix_mock_bgm_download_writes_library_file(monkeypatch, tmp_path) -> None:
    bgm_dir = tmp_path / "bgm"
    monkeypatch.setattr(video_matrix_api, "BGM_DIR", bgm_dir)
    client = TestClient(create_app())

    response = client.post(
        "/api/video-matrix/bgm/mock-download",
        json={"filename": "Corporate Industry.mp3", "title": "Corporate Industry", "artist": "Ivan_Luzan"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mock"] is True
    assert payload["filename"] == "Corporate_Industry.mp3"
    target = bgm_dir / "Corporate_Industry.mp3"
    assert target.exists()
    assert b"GasGx simulated MP3 download" in target.read_bytes()


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

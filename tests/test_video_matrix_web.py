from __future__ import annotations

from fastapi.testclient import TestClient

from gasgx_distribution.web import create_app


def test_video_matrix_api_state_and_preview() -> None:
    client = TestClient(create_app())

    state = client.get("/api/video-matrix/state")
    assert state.status_code == 200
    payload = state.json()
    assert payload["cover_templates"]
    assert "industrial_engine_hook" in payload["cover_templates"]

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


def test_video_matrix_static_entry_exists() -> None:
    client = TestClient(create_app())

    response = client.get("/")

    assert response.status_code == 200
    html = response.text
    assert 'data-view="video-matrix"' in html
    assert 'id="vm-cover-gallery"' in html

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

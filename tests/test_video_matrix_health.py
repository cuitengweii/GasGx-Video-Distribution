from __future__ import annotations

from fastapi.testclient import TestClient

from gasgx_distribution import web


def test_video_matrix_health_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(web, "ffmpeg_runtime_health", lambda: {"ffmpeg_ok": True, "ffprobe_ok": True})
    client = TestClient(web.create_app())

    response = client.get("/api/video-matrix/health")

    assert response.status_code == 200
    assert response.json() == {"ffmpeg_ok": True, "ffprobe_ok": True}

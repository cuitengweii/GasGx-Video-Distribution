from __future__ import annotations

import importlib.util
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).resolve().parents[1]
IMPORTER_PATH = ROOT / "tools" / "import_bgm_cloud.py"


def _load_importer():
    spec = importlib.util.spec_from_file_location("import_bgm_cloud", IMPORTER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


importer = _load_importer()


@pytest.fixture
def music_server():
    class Handler(BaseHTTPRequestHandler):
        routes: dict[str, tuple[int, dict[str, str], bytes]] = {}
        hits: dict[str, int] = {}

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            self.__class__.hits[path] = self.__class__.hits.get(path, 0) + 1
            route = self.__class__.routes.get(path)
            if route is None:
                self.send_response(404)
                self.end_headers()
                return
            status, headers, body = route
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            if body:
                self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield base_url, Handler.routes, Handler.hits
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _write_urls(path: Path, urls: list[str]) -> None:
    path.write_text("\n".join(urls) + "\n", encoding="utf-8")


def test_url_list_ignores_comments_and_limit_is_capped(tmp_path: Path) -> None:
    urls_path = tmp_path / "urls.txt"
    urls_path.write_text(
        "# authorized URLs\n\n"
        " https://example.test/audio/Business Track.mp3 \n"
        "https://example.test/audio/Business Track.mp3\n",
        encoding="utf-8",
    )

    assert importer.read_url_list(urls_path) == [
        "https://example.test/audio/Business Track.mp3",
        "https://example.test/audio/Business Track.mp3",
    ]
    assert importer._safe_mp3_filename("https://example.test/audio/Business Track.wav", 1) == "Business_Track.mp3"
    assert importer._normalize_limit(1000) == 300


def test_import_downloads_mp3_and_records_failures(tmp_path: Path, music_server) -> None:
    base_url, routes, _hits = music_server
    routes["/song.mp3"] = (200, {"Content-Type": "audio/mpeg"}, b"ID3\x04song")
    routes["/stream"] = (200, {"Content-Type": "audio/mpeg"}, b"ID3\x04stream")
    routes["/page"] = (200, {"Content-Type": "text/html"}, b"<html></html>")
    urls_path = tmp_path / "urls.txt"
    _write_urls(
        urls_path,
        [
            f"{base_url}/song.mp3",
            f"{base_url}/stream",
            f"{base_url}/page",
            f"{base_url}/missing.mp3",
            f"{base_url}/song.mp3",
        ],
    )

    manifest = importer.import_bgm_cloud(
        urls_path=urls_path,
        out_dir=tmp_path / "bgm_cloud",
        manifest_path=tmp_path / "manifest.json",
        limit=10,
        max_bytes=1024 * 1024,
    )

    out_dir = tmp_path / "bgm_cloud"
    assert (out_dir / "song.mp3").read_bytes() == b"ID3\x04song"
    assert (out_dir / "stream.mp3").read_bytes() == b"ID3\x04stream"
    assert not (out_dir / "page.mp3").exists()
    assert manifest["summary"]["success_count"] == 2
    assert manifest["summary"]["failed_count"] == 2
    assert manifest["summary"]["run"]["duplicate_url"] == 1

    records = {entry["url"]: entry for entry in manifest["entries"]}
    assert records[f"{base_url}/page"]["status"] == "failed"
    assert "MP3 response" in records[f"{base_url}/page"]["reason"]
    assert records[f"{base_url}/missing.mp3"]["http_status"] == 404


def test_import_respects_limit_and_resumes_without_redownloading(tmp_path: Path, music_server) -> None:
    base_url, routes, hits = music_server
    for index in range(1, 4):
        routes[f"/track-{index}.mp3"] = (200, {"Content-Type": "audio/mpeg"}, f"ID3-{index}".encode("utf-8"))
    urls_path = tmp_path / "urls.txt"
    _write_urls(urls_path, [f"{base_url}/track-{index}.mp3" for index in range(1, 4)])
    out_dir = tmp_path / "bgm_cloud"
    manifest_path = tmp_path / "manifest.json"

    first = importer.import_bgm_cloud(urls_path=urls_path, out_dir=out_dir, manifest_path=manifest_path, limit=2)
    assert first["summary"]["success_count"] == 2
    assert sorted(path.name for path in out_dir.glob("*.mp3")) == ["track-1.mp3", "track-2.mp3"]
    first_hits = dict(hits)

    second = importer.import_bgm_cloud(urls_path=urls_path, out_dir=out_dir, manifest_path=manifest_path, limit=2)
    assert second["summary"]["success_count"] == 2
    assert second["summary"]["run"]["resumed"] == 2
    assert hits == first_hits


def test_import_skips_duplicate_filename_from_different_urls(tmp_path: Path, music_server) -> None:
    base_url, routes, _hits = music_server
    routes["/a/track.mp3"] = (200, {"Content-Type": "audio/mpeg"}, b"ID3-first")
    routes["/b/track.mp3"] = (200, {"Content-Type": "audio/mpeg"}, b"ID3-second")
    urls_path = tmp_path / "urls.txt"
    _write_urls(urls_path, [f"{base_url}/a/track.mp3", f"{base_url}/b/track.mp3"])

    manifest = importer.import_bgm_cloud(
        urls_path=urls_path,
        out_dir=tmp_path / "bgm_cloud",
        manifest_path=tmp_path / "manifest.json",
        limit=10,
    )

    records = {entry["url"]: entry for entry in manifest["entries"]}
    assert manifest["summary"]["success_count"] == 1
    assert records[f"{base_url}/b/track.mp3"]["status"] == "skipped"
    assert records[f"{base_url}/b/track.mp3"]["reason"] == "duplicate_filename"


def test_import_rejects_oversized_audio_before_writing_file(tmp_path: Path, music_server) -> None:
    base_url, routes, _hits = music_server
    routes["/large.mp3"] = (200, {"Content-Type": "audio/mpeg", "Content-Length": "1024"}, b"ID3")
    urls_path = tmp_path / "urls.txt"
    _write_urls(urls_path, [f"{base_url}/large.mp3"])

    manifest = importer.import_bgm_cloud(
        urls_path=urls_path,
        out_dir=tmp_path / "bgm_cloud",
        manifest_path=tmp_path / "manifest.json",
        limit=10,
        max_bytes=8,
    )

    assert not (tmp_path / "bgm_cloud" / "large.mp3").exists()
    assert manifest["summary"]["failed_count"] == 1
    assert "exceeds" in manifest["entries"][0]["reason"]
    saved = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert saved["summary"]["failed_count"] == 1

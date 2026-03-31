from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from cybercar import engine, pipeline


def _workspace(tmp_path: Path) -> engine.Workspace:
    root = tmp_path / "workspace"
    downloads = root / "1_Downloads"
    processed = root / "2_Processed"
    archive = root / "3_Archived"
    history = root / "download_history.txt"
    image_downloads = root / "1_Downloads_Images"
    image_processed = root / "2_Processed_Images"
    image_history = root / "download_history_images.txt"
    for path in (downloads, processed, archive, image_downloads, image_processed):
        path.mkdir(parents=True, exist_ok=True)
    return engine.Workspace(
        root=root,
        downloads=downloads,
        processed=processed,
        archive=archive,
        history=history,
        image_downloads=image_downloads,
        image_processed=image_processed,
        image_history=image_history,
    )


def _install_collect_mocks(tmp_path: Path, monkeypatch, runtime_config: dict[str, object]) -> list[dict[str, object]]:
    captured: list[dict[str, object]] = []
    workspace_ctx = SimpleNamespace(root=tmp_path)

    monkeypatch.setattr(pipeline.core, "init_workspace", lambda workspace: workspace_ctx)
    monkeypatch.setattr(
        pipeline.core,
        "_resolve_network_proxy",
        lambda proxy, use_system_proxy=False: (proxy, use_system_proxy),
    )
    monkeypatch.setattr(pipeline.core, "_load_runtime_config", lambda path: runtime_config)
    monkeypatch.setattr(pipeline.core, "_normalize_keyword_list", lambda values, defaults: [])
    monkeypatch.setattr(pipeline.core, "_spark_config_ready", lambda config: False)
    monkeypatch.setattr(pipeline.core, "_log", lambda message: None)
    monkeypatch.setattr(pipeline.core, "download_from_x", lambda *args, **kwargs: captured.append(dict(kwargs)) or [])
    monkeypatch.setattr(pipeline.core, "process_video_fingerprint", lambda *args, **kwargs: [])
    monkeypatch.setattr(pipeline, "_resolve_sorted_output_root", lambda args, workspace: tmp_path / "sorted")
    monkeypatch.setattr(pipeline, "_export_sorted_batch", lambda outputs, export_root: None)
    monkeypatch.setattr(pipeline, "_collect_x_urls_for_outputs", lambda workspace, outputs: [])
    return captured


def test_run_collect_once_prefers_cli_x_download_overrides(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "x_download": {
            "socket_timeout_seconds": 25,
            "extractor_retries": 2,
            "download_retries": 2,
            "fragment_retries": 2,
            "retry_sleep_seconds": 1.0,
            "batch_retry_sleep_seconds": 1.0,
        }
    }
    captured = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--xiaohongshu-video-only",
            "--tweet-url",
            "https://x.test/post/fast",
            "--x-download-socket-timeout",
            "12",
            "--x-download-extractor-retries",
            "1",
            "--x-download-retries",
            "1",
            "--x-download-fragment-retries",
            "1",
            "--x-download-retry-sleep",
            "0",
            "--x-download-batch-retry-sleep",
            "0",
        ]
    )

    pipeline._run_collect_once(args)

    assert len(captured) == 1
    assert captured[0]["x_download_socket_timeout"] == 12
    assert captured[0]["x_download_extractor_retries"] == 1
    assert captured[0]["x_download_retries"] == 1
    assert captured[0]["x_download_fragment_retries"] == 1
    assert captured[0]["x_download_retry_sleep"] == 0.0
    assert captured[0]["x_download_batch_retry_sleep"] == 0.0


def test_run_collect_once_uses_runtime_x_download_defaults(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "x_download": {
            "socket_timeout_seconds": 25,
            "extractor_retries": 2,
            "download_retries": 2,
            "fragment_retries": 2,
            "retry_sleep_seconds": 1.0,
            "batch_retry_sleep_seconds": 1.0,
        }
    }
    captured = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--xiaohongshu-video-only",
            "--tweet-url",
            "https://x.test/post/default",
        ]
    )

    pipeline._run_collect_once(args)

    assert len(captured) == 1
    assert captured[0]["x_download_socket_timeout"] == 25
    assert captured[0]["x_download_extractor_retries"] == 2
    assert captured[0]["x_download_retries"] == 2
    assert captured[0]["x_download_fragment_retries"] == 2
    assert captured[0]["x_download_retry_sleep"] == 1.0
    assert captured[0]["x_download_batch_retry_sleep"] == 1.0


def test_run_collect_once_uses_dedicated_x_session_settings(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {"x_download": {}}
    captured = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--tweet-url",
            "https://x.test/post/session",
            "--x-debug-port",
            "9555",
            "--x-chrome-user-data-dir",
            str(tmp_path / "profiles" / "x_alt"),
            "--x-cookie-file",
            str(tmp_path / "config" / "x_cookies.alt.json"),
        ]
    )

    pipeline._run_collect_once(args)

    assert captured
    assert all(item["debug_port"] == 9555 for item in captured)
    assert all(item["chrome_user_data_dir"] == str(tmp_path / "profiles" / "x_alt") for item in captured)
    assert all(item["x_cookie_file"] == str(tmp_path / "config" / "x_cookies.alt.json") for item in captured)


def test_run_collect_once_collects_domestic_source_urls_when_configured(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "sources": {
            "platforms": "douyin,xiaohongshu",
            "keywords": [],
            "watch_accounts": {"douyin": [], "xiaohongshu": []},
        },
        "x_download": {},
    }
    captured_x = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)
    captured_domestic: list[dict[str, object]] = []
    monkeypatch.setattr(
        pipeline.core,
        "download_from_source_urls",
        lambda *args, **kwargs: captured_domestic.append(dict(kwargs)) or [],
    )

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--debug-port",
            "9444",
            "--source-platforms",
            "douyin,xiaohongshu",
            "--source-url",
            "https://www.douyin.com/video/123",
            "--source-url",
            "https://www.xiaohongshu.com/explore/456",
        ]
    )

    pipeline._run_collect_once(args)

    assert captured_x == []
    assert len(captured_domestic) == 2
    assert {item["source_platform"] for item in captured_domestic} == {"douyin", "xiaohongshu"}
    assert all(item["debug_port"] == 9444 for item in captured_domestic)
    include_images_by_platform = {str(item["source_platform"]): bool(item["include_images"]) for item in captured_domestic}
    assert include_images_by_platform == {"douyin": False, "xiaohongshu": True}


def test_run_collect_once_continues_when_one_domestic_source_platform_fails(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "sources": {
            "platforms": "douyin,xiaohongshu",
            "keywords": [],
            "watch_accounts": {"douyin": [], "xiaohongshu": []},
        },
        "x_download": {},
    }
    captured_x = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)
    captured_domestic: list[dict[str, object]] = []

    def fake_download_from_source_urls(*args, **kwargs):
        payload = dict(kwargs)
        if payload.get("source_platform") == "douyin":
            raise RuntimeError("douyin source failed")
        captured_domestic.append(payload)
        return []

    monkeypatch.setattr(pipeline.core, "download_from_source_urls", fake_download_from_source_urls)

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--source-platforms",
            "douyin,xiaohongshu",
            "--source-url",
            "https://www.douyin.com/video/123",
            "--source-url",
            "https://www.xiaohongshu.com/explore/456",
        ]
    )

    pipeline._run_collect_once(args)

    assert captured_x == []
    assert len(captured_domestic) == 1
    assert captured_domestic[0]["source_platform"] == "xiaohongshu"


def test_run_collect_once_discovers_domestic_post_urls_from_keywords(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "sources": {
            "platforms": "douyin,xiaohongshu",
            "keywords": ["cybertruck"],
            "watch_accounts": {"douyin": [], "xiaohongshu": []},
        },
        "x_download": {},
    }
    captured_x = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)
    captured_domestic: list[dict[str, object]] = []

    def fake_download_from_source_urls(*args, **kwargs):
        captured_domestic.append(dict(kwargs))
        return []

    def fake_discover_domestic_keyword_urls(**kwargs):
        platform = str(kwargs.get("platform") or "")
        if platform == "douyin":
            return ["https://www.douyin.com/video/1234567890"]
        if platform == "xiaohongshu":
            return ["https://www.xiaohongshu.com/explore/66aa77bb88cc99dd00ee11ff"]
        return []

    monkeypatch.setattr(pipeline.core, "download_from_source_urls", fake_download_from_source_urls)
    monkeypatch.setattr(pipeline.core, "discover_domestic_keyword_urls", fake_discover_domestic_keyword_urls)
    monkeypatch.setattr(
        pipeline.core,
        "_normalize_keyword_list",
        lambda values, defaults: [token.strip() for token in str(values or "").split(",") if token.strip()],
    )

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--source-platforms",
            "douyin,xiaohongshu",
            "--source-keywords",
            "cybertruck",
        ]
    )

    pipeline._run_collect_once(args)

    assert captured_x == []
    assert len(captured_domestic) == 2
    mapped = {str(item.get("source_platform")): list(item.get("source_urls") or []) for item in captured_domestic}
    assert mapped["douyin"] == ["https://www.douyin.com/video/1234567890"]
    assert mapped["xiaohongshu"] == ["https://www.xiaohongshu.com/explore/66aa77bb88cc99dd00ee11ff"]


def test_run_collect_once_extracts_keyword_from_douyin_search_url(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "sources": {
            "platforms": "douyin",
            "keywords": [],
            "watch_accounts": {"douyin": []},
        },
        "x_download": {},
    }
    captured_x = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)
    captured_domestic: list[dict[str, object]] = []
    discovered_keywords: list[str] = []

    def fake_download_from_source_urls(*args, **kwargs):
        captured_domestic.append(dict(kwargs))
        return []

    def fake_discover_domestic_keyword_urls(**kwargs):
        discovered_keywords.append(str(kwargs.get("keyword") or ""))
        return ["https://www.douyin.com/video/2222333344445555"]

    monkeypatch.setattr(pipeline.core, "download_from_source_urls", fake_download_from_source_urls)
    monkeypatch.setattr(pipeline.core, "discover_domestic_keyword_urls", fake_discover_domestic_keyword_urls)
    monkeypatch.setattr(
        pipeline.core,
        "_normalize_keyword_list",
        lambda values, defaults: [token.strip() for token in str(values or "").split(",") if token.strip()],
    )

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--source-platforms",
            "douyin",
            "--source-url",
            "https://www.douyin.com/search/cybertruck?type=video",
        ]
    )

    pipeline._run_collect_once(args)

    assert captured_x == []
    assert discovered_keywords == ["cybertruck"]
    assert len(captured_domestic) == 1
    assert captured_domestic[0]["source_urls"] == ["https://www.douyin.com/video/2222333344445555"]


def test_resolve_effective_source_keywords_prefers_latest_state(tmp_path: Path) -> None:
    workspace = SimpleNamespace(root=tmp_path)
    latest_path = tmp_path / "runtime" / "source_keywords_latest.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps({"keywords": ["赛博皮卡", "Cybertruck"]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    args = SimpleNamespace(source_keywords="", keyword="Cybertruck")
    runtime_config = {"sources": {"keywords": ["old-keyword"], "prefer_latest_keywords": True}}

    resolved, mode = pipeline._resolve_effective_source_keywords(args, runtime_config, workspace)

    assert resolved == ["赛博皮卡", "Cybertruck"]
    assert mode == "latest_state"


def test_resolve_effective_source_keywords_can_disable_latest_preference(tmp_path: Path) -> None:
    workspace = SimpleNamespace(root=tmp_path)
    latest_path = tmp_path / "runtime" / "source_keywords_latest.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps({"keywords": ["赛博皮卡", "Cybertruck"]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    args = SimpleNamespace(source_keywords="", keyword="Cybertruck")
    runtime_config = {"sources": {"keywords": ["china cybertruck"], "prefer_latest_keywords": False}}

    resolved, mode = pipeline._resolve_effective_source_keywords(args, runtime_config, workspace)

    assert resolved == ["china cybertruck"]
    assert mode == "config"


def test_run_collect_once_persists_latest_keywords_from_domestic_search_urls(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "sources": {
            "platforms": "douyin",
            "keywords": [],
            "watch_accounts": {"douyin": []},
            "prefer_latest_keywords": True,
        },
        "x_download": {},
    }
    _install_collect_mocks(tmp_path, monkeypatch, runtime_config)
    monkeypatch.setattr(pipeline.core, "_normalize_keyword_list", engine._normalize_keyword_list)
    monkeypatch.setattr(pipeline.core, "discover_domestic_keyword_urls", lambda **kwargs: [])

    parser = pipeline._build_parser()
    args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--source-platforms",
            "douyin",
            "--source-url",
            "https://www.douyin.com/search/cybertruck?type=video",
        ]
    )

    pipeline._run_collect_once(args)

    latest_state_path = tmp_path / "runtime" / "source_keywords_latest.json"
    assert latest_state_path.exists()
    payload = json.loads(latest_state_path.read_text(encoding="utf-8"))
    assert payload.get("mode") == "search_url"
    assert payload.get("source_platforms") == ["douyin"]
    assert payload.get("keywords") == ["cybertruck"]


def test_run_collect_once_reuses_latest_keywords_in_followup_run(tmp_path: Path, monkeypatch) -> None:
    runtime_config = {
        "sources": {
            "platforms": "douyin",
            "keywords": [],
            "watch_accounts": {"douyin": []},
            "prefer_latest_keywords": True,
        },
        "x_download": {},
    }
    captured_x = _install_collect_mocks(tmp_path, monkeypatch, runtime_config)
    discovered_keywords: list[str] = []
    monkeypatch.setattr(pipeline.core, "_normalize_keyword_list", engine._normalize_keyword_list)
    monkeypatch.setattr(pipeline.core, "download_from_source_urls", lambda *args, **kwargs: [])

    def fake_discover_domestic_keyword_urls(**kwargs):
        discovered_keywords.append(str(kwargs.get("keyword") or ""))
        return []

    monkeypatch.setattr(pipeline.core, "discover_domestic_keyword_urls", fake_discover_domestic_keyword_urls)
    parser = pipeline._build_parser()

    first_args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--source-platforms",
            "douyin",
            "--source-url",
            "https://www.douyin.com/search/cybertruck?type=video",
        ]
    )
    pipeline._run_collect_once(first_args)

    second_args = parser.parse_args(
        [
            "--workspace",
            str(tmp_path / "workspace"),
            "--limit",
            "1",
            "--source-platforms",
            "douyin",
        ]
    )
    pipeline._run_collect_once(second_args)

    assert captured_x == []
    assert discovered_keywords == ["cybertruck", "cybertruck"]
    assert discovered_keywords[-1] == "cybertruck"
    latest_state_path = tmp_path / "runtime" / "source_keywords_latest.json"
    payload = json.loads(latest_state_path.read_text(encoding="utf-8"))
    assert payload.get("mode") == "search_url"
    assert payload.get("keywords") == ["cybertruck"]


def test_build_parser_uses_configured_proxy_defaults(monkeypatch) -> None:
    monkeypatch.setattr(pipeline.core, "_default_network_proxy", lambda: "http://127.0.0.1:33210")
    monkeypatch.setattr(pipeline.core, "_default_use_system_proxy", lambda: False)

    parser = pipeline._build_parser()
    args = parser.parse_args([])

    assert args.proxy == "http://127.0.0.1:33210"
    assert args.use_system_proxy is False


def test_download_from_x_uses_direct_status_fallback_for_partial_failures(tmp_path: Path, monkeypatch) -> None:
    workspace = _workspace(tmp_path)
    logs: list[str] = []
    fallback_calls: list[dict[str, object]] = []
    failed_url = "https://x.com/fallback/status/222"

    monkeypatch.setattr(engine, "_log", lambda message: logs.append(str(message)))
    monkeypatch.setattr(engine, "_ensure_binary", lambda name: None)
    monkeypatch.setattr(engine, "_resolve_network_proxy", lambda proxy, use_system_proxy=False: (proxy, use_system_proxy))
    monkeypatch.setattr(
        engine,
        "_export_x_cookies_for_ytdlp",
        lambda chrome_user_data_dir, x_cookie_file="": (None, "skipped-empty"),
    )
    monkeypatch.setattr(engine, "_build_subprocess_network_env", lambda proxy=None, use_system_proxy=False: {})
    monkeypatch.setattr(engine, "_filter_already_processed_x_urls", lambda workspace, urls: (list(urls), []))

    def fake_run_ytdlp_download_with_retries(*args, **kwargs):
        (workspace.downloads / "111__first__clip.mp4").write_text("video", encoding="utf-8")
        return (
            subprocess.CompletedProcess(args=["yt-dlp"], returncode=1, stdout="", stderr="ERROR: unable to download JSON metadata: read timed out"),
            engine.XDownloadRetryStats(
                initial_batch_failures=1,
                initial_failed_urls=(failed_url,),
                retry_batch_failures=1,
                retry_failed_urls=(failed_url,),
                transport_fallback_attempts=1,
                transport_failed_urls=(failed_url,),
                final_failed_urls=(failed_url,),
            ),
        )

    def fake_download_x_videos_from_status_urls(**kwargs):
        fallback_calls.append(dict(kwargs))
        (workspace.downloads / "222__second__clip.mp4").write_text("video", encoding="utf-8")
        return [workspace.downloads / "222__second__clip.mp4"]

    monkeypatch.setattr(engine, "_run_ytdlp_download_with_retries", fake_run_ytdlp_download_with_retries)
    monkeypatch.setattr(engine, "_download_x_videos_from_status_urls", fake_download_x_videos_from_status_urls)

    files = engine.download_from_x(
        workspace=workspace,
        limit=2,
        tweet_urls=[
            "https://x.com/source/status/111",
            failed_url,
        ],
        auto_discover_x=False,
    )

    assert fallback_calls
    assert fallback_calls[0]["status_urls"] == [failed_url]
    assert fallback_calls[0]["limit"] == 1
    assert any(path.name == "222__second__clip.mp4" for path in files)
    assert any("cookie_export_status=skipped-empty" in line for line in logs)
    assert any("direct_status_fallback_attempts=1" in line for line in logs)


def test_transport_failure_classifier_ignores_non_transport_messages() -> None:
    result = subprocess.CompletedProcess(
        args=["yt-dlp"],
        returncode=1,
        stdout="",
        stderr="ERROR: this tweet has no video",
    )

    assert engine._looks_like_x_metadata_transport_failure(result) is False


def test_next_x_discovery_scroll_wait_seconds_uses_randomized_one_to_three_second_window(monkeypatch) -> None:
    captured: dict[str, tuple[float, float]] = {}

    def fake_uniform(start: float, end: float) -> float:
        captured["range"] = (start, end)
        return 2.4

    monkeypatch.setattr(engine.random, "uniform", fake_uniform)

    wait_seconds = engine._next_x_discovery_scroll_wait_seconds(engine.X_DISCOVERY_SCROLL_WAIT_SECONDS)

    assert wait_seconds == 2.4
    assert captured["range"] == (1.0, 3.0)


def test_next_x_discovery_scroll_wait_seconds_clamps_to_minimum_window(monkeypatch) -> None:
    captured: dict[str, tuple[float, float]] = {}

    def fake_uniform(start: float, end: float) -> float:
        captured["range"] = (start, end)
        return start

    monkeypatch.setattr(engine.random, "uniform", fake_uniform)

    wait_seconds = engine._next_x_discovery_scroll_wait_seconds(0.2)

    assert wait_seconds == 1.0
    assert captured["range"] == (1.0, 1.0)


def test_download_from_x_skips_compat_search_when_auto_discovery_disabled(tmp_path: Path, monkeypatch) -> None:
    workspace = _workspace(tmp_path)
    logs: list[str] = []
    commands: list[list[str]] = []

    monkeypatch.setattr(engine, "_log", lambda message: logs.append(str(message)))
    monkeypatch.setattr(engine, "_ensure_binary", lambda name: None)
    monkeypatch.setattr(engine, "_resolve_network_proxy", lambda proxy, use_system_proxy=False: (proxy, use_system_proxy))
    monkeypatch.setattr(engine, "_build_subprocess_network_env", lambda proxy=None, use_system_proxy=False: {})
    monkeypatch.setattr(engine, "_export_x_cookies_for_ytdlp", lambda *args, **kwargs: (None, "skipped-empty"))
    monkeypatch.setattr(engine, "_run_command_result", lambda cmd, step_name, env=None: commands.append(list(cmd)) or subprocess.CompletedProcess(cmd, 0, "", ""))

    files = engine.download_from_x(
        workspace=workspace,
        keyword="Cybertruck",
        limit=1,
        tweet_urls=[],
        auto_discover_x=False,
        x_download_fail_fast=False,
    )

    assert files == []
    assert commands == []
    assert any("auto-discovery disabled" in line for line in logs)


def test_download_from_x_skips_compat_search_when_fail_fast_has_no_discovered_urls(tmp_path: Path, monkeypatch) -> None:
    workspace = _workspace(tmp_path)
    logs: list[str] = []
    commands: list[list[str]] = []

    monkeypatch.setattr(engine, "_log", lambda message: logs.append(str(message)))
    monkeypatch.setattr(engine, "_ensure_binary", lambda name: None)
    monkeypatch.setattr(engine, "_resolve_network_proxy", lambda proxy, use_system_proxy=False: (proxy, use_system_proxy))
    monkeypatch.setattr(engine, "_build_subprocess_network_env", lambda proxy=None, use_system_proxy=False: {})
    monkeypatch.setattr(engine, "_export_x_cookies_for_ytdlp", lambda *args, **kwargs: (None, "skipped-empty"))
    monkeypatch.setattr(engine, "discover_x_video_urls", lambda **kwargs: [])
    monkeypatch.setattr(engine, "discover_x_urls_via_seed_accounts", lambda **kwargs: [])
    monkeypatch.setattr(engine, "_run_command_result", lambda cmd, step_name, env=None: commands.append(list(cmd)) or subprocess.CompletedProcess(cmd, 0, "", ""))

    files = engine.download_from_x(
        workspace=workspace,
        keyword="Cybertruck",
        limit=1,
        tweet_urls=[],
        auto_discover_x=True,
        x_download_fail_fast=True,
    )

    assert files == []
    assert commands == []
    assert any("fail-fast enabled and no discovered URLs available" in line for line in logs)


def test_filter_already_processed_x_urls_uses_collect_ledger_sources(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    workspace.history.write_text("111:abc\n", encoding="utf-8")
    (workspace.root / "content_fingerprint_index.json").write_text(
        json.dumps(
            [
                {
                    "processed_name": "DRAFT_222__reviewed.mp4",
                    "media_kind": "video",
                    "status_url": "https://x.com/reviewed/status/222",
                    "status_id": "222",
                    "media_id": "222",
                    "title_text": "reviewed clip",
                    "description_text": "reviewed clip",
                    "uploader_text": "reviewed",
                }
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (workspace.root / engine.DEFAULT_REVIEW_STATE_FILE).write_text(
        json.dumps(
            {
                "items": {
                    "video|DRAFT_222__reviewed.mp4": {
                        "status": "approved",
                        "processed_name": "DRAFT_222__reviewed.mp4",
                        "media_kind": "video",
                    }
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (workspace.root / "uploaded_content_fingerprint_index.json").write_text(
        json.dumps(
            [
                {
                    "processed_name": "DRAFT_333__published.mp4",
                    "media_kind": "video",
                    "status_url": "https://x.com/published/status/333",
                    "status_id": "333",
                    "media_id": "333",
                    "title_text": "published clip",
                    "description_text": "published clip",
                    "uploader_text": "published",
                }
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    remaining, skipped = engine._filter_already_processed_x_urls(
        workspace,
        [
            "https://x.com/history/status/111",
            "https://x.com/reviewed/status/222",
            "https://x.com/published/status/333",
            "https://x.com/fresh/status/444",
        ],
    )

    assert remaining == ["https://x.com/fresh/status/444"]
    assert {(item["status_id"], item["state"]) for item in skipped} == {
        ("111", "downloaded"),
        ("222", "approved"),
        ("333", "published"),
    }
    assert (workspace.root / engine.DEFAULT_CANDIDATE_LEDGER_FILE).exists()


def test_download_from_x_opens_extra_discovery_rounds_after_seen_filter(tmp_path: Path, monkeypatch) -> None:
    workspace = _workspace(tmp_path)
    workspace.history.write_text("111:abc\n", encoding="utf-8")
    logs: list[str] = []
    selected_batches: list[list[str]] = []
    discovery_calls: list[int] = []

    monkeypatch.setattr(engine, "_log", lambda message: logs.append(str(message)))
    monkeypatch.setattr(engine, "_ensure_binary", lambda name: None)
    monkeypatch.setattr(engine, "_resolve_network_proxy", lambda proxy, use_system_proxy=False: (proxy, use_system_proxy))
    monkeypatch.setattr(engine, "_export_x_cookies_for_ytdlp", lambda *args, **kwargs: (None, "skipped-empty"))
    monkeypatch.setattr(engine, "_build_subprocess_network_env", lambda proxy=None, use_system_proxy=False: {})
    monkeypatch.setattr(engine, "discover_x_urls_via_seed_accounts", lambda **kwargs: [])

    def fake_discover_x_video_urls(**kwargs):
        discovery_calls.append(int(kwargs["url_limit"]))
        if len(discovery_calls) == 1:
            return ["https://x.com/history/status/111"]
        if len(discovery_calls) == 2:
            return [
                "https://x.com/history/status/111",
                "https://x.com/fresh/status/222",
            ]
        return [
            "https://x.com/history/status/111",
            "https://x.com/fresh/status/222",
            "https://x.com/fresh/status/333",
        ]

    def fake_run_ytdlp_download_with_retries(cmd, *, selected_urls, **kwargs):
        selected_batches.append(list(selected_urls))
        return (
            subprocess.CompletedProcess(args=["yt-dlp"], returncode=0, stdout="", stderr=""),
            engine.XDownloadRetryStats(),
        )

    monkeypatch.setattr(engine, "discover_x_video_urls", fake_discover_x_video_urls)
    monkeypatch.setattr(engine, "_run_ytdlp_download_with_retries", fake_run_ytdlp_download_with_retries)

    files = engine.download_from_x(
        workspace=workspace,
        keyword="Cybertruck",
        limit=2,
        tweet_urls=[],
        auto_discover_x=True,
        x_download_fail_fast=False,
    )

    assert files == []
    assert len(discovery_calls) == 3
    assert selected_batches == [["https://x.com/fresh/status/222", "https://x.com/fresh/status/333"]]
    assert any("round=2" in line for line in logs)
    assert any("round=3" in line for line in logs)
    assert any("Total previously-seen URLs filtered before download: 3" in line for line in logs)
    assert any(
        "Collect summary:" in line
        and "target_new=2" in line
        and "desired_candidates=16" in line
        and "discovery_rounds=3" in line
        and "filtered_seen=3" in line
        and "selected_urls=2" in line
        and "delivered=0" in line
        for line in logs
    )


def test_refresh_collect_candidate_ledger_normalizes_video_status_id_from_status_url(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    (workspace.root / "content_fingerprint_index.json").write_text(
        json.dumps(
            [
                {
                    "processed_name": "DRAFT_2034238391055949824__Giggly_Georgy_in__Self_Driving.mp4",
                    "media_kind": "video",
                    "status_url": "https://x.com/DakotaGeorgy/status/2034238454293528593",
                    "status_id": "2034238454293528593",
                    "media_id": "2034238391055949824",
                    "title_text": "Self Driving",
                    "description_text": "Self Driving",
                    "uploader_text": "DakotaGeorgy",
                }
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    ledger_path = workspace.root / engine.DEFAULT_CANDIDATE_LEDGER_FILE
    ledger_path.write_text(
        json.dumps(
            {
                "version": 1,
                "items": {
                    "x:2034238391055949824:2034238391055949824": {
                        "candidate_id": "x:2034238391055949824:2034238391055949824",
                        "status_id": "2034238391055949824",
                        "media_key": "2034238391055949824",
                        "media_kind": "video",
                        "state": "downloaded",
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    items = engine._refresh_collect_candidate_ledger(workspace)

    assert "x:2034238454293528593:2034238454293528593" in items
    assert "x:2034238391055949824:2034238391055949824" not in items
    assert items["x:2034238454293528593:2034238454293528593"]["processed_name"] == "DRAFT_2034238391055949824__Giggly_Georgy_in__Self_Driving.mp4"
    assert items["x:2034238454293528593:2034238454293528593"]["state"] == "processed"


def test_decode_escaped_media_url_normalizes_backslash_escapes() -> None:
    raw = r"https:\/\/sns-video-hw.xhscdn.com\/stream\/abc.mp4?token=1\u0026vid=2"
    normalized = engine._decode_escaped_media_url(raw)
    assert normalized == "https://sns-video-hw.xhscdn.com/stream/abc.mp4?token=1&vid=2"


def test_extract_domestic_video_candidate_urls_accepts_escaped_payload_urls() -> None:
    class FakePage:
        def __init__(self, payload):
            self._payload = payload

        def run_js(self, *_args, **_kwargs):
            return list(self._payload)

    douyin_page = FakePage(
        [
            r"https:\/\/v3-dy-o.zjcdn.com\/video\/tos\/cn\/sample.mp4?a=1",
            r"https:\/\/www.douyin.com\/video\/1234567890123456789",
        ]
    )
    xiaohongshu_page = FakePage(
        [
            r"https:\/\/sns-video-hw.xhscdn.com\/stream\/test.m3u8?auth=1",
            r"https:\/\/www.xiaohongshu.com\/explore\/69c3ca0d0000000021039c46",
        ]
    )

    douyin_urls = engine._extract_domestic_video_candidate_urls(douyin_page, "douyin")
    xiaohongshu_urls = engine._extract_domestic_video_candidate_urls(xiaohongshu_page, "xiaohongshu")

    assert douyin_urls == ["https://v3-dy-o.zjcdn.com/video/tos/cn/sample.mp4?a=1"]
    assert xiaohongshu_urls == ["https://sns-video-hw.xhscdn.com/stream/test.m3u8?auth=1"]


def test_download_domestic_browser_fallback_loads_profile_cookies_with_platform_keyword(
    tmp_path: Path,
    monkeypatch,
) -> None:
    workspace = _workspace(tmp_path)
    cookie_calls: list[tuple[str, str]] = []

    class FakePage:
        def get(self, _url: str) -> None:
            return None

        def run_js(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(
        engine,
        "_load_source_platform_profile_cookie_jar",
        lambda chrome_user_data_dir, *, platform: cookie_calls.append((str(chrome_user_data_dir), str(platform))) or (None, "skipped"),
    )
    monkeypatch.setattr(engine, "_build_network_opener", lambda **kwargs: SimpleNamespace(open=lambda *a, **k: None))
    monkeypatch.setattr(engine, "_connect_chrome", lambda **kwargs: FakePage())
    monkeypatch.setattr(engine, "_prepare_upload_tab", lambda page: page)
    monkeypatch.setattr(engine, "_run_page_action", lambda page, desc, fn: fn())
    monkeypatch.setattr(engine, "_extract_domestic_video_candidate_urls", lambda page, platform: [])
    monkeypatch.setattr(engine, "_close_work_tab", lambda *args, **kwargs: None)
    monkeypatch.setattr(engine, "_log", lambda message: None)

    files = engine._download_domestic_videos_via_browser(
        workspace,
        source_urls=["https://www.douyin.com/video/1234567890123456789"],
        source_platform="douyin",
        limit=1,
        chrome_user_data_dir=str(tmp_path / "profile_douyin"),
    )

    assert files == []
    assert cookie_calls == [(str(tmp_path / "profile_douyin"), "douyin")]

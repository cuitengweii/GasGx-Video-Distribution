from __future__ import annotations

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

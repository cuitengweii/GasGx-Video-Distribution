from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from cybercar import pipeline


def test_run_collect_once_passes_x_download_overrides(tmp_path: Path, monkeypatch) -> None:
    captured: list[dict[str, object]] = []
    workspace_ctx = SimpleNamespace(root=tmp_path)

    monkeypatch.setattr(pipeline.core, "init_workspace", lambda workspace: workspace_ctx)
    monkeypatch.setattr(pipeline.core, "_resolve_network_proxy", lambda proxy, use_system_proxy=False: (proxy, use_system_proxy))
    monkeypatch.setattr(pipeline.core, "_load_runtime_config", lambda path: {})
    monkeypatch.setattr(pipeline.core, "_normalize_keyword_list", lambda values, defaults: [])
    monkeypatch.setattr(pipeline.core, "_spark_config_ready", lambda config: False)
    monkeypatch.setattr(pipeline.core, "_log", lambda message: None)
    monkeypatch.setattr(pipeline.core, "download_from_x", lambda *args, **kwargs: captured.append(dict(kwargs)) or [])
    monkeypatch.setattr(pipeline.core, "process_video_fingerprint", lambda *args, **kwargs: [])
    monkeypatch.setattr(pipeline, "_resolve_sorted_output_root", lambda args, workspace: tmp_path / "sorted")
    monkeypatch.setattr(pipeline, "_export_sorted_batch", lambda outputs, export_root: None)
    monkeypatch.setattr(pipeline, "_collect_x_urls_for_outputs", lambda workspace, outputs: [])

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

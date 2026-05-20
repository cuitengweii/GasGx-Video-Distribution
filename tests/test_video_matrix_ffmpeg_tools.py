from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from gasgx_distribution.video_matrix import ffmpeg_tools


def test_run_command_uses_lossy_utf8_output_decoding(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run(args, **kwargs):
        captured["args"] = args
        captured.update(kwargs)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ffmpeg_tools.subprocess, "run", fake_run)

    ffmpeg_tools.run_command(["ffmpeg", "-version"])

    assert captured["encoding"] == "utf-8"
    assert captured["errors"] == "replace"
    assert captured["text"] is True
    assert captured["capture_output"] is True


def test_run_command_handles_empty_failed_output(monkeypatch) -> None:
    def fake_run(args, **kwargs):
        return subprocess.CompletedProcess(args=args, returncode=1, stdout=None, stderr=None)

    monkeypatch.setattr(ffmpeg_tools.subprocess, "run", fake_run)

    with pytest.raises(ffmpeg_tools.FFmpegError, match="FFmpeg command failed"):
        ffmpeg_tools.run_command(["ffmpeg", "-bad"])


def test_run_command_falls_back_to_stdout_on_failure(monkeypatch) -> None:
    def fake_run(args, **kwargs):
        return subprocess.CompletedProcess(args=args, returncode=1, stdout="stdout failure", stderr=None)

    monkeypatch.setattr(ffmpeg_tools.subprocess, "run", fake_run)

    with pytest.raises(ffmpeg_tools.FFmpegError, match="stdout failure"):
        ffmpeg_tools.run_command(["ffmpeg", "-bad"])


def test_concat_video_uses_filter_complex_script(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run_command(args):
        captured["args"] = list(args)
        option = "-filter_complex_script" if "-filter_complex_script" in captured["args"] else "-/filter_complex"
        script_path = Path(captured["args"][captured["args"].index(option) + 1])
        captured["script_path"] = script_path
        captured["script_text"] = script_path.read_text(encoding="utf-8")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ffmpeg_tools, "resolve_binary", lambda name: name)
    monkeypatch.setattr(ffmpeg_tools, "run_command", fake_run_command)

    long_filter = "scale=1080:1920," + "drawbox=x=0:y=0:w=10:h=10;" * 2000 + "[0:v]null[vout]"
    output = tmp_path / "rendered.mp4"

    ffmpeg_tools.concat_video(long_filter, [tmp_path / "clip.mp4"], output)

    args = captured["args"]
    assert "-filter_complex_script" in args or "-/filter_complex" in args
    assert "-filter_complex" not in args
    assert long_filter not in args
    assert captured["script_text"] == long_filter
    assert not Path(captured["script_path"]).exists()


def test_concat_video_seeks_bgm_offset_before_audio_input(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run_command(args):
        captured["args"] = list(args)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ffmpeg_tools, "resolve_binary", lambda name: name)
    monkeypatch.setattr(ffmpeg_tools, "run_command", fake_run_command)

    ffmpeg_tools.concat_video(
        "[0:v]null[vout]",
        [tmp_path / "clip.mp4"],
        tmp_path / "rendered.mp4",
        bgm_path=tmp_path / "long.mp3",
        bgm_start_offset=12.345,
    )

    args = captured["args"]
    ss_index = args.index("-ss")
    assert args[ss_index + 1] == "12.345"
    assert args[ss_index - 2:ss_index] == ["-stream_loop", "-1"]
    assert args[ss_index + 2:ss_index + 4] == ["-i", str(tmp_path / "long.mp3")]


def test_concat_video_builds_audio_mix_when_mining_track_is_available(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}
    mining = tmp_path / "mining.mp3"
    mining.write_bytes(b"mining")

    def fake_run_command(args):
        captured["args"] = list(args)
        option = "-filter_complex_script" if "-filter_complex_script" in captured["args"] else "-/filter_complex"
        script_path = Path(captured["args"][captured["args"].index(option) + 1])
        captured["script_text"] = script_path.read_text(encoding="utf-8")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ffmpeg_tools, "resolve_binary", lambda name: name)
    monkeypatch.setattr(ffmpeg_tools, "run_command", fake_run_command)

    ffmpeg_tools.concat_video(
        "[0:v]null[vout]",
        [tmp_path / "clip.mp4"],
        tmp_path / "rendered.mp4",
        bgm_path=tmp_path / "library.mp3",
        mining_bgm_path=mining,
        mining_bgm_volume=1.2,
        library_bgm_volume=0.3,
    )

    args = captured["args"]
    assert "-map" in args
    assert "[aout]" in args
    assert "amix=inputs=2:duration=shortest:dropout_transition=2" in str(captured["script_text"])
    assert "volume=1.200" in str(captured["script_text"])
    assert "volume=0.300" in str(captured["script_text"])


def test_normalize_filter_graph_compacts_empty_tokens() -> None:
    raw = "[0:v]scale=1080:1920,,format=yuv420p[v0];;[v0]null[vout];"

    normalized = ffmpeg_tools._normalize_filter_graph(raw)

    assert normalized == "[0:v]scale=1080:1920,format=yuv420p[v0];[v0]null[vout]"
    assert ";;" not in normalized
    assert ",," not in normalized


def test_build_audio_mix_filter_script_avoids_double_semicolon(tmp_path: Path) -> None:
    base_script = tmp_path / "base.filter_complex.txt"
    base_script.write_text("[0:v]null[vout];", encoding="utf-8")

    mixed_path = ffmpeg_tools._build_audio_mix_filter_script(
        base_script,
        library_audio_index=1,
        mining_audio_index=2,
        mining_bgm_volume=1.0,
        library_bgm_volume=0.5,
    )
    mixed_text = mixed_path.read_text(encoding="utf-8")

    assert ";;" not in mixed_text
    assert mixed_text.startswith("[0:v]null[vout];[1:a]aformat=")
    assert mixed_text.endswith("alimiter=limit=0.95[aout]")


def test_ffmpeg_runtime_health_reports_binary_paths(monkeypatch) -> None:
    monkeypatch.setattr(ffmpeg_tools, "resolve_binary", lambda name: f"/bin/{name}")

    health = ffmpeg_tools.ffmpeg_runtime_health()

    assert health["ffmpeg_ok"] is True
    assert health["ffprobe_ok"] is True
    assert health["ffmpeg_path"] == "/bin/ffmpeg"
    assert health["ffprobe_path"] == "/bin/ffprobe"


def test_resolve_ffmpeg_threads_prefers_env_override(monkeypatch) -> None:
    monkeypatch.setenv("VIDEO_MATRIX_FFMPEG_THREADS", "5")

    assert ffmpeg_tools.resolve_ffmpeg_threads(worker_count=2, concurrent_jobs=2) == 5


def test_resolve_ffmpeg_threads_scales_down_with_workers(monkeypatch) -> None:
    monkeypatch.delenv("VIDEO_MATRIX_FFMPEG_THREADS", raising=False)
    monkeypatch.setattr(ffmpeg_tools.os, "cpu_count", lambda: 16)

    assert ffmpeg_tools.resolve_ffmpeg_threads(worker_count=2, concurrent_jobs=1) == 8

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
        script_path = Path(captured["args"][captured["args"].index("-filter_complex_script") + 1])
        captured["script_path"] = script_path
        captured["script_text"] = script_path.read_text(encoding="utf-8")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(ffmpeg_tools, "resolve_binary", lambda name: name)
    monkeypatch.setattr(ffmpeg_tools, "run_command", fake_run_command)

    long_filter = "scale=1080:1920," + "drawbox=x=0:y=0:w=10:h=10;" * 2000 + "[0:v]null[vout]"
    output = tmp_path / "rendered.mp4"

    ffmpeg_tools.concat_video(long_filter, [tmp_path / "clip.mp4"], output)

    args = captured["args"]
    assert "-filter_complex_script" in args
    assert "-filter_complex" not in args
    assert long_filter not in args
    assert captured["script_text"] == long_filter
    assert not Path(captured["script_path"]).exists()

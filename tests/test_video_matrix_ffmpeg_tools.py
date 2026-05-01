from __future__ import annotations

import subprocess

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

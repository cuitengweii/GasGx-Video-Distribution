from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Iterable


class FFmpegError(RuntimeError):
    pass


def resolve_binary(name: str) -> str:
    direct = shutil.which(name)
    if direct:
        return direct
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        sibling = Path(ffmpeg_path).with_name(f"{name}.exe")
        if sibling.exists():
            return str(sibling)
    raise FFmpegError(f"Unable to find required binary: {name}")


def run_command(args: Iterable[str]) -> subprocess.CompletedProcess[str]:
    process = subprocess.run(
        list(args),
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )
    if process.returncode != 0:
        stderr = process.stderr or ""
        stdout = process.stdout or ""
        raise FFmpegError(stderr.strip() or stdout.strip() or "FFmpeg command failed")
    return process


def probe_media(path: Path) -> dict:
    try:
        ffprobe = resolve_binary("ffprobe")
    except FFmpegError:
        return _probe_with_ffmpeg(path)

    result = run_command(
        [
            ffprobe,
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
            str(path),
        ]
    )
    return json.loads(result.stdout)


def normalize_clip(
    source: Path,
    target: Path,
    width: int,
    height: int,
    fps: int,
) -> None:
    ffmpeg = resolve_binary("ffmpeg")
    target.parent.mkdir(parents=True, exist_ok=True)
    vf = (
        f"scale={width}:{height}:force_original_aspect_ratio=increase,"
        f"crop={width}:{height},fps={fps},eq=contrast=1.15:brightness=-0.03:saturation=1.05"
    )
    run_command(
        [
            ffmpeg,
            "-y",
            "-i",
            str(source),
            "-vf",
            vf,
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "fast",
            "-crf",
            "20",
            str(target),
        ]
    )


def concat_video(filter_complex: str, inputs: list[Path], output: Path, bgm_path: Path | None = None) -> None:
    ffmpeg = resolve_binary("ffmpeg")
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [ffmpeg, "-y"]
    for clip in inputs:
        command.extend(["-i", str(clip)])
    if bgm_path is not None:
        command.extend(["-stream_loop", "-1", "-i", str(bgm_path)])
    command.extend(
        [
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
            "-c:v",
            "libx264",
            "-preset",
            "fast",
            "-crf",
            "20",
        ]
    )
    if bgm_path is not None:
        command.extend(
            [
                "-map",
                f"{len(inputs)}:a",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-shortest",
            ]
        )
    else:
        command.append("-an")
    command.append(str(output))
    run_command(command)


def extract_frame(video_path: Path, output_path: Path, timestamp: float) -> None:
    ffmpeg = resolve_binary("ffmpeg")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    run_command(
        [
            ffmpeg,
            "-y",
            "-ss",
            str(timestamp),
            "-i",
            str(video_path),
            "-frames:v",
            "1",
            str(output_path),
        ]
    )


def _probe_with_ffmpeg(path: Path) -> dict:
    ffmpeg = resolve_binary("ffmpeg")
    process = subprocess.run(
        [ffmpeg, "-hide_banner", "-i", str(path)],
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )
    stderr = process.stderr or ""
    if not stderr:
        raise FFmpegError("Unable to inspect media with ffmpeg fallback")

    duration = _extract_duration(stderr)
    width, height, fps = _extract_video_stream(stderr)
    return {
        "format": {
            "filename": str(path),
            "duration": duration,
        },
        "streams": [
            {
                "codec_type": "video",
                "width": width,
                "height": height,
                "avg_frame_rate": f"{fps}/1" if fps else "0/1",
                "duration": duration,
            }
        ],
    }


def _extract_duration(stderr: str) -> str:
    match = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", stderr)
    if not match:
        return "0.0"
    hours, minutes, seconds = match.groups()
    total = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    return f"{total:.3f}"


def _extract_video_stream(stderr: str) -> tuple[int, int, int]:
    resolution_match = re.search(r"Video:.*?(\d{2,5})x(\d{2,5})", stderr, flags=re.DOTALL)
    fps_match = re.search(r"(\d+(?:\.\d+)?)\s*fps", stderr)
    if not resolution_match:
        raise FFmpegError("Unable to parse video stream details from ffmpeg output")
    width = int(resolution_match.group(1))
    height = int(resolution_match.group(2))
    fps = int(round(float(fps_match.group(1)))) if fps_match else 0
    return width, height, fps

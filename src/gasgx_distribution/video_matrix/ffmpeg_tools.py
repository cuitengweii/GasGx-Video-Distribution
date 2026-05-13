from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Iterable


class FFmpegError(RuntimeError):
    pass


def ffmpeg_runtime_health() -> dict[str, str | bool]:
    health: dict[str, str | bool] = {"ffmpeg_ok": False, "ffprobe_ok": False}
    try:
        health["ffmpeg_path"] = resolve_binary("ffmpeg")
        health["ffmpeg_ok"] = True
    except FFmpegError as exc:
        health["ffmpeg_error"] = str(exc)
    try:
        health["ffprobe_path"] = resolve_binary("ffprobe")
        health["ffprobe_ok"] = True
    except FFmpegError as exc:
        health["ffprobe_error"] = str(exc)
    return health


def resolve_ffmpeg_threads(worker_count: int | None = None, concurrent_jobs: int | None = None) -> int:
    env_threads = _read_int_env("VIDEO_MATRIX_FFMPEG_THREADS")
    if env_threads is not None:
        return max(1, env_threads)
    cpu_count = os.cpu_count() or 1
    workers = max(1, worker_count or 1)
    jobs = max(1, concurrent_jobs or 1)
    threads = cpu_count // max(1, workers * jobs)
    return max(1, threads)


def acquire_encode_slot() -> None:
    _ENCODE_SLOTS.acquire()


def release_encode_slot() -> None:
    _ENCODE_SLOTS.release()


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
    command = list(args)
    acquire_encode_slot()
    try:
        process = subprocess.run(
            command,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
        )
        if process.returncode != 0:
            stderr = process.stderr or ""
            stdout = process.stdout or ""
            message = stderr.strip() or stdout.strip() or "FFmpeg command failed"
            raise FFmpegError(f"[exit={process.returncode}] {message}")
        return process
    finally:
        release_encode_slot()


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
    speed_mode: str = "quality",
    threads: int | None = None,
) -> None:
    ffmpeg = resolve_binary("ffmpeg")
    target.parent.mkdir(parents=True, exist_ok=True)
    threads = max(1, threads or resolve_ffmpeg_threads())
    vf = (
        f"scale={width}:{height}:force_original_aspect_ratio=increase,"
        f"crop={width}:{height},fps={fps},eq=contrast=1.15:brightness=-0.03:saturation=1.05"
    )
    profile = _video_encode_profile(speed_mode)
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
            "-threads",
            str(threads),
            "-x264-params",
            f"threads={threads}",
            "-preset",
            profile["preset"],
            "-crf",
            profile["crf"],
            str(target),
        ]
    )


def concat_video(
    filter_complex: str,
    inputs: list[Path],
    output: Path,
    bgm_path: Path | None = None,
    speed_mode: str = "quality",
    threads: int | None = None,
    bgm_start_offset: float = 0.0,
) -> None:
    ffmpeg = resolve_binary("ffmpeg")
    output.parent.mkdir(parents=True, exist_ok=True)
    filter_script_path = output.parent / f".{output.stem}.filter_complex.txt"
    filter_script_path.write_text(filter_complex, encoding="utf-8")
    profile = _video_encode_profile(speed_mode)
    threads = max(1, threads or resolve_ffmpeg_threads())
    try:
        failures: list[str] = []
        for filter_mode in ("new", "legacy"):
            for include_bgm in ((True, False) if bgm_path is not None else (False,)):
                command = _build_concat_command(
                    ffmpeg=ffmpeg,
                    inputs=inputs,
                    output=output,
                    bgm_path=bgm_path if include_bgm else None,
                    filter_script_path=filter_script_path,
                    filter_mode=filter_mode,
                    profile=profile,
                    threads=threads,
                    speed_mode=speed_mode,
                    bgm_start_offset=bgm_start_offset,
                )
                try:
                    run_command(command)
                    return
                except FFmpegError as exc:
                    failures.append(f"{filter_mode}/bgm={include_bgm}: {exc}")
                    # Older ffmpeg builds don't understand the new script syntax.
                    if filter_mode == "new" and "Unrecognized option '-/filter_complex'" in str(exc):
                        break
        raise FFmpegError("\n".join(failures))
    finally:
        filter_script_path.unlink(missing_ok=True)


def _build_concat_command(
    *,
    ffmpeg: str,
    inputs: list[Path],
    output: Path,
    bgm_path: Path | None,
    filter_script_path: Path,
    filter_mode: str,
    profile: dict[str, str],
    threads: int,
    speed_mode: str,
    bgm_start_offset: float,
) -> list[str]:
    command = [ffmpeg, "-y"]
    for clip in inputs:
        command.extend(["-i", str(clip)])
    if bgm_path is not None:
        command.extend(["-stream_loop", "-1"])
        offset = max(0.0, float(bgm_start_offset or 0.0))
        if offset:
            command.extend(["-ss", f"{offset:.3f}"])
        command.extend(["-i", str(bgm_path)])
    if filter_mode == "new":
        command.extend(["-/filter_complex", str(filter_script_path)])
    else:
        command.extend(["-filter_complex_script", str(filter_script_path)])
    command.extend(
        [
            "-map",
            "[vout]",
            "-c:v",
            "libx264",
            "-threads",
            str(threads),
            "-x264-params",
            f"threads={threads}",
            "-preset",
            profile["preset"],
            "-crf",
            profile["crf"],
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
                "128k" if speed_mode == "fast_first" else "192k",
                "-shortest",
            ]
        )
    else:
        command.append("-an")
    command.append(str(output))
    return command


def append_video_tail(
    main_video: Path,
    ending_video: Path,
    output: Path,
    width: int,
    height: int,
    fps: int,
    threads: int | None = None,
) -> None:
    ffmpeg = resolve_binary("ffmpeg")
    output.parent.mkdir(parents=True, exist_ok=True)
    threads = max(1, threads or resolve_ffmpeg_threads())
    filter_complex = (
        f"[0:v]fps={fps},scale={width}:{height}:force_original_aspect_ratio=increase,"
        f"crop={width}:{height},setpts=PTS-STARTPTS,setsar=1,format=yuv420p[v0];"
        f"[1:v]fps={fps},scale={width}:{height}:force_original_aspect_ratio=increase,"
        f"crop={width}:{height},setpts=PTS-STARTPTS,setsar=1,format=yuv420p[v1];"
        "[0:a]aformat=sample_fmts=fltp:sample_rates=44100:channel_layouts=stereo,asetpts=PTS-STARTPTS[a0];"
        "[1:a]aformat=sample_fmts=fltp:sample_rates=44100:channel_layouts=stereo,asetpts=PTS-STARTPTS[a1];"
        "[v0][a0][v1][a1]concat=n=2:v=1:a=1[vout][aout]"
    )
    run_command(
        [
            ffmpeg,
            "-y",
            "-i",
            str(main_video),
            "-i",
            str(ending_video),
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
            "-map",
            "[aout]",
            "-c:v",
            "libx264",
            "-threads",
            str(threads),
            "-x264-params",
            f"threads={threads}",
            "-preset",
            "fast",
            "-crf",
            "20",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            str(output),
        ]
    )


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


def _video_encode_profile(speed_mode: str) -> dict[str, str]:
    if str(speed_mode).strip().lower() == "fast_first":
        return {"preset": "veryfast", "crf": "23"}
    return {"preset": "fast", "crf": "20"}


def _resolve_encode_slots() -> int:
    env_slots = _read_int_env("VIDEO_MATRIX_GLOBAL_ENCODE_SLOTS")
    if env_slots is not None:
        return max(1, env_slots)
    cpu_count = os.cpu_count() or 1
    return max(1, cpu_count // 4)


def _read_int_env(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return None
    try:
        return int(str(raw).strip())
    except ValueError:
        return None


_ENCODE_SLOTS = threading.BoundedSemaphore(max(1, _resolve_encode_slots()))

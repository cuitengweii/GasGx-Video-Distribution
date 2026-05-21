from __future__ import annotations

from pathlib import Path

import numpy as np


def detect_beat_grid(
    audio_path: Path,
    duration_hint: float,
    target_bpm_min: int = 120,
    target_bpm_max: int = 130,
    fallback_spacing: float = 0.48,
    mode: str = "auto",
) -> list[float]:
    if mode == "fallback":
        return _fallback_grid(duration_hint, spacing=fallback_spacing)
    try:
        import librosa
    except Exception:
        return _fallback_grid(duration_hint, spacing=fallback_spacing)

    try:
        y, sr = librosa.load(str(audio_path), sr=None, mono=True)
        tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr, trim=False)
        beat_times = librosa.frames_to_time(beat_frames, sr=sr).tolist()
        if not beat_times:
            return _fallback_grid(duration_hint)
        bounded_tempo = float(np.clip(tempo, target_bpm_min, target_bpm_max))
        spacing = 60.0 / bounded_tempo
        if len(beat_times) < 4:
            return _fallback_grid(duration_hint, spacing=spacing)
        return _bound_detected_grid(beat_times, duration_hint, spacing=spacing)
    except Exception:
        return _fallback_grid(duration_hint, spacing=fallback_spacing)


def _fallback_grid(duration_hint: float, spacing: float = 0.48) -> list[float]:
    total = max(float(duration_hint), 0.0)
    step = max(float(spacing), 0.05)
    current = 0.0
    beats: list[float] = []
    while current <= total + 0.0001:
        beats.append(round(current, 4))
        current += step
    if len(beats) < 2:
        beats.append(round(total if total > 0 else step, 4))
    return beats


def _bound_detected_grid(beat_times: list[float], duration_hint: float, *, spacing: float) -> list[float]:
    limit = max(float(duration_hint), 0.0)
    bounded = [round(float(point), 4) for point in beat_times if 0.0 <= float(point) <= limit + 0.0001]
    if not bounded or bounded[0] > 0.0001:
        bounded.insert(0, 0.0)
    if len(bounded) < 2:
        return _fallback_grid(duration_hint, spacing=spacing)
    return bounded

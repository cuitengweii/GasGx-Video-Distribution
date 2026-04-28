from __future__ import annotations

from pathlib import Path

import numpy as np


def detect_beat_grid(audio_path: Path, duration_hint: float, target_bpm_min: int = 120, target_bpm_max: int = 130) -> list[float]:
    try:
        import librosa
    except Exception:
        return _fallback_grid(duration_hint)

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
        return [round(float(point), 4) for point in beat_times]
    except Exception:
        return _fallback_grid(duration_hint)


def _fallback_grid(duration_hint: float, spacing: float = 0.48) -> list[float]:
    total = max(duration_hint, 12.0)
    current = 0.0
    beats: list[float] = []
    while current <= total + 0.0001:
        beats.append(round(current, 4))
        current += spacing
    return beats

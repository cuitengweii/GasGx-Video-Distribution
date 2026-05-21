from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np

from gasgx_distribution.video_matrix.beat import detect_beat_grid


def test_detect_beat_grid_auto_mode_respects_duration_hint(monkeypatch) -> None:
    fake_librosa = SimpleNamespace(
        load=lambda *_args, **_kwargs: (np.array([0.0], dtype=np.float32), 44100),
        beat=SimpleNamespace(
            beat_track=lambda *_args, **_kwargs: (128.0, np.array([1, 2, 3, 4, 5])),
        ),
        frames_to_time=lambda *_args, **_kwargs: np.array([0.12, 0.48, 0.96, 1.44, 2.2]),
    )
    monkeypatch.setitem(sys.modules, "librosa", fake_librosa)

    beat_grid = detect_beat_grid(Path("bgm.mp3"), duration_hint=1.0)

    assert beat_grid == [0.0, 0.12, 0.48, 0.96]


def test_detect_beat_grid_fallback_mode_respects_duration_hint() -> None:
    beat_grid = detect_beat_grid(Path("bgm.mp3"), duration_hint=1.0, fallback_spacing=0.5, mode="fallback")
    assert beat_grid == [0.0, 0.5, 1.0]


def test_detect_beat_grid_fallback_mode_keeps_two_points_for_short_hint() -> None:
    beat_grid = detect_beat_grid(Path("bgm.mp3"), duration_hint=0.2, fallback_spacing=0.48, mode="fallback")
    assert beat_grid == [0.0, 0.2]

from __future__ import annotations

from typing import Any

from cybercar import engine


def test_read_generic_file_inputs_snapshot_aggregates_frame_contexts() -> None:
    class FakeOwner:
        def __init__(self, payload: dict[str, Any], frames: list[Any] | None = None) -> None:
            self.payload = payload
            self._frames = frames or []

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return list(self._frames)

        def run_js(self, _script: str) -> dict[str, Any]:
            return dict(self.payload)

    frame = FakeOwner(
        {
            "total": 1,
            "max_count": 2,
            "sample": [{"name": "frame-upload"}],
            "root_count": 3,
        }
    )
    primary = FakeOwner({"total": 0, "max_count": 0, "sample": [], "root_count": 1}, frames=[frame])

    snapshot = engine._read_generic_file_inputs_snapshot(primary, None)

    assert snapshot["total"] == 1
    assert snapshot["max_count"] == 2
    assert snapshot["root_count"] == 4
    assert snapshot["sample"] == [{"name": "frame-upload"}]


def test_activate_upload_trigger_generic_scans_frame_contexts(monkeypatch) -> None:
    class FakeFrame:
        def __init__(self) -> None:
            self.run_js_calls = 0

        def run_js(self, script: str) -> str:
            self.run_js_calls += 1
            if "__TRIGGER_PATTERN__" in script:
                raise AssertionError("trigger pattern placeholder should be replaced")
            if "collectAllRoots" in script and "clicked:" in script:
                return "clicked:iframe-upload|storage|6400"
            return ""

    class FakeOwner:
        def __init__(self, frame: FakeFrame) -> None:
            self.frame = frame

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return [self.frame]

        def run_js(self, _script: str) -> str:
            return "not_found"

    frame = FakeFrame()
    owner = FakeOwner(frame)

    monkeypatch.setattr(engine, "_log", lambda *_args, **_kwargs: None)

    engine._activate_upload_trigger_generic(owner, None, platform_name="xiaohongshu")

    assert frame.run_js_calls == 1


def test_collect_upload_contexts_recurses_nested_frames() -> None:
    class FakeOwner:
        def __init__(self, frames: list[Any] | None = None) -> None:
            self._frames = frames or []

        def get_frames(self, timeout: float = 0) -> list[Any]:
            return list(self._frames)

    leaf = FakeOwner()
    child = FakeOwner([leaf])
    root = FakeOwner([child])

    contexts = engine._collect_upload_contexts(root, None)

    assert contexts == [root, child, leaf]

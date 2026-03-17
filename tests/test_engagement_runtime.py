from __future__ import annotations

from pathlib import Path

from cybercar.services.engagement import runtime


class _FakeWorkspace:
    def __init__(self, root: Path) -> None:
        self.root = root


def test_markdown_path_uses_platform_specific_filename(tmp_path: Path) -> None:
    workspace = _FakeWorkspace(tmp_path / "runtime")

    assert runtime._markdown_path(workspace, "douyin").name == "douyin_comment_reply_records.md"
    assert runtime._markdown_path(workspace, "kuaishou").name == "kuaishou_comment_reply_records.md"

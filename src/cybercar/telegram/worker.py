from __future__ import annotations

from .bootstrap import worker_main


def main(passthrough: list[str] | None = None) -> int:
    return worker_main(passthrough)

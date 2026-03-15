from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Optional, Union


_WINDOWS_ABS_RE = re.compile(r"^[A-Za-z]:[\\/]")


def is_windows_absolute_path(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(_WINDOWS_ABS_RE.match(text) or text.startswith("\\\\"))


def normalize_path_text(value: Any) -> str:
    text = str(value or "").strip().strip("\"'")
    if not text:
        return ""
    text = os.path.expandvars(text)
    if "\\" in text and not is_windows_absolute_path(text):
        text = text.replace("\\", "/")
    return text


def resolve_local_path(
    value: Any,
    *,
    base_dir: Union[str, os.PathLike],
    default: Any = "",
    allow_memory: bool = False,
) -> str:
    """Resolve a path string with cross-platform fallback behavior."""
    raw = normalize_path_text(value)
    fallback = normalize_path_text(default)

    if not raw:
        raw = fallback
    if not raw:
        return ""
    if allow_memory and raw == ":memory:":
        return raw

    if is_windows_absolute_path(raw) and os.name != "nt":
        # Non-Windows runners cannot use drive-letter paths.
        # Fall back to defaults when possible.
        if fallback and fallback != raw:
            raw = fallback
        else:
            raw = re.sub(r"^[A-Za-z]:[\\/]", "", raw).replace("\\", "/")

    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path(base_dir) / path
    return str(path.resolve())


def portable_relpath(
    value: Any,
    *,
    base_dir: Optional[Union[str, os.PathLike]] = None,
) -> str:
    text = normalize_path_text(value)
    if not text:
        return ""
    path = Path(text).expanduser()
    try:
        path = path.resolve()
    except Exception:
        pass
    if base_dir is not None:
        try:
            return path.relative_to(Path(base_dir).resolve()).as_posix()
        except Exception:
            pass
    return path.as_posix()

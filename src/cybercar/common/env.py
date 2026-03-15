from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Union

from .paths import resolve_local_path


def _find_dotenv(start_dir: Path) -> Path:
    current = start_dir.resolve()
    for parent in (current, *current.parents):
        candidate = parent / ".env"
        if candidate.exists():
            return candidate
    return Path()


def _strip_env_quotes(value: str) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _load_env_file_fallback(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key:
                continue
            os.environ.setdefault(key, _strip_env_quotes(value))
        return True
    except Exception:
        return False


def load_dotenv_if_available(env_path: Optional[Union[str, os.PathLike]] = None) -> bool:
    """Load .env when python-dotenv is installed.

    Returns True when the load call runs successfully, otherwise False.
    """
    resolved_path = ""
    if env_path is not None:
        resolved_path = resolve_local_path(env_path, base_dir=Path.cwd(), default="")
    elif not os.getenv("PYTHON_DOTENV_DISABLED"):
        found = _find_dotenv(Path.cwd())
        if found:
            resolved_path = str(found)

    try:
        from dotenv import load_dotenv
    except Exception:
        if resolved_path:
            return _load_env_file_fallback(Path(resolved_path))
        return _load_env_file_fallback(_find_dotenv(Path.cwd()))

    if resolved_path:
        path = Path(resolved_path)
        if path.exists():
            load_dotenv(path)
            return True
        return False

    load_dotenv()
    return True

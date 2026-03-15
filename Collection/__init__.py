from __future__ import annotations

from pathlib import Path
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)  # type: ignore[name-defined]
_src_pkg = Path(__file__).resolve().parents[1] / "src" / "Collection"
if _src_pkg.is_dir():
    __path__.append(str(_src_pkg))  # type: ignore[attr-defined]

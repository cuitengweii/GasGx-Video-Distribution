from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_IMPL_PATH = Path(r"D:\code\CyberCar\src\cybercar\wechat_visibility_batch.py")
_SPEC = spec_from_file_location("cybercar._wechat_visibility_batch_impl", _IMPL_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Unable to load WeChat batch implementation from {_IMPL_PATH}")

_MODULE = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

for _name, _value in vars(_MODULE).items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _value

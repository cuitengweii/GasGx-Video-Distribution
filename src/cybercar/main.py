"""Compatibility wrapper for legacy internal imports."""

from . import engine as _engine

globals().update({name: value for name, value in vars(_engine).items() if not name.startswith("__")})

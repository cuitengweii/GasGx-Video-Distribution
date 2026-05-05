from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DEFAULTS_PATH = ROOT / "config" / "video_matrix" / "defaults.json"
ENV_FONT_DIRS = "FONT_DIRS"
ENV_FONT_DIRS_ALT = "GASGX_FONT_DIRS"
DEFAULT_FONT_DIRS = [
    r"C:\Windows\Fonts",
    "/Library/Fonts",
    "/System/Library/Fonts",
    "/usr/share/fonts",
    "/usr/local/share/fonts",
]

FONT_FILES = [
    "msyh.ttc",
    "msyhbd.ttc",
    "simhei.ttf",
    "simsun.ttc",
    "NotoSansSC-VF.ttf",
    "Noto Sans SC (TrueType).otf",
    "Noto Sans SC Bold (TrueType).otf",
    "Noto Sans SC Medium (TrueType).otf",
    "arial.ttf",
    "arialbd.ttf",
    "segoeui.ttf",
    "segoeuib.ttf",
    "ariblk.ttf",
    "impact.ttf",
    "bahnschrift.ttf",
    "trebuc.ttf",
    "seguibl.ttf",
    "framd.ttf",
    "FRAHV.TTF",
    "georgia.ttf",
    "georgiab.ttf",
    "times.ttf",
    "cour.ttf",
    "consola.ttf",
    "comic.ttf",
    "comicbd.ttf",
    "COOPBL.TTF",
    "SHOWG.TTF",
    "lucon.ttf",
    "AlibabaPuHuiTi-Heavy.ttf",
    "Alibaba-PuHuiTi-Heavy.ttf",
    "SourceHanSansSC-Heavy.otf",
    "Source Han Sans SC Heavy.otf",
    "HarmonyOS_Sans_SC_Bold.ttf",
    "HarmonyOS Sans SC Bold.ttf",
    "YouSheBiaoTiHei.ttf",
    "YouSheBiaoTiHei-2.ttf",
]

FONT_FAMILY_FILES = {
    "microsoft yahei": ("msyh.ttc", "msyhbd.ttc"),
    "microsoft yahei bold": ("msyhbd.ttc", "msyh.ttc"),
    "microsoft jhenghei": ("msjh.ttc", "msjhbd.ttc"),
    "noto sans sc": ("NotoSansSC-VF.ttf", "Noto Sans SC (TrueType).otf"),
    "noto sans sc bold": ("Noto Sans SC Bold (TrueType).otf", "NotoSansSC-VF.ttf"),
    "arial black": ("ariblk.ttf", "arialbd.ttf"),
    "impact": ("impact.ttf", "ariblk.ttf"),
    "dinnextltpro-bold": ("DINNextLTPro-Bold.ttf", "bahnschrift.ttf"),
    "din condensed": ("DINNextLTPro-Bold.ttf", "bahnschrift.ttf"),
    "dinnextltpro-medium": ("DINNextLTPro-Medium.ttf", "bahnschrift.ttf"),
    "bahnschrift": ("bahnschrift.ttf",),
    "bahnschrift condensed": ("bahnschrift.ttf",),
    "arial narrow": ("arialn.ttf", "arial.ttf"),
    "trebuchet ms": ("trebuc.ttf",),
    "segoe ui black": ("seguibl.ttf", "segoeuib.ttf"),
    "franklin gothic heavy": ("FRAHV.TTF", "framd.ttf"),
    "georgia": ("georgia.ttf",),
    "times new roman": ("times.ttf",),
    "courier new": ("cour.ttf",),
    "consolas": ("consola.ttf",),
    "comic sans ms": ("comic.ttf", "comicbd.ttf"),
    "cooper black": ("COOPBL.TTF", "georgiab.ttf"),
    "showcard gothic": ("SHOWG.TTF", "ariblk.ttf"),
    "lucida console": ("lucon.ttf", "cour.ttf"),
    "english serif luxe": ("georgia.ttf", "times.ttf"),
    "english data mono": ("lucon.ttf", "cour.ttf"),
    "english pop comic": ("comic.ttf", "comicbd.ttf", "ariblk.ttf"),
    "retro bold": ("COOPBL.TTF", "georgiab.ttf"),
    "sign comic": ("SHOWG.TTF", "ariblk.ttf"),
    "simhei": ("simhei.ttf",),
    "simsun": ("simsun.ttc",),
    "alibaba puhuiti heavy": ("AlibabaPuHuiTi-Heavy.ttf", "Alibaba-PuHuiTi-Heavy.ttf", "msyhbd.ttc"),
    "source han sans heavy": ("SourceHanSansSC-Heavy.otf", "Source Han Sans SC Heavy.otf", "Noto Sans SC Bold (TrueType).otf"),
    "harmonyos sans sc bold": ("HarmonyOS_Sans_SC_Bold.ttf", "HarmonyOS Sans SC Bold.ttf", "Noto Sans SC Bold (TrueType).otf"),
    "youshebiaotihei": ("YouSheBiaoTiHei.ttf", "YouSheBiaoTiHei-2.ttf", "simhei.ttf"),
}


@lru_cache(maxsize=1)
def load_font_dirs() -> tuple[Path, ...]:
    dirs = _env_font_dirs()
    if dirs:
        return dirs
    dirs = _config_font_dirs()
    if dirs:
        return dirs
    return tuple(Path(item) for item in DEFAULT_FONT_DIRS)


def build_font_candidates() -> tuple[Path, ...]:
    candidates: list[Path] = []
    seen: set[str] = set()
    for font_dir in load_font_dirs():
        for filename in FONT_FILES:
            candidate = font_dir / filename
            key = str(candidate).lower()
            if key not in seen:
                candidates.append(candidate)
                seen.add(key)
    return tuple(candidates)


def build_font_candidates_for_family(font_family: str | None) -> tuple[Path, ...]:
    candidates: list[Path] = []
    for family in _font_family_names(font_family):
        for filename in FONT_FAMILY_FILES.get(family, ()):
            for font_dir in load_font_dirs():
                candidates.append(font_dir / filename)
    candidates.extend(build_font_candidates())
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate).lower()
        if key not in seen:
            unique.append(candidate)
            seen.add(key)
    return tuple(unique)


def _env_font_dirs() -> tuple[Path, ...]:
    raw = os.environ.get(ENV_FONT_DIRS) or os.environ.get(ENV_FONT_DIRS_ALT) or ""
    return _split_paths(raw)


def _config_font_dirs() -> tuple[Path, ...]:
    if not DEFAULTS_PATH.exists():
        return ()
    try:
        payload = json.loads(DEFAULTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return ()
    raw = payload.get("font_dirs")
    if isinstance(raw, list):
        return tuple(Path(str(item)).expanduser() for item in raw if str(item).strip())
    if isinstance(raw, str):
        return _split_paths(raw)
    return ()


def _split_paths(raw: str) -> tuple[Path, ...]:
    parts = [part.strip() for part in raw.split(os.pathsep) if part.strip()]
    return tuple(Path(part).expanduser() for part in parts)


def _font_family_names(font_family: str | None) -> list[str]:
    if not font_family:
        return []
    names: list[str] = []
    for item in str(font_family).split(","):
        name = item.strip().strip("'\"").strip().lower()
        if name and name not in {"sans-serif", "serif", "monospace"}:
            names.append(name)
    return names

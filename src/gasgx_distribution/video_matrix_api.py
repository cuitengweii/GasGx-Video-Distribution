from __future__ import annotations

import base64
import json
import os
import random
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlparse
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from PIL import Image
from pydantic import BaseModel

from . import service
from .video_matrix.cover import render_cover_preview_image
from .video_matrix.cover_templates import DEFAULT_COVER_TEMPLATE_ID, load_cover_templates, require_cover_template
from .video_matrix.ingestion import VIDEO_EXTENSIONS, ensure_category_dirs
from .video_matrix.pipeline import run_pipeline
from .video_matrix.settings import ProjectSettings
from .video_matrix.template_preview import render_video_template_preview_image
from .video_matrix.templates import DEFAULT_TEMPLATE_ID, load_templates, save_templates
from .video_matrix.ui_state import load_ui_state, save_ui_state


ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "config" / "video_matrix"
CONFIG_PATH = CONFIG_DIR / "defaults.json"
TEMPLATES_PATH = CONFIG_DIR / "templates.json"
COVER_TEMPLATES_PATH = CONFIG_DIR / "cover_templates.json"
UI_STATE_PATH = CONFIG_DIR / "ui_state.json"
BGM_LIBRARY_PATH = CONFIG_DIR / "bgm_library.json"
TMP_DIR = ROOT / "runtime" / "video_matrix" / "web_uploads"
BGM_DIR = ROOT / "runtime" / "video_matrix" / "bgm"
MODEL_IMAGE_DIR = ROOT / "runtime" / "video_matrix" / "modelimg"
ENDING_TEMPLATE_DIR = ROOT / "runtime" / "video_matrix" / "ending_template"
SIGNATURE_HISTORY_PATH = ROOT / "runtime" / "video_matrix" / "signature_history.json"
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
ENDING_TEMPLATE_EXTENSIONS = VIDEO_EXTENSIONS | IMAGE_EXTENSIONS
PIXABAY_INDUSTRY_TRACKS = [
    {"title": "Corporate Industry", "artist": "Ivan_Luzan", "duration": "2:29", "source_url": "https://pixabay.com/music/upbeat-corporate-industry-408747/"},
    {"title": "Industry", "artist": "MomotMusic", "duration": "2:11", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "AI Industry", "artist": "AlisiaBeats", "duration": "1:58", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "Simple Piano Melody", "artist": "Good_B_Music", "duration": "1:31", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "Industrial", "artist": "Audioknap", "duration": "1:43", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "Abandoned Industry", "artist": "HarumachiMusic", "duration": "1:42", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "Industrial", "artist": "Bransboynd", "duration": "2:12", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "Technology", "artist": "Crab_Audio", "duration": "2:05", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "Heavy Industry", "artist": "SPmusic", "duration": "3:22", "source_url": "https://pixabay.com/music/search/industry/"},
    {"title": "Visite rapide dans l'industrie", "artist": "Jean-Paul-V", "duration": "4:00", "source_url": "https://pixabay.com/music/search/industry/"},
]
PIXABAY_AUDIO_PATTERN = re.compile(r"https://cdn\.pixabay\.com/download/audio/[^\"'\\<>\s]+")

router = APIRouter(prefix="/api/video-matrix", tags=["video-matrix"])
_executor = ThreadPoolExecutor(max_workers=2)
_jobs: dict[str, dict[str, Any]] = {}


class OpenFolderPayload(BaseModel):
    path: str = ""


class MaterialCategoryPayload(BaseModel):
    label: str = ""


class MaterialCategoryRenamePayload(BaseModel):
    label: str = ""


class BgmDownloadPayload(BaseModel):
    url: str = ""
    filename: str = ""


def _video_matrix_app_setting(default: Any = None) -> Any:
    if service.brand_database_backend() != "supabase":
        return default
    try:
        return service._app_setting("video_matrix_state", default)
    except service.SupabaseError:
        return default


def _save_video_matrix_app_setting(payload: Any) -> bool:
    if service.brand_database_backend() != "supabase":
        return False
    try:
        service._save_app_setting("video_matrix_state", payload)
    except service.SupabaseError:
        return False
    return True


def _persist_video_matrix_state(payload: Any) -> None:
    if service.brand_database_backend() != "supabase":
        raise HTTPException(status_code=400, detail="Video matrix database backend is not enabled")
    try:
        service._save_app_setting("video_matrix_state", payload)
    except service.SupabaseError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to save video matrix state to database: {exc}") from exc


def _local_video_matrix_state() -> dict[str, Any]:
    return {
        "settings": _settings_payload(_local_settings()),
        "ui_state": load_ui_state(UI_STATE_PATH),
        "templates": load_templates(TEMPLATES_PATH),
        "cover_templates": load_cover_templates(COVER_TEMPLATES_PATH),
        "bgm_library": _load_json(BGM_LIBRARY_PATH, {}),
        "signature_history": _load_json(SIGNATURE_HISTORY_PATH, []),
    }


def _next_material_category_id(categories: list[dict[str, Any]]) -> str:
    used = {str(item.get("id") or "") for item in categories if isinstance(item, dict)}
    for code in range(ord("A"), ord("Z") + 1):
        category_id = f"category_{chr(code)}"
        if category_id not in used:
            return category_id
    raise HTTPException(status_code=400, detail="No category ids are available")


def _rename_category_dir(source_root: Path, old_id: str, new_id: str) -> None:
    old_path = source_root / old_id
    new_path = source_root / new_id
    if old_path == new_path or not old_path.exists() or new_path.exists():
        return
    old_path.rename(new_path)


def _apply_category_id_map(payload: dict[str, Any], id_map: dict[str, str]) -> None:
    if not id_map:
        return
    recent_limits = dict(payload.get("recent_limits") or {})
    for old_id, new_id in id_map.items():
        if old_id in recent_limits and new_id not in recent_limits:
            recent_limits[new_id] = recent_limits.pop(old_id)
        elif old_id in recent_limits:
            recent_limits.pop(old_id, None)
    payload["recent_limits"] = recent_limits
    sequence = []
    for item in payload.get("composition_sequence") or []:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        row["category_id"] = id_map.get(str(row.get("category_id") or ""), row.get("category_id"))
        sequence.append(row)
    if sequence:
        payload["composition_sequence"] = sequence


def _legacy_material_category_id_map(categories: list[dict[str, Any]]) -> dict[str, str]:
    category_ids = {str(item.get("id") or "") for item in categories if isinstance(item, dict)}
    id_map: dict[str, str] = {}
    for index, letter in enumerate(("I", "J"), start=1):
        legacy_id = f"category_custom_{index}"
        target_id = f"category_{letter}"
        if target_id in category_ids:
            id_map[legacy_id] = target_id
    return id_map


def _has_legacy_category_ids(payload: dict[str, Any], id_map: dict[str, str]) -> bool:
    if not id_map:
        return False
    if any(key in dict(payload.get("recent_limits") or {}) for key in id_map):
        return True
    if any(str(item) in id_map for item in payload.get("active_category_ids") or []):
        return True
    return any(
        isinstance(item, dict) and str(item.get("category_id") or "") in id_map
        for item in payload.get("composition_sequence") or []
    )


def _remove_website_url_state(current: dict[str, Any]) -> bool:
    changed = False
    settings_payload = dict(current.get("settings") or {})
    if "website_url" in settings_payload:
        settings_payload.pop("website_url", None)
        current["settings"] = settings_payload
        changed = True
    ui_state = dict(current.get("ui_state") or {})
    if "gasgx.com/roi" in str(ui_state.get("cta") or ""):
        ui_state["cta"] = ""
        current["ui_state"] = ui_state
        changed = True
    templates = current.get("cover_templates")
    if isinstance(templates, dict):
        next_templates = {}
        for template_id, template in templates.items():
            if isinstance(template, dict):
                item = dict(template)
                if str(item.get("cta") or "").strip().lower() in {"www.gasgx.com/roi", "https://www.gasgx.com/roi"}:
                    item["cta"] = ""
                    changed = True
                next_templates[template_id] = item
            else:
                next_templates[template_id] = template
        if changed:
            current["cover_templates"] = next_templates
    return changed


def _remove_retired_video_matrix_state(current: dict[str, Any]) -> bool:
    changed = False
    settings_payload = dict(current.get("settings") or {})
    for key in ("website_url", "hud_enable_live_data", "hud_fixed_formulas", "hud_sources"):
        if key in settings_payload:
            settings_payload.pop(key, None)
            changed = True
    if changed:
        current["settings"] = settings_payload
    ui_state = dict(current.get("ui_state") or {})
    for key in ("cta", "transcript_text", "use_live_data"):
        if key in ui_state:
            ui_state.pop(key, None)
            changed = True
    if changed:
        current["ui_state"] = ui_state
    return changed


def _normalize_material_category_ids(current: dict[str, Any]) -> bool:
    settings_payload = dict(current.get("settings") or {})
    categories = [dict(item) for item in settings_payload.get("material_categories") or [] if isinstance(item, dict)]
    if not categories:
        return False
    changed = False
    id_map: dict[str, str] = {}
    used = {str(item.get("id") or "") for item in categories if isinstance(item, dict) and not str(item.get("id") or "").startswith("category_custom_")}
    for category in categories:
        old_id = str(category.get("id") or "").strip()
        if not old_id.startswith("category_custom_"):
            continue
        new_id = _next_material_category_id([{"id": item} for item in used])
        used.add(new_id)
        category["id"] = new_id
        id_map[old_id] = new_id
        changed = True
    ui_state = dict(current.get("ui_state") or {})
    legacy_map = _legacy_material_category_id_map(categories)
    if legacy_map and (_has_legacy_category_ids(settings_payload, legacy_map) or _has_legacy_category_ids(ui_state, legacy_map)):
        id_map.update({key: value for key, value in legacy_map.items() if key not in id_map})
        changed = True
    if not changed:
        return False
    settings_payload["material_categories"] = categories
    _apply_category_id_map(settings_payload, id_map)
    _apply_category_id_map(ui_state, id_map)
    if isinstance(settings_payload.get("active_category_ids"), list):
        settings_payload["active_category_ids"] = [id_map.get(str(item), str(item)) for item in settings_payload["active_category_ids"]]
    if isinstance(ui_state.get("active_category_ids"), list):
        ui_state["active_category_ids"] = [id_map.get(str(item), str(item)) for item in ui_state["active_category_ids"]]
    source_root = Path(str(settings_payload.get("source_root") or _settings().source_root)).expanduser()
    for old_id, new_id in id_map.items():
        _rename_category_dir(source_root, old_id, new_id)
    current["settings"] = settings_payload
    current["ui_state"] = ui_state
    return True


def _rename_material_category_in_settings(settings_payload: dict[str, Any], category_id: str, label: str) -> dict[str, str]:
    categories = [dict(item) for item in settings_payload.get("material_categories") or [] if isinstance(item, dict)]
    for category in categories:
        if str(category.get("id") or "") == category_id:
            category["label"] = label
            settings_payload["material_categories"] = categories
            return {"id": category_id, "label": label}
    raise HTTPException(status_code=404, detail="Unknown category")


def _complete_video_matrix_state(stored: dict[str, Any] | None) -> tuple[dict[str, Any], bool]:
    current = dict(stored or {})
    defaults = _local_video_matrix_state()
    changed = False
    for key, value in defaults.items():
        if not current.get(key):
            current[key] = value
            changed = True
    current["settings"] = _merge_settings_payload(dict(current.get("settings") or {}))
    changed = _remove_website_url_state(current) or changed
    changed = _remove_retired_video_matrix_state(current) or changed
    changed = _normalize_material_category_ids(current) or changed
    return current, changed


@router.get("/state")
def get_state() -> dict[str, Any]:
    stored = _video_matrix_app_setting()
    if isinstance(stored, dict):
        stored, changed = _complete_video_matrix_state(stored)
        if changed:
            _save_video_matrix_app_setting(stored)
        settings_payload = dict(stored["settings"])
        source_root = Path(settings_payload.get("source_root") or _settings().source_root)
        categories = settings_payload.get("material_categories") or []
        BGM_DIR.mkdir(parents=True, exist_ok=True)
        ENDING_TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
        return {
            "settings": settings_payload,
            "ui_state": stored.get("ui_state") or {},
            "templates": stored.get("templates") or {},
            "cover_templates": stored.get("cover_templates") or {},
            "bgm_library": stored.get("bgm_library") or {},
            "local_bgm_dir": str(BGM_DIR),
            "local_bgm": [path.name for path in _list_local_bgm_files(BGM_DIR)],
            "ending_template_dir": str(ENDING_TEMPLATE_DIR),
            "ending_templates": _list_ending_templates(),
            "category_counts": _count_category_files(source_root, categories),
            "source_dirs": {category["id"]: str(source_root / category["id"]) for category in categories},
            "source_videos": _list_source_preview_videos(source_root, categories),
        }
    local_state = _local_video_matrix_state()
    if _normalize_material_category_ids(local_state):
        CONFIG_PATH.write_text(json.dumps(local_state["settings"], indent=2, ensure_ascii=False), encoding="utf-8")
        save_ui_state(UI_STATE_PATH, local_state.get("ui_state") or {})
    settings = _settings_from_payload(local_state["settings"])
    ensure_category_dirs(settings.source_root, settings.material_categories)
    BGM_DIR.mkdir(parents=True, exist_ok=True)
    ENDING_TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "settings": _settings_payload(settings),
        "ui_state": load_ui_state(UI_STATE_PATH),
        "templates": load_templates(TEMPLATES_PATH),
        "cover_templates": load_cover_templates(COVER_TEMPLATES_PATH),
        "bgm_library": _load_json(BGM_LIBRARY_PATH, {}),
        "local_bgm_dir": str(BGM_DIR),
        "local_bgm": [path.name for path in _list_local_bgm_files(BGM_DIR)],
        "ending_template_dir": str(ENDING_TEMPLATE_DIR),
        "ending_templates": _list_ending_templates(),
        "category_counts": _count_category_files(settings.source_root, settings.material_categories),
        "source_dirs": {
            category["id"]: str(settings.source_root / category["id"])
            for category in settings.material_categories
        },
        "source_videos": _list_source_preview_videos(settings.source_root, settings.material_categories),
    }


@router.post("/state")
def post_state(payload: dict[str, Any]) -> dict[str, Any]:
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        state = dict(current.get("ui_state") or {})
        state.update(payload)
        current["ui_state"] = state
        if _save_video_matrix_app_setting(current):
            return {"ok": True, "ui_state": state}
        save_ui_state(UI_STATE_PATH, state)
        return {"ok": True, "ui_state": state}
    state = load_ui_state(UI_STATE_PATH)
    state.update(payload)
    save_ui_state(UI_STATE_PATH, state)
    return {"ok": True, "ui_state": state}


@router.post("/material-categories")
def add_material_category(payload: MaterialCategoryPayload) -> dict[str, Any]:
    label = payload.label.strip()
    if not label:
        raise HTTPException(status_code=400, detail="label is required")
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        settings_payload = dict(current.get("settings") or _settings_payload(_settings()))
        categories = list(settings_payload.get("material_categories") or [])
        category = {"id": _next_material_category_id(categories), "label": label}
        categories.append(category)
        settings_payload["material_categories"] = categories
        recent_limits = dict(settings_payload.get("recent_limits") or {})
        recent_limits[category["id"]] = 6
        settings_payload["recent_limits"] = recent_limits
        current["settings"] = settings_payload
        _save_video_matrix_app_setting(current)
        ensure_category_dirs(Path(settings_payload["source_root"]), categories)
        return {"ok": True, "category": category}
    config = _load_json(CONFIG_PATH, {})
    local_state = {"settings": config, "ui_state": load_ui_state(UI_STATE_PATH)}
    if _normalize_material_category_ids(local_state):
        config = local_state["settings"]
        save_ui_state(UI_STATE_PATH, local_state.get("ui_state") or {})
    categories = list(config.get("material_categories") or [])
    category_id = _next_material_category_id(categories)
    categories.append({"id": category_id, "label": label})
    config["material_categories"] = categories
    recent_limits = dict(config.get("recent_limits") or {})
    recent_limits[category_id] = 6
    config["recent_limits"] = recent_limits
    CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    settings = ProjectSettings.from_file(CONFIG_PATH)
    ensure_category_dirs(settings.source_root, settings.material_categories)
    return {"ok": True, "category": {"id": category_id, "label": label}}


@router.post("/material-categories/{category_id}")
def rename_material_category(category_id: str, payload: MaterialCategoryRenamePayload) -> dict[str, Any]:
    label = payload.label.strip()
    if not label:
        raise HTTPException(status_code=400, detail="label is required")
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        settings_payload = dict(current.get("settings") or _settings_payload(_settings()))
        category = _rename_material_category_in_settings(settings_payload, category_id, label)
        current["settings"] = settings_payload
        _persist_video_matrix_state(current)
        return {"ok": True, "category": category, "storage": "database"}
    config = _load_json(CONFIG_PATH, {})
    category = _rename_material_category_in_settings(config, category_id, label)
    CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True, "category": category, "storage": "file"}


@router.post("/templates/{template_id}")
def save_video_template(template_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        templates = dict(current.get("templates") or {})
        templates[template_id] = payload
        current["templates"] = templates
        _persist_video_matrix_state(current)
        return {"ok": True, "template_id": template_id, "template": payload, "storage": "database"}
    templates = load_templates(TEMPLATES_PATH)
    templates[template_id] = payload
    save_templates(TEMPLATES_PATH, templates)
    return {"ok": True, "template_id": template_id, "template": payload, "storage": "local"}


@router.post("/template-preview")
def template_preview(payload: dict[str, Any]) -> dict[str, str]:
    background = _cover_preview_background(str(payload.get("background_image_url") or ""))
    image = render_video_template_preview_image(
        _settings(),
        payload.get("template") or {},
        hud_text=str(payload.get("hud_text") or ""),
        slogan=str(payload.get("slogan") or ""),
        title=str(payload.get("title") or ""),
        background=background,
    )
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return {"data_url": f"data:image/png;base64,{encoded}"}


@router.post("/cover-templates/{template_id}")
def save_cover_template(template_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        templates = dict(current.get("cover_templates") or {})
        templates[template_id] = payload
        current["cover_templates"] = templates
        _persist_video_matrix_state(current)
        return {"ok": True, "template_id": template_id, "template": payload, "storage": "database"}
    templates = load_cover_templates(COVER_TEMPLATES_PATH)
    templates[template_id] = payload
    COVER_TEMPLATES_PATH.write_text(json.dumps(templates, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True, "template_id": template_id, "template": payload, "storage": "local"}


@router.put("/cover-templates")
def replace_cover_templates(payload: dict[str, Any]) -> dict[str, Any]:
    templates = payload.get("templates") or {}
    if not isinstance(templates, dict) or not templates:
        raise HTTPException(status_code=400, detail="templates must be a non-empty object")
    selected_cover = str(payload.get("selected_cover") or next(iter(templates)))
    if selected_cover not in templates:
        selected_cover = next(iter(templates))
    templates = {str(key): dict(value) for key, value in templates.items()}
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        current["cover_templates"] = templates
        current.setdefault("ui_state", {})["cover_template_id"] = selected_cover
        _persist_video_matrix_state(current)
        return {"ok": True, "cover_templates": templates, "selected_cover": selected_cover, "storage": "database"}
    COVER_TEMPLATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    COVER_TEMPLATES_PATH.write_text(json.dumps(templates, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True, "cover_templates": templates, "selected_cover": selected_cover, "storage": "local"}


@router.post("/cover-preview")
def cover_preview(payload: dict[str, Any]) -> dict[str, str]:
    settings = _settings()
    template = payload.get("template") or {}
    headline = str(payload.get("headline") or "Gas Engines That Turn Field Gas Into Power")
    subhead = str(payload.get("subhead") or "Generator sets for onsite industrial load")
    background = _cover_preview_background(str(payload.get("background_image_url") or ""))
    image = render_cover_preview_image(
        settings,
        template,
        headline=headline,
        subhead=subhead,
        hud_lines=_hud_lines(str(payload.get("hud_text") or "")),
        background=background,
    )
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return {"data_url": f"data:image/png;base64,{encoded}"}


def _cover_preview_background(url: str) -> Image.Image | None:
    parsed = urlparse(url or "")
    if parsed.path.startswith("/api/video-matrix/model-images/"):
        target = MODEL_IMAGE_DIR / Path(unquote(parsed.path).rsplit("/", 1)[-1]).name
        if target.exists() and target.suffix.lower() in IMAGE_EXTENSIONS:
            return Image.open(target).convert("RGB")
    return None


@router.post("/open-folder")
def open_folder(payload: OpenFolderPayload) -> dict[str, Any]:
    if not payload.path:
        raise HTTPException(status_code=400, detail="path is required")
    path = Path(payload.path).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    os.startfile(str(path))
    return {"ok": True, "path": str(path)}


@router.get("/preview-file")
def preview_file(path: str) -> FileResponse:
    if not path:
        raise HTTPException(status_code=400, detail="path is required")
    video_path = Path(path).expanduser().resolve()
    if not video_path.exists() or not video_path.is_file():
        raise HTTPException(status_code=404, detail="Video file not found")
    if video_path.suffix.lower() not in VIDEO_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported preview file")
    return FileResponse(video_path, media_type="video/mp4", filename=video_path.name)


@router.get("/model-images")
def model_images() -> dict[str, Any]:
    MODEL_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    images = [
        {"name": path.name, "url": f"/api/video-matrix/model-images/{path.name}"}
        for path in sorted(MODEL_IMAGE_DIR.iterdir(), key=lambda item: item.stat().st_mtime, reverse=True)
        if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
    ]
    return {"directory": str(MODEL_IMAGE_DIR), "images": images}


@router.get("/model-images/{filename}")
def model_image_file(filename: str) -> FileResponse:
    target = MODEL_IMAGE_DIR / Path(filename).name
    if not target.exists() or target.suffix.lower() not in IMAGE_EXTENSIONS:
        raise HTTPException(status_code=404, detail="Model image not found")
    return FileResponse(target, media_type=_image_media_type(target), filename=target.name)


@router.get("/ending-templates/{filename}")
def ending_template_file(filename: str) -> FileResponse:
    target = ENDING_TEMPLATE_DIR / Path(filename).name
    if not target.exists() or target.suffix.lower() not in ENDING_TEMPLATE_EXTENSIONS:
        raise HTTPException(status_code=404, detail="Ending template not found")
    media_type = "video/mp4" if target.suffix.lower() in VIDEO_EXTENSIONS else _image_media_type(target)
    return FileResponse(target, media_type=media_type, filename=target.name)


@router.get("/bgm/{filename}")
def local_bgm_file(filename: str) -> FileResponse:
    target = BGM_DIR / Path(filename).name
    if not target.exists() or target.suffix.lower() not in {".mp3", ".wav", ".m4a"}:
        raise HTTPException(status_code=404, detail="BGM file not found")
    return FileResponse(target, media_type=_audio_media_type(target), filename=target.name)


@router.get("/pixabay/industry")
def pixabay_industry_tracks() -> dict[str, Any]:
    tracks = []
    for track in PIXABAY_INDUSTRY_TRACKS[:10]:
        item = dict(track)
        item["audio_url"] = _resolve_pixabay_audio_url(str(item.get("source_url") or ""))
        tracks.append(item)
    return {"tracks": tracks, "source_url": "https://pixabay.com/music/search/industry/"}


@lru_cache(maxsize=64)
def _resolve_pixabay_audio_url(source_url: str) -> str:
    if not source_url.startswith("https://pixabay.com/music/"):
        return ""
    request = UrlRequest(
        source_url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "Chrome/124.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=12) as response:
            html = response.read(2 * 1024 * 1024).decode("utf-8", errors="ignore")
    except Exception:
        return ""
    match = PIXABAY_AUDIO_PATTERN.search(html)
    return match.group(0).replace("\\u0026", "&") if match else ""


@router.post("/bgm/download")
def download_bgm(payload: BgmDownloadPayload) -> dict[str, Any]:
    url = payload.url.strip()
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=400, detail="A valid http/https audio URL is required")
    filename = _safe_bgm_filename(payload.filename or unquote(Path(parsed.path).name))
    if not filename:
        raise HTTPException(status_code=400, detail="The audio URL must include a supported filename")
    target = BGM_DIR / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    request = UrlRequest(url, headers={"User-Agent": "GasGx Video Distribution/0.1"})
    try:
        with urlopen(request, timeout=30) as response:
            content_type = response.headers.get("Content-Type", "")
            if content_type and not content_type.lower().startswith(("audio/", "application/octet-stream")):
                raise HTTPException(status_code=400, detail="The URL did not return an audio file")
            target.write_bytes(response.read(80 * 1024 * 1024))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to download audio: {exc}") from exc
    return {"ok": True, "filename": target.name, "path": str(target.resolve())}


@router.post("/generate")
async def generate(
    payload: str = Form(...),
    bgm_file: UploadFile | None = File(None),
    source_files: list[UploadFile] | None = File(None),
) -> dict[str, Any]:
    request = json.loads(payload)
    job_id = uuid.uuid4().hex[:12]
    temp_root = TMP_DIR / job_id
    temp_root.mkdir(parents=True, exist_ok=True)
    bgm_path = await _resolve_bgm_path(request, temp_root, bgm_file)
    source_root = await _resolve_source_root(request, temp_root, source_files or [])
    _jobs[job_id] = {"status": "queued", "progress": 0, "message": "Queued", "assets": [], "error": ""}
    if service.brand_database_backend() == "supabase":
        service._brand_supabase().insert(
            "video_matrix_jobs",
            {"job_key": job_id, "status": "queued", "progress": 0, "message": "Queued", "request_json": request, "assets_json": [], "error": "", "created_at": service.now_ts(), "updated_at": service.now_ts()},
        )
    _executor.submit(_run_generate_job, job_id, request, bgm_path, source_root)
    return {"job_id": job_id}


@router.get("/jobs/{job_id}")
def job_status(job_id: str) -> dict[str, Any]:
    if service.brand_database_backend() == "supabase":
        row = service._brand_supabase().select_one("video_matrix_jobs", filters={"job_key": job_id})
        if row is None:
            raise HTTPException(status_code=404, detail="Unknown job")
        return {
            "status": row.get("status"),
            "progress": row.get("progress"),
            "message": row.get("message"),
            "assets": row.get("assets_json") or [],
            "error": row.get("error") or "",
        }
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Unknown job")
    return _jobs[job_id]


def _run_generate_job(job_id: str, request: dict[str, Any], bgm_path: Path, source_root: Path | None) -> None:
    try:
        settings = _settings()
        if request.get("video_duration_min") is not None:
            settings.video_duration_min = max(
                1.0,
                float(request.get("video_duration_min") or settings.video_duration_min),
            )
        if request.get("video_duration_max") is not None:
            settings.video_duration_max = max(
                settings.video_duration_min,
                float(request.get("video_duration_max") or settings.video_duration_max),
            )
        if request.get("target_fps") is not None:
            settings.target_fps = 60 if int(request.get("target_fps") or settings.target_fps) == 60 else 30
        request = _normalize_request_category_ids(request, settings)
        generation_history = _load_generation_history(settings.variant_history_limit)
        existing_signatures = _load_signature_history(settings) | set(generation_history["signatures"])
        video_state, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        templates = video_state.get("templates") or load_templates(TEMPLATES_PATH)
        cover_templates = video_state.get("cover_templates") or load_cover_templates(COVER_TEMPLATES_PATH)
        template_id = str(request.get("template_id") or DEFAULT_TEMPLATE_ID)
        cover_template_id = str(request.get("cover_template_id") or DEFAULT_COVER_TEMPLATE_ID)
        template_config = templates.get(template_id) or next(iter(templates.values()))
        cover_template_config = require_cover_template(cover_templates, cover_template_id)
        ending_cover_template_config = request.get("ending_cover_template") if isinstance(request.get("ending_cover_template"), dict) else None
        ending_template_path = _resolve_ending_template_path(request)
        recent_limits = request.get("recent_limits") if request.get("source_mode") == "Category folders" else None
        active_category_ids = request.get("active_category_ids") if request.get("source_mode") == "Category folders" else None

        def progress(stage: str, value: float, message: str) -> None:
            _jobs[job_id].update({"status": "running", "stage": stage, "progress": value, "message": message})
            if service.brand_database_backend() == "supabase":
                service._brand_supabase().update("video_matrix_jobs", {"status": "running", "stage": stage, "progress": value, "message": message, "updated_at": service.now_ts()}, filters={"job_key": job_id})

        assets = run_pipeline(
            settings=settings,
            bgm_path=bgm_path,
            output_count=int(request.get("output_count") or settings.output_count),
            source_root=source_root,
            output_root=Path(request["output_root"]).expanduser().resolve() if request.get("output_root") else None,
            progress_callback=progress,
            output_types=set(request.get("output_options") or ["mp4"]),
            copy_language=str(request.get("copy_language") or "zh"),
            max_workers=int(request.get("max_workers") or 3),
            recent_limits=recent_limits,
            active_category_ids=active_category_ids,
            template_config=template_config,
            cover_template_id=cover_template_id,
            cover_template_config=cover_template_config,
            ending_cover_template_config=ending_cover_template_config,
            composition_sequence=_request_composition_sequence(request, settings),
            existing_signatures=existing_signatures if settings.variant_history_enabled else None,
            recent_clip_ids=set(generation_history["clip_ids"]),
            recent_segment_keys=set(generation_history["segment_keys"]),
            ending_template_path=ending_template_path,
            text_overrides={
                "headline": str(request.get("headline") or ""),
                "subhead": str(request.get("subhead") or ""),
                "hud_text": str(request.get("hud_text") or ""),
                "follow_text": str(request.get("follow_text") or ""),
            },
        )
        complete_payload = {
            "status": "complete",
            "progress": 1.0,
            "message": f"Completed {len(assets)} exports",
            "assets": [
                {
                    "video_path": str(asset.video_path),
                    "cover_path": str(asset.cover_path) if asset.cover_path else "",
                    "copy_path": str(asset.copy_path) if asset.copy_path else "",
                    "manifest_path": str(asset.manifest_path) if asset.manifest_path else "",
                }
                for asset in assets
            ],
        }
        _jobs[job_id].update(complete_payload)
        if service.brand_database_backend() == "supabase":
            service._brand_supabase().update("video_matrix_jobs", {"status": "complete", "progress": 1, "message": complete_payload["message"], "assets_json": complete_payload["assets"], "updated_at": service.now_ts()}, filters={"job_key": job_id})
        _save_generation_history(job_id, request, bgm_path, assets, settings, template_id, cover_template_id)
        _save_signature_history(settings, existing_signatures | {asset.variant.signature for asset in assets})
        _save_ui_state(_ui_state_from_request(request))
    except Exception as exc:  # pragma: no cover - surfaced through job endpoint
        _jobs[job_id].update({"status": "error", "error": str(exc), "message": str(exc)})
        if service.brand_database_backend() == "supabase":
            service._brand_supabase().update("video_matrix_jobs", {"status": "error", "error": str(exc), "message": str(exc), "updated_at": service.now_ts()}, filters={"job_key": job_id})


async def _resolve_bgm_path(request: dict[str, Any], temp_root: Path, bgm_file: UploadFile | None) -> Path:
    if request.get("bgm_source") == "Local library":
        filename = Path(str(request.get("bgm_library_id") or "")).name
        local_files = _list_local_bgm_files(BGM_DIR)
        candidate = BGM_DIR / filename if filename else None
        if candidate is not None and candidate.exists():
            return candidate.resolve()
        if local_files:
            recent_bgm = _recent_bgm_names()
            fresh = [path for path in local_files if path.name not in recent_bgm]
            return random.choice(fresh or local_files).resolve()
    if bgm_file is None:
        raise HTTPException(status_code=400, detail="BGM file is required")
    target = temp_root / "bgm" / Path(bgm_file.filename or "bgm.mp3").name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(await bgm_file.read())
    return target


async def _resolve_source_root(request: dict[str, Any], temp_root: Path, source_files: list[UploadFile]) -> Path | None:
    if request.get("source_mode") != "Upload files":
        return None
    if not source_files:
        raise HTTPException(status_code=400, detail="Source files are required")
    source_root = temp_root / "incoming"
    source_root.mkdir(parents=True, exist_ok=True)
    for upload in source_files:
        filename = Path(upload.filename or "source.mp4").name
        if Path(filename).suffix.lower() not in VIDEO_EXTENSIONS:
            continue
        (source_root / filename).write_bytes(await upload.read())
    return source_root


def _settings() -> ProjectSettings:
    if service.brand_database_backend() == "supabase":
        stored = _video_matrix_app_setting({})
        if isinstance(stored, dict) and stored.get("settings"):
            return _settings_from_payload(_merge_settings_payload(dict(stored.get("settings") or {})))
    return _local_settings()


def _local_settings() -> ProjectSettings:
    return ProjectSettings.from_file(CONFIG_PATH)


def _settings_payload(settings: ProjectSettings) -> dict[str, Any]:
    return {
        "project_name": settings.project_name,
        "source_root": str(settings.source_root),
        "library_root": str(settings.library_root),
        "output_root": str(settings.output_root),
        "output_count": settings.output_count,
        "target_width": settings.target_width,
        "target_height": settings.target_height,
        "target_fps": settings.target_fps,
        "video_duration_min": settings.video_duration_min,
        "video_duration_max": settings.video_duration_max,
        "default_title_prefix": settings.default_title_prefix,
        "recent_limits": settings.recent_limits,
        "material_categories": settings.material_categories,
        "slogans": settings.slogans,
        "titles": settings.titles,
        "composition_sequence": settings.composition_sequence,
        "beat_detection": settings.beat_detection,
        "max_variant_attempts": settings.max_variant_attempts,
        "variant_history_enabled": settings.variant_history_enabled,
        "variant_history_limit": settings.variant_history_limit,
        "enhancement_modules": settings.enhancement_modules,
        "copy_mode": settings.copy_mode,
    }


def _merge_settings_payload(payload: dict[str, Any]) -> dict[str, Any]:
    merged = _settings_payload(_local_settings())
    payload.pop("website_url", None)
    payload.pop("hud_enable_live_data", None)
    payload.pop("hud_fixed_formulas", None)
    payload.pop("hud_sources", None)
    merged.update(payload)
    return merged


def _settings_from_payload(payload: dict[str, Any]) -> ProjectSettings:
    settings = _local_settings()
    settings.project_name = str(payload.get("project_name") or settings.project_name)
    settings.source_root = Path(str(payload.get("source_root") or settings.source_root)).expanduser().resolve()
    settings.library_root = Path(str(payload.get("library_root") or settings.library_root)).expanduser().resolve()
    settings.output_root = Path(str(payload.get("output_root") or settings.output_root)).expanduser().resolve()
    settings.output_count = int(payload.get("output_count") or settings.output_count)
    settings.target_width = int(payload.get("target_width") or settings.target_width)
    settings.target_height = int(payload.get("target_height") or settings.target_height)
    settings.target_fps = int(payload.get("target_fps") or settings.target_fps)
    settings.recent_limits = {str(key): int(value) for key, value in dict(payload.get("recent_limits") or {}).items()}
    settings.material_categories = [dict(item) for item in payload.get("material_categories") or settings.material_categories if isinstance(item, dict)]
    settings.video_duration_min = float(payload.get("video_duration_min") or settings.video_duration_min)
    settings.video_duration_max = float(payload.get("video_duration_max") or settings.video_duration_max)
    settings.default_title_prefix = str(payload.get("default_title_prefix") or settings.default_title_prefix)
    settings.slogans = list(payload.get("slogans") or settings.slogans)
    settings.titles = list(payload.get("titles") or settings.titles)
    settings.composition_sequence = list(payload.get("composition_sequence") or settings.composition_sequence)
    settings.beat_detection = dict(payload.get("beat_detection") or settings.beat_detection)
    settings.max_variant_attempts = int(payload.get("max_variant_attempts") or settings.max_variant_attempts)
    settings.variant_history_enabled = bool(payload.get("variant_history_enabled", settings.variant_history_enabled))
    settings.variant_history_limit = int(payload.get("variant_history_limit") or settings.variant_history_limit)
    settings.enhancement_modules = dict(payload.get("enhancement_modules") or settings.enhancement_modules)
    settings.copy_mode = str(payload.get("copy_mode") or settings.copy_mode)
    return settings


def _load_signature_history(settings: ProjectSettings) -> set[str]:
    if not settings.variant_history_enabled:
        return set()
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        raw = current.get("signature_history") or []
    else:
        raw = _load_json(SIGNATURE_HISTORY_PATH, [])
    if not isinstance(raw, list):
        return set()
    return {str(item) for item in raw if str(item).strip()}


def _recent_bgm_names(limit: int = 5000) -> set[str]:
    if service.brand_database_backend() != "supabase":
        return set()
    try:
        rows = service._brand_supabase().select_where(
            "video_matrix_generation_runs",
            params={"limit": str(max(1, limit))},
            order="created_at.desc",
        )
    except service.SupabaseError:
        return set()
    return {str(row.get("bgm_filename") or "").strip() for row in rows if str(row.get("bgm_filename") or "").strip()}


def _load_generation_history(limit: int) -> dict[str, set[str]]:
    history: dict[str, set[str]] = {"signatures": set(), "clip_ids": set(), "segment_keys": set(), "bgm_names": set()}
    if service.brand_database_backend() != "supabase":
        return history
    capped = str(max(1, int(limit or 5000)))
    try:
        assets = service._brand_supabase().select_where(
            "video_matrix_generation_assets",
            params={"limit": capped},
            order="created_at.desc",
        )
        segments = service._brand_supabase().select_where(
            "video_matrix_generation_segments",
            params={"limit": capped},
            order="created_at.desc",
        )
        runs = service._brand_supabase().select_where(
            "video_matrix_generation_runs",
            params={"limit": capped},
            order="created_at.desc",
        )
    except service.SupabaseError:
        return history
    history["signatures"] = {str(row.get("signature") or "").strip() for row in assets if str(row.get("signature") or "").strip()}
    history["clip_ids"] = {str(row.get("clip_id") or "").strip() for row in segments if str(row.get("clip_id") or "").strip()}
    history["segment_keys"] = {
        f"{row.get('clip_id')}:{float(row.get('start_time') or 0)}:{float(row.get('duration') or 0)}"
        for row in segments
        if str(row.get("clip_id") or "").strip()
    }
    history["bgm_names"] = {str(row.get("bgm_filename") or "").strip() for row in runs if str(row.get("bgm_filename") or "").strip()}
    return history


def _save_generation_history(
    job_id: str,
    request: dict[str, Any],
    bgm_path: Path,
    assets: list,
    settings: ProjectSettings,
    template_id: str,
    cover_template_id: str,
) -> None:
    if service.brand_database_backend() != "supabase" or not assets:
        return
    ts = service.now_ts()
    try:
        run = service._brand_supabase().insert(
            "video_matrix_generation_runs",
            {
                "job_key": job_id,
                "bgm_filename": bgm_path.name,
                "bgm_path": str(bgm_path),
                "request_json": request,
                "composition_json": _request_composition_sequence(request, settings),
                "created_at": ts,
            },
        )
        run_id = run.get("id")
        for asset in assets:
            asset_row = service._brand_supabase().insert(
                "video_matrix_generation_assets",
                {
                    "run_id": run_id,
                    "job_key": job_id,
                    "sequence_number": asset.variant.sequence_number,
                    "signature": asset.variant.signature,
                    "title": asset.variant.title,
                    "slogan": asset.variant.slogan,
                    "video_path": str(asset.video_path),
                    "cover_path": str(asset.cover_path) if asset.cover_path else "",
                    "copy_path": str(asset.copy_path) if asset.copy_path else "",
                    "manifest_path": str(asset.manifest_path) if asset.manifest_path else "",
                    "template_id": template_id,
                    "cover_template_id": cover_template_id,
                    "copy_language": str(request.get("copy_language") or "zh"),
                    "metadata_json": {"hud_lines": asset.variant.hud_lines},
                    "created_at": ts,
                },
            )
            asset_id = asset_row.get("id")
            for segment in asset.variant.segments:
                service._brand_supabase().insert(
                    "video_matrix_generation_segments",
                    {
                        "run_id": run_id,
                        "asset_id": asset_id,
                        "job_key": job_id,
                        "sequence_number": asset.variant.sequence_number,
                        "segment_index": segment.index,
                        "clip_id": segment.clip.clip_id,
                        "category": segment.category,
                        "source_path": str(segment.clip.source_path),
                        "normalized_path": str(segment.clip.normalized_path),
                        "start_time": segment.start_time,
                        "duration": segment.duration,
                        "created_at": ts,
                    },
                )
    except service.SupabaseError:
        return


def _save_signature_history(settings: ProjectSettings, signatures: set[str]) -> None:
    if not settings.variant_history_enabled:
        return
    limit = settings.variant_history_limit
    history = sorted(signatures)[-limit:] if limit else []
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        current["signature_history"] = history
        _save_video_matrix_app_setting(current)
        return
    SIGNATURE_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SIGNATURE_HISTORY_PATH.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")


def _save_ui_state(state: dict[str, Any]) -> None:
    if service.brand_database_backend() == "supabase":
        current, _ = _complete_video_matrix_state(_video_matrix_app_setting({}) or {})
        current["ui_state"] = state
        if _save_video_matrix_app_setting(current):
            return
    save_ui_state(UI_STATE_PATH, state)


def _count_category_files(root: Path, categories: list[dict[str, str]]) -> dict[str, int]:
    return {
        category["id"]: len([path for path in (root / category["id"]).glob("*") if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS])
        for category in categories
    }


def _list_source_preview_videos(root: Path, categories: list[dict[str, str]], limit: int = 12) -> list[dict[str, str]]:
    videos: list[Path] = []
    for category in categories:
        folder = root / category["id"]
        if not folder.exists():
            continue
        videos.extend(path for path in folder.iterdir() if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS)
    videos = sorted(videos, key=lambda path: path.stat().st_mtime, reverse=True)[:limit]
    return [{"name": path.name, "path": str(path)} for path in videos]


def _list_ending_template_files() -> list[Path]:
    if not ENDING_TEMPLATE_DIR.exists():
        return []
    return [
        path
        for path in sorted(ENDING_TEMPLATE_DIR.iterdir(), key=lambda item: item.stat().st_mtime, reverse=True)
        if path.is_file() and path.suffix.lower() in ENDING_TEMPLATE_EXTENSIONS
    ]


def _list_ending_templates() -> list[dict[str, str]]:
    return [
        {
            "name": path.name,
            "path": str(path),
            "type": "video" if path.suffix.lower() in VIDEO_EXTENSIONS else "image",
            "url": f"/api/video-matrix/ending-templates/{quote(path.name)}",
        }
        for path in _list_ending_template_files()
    ]


def _resolve_ending_template_path(request: dict[str, Any]) -> Path | None:
    mode = str(request.get("ending_template_mode") or "dynamic").strip().lower()
    if mode not in {"random", "specific"}:
        return None
    candidates = _list_ending_template_files()
    if not candidates:
        return None
    if mode == "random":
        requested_names = {
            Path(str(name)).name
            for name in request.get("ending_template_ids") or []
            if str(name).strip()
        }
        selected_candidates = [candidate for candidate in candidates if not requested_names or candidate.name in requested_names]
        return random.choice(selected_candidates or candidates).resolve()
    filename = Path(str(request.get("ending_template_id") or "")).name
    if not filename:
        raise ValueError("Ending template is required when specific mode is selected")
    for candidate in candidates:
        if candidate.name == filename:
            return candidate.resolve()
    raise ValueError(f"Selected ending template was not found: {filename}")


def _list_local_bgm_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return sorted(path for path in folder.iterdir() if path.suffix.lower() in {".mp3", ".wav", ".m4a"})


def _safe_bgm_filename(value: str) -> str:
    name = Path(value).name.strip()
    if not name:
        return ""
    suffix = Path(name).suffix.lower()
    if suffix not in {".mp3", ".wav", ".m4a"}:
        return ""
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(name).stem).strip("._-") or "bgm"
    return f"{stem}{suffix}"


def _audio_media_type(path: Path) -> str:
    return {
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".m4a": "audio/mp4",
    }.get(path.suffix.lower(), "application/octet-stream")


def _image_media_type(path: Path) -> str:
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }.get(path.suffix.lower(), "application/octet-stream")


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _hud_lines(value: str) -> list[str]:
    return [line.strip() for line in value.splitlines() if line.strip()]


def _ui_state_from_request(request: dict[str, Any]) -> dict[str, Any]:
    keys = {
        "output_count",
        "max_workers",
        "output_options",
        "output_root",
        "template_id",
        "cover_template_id",
        "copy_language",
        "source_mode",
        "active_category_ids",
        "recent_limits",
        "video_duration_min",
        "video_duration_max",
        "target_fps",
        "headline",
        "subhead",
        "follow_text",
        "hud_text",
        "ending_template_mode",
        "ending_template_id",
        "ending_template_ids",
        "ending_template_dir",
        "ending_cover_template_id",
        "ending_cover_templates",
        "ending_cover_template",
        "bgm_source",
        "bgm_library_id",
        "composition_sequence",
        "composition_customized",
    }
    return {key: request[key] for key in keys if key in request}


def _request_composition_sequence(request: dict[str, Any], settings: ProjectSettings) -> list[dict[str, Any]]:
    request = _normalize_request_category_ids(request, settings)
    sequence = request.get("composition_sequence")
    if isinstance(sequence, list) and sequence:
        return sequence
    active = request.get("active_category_ids")
    if isinstance(active, list) and active:
        defaults = {
            str(item.get("category_id")): float(item.get("duration", 2.0))
            for item in settings.composition_sequence
            if isinstance(item, dict) and item.get("category_id")
        }
        return [
            {"category_id": str(category_id), "duration": defaults.get(str(category_id), 2.0)}
            for category_id in active
            if str(category_id).strip()
        ]
    return settings.composition_sequence


def _normalize_request_category_ids(request: dict[str, Any], settings: ProjectSettings) -> dict[str, Any]:
    id_map = _legacy_material_category_id_map(settings.material_categories)
    if not id_map or not _has_legacy_category_ids(request, id_map):
        return request
    normalized = dict(request)
    _apply_category_id_map(normalized, id_map)
    if isinstance(normalized.get("active_category_ids"), list):
        normalized["active_category_ids"] = [id_map.get(str(item), str(item)) for item in normalized["active_category_ids"]]
    return normalized

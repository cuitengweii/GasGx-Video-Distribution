from __future__ import annotations

import base64
import json
import os
import random
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

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

router = APIRouter(prefix="/api/video-matrix", tags=["video-matrix"])
_executor = ThreadPoolExecutor(max_workers=2)
_jobs: dict[str, dict[str, Any]] = {}


class OpenFolderPayload(BaseModel):
    path: str = ""


class MaterialCategoryPayload(BaseModel):
    label: str = ""


class BgmDownloadPayload(BaseModel):
    url: str = ""
    filename: str = ""


@router.get("/state")
def get_state() -> dict[str, Any]:
    settings = _settings()
    ensure_category_dirs(settings.source_root, settings.material_categories)
    BGM_DIR.mkdir(parents=True, exist_ok=True)
    return {
        "settings": _settings_payload(settings),
        "ui_state": load_ui_state(UI_STATE_PATH),
        "templates": load_templates(TEMPLATES_PATH),
        "cover_templates": load_cover_templates(COVER_TEMPLATES_PATH),
        "bgm_library": _load_json(BGM_LIBRARY_PATH, {}),
        "local_bgm_dir": str(BGM_DIR),
        "local_bgm": [path.name for path in _list_local_bgm_files(BGM_DIR)],
        "category_counts": _count_category_files(settings.source_root, settings.material_categories),
        "source_dirs": {
            category["id"]: str(settings.source_root / category["id"])
            for category in settings.material_categories
        },
    }


@router.post("/state")
def post_state(payload: dict[str, Any]) -> dict[str, Any]:
    state = load_ui_state(UI_STATE_PATH)
    state.update(payload)
    save_ui_state(UI_STATE_PATH, state)
    return {"ok": True, "ui_state": state}


@router.post("/material-categories")
def add_material_category(payload: MaterialCategoryPayload) -> dict[str, Any]:
    label = payload.label.strip()
    if not label:
        raise HTTPException(status_code=400, detail="label is required")
    config = _load_json(CONFIG_PATH, {})
    categories = list(config.get("material_categories") or [])
    existing_ids = {str(item.get("id") or "") for item in categories if isinstance(item, dict)}
    next_index = 1
    while f"category_custom_{next_index}" in existing_ids:
        next_index += 1
    category_id = f"category_custom_{next_index}"
    categories.append({"id": category_id, "label": label})
    config["material_categories"] = categories
    recent_limits = dict(config.get("recent_limits") or {})
    recent_limits[category_id] = 6
    config["recent_limits"] = recent_limits
    CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8")
    settings = ProjectSettings.from_file(CONFIG_PATH)
    ensure_category_dirs(settings.source_root, settings.material_categories)
    return {"ok": True, "category": {"id": category_id, "label": label}}


@router.post("/templates/{template_id}")
def save_video_template(template_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    templates = load_templates(TEMPLATES_PATH)
    templates[template_id] = payload
    save_templates(TEMPLATES_PATH, templates)
    return {"ok": True, "template_id": template_id, "template": payload}


@router.post("/template-preview")
def template_preview(payload: dict[str, Any]) -> dict[str, str]:
    image = render_video_template_preview_image(
        _settings(),
        payload.get("template") or {},
        hud_text=str(payload.get("hud_text") or ""),
        slogan=str(payload.get("slogan") or ""),
        title=str(payload.get("title") or ""),
    )
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return {"data_url": f"data:image/png;base64,{encoded}"}


@router.post("/cover-templates/{template_id}")
def save_cover_template(template_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    templates = load_cover_templates(COVER_TEMPLATES_PATH)
    templates[template_id] = payload
    COVER_TEMPLATES_PATH.write_text(json.dumps(templates, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"ok": True, "template_id": template_id, "template": payload}


@router.post("/cover-preview")
def cover_preview(payload: dict[str, Any]) -> dict[str, str]:
    settings = _settings()
    template = payload.get("template") or {}
    headline = str(payload.get("headline") or "Gas Engines That Turn Field Gas Into Power")
    subhead = str(payload.get("subhead") or "Generator sets for onsite industrial load")
    image = render_cover_preview_image(
        settings,
        template,
        headline=headline,
        subhead=subhead,
        hud_lines=_hud_lines(str(payload.get("hud_text") or "")),
    )
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return {"data_url": f"data:image/png;base64,{encoded}"}


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


@router.get("/bgm/{filename}")
def local_bgm_file(filename: str) -> FileResponse:
    target = BGM_DIR / Path(filename).name
    if not target.exists() or target.suffix.lower() not in {".mp3", ".wav", ".m4a"}:
        raise HTTPException(status_code=404, detail="BGM file not found")
    return FileResponse(target, media_type=_audio_media_type(target), filename=target.name)


@router.get("/pixabay/industry")
def pixabay_industry_tracks() -> dict[str, Any]:
    return {"tracks": PIXABAY_INDUSTRY_TRACKS[:10], "source_url": "https://pixabay.com/music/search/industry/"}


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
    _executor.submit(_run_generate_job, job_id, request, bgm_path, source_root)
    return {"job_id": job_id}


@router.get("/jobs/{job_id}")
def job_status(job_id: str) -> dict[str, Any]:
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Unknown job")
    return _jobs[job_id]


def _run_generate_job(job_id: str, request: dict[str, Any], bgm_path: Path, source_root: Path | None) -> None:
    try:
        settings = _settings()
        settings.hud_enable_live_data = bool(request.get("use_live_data", True))
        templates = load_templates(TEMPLATES_PATH)
        cover_templates = load_cover_templates(COVER_TEMPLATES_PATH)
        template_id = str(request.get("template_id") or DEFAULT_TEMPLATE_ID)
        cover_template_id = str(request.get("cover_template_id") or DEFAULT_COVER_TEMPLATE_ID)
        template_config = templates.get(template_id) or next(iter(templates.values()))
        cover_template_config = require_cover_template(cover_templates, cover_template_id)
        recent_limits = request.get("recent_limits") if request.get("source_mode") == "Category folders" else None

        def progress(stage: str, value: float, message: str) -> None:
            _jobs[job_id].update({"status": "running", "stage": stage, "progress": value, "message": message})

        assets = run_pipeline(
            settings=settings,
            bgm_path=bgm_path,
            output_count=int(request.get("output_count") or settings.output_count),
            source_root=source_root,
            output_root=Path(request["output_root"]).expanduser().resolve() if request.get("output_root") else None,
            progress_callback=progress,
            transcript_text=str(request.get("transcript_text") or ""),
            output_types=set(request.get("output_options") or ["mp4"]),
            copy_language=str(request.get("copy_language") or "zh"),
            max_workers=int(request.get("max_workers") or 3),
            recent_limits=recent_limits,
            template_config=template_config,
            cover_template_id=cover_template_id,
            cover_template_config=cover_template_config,
            text_overrides={
                "headline": str(request.get("headline") or ""),
                "subhead": str(request.get("subhead") or ""),
                "cta": str(request.get("cta") or ""),
                "hud_text": str(request.get("hud_text") or ""),
                "follow_text": str(request.get("follow_text") or ""),
            },
        )
        _jobs[job_id].update(
            {
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
        )
        save_ui_state(UI_STATE_PATH, _ui_state_from_request(request))
    except Exception as exc:  # pragma: no cover - surfaced through job endpoint
        _jobs[job_id].update({"status": "error", "error": str(exc), "message": str(exc)})


async def _resolve_bgm_path(request: dict[str, Any], temp_root: Path, bgm_file: UploadFile | None) -> Path:
    if request.get("bgm_source") == "Local library":
        filename = Path(str(request.get("bgm_library_id") or "")).name
        local_files = _list_local_bgm_files(BGM_DIR)
        candidate = BGM_DIR / filename if filename else None
        if candidate is not None and candidate.exists():
            return candidate.resolve()
        if local_files:
            return random.choice(local_files).resolve()
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
        "recent_limits": settings.recent_limits,
        "material_categories": settings.material_categories,
        "hud_enable_live_data": settings.hud_enable_live_data,
    }


def _count_category_files(root: Path, categories: list[dict[str, str]]) -> dict[str, int]:
    return {
        category["id"]: len([path for path in (root / category["id"]).glob("*") if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS])
        for category in categories
    }


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


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _hud_lines(value: str) -> list[str]:
    lines = [line.strip() for line in value.splitlines() if line.strip()]
    return lines or ["BTC/USD -> ONSITE VALUE", "GAS INPUT -> HASH OUTPUT"]


def _ui_state_from_request(request: dict[str, Any]) -> dict[str, Any]:
    keys = {
        "output_count",
        "max_workers",
        "output_options",
        "template_id",
        "cover_template_id",
        "copy_language",
        "source_mode",
        "use_live_data",
        "headline",
        "subhead",
        "cta",
        "follow_text",
        "hud_text",
        "bgm_source",
        "bgm_library_id",
    }
    return {key: request[key] for key in keys if key in request}

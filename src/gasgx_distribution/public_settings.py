from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .paths import get_paths
from .platforms import SUPPORTED_PLATFORMS
from .video_matrix.output_root import resolve_video_matrix_output_root

DEFAULT_COMMON_SETTINGS: dict[str, Any] = {
    "material_dir": "runtime/materials/videos",
    "material_dir_follows_video_matrix": True,
    "publish_mode": "publish",
    "topics": "#天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿",
    "upload_timeout": 60,
    "operator_wechats": ["aamecc", "aalbcc"],
    "wechat_content_type": "short_video",
    "wechat_visibility": "public",
    "wechat_comment_permission": "public",
    "wechat_collection_name": "GasGx",
    "wechat_declare_original": False,
    "wechat_short_title": "GasGx燃气发电挖矿",
    "wechat_location": "",
    "wechat_caption": "",
}

DEFAULT_MATRIX_WECHAT_JOB_SETTINGS: dict[str, Any] = {
    "enabled": False,
    "batch_size": 5,
    "batch_interval_min_minutes": 5,
    "batch_interval_max_minutes": 15,
    "run_interval_minutes": 1440,
    "schedule_mode": "interval",
    "daily_time": "09:00",
    "rotate_start_group": True,
    "shuffle_within_batch": True,
    "retry_failed_last": True,
}

DEFAULT_MATRIX_WECHAT_STATS_CAPTURE_SETTINGS: dict[str, Any] = {
    "enabled": False,
    "batch_size": 5,
    "run_interval_minutes": 1440,
    "schedule_mode": "daily",
    "daily_time": "08:30",
    "limit": 0,
}

DEFAULT_PLATFORM_SETTINGS: dict[str, Any] = {
    "enabled": True,
    "content_type": "short_video",
    "caption": "",
    "visibility": "public",
    "comment_permission": "public",
    "publish_mode": "inherit",
    "upload_timeout": 60,
}

DEFAULT_WECHAT_PLATFORM_SETTINGS: dict[str, Any] = {
    **DEFAULT_PLATFORM_SETTINGS,
    "content_type": "inherit",
    "caption": "inherit",
    "visibility": "inherit",
    "comment_permission": "inherit",
    "collection_name": "inherit",
    "declare_original": "inherit",
    "short_title": "inherit",
    "location": "inherit",
}

SUPPORTED_PLATFORM_KEYS = tuple(item.key for item in SUPPORTED_PLATFORMS)


def settings_path() -> Path:
    path = get_paths().runtime_root / "publish_settings.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def legacy_wechat_settings_path() -> Path:
    return get_paths().runtime_root / "wechat_publish_settings.json"


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_timeout(value: Any) -> int:
    try:
        return max(60, int(value or 60))
    except Exception:
        return 60


def _normalize_positive_int(value: Any, default: int, *, minimum: int = 1, maximum: int = 999) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return min(maximum, max(minimum, parsed))


def _normalize_common(payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(DEFAULT_COMMON_SETTINGS)
    merged.update({key: value for key, value in payload.items() if key in merged})
    mode = str(merged.get("publish_mode") or "publish").strip().lower()
    merged["publish_mode"] = "draft" if mode == "draft" else "publish"
    merged["material_dir"] = str(merged.get("material_dir") or DEFAULT_COMMON_SETTINGS["material_dir"]).strip()
    merged["material_dir_follows_video_matrix"] = _normalize_bool(
        merged.get("material_dir_follows_video_matrix", DEFAULT_COMMON_SETTINGS["material_dir_follows_video_matrix"])
    )
    merged["topics"] = str(merged.get("topics") or DEFAULT_COMMON_SETTINGS["topics"]).strip()
    merged["upload_timeout"] = _normalize_timeout(merged.get("upload_timeout"))
    raw_operators = merged.get("operator_wechats")
    merged["wechat_content_type"] = str(merged.get("wechat_content_type") or DEFAULT_COMMON_SETTINGS["wechat_content_type"]).strip()
    merged["wechat_visibility"] = str(merged.get("wechat_visibility") or DEFAULT_COMMON_SETTINGS["wechat_visibility"]).strip()
    merged["wechat_comment_permission"] = str(merged.get("wechat_comment_permission") or DEFAULT_COMMON_SETTINGS["wechat_comment_permission"]).strip()
    merged["wechat_collection_name"] = str(merged.get("wechat_collection_name") or DEFAULT_COMMON_SETTINGS["wechat_collection_name"]).strip()
    merged["wechat_declare_original"] = _normalize_bool(merged.get("wechat_declare_original", DEFAULT_COMMON_SETTINGS["wechat_declare_original"]))
    merged["wechat_short_title"] = str(merged.get("wechat_short_title") or DEFAULT_COMMON_SETTINGS["wechat_short_title"]).strip() or DEFAULT_COMMON_SETTINGS["wechat_short_title"]
    merged["wechat_location"] = str(merged.get("wechat_location") or DEFAULT_COMMON_SETTINGS["wechat_location"]).strip()
    merged["wechat_caption"] = str(merged.get("wechat_caption") or DEFAULT_COMMON_SETTINGS["wechat_caption"]).strip()
    if not isinstance(raw_operators, list):
        raw_operators = DEFAULT_COMMON_SETTINGS["operator_wechats"]
    operators = []
    for item in raw_operators:
        value = str(item or "").strip()
        if value and value not in operators:
            operators.append(value)
    merged["operator_wechats"] = operators or list(DEFAULT_COMMON_SETTINGS["operator_wechats"])
    return merged


def _normalize_matrix_wechat_job(payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(DEFAULT_MATRIX_WECHAT_JOB_SETTINGS)
    merged.update({key: value for key, value in payload.items() if key in merged})
    merged["enabled"] = _normalize_bool(merged.get("enabled"))
    merged["batch_size"] = _normalize_positive_int(merged.get("batch_size"), 5, minimum=1, maximum=30)
    merged["batch_interval_min_minutes"] = _normalize_positive_int(
        merged.get("batch_interval_min_minutes"),
        5,
        minimum=0,
        maximum=240,
    )
    merged["batch_interval_max_minutes"] = _normalize_positive_int(
        merged.get("batch_interval_max_minutes"),
        15,
        minimum=0,
        maximum=240,
    )
    if merged["batch_interval_max_minutes"] < merged["batch_interval_min_minutes"]:
        merged["batch_interval_max_minutes"] = merged["batch_interval_min_minutes"]
    merged["run_interval_minutes"] = _normalize_positive_int(
        merged.get("run_interval_minutes"),
        1440,
        minimum=5,
        maximum=10080,
    )
    schedule_mode = str(merged.get("schedule_mode") or "interval").strip().lower()
    merged["schedule_mode"] = "daily" if schedule_mode == "daily" else "interval"
    raw_daily_time = str(merged.get("daily_time") or "09:00").strip()
    try:
        hour_text, minute_text = raw_daily_time.split(":", 1)
        hour = min(23, max(0, int(hour_text)))
        minute = min(59, max(0, int(minute_text)))
    except Exception:
        hour, minute = 9, 0
    merged["daily_time"] = f"{hour:02d}:{minute:02d}"
    merged["rotate_start_group"] = _normalize_bool(merged.get("rotate_start_group"))
    merged["shuffle_within_batch"] = _normalize_bool(merged.get("shuffle_within_batch"))
    merged["retry_failed_last"] = _normalize_bool(merged.get("retry_failed_last"))
    return merged


def _normalize_matrix_wechat_stats_capture_job(payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(DEFAULT_MATRIX_WECHAT_STATS_CAPTURE_SETTINGS)
    merged.update({key: value for key, value in payload.items() if key in merged})
    merged["enabled"] = _normalize_bool(merged.get("enabled"))
    merged["batch_size"] = _normalize_positive_int(merged.get("batch_size"), 5, minimum=1, maximum=30)
    merged["run_interval_minutes"] = _normalize_positive_int(
        merged.get("run_interval_minutes"),
        1440,
        minimum=5,
        maximum=10080,
    )
    schedule_mode = str(merged.get("schedule_mode") or "daily").strip().lower()
    merged["schedule_mode"] = "interval" if schedule_mode == "interval" else "daily"
    raw_daily_time = str(merged.get("daily_time") or "08:30").strip()
    try:
        hour_text, minute_text = raw_daily_time.split(":", 1)
        hour = min(23, max(0, int(hour_text)))
        minute = min(59, max(0, int(minute_text)))
    except Exception:
        hour, minute = 8, 30
    merged["daily_time"] = f"{hour:02d}:{minute:02d}"
    merged["limit"] = _normalize_positive_int(merged.get("limit"), 1, minimum=0, maximum=200)
    return merged


def _normalize_jobs(payload: dict[str, Any]) -> dict[str, Any]:
    raw_matrix = payload.get("matrix_wechat_publish")
    raw_stats = payload.get("matrix_wechat_stats_capture")
    return {
        "matrix_wechat_publish": _normalize_matrix_wechat_job(raw_matrix if isinstance(raw_matrix, dict) else {}),
        "matrix_wechat_stats_capture": _normalize_matrix_wechat_stats_capture_job(raw_stats if isinstance(raw_stats, dict) else {}),
    }


def _platform_defaults(platform: str) -> dict[str, Any]:
    if platform == "wechat":
        return dict(DEFAULT_WECHAT_PLATFORM_SETTINGS)
    return dict(DEFAULT_PLATFORM_SETTINGS)


def _normalize_platform(platform: str, payload: dict[str, Any]) -> dict[str, Any]:
    merged = _platform_defaults(platform)
    merged.update({key: value for key, value in payload.items() if key in merged})
    merged["enabled"] = _normalize_bool(merged.get("enabled"))
    merged["content_type"] = str(merged.get("content_type") or "short_video").strip()
    merged["caption"] = str(merged.get("caption") or "").strip()
    merged["visibility"] = str(merged.get("visibility") or "public").strip()
    merged["comment_permission"] = str(merged.get("comment_permission") or "public").strip()
    mode = str(merged.get("publish_mode") or "inherit").strip().lower()
    merged["publish_mode"] = mode if mode in {"inherit", "publish", "draft"} else "inherit"
    merged["upload_timeout"] = _normalize_timeout(merged.get("upload_timeout"))
    if platform == "wechat":
        merged["collection_name"] = str(merged.get("collection_name") or "").strip()
        declare_original = merged.get("declare_original")
        if str(declare_original or "").strip().lower() == "inherit":
            merged["declare_original"] = "inherit"
        else:
            merged["declare_original"] = _normalize_bool(declare_original)
        short_title = merged.get("short_title")
        if str(short_title or "").strip().lower() == "inherit":
            merged["short_title"] = "inherit"
        else:
            merged["short_title"] = str(short_title or "GasGx燃气发电挖矿").strip() or "GasGx燃气发电挖矿"
        location = merged.get("location")
        if str(location or "").strip().lower() == "inherit":
            merged["location"] = "inherit"
        else:
            merged["location"] = str(location or "").strip()
    return merged


def _resolve_wechat_common_value(value: Any, fallback: Any, *, blank_is_fallback: bool = True) -> Any:
    if value is None:
        return fallback
    text = str(value).strip().lower()
    if text == "inherit" or (blank_is_fallback and text == ""):
        return fallback
    return value


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _legacy_wechat_payload() -> dict[str, Any]:
    payload = _read_json(legacy_wechat_settings_path())
    if not payload:
        return {}
    common = {
        "material_dir": payload.get("material_dir"),
        "publish_mode": payload.get("publish_mode"),
        "topics": payload.get("topics"),
        "upload_timeout": payload.get("upload_timeout"),
    }
    platform = {
        "caption": payload.get("caption"),
        "collection_name": payload.get("collection_name"),
        "declare_original": payload.get("declare_original"),
        "short_title": payload.get("short_title"),
        "location": payload.get("location"),
        "publish_mode": "inherit",
        "upload_timeout": payload.get("upload_timeout"),
    }
    return {"common": common, "platforms": {"wechat": platform}}


def _normalize_distribution_settings(payload: dict[str, Any]) -> dict[str, Any]:
    if "common" not in payload and "platforms" not in payload and "jobs" not in payload:
        payload = _legacy_wechat_payload()
    common = _normalize_common(payload.get("common") if isinstance(payload.get("common"), dict) else {})
    jobs = _normalize_jobs(payload.get("jobs") if isinstance(payload.get("jobs"), dict) else {})
    raw_platforms = payload.get("platforms") if isinstance(payload.get("platforms"), dict) else {}
    platforms = {
        platform: _normalize_platform(
            platform,
            raw_platforms.get(platform) if isinstance(raw_platforms.get(platform), dict) else {},
        )
        for platform in SUPPORTED_PLATFORM_KEYS
    }
    return {"common": common, "jobs": jobs, "platforms": platforms}


def load_distribution_settings() -> dict[str, Any]:
    return _normalize_distribution_settings(_read_json(settings_path()))


def save_distribution_settings(payload: dict[str, Any]) -> dict[str, Any]:
    settings = _normalize_distribution_settings(payload)
    settings_path().write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
    return settings


def load_platform_publish_settings(platform: str) -> dict[str, Any]:
    settings = load_distribution_settings()
    common = settings["common"]
    platform_settings = settings["platforms"].get(platform, _platform_defaults(platform))
    publish_mode = platform_settings.get("publish_mode")
    upload_timeout = platform_settings.get("upload_timeout")
    if upload_timeout == DEFAULT_PLATFORM_SETTINGS["upload_timeout"]:
        upload_timeout = common["upload_timeout"]
    if platform == "wechat":
        platform_settings = {
            **platform_settings,
            "content_type": _resolve_wechat_common_value(platform_settings.get("content_type"), common.get("wechat_content_type", "short_video")),
            "visibility": _resolve_wechat_common_value(platform_settings.get("visibility"), common.get("wechat_visibility", "public")),
            "comment_permission": _resolve_wechat_common_value(platform_settings.get("comment_permission"), common.get("wechat_comment_permission", "public")),
            "collection_name": _resolve_wechat_common_value(platform_settings.get("collection_name"), common.get("wechat_collection_name", "GasGx"), blank_is_fallback=False),
            "declare_original": _resolve_wechat_common_value(platform_settings.get("declare_original"), common.get("wechat_declare_original", False)),
            "short_title": _resolve_wechat_common_value(platform_settings.get("short_title"), common.get("wechat_short_title", DEFAULT_COMMON_SETTINGS["wechat_short_title"])),
            "location": _resolve_wechat_common_value(platform_settings.get("location"), common.get("wechat_location", "")),
            "caption": _resolve_wechat_common_value(platform_settings.get("caption"), common.get("wechat_caption", "")),
        }
    return {
        **platform_settings,
        "material_dir": common["material_dir"],
        "material_dir_follows_video_matrix": common["material_dir_follows_video_matrix"],
        "publish_mode": common["publish_mode"] if publish_mode == "inherit" else publish_mode,
        "topics": common["topics"],
        "upload_timeout": upload_timeout,
    }


def load_wechat_publish_settings() -> dict[str, Any]:
    return load_platform_publish_settings("wechat")


def save_wechat_publish_settings(payload: dict[str, Any]) -> dict[str, Any]:
    settings = load_distribution_settings()
    settings["common"].update(
        {
            "material_dir": payload.get("material_dir", settings["common"]["material_dir"]),
            "publish_mode": payload.get("publish_mode", settings["common"]["publish_mode"]),
            "topics": payload.get("topics", settings["common"]["topics"]),
            "upload_timeout": payload.get("upload_timeout", settings["common"]["upload_timeout"]),
        }
    )
    settings["platforms"]["wechat"].update(
        {
            "caption": payload.get("caption", settings["platforms"]["wechat"]["caption"]),
            "collection_name": payload.get("collection_name", settings["platforms"]["wechat"]["collection_name"]),
            "declare_original": payload.get("declare_original", settings["platforms"]["wechat"]["declare_original"]),
            "short_title": payload.get("short_title", settings["platforms"]["wechat"]["short_title"]),
            "location": payload.get("location", settings["platforms"]["wechat"]["location"]),
        }
    )
    save_distribution_settings(settings)
    return load_wechat_publish_settings()


def resolve_material_dir(settings: dict[str, Any] | None = None, *, material_dir_override: str | None = None) -> Path:
    if material_dir_override is not None and str(material_dir_override).strip():
        raw = str(material_dir_override).strip()
        path = Path(raw)
        if not path.is_absolute():
            path = get_paths().repo_root / path
        path.mkdir(parents=True, exist_ok=True)
        return path.resolve()
    active = settings or load_distribution_settings()["common"]
    if _normalize_bool(active.get("material_dir_follows_video_matrix", True)):
        path = resolve_video_matrix_output_root()
    else:
        raw = str(active.get("material_dir") or DEFAULT_COMMON_SETTINGS["material_dir"]).strip()
        path = Path(raw)
        if not path.is_absolute():
            path = get_paths().repo_root / path
        path = path.resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path

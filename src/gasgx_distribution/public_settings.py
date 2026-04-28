from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .paths import get_paths
from .platforms import SUPPORTED_PLATFORMS

DEFAULT_COMMON_SETTINGS: dict[str, Any] = {
    "material_dir": "runtime/materials/videos",
    "publish_mode": "publish",
    "topics": "#天然气 #天然气发电机组 #燃气发电机组 #海外发电 #海外挖矿",
    "upload_timeout": 60,
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
    "collection_name": "赛博皮卡天津港现车",
    "declare_original": False,
    "short_title": "GasGx",
    "location": "",
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
    merged["topics"] = str(merged.get("topics") or DEFAULT_COMMON_SETTINGS["topics"]).strip()
    merged["upload_timeout"] = _normalize_timeout(merged.get("upload_timeout"))
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


def _normalize_jobs(payload: dict[str, Any]) -> dict[str, Any]:
    raw_matrix = payload.get("matrix_wechat_publish")
    return {
        "matrix_wechat_publish": _normalize_matrix_wechat_job(raw_matrix if isinstance(raw_matrix, dict) else {})
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
        merged["declare_original"] = _normalize_bool(merged.get("declare_original"))
        merged["short_title"] = str(merged.get("short_title") or "GasGx").strip() or "GasGx"
        merged["location"] = str(merged.get("location") or "").strip()
    return merged


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
    return {
        **platform_settings,
        "material_dir": common["material_dir"],
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


def resolve_material_dir(settings: dict[str, Any] | None = None) -> Path:
    active = settings or load_distribution_settings()["common"]
    raw = str(active.get("material_dir") or DEFAULT_COMMON_SETTINGS["material_dir"]).strip()
    path = Path(raw)
    if not path.is_absolute():
        path = get_paths().repo_root / path
    path.mkdir(parents=True, exist_ok=True)
    return path

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import Request

from . import control_plane, service


def resolve_brand_instance(request: Request) -> dict[str, Any]:
    brand_id = request.headers.get("x-brand-instance", "")
    host = request.headers.get("host", "")
    host_name = str(host or "").split(":", 1)[0].lower()
    if not brand_id and host_name in {"127.0.0.1", "localhost"}:
        brand_id = os.getenv("LOCAL_BRAND_INSTANCE", "gasgx")
    if brand_id == "gasgx" and os.getenv("SUPABASE_URL") and os.getenv("BRAND_SUPABASE_SERVICE_KEY"):
        return {
            "id": "gasgx",
            "name": "GasGx",
            "domain": host_name or "127.0.0.1",
            "supabase_url": os.getenv("SUPABASE_URL", ""),
            "service_key_ref": "env:BRAND_SUPABASE_SERVICE_KEY",
            "anon_key": "env:SUPABASE_KEY",
            "database_path": str(control_plane.brand_database_path("gasgx")),
            "schema_version": control_plane.SCHEMA_VERSION,
            "app_version": control_plane.APP_VERSION,
            "status": "active",
            "has_service_key_ref": True,
        }
    return control_plane.find_brand_runtime_instance(host=host, brand_id=brand_id)


def brand_database_path(instance: dict[str, Any]) -> Path:
    raw_path = str(instance.get("database_path") or "")
    if raw_path:
        return Path(raw_path)
    return control_plane.brand_database_path(str(instance.get("id") or "default"))


async def bind_tenant_database(request: Request, call_next):
    if request.url.path == "/" or request.url.path.startswith("/static/"):
        return await call_next(request)
    instance = resolve_brand_instance(request)
    request.state.brand_instance = instance
    with service.use_brand_runtime(instance):
        with service.use_brand_database(brand_database_path(instance)):
            response = await call_next(request)
    response.headers["x-brand-instance"] = str(instance.get("id") or "default")
    return response

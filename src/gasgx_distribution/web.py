from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import control_plane, service
from .platforms import SUPPORTED_PLATFORMS
from .scheduler import scheduler_status, start_scheduler, trigger_matrix_wechat_job
from .tenant import bind_tenant_database
from .video_matrix_api import router as video_matrix_router


def _model_payload(model: BaseModel, *, exclude_unset: bool = False) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_unset=exclude_unset)  # type: ignore[attr-defined]
    return model.dict(exclude_unset=exclude_unset)


class AccountPayload(BaseModel):
    account_key: str = Field(default="")
    display_name: str = Field(default="")
    niche: str = Field(default="")
    status: str = Field(default="active")
    notes: str = Field(default="")
    platforms: list[str] = Field(default_factory=list)


class TaskPayload(BaseModel):
    account_id: int | None = None
    platform: str = ""
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)


class WechatPublishSettingsPayload(BaseModel):
    material_dir: str = ""
    publish_mode: str = "publish"
    topics: str = ""
    collection_name: str = ""
    caption: str = ""
    declare_original: bool = False
    short_title: str = "GasGx"
    location: str = ""
    upload_timeout: int = 60


class DistributionSettingsPayload(BaseModel):
    common: dict[str, Any] = Field(default_factory=dict)
    jobs: dict[str, dict[str, Any]] = Field(default_factory=dict)
    platforms: dict[str, dict[str, Any]] = Field(default_factory=dict)


class OpenMaterialDirPayload(BaseModel):
    material_dir: str = ""


class AiRobotConfigPayload(BaseModel):
    enabled: bool = False
    bot_name: str = ""
    webhook_url: str = ""
    webhook_secret: str = ""
    signing_secret: str = ""
    target_id: str = ""


class AiRobotMessagePayload(BaseModel):
    message_type: str = "text"
    text: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class TelegramResolvePayload(BaseModel):
    token: str = ""


class BrandInstancePayload(BaseModel):
    id: str = ""
    name: str
    domain: str = ""
    supabase_url: str = ""
    service_key_ref: str = ""
    anon_key: str = ""
    status: str = "active"


class BrandSettingsPayload(BaseModel):
    name: str = ""
    slogan: str = ""
    logo_asset_path: str = ""
    primary_color: str = ""
    theme_id: str = ""
    default_account_prefix: str = ""


def create_app() -> FastAPI:
    control_plane.ensure_control_database()
    service.ensure_database()
    app = FastAPI(title="GasGx Video Distribution", version="0.1.0")
    app.include_router(video_matrix_router)
    app.middleware("http")(bind_tenant_database)

    @app.middleware("http")
    async def disable_console_cache(request: Request, call_next):
        response = await call_next(request)
        if request.url.path == "/" or request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-store, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    start_scheduler()
    static_dir = Path(__file__).resolve().parent / "web" / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/api/summary")
    def summary() -> dict[str, Any]:
        return service.dashboard_summary()

    @app.get("/api/platforms")
    def platforms() -> list[dict[str, Any]]:
        return [item.__dict__ for item in SUPPORTED_PLATFORMS]

    @app.get("/api/brand")
    def get_brand(request: Request) -> dict[str, Any]:
        return {
            "instance": request.state.brand_instance,
            "settings": service.load_brand_settings(),
        }

    @app.get("/api/system/supabase-health")
    def supabase_health(request: Request) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []

        def record(name: str, fn) -> None:
            try:
                details = fn()
            except Exception as exc:  # pragma: no cover - exact backend errors vary.
                checks.append({"name": name, "ok": False, "error": str(exc)})
                return
            checks.append({"name": name, "ok": True, "details": details})

        record(
            "control_plane",
            lambda: {
                "backend": control_plane.control_backend(),
                "brand_count": len(control_plane.list_brand_instances()),
            },
        )
        record(
            "tenant",
            lambda: {
                "brand_id": request.state.brand_instance.get("id"),
                "domain": request.state.brand_instance.get("domain"),
                "status": request.state.brand_instance.get("status"),
            },
        )
        record(
            "brand_database",
            lambda: {
                "backend": service.brand_database_backend(),
                "brand_name": service.load_brand_settings().get("name"),
            },
        )
        record(
            "ai_robot_queue",
            lambda: {
                "config_count": len(service.list_ai_robot_configs()),
                "queued_message_count": len(service.list_ai_robot_messages()),
            },
        )

        ok = all(item["ok"] for item in checks)
        return {
            "ok": ok,
            "app_version": control_plane.APP_VERSION,
            "schema_version": control_plane.SCHEMA_VERSION,
            "brand_id": request.state.brand_instance.get("id"),
            "checks": checks,
        }

    @app.post("/api/system/initialize")
    def system_initialize() -> dict[str, Any]:
        return service.initialize_system()

    @app.patch("/api/brand")
    def update_brand(payload: BrandSettingsPayload) -> dict[str, Any]:
        return service.save_brand_settings(_model_payload(payload, exclude_unset=True))

    @app.get("/control/brands")
    def control_brands() -> list[dict[str, Any]]:
        return control_plane.list_brand_instances()

    @app.post("/control/brands")
    def create_control_brand(payload: BrandInstancePayload) -> dict[str, Any]:
        try:
            return control_plane.create_brand_instance(_model_payload(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/control/brands/{brand_id}/provision")
    def provision_control_brand(brand_id: str) -> dict[str, Any]:
        try:
            return control_plane.provision_brand_instance(brand_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/control/upgrades")
    def create_control_upgrade() -> dict[str, Any]:
        return control_plane.run_full_upgrade()

    @app.get("/control/upgrades/{run_id}")
    def get_control_upgrade(run_id: int) -> dict[str, Any]:
        item = control_plane.get_upgrade_run(run_id)
        if item is None:
            raise HTTPException(status_code=404, detail="upgrade run not found")
        return item

    @app.get("/api/settings/wechat-publish")
    def get_wechat_publish_settings() -> dict[str, Any]:
        return service.load_wechat_publish_settings_db()

    @app.patch("/api/settings/wechat-publish")
    def update_wechat_publish_settings(payload: WechatPublishSettingsPayload) -> dict[str, Any]:
        return service.save_wechat_publish_settings_db(_model_payload(payload))

    @app.get("/api/settings/distribution")
    def get_distribution_settings() -> dict[str, Any]:
        return service.load_distribution_settings_db()

    @app.patch("/api/settings/distribution")
    def update_distribution_settings(payload: DistributionSettingsPayload) -> dict[str, Any]:
        return service.save_distribution_settings_db(_model_payload(payload))

    @app.post("/api/settings/material-dir/open")
    def open_material_dir(payload: OpenMaterialDirPayload) -> dict[str, Any]:
        return service.open_material_directory(payload.material_dir)

    @app.get("/api/ai-robots/configs")
    def ai_robot_configs() -> list[dict[str, Any]]:
        return service.list_ai_robot_configs()

    @app.put("/api/ai-robots/{platform}/config")
    def save_ai_robot_config(platform: str, payload: AiRobotConfigPayload) -> dict[str, Any]:
        try:
            return service.save_ai_robot_config(platform, _model_payload(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.delete("/api/ai-robots/{platform}/config")
    def delete_ai_robot_config(platform: str) -> dict[str, Any]:
        try:
            deleted = service.delete_ai_robot_config(platform)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "deleted": deleted, "platform": platform}

    @app.get("/api/ai-robots/messages")
    def ai_robot_messages() -> list[dict[str, Any]]:
        return service.list_ai_robot_messages()

    @app.post("/api/ai-robots/telegram/resolve")
    def resolve_telegram(payload: TelegramResolvePayload) -> dict[str, Any]:
        try:
            return service.resolve_telegram_bot_setup(payload.token)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/ai-robots/messages/send-worker")
    def run_ai_robot_sender(limit: int = Query(default=10, ge=1, le=100)) -> dict[str, Any]:
        return service.run_ai_robot_sender_worker(limit=limit)

    @app.post("/api/ai-robots/{platform}/messages")
    def create_ai_robot_message(platform: str, payload: AiRobotMessagePayload) -> dict[str, Any]:
        data = _model_payload(payload)
        message = dict(data.get("payload") or {})
        if data.get("text"):
            message["text"] = data["text"]
        message["message_type"] = data.get("message_type") or "text"
        try:
            return service.enqueue_ai_robot_message(platform, message)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/ai-robots/{platform}/test-message")
    def create_ai_robot_test_message(platform: str, payload: AiRobotMessagePayload) -> dict[str, Any]:
        data = _model_payload(payload)
        message = dict(data.get("payload") or {})
        if data.get("text"):
            message["text"] = data["text"]
        message["message_type"] = data.get("message_type") or "text"
        try:
            queued = service.enqueue_ai_robot_message(platform, message, test=True)
            return service.send_ai_robot_message_now(queued)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/ai-robots/{platform}/webhook")
    async def ai_robot_webhook(platform: str, request: Request, x_gasgx_signature: str = Header(default="")) -> dict[str, Any]:
        body = await request.body()
        if platform.lower() in {"lark", "feishu"}:
            try:
                payload = json.loads(body.decode("utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict) and payload.get("challenge"):
                return {"challenge": payload["challenge"]}
        try:
            verification = service.verify_ai_robot_webhook(platform, body, x_gasgx_signature)
            message = service.enqueue_ai_robot_message(platform, {"message_type": "webhook", "body": body.decode("utf-8", errors="replace")})
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        return {**verification, "message_id": message.get("id")}

    @app.get("/api/jobs/matrix-wechat/status")
    def matrix_wechat_job_status() -> dict[str, Any]:
        return scheduler_status()

    @app.post("/api/jobs/matrix-wechat/run-now")
    def matrix_wechat_job_run_now() -> dict[str, Any]:
        return trigger_matrix_wechat_job()

    @app.get("/api/accounts")
    def accounts() -> list[dict[str, Any]]:
        return service.list_accounts()

    @app.post("/api/accounts")
    def create_account(payload: AccountPayload) -> dict[str, Any]:
        try:
            return service.create_account(_model_payload(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.patch("/api/accounts/{account_id}")
    def update_account(account_id: int, payload: AccountPayload) -> dict[str, Any]:
        account = service.update_account(account_id, _model_payload(payload, exclude_unset=True))
        if account is None:
            raise HTTPException(status_code=404, detail="account not found")
        return account

    @app.post("/api/accounts/{account_id}/platforms/{platform}/open-browser")
    def open_browser(account_id: int, platform: str) -> dict[str, Any]:
        try:
            return service.open_account_browser(account_id, platform)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/accounts/{account_id}/platforms/{platform}/login-status")
    def login_status(account_id: int, platform: str) -> dict[str, Any]:
        try:
            return service.check_login_status(account_id, platform)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/tasks")
    def create_task(payload: TaskPayload) -> dict[str, Any]:
        try:
            return service.create_task(_model_payload(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/tasks")
    def tasks() -> list[dict[str, Any]]:
        return service.list_tasks()

    @app.get("/api/tasks/{task_id}")
    def task(task_id: int) -> dict[str, Any]:
        item = service.get_task(task_id)
        if item is None:
            raise HTTPException(status_code=404, detail="task not found")
        return item

    @app.delete("/api/tasks/{task_id}")
    def delete_task(task_id: int) -> dict[str, Any]:
        if not service.delete_task(task_id):
            raise HTTPException(status_code=404, detail="task not found")
        return {"ok": True, "deleted": task_id}

    @app.get("/api/stats")
    def stats(account_id: int | None = Query(default=None), platform: str = Query(default="")) -> list[dict[str, Any]]:
        return service.list_stats(account_id=account_id, platform=platform)

    @app.get("/api/stats/analytics")
    def stats_analytics() -> dict[str, list[dict[str, Any]]]:
        return service.list_analytics_items()

    @app.post("/api/stats/import")
    def import_stats(payload: dict[str, Any]) -> dict[str, Any]:
        return service.import_stats(payload)

    return app


app = create_app()

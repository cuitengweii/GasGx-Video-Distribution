from __future__ import annotations

import html
import json
import logging
import threading
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import control_plane, service
from .platforms import SUPPORTED_PLATFORMS
from .public_settings import resolve_material_dir
from .scheduler import (
    scheduler_status,
    start_scheduler,
    trigger_matrix_wechat_job,
    trigger_matrix_wechat_login_check,
    trigger_matrix_wechat_stats_capture,
)
from .tenant import bind_tenant_database
from .video_matrix.ffmpeg_tools import ffmpeg_runtime_health
from .video_matrix_api import router as video_matrix_router
from .wechat_stats_capture import capture_status

_TERMINAL_EXECUTION_API_LOCK = threading.RLock()


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


class TaskBulkPayload(BaseModel):
    ids: list[int] = Field(default_factory=list)


class TaskStatusPayload(BaseModel):
    ids: list[int] = Field(default_factory=list)
    status: str


class TerminalWindowPayload(BaseModel):
    id: int
    enabled: bool = True
    operator_wechat: str = ""
    color: str = ""


class TerminalStartPayload(BaseModel):
    windows: list[TerminalWindowPayload] = Field(default_factory=list)


class TerminalPollPayload(BaseModel):
    allow_browser_open: bool = False
    allow_login_probe: bool = False


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
    password: str = ""


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


class NotificationActionPayload(BaseModel):
    actor: str = "Allen"
    note: str = ""
    assigned_to: str = ""


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


class OperatorLoginPayload(BaseModel):
    user_id: str
    password: str = ""


class OperatorUserPayload(BaseModel):
    name: str
    role_id: str
    password: str = ""


class OperatorUserRolePayload(BaseModel):
    role_id: str


class OperatorUserPasswordPayload(BaseModel):
    password: str


class OperatorWechatPayload(BaseModel):
    operator_wechat: str


class SystemInitializePayload(BaseModel):
    password: str = ""


class WechatStatsCapturePayload(BaseModel):
    target_date: str = ""
    limit: int = 0
    dry_run: bool = False


class OperatorRolePayload(BaseModel):
    name: str


class OperatorPermissionsPayload(BaseModel):
    permissions: list[str] = Field(default_factory=list)


def create_app() -> FastAPI:
    control_plane.ensure_control_database()
    service.ensure_database()
    app = FastAPI(title="GasGx Video Distribution", version="0.1.0")
    app.include_router(video_matrix_router)
    app.middleware("http")(bind_tenant_database)

    @app.middleware("http")
    async def disable_console_cache(request: Request, call_next):
        response = await call_next(request)
        if (
            request.url.path == "/"
            or request.url.path.startswith("/static/")
            or request.url.path.startswith("/help-center/")
        ):
            response.headers["Cache-Control"] = "no-store, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        content_type = response.headers.get("content-type", "")
        if content_type.startswith("application/json") and "charset=" not in content_type.lower():
            response.headers["content-type"] = "application/json; charset=utf-8"
        return response

    start_scheduler()
    health = ffmpeg_runtime_health()
    logging.getLogger(__name__).info(
        "video matrix ffmpeg health: ffmpeg_ok=%s ffprobe_ok=%s ffmpeg_path=%s ffprobe_path=%s",
        health.get("ffmpeg_ok"),
        health.get("ffprobe_ok"),
        health.get("ffmpeg_path", health.get("ffmpeg_error", "")),
        health.get("ffprobe_path", health.get("ffprobe_error", "")),
    )
    static_dir = Path(__file__).resolve().parent / "web" / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/help-center/{doc_name}")
    def help_center_index(doc_name: str) -> FileResponse:
        safe_name = Path(doc_name).name
        if safe_name != doc_name or not safe_name.endswith(".md"):
            raise HTTPException(status_code=404, detail="help doc not found")
        path = Path(__file__).resolve().parents[2] / "docs" / "help" / safe_name
        if not path.exists():
            raise HTTPException(status_code=404, detail="help doc not found")
        return FileResponse(static_dir / "index.html")

    @app.get("/api/summary")
    def summary() -> dict[str, Any]:
        return service.dashboard_summary()

    @app.get("/api/platforms")
    def platforms() -> list[dict[str, Any]]:
        return [item.__dict__ for item in SUPPORTED_PLATFORMS]

    @app.get("/api/help-docs/{doc_name}")
    def help_doc(doc_name: str) -> dict[str, Any]:
        safe_name = Path(doc_name).name
        if safe_name != doc_name or not safe_name.endswith(".md"):
            raise HTTPException(status_code=404, detail="help doc not found")
        path = Path(__file__).resolve().parents[2] / "docs" / "help" / safe_name
        if not path.exists():
            raise HTTPException(status_code=404, detail="help doc not found")
        return {
            "name": safe_name,
            "path": f"docs/help/{safe_name}",
            "content": path.read_text(encoding="utf-8"),
        }

    @app.get("/help-docs/{doc_name}")
    def help_doc_page(doc_name: str) -> HTMLResponse:
        safe_name = Path(doc_name).name
        if safe_name != doc_name or not safe_name.endswith(".md"):
            raise HTTPException(status_code=404, detail="help doc not found")
        path = Path(__file__).resolve().parents[2] / "docs" / "help" / safe_name
        if not path.exists():
            raise HTTPException(status_code=404, detail="help doc not found")

        markdown = path.read_text(encoding="utf-8")
        title = safe_name
        for line in markdown.splitlines():
            if line.startswith("# "):
                title = line[2:].strip()
                break

        lines = markdown.splitlines()
        body_parts: list[str] = []
        list_open = False
        code_open = False

        def close_list() -> None:
            nonlocal list_open
            if list_open:
                body_parts.append("</ul>")
                list_open = False

        for raw in lines:
            line = raw.rstrip("\n")
            trimmed = line.strip()
            if trimmed.startswith("```"):
                close_list()
                body_parts.append("</code></pre>" if code_open else "<pre><code>")
                code_open = not code_open
                continue
            if code_open:
                body_parts.append(html.escape(line))
                continue
            if not trimmed:
                close_list()
                continue
            if trimmed.startswith("# "):
                close_list()
                body_parts.append(f"<h1>{html.escape(trimmed[2:])}</h1>")
                continue
            if trimmed.startswith("## "):
                close_list()
                body_parts.append(f"<h2>{html.escape(trimmed[3:])}</h2>")
                continue
            if trimmed.startswith("### "):
                close_list()
                body_parts.append(f"<h3>{html.escape(trimmed[4:])}</h3>")
                continue
            if trimmed.startswith("- "):
                if not list_open:
                    body_parts.append("<ul>")
                    list_open = True
                body_parts.append(f"<li>{html.escape(trimmed[2:])}</li>")
                continue
            close_list()
            body_parts.append(f"<p>{html.escape(trimmed)}</p>")
        close_list()
        if code_open:
            body_parts.append("</code></pre>")

        page_html = f"""<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)} - GasGx 帮助文档</title>
    <style>
      :root {{ color-scheme: dark; }}
      body {{
        margin: 0;
        font-family: "Microsoft YaHei", "PingFang SC", sans-serif;
        background: #06090f;
        color: #e6ecf8;
      }}
      .wrap {{ max-width: 960px; margin: 0 auto; padding: 28px 20px 48px; }}
      .bar {{ margin-bottom: 18px; display: flex; gap: 12px; align-items: center; }}
      .back {{
        display: inline-block;
        text-decoration: none;
        border: 1px solid #2a3244;
        color: #b9c3d7;
        padding: 8px 12px;
        border-radius: 10px;
      }}
      .doc {{
        border: 1px solid #243040;
        border-radius: 14px;
        padding: 22px;
        background: #0b111b;
      }}
      h1, h2, h3 {{ color: #f4f7ff; margin: 14px 0 10px; }}
      p, li {{ line-height: 1.75; color: #c8d2e8; }}
      ul {{ padding-left: 22px; }}
      pre {{
        background: #0f1727;
        border: 1px solid #2c3b56;
        border-radius: 10px;
        padding: 14px;
        overflow: auto;
      }}
      code {{ font-family: Consolas, "Courier New", monospace; }}
    </style>
  </head>
  <body>
    <main class="wrap">
      <div class="bar">
        <a class="back" href="/#help-center">返回帮助文档列表</a>
      </div>
      <article class="doc">
        {''.join(body_parts)}
      </article>
    </main>
  </body>
</html>"""
        return HTMLResponse(content=page_html)

    @app.get("/api/brand")
    def get_brand(request: Request) -> dict[str, Any]:
        return {
            "instance": request.state.brand_instance,
            "settings": service.public_brand_settings(),
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

        control_backend = control_plane.control_backend()
        brand_backend = service.brand_database_backend()
        if control_backend == "supabase":
            record(
                "control_plane",
                lambda: {
                    "backend": control_backend,
                    "brand_count": len(control_plane.list_brand_instances()),
                },
            )
        else:
            record("control_plane", lambda: {"backend": control_backend, "brand_count": "local"})
        record(
            "tenant",
            lambda: {
                "brand_id": request.state.brand_instance.get("id"),
                "domain": request.state.brand_instance.get("domain"),
                "status": request.state.brand_instance.get("status"),
            },
        )
        record("brand_database", lambda: {"backend": brand_backend, "brand_name": service.load_brand_settings().get("name")})
        record("sync_outbox", service.sync_status)
        if brand_backend == "sqlite":
            record(
                "ai_robot_queue",
                lambda: {
                    "config_count": len(service.list_ai_robot_configs()),
                    "queued_message_count": len(service.list_ai_robot_messages()),
                },
            )
        else:
            record("ai_robot_queue", lambda: {"backend": brand_backend, "remote_check": "skipped"})

        ok = all(item["ok"] for item in checks)
        return {
            "ok": ok,
            "app_version": control_plane.APP_VERSION,
            "schema_version": control_plane.SCHEMA_VERSION,
            "brand_id": request.state.brand_instance.get("id"),
            "checks": checks,
        }

    @app.get("/api/video-matrix/health")
    def video_matrix_health() -> dict[str, Any]:
        return ffmpeg_runtime_health()

    @app.get("/api/system/database-dictionary")
    def database_dictionary() -> dict[str, Any]:
        return service.database_dictionary()

    @app.post("/api/system/supabase-read-cache/clear")
    def clear_supabase_read_cache_route() -> dict[str, Any]:
        return service.clear_supabase_read_cache()

    @app.post("/api/system/cache/clear")
    def clear_runtime_cache_route() -> dict[str, Any]:
        return service.clear_runtime_caches()

    @app.get("/api/sync/status")
    def sync_status_route() -> dict[str, Any]:
        return service.sync_status()

    @app.post("/api/sync/supabase/push")
    def push_sync_supabase_route(limit: int = Query(default=2000, ge=1, le=2000)) -> dict[str, Any]:
        return service.push_sync_outbox_to_supabase(limit=limit)

    @app.post("/api/sync/supabase/pull")
    def pull_sync_supabase_route() -> dict[str, Any]:
        return service.pull_accounts_from_supabase_to_sqlite()

    @app.post("/api/sync/retry")
    def retry_sync_route() -> dict[str, Any]:
        return service.retry_sync_outbox()

    @app.post("/api/system/initialize")
    def system_initialize(payload: SystemInitializePayload) -> dict[str, Any]:
        try:
            service.login_operator_user("allen", payload.password)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        return service.initialize_system()

    @app.patch("/api/brand")
    def update_brand(payload: BrandSettingsPayload) -> dict[str, Any]:
        return service.public_brand_settings(service.save_brand_settings(_model_payload(payload, exclude_unset=True)))

    @app.get("/api/auth/state")
    def auth_state(current_user_id: str = Query(default="allen"), editing_role_id: str = Query(default="super_admin")) -> dict[str, Any]:
        return service.operator_auth_state(current_user_id=current_user_id, editing_role_id=editing_role_id)

    @app.post("/api/auth/login")
    def auth_login(payload: OperatorLoginPayload) -> dict[str, Any]:
        try:
            return service.login_operator_user(payload.user_id, payload.password)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

    @app.post("/api/auth/users")
    def auth_create_user(payload: OperatorUserPayload) -> dict[str, Any]:
        try:
            return service.create_operator_user(payload.name, payload.role_id, payload.password)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.patch("/api/auth/users/{user_id}/role")
    def auth_update_user_role(user_id: str, payload: OperatorUserRolePayload) -> dict[str, Any]:
        try:
            return service.update_operator_user_role(user_id, payload.role_id)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.patch("/api/auth/users/{user_id}/password")
    def auth_update_user_password(user_id: str, payload: OperatorUserPasswordPayload) -> dict[str, Any]:
        try:
            return service.update_operator_user_password(user_id, payload.password)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/auth/roles")
    def auth_create_role(payload: OperatorRolePayload) -> dict[str, Any]:
        try:
            return service.create_operator_role(payload.name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.put("/api/auth/roles/{role_id}/permissions")
    def auth_save_permissions(role_id: str, payload: OperatorPermissionsPayload) -> dict[str, Any]:
        try:
            return service.save_operator_role_permissions(role_id, payload.permissions)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

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
        data = service.load_distribution_settings_db()
        return {**data, "resolved_material_dir": str(resolve_material_dir())}

    @app.patch("/api/settings/distribution")
    def update_distribution_settings(payload: DistributionSettingsPayload) -> dict[str, Any]:
        data = service.save_distribution_settings_db(_model_payload(payload))
        return {**data, "resolved_material_dir": str(resolve_material_dir())}

    @app.get("/api/operator-wechats")
    def operator_wechats() -> list[str]:
        return service.list_operator_wechats()

    @app.post("/api/operator-wechats")
    def add_operator_wechat(payload: OperatorWechatPayload) -> dict[str, Any]:
        try:
            return service.add_operator_wechat(payload.operator_wechat)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/settings/material-dir/open")
    def open_material_dir(payload: OpenMaterialDirPayload) -> dict[str, Any]:
        try:
            service.login_operator_user("allen", payload.password)
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        return service.open_material_directory(payload.material_dir)

    @app.post("/api/system/open-directory/{kind}")
    def open_system_directory(kind: str) -> dict[str, Any]:
        try:
            return service.open_system_directory(kind)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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

    @app.post("/api/jobs/matrix-wechat/login-check")
    def matrix_wechat_login_check() -> dict[str, Any]:
        return trigger_matrix_wechat_login_check()

    @app.get("/api/jobs/matrix-wechat/stats-capture/status")
    def matrix_wechat_stats_capture_status() -> dict[str, Any]:
        return capture_status()

    @app.post("/api/jobs/matrix-wechat/stats-capture/run-now")
    def matrix_wechat_stats_capture_run_now(payload: WechatStatsCapturePayload) -> dict[str, Any]:
        return trigger_matrix_wechat_stats_capture(
            target_date=payload.target_date,
            limit=payload.limit,
            dry_run=payload.dry_run,
        )

    @app.get("/api/login-qr-batches")
    def login_qr_batches(limit: int = Query(default=20)) -> list[dict[str, Any]]:
        return service.list_login_qr_batches(limit=limit)

    @app.get("/api/terminal-execution/state")
    def terminal_execution_state() -> dict[str, Any]:
        with _TERMINAL_EXECUTION_API_LOCK:
            return service.terminal_execution_state()

    @app.post("/api/terminal-execution/start")
    def terminal_execution_start(payload: TerminalStartPayload) -> dict[str, Any]:
        try:
            with _TERMINAL_EXECUTION_API_LOCK:
                return service.start_terminal_execution(_model_payload(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/terminal-execution/start-login")
    def terminal_execution_start_login() -> dict[str, Any]:
        try:
            with _TERMINAL_EXECUTION_API_LOCK:
                return service.start_terminal_login()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/terminal-execution/poll")
    def terminal_execution_poll(payload: TerminalPollPayload | None = Body(default=None)) -> dict[str, Any]:
        data = _model_payload(payload) if payload is not None else {}
        with _TERMINAL_EXECUTION_API_LOCK:
            return service.poll_terminal_execution(
                allow_browser_open=bool(data.get("allow_browser_open")),
                allow_login_probe=bool(data.get("allow_login_probe")),
            )

    @app.get("/api/terminal-execution/windows/{window_id}/qr-image")
    def terminal_execution_qr_image(window_id: int) -> FileResponse:
        path = service.terminal_qr_image_path(window_id)
        if path is None:
            raise HTTPException(status_code=404, detail="qr image not found")
        return FileResponse(
            path,
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )

    @app.post("/api/terminal-execution/windows/{window_id}/accounts/{account_id}/qr")
    def terminal_execution_account_qr(window_id: int, account_id: int) -> dict[str, Any]:
        try:
            with _TERMINAL_EXECUTION_API_LOCK:
                return service.open_terminal_account_qr(window_id, account_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/terminal-execution/windows/{window_id}/confirm-login")
    def terminal_execution_confirm_login(window_id: int) -> dict[str, Any]:
        try:
            with _TERMINAL_EXECUTION_API_LOCK:
                return service.confirm_terminal_login_ready(window_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/terminal-execution/windows/{window_id}/manual-publish")
    def terminal_execution_manual_publish(window_id: int) -> dict[str, Any]:
        try:
            with _TERMINAL_EXECUTION_API_LOCK:
                return service.manual_terminal_publish(window_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/terminal-execution/windows/{window_id}/accounts/{account_id}/reset-manual-flow")
    def terminal_execution_reset_manual_flow(window_id: int, account_id: int) -> dict[str, Any]:
        try:
            with _TERMINAL_EXECUTION_API_LOCK:
                return service.reset_terminal_manual_flow(window_id, account_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/terminal-execution/windows/{window_id}/confirm-publish-success")
    def terminal_execution_confirm_publish_success(window_id: int) -> dict[str, Any]:
        try:
            with _TERMINAL_EXECUTION_API_LOCK:
                return service.confirm_terminal_publish_success(window_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/notification-routes")
    def notification_routes() -> list[dict[str, Any]]:
        return service.list_notification_routes()

    @app.get("/api/notification-events")
    def notification_events() -> list[dict[str, Any]]:
        return service.list_notification_event_definitions()

    @app.get("/api/notification-policies")
    def notification_policies() -> list[dict[str, Any]]:
        return service.list_notification_policies()

    @app.put("/api/notification-policies")
    def save_notification_policies(payload: Any = Body(default_factory=dict)) -> list[dict[str, Any]]:
        try:
            return service.save_notification_policies(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/notification-incidents")
    def notification_incidents(status: str = "", limit: int = Query(default=100, ge=1, le=500)) -> list[dict[str, Any]]:
        return service.list_notification_incidents(status=status, limit=limit)

    @app.post("/api/notification-incidents/{incident_id}/{action}")
    def notification_incident_action(incident_id: int, action: str, payload: NotificationActionPayload = Body(default_factory=NotificationActionPayload)) -> dict[str, Any]:
        try:
            return service.act_on_notification_incident(incident_id, action, _model_payload(payload))
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/stats/notification-sla")
    def notification_sla() -> dict[str, Any]:
        return service.notification_sla_summary()

    @app.post("/api/notification-ops/daily-summary")
    def notification_daily_summary(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
        return service.send_daily_ops_summary(str(payload.get("target_date") or ""), notify=bool(payload.get("notify", True)))

    @app.post("/api/notification-routes/{event_type}/{platform}")
    def save_notification_route(event_type: str, platform: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            enabled = bool(payload.get("enabled"))
            route = service.save_notification_route(event_type, platform, enabled)
            if enabled and bool(payload.get("send_probe")):
                route["probe"] = service.send_notification_route_probe(event_type, platform)
            return route
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/accounts")
    def accounts() -> list[dict[str, Any]]:
        return service.list_accounts()

    @app.post("/api/accounts")
    def create_account(payload: AccountPayload) -> dict[str, Any]:
        try:
            return service.create_account(_model_payload(payload))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/accounts/repair-config")
    def repair_account_configs() -> dict[str, Any]:
        return service.repair_account_configs()

    @app.patch("/api/accounts/{account_id}")
    def update_account(account_id: int, payload: AccountPayload) -> dict[str, Any]:
        account = service.update_account(account_id, _model_payload(payload, exclude_unset=True))
        if account is None:
            raise HTTPException(status_code=404, detail="account not found")
        return account

    @app.delete("/api/accounts/{account_id}")
    def delete_account(account_id: int) -> dict[str, Any]:
        if not service.delete_account(account_id):
            raise HTTPException(status_code=404, detail="account not found")
        return {"ok": True, "deleted": account_id}

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

    @app.post("/api/tasks/bulk-delete")
    def bulk_delete_tasks(payload: TaskBulkPayload) -> dict[str, Any]:
        return {"ok": True, "deleted": service.delete_tasks(payload.ids)}

    @app.post("/api/tasks/bulk-status")
    def bulk_update_task_status(payload: TaskStatusPayload) -> dict[str, Any]:
        try:
            updated = service.update_tasks_status(payload.ids, payload.status)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True, "updated": updated, "status": payload.status}

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
    def stats(
        account_id: int | None = Query(default=None),
        platform: str = Query(default=""),
        stat_date: str = Query(default=""),
    ) -> list[dict[str, Any]]:
        return service.list_stats(account_id=account_id, platform=platform, stat_date=stat_date)

    @app.get("/api/stats/accounts")
    def account_stats(account_id: int | None = Query(default=None), stat_date: str = Query(default="")) -> list[dict[str, Any]]:
        return service.list_wechat_account_stats(account_id=account_id, stat_date=stat_date)

    @app.get("/api/stats/analytics")
    def stats_analytics() -> dict[str, list[dict[str, Any]]]:
        return service.list_analytics_items()

    @app.post("/api/stats/import")
    def import_stats(payload: dict[str, Any]) -> dict[str, Any]:
        return service.import_stats(payload)

    return app


app = create_app()

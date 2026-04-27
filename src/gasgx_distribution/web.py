from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import service
from .platforms import SUPPORTED_PLATFORMS
from .public_settings import (
    load_distribution_settings,
    load_wechat_publish_settings,
    save_distribution_settings,
    save_wechat_publish_settings,
)


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
    collection_name: str = ""
    caption: str = ""
    declare_original: bool = False
    upload_timeout: int = 60


class DistributionSettingsPayload(BaseModel):
    common: dict[str, Any] = Field(default_factory=dict)
    platforms: dict[str, dict[str, Any]] = Field(default_factory=dict)


def create_app() -> FastAPI:
    service.ensure_database()
    app = FastAPI(title="GasGx Video Distribution", version="0.1.0")
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

    @app.get("/api/settings/wechat-publish")
    def get_wechat_publish_settings() -> dict[str, Any]:
        return load_wechat_publish_settings()

    @app.patch("/api/settings/wechat-publish")
    def update_wechat_publish_settings(payload: WechatPublishSettingsPayload) -> dict[str, Any]:
        return save_wechat_publish_settings(_model_payload(payload))

    @app.get("/api/settings/distribution")
    def get_distribution_settings() -> dict[str, Any]:
        return load_distribution_settings()

    @app.patch("/api/settings/distribution")
    def update_distribution_settings(payload: DistributionSettingsPayload) -> dict[str, Any]:
        return save_distribution_settings(_model_payload(payload))

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

    @app.get("/api/stats")
    def stats(account_id: int | None = Query(default=None), platform: str = Query(default="")) -> list[dict[str, Any]]:
        return service.list_stats(account_id=account_id, platform=platform)

    @app.post("/api/stats/import")
    def import_stats(payload: dict[str, Any]) -> dict[str, Any]:
        return service.import_stats(payload)

    return app


app = create_app()

from __future__ import annotations

import json
import os
from typing import Any, Mapping

from .telegram_api import call_telegram_api


DEFAULT_TELEGRAM_TIMEOUT_SECONDS = 20
DEFAULT_TELEGRAM_API_BASE = "https://api.telegram.org"


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def resolve_telegram_bot_settings(
    raw: Mapping[str, Any] | None = None,
    *,
    env_prefix: str = "NOTIFY_",
) -> dict[str, Any]:
    source = raw if isinstance(raw, Mapping) else {}
    prefix = str(env_prefix or "").strip()

    def _env(name: str) -> str:
        if not prefix:
            return ""
        return str(os.getenv(f"{prefix}{name}", "") or "").strip()

    bot_token = str(source.get("bot_token") or source.get("token") or "").strip()
    if not bot_token:
        bot_token = _env("TELEGRAM_BOT_TOKEN") or _env("TELEGRAM_TOKEN")
    chat_id = str(source.get("chat_id") or "").strip() or _env("TELEGRAM_CHAT_ID")
    timeout_seconds = max(
        5,
        _to_int(source.get("timeout_seconds"), _to_int(_env("TELEGRAM_TIMEOUT_SECONDS"), DEFAULT_TELEGRAM_TIMEOUT_SECONDS)),
    )
    api_base = str(source.get("api_base") or "").strip() or _env("TELEGRAM_API_BASE") or DEFAULT_TELEGRAM_API_BASE
    return {
        "bot_token": bot_token,
        "chat_id": chat_id,
        "timeout_seconds": timeout_seconds,
        "api_base": api_base,
    }


def send_notification(
    *,
    subject: str = "",
    text_body: str = "",
    provider: str = "telegram_bot",
    env_prefix: str = "NOTIFY_",
    telegram: Mapping[str, Any] | None = None,
    card_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    provider_name = str(provider or "telegram_bot").strip().lower()
    result = {"ok": False, "provider": provider_name, "error": ""}
    if provider_name != "telegram_bot":
        result["error"] = f"unsupported provider: {provider_name}"
        return result

    settings = resolve_telegram_bot_settings(telegram, env_prefix=env_prefix)
    bot_token = str(settings.get("bot_token") or "").strip()
    chat_id = str(settings.get("chat_id") or "").strip()
    if not bot_token or not chat_id:
        result["error"] = "missing telegram bot_token/chat_id"
        return result

    lines = [str(subject or "").strip(), str(text_body or "").strip()]
    text = "\n\n".join(part for part in lines if part).strip()
    if not text and isinstance(card_payload, Mapping):
        text = str(card_payload.get("text") or "").strip()
    if not text:
        result["error"] = "empty notification body"
        return result

    params: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": "false",
    }
    if isinstance(card_payload, Mapping):
        parse_mode = str(card_payload.get("parse_mode") or "").strip()
        reply_markup = card_payload.get("reply_markup")
        if parse_mode:
            params["parse_mode"] = parse_mode
        if reply_markup:
            params["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    try:
        response = call_telegram_api(
            bot_token=bot_token,
            method="sendMessage",
            params=params,
            timeout_seconds=int(settings.get("timeout_seconds") or DEFAULT_TELEGRAM_TIMEOUT_SECONDS),
            api_base=str(settings.get("api_base") or DEFAULT_TELEGRAM_API_BASE),
            use_post=True,
        )
        result["ok"] = True
        result["response"] = response
        return result
    except Exception as exc:
        result["error"] = str(exc)
        return result

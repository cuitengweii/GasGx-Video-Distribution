from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any, Dict, Mapping, Optional

import requests

from .env import load_dotenv_if_available

DEFAULT_TELEGRAM_API_BASE = "https://api.telegram.org"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_UPDATES_LIMIT = 100

_TOKEN_PATTERN = re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b")
_USERNAME_PATTERN = re.compile(r"(?:https?://)?t\.me/([A-Za-z0-9_]{4,})", re.IGNORECASE)


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        key = str(name or "").strip()
        if not key:
            continue
        value = str(os.getenv(key, "") or "").strip()
        if value:
            return value
    return str(default or "").strip()


def extract_botfather_bootstrap(text: str) -> Dict[str, str]:
    raw = str(text or "")
    token_match = _TOKEN_PATTERN.search(raw)
    user_match = _USERNAME_PATTERN.search(raw)
    return {
        "bot_token": str(token_match.group(0)).strip() if token_match else "",
        "bot_username": str(user_match.group(1)).strip() if user_match else "",
    }


def _telegram_get(
    *,
    bot_token: str,
    method: str,
    api_base: str,
    timeout_seconds: int,
    params: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    base = str(api_base or DEFAULT_TELEGRAM_API_BASE).rstrip("/")
    url = f"{base}/bot{bot_token}/{method}"
    resp = requests.get(url, params=dict(params or {}), timeout=max(5, int(timeout_seconds)))
    payload = resp.json() if (resp.text or "").strip() else {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"telegram {method} invalid response")
    if not bool(payload.get("ok")):
        raise RuntimeError(f"telegram {method} failed: {payload.get('description') or 'unknown'}")
    return payload


def _extract_chat(msg: Mapping[str, Any]) -> Dict[str, str]:
    chat = msg.get("chat") if isinstance(msg.get("chat"), Mapping) else {}
    return {
        "chat_id": str(chat.get("id") or "").strip(),
        "chat_type": str(chat.get("type") or "").strip(),
        "chat_title": str(chat.get("title") or "").strip(),
        "chat_username": str(chat.get("username") or "").strip(),
        "first_name": str(chat.get("first_name") or "").strip(),
        "last_name": str(chat.get("last_name") or "").strip(),
        "text_preview": str(msg.get("text") or msg.get("caption") or "").strip(),
    }


def _extract_update_chat(update: Mapping[str, Any]) -> Dict[str, str]:
    for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
        msg = update.get(key)
        if isinstance(msg, Mapping):
            return _extract_chat(msg)

    callback = update.get("callback_query")
    if isinstance(callback, Mapping):
        msg = callback.get("message")
        if isinstance(msg, Mapping):
            return _extract_chat(msg)

    member = update.get("my_chat_member")
    if isinstance(member, Mapping):
        chat = member.get("chat") if isinstance(member.get("chat"), Mapping) else {}
        return {
            "chat_id": str(chat.get("id") or "").strip(),
            "chat_type": str(chat.get("type") or "").strip(),
            "chat_title": str(chat.get("title") or "").strip(),
            "chat_username": str(chat.get("username") or "").strip(),
            "first_name": str(chat.get("first_name") or "").strip(),
            "last_name": str(chat.get("last_name") or "").strip(),
            "text_preview": "",
        }

    return {
        "chat_id": "",
        "chat_type": "",
        "chat_title": "",
        "chat_username": "",
        "first_name": "",
        "last_name": "",
        "text_preview": "",
    }


def find_chat_candidates(
    *,
    bot_token: str,
    api_base: str = DEFAULT_TELEGRAM_API_BASE,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    limit: int = DEFAULT_UPDATES_LIMIT,
    offset: int = 0,
) -> Dict[str, Any]:
    token = str(bot_token or "").strip()
    if not token:
        return {"ok": False, "error": "missing bot token", "chat_candidates": []}

    bot_resp = _telegram_get(
        bot_token=token,
        method="getMe",
        api_base=api_base,
        timeout_seconds=timeout_seconds,
    )
    bot_info = bot_resp.get("result") if isinstance(bot_resp, Mapping) else {}
    if not isinstance(bot_info, Mapping):
        bot_info = {}

    updates_resp = _telegram_get(
        bot_token=token,
        method="getUpdates",
        api_base=api_base,
        timeout_seconds=timeout_seconds,
        params={
            "limit": max(1, min(100, _to_int(limit, DEFAULT_UPDATES_LIMIT))),
            "offset": max(0, _to_int(offset, 0)),
        },
    )
    updates = updates_resp.get("result") if isinstance(updates_resp, Mapping) else []
    if not isinstance(updates, list):
        updates = []

    by_chat_id: Dict[str, Dict[str, Any]] = {}
    for item in updates:
        if not isinstance(item, Mapping):
            continue
        update_id = _to_int(item.get("update_id"), 0)
        chat = _extract_update_chat(item)
        chat_id = str(chat.get("chat_id") or "").strip()
        if not chat_id:
            continue
        current = by_chat_id.get(chat_id, {})
        prev_update = _to_int(current.get("latest_update_id"), -1)
        if update_id < prev_update:
            continue
        by_chat_id[chat_id] = {
            "chat_id": chat_id,
            "chat_type": str(chat.get("chat_type") or "").strip(),
            "chat_title": str(chat.get("chat_title") or "").strip(),
            "chat_username": str(chat.get("chat_username") or "").strip(),
            "first_name": str(chat.get("first_name") or "").strip(),
            "last_name": str(chat.get("last_name") or "").strip(),
            "text_preview": str(chat.get("text_preview") or "").strip(),
            "latest_update_id": update_id,
        }

    candidates = sorted(
        by_chat_id.values(),
        key=lambda x: _to_int(x.get("latest_update_id"), 0),
        reverse=True,
    )
    return {
        "ok": True,
        "error": "",
        "bot": {
            "id": _to_int(bot_info.get("id"), 0),
            "username": str(bot_info.get("username") or "").strip(),
            "first_name": str(bot_info.get("first_name") or "").strip(),
        },
        "updates_count": len(updates),
        "chat_candidates": candidates,
    }


def resolve_chat_id_from_botfather_message(
    *,
    botfather_text: str,
    api_base: str = DEFAULT_TELEGRAM_API_BASE,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    limit: int = DEFAULT_UPDATES_LIMIT,
    offset: int = 0,
) -> Dict[str, Any]:
    extracted = extract_botfather_bootstrap(botfather_text)
    token = str(extracted.get("bot_token") or "").strip()
    if not token:
        return {
            "ok": False,
            "error": "bot token not found in botfather text",
            "bot_token": "",
            "bot_username": str(extracted.get("bot_username") or "").strip(),
            "chat_candidates": [],
        }
    result = find_chat_candidates(
        bot_token=token,
        api_base=api_base,
        timeout_seconds=timeout_seconds,
        limit=limit,
        offset=offset,
    )
    result["bot_token"] = token
    result["bot_username"] = str(extracted.get("bot_username") or "").strip()
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resolve Telegram chat_id from BotFather new-bot message text."
    )
    parser.add_argument("--bot-token", default="", help="Telegram bot token. If empty, parse from --botfather-*.")
    parser.add_argument("--botfather-text", default="", help="Raw message text from @BotFather.")
    parser.add_argument("--botfather-file", default="", help="Path of a text file containing BotFather message.")
    parser.add_argument("--api-base", default=DEFAULT_TELEGRAM_API_BASE)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--limit", type=int, default=DEFAULT_UPDATES_LIMIT)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--json", action="store_true", help="Print full JSON.")
    return parser


def main() -> int:
    load_dotenv_if_available()
    args = _build_parser().parse_args()

    botfather_text = str(args.botfather_text or "")
    botfather_file = str(args.botfather_file or "").strip()
    if botfather_file:
        try:
            botfather_text = open(botfather_file, "r", encoding="utf-8").read()
        except Exception as exc:
            print(f"[Resolver] Failed to read --botfather-file: {exc}")
            return 2

    extracted = extract_botfather_bootstrap(botfather_text)
    bot_token = str(args.bot_token or "").strip() or str(extracted.get("bot_token") or "").strip()
    if not bot_token:
        bot_token = _env_first(
            "CYBERCAR_NOTIFY_TELEGRAM_BOT_TOKEN",
            "NOTIFY_TELEGRAM_BOT_TOKEN",
            "CYBERCAR_NOTIFY_TELEGRAM_TOKEN",
            "NOTIFY_TELEGRAM_TOKEN",
            default="",
        )
    if not bot_token:
        print("[Resolver] Missing bot token. Provide --bot-token or --botfather-text.")
        return 2

    try:
        result = find_chat_candidates(
            bot_token=bot_token,
            api_base=str(args.api_base or DEFAULT_TELEGRAM_API_BASE),
            timeout_seconds=max(5, int(args.timeout_seconds)),
            limit=max(1, min(100, int(args.limit))),
            offset=max(0, int(args.offset)),
        )
    except Exception as exc:
        print(f"[Resolver] Query failed: {exc}")
        return 1

    # Attach parse metadata for easier debugging/output.
    result["bot_username_from_text"] = str(extracted.get("bot_username") or "").strip()
    result["token_from_text"] = bool(extracted.get("bot_token"))

    if bool(args.json):
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("ok") else 1

    if not bool(result.get("ok")):
        print(f"[Resolver] Failed: {result.get('error') or 'unknown'}")
        return 1

    bot = result.get("bot") if isinstance(result.get("bot"), Mapping) else {}
    print(f"[Resolver] Bot: @{bot.get('username') or '-'} (id={bot.get('id') or 0})")
    print(f"[Resolver] Updates scanned: {result.get('updates_count') or 0}")

    rows = result.get("chat_candidates") if isinstance(result.get("chat_candidates"), list) else []
    if not rows:
        print("[Resolver] No chat_id found yet.")
        print("[Resolver] Send one message to the bot first (/start in private chat), then retry.")
        return 3

    print("[Resolver] Found chat candidates:")
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        chat_id = str(row.get("chat_id") or "").strip()
        chat_type = str(row.get("chat_type") or "").strip() or "-"
        title = str(row.get("chat_title") or "").strip()
        username = str(row.get("chat_username") or "").strip()
        first_name = str(row.get("first_name") or "").strip()
        preview = str(row.get("text_preview") or "").strip()
        label = title or username or first_name or "-"
        print(f"  - chat_id={chat_id} | type={chat_type} | name={label} | preview={preview[:80]}")

    top = rows[0] if isinstance(rows[0], Mapping) else {}
    top_chat_id = str(top.get("chat_id") or "").strip()
    if top_chat_id:
        print("")
        print(f"[Resolver] Recommended NOTIFY_TELEGRAM_CHAT_ID={top_chat_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping

from .env import load_dotenv_if_available
from .telegram_chat_id_resolver import extract_botfather_bootstrap, find_chat_candidates

try:
    from cybercar.settings import get_paths as _get_paths
except Exception:
    _get_paths = None  # type: ignore


def _default_registry_file() -> Path:
    if _get_paths is not None:
        try:
            return (_get_paths().runtime_root / "secrets" / "telegram_bot_registry.json").resolve()
        except Exception:
            pass
    return (Path(__file__).resolve().parents[3] / "runtime" / "secrets" / "telegram_bot_registry.json").resolve()


DEFAULT_REGISTRY_FILE = _default_registry_file()

_TOKEN_PATTERN = re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b")
_USERNAME_PATTERN = re.compile(r"(?:https?://)?t\.me/([A-Za-z0-9_]{4,})", re.IGNORECASE)
_BOT_NAME_PATTERN = re.compile(
    r"(?im)^\s*(?:name|bot_name|bot-name|robot_name|telegram_name|display_name)\s*[:=]\s*(.+?)\s*$"
)
_CHAT_ID_PATTERN = re.compile(r"(?im)^\s*(?:chat_id|chat-id|chatid)\s*[:=]\s*(-?\d+)\s*$")
_MANAGER_PATTERN = re.compile(r"(?im)^\s*(?:is_manager|manager|master)\s*[:=]\s*(true|false|1|0|yes|no)\s*$")
_KEYWORDS_PATTERN = re.compile(r"(?im)^\s*(?:keywords|keyword|keys|route_keywords)\s*[:=]\s*(.+?)\s*$")
_HANDLER_TYPE_PATTERN = re.compile(r"(?im)^\s*(?:handler_type|handler|route_handler)\s*[:=]\s*(.+?)\s*$")
_HANDLER_ENABLED_PATTERN = re.compile(r"(?im)^\s*(?:handler_enabled|handler_on|route_enabled)\s*[:=]\s*(true|false|1|0|yes|no)\s*$")
_AUTO_ALIAS_SPLIT_PATTERN = re.compile(r"[_\-\s]+")
_AUTO_ALIAS_SKIP_TOKENS = {"cui", "bot", "telegram", "robot", "notify"}
_AUTO_ALIAS_BLOCKLIST = {"cybercar"}
_AUTO_ALIAS_MIN_LENGTH = 3
_SWITCHBOT_ALIAS_OVERRIDES = {"cyber": "CyberCar", "cybercar": "CyberCar"}

HANDLER_TYPE_MANAGER = "manager"
HANDLER_TYPE_CYBERCAR = "cybercar"
HANDLER_TYPE_PASSIVE = "passive"
VALID_HANDLER_TYPES = {
    HANDLER_TYPE_MANAGER,
    HANDLER_TYPE_CYBERCAR,
    HANDLER_TYPE_PASSIVE,
}
DEFAULT_HANDLER_TYPE = HANDLER_TYPE_PASSIVE
COMMAND_PROFILE_AUTO = "auto"
COMMAND_PROFILE_MANAGER = HANDLER_TYPE_MANAGER
COMMAND_PROFILE_CYBERCAR = HANDLER_TYPE_CYBERCAR
COMMAND_PROFILE_GASGX = "gasgx"
VALID_COMMAND_PROFILES = {
    COMMAND_PROFILE_AUTO,
    COMMAND_PROFILE_MANAGER,
    COMMAND_PROFILE_CYBERCAR,
    COMMAND_PROFILE_GASGX,
}


def _is_legacy_cyber_alias(token: str) -> bool:
    # Accept historical naming to keep old registry values working.
    return token == ("cyber" + "truck")


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = str(value or "").strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _normalize_username(value: Any) -> str:
    token = str(value or "").strip()
    if token.startswith("@"):
        token = token[1:]
    return token


def _mask_token(value: str) -> str:
    token = str(value or "").strip()
    if len(token) <= 14:
        return token
    return f"{token[:8]}...{token[-6:]}"


def normalize_handler_type(value: Any, *, is_manager: bool = False) -> str:
    token = str(value or "").strip().lower()
    if _is_legacy_cyber_alias(token):
        token = HANDLER_TYPE_CYBERCAR
    if token not in VALID_HANDLER_TYPES:
        token = HANDLER_TYPE_MANAGER if bool(is_manager) else DEFAULT_HANDLER_TYPE
    if bool(is_manager):
        return HANDLER_TYPE_MANAGER
    return token


def parse_handler_type(value: Any) -> str:
    token = str(value or "").strip().lower()
    if _is_legacy_cyber_alias(token):
        token = HANDLER_TYPE_CYBERCAR
    return token if token in VALID_HANDLER_TYPES else ""


def normalize_command_profile(value: Any) -> str:
    token = str(value or "").strip().lower()
    if _is_legacy_cyber_alias(token):
        token = COMMAND_PROFILE_CYBERCAR
    return token if token in VALID_COMMAND_PROFILES else COMMAND_PROFILE_AUTO


def default_registry() -> Dict[str, Any]:
    return {
        "version": 1,
        "manager_bot_username": "",
        "updated_at": _now_text(),
        "bots": [],
    }


def _split_keywords(value: Any) -> List[str]:
    if isinstance(value, list):
        raw_parts = [str(x or "").strip() for x in value]
    else:
        raw_parts = [x.strip() for x in re.split(r"[,\n\r;\s]+", str(value or ""))]
    output: List[str] = []
    seen: set[str] = set()
    for part in raw_parts:
        token = str(part or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(token)
    return output


def _merge_keywords(*values: Any) -> List[str]:
    merged: List[str] = []
    for item in values:
        if isinstance(item, list):
            merged.extend(_split_keywords(item))
        else:
            merged.extend(_split_keywords(str(item or "")))
    return _split_keywords(merged)


def _primary_alias_token(*values: str) -> str:
    for raw in values:
        parts = [x.strip().lower() for x in _AUTO_ALIAS_SPLIT_PATTERN.split(str(raw or "")) if x.strip()]
        for token in parts:
            if len(token) < _AUTO_ALIAS_MIN_LENGTH:
                continue
            if token in _AUTO_ALIAS_SKIP_TOKENS:
                continue
            if token in _AUTO_ALIAS_BLOCKLIST:
                continue
            return token
    return ""


def _auto_alias_keywords(bot_name: str, bot_username: str, is_manager: bool) -> List[str]:
    aliases: List[str] = []
    primary = _primary_alias_token(str(bot_username or ""), str(bot_name or ""))
    if primary:
        aliases.append(primary)
    if bool(is_manager):
        aliases.append("bot")
    return _split_keywords(aliases)


def _default_keywords(bot_name: str, bot_username: str, is_manager: bool = False) -> List[str]:
    values: List[str] = []
    for item in (str(bot_name or "").strip(), str(bot_username or "").strip()):
        if item:
            values.append(item)
    values.extend(_auto_alias_keywords(bot_name, bot_username, bool(is_manager)))
    return _merge_keywords(values)


def _normalize_bot_record(raw: Mapping[str, Any]) -> Dict[str, Any]:
    username = _normalize_username(raw.get("bot_username") or raw.get("telegram_unique_id") or "")
    name = str(raw.get("bot_name") or "").strip() or username
    token = str(raw.get("bot_token") or "").strip()
    chat_id = str(raw.get("chat_id") or "").strip()
    is_manager = bool(raw.get("is_manager"))
    handler_type = normalize_handler_type(raw.get("handler_type"), is_manager=is_manager)
    handler_enabled = _to_bool(raw.get("handler_enabled"), default=True)
    command_profile = normalize_command_profile(raw.get("command_profile"))
    keywords = _merge_keywords(raw.get("keywords"), _default_keywords(name, username, is_manager))
    return {
        "bot_name": name,
        "bot_username": username,
        "telegram_unique_id": username,
        "bot_token": token,
        "chat_id": chat_id,
        "keywords": keywords,
        "is_manager": is_manager,
        "handler_type": handler_type,
        "handler_enabled": handler_enabled,
        "command_profile": command_profile,
        "enabled": _to_bool(raw.get("enabled"), default=True),
        "config_text": str(raw.get("config_text") or "").strip(),
        "created_at": str(raw.get("created_at") or "").strip() or _now_text(),
        "updated_at": str(raw.get("updated_at") or "").strip() or _now_text(),
    }


def _normalize_registry(raw: Mapping[str, Any]) -> Dict[str, Any]:
    data = default_registry()
    data["version"] = int(raw.get("version") or 1)
    data["manager_bot_username"] = _normalize_username(raw.get("manager_bot_username") or "")
    data["updated_at"] = str(raw.get("updated_at") or "").strip() or _now_text()

    bots = raw.get("bots")
    rows: List[Dict[str, Any]] = []
    if isinstance(bots, list):
        for item in bots:
            if isinstance(item, Mapping):
                row = _normalize_bot_record(item)
                if row["bot_username"] or row["bot_token"]:
                    rows.append(row)
    manager_username = data["manager_bot_username"]
    for row in rows:
        if not isinstance(row, dict):
            continue
        same_manager = _normalize_username(row.get("bot_username") or "") == manager_username
        row["is_manager"] = bool(same_manager)
        normalized_type = normalize_handler_type(row.get("handler_type"), is_manager=bool(same_manager))
        if (not same_manager) and normalized_type == HANDLER_TYPE_MANAGER:
            normalized_type = DEFAULT_HANDLER_TYPE
        row["handler_type"] = normalized_type
        row["command_profile"] = normalize_command_profile(row.get("command_profile"))
    data["bots"] = rows
    if (not data["manager_bot_username"]) and rows:
        data["manager_bot_username"] = _normalize_username(rows[0].get("bot_username") or "")
        rows[0]["is_manager"] = True
        rows[0]["handler_type"] = HANDLER_TYPE_MANAGER
        rows[0]["keywords"] = _merge_keywords(
            rows[0].get("keywords"),
            _default_keywords(
                str(rows[0].get("bot_name") or ""),
                str(rows[0].get("bot_username") or ""),
                True,
            ),
        )
    return data


def load_registry(path: Path | str = DEFAULT_REGISTRY_FILE) -> Dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return default_registry()
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return default_registry()
    if not isinstance(payload, Mapping):
        return default_registry()
    return _normalize_registry(payload)


def save_registry(data: Mapping[str, Any], path: Path | str = DEFAULT_REGISTRY_FILE) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_registry(data if isinstance(data, Mapping) else {})
    normalized["updated_at"] = _now_text()
    target.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def set_manager_bot(registry: Dict[str, Any], bot_username: str) -> bool:
    username = _normalize_username(bot_username)
    if not username:
        return False
    bots = registry.get("bots")
    if not isinstance(bots, list):
        bots = []
        registry["bots"] = bots
    found = False
    for row in bots:
        if not isinstance(row, dict):
            continue
        was_manager = bool(row.get("is_manager"))
        same = _normalize_username(row.get("bot_username") or "") == username
        row["is_manager"] = bool(same)
        existing_handler = normalize_handler_type(row.get("handler_type"), is_manager=was_manager)
        if same:
            row["handler_type"] = HANDLER_TYPE_MANAGER
        elif was_manager and existing_handler == HANDLER_TYPE_MANAGER:
            row["handler_type"] = DEFAULT_HANDLER_TYPE
        else:
            row["handler_type"] = normalize_handler_type(existing_handler, is_manager=False)
        row["handler_enabled"] = _to_bool(row.get("handler_enabled"), default=True)
        row["keywords"] = _merge_keywords(
            row.get("keywords"),
            _default_keywords(
                str(row.get("bot_name") or ""),
                str(row.get("bot_username") or ""),
                bool(same),
            ),
        )
        if same:
            found = True
            row["updated_at"] = _now_text()
    if found:
        registry["manager_bot_username"] = username
        registry["updated_at"] = _now_text()
    return found


def set_bot_chat_id(registry: Dict[str, Any], bot_username: str, chat_id: str) -> bool:
    username = _normalize_username(bot_username)
    value = str(chat_id or "").strip()
    if not username or not value:
        return False
    bots = registry.get("bots")
    if not isinstance(bots, list):
        return False
    for row in bots:
        if not isinstance(row, dict):
            continue
        if _normalize_username(row.get("bot_username") or "") == username:
            row["chat_id"] = value
            row["updated_at"] = _now_text()
            registry["updated_at"] = _now_text()
            return True
    return False


def set_bot_keywords(registry: Dict[str, Any], bot_username: str, keywords: Any) -> bool:
    username = _normalize_username(bot_username)
    values = _split_keywords(keywords)
    if not username or not values:
        return False
    bots = registry.get("bots")
    if not isinstance(bots, list):
        return False
    for row in bots:
        if not isinstance(row, dict):
            continue
        if _normalize_username(row.get("bot_username") or "") == username:
            row["keywords"] = _merge_keywords(
                values,
                _default_keywords(
                    str(row.get("bot_name") or ""),
                    str(row.get("bot_username") or ""),
                    bool(row.get("is_manager")),
                ),
            )
            row["updated_at"] = _now_text()
            registry["updated_at"] = _now_text()
            return True
    return False


def set_bot_handler_type(registry: Dict[str, Any], bot_username: str, handler_type: str) -> bool:
    username = _normalize_username(bot_username)
    target_handler = parse_handler_type(handler_type)
    if not username or not target_handler:
        return False
    bots = registry.get("bots")
    if not isinstance(bots, list):
        return False
    for row in bots:
        if not isinstance(row, dict):
            continue
        if _normalize_username(row.get("bot_username") or "") != username:
            continue
        is_manager = bool(row.get("is_manager"))
        if (not is_manager) and target_handler == HANDLER_TYPE_MANAGER:
            return False
        if is_manager and target_handler != HANDLER_TYPE_MANAGER:
            return False
        row["handler_type"] = normalize_handler_type(target_handler, is_manager=is_manager)
        row["updated_at"] = _now_text()
        registry["updated_at"] = _now_text()
        return True
    return False


def set_bot_handler_enabled(registry: Dict[str, Any], bot_username: str, enabled: bool) -> bool:
    username = _normalize_username(bot_username)
    if not username:
        return False
    bots = registry.get("bots")
    if not isinstance(bots, list):
        return False
    for row in bots:
        if not isinstance(row, dict):
            continue
        if _normalize_username(row.get("bot_username") or "") == username:
            row["handler_enabled"] = bool(enabled)
            row["updated_at"] = _now_text()
            registry["updated_at"] = _now_text()
            return True
    return False


def set_active_cybercar_bot(registry: Dict[str, Any], bot_username: str) -> bool:
    username = _normalize_username(bot_username)
    if not username:
        return False
    bots = registry.get("bots")
    if not isinstance(bots, list):
        return False
    found = False
    for row in bots:
        if not isinstance(row, dict):
            continue
        row_username = _normalize_username(row.get("bot_username") or "")
        is_manager = bool(row.get("is_manager"))
        row_handler_enabled = _to_bool(row.get("handler_enabled"), default=True)
        if row_username == username:
            if is_manager:
                return False
            if not row_handler_enabled:
                return False
            found = True
    if not found:
        return False

    changed = False
    now = _now_text()
    for row in bots:
        if not isinstance(row, dict):
            continue
        if not _to_bool(row.get("handler_enabled"), default=True):
            continue
        row_username = _normalize_username(row.get("bot_username") or "")
        is_manager = bool(row.get("is_manager"))
        target_type = HANDLER_TYPE_MANAGER if is_manager else (HANDLER_TYPE_CYBERCAR if row_username == username else HANDLER_TYPE_PASSIVE)
        current_type = normalize_handler_type(row.get("handler_type"), is_manager=is_manager)
        if current_type != target_type:
            row["handler_type"] = target_type
            row["updated_at"] = now
            changed = True

    if changed:
        registry["updated_at"] = now
    return True


def list_switchable_command_bots(registry: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows = registry.get("bots")
    if not isinstance(rows, list):
        return []
    manager_username = _normalize_username(registry.get("manager_bot_username") or "")
    output: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        if not _to_bool(row.get("enabled"), default=True):
            continue
        if not _to_bool(row.get("handler_enabled"), default=True):
            continue
        username = _normalize_username(row.get("bot_username") or "")
        if not username:
            continue
        is_manager = bool(row.get("is_manager")) or (username == manager_username)
        if is_manager:
            continue
        handler_type = normalize_handler_type(row.get("handler_type"), is_manager=False)
        project_alias = _switchbot_project_alias(
            bot_username=str(row.get("bot_username") or ""),
            bot_name=str(row.get("bot_name") or ""),
        )
        output.append(
            {
                "bot_name": str(row.get("bot_name") or "").strip(),
                "bot_username": username,
                "project_alias": project_alias,
                "chat_id": str(row.get("chat_id") or "").strip(),
                "enabled": True,
                "handler_enabled": _to_bool(row.get("handler_enabled"), default=True),
                "handler_type": handler_type,
                "command_profile": normalize_command_profile(row.get("command_profile")),
                "is_active_command": bool(_to_bool(row.get("handler_enabled"), default=True) and handler_type == HANDLER_TYPE_CYBERCAR),
            }
        )
    return output

def _switchbot_first_word(*values: Any) -> str:
    for raw in values:
        parts = [x.strip() for x in _AUTO_ALIAS_SPLIT_PATTERN.split(str(raw or "")) if str(x or "").strip()]
        for part in parts:
            token = str(part).strip()
            if not token:
                continue
            if token.lower() in _AUTO_ALIAS_SKIP_TOKENS:
                continue
            return token
    return ""


def _switchbot_project_alias(*, bot_username: str, bot_name: str) -> str:
    primary = _switchbot_first_word(str(bot_username or ""), str(bot_name or ""))
    if not primary:
        primary = str(bot_username or "").strip() or str(bot_name or "").strip()
    if primary.lower() in _SWITCHBOT_ALIAS_OVERRIDES:
        return str(_SWITCHBOT_ALIAS_OVERRIDES.get(primary.lower()) or "").strip() or primary
    return primary


def resolve_switchable_command_bot_username(registry: Mapping[str, Any], selector: Any) -> str:
    target = _normalize_username(selector)
    if not target:
        return ""
    rows = list_switchable_command_bots(registry)
    if not rows:
        return ""
    for row in rows:
        username = _normalize_username(row.get("bot_username") or "")
        if username and username.lower() == target.lower():
            return username
    alias_hits: List[str] = []
    legacy_hits: List[str] = []
    for row in rows:
        alias = str(row.get("project_alias") or "").strip()
        username = _normalize_username(row.get("bot_username") or "")
        if alias and username and alias.lower() == target.lower():
            alias_hits.append(username)
        raw_first = _switchbot_first_word(str(row.get("bot_username") or ""), str(row.get("bot_name") or ""))
        if raw_first and username and raw_first.lower() == target.lower():
            legacy_hits.append(username)
    if len(alias_hits) == 1:
        return alias_hits[0]
    if len(legacy_hits) == 1:
        return legacy_hits[0]
    return ""


def _extract_fields(text: str) -> Dict[str, Any]:
    raw = str(text or "")
    name_match = _BOT_NAME_PATTERN.search(raw)
    chat_match = _CHAT_ID_PATTERN.search(raw)
    manager_match = _MANAGER_PATTERN.search(raw)
    keywords_match = _KEYWORDS_PATTERN.search(raw)
    handler_type_match = _HANDLER_TYPE_PATTERN.search(raw)
    handler_enabled_match = _HANDLER_ENABLED_PATTERN.search(raw)
    parsed_handler_type = parse_handler_type(str(handler_type_match.group(1)).strip()) if handler_type_match else ""
    parsed_handler_enabled: Any = None
    if handler_enabled_match:
        parsed_handler_enabled = _to_bool(str(handler_enabled_match.group(1)).strip(), True)
    return {
        "bot_name": str(name_match.group(1)).strip() if name_match else "",
        "chat_id": str(chat_match.group(1)).strip() if chat_match else "",
        "is_manager": _to_bool(str(manager_match.group(1)).strip(), False) if manager_match else False,
        "keywords": _split_keywords(str(keywords_match.group(1)).strip()) if keywords_match else [],
        "handler_type": parsed_handler_type,
        "handler_enabled": parsed_handler_enabled,
    }


def _nearest_username(text: str, anchor: int, max_distance: int = 1800) -> str:
    best = ""
    best_distance = max_distance + 1
    for match in _USERNAME_PATTERN.finditer(text):
        distance = abs(match.start() - int(anchor))
        if distance <= max_distance and distance < best_distance:
            best = _normalize_username(match.group(1))
            best_distance = distance
    return best


def parse_botfather_entries(text: str) -> List[Dict[str, Any]]:
    raw = str(text or "")
    token_matches = list(_TOKEN_PATTERN.finditer(raw))
    if not token_matches:
        return []

    global_fields = _extract_fields(raw)
    entries: List[Dict[str, Any]] = []
    for idx, match in enumerate(token_matches):
        token = str(match.group(0)).strip()
        token_id = token.split(":", 1)[0]
        left_limit = token_matches[idx - 1].end() if idx > 0 else 0
        right_limit = token_matches[idx + 1].start() if (idx + 1) < len(token_matches) else len(raw)

        marker = "Done! Congratulations on your new bot."
        left = left_limit
        marker_left = raw.rfind(marker, left_limit, match.start())
        if marker_left >= 0:
            left = marker_left

        right = right_limit
        marker_right = raw.find(marker, match.end(), right_limit)
        if marker_right >= 0:
            right = marker_right

        snippet = raw[left:right]
        local_bootstrap = extract_botfather_bootstrap(snippet)
        local_fields = _extract_fields(snippet)

        bot_username = _normalize_username(local_bootstrap.get("bot_username") or "")
        if not bot_username:
            bot_username = _nearest_username(raw, match.start())
        if not bot_username:
            bot_username = f"bot_{token_id}"

        use_global = len(token_matches) == 1
        bot_name = str(local_fields.get("bot_name") or "").strip()
        if not bot_name and use_global:
            bot_name = str(global_fields.get("bot_name") or "").strip()
        if not bot_name:
            bot_name = bot_username

        chat_id = str(local_fields.get("chat_id") or "").strip()
        if not chat_id and use_global:
            chat_id = str(global_fields.get("chat_id") or "").strip()

        is_manager = bool(local_fields.get("is_manager"))
        if use_global and (not is_manager):
            is_manager = bool(global_fields.get("is_manager"))

        handler_type = parse_handler_type(local_fields.get("handler_type"))
        if (not handler_type) and use_global:
            handler_type = parse_handler_type(global_fields.get("handler_type"))
        handler_type = normalize_handler_type(handler_type, is_manager=is_manager)

        handler_enabled_value = local_fields.get("handler_enabled")
        if (handler_enabled_value is None) and use_global:
            handler_enabled_value = global_fields.get("handler_enabled")
        handler_enabled = _to_bool(handler_enabled_value, default=True)

        keywords = _split_keywords(local_fields.get("keywords"))
        if (not keywords) and use_global:
            keywords = _split_keywords(global_fields.get("keywords"))
        if not keywords:
            keywords = _default_keywords(bot_name, bot_username, is_manager)
        else:
            keywords = _merge_keywords(keywords, _default_keywords(bot_name, bot_username, is_manager))

        config_text = snippet.strip() if len(token_matches) > 1 else raw.strip()
        entry = {
            "bot_name": bot_name,
            "bot_username": bot_username,
            "telegram_unique_id": bot_username,
            "bot_token": token,
            "chat_id": chat_id,
            "keywords": keywords,
            "is_manager": is_manager,
            "handler_type": handler_type,
            "handler_enabled": handler_enabled,
            "enabled": True,
            "config_text": config_text,
            "_order": idx,
        }
        entries.append(entry)

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in entries:
        key = _normalize_username(row.get("bot_username") or "")
        if not key:
            continue
        deduped[key] = row
    result = list(deduped.values())
    result.sort(key=lambda x: int(x.get("_order") or 0))
    for row in result:
        row.pop("_order", None)
    return result


def upsert_bot(registry: Dict[str, Any], raw_entry: Mapping[str, Any]) -> Dict[str, Any]:
    bots = registry.get("bots")
    if not isinstance(bots, list):
        bots = []
        registry["bots"] = bots

    incoming = _normalize_bot_record(raw_entry)
    key = _normalize_username(incoming.get("bot_username") or "")
    if not key and incoming.get("bot_token"):
        key = f"bot_{str(incoming['bot_token']).split(':', 1)[0]}"
        incoming["bot_username"] = key
        incoming["telegram_unique_id"] = key
        if not incoming.get("bot_name"):
            incoming["bot_name"] = key

    now = _now_text()
    matched_index = -1
    for idx, row in enumerate(bots):
        if not isinstance(row, dict):
            continue
        existing_key = _normalize_username(row.get("bot_username") or "")
        if existing_key and key and existing_key == key:
            matched_index = idx
            break
        if row.get("bot_token") and incoming.get("bot_token") and row.get("bot_token") == incoming.get("bot_token"):
            matched_index = idx
            break

    action = "created"
    if matched_index >= 0:
        action = "updated"
        existing = _normalize_bot_record(bots[matched_index])
        merged = dict(existing)
        raw_handler_type = parse_handler_type(raw_entry.get("handler_type") if isinstance(raw_entry, Mapping) else "")
        raw_handler_enabled = None
        raw_command_profile = ""
        if isinstance(raw_entry, Mapping) and ("handler_enabled" in raw_entry):
            raw_handler_enabled = _to_bool(raw_entry.get("handler_enabled"), default=True)
        if isinstance(raw_entry, Mapping) and ("command_profile" in raw_entry):
            raw_command_profile = normalize_command_profile(raw_entry.get("command_profile"))
        for field in ("bot_name", "bot_username", "telegram_unique_id", "bot_token", "chat_id", "config_text"):
            value = str(incoming.get(field) or "").strip()
            if value:
                merged[field] = value
        merged["enabled"] = bool(incoming.get("enabled", existing.get("enabled", True)))
        merged["is_manager"] = bool(existing.get("is_manager")) or bool(incoming.get("is_manager"))
        if raw_handler_type:
            merged["handler_type"] = normalize_handler_type(raw_handler_type, is_manager=bool(merged.get("is_manager")))
        else:
            merged["handler_type"] = normalize_handler_type(
                merged.get("handler_type"),
                is_manager=bool(merged.get("is_manager")),
            )
        if raw_handler_enabled is not None:
            merged["handler_enabled"] = bool(raw_handler_enabled)
        else:
            merged["handler_enabled"] = _to_bool(existing.get("handler_enabled"), default=True)
        if raw_command_profile:
            merged["command_profile"] = raw_command_profile
        else:
            merged["command_profile"] = normalize_command_profile(existing.get("command_profile"))
        incoming_keywords = _split_keywords(incoming.get("keywords"))
        base_keywords = incoming_keywords if incoming_keywords else _split_keywords(merged.get("keywords"))
        merged["keywords"] = _merge_keywords(
            base_keywords,
            _default_keywords(
                str(merged.get("bot_name") or ""),
                str(merged.get("bot_username") or ""),
                bool(merged.get("is_manager")),
            ),
        )
        merged["created_at"] = existing.get("created_at") or now
        merged["updated_at"] = now
        bots[matched_index] = merged
        payload = merged
    else:
        incoming["handler_type"] = normalize_handler_type(
            incoming.get("handler_type"),
            is_manager=bool(incoming.get("is_manager")),
        )
        incoming["handler_enabled"] = _to_bool(incoming.get("handler_enabled"), default=True)
        incoming["command_profile"] = normalize_command_profile(incoming.get("command_profile"))
        incoming["keywords"] = _merge_keywords(
            incoming.get("keywords"),
            _default_keywords(
                str(incoming.get("bot_name") or ""),
                str(incoming.get("bot_username") or ""),
                bool(incoming.get("is_manager")),
            ),
        )
        incoming["created_at"] = now
        incoming["updated_at"] = now
        bots.append(incoming)
        payload = incoming

    if (not registry.get("manager_bot_username")) and bots:
        first_username = _normalize_username(bots[0].get("bot_username") or "")
        if first_username:
            registry["manager_bot_username"] = first_username
            bots[0]["is_manager"] = True
            bots[0]["handler_type"] = HANDLER_TYPE_MANAGER

    if payload.get("is_manager"):
        set_manager_bot(registry, str(payload.get("bot_username") or ""))

    registry["updated_at"] = now
    return {"action": action, "bot": payload}


def list_bots(registry: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows = registry.get("bots")
    if not isinstance(rows, list):
        return []
    output: List[Dict[str, Any]] = []
    manager_username = _normalize_username(registry.get("manager_bot_username") or "")
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        username = _normalize_username(row.get("bot_username") or "")
        is_manager = bool(row.get("is_manager")) or (username == manager_username)
        keywords = _merge_keywords(
            row.get("keywords"),
            _default_keywords(str(row.get("bot_name") or ""), username, is_manager),
        )
        output.append(
            {
                "bot_name": str(row.get("bot_name") or "").strip(),
                "bot_username": username,
                "telegram_unique_id": username,
                "chat_id": str(row.get("chat_id") or "").strip(),
                "keywords": keywords,
                "bot_token_masked": _mask_token(str(row.get("bot_token") or "").strip()),
                "is_manager": is_manager,
                "handler_type": normalize_handler_type(row.get("handler_type"), is_manager=is_manager),
                "handler_enabled": _to_bool(row.get("handler_enabled"), default=True),
                "command_profile": normalize_command_profile(row.get("command_profile")),
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
        )
    return output


def bootstrap_registry_from_text(
    *,
    text: str,
    registry_file: Path | str = DEFAULT_REGISTRY_FILE,
    manager_first: bool = True,
    auto_resolve_chat_id: bool = False,
) -> Dict[str, Any]:
    registry = load_registry(registry_file)
    entries = parse_botfather_entries(text)
    results: List[Dict[str, Any]] = []
    for idx, entry in enumerate(entries):
        if manager_first and idx == 0 and (not registry.get("manager_bot_username")):
            entry["is_manager"] = True
        if auto_resolve_chat_id and (not str(entry.get("chat_id") or "").strip()):
            try:
                probe = find_chat_candidates(bot_token=str(entry.get("bot_token") or ""))
            except Exception:
                probe = {"ok": False, "chat_candidates": []}
            if bool(probe.get("ok")) and isinstance(probe.get("chat_candidates"), list) and probe["chat_candidates"]:
                candidate = probe["chat_candidates"][0]
                if isinstance(candidate, Mapping):
                    chat_id = str(candidate.get("chat_id") or "").strip()
                    if chat_id:
                        entry["chat_id"] = chat_id
        results.append(upsert_bot(registry, entry))
    save_registry(registry, registry_file)
    return {"ok": True, "count": len(results), "results": results, "registry": registry}


def _read_text(file_path: str, inline_text: str) -> str:
    if str(file_path or "").strip():
        return Path(file_path).read_text(encoding="utf-8")
    return str(inline_text or "")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Telegram bot registry utility.")
    parser.add_argument("--registry-file", default=str(DEFAULT_REGISTRY_FILE))
    parser.add_argument("--import-text", default="", help="Raw BotFather text (supports multiple blocks).")
    parser.add_argument("--import-file", default="", help="Path to raw BotFather text file.")
    parser.add_argument("--manager-first", action="store_true", help="Mark first imported bot as manager.")
    parser.add_argument("--auto-resolve-chat-id", action="store_true")
    parser.add_argument("--list", action="store_true", help="Print registry bot list.")
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def main() -> int:
    load_dotenv_if_available()
    args = _build_parser().parse_args()
    registry_file = Path(str(args.registry_file or DEFAULT_REGISTRY_FILE))

    if bool(args.import_text) or bool(args.import_file):
        payload = _read_text(str(args.import_file or ""), str(args.import_text or ""))
        result = bootstrap_registry_from_text(
            text=payload,
            registry_file=registry_file,
            manager_first=bool(args.manager_first),
            auto_resolve_chat_id=bool(args.auto_resolve_chat_id),
        )
        if bool(args.json):
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"[Registry] Imported {result.get('count') or 0} bot config(s).")
            rows = list_bots(result.get("registry") if isinstance(result.get("registry"), Mapping) else {})
            for item in rows:
                manager_flag = "M" if bool(item.get("is_manager")) else "-"
                print(
                    f"  [{manager_flag}] @{item.get('bot_username') or '-'} "
                    f"name={item.get('bot_name') or '-'} chat_id={item.get('chat_id') or '-'} "
                    f"keywords={','.join(item.get('keywords') or []) or '-'} "
                    f"handler={item.get('handler_type') or '-'} enabled={bool(item.get('handler_enabled'))} "
                    f"token={item.get('bot_token_masked') or '-'}"
                )
        return 0

    registry = load_registry(registry_file)
    if bool(args.list):
        rows = list_bots(registry)
        if bool(args.json):
            print(json.dumps(rows, ensure_ascii=False, indent=2))
        else:
            print(f"[Registry] file={registry_file}")
            print(f"[Registry] manager={registry.get('manager_bot_username') or '-'}")
            for item in rows:
                manager_flag = "M" if bool(item.get("is_manager")) else "-"
                print(
                    f"  [{manager_flag}] @{item.get('bot_username') or '-'} "
                    f"name={item.get('bot_name') or '-'} chat_id={item.get('chat_id') or '-'} "
                    f"keywords={','.join(item.get('keywords') or []) or '-'} "
                    f"handler={item.get('handler_type') or '-'} enabled={bool(item.get('handler_enabled'))} "
                    f"token={item.get('bot_token_masked') or '-'}"
                )
        return 0

    print("[Registry] Nothing to do. Use --list or --import-*.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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
_KEYWORDS_PATTERN = re.compile(r"(?im)^\s*(?:keywords|keyword|keys)\s*[:=]\s*(.+?)\s*$")
_AUTO_ALIAS_SPLIT_PATTERN = re.compile(r"[_\-\s]+")
_AUTO_ALIAS_SKIP_TOKENS = {"cui", "bot", "telegram", "robot", "notify"}
_AUTO_ALIAS_BLOCKLIST = {"cybercar"}
_AUTO_ALIAS_MIN_LENGTH = 3


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
        merged.extend(_split_keywords(item))
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


def _default_keywords(bot_name: str, bot_username: str) -> List[str]:
    values: List[str] = []
    for item in (str(bot_name or "").strip(), str(bot_username or "").strip()):
        if item:
            values.append(item)
    primary = _primary_alias_token(str(bot_username or ""), str(bot_name or ""))
    if primary:
        values.append(primary)
    return _merge_keywords(values)


def default_registry() -> Dict[str, Any]:
    return {
        "version": 1,
        "updated_at": _now_text(),
        "bots": [],
    }


def _normalize_bot_record(raw: Mapping[str, Any]) -> Dict[str, Any]:
    username = _normalize_username(raw.get("bot_username") or raw.get("telegram_unique_id") or "")
    name = str(raw.get("bot_name") or "").strip() or username
    token = str(raw.get("bot_token") or "").strip()
    chat_id = str(raw.get("chat_id") or "").strip()
    return {
        "bot_name": name,
        "bot_username": username,
        "telegram_unique_id": username,
        "bot_token": token,
        "chat_id": chat_id,
        "keywords": _merge_keywords(raw.get("keywords"), _default_keywords(name, username)),
        "enabled": _to_bool(raw.get("enabled"), default=True),
        "config_text": str(raw.get("config_text") or "").strip(),
        "created_at": str(raw.get("created_at") or "").strip() or _now_text(),
        "updated_at": str(raw.get("updated_at") or "").strip() or _now_text(),
    }


def _normalize_registry(raw: Mapping[str, Any]) -> Dict[str, Any]:
    data = default_registry()
    data["version"] = int(raw.get("version") or 1)
    data["updated_at"] = str(raw.get("updated_at") or "").strip() or _now_text()
    bots = raw.get("bots")
    rows: List[Dict[str, Any]] = []
    if isinstance(bots, list):
        for item in bots:
            if isinstance(item, Mapping):
                row = _normalize_bot_record(item)
                if row["bot_username"] or row["bot_token"]:
                    rows.append(row)
    data["bots"] = rows
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


def list_bots(registry: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows = registry.get("bots")
    if not isinstance(rows, list):
        return []
    output: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        username = _normalize_username(row.get("bot_username") or "")
        output.append(
            {
                "bot_name": str(row.get("bot_name") or "").strip(),
                "bot_username": username,
                "telegram_unique_id": username,
                "chat_id": str(row.get("chat_id") or "").strip(),
                "keywords": _merge_keywords(row.get("keywords"), _default_keywords(str(row.get("bot_name") or ""), username)),
                "bot_token_masked": _mask_token(str(row.get("bot_token") or "").strip()),
                "enabled": _to_bool(row.get("enabled"), default=True),
                "updated_at": str(row.get("updated_at") or "").strip(),
            }
        )
    return output


def _extract_fields(text: str) -> Dict[str, Any]:
    raw = str(text or "")
    name_match = _BOT_NAME_PATTERN.search(raw)
    chat_match = _CHAT_ID_PATTERN.search(raw)
    keywords_match = _KEYWORDS_PATTERN.search(raw)
    return {
        "bot_name": str(name_match.group(1)).strip() if name_match else "",
        "chat_id": str(chat_match.group(1)).strip() if chat_match else "",
        "keywords": _split_keywords(str(keywords_match.group(1)).strip()) if keywords_match else [],
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

        keywords = _split_keywords(local_fields.get("keywords"))
        if (not keywords) and use_global:
            keywords = _split_keywords(global_fields.get("keywords"))
        keywords = _merge_keywords(keywords, _default_keywords(bot_name, bot_username))

        config_text = snippet.strip() if len(token_matches) > 1 else raw.strip()
        entries.append(
            {
                "bot_name": bot_name,
                "bot_username": bot_username,
                "telegram_unique_id": bot_username,
                "bot_token": token,
                "chat_id": chat_id,
                "keywords": keywords,
                "enabled": True,
                "config_text": config_text,
                "_order": idx,
            }
        )

    deduped: Dict[str, Dict[str, Any]] = {}
    for row in entries:
        key = _normalize_username(row.get("bot_username") or "")
        if key:
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
        for field in ("bot_name", "bot_username", "telegram_unique_id", "bot_token", "chat_id", "config_text"):
            value = str(incoming.get(field) or "").strip()
            if value:
                merged[field] = value
        merged["enabled"] = bool(incoming.get("enabled", existing.get("enabled", True)))
        incoming_keywords = _split_keywords(incoming.get("keywords"))
        base_keywords = incoming_keywords if incoming_keywords else _split_keywords(merged.get("keywords"))
        merged["keywords"] = _merge_keywords(
            base_keywords,
            _default_keywords(str(merged.get("bot_name") or ""), str(merged.get("bot_username") or "")),
        )
        merged["created_at"] = existing.get("created_at") or now
        merged["updated_at"] = now
        bots[matched_index] = merged
        payload = merged
    else:
        incoming["created_at"] = now
        incoming["updated_at"] = now
        bots.append(incoming)
        payload = incoming

    registry["updated_at"] = now
    return {"action": action, "bot": payload}


def bootstrap_registry_from_text(
    *,
    text: str,
    registry_file: Path | str = DEFAULT_REGISTRY_FILE,
    auto_resolve_chat_id: bool = False,
) -> Dict[str, Any]:
    registry = load_registry(registry_file)
    entries = parse_botfather_entries(text)
    results: List[Dict[str, Any]] = []
    for entry in entries:
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
    parser.add_argument("--import-text", default="", help="Raw BotFather text.")
    parser.add_argument("--import-file", default="", help="Path to raw BotFather text file.")
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
            auto_resolve_chat_id=bool(args.auto_resolve_chat_id),
        )
        if bool(args.json):
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"[Registry] Imported {result.get('count') or 0} bot config(s).")
            rows = list_bots(result.get("registry") if isinstance(result.get("registry"), Mapping) else {})
            for item in rows:
                print(
                    f"  @{item.get('bot_username') or '-'} "
                    f"name={item.get('bot_name') or '-'} chat_id={item.get('chat_id') or '-'} "
                    f"keywords={','.join(item.get('keywords') or []) or '-'} "
                    f"enabled={bool(item.get('enabled'))} token={item.get('bot_token_masked') or '-'}"
                )
        return 0

    registry = load_registry(registry_file)
    if bool(args.list):
        rows = list_bots(registry)
        if bool(args.json):
            print(json.dumps(rows, ensure_ascii=False, indent=2))
        else:
            print(f"[Registry] file={registry_file}")
            for item in rows:
                print(
                    f"  @{item.get('bot_username') or '-'} "
                    f"name={item.get('bot_name') or '-'} chat_id={item.get('chat_id') or '-'} "
                    f"keywords={','.join(item.get('keywords') or []) or '-'} "
                    f"enabled={bool(item.get('enabled'))} token={item.get('bot_token_masked') or '-'}"
                )
        return 0

    print("[Registry] Nothing to do. Use --list or --import-*.") 
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

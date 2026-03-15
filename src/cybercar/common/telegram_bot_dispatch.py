from __future__ import annotations

import argparse
import re
from typing import Any, Dict, Mapping

import requests

from .telegram_bot_registry import DEFAULT_REGISTRY_FILE, load_registry

DEFAULT_TELEGRAM_API_BASE = "https://api.telegram.org"


def _split_tokens(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = [x.strip().lower() for x in re.split(r"[,\s;/|]+", text) if x.strip()]
    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        deduped.append(part)
    return deduped


def _score_bot(keyword_tokens: list[str], row: Mapping[str, Any]) -> int:
    username = str(row.get("bot_username") or "").strip().lower()
    name = str(row.get("bot_name") or "").strip().lower()
    keywords = [str(x or "").strip().lower() for x in (row.get("keywords") if isinstance(row.get("keywords"), list) else [])]

    score = 0
    for token in keyword_tokens:
        if not token:
            continue
        if token in keywords:
            score += 100
        if token == username or token == name:
            score += 90
        if token in username:
            score += 30
        if token in name:
            score += 20
        for kw in keywords:
            if token and (token in kw):
                score += 15
    return score


def resolve_bot_by_keyword(
    *,
    keyword: str,
    registry_file: str = str(DEFAULT_REGISTRY_FILE),
) -> Dict[str, Any]:
    registry = load_registry(registry_file)
    rows = registry.get("bots")
    if not isinstance(rows, list):
        rows = []

    key_tokens = _split_tokens(keyword)
    if not key_tokens:
        return {"ok": False, "error": "empty keyword", "bot": {}}

    best: dict[str, Any] = {}
    best_score = -1
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        if not bool(item.get("enabled", True)):
            continue
        score = _score_bot(key_tokens, item)
        if score > best_score:
            best = dict(item)
            best_score = score

    if best_score <= 0:
        return {"ok": False, "error": "no bot matched keyword", "bot": {}}

    return {
        "ok": True,
        "error": "",
        "score": best_score,
        "bot": {
            "bot_name": str(best.get("bot_name") or "").strip(),
            "bot_username": str(best.get("bot_username") or "").strip(),
            "bot_token": str(best.get("bot_token") or "").strip(),
            "chat_id": str(best.get("chat_id") or "").strip(),
            "keywords": list(best.get("keywords") or []),
        },
    }


def send_text_by_keyword(
    *,
    keyword: str,
    text: str,
    subject: str = "",
    registry_file: str = str(DEFAULT_REGISTRY_FILE),
    api_base: str = DEFAULT_TELEGRAM_API_BASE,
    timeout_seconds: int = 20,
) -> Dict[str, Any]:
    content = "\n".join(x for x in [str(subject or "").strip(), str(text or "").strip()] if x).strip()
    if not content:
        return {"ok": False, "error": "empty message", "provider": "telegram_keyword_dispatch"}

    resolved = resolve_bot_by_keyword(keyword=keyword, registry_file=registry_file)
    if not bool(resolved.get("ok")):
        return {
            "ok": False,
            "error": str(resolved.get("error") or "resolve failed"),
            "provider": "telegram_keyword_dispatch",
            "keyword": keyword,
        }

    bot = resolved.get("bot") if isinstance(resolved.get("bot"), Mapping) else {}
    token = str(bot.get("bot_token") or "").strip()
    chat_id = str(bot.get("chat_id") or "").strip()
    username = str(bot.get("bot_username") or "").strip()
    if not token:
        return {"ok": False, "error": f"bot @{username or '-'} missing token", "provider": "telegram_keyword_dispatch"}
    if not chat_id:
        return {"ok": False, "error": f"bot @{username or '-'} missing chat_id", "provider": "telegram_keyword_dispatch"}

    endpoint = f"{str(api_base or DEFAULT_TELEGRAM_API_BASE).rstrip('/')}/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": content}
    try:
        resp = requests.post(endpoint, json=payload, timeout=max(5, int(timeout_seconds)))
        body_text = (resp.text or "").strip()
        parsed = resp.json() if body_text else {}
        if not (200 <= resp.status_code < 300):
            return {
                "ok": False,
                "error": body_text[:500] or f"http {resp.status_code}",
                "provider": "telegram_keyword_dispatch",
                "bot_username": username,
                "status_code": resp.status_code,
            }
        ok_flag = bool(parsed.get("ok")) if isinstance(parsed, Mapping) else False
        if not ok_flag:
            return {
                "ok": False,
                "error": str(parsed.get("description") or "telegram send failed") if isinstance(parsed, Mapping) else "telegram send failed",
                "provider": "telegram_keyword_dispatch",
                "bot_username": username,
            }
        return {
            "ok": True,
            "error": "",
            "provider": "telegram_keyword_dispatch",
            "keyword": keyword,
            "bot_username": username,
            "chat_id": chat_id,
            "score": resolved.get("score"),
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "provider": "telegram_keyword_dispatch",
            "bot_username": username,
        }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dispatch telegram messages by keyword.")
    parser.add_argument("--keyword", required=True, help="Keyword to select bot.")
    parser.add_argument("--text", default="", help="Message text body.")
    parser.add_argument("--subject", default="", help="Optional title line.")
    parser.add_argument("--registry-file", default=str(DEFAULT_REGISTRY_FILE))
    parser.add_argument("--api-base", default=DEFAULT_TELEGRAM_API_BASE)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--resolve-only", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    if bool(args.resolve_only):
        result = resolve_bot_by_keyword(keyword=str(args.keyword or ""), registry_file=str(args.registry_file or ""))
    else:
        result = send_text_by_keyword(
            keyword=str(args.keyword or ""),
            subject=str(args.subject or ""),
            text=str(args.text or ""),
            registry_file=str(args.registry_file or ""),
            api_base=str(args.api_base or DEFAULT_TELEGRAM_API_BASE),
            timeout_seconds=max(5, int(args.timeout_seconds)),
        )

    if bool(args.json):
        import json

        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if bool(result.get("ok")):
            print(
                f"[Dispatch] ok keyword={args.keyword} bot=@{result.get('bot_username') or '-'} "
                f"chat_id={result.get('chat_id') or '-'}"
            )
        else:
            print(f"[Dispatch] failed: {result.get('error') or 'unknown'}")
    return 0 if bool(result.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())

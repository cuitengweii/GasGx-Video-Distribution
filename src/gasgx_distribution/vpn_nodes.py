from __future__ import annotations

import base64
import copy
import json
import re
import warnings
from pathlib import Path
from typing import Any

import requests

from .paths import get_paths

_COUNTRY_PATTERNS: list[tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"(香港|HKG|\bHK\b|Hong\s*Kong)", re.IGNORECASE), "HK", "香港"),
    (re.compile(r"(新加坡|SGP|\bSG\b|Singapore)", re.IGNORECASE), "SG", "新加坡"),
    (re.compile(r"(日本|JPN|\bJP\b|Japan)", re.IGNORECASE), "JP", "日本"),
    (re.compile(r"(台湾|TWN|\bTW\b|Taiwan)", re.IGNORECASE), "TW", "台湾"),
    (re.compile(r"(韩国|KOR|\bKR\b|Korea)", re.IGNORECASE), "KR", "韩国"),
    (re.compile(r"(越南|VNM|\bVN\b|Vietnam)", re.IGNORECASE), "VN", "越南"),
    (re.compile(r"(印度|IND|\bIN\b|India)", re.IGNORECASE), "IN", "印度"),
    (re.compile(r"(俄罗斯|RUS|\bRU\b|Russia)", re.IGNORECASE), "RU", "俄罗斯"),
    (re.compile(r"(英国|GBR|\bUK\b|\bGB\b|United\s+Kingdom|Britain)", re.IGNORECASE), "GB", "英国"),
    (re.compile(r"(荷兰|NLD|\bNL\b|Netherlands)", re.IGNORECASE), "NL", "荷兰"),
    (re.compile(r"(德国|DEU|\bDE\b|Germany)", re.IGNORECASE), "DE", "德国"),
    (re.compile(r"(法国|FRA|\bFR\b|France)", re.IGNORECASE), "FR", "法国"),
    (re.compile(r"(加拿大|CAN|\bCA\b|Canada)", re.IGNORECASE), "CA", "加拿大"),
    (re.compile(r"(美国|USA|\bUS\b|United\s+States|America)", re.IGNORECASE), "US", "美国"),
]


def vpn_cache_path() -> Path:
    path = get_paths().runtime_root / "vpn_nodes.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _now_ts() -> int:
    import time

    return int(time.time())


def _default_cache() -> dict[str, Any]:
    return {
        "subscription_url": "",
        "fetched_at": 0,
        "refreshed_at": 0,
        "node_count": 0,
        "countries": {},
        "nodes": [],
    }


def _countries_summary(nodes: list[dict[str, Any]]) -> dict[str, int]:
    countries: dict[str, int] = {}
    for item in nodes:
        label = str(item.get("country_label") or "鏈煡").strip() or "鏈煡"
        countries[label] = countries.get(label, 0) + 1
    return countries


def _normalize_country(name: str, server: str = "") -> tuple[str, str]:
    text = f"{name} {server}".strip()
    for pattern, code, label in _COUNTRY_PATTERNS:
        if pattern.search(text):
            return code, label
    return "", "未知"


def _slugify(value: str) -> str:
    text = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "-", str(value or "").strip())
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text.lower() or "node"


def _normalize_proxy_url(value: str, scheme_hint: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "://" in text:
        return text
    if scheme_hint in {"http", "https", "socks5", "socks4", "socks"}:
        return f"{scheme_hint}://{text}"
    return ""


def _decode_base64_text(value: str) -> str:
    raw = re.sub(r"\s+", "", str(value or "").strip())
    if not raw:
        return ""
    padded = raw + "=" * (-len(raw) % 4)
    try:
        decoded = base64.b64decode(padded, validate=False)
        return decoded.decode("utf-8", errors="replace").strip()
    except Exception:
        return ""


def _parse_vmess_uri(uri: str) -> dict[str, Any]:
    payload = _decode_base64_text(uri.split("://", 1)[1] if "://" in uri else uri)
    data: dict[str, Any] = {}
    try:
        parsed = json.loads(payload) if payload else {}
        if isinstance(parsed, dict):
            data = parsed
    except Exception:
        data = {}
    name = str(data.get("ps") or data.get("name") or "").strip()
    server = str(data.get("add") or data.get("server") or data.get("host") or "").strip()
    port = str(data.get("port") or "").strip()
    country_code, country_label = _normalize_country(name, server)
    return {
        "scheme": "vmess",
        "name": name,
        "display_name": name or server or "vmess-node",
        "server": server,
        "port": port,
        "country_code": country_code,
        "country_label": country_label,
        "proxy_url": "",
        "raw": uri,
        "raw_payload": data,
        "node_key": _slugify(f"vmess-{country_code or 'unknown'}-{server}-{port}-{name}"),
    }


def _parse_standard_uri(uri: str) -> dict[str, Any]:
    scheme, rest = uri.split("://", 1)
    scheme = scheme.strip().lower()
    name = ""
    if "#" in rest:
        rest, fragment = rest.split("#", 1)
        name = requests.utils.unquote(fragment).strip()
    server = ""
    port = ""
    proxy_url = ""
    try:
        from urllib.parse import urlsplit

        parsed = urlsplit(f"{scheme}://{rest}")
        server = parsed.hostname or ""
        port = str(parsed.port or "")
        proxy_url = _normalize_proxy_url(f"{server}:{port}" if server and port else rest, scheme_hint=scheme)
    except Exception:
        proxy_url = _normalize_proxy_url(rest, scheme_hint=scheme)
    country_code, country_label = _normalize_country(name, server)
    display_name = name or server or scheme
    return {
        "scheme": scheme,
        "name": name,
        "display_name": display_name,
        "server": server,
        "port": port,
        "country_code": country_code,
        "country_label": country_label,
        "proxy_url": proxy_url,
        "raw": uri,
        "raw_payload": {},
        "node_key": _slugify(f"{scheme}-{country_code or 'unknown'}-{server}-{port}-{name}"),
    }


def _parse_node_line(line: str) -> dict[str, Any] | None:
    text = str(line or "").strip()
    if not text or text.startswith("●") or text.startswith("#"):
        return None
    if "://" not in text:
        decoded = _decode_base64_text(text)
        if decoded and "://" in decoded:
            text = decoded.strip()
        else:
            return None
    scheme = text.split("://", 1)[0].strip().lower()
    if scheme == "vmess":
        return _parse_vmess_uri(text)
    if scheme in {"http", "https", "socks", "socks4", "socks5", "trojan", "ss", "vless"}:
        return _parse_standard_uri(text)
    return None


def parse_subscription_text(text: str) -> list[dict[str, Any]]:
    decoded = _decode_base64_text(text)
    payload = decoded if decoded and any(token in decoded for token in ("://", "vmess://")) else str(text or "").strip()
    nodes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_line in payload.splitlines():
        node = _parse_node_line(raw_line)
        if not node:
            continue
        key = str(node.get("node_key") or "")
        if key in seen:
            continue
        seen.add(key)
        nodes.append(node)
    return nodes


def load_vpn_node_cache() -> dict[str, Any]:
    path = vpn_cache_path()
    if not path.exists():
        return _default_cache()
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return _default_cache()
    if not isinstance(payload, dict):
        return _default_cache()
    cache = _default_cache()
    cache.update({key: payload.get(key, cache[key]) for key in cache})
    nodes = payload.get("nodes") if isinstance(payload.get("nodes"), list) else []
    normalized_nodes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in nodes:
        if not isinstance(item, dict):
            continue
        node = dict(item)
        node["scheme"] = str(node.get("scheme") or "").strip().lower()
        node["name"] = str(node.get("name") or "").strip()
        node["display_name"] = str(node.get("display_name") or node["name"] or node.get("server") or node["scheme"] or "node").strip()
        node["server"] = str(node.get("server") or "").strip()
        node["port"] = str(node.get("port") or "").strip()
        node["country_code"] = str(node.get("country_code") or "").strip().upper()
        country_label = str(node.get("country_label") or "").strip()
        if not country_label:
            country_label = "未知"
        node["country_label"] = country_label
        node["proxy_url"] = str(node.get("proxy_url") or "").strip()
        node["raw"] = str(node.get("raw") or "").strip()
        node["node_key"] = str(node.get("node_key") or _slugify(f"{node['scheme']}-{node['server']}-{node['port']}-{node['display_name']}")).strip()
        node["node_key"] = node["node_key"] or _slugify(node["display_name"])
        if node["node_key"] in seen:
            continue
        seen.add(node["node_key"])
        normalized_nodes.append(node)
    cache["nodes"] = normalized_nodes
    cache["node_count"] = len(normalized_nodes)
    cache["countries"] = _countries_summary(normalized_nodes)
    return cache


def save_vpn_node_cache(payload: dict[str, Any]) -> dict[str, Any]:
    cache = _default_cache()
    if isinstance(payload, dict):
        cache.update({key: payload.get(key, cache[key]) for key in cache})
    nodes = cache.get("nodes") if isinstance(cache.get("nodes"), list) else []
    cache["nodes"] = [item for item in nodes if isinstance(item, dict)]
    cache["node_count"] = len(cache["nodes"])
    cache["countries"] = _countries_summary(cache["nodes"])
    cache["refreshed_at"] = int(cache.get("refreshed_at") or 0)
    cache["fetched_at"] = int(cache.get("fetched_at") or 0)
    path = vpn_cache_path()
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    return cache


def refresh_vpn_node_cache(subscription_url: str, *, timeout_seconds: int = 30) -> dict[str, Any]:
    url = str(subscription_url or "").strip()
    if not url:
        raise ValueError("请先填写订阅地址")
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Accept": "*/*",
    }
    try:
        response = requests.get(url, headers=headers, timeout=max(5, int(timeout_seconds or 30)), verify=False)
        response.raise_for_status()
    except requests.RequestException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code:
            raise ValueError(f"拉取订阅失败，服务器返回状态码 {status_code}") from exc
        raise ValueError("拉取订阅失败，请检查订阅地址是否可访问") from exc
    text = response.text.strip()
    nodes = parse_subscription_text(text)
    if not nodes:
        raise ValueError("订阅内容解析失败，请检查订阅源格式")
    now = _now_ts()
    cache = {
        "subscription_url": url,
        "fetched_at": now,
        "refreshed_at": now,
        "node_count": len(nodes),
        "nodes": nodes,
    }
    return save_vpn_node_cache(cache)


def vpn_nodes_by_key(cache: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    current = cache if isinstance(cache, dict) else load_vpn_node_cache()
    nodes = current.get("nodes") if isinstance(current.get("nodes"), list) else []
    result: dict[str, dict[str, Any]] = {}
    for item in nodes:
        if not isinstance(item, dict):
            continue
        key = str(item.get("node_key") or "").strip()
        if key:
            result[key] = item
    return result


def resolve_vpn_node(node_key: str, cache: dict[str, Any] | None = None) -> dict[str, Any] | None:
    key = str(node_key or "").strip()
    if not key:
        return None
    return vpn_nodes_by_key(cache).get(key)


def vpn_node_options(cache: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    current = cache if isinstance(cache, dict) else load_vpn_node_cache()
    nodes = current.get("nodes") if isinstance(current.get("nodes"), list) else []
    options: list[dict[str, Any]] = []
    for item in nodes:
        if not isinstance(item, dict):
            continue
        options.append(
            {
                "node_key": str(item.get("node_key") or "").strip(),
                "display_name": str(item.get("display_name") or "").strip(),
                "country_code": str(item.get("country_code") or "").strip(),
                "country_label": str(item.get("country_label") or "未知").strip() or "未知",
                "server": str(item.get("server") or "").strip(),
                "port": str(item.get("port") or "").strip(),
                "scheme": str(item.get("scheme") or "").strip(),
                "proxy_url": str(item.get("proxy_url") or "").strip(),
                "raw": str(item.get("raw") or "").strip(),
            }
        )
    options.sort(key=lambda item: (item["country_label"], item["display_name"], item["server"], item["port"]))
    return options


def vpn_nodes_summary(cache: dict[str, Any] | None = None) -> dict[str, Any]:
    current = cache if isinstance(cache, dict) else load_vpn_node_cache()
    nodes = vpn_node_options(current)
    return {
        "subscription_url": str(current.get("subscription_url") or ""),
        "fetched_at": int(current.get("fetched_at") or 0),
        "refreshed_at": int(current.get("refreshed_at") or 0),
        "node_count": len(nodes),
        "countries": dict(current.get("countries") or _countries_summary(nodes)),
        "nodes": nodes,
    }

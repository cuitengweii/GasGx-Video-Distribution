from __future__ import annotations

import os
import time
from typing import Any, Dict, Mapping

import requests
from requests.adapters import HTTPAdapter

try:
    import winreg  # type: ignore
except Exception:  # pragma: no cover
    winreg = None  # type: ignore

DEFAULT_TELEGRAM_API_BASE = "https://api.telegram.org"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_GET_RETRY_COUNT = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0

_SESSIONS: dict[str, requests.Session] = {}


def _env_first(*names: str, default: str = "") -> str:
    for name in names:
        key = str(name or "").strip()
        if not key:
            continue
        value = str(os.getenv(key, "") or "").strip()
        if value:
            return value
    return str(default or "").strip()


def _parse_bool_token(raw: str, default: bool = False) -> bool:
    token = str(raw or "").strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    return default


def _normalize_proxy_url(raw: Any) -> str:
    token = str(raw or "").strip()
    if not token:
        return ""
    if ";" in token and "=" in token:
        entries: dict[str, str] = {}
        for part in token.split(";"):
            name, sep, value = part.partition("=")
            if not sep:
                continue
            key = str(name or "").strip().lower()
            normalized = _normalize_proxy_url(value)
            if normalized:
                entries[key] = normalized
        for key in ("https", "http", "socks", "socks5"):
            if entries.get(key):
                return entries[key]
        return ""
    if "://" not in token:
        return f"http://{token}"
    return token


def _detect_windows_manual_proxy() -> str:
    if os.name != "nt" or winreg is None:
        return ""
    key_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path) as key:  # type: ignore[arg-type]
            enabled = int(winreg.QueryValueEx(key, "ProxyEnable")[0] or 0) > 0
            server = str(winreg.QueryValueEx(key, "ProxyServer")[0] or "").strip()
    except Exception:
        return ""
    if not enabled or not server:
        return ""
    return _normalize_proxy_url(server)


def _resolve_session_proxy() -> str:
    explicit_proxy = _normalize_proxy_url(
        _env_first(
            "CYBERCAR_PROXY",
            "CYBERCAR_HTTPS_PROXY",
            "CYBERCAR_HTTP_PROXY",
        )
    )
    if explicit_proxy:
        return explicit_proxy
    if _parse_bool_token(_env_first("CYBERCAR_USE_SYSTEM_PROXY"), default=False):
        return _detect_windows_manual_proxy()
    return ""


def _session_key(*, use_post: bool) -> str:
    proxy_key = _resolve_session_proxy() or "direct"
    return f"{'post' if use_post else 'get'}|{proxy_key}"


def _telegram_session(*, use_post: bool) -> requests.Session:
    key = _session_key(use_post=use_post)
    session = _SESSIONS.get(key)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        proxy = _resolve_session_proxy()
        if proxy and hasattr(session, "proxies"):
            session.proxies.update({"http": proxy, "https": proxy})
        _SESSIONS[key] = session
    return session


def _reset_telegram_session(*, use_post: bool | None = None) -> None:
    keys = [_session_key(use_post=bool(use_post))] if use_post is not None else list(_SESSIONS)
    for key in keys:
        session = _SESSIONS.pop(key, None)
        if session is None:
            continue
        try:
            session.close()
        except Exception:
            pass


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _retry_sleep_seconds(attempt: int, backoff_seconds: float) -> float:
    base = max(0.05, _to_float(backoff_seconds, DEFAULT_RETRY_BACKOFF_SECONDS))
    return min(5.0, base * float(max(1, attempt + 1)))


def _is_retryable_request_error(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.SSLError,
        ),
    )


def call_telegram_api(
    *,
    bot_token: str,
    method: str,
    params: Mapping[str, Any] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    api_base: str = DEFAULT_TELEGRAM_API_BASE,
    use_post: bool = False,
    max_retries: int | None = None,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
) -> Dict[str, Any]:
    token = str(bot_token or "").strip()
    method_name = str(method or "").strip()
    if not token:
        raise RuntimeError("telegram missing bot_token")
    if not method_name:
        raise RuntimeError("telegram missing method")

    base = str(api_base or DEFAULT_TELEGRAM_API_BASE).strip() or DEFAULT_TELEGRAM_API_BASE
    endpoint = f"{base.rstrip('/')}/bot{token}/{method_name}"
    timeout = max(5, _to_int(timeout_seconds, DEFAULT_TIMEOUT_SECONDS))
    payload = dict(params or {})

    if max_retries is None:
        retry_count = 0 if use_post else DEFAULT_GET_RETRY_COUNT
    else:
        retry_count = max(0, _to_int(max_retries, 0))

    total_attempts = 1 + retry_count
    for attempt in range(total_attempts):
        session = _telegram_session(use_post=use_post)
        try:
            if use_post:
                resp = session.post(endpoint, data=payload, timeout=timeout)
            else:
                resp = session.get(endpoint, params=payload, timeout=timeout)
            break
        except requests.exceptions.RequestException as exc:
            should_retry = (attempt + 1) < total_attempts and _is_retryable_request_error(exc)
            if not should_retry:
                raise
            _reset_telegram_session(use_post=use_post)
            time.sleep(_retry_sleep_seconds(attempt, retry_backoff_seconds))

    try:
        body = resp.json() if (resp.text or "").strip() else {}
    except Exception as exc:
        raise RuntimeError(f"telegram {method_name} invalid json response: http_status={resp.status_code}") from exc
    if not isinstance(body, dict):
        raise RuntimeError(f"telegram {method_name} invalid response")
    if not bool(body.get("ok")):
        raise RuntimeError(f"telegram {method_name} failed: {body.get('description') or 'unknown'}")
    return body

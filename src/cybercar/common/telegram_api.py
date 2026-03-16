from __future__ import annotations

import time
from typing import Any, Dict, Mapping

import requests
from requests.adapters import HTTPAdapter

DEFAULT_TELEGRAM_API_BASE = "https://api.telegram.org"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_GET_RETRY_COUNT = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 1.0

_SESSION: requests.Session | None = None


def _telegram_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=8, pool_maxsize=8)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _SESSION = session
    return _SESSION


def _reset_telegram_session() -> None:
    global _SESSION
    session = _SESSION
    _SESSION = None
    if session is None:
        return
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
        session = _telegram_session()
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
            _reset_telegram_session()
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

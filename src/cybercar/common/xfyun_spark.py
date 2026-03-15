"""Reusable XFYUN Spark client and config helpers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import ssl
from contextlib import contextmanager
from datetime import datetime
from time import mktime
from typing import Any, Optional
from urllib.parse import urlencode, urlparse
from wsgiref.handlers import format_date_time

try:
    from websocket import create_connection
except Exception:  # pragma: no cover - optional runtime dependency
    create_connection = None


SPARK_CHAT_TIMEOUT_SECONDS = 20
PROXY_ENV_KEYS = (
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "all_proxy",
    "ALL_PROXY",
)


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _network_mode() -> str:
    raw = str(os.getenv("GASGX_NETWORK_MODE", os.getenv("P17_NETWORK_MODE", "auto")) or "auto").strip().lower()
    if raw in {"tun", "proxy", "direct", "auto"}:
        return raw
    return "auto"


def _spark_proxy_mode_order() -> list[bool]:
    mode = _network_mode()
    if mode in {"tun", "direct"}:
        return [False]

    if mode == "proxy":
        try_second_mode = _to_bool(
            os.getenv("XFYUN_SPARK_TRY_SECOND_MODE", os.getenv("SPARK_TRY_SECOND_MODE", "1")),
            default=True,
        )
        return [True, False] if try_second_mode else [True]

    bypass_proxy_first = _to_bool(
        os.getenv("XFYUN_SPARK_BYPASS_PROXY", os.getenv("SPARK_BYPASS_PROXY", "1")),
        default=True,
    )
    try_second_mode = _to_bool(
        os.getenv("XFYUN_SPARK_TRY_SECOND_MODE", os.getenv("SPARK_TRY_SECOND_MODE", "1")),
        default=True,
    )
    order = [False] if bypass_proxy_first else [True]
    if try_second_mode:
        order.append(not order[0])
    return order


@contextmanager
def _proxy_env(enabled: bool):
    backup = {k: os.environ.get(k) for k in PROXY_ENV_KEYS}
    try:
        if not enabled:
            for k in PROXY_ENV_KEYS:
                os.environ.pop(k, None)
        yield
    finally:
        for k, v in backup.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def default_spark_settings() -> dict[str, Any]:
    return {
        "enabled": True,
        "url": "",
        "app_id": "",
        "api_key": "",
        "api_secret": "",
        "domain": "generalv3.5",
        "temperature": 0.3,
        "max_tokens": 512,
    }


def merge_spark_settings(raw: Any) -> dict[str, Any]:
    cfg = default_spark_settings()
    if isinstance(raw, dict):
        cfg["enabled"] = _to_bool(raw.get("enabled"), default=cfg["enabled"])
        cfg["url"] = str(raw.get("url", "") or "").strip()
        cfg["app_id"] = str(raw.get("app_id", "") or "").strip()
        cfg["api_key"] = str(raw.get("api_key", "") or "").strip()
        cfg["api_secret"] = str(raw.get("api_secret", "") or "").strip()
        cfg["domain"] = str(raw.get("domain", "") or "").strip() or cfg["domain"]
        cfg["temperature"] = max(0.0, min(1.0, _to_float(raw.get("temperature"), default=cfg["temperature"])))
        cfg["max_tokens"] = max(128, min(4096, _to_int(raw.get("max_tokens"), default=cfg["max_tokens"])))

    env_map = {
        "enabled": os.getenv("XFYUN_SPARK_ENABLED", ""),
        "url": os.getenv("XFYUN_SPARK_URL", ""),
        "app_id": os.getenv("XFYUN_SPARK_APP_ID", ""),
        "api_key": os.getenv("XFYUN_SPARK_API_KEY", ""),
        "api_secret": os.getenv("XFYUN_SPARK_API_SECRET", ""),
        "domain": os.getenv("XFYUN_SPARK_DOMAIN", ""),
        "temperature": os.getenv("XFYUN_SPARK_TEMPERATURE", ""),
        "max_tokens": os.getenv("XFYUN_SPARK_MAX_TOKENS", ""),
    }
    if env_map["enabled"]:
        cfg["enabled"] = _to_bool(env_map["enabled"], default=cfg["enabled"])
    if env_map["url"]:
        cfg["url"] = env_map["url"].strip()
    if env_map["app_id"]:
        cfg["app_id"] = env_map["app_id"].strip()
    if env_map["api_key"]:
        cfg["api_key"] = env_map["api_key"].strip()
    if env_map["api_secret"]:
        cfg["api_secret"] = env_map["api_secret"].strip()
    if env_map["domain"]:
        cfg["domain"] = env_map["domain"].strip()
    if env_map["temperature"]:
        cfg["temperature"] = max(0.0, min(1.0, _to_float(env_map["temperature"], default=cfg["temperature"])))
    if env_map["max_tokens"]:
        cfg["max_tokens"] = max(128, min(4096, _to_int(env_map["max_tokens"], default=cfg["max_tokens"])))
    return cfg


def spark_settings_ready(settings: Optional[dict[str, Any]]) -> bool:
    cfg = settings or {}
    required = ("url", "app_id", "api_key", "api_secret")
    if not _to_bool(cfg.get("enabled"), default=False):
        return False
    if create_connection is None:
        return False
    return all(str(cfg.get(k, "") or "").strip() for k in required)


def extract_json_object(raw: str) -> Optional[dict[str, Any]]:
    text = (raw or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else None
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        payload = json.loads(text[start : end + 1])
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


class SparkAIClient:
    """Minimal Spark chat client for reuse across projects."""

    def __init__(self, settings: Optional[dict[str, Any]] = None, timeout_seconds: int = SPARK_CHAT_TIMEOUT_SECONDS):
        cfg = merge_spark_settings(settings)
        self.enabled = _to_bool(cfg.get("enabled"), default=False)
        self.url = str(cfg.get("url", "") or "").strip()
        self.app_id = str(cfg.get("app_id", "") or "").strip()
        self.api_key = str(cfg.get("api_key", "") or "").strip()
        self.api_secret = str(cfg.get("api_secret", "") or "").strip()
        self.domain = str(cfg.get("domain", "") or "").strip() or "generalv3.5"
        self.temperature = max(0.0, min(1.0, _to_float(cfg.get("temperature"), default=0.3)))
        self.max_tokens = max(128, min(4096, _to_int(cfg.get("max_tokens"), default=512)))
        self.timeout_seconds = max(5, int(timeout_seconds))
        parsed = urlparse(self.url)
        self.host = parsed.netloc
        self.path = parsed.path
        self.last_error = ""

    def is_ready(self) -> bool:
        if not self.enabled:
            return False
        if create_connection is None:
            return False
        return bool(self.host and self.path and self.app_id and self.api_key and self.api_secret)

    def _create_auth_url(self) -> str:
        now = datetime.now()
        date = format_date_time(mktime(now.timetuple()))
        signature_origin = f"host: {self.host}\ndate: {date}\nGET {self.path} HTTP/1.1"
        signature_sha = hmac.new(
            self.api_secret.encode("utf-8"),
            signature_origin.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).digest()
        signature_base64 = base64.b64encode(signature_sha).decode("utf-8")
        auth_origin = (
            f'api_key="{self.api_key}", algorithm="hmac-sha256", '
            f'headers="host date request-line", signature="{signature_base64}"'
        )
        authorization = base64.b64encode(auth_origin.encode("utf-8")).decode("utf-8")
        params = {"authorization": authorization, "date": date, "host": self.host}
        return f"{self.url}?{urlencode(params)}"

    def chat(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        if not self.is_ready():
            self.last_error = "spark_client_not_ready"
            return None

        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "header": {"app_id": self.app_id, "uid": "spark-shared"},
            "parameter": {
                "chat": {
                    "domain": self.domain,
                    "temperature": self.temperature,
                    "max_tokens": self.max_tokens,
                }
            },
            "payload": {"message": {"text": messages}},
        }

        self.last_error = ""
        use_env_proxy_order = _spark_proxy_mode_order()
        errors = []

        for use_env_proxy in use_env_proxy_order:
            mode = "env_proxy" if use_env_proxy else "direct"
            ws = None
            try:
                with _proxy_env(enabled=use_env_proxy):
                    ws = create_connection(  # type: ignore[misc]
                        self._create_auth_url(),
                        sslopt={"cert_reqs": ssl.CERT_NONE},
                        timeout=self.timeout_seconds,
                    )
                    ws.send(json.dumps(payload, ensure_ascii=False))

                    response = ""
                    while True:
                        raw = ws.recv()
                        if not raw:
                            break
                        data = json.loads(raw)
                        header = data.get("header") or {}
                        code = int(header.get("code", 0))
                        if code != 0:
                            msg = str(header.get("message") or "").strip()
                            errors.append(f"code={code} mode={mode} msg={msg or '-'}")
                            response = ""
                            break
                        choices = (data.get("payload") or {}).get("choices") or {}
                        text_list = choices.get("text") or []
                        if text_list:
                            response += str(text_list[0].get("content", "") or "")
                        if int(choices.get("status", 2)) == 2:
                            break

                    clean = response.strip()
                    if clean:
                        self.last_error = ""
                        return clean
                    if not errors:
                        errors.append(f"empty_response mode={mode}")
            except Exception as exc:
                errors.append(f"{type(exc).__name__} mode={mode}: {exc}")
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

        self.last_error = " | ".join(errors[-2:]) if errors else "unknown_error"
        return None


__all__ = [
    "SparkAIClient",
    "default_spark_settings",
    "merge_spark_settings",
    "spark_settings_ready",
    "extract_json_object",
]

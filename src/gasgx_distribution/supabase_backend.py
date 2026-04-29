from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - python-dotenv is a declared dependency.
    load_dotenv = None


class SupabaseError(RuntimeError):
    pass


class SupabaseRestClient:
    def __init__(self, url: str, service_role_key: str) -> None:
        self.url = url.rstrip("/")
        self.service_role_key = service_role_key
        if not self.url or not self.service_role_key:
            raise SupabaseError("supabase url and service role key are required")

    @classmethod
    def from_env(cls, *, prefix: str = "CONTROL_SUPABASE") -> "SupabaseRestClient":
        if load_dotenv is not None:
            load_dotenv()
        return cls(
            os.getenv(f"{prefix}_URL", "") or os.getenv("SUPABASE_URL", ""),
            os.getenv(f"{prefix}_SERVICE_ROLE_KEY", "") or os.getenv(f"{prefix}_SERVICE_KEY", ""),
        )

    @classmethod
    def from_instance(cls, instance: dict[str, Any]) -> "SupabaseRestClient":
        service_key_ref = str(instance.get("service_key_ref") or "")
        service_key = resolve_service_key_ref(service_key_ref)
        return cls(str(instance.get("supabase_url") or ""), service_key)

    def _headers(self, *, prefer: str = "") -> dict[str, str]:
        headers = {
            "apikey": self.service_role_key,
            "authorization": f"Bearer {self.service_role_key}",
            "content-type": "application/json",
        }
        if prefer:
            headers["prefer"] = prefer
        return headers

    def _endpoint(self, table: str) -> str:
        return f"{self.url}/rest/v1/{quote(table, safe='')}"

    def _timeout(self) -> float:
        return float(os.getenv("SUPABASE_REST_TIMEOUT", "8") or 8)

    def select(self, table: str, *, filters: dict[str, Any] | None = None, order: str = "") -> list[dict[str, Any]]:
        params: dict[str, str] = {"select": "*"}
        for key, value in (filters or {}).items():
            params[key] = f"eq.{value}"
        if order:
            params["order"] = order
        response = requests.get(self._endpoint(table), headers=self._headers(), params=params, timeout=self._timeout())
        return self._json_response(response)

    def select_where(self, table: str, *, params: dict[str, str], order: str = "") -> list[dict[str, Any]]:
        query = {"select": "*", **params}
        if order:
            query["order"] = order
        response = requests.get(self._endpoint(table), headers=self._headers(), params=query, timeout=self._timeout())
        return self._json_response(response)

    def select_one(self, table: str, *, filters: dict[str, Any]) -> dict[str, Any] | None:
        rows = self.select(table, filters=filters)
        return rows[0] if rows else None

    def insert(self, table: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = requests.post(
            self._endpoint(table),
            headers=self._headers(prefer="return=representation"),
            json=payload,
            timeout=self._timeout(),
        )
        rows = self._json_response(response)
        return rows[0] if isinstance(rows, list) and rows else {}

    def update(self, table: str, payload: dict[str, Any], *, filters: dict[str, Any]) -> dict[str, Any]:
        params = {key: f"eq.{value}" for key, value in filters.items()}
        response = requests.patch(
            self._endpoint(table),
            headers=self._headers(prefer="return=representation"),
            params=params,
            json=payload,
            timeout=self._timeout(),
        )
        rows = self._json_response(response)
        return rows[0] if isinstance(rows, list) and rows else {}

    def delete(self, table: str, *, filters: dict[str, Any]) -> bool:
        params = {key: f"eq.{value}" for key, value in filters.items()}
        response = requests.delete(
            self._endpoint(table),
            headers=self._headers(),
            params=params,
            timeout=self._timeout(),
        )
        self._json_response(response)
        return response.status_code in {200, 204}

    def rpc(self, function_name: str, payload: dict[str, Any] | None = None) -> Any:
        response = requests.post(
            f"{self.url}/rest/v1/rpc/{quote(function_name, safe='')}",
            headers=self._headers(),
            json=payload or {},
            timeout=self._timeout(),
        )
        return self._json_response(response)

    def upsert(self, table: str, payload: dict[str, Any], *, on_conflict: str) -> dict[str, Any]:
        response = requests.post(
            self._endpoint(table),
            headers=self._headers(prefer="resolution=merge-duplicates,return=representation"),
            params={"on_conflict": on_conflict},
            json=payload,
            timeout=self._timeout(),
        )
        rows = self._json_response(response)
        return rows[0] if isinstance(rows, list) and rows else {}

    @staticmethod
    def _json_response(response: requests.Response) -> Any:
        if response.status_code >= 400:
            raise SupabaseError(f"supabase request failed: {response.status_code} {response.text}")
        if not response.text:
            return []
        return response.json()


def resolve_service_key_ref(service_key_ref: str) -> str:
    value = service_key_ref.strip()
    if value.startswith("env:"):
        return os.getenv(value[4:], "")
    return value

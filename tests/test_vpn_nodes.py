from __future__ import annotations

import base64
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from gasgx_distribution import service, vpn_nodes


def _isolated_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("BRAND_DATABASE_BACKEND", "sqlite")

    class FakePaths:
        repo_root = tmp_path
        runtime_root = tmp_path / "runtime"
        profiles_root = tmp_path / "profiles" / "matrix"
        database_path = tmp_path / "runtime" / "gasgx_distribution.db"

        def ensure(self) -> None:
            self.runtime_root.mkdir(parents=True, exist_ok=True)
            self.profiles_root.mkdir(parents=True, exist_ok=True)
            self.database_path.parent.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("gasgx_distribution.vpn_nodes.get_paths", lambda: FakePaths())


def test_refresh_vpn_node_cache_parses_base64_subscription(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    nodes = [
        {"add": "hk.example.net", "port": "21966", "ps": "HKG node"},
        {"add": "ca.example.net", "port": "20101", "ps": "CAN node"},
    ]
    subscription_text = "\n".join(
        [
            "vmess://" + base64.b64encode(json.dumps(item, ensure_ascii=False).encode("utf-8")).decode("ascii")
            for item in nodes
        ]
    )
    encoded_subscription = base64.b64encode(subscription_text.encode("utf-8")).decode("ascii")

    def fake_get(url, headers=None, timeout=None, verify=None):  # noqa: ANN001
        assert url == "https://example.invalid/subscribe"
        return SimpleNamespace(text=encoded_subscription, raise_for_status=lambda: None)

    monkeypatch.setattr(vpn_nodes.requests, "get", fake_get)

    cache = vpn_nodes.refresh_vpn_node_cache("https://example.invalid/subscribe")

    assert cache["subscription_url"] == "https://example.invalid/subscribe"
    assert cache["node_count"] == 2
    assert cache["nodes"][0]["country_code"] == "HK"
    assert cache["nodes"][1]["country_code"] == "CA"
    assert vpn_nodes.resolve_vpn_node(cache["nodes"][0]["node_key"], cache)["country_code"] == "HK"
    assert (tmp_path / "runtime" / "vpn_nodes.json").exists()


def test_parse_subscription_text_skips_notes_and_deduplicates() -> None:
    text = "\n".join(
        [
            "note line to skip",
            "vmess://" + base64.b64encode(json.dumps({"add": "us-nf-1.akijp.cloud", "port": "40012", "ps": "USA | Direct Access"}).encode("utf-8")).decode("ascii"),
            "vmess://" + base64.b64encode(json.dumps({"add": "us-nf-1.akijp.cloud", "port": "40012", "ps": "USA | Direct Access"}).encode("utf-8")).decode("ascii"),
        ]
    )

    nodes = vpn_nodes.parse_subscription_text(base64.b64encode(text.encode("utf-8")).decode("ascii"))

    assert len(nodes) == 1
    assert nodes[0]["country_code"] == "US"


def test_refresh_vpn_node_cache_wraps_request_errors(monkeypatch, tmp_path: Path) -> None:
    _isolated_paths(monkeypatch, tmp_path)

    def fake_get(url, headers=None, timeout=None, verify=None):  # noqa: ANN001
        raise requests.exceptions.Timeout("boom")

    monkeypatch.setattr(vpn_nodes.requests, "get", fake_get)

    with pytest.raises(ValueError):
        vpn_nodes.refresh_vpn_node_cache("https://example.invalid/subscribe")


def test_refresh_vpn_nodes_uses_provided_subscription_url_and_persists_it(monkeypatch) -> None:
    saved_payload = {}
    requested_url = "https://example.invalid/subscribe"
    initial_settings = {
        "common": {},
        "jobs": {},
        "platforms": {},
        "vpn": {"subscription_url": "https://old.invalid/subscribe"},
    }

    monkeypatch.setattr(service, "load_distribution_settings_db", lambda: initial_settings)

    def fake_save(payload):  # noqa: ANN001
        saved_payload["payload"] = payload
        return payload

    monkeypatch.setattr(service, "save_distribution_settings_db", fake_save)
    monkeypatch.setattr(
        service,
        "refresh_vpn_node_cache",
        lambda url: {
            "subscription_url": url,
            "fetched_at": 1,
            "refreshed_at": 1,
            "node_count": 0,
            "countries": {},
            "nodes": [],
        },
    )

    result = service.refresh_vpn_nodes(requested_url)

    assert result["subscription_url"] == requested_url
    assert saved_payload["payload"]["vpn"]["subscription_url"] == requested_url


def test_refresh_vpn_nodes_requires_subscription_url(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "load_distribution_settings_db",
        lambda: {"common": {}, "jobs": {}, "platforms": {}, "vpn": {"subscription_url": ""}},
    )

    with pytest.raises(ValueError):
        service.refresh_vpn_nodes()

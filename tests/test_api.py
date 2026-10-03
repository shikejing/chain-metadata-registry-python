"""End-to-end HTTP API tests."""

from __future__ import annotations

import tempfile

from fastapi.testclient import TestClient

from chain_metadata_registry.api import create_app

from .conftest import make_config, static_source


def _e(source_id, entry_id, height, asset_id, **fields):
    return {"entry_id": entry_id, "source_id": source_id, "height": height,
            "asset_id": asset_id, "fields": fields}


def _client(tmpdir: str, **kwargs):
    cfg = make_config(
        [
            static_source("hi", 10, 0, [_e("hi", "e1", 1, "x", name="High", symbol="HI")]),
            static_source("lo", 1, 1, [_e("lo", "e2", 1, "x", name="Low", decimals=6)]),
        ],
        tmpdir,
        **kwargs,
    )
    app = create_app(cfg, background_refresh=False)
    return TestClient(app), cfg


def test_health_and_root():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp)
        r = client.get("/")
        assert r.status_code == 200
        r = client.get("/health")
        assert r.status_code == 200


def test_assets_after_refresh():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp)
        client.post("/v1/refresh")
        r = client.get("/v1/assets")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        # Every response carries freshness metadata.
        assert "snapshot_id" in body
        assert "committed_at" in body
        assert "stale" in body
        assert "degraded_sources" in body


def test_asset_provenance():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp)
        client.post("/v1/refresh")
        r = client.get("/v1/assets/x")
        assert r.status_code == 200
        asset = r.json()["asset"]
        assert asset["fields"]["name"]["value"] == "High"
        assert asset["fields"]["name"]["source_id"] == "hi"
        assert asset["fields"]["decimals"]["value"] == 6
        assert asset["fields"]["decimals"]["source_id"] == "lo"
        assert asset["fields"]["name"]["entry_id"] == "e1"


def test_asset_not_found():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp)
        client.post("/v1/refresh")
        r = client.get("/v1/assets/nope")
        assert r.status_code == 404


def test_entry_status_and_rollbacks():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp)
        client.post("/v1/refresh")
        r = client.get("/v1/entries/e1")
        assert r.status_code == 200
        assert r.json()["status"] == "present"
        r = client.get("/v1/entries/unknown-id")
        assert r.json()["status"] == "unknown"
        r = client.get("/v1/rollbacks")
        assert r.status_code == 200


def test_refresh_endpoint_returns_outcome():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp)
        r = client.post("/v1/refresh")
        assert r.status_code == 200
        body = r.json()
        assert body["changed"] is True
        assert "rebuilt_assets" in body
        assert "view" in body


def test_stale_policy_error_returns_503():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp, ttl=0.0, stale_policy="error")
        client.post("/v1/refresh")
        # With ttl=0, the snapshot is immediately stale.
        r = client.get("/v1/assets")
        assert r.status_code == 503


def test_query_before_any_snapshot():
    with tempfile.TemporaryDirectory() as tmp:
        client, _ = _client(tmp)
        # No refresh yet: asset/snapshot endpoints 404, entry unknown, rollbacks empty.
        assert client.get("/v1/assets").status_code == 404
        assert client.get("/v1/assets/x").status_code == 404
        assert client.get("/v1/snapshot").status_code == 404
        r = client.get("/v1/entries/e1")
        assert r.status_code == 200
        assert r.json()["status"] == "unknown"
        r = client.get("/v1/rollbacks")
        assert r.status_code == 200
        assert r.json()["count"] == 0
        r = client.get("/health")
        assert r.json()["status"] == "no_snapshot"

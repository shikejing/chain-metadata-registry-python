import json

from fastapi.testclient import TestClient

from registry.api import create_app
from registry.config import RegistryConfig, SourceConfig


def make_client(tmp_path):
    src_file = tmp_path / "chain.json"
    src_file.write_text(json.dumps([
        {"asset_id": "ETH", "entry_id": "1",
         "fields": {"name": "Ether", "decimals": 18}},
    ]))
    cfg = RegistryConfig(sources=(SourceConfig(
        name="chain", priority=0, path=str(src_file)),))
    return TestClient(create_app(cfg)), src_file


def test_query_flow(tmp_path):
    client, _ = make_client(tmp_path)
    assert client.post("/refresh").status_code == 200
    resp = client.get("/assets/ETH")
    assert resp.status_code == 200
    body = resp.json()
    assert body["fields"]["name"] == "Ether"
    assert body["provenance"] == {"name": "chain", "decimals": "chain"}
    assert client.get("/assets/NOPE").status_code == 404


def test_reorg_endpoint_and_history(tmp_path):
    client, src_file = make_client(tmp_path)
    client.post("/refresh")
    resp = client.post("/reorg", json={
        "rollback_count": 1,
        "replay": [{"asset_id": "ETH", "entry_id": "2",
                    "fields": {"name": "Ether v2"}}],
    })
    assert resp.status_code == 200
    assert resp.json()["rolled_back"] == ["1"]
    body = client.get("/assets/ETH").json()
    assert body["fields"]["name"] == "Ether v2"
    reorgs = client.get("/reorgs").json()["reorgs"]
    assert reorgs[0]["rolled_back"] == ["1"]
    # Window exceeded -> explicit 409, nothing silently dropped.
    resp = client.post("/reorg", json={"rollback_count": 10**6})
    assert resp.status_code == 409


def test_health_endpoint(tmp_path):
    client, _ = make_client(tmp_path)
    client.post("/refresh")
    assert client.get("/health").json()["sources"] == {"chain": "ok"}

from __future__ import annotations

import sqlite3

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from prod_ops_mcp.config import OpsMCPConfig
from prod_ops_mcp.repository import ReadOnlyOperationsRepository
from prod_ops_mcp.server import create_app


def config(path):
    return OpsMCPConfig(
        enabled=True,
        bind_host="127.0.0.1",
        port=8091,
        database_path=str(path),
        path_secret="p" * 40,
        bearer_token="b" * 40,
        allow_path_only_auth=True,
        allowed_origins=(),
        max_request_bytes=32768,
        max_response_bytes=196608,
        max_concurrency=1,
        ingress_requests_per_minute=30,
        ingress_burst=5,
        requests_per_minute=60,
        burst=10,
        egress_bytes_per_hour=1048576,
        path_only_requests_per_minute=4,
        path_only_egress_bytes_per_hour=262144,
        db_timeout_ms=500,
        cache_ttl_seconds=10,
    )


@pytest_asyncio.fixture
async def client(tmp_path):
    path = tmp_path / "db.sqlite"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE event(id INTEGER PRIMARY KEY, title TEXT, date TEXT, time TEXT)")
    connection.execute("INSERT INTO event VALUES(1,'Тест','2026-08-10','10:00')")
    connection.commit()
    connection.close()
    cfg = config(path)
    app = create_app(cfg, ReadOnlyOperationsRepository(str(path), query_timeout_ms=500))
    async with TestClient(TestServer(app)) as test_client:
        yield test_client, cfg


@pytest.mark.asyncio
async def test_initialize_requires_auth_and_lists_read_only_tools(client):
    http, cfg = client
    endpoint = f"/{cfg.path_secret}/mcp"
    rejected = await http.post(endpoint, json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    # Path-only is explicitly enabled for this fixture.
    assert rejected.status == 200
    payload = await rejected.json()
    assert payload["result"]["protocolVersion"] == "2025-11-25"

    listed = await http.post(
        endpoint,
        headers={"Authorization": "Bearer " + "b" * 40},
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    )
    data = await listed.json()
    names = {item["name"] for item in data["result"]["tools"]}
    assert "event_explain" in names
    assert all(item["annotations"]["readOnlyHint"] for item in data["result"]["tools"])


@pytest.mark.asyncio
async def test_path_only_auth_hides_deeper_incident_tools(client):
    http, cfg = client
    endpoint = f"/{cfg.path_secret}/mcp"
    response = await http.post(
        endpoint,
        json={"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {}},
    )
    data = await response.json()
    names = {item["name"] for item in data["result"]["tools"]}
    assert names == {"prod_health_snapshot", "events_find", "social_capabilities"}


@pytest.mark.asyncio
async def test_get_is_authenticated_even_without_sse(client):
    http, cfg = client
    endpoint = f"/{cfg.path_secret}/mcp"
    # This fixture permits path-only mode, so GET is authenticated by the path
    # and then rejected only because stateless MVP has no SSE stream.
    response = await http.get(endpoint)
    assert response.status == 405


"""Two actual PKCE sessions through real MCP/application/SQLite boundaries."""

import time
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from dataclasses import replace
from test_private_events_mcp_application_commands import setup
from test_private_events_mcp_partner_protocol import login, rpc
from _helpers.mcp_owner_oauth import login as owner_login
from private_events_mcp.integration import attach_private_events_mcp
from private_events_mcp.partner_access import PARTNER_ACTIONS, PARTNER_SCOPES


def data(response):
    assert not response.get("error"), response
    assert not response["result"].get("isError"), response
    return response["result"]["structuredContent"]


@pytest.mark.asyncio
async def test_two_partner_oauth_commands_and_catalog(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    c = replace(
        c, authenticated_requests_per_minute=1000, anonymous_requests_per_minute=1000
    )
    app = web.Application()
    server = attach_private_events_mcp(app, c, event_database=db)

    async def reconcile(*args):
        pass

    server.event_commands.reconcile = reconcile
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        ot = await owner_login(
            client,
            c,
            "partners:manage events:read events:write promo:read promo:write operations:read",
        )
        tokens = []
        for n in (1, 2):
            _, r = await rpc(
                client,
                c.mcp_path,
                ot,
                "partner_create",
                dict(
                    tenant_id=f"http{n}",
                    organization_id=f"org{n}",
                    display_name=f"Partner {n}",
                    redirect_uris=["http://127.0.0.1:8421/callback"],
                    event_ids=[eid] if n == 1 else [],
                    expires_at=int(time.time()) + 3600,
                    policy=dict(
                        scopes=sorted(PARTNER_SCOPES),
                        actions=sorted(PARTNER_ACTIONS),
                        auto_approve=["event_create"],
                    ),
                ),
            )
            status, t = await login(client, c, data(r))
            assert status == 200
            tokens.append(t["access_token"])
        at, bt = tokens
        _, r = await rpc(client, c.partner_mcp_path, at, "", method="tools/list")
        names = {t["name"] for t in r["result"]["tools"]}
        assert {
            "event_cancel_prepare",
            "event_reschedule_commit",
            "promo_campaign_state_prepare",
            "promo_campaign_update_prepare",
            "promo_campaign_update_commit",
            "event_publication_status",
        } <= names
        assert not names & {
            "partner_create",
            "event_operation_decide",
            "promo_operation_decide",
            "operations_snapshot",
        }
        _, owner_catalog = await rpc(
            client, c.mcp_path, ot, "", method="tools/list"
        )
        owner_names = {tool["name"] for tool in owner_catalog["result"]["tools"]}
        assert {
            "promo_campaign_update_prepare",
            "promo_campaign_update_commit",
        } <= owner_names

        _, r = await rpc(client, c.partner_mcp_path, at, "partner_workspace_get")
        assert (
            data(r)["capabilities"]["event_operations"]
            and data(r)["capabilities"]["promo_operations"]
        )
        for foreign in (eid, eid + 1000):
            _, r = await rpc(
                client,
                c.partner_mcp_path,
                bt,
                "event_publication_status",
                {"event_id": foreign},
            )
            assert r["result"]["isError"]
        _, r = await rpc(
            client,
            c.partner_mcp_path,
            at,
            "event_cancel_prepare",
            {
                "request": {
                    "event_id": eid,
                    "organizer_comment": "Cancelled by organizer",
                    "source": {"type": "organizer", "external_id": "http-cancel"},
                },
                "idempotency_key": "http-cancel-op",
            },
        )
        prepared = data(r)
        commit = {k: prepared[k] for k in ("preparation_ref", "action_digest")}
        _, r = await rpc(client, c.partner_mcp_path, at, "event_cancel_commit", commit)
        assert data(r)["status"] == "review_required"
        _, r = await rpc(
            client,
            c.mcp_path,
            ot,
            "event_operation_decide",
            {**commit, "decision": "approve"},
        )
        assert data(r)["status"] == "accepted"
        _, r = await rpc(
            client,
            c.partner_mcp_path,
            at,
            "event_operation_get",
            {"operation_ref": prepared["operation_ref"]},
        )
        assert data(r)["status"] == "accepted"
        assert (await rpc(client, c.mcp_path, at, "", method="tools/list"))[0] == 401
        assert (await rpc(client, c.codex_mcp_path, at, "", method="tools/list"))[
            0
        ] == 401
    finally:
        await client.close()

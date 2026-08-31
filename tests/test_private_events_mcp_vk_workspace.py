from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pytest

from private_events_mcp.auth_store import OAuthStateStore
from private_events_mcp.crypto import AccessIdentity
from private_events_mcp.social_workspace import (
    EditorialSampleState,
    SocialReadPurpose,
    SocialTargetKind,
    SocialWorkspaceValidationError,
    TargetLocatorKind,
    validate_editorial_sample_response,
    validate_prepare_request,
    validate_read_request,
)
from private_events_mcp.social_workspace_runtime import (
    RuntimePrincipal,
    SocialWorkspaceRuntime,
)
from private_events_mcp.tool_catalog import ToolCallContext
from private_events_mcp_vk_adapter import (
    VK_API_VERSION,
    VK_FIXED_METHOD_ALLOWLIST,
    VK_OPERATION_ACTORS,
    VKActor,
    VKWorkspaceAdapter,
    VKWorkspaceError,
)


class FakeRefs:
    def __init__(self) -> None:
        self.values: dict[tuple[str, str], dict[str, Any]] = {}

    def mint(self, kind: str, native_value: Mapping[str, Any]) -> str:
        raw = json.dumps(dict(native_value), sort_keys=True, separators=(",", ":"))
        prefix = {
            "target": "tgt", "item": "itm", "asset": "ast",
            "cursor": "cur", "sample": "smp",
        }[kind]
        ref = f"{prefix}_{hashlib.sha256((kind + raw).encode()).hexdigest()[:24]}"
        self.values[(kind, ref)] = dict(native_value)
        return ref

    def resolve(self, kind: str, opaque_ref: str) -> Mapping[str, Any]:
        return dict(self.values[(kind, opaque_ref)])

    def put_named(self, kind: str, opaque_ref: str, value: Mapping[str, Any]) -> None:
        self.values[(kind, opaque_ref)] = dict(value)


class FakeGovernor:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, str]] = []

    async def before_call(self, actor: VKActor, capability: str) -> None:
        self.events.append(("before", actor.value, capability))

    async def after_call(self, actor: VKActor, capability: str, outcome: str) -> None:
        self.events.append(("after", actor.value, outcome))


class FakeCooldown:
    def __init__(self) -> None:
        self.captchas: list[VKActor] = []
        self.successes: list[VKActor] = []
        self.blocked: set[VKActor] = set()

    async def ensure_available(self, actor: VKActor) -> None:
        if actor in self.blocked:
            raise RuntimeError("captcha cooldown active with provider_secret")

    async def record_captcha(self, actor: VKActor) -> None:
        self.captchas.append(actor)
        self.blocked.add(actor)

    async def record_success(self, actor: VKActor) -> None:
        self.successes.append(actor)


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.denied: set[tuple[VKActor, str]] = set()
        self.timeout_method: str | None = None
        self.captcha_method: str | None = None
        self.api_error_method: str | None = None
        self.api_error_code = 27
        self.flagged_wall = False
        self.last_message_peer = 101
        self.last_wall_post: dict[str, Any] = {}

    def permits(self, actor: VKActor, capability: str) -> bool:
        return (actor, capability) not in self.denied

    async def invoke(self, **call: Any) -> Any:
        self.calls.append(call)
        method = call["method"]
        params = call["params"]
        if method == self.timeout_method:
            await asyncio.sleep(0.1)
        if method == self.captcha_method:
            return {"error": {"error_code": 14, "error_msg": "provider_secret"}}
        if method == self.api_error_method:
            return {
                "error": {
                    "error_code": self.api_error_code,
                    "error_msg": "provider_secret must not escape",
                }
            }
        if method == "utils.resolveScreenName":
            return {"type": "group" if params["screen_name"] == "named_club" else "user", "object_id": 101}
        if method == "users.get":
            user_id = int(params.get("user_ids", 777))
            return [{"id": user_id, "first_name": "Ticket", "last_name": "Winner", "screen_name": "ticket_winner", "status": "provider_secret biography"}]
        if method == "groups.getById":
            return {"groups": [{"id": 101, "name": "Named Community", "screen_name": "named_club", "description": "provider_secret description", "activity": "Culture", "site": "https://example.test", "members_count": 4321}]}
        if method == "groups.search":
            query = params["q"]
            return {"items": [
                {"id": 300 + len(query), "name": f"Community {query}", "screen_name": f"c{len(query)}", "members_count": 10},
                {"id": 333, "name": "Common", "screen_name": "common", "members_count": 20},
            ]}
        if method == "wall.get":
            if (
                params.get("filter") == "postponed"
                and self.last_wall_post.get("publish_date") is not None
            ):
                attachments = [
                    {
                        "type": "photo",
                        # VK re-owns the saved photo on the community wall.
                        "photo": {"owner_id": -101, "id": 9876},
                    }
                    for value in str(
                        self.last_wall_post.get("attachments") or ""
                    ).split(",")
                    if value.startswith("photo")
                ]
                return {
                    "items": [
                        {
                            "id": 801,
                            "owner_id": -101,
                            "date": self.last_wall_post["publish_date"],
                            "text": self.last_wall_post.get("message", ""),
                            "attachments": attachments,
                        }
                    ]
                }
            offset, count = params.get("offset", 0), params["count"]
            posts = [self._post(offset + index + 1) for index in range(count)]
            if self.flagged_wall and posts:
                posts[0]["marked_as_ads"] = 1
                if len(posts) > 1:
                    posts[1]["copy_history"] = [{"id": 1}]
                if len(posts) > 2:
                    posts[2]["is_pinned"] = 1
            return {"items": posts}
        if method in {"wall.search", "newsfeed.search"}:
            return {"items": [self._post(501), self._post(502)]}
        if method == "wall.getById":
            owner_id, post_id = str(params["posts"]).split("_", 1)
            attachments = (
                [
                    {
                        "type": "photo",
                        "photo": {
                            "id": 55,
                            "owner_id": int(owner_id),
                            "sizes": [{
                                "url": "https://sun.userapi.com/wall-photo.jpg",
                                "width": 720,
                                "height": 480,
                            }],
                        },
                    }
                    for value in str(self.last_wall_post.get("attachments") or "").split(",")
                    if value.startswith("photo")
                ]
                if "attachments" in self.last_wall_post
                else self._post(int(post_id))["attachments"]
            )
            return [{**self._post(int(post_id)), "owner_id": int(owner_id),
                     "text": self.last_wall_post.get("message", self._post(int(post_id))["text"]),
                     "attachments": attachments,
                     "likes": {"count": 7}, "comments": {"count": 4}, "reposts": {"count": 2}}]
        if method == "notifications.get":
            if params.get("start_from") == "provider-page-2":
                return {"items": []}
            return {
                "items": [
                    {
                        "type": "comment_post",
                        "date": 1_700_001_000,
                        "parent": {"owner_id": -101, "id": 501},
                        "feedback": {
                            "id": 91,
                            "date": 1_700_001_001,
                            "text": "Вероятно, в событии неверная дата provider_secret",
                        },
                    },
                    {
                        "type": "like_post",
                        "date": 1_700_001_002,
                        "parent": {"owner_id": -101, "id": 501},
                        "feedback": {"id": 92, "text": "must be skipped"},
                    },
                ],
                "next_from": "provider-page-2",
            }
        if method == "wall.getComments":
            return {"items": [{"id": 61, "date": 1_700_000_010, "text": "A comment"}]}
        if method == "likes.getList":
            return {"count": 9, "items": [777]}
        if method == "messages.getHistory":
            return {"items": [{"id": 901, "peer_id": params["peer_id"], "date": 1_700_000_100, "text": "Your reminder"}]}
        if method == "messages.getById":
            message_id = int(str(params["message_ids"]).split(",")[0])
            return {"items": [{"id": message_id, "peer_id": self.last_message_peer, "date": 1_700_000_100, "text": "You won tickets"}]}
        if method == "messages.getConversations":
            if params.get("extended") == 1:
                return {
                    "items": [
                        {
                            "conversation": {
                                "peer": {"id": 101, "type": "user"},
                                "unread_count": 1,
                            },
                            "last_message": {
                                "text": "private body provider_secret must not cross",
                                "id": 910,
                            },
                        },
                        {
                            "conversation": {
                                "peer": {"id": 2_000_000_007, "type": "chat"},
                                "chat_settings": {"title": "Ticket winners"},
                                "unread_count": 2,
                            },
                            "last_message": {
                                "text": "second private body must not cross",
                                "id": 911,
                            },
                        },
                        {
                            "conversation": {
                                "peer": {"id": -333, "type": "group"},
                                "unread_count": 3,
                            },
                            "last_message": {
                                "text": "third private body must not cross",
                                "id": 912,
                            },
                        },
                    ],
                    "profiles": [
                        {"id": 101, "first_name": "Ticket", "last_name": "Winner"}
                    ],
                    "groups": [{"id": 333, "name": "VK Tickets"}],
                }
            return {"items": [{"conversation": {"peer": {"id": 101}}, "last_message": {"id": 902, "peer_id": 101, "date": 1_700_000_101, "text": "Conversation history"}}]}
        if method == "stories.get":
            return {
                "items": [
                    {
                        "type": "stories",
                        "owner_id": -101,
                        "stories": [
                            {
                                "id": 71,
                                "owner_id": -101,
                                "date": 1_700_000_200,
                                "text": "Story",
                                "photo": {
                                    "sizes": [
                                        {
                                            "url": "https://sun9-1.userapi.com/story.jpg",
                                            "width": 1080,
                                            "height": 1920,
                                        }
                                    ]
                                },
                            }
                        ],
                    }
                ]
            }
        if method == "stories.getStats":
            return {
                "views": {"count": 45, "state": "on"},
                "likes": {"count": 3, "state": "on"},
                "replies": {"count": 1, "state": "on"},
                "shares": {"count": 2, "state": "on"},
            }
        if method == "stats.get":
            return [
                {
                    "visitors": {"views": 20},
                    "activity": {"likes": 4, "comments": 2, "copies": 1},
                },
                {
                    "visitors": {"views": 30},
                    "activity": {"likes": 5, "comments": 3, "copies": 2},
                },
            ]
        if method == "groups.getMembers":
            return {"count": 4321, "items": []}
        if method == "messages.send":
            self.last_message_peer = params["peer_id"]
            return {"message_id": 901}
        if method == "wall.post":
            self.last_wall_post = dict(params)
            return {"post_id": 801}
        if method == "wall.createComment":
            return {"comment_id": 802}
        if method == "stories.save":
            return {"items": [{"id": 803}]}
        if method in {"wall.edit", "wall.delete", "likes.add", "likes.delete", "wall.repost", "messages.edit", "messages.delete"}:
            return {"success": 1}
        raise AssertionError(f"unexpected fixed VK call: {method}")

    @staticmethod
    def _post(post_id: int) -> dict[str, Any]:
        return {"id": post_id, "owner_id": -101, "date": 1_700_000_000 + post_id,
                "text": f"Post {post_id} provider_secret",
                "attachments": [{"type": "photo", "photo": {
                    "id": 55, "owner_id": -101,
                    "sizes": [{"url": "https://sun.userapi.com/wall-photo.jpg",
                               "width": 720, "height": 480}],
                }}],
                "views": {"count": post_id}, "likes": {"count": 2},
                "comments": {"count": 1}, "reposts": {"count": 1}}


def _timestamp(day: str, hour: int = 12) -> int:
    from datetime import datetime, timezone

    return int(datetime.fromisoformat(f"{day}T{hour:02d}:00:00+00:00").timestamp())


@pytest.fixture
def workspace() -> tuple[VKWorkspaceAdapter, FakeTransport, FakeRefs, FakeGovernor, FakeCooldown]:
    transport, refs, governor, cooldown = FakeTransport(), FakeRefs(), FakeGovernor(), FakeCooldown()
    adapter = VKWorkspaceAdapter(
        transport=transport,
        refs=refs,
        governor=governor,
        cooldown=cooldown,
        sanitize_text=lambda text: text.replace("provider_secret", "[redacted]"),
        timeout_seconds=0.02,
    )
    return adapter, transport, refs, governor, cooldown


def mint_target(refs: FakeRefs, kind: str = "community") -> str:
    if kind == "community":
        return refs.mint("target", {"kind": kind, "group_id": 101, "owner_id": -101})
    if kind == "user":
        return refs.mint("target", {"kind": kind, "user_id": 101, "peer_id": 101})
    return refs.mint("target", {"kind": kind, "user_id": 777, "peer_id": 777})


def read_request(operation: str, **updates: Any):
    payload: dict[str, Any] = {"platform": "vk", "operation": operation}
    payload.update(updates)
    return validate_read_request(payload)


@pytest.mark.asyncio
async def test_fixed_allowlist_version_and_exact_resolution_without_native_id_leakage(workspace) -> None:
    adapter, transport, _, _, _ = workspace
    user_request = read_request("resolve_target", target_locator={"kind": "profile_link", "value": "https://vk.com/ticket_winner"}, expected_target_kinds=["user"])
    user = await adapter.resolve(user_request)
    self_request = read_request("resolve_target", target_locator={"kind": "self"}, expected_target_kinds=["self"])
    own = await adapter.resolve(self_request)
    group_request = replace(user_request, target_locator=replace(user_request.target_locator, kind=TargetLocatorKind.USERNAME, value="named_club"), expected_target_kinds=(SocialTargetKind.COMMUNITY,))
    group = await adapter.resolve(group_request)
    assert (user["kind"], own["kind"], group["kind"]) == ("user", "self", "community")
    public = json.dumps([user, own, group])
    assert '"object_id"' not in public and '"group_id"' not in public and '"user_id"' not in public
    assert "provider_secret" not in public
    assert all(call["method"] in VK_FIXED_METHOD_ALLOWLIST for call in transport.calls)
    assert all(call["version"] == VK_API_VERSION for call in transport.calls)
    assert all(set(call) == {"actor", "method", "params", "version", "timeout_seconds"} for call in transport.calls)
    assert not hasattr(adapter, "call") and not hasattr(adapter, "raw_method") and not hasattr(adapter, "vk_api")
    assert all(actor in {item.value for item in VKActor} for actor, _ in VK_OPERATION_ACTORS.values())


@pytest.mark.asyncio
async def test_exact_post_link_resolution_and_bounded_comment_notifications(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    resolved = await adapter.read(
        read_request(
            "resolve_item",
            target_locator={
                "kind": "profile_link",
                "value": "https://vk.com/wall-101_501",
            },
            read_access="public",
        )
    )
    assert resolved["item"]["kind"] == "post"
    assert resolved["source_target"]["title"] == "Named Community"
    assert resolved["item"]["target_ref"] == resolved["source_target"]["target_ref"]
    assert "owner_id" not in json.dumps(resolved)
    assert "provider_secret" not in json.dumps(resolved)

    first = await adapter.read(
        read_request(
            "list_notifications",
            limit=25,
            date_from="2023-11-14",
            date_to="2023-11-15",
        )
    )
    assert len(first["results"]) == 1
    hint = first["results"][0]
    assert hint["source_kind"] == "comment"
    assert "неверная дата" in hint["text"]
    assert "provider_secret" not in hint["text"]
    assert refs.resolve("item", hint["root_item_ref"]) == {
        "kind": "post",
        "group_id": 101,
        "owner_id": -101,
        "post_id": 501,
    }
    second = await adapter.read(
        read_request(
            "list_notifications",
            limit=25,
            date_from="2023-11-14",
            date_to="2023-11-15",
            cursor=first["next_cursor"],
        )
    )
    assert second == {"results": [], "trust": "untrusted_external_data"}
    calls = [call for call in transport.calls if call["method"] == "notifications.get"]
    assert calls[0]["actor"] is VKActor.NOTIFICATION_READER
    assert calls[0]["params"]["count"] == 25
    assert calls[0]["params"]["filters"] == "comments,mentions"
    assert calls[1]["params"]["start_from"] == "provider-page-2"


@pytest.mark.asyncio
async def test_notification_cursor_rejects_oversized_provider_state_before_persisting(
    workspace,
) -> None:
    adapter, transport, refs, _, _ = workspace
    original_invoke = transport.invoke

    async def oversized_cursor(**call: Any) -> Any:
        if call["method"] == "notifications.get":
            return {"items": [], "next_from": "X" * 100_000}
        return await original_invoke(**call)

    transport.invoke = oversized_cursor
    with pytest.raises(VKWorkspaceError, match="cursor_invalid"):
        await adapter.read(read_request("list_notifications", limit=25))
    assert not any(kind == "cursor" for kind, _ref in refs.values)


@pytest.mark.asyncio
async def test_runtime_notification_hint_to_exact_thread_chain_uses_only_public_refs(
    workspace, tmp_path,
) -> None:
    adapter, _, _, _, _ = workspace
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"vk": adapter},
        encryption_key="runtime-vk-notification-key-123456",
    )
    identity = AccessIdentity(
        "operator",
        "chatgpt",
        frozenset({"vk:notifications:read", "vk:read:public"}),
        "https://mcp.example",
        "jti-notifications",
        2_000_000_000,
    )
    context = ToolCallContext(identity, identity.audience)
    resolved = await runtime.read(
        read_request(
            "resolve_item",
            target_locator={
                "kind": "profile_link",
                "value": "https://vk.com/wall-101_501",
            },
            read_access="public",
        ),
        context,
    )
    hints = await runtime.read(
        read_request("list_notifications", limit=25), context
    )
    root_ref = hints["results"][0]["root_item_ref"]
    thread = await runtime.read(
        read_request(
            "list_comments",
            item_ref=root_ref,
            read_access="public",
            limit=25,
        ),
        context,
    )
    assert resolved["item"]["target_ref"] == resolved["source_target"]["target_ref"]
    assert thread["root_item_ref"] == root_ref
    assert thread["items"][0]["text"] == "A comment"
    public = json.dumps({"resolved": resolved, "hints": hints, "thread": thread})
    assert "owner_id" not in public
    assert "post_id" not in public
    assert "comment_id" not in public
    assert "provider-page-2" not in public


def test_exact_post_link_and_notification_contracts_reject_unsafe_inputs() -> None:
    for value in (
        "https://evil.example/wall-101_501",
        "https://vk.com:444/wall-101_501",
        "https://user@vk.com/wall-101_501",
        "https://vk.com/wall-101_501?reply=1",
        "https://vk.com/club101",
    ):
        with pytest.raises(SocialWorkspaceValidationError):
            request = read_request(
                "resolve_item",
                target_locator={"kind": "profile_link", "value": value},
                read_access="public",
            )
            VKWorkspaceAdapter._parse_post_link(request.target_locator.value or "")
    with pytest.raises(SocialWorkspaceValidationError):
        read_request("list_notifications", limit=26)
    with pytest.raises(SocialWorkspaceValidationError):
        read_request(
            "list_notifications", date_from="2026-08-01", date_to="2026-08-05"
        )


@pytest.mark.asyncio
async def test_named_community_editorial_sample_pages_to_100_with_metadata_and_schema(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    target_ref = mint_target(refs)
    request = read_request("editorial_sample", target_ref=target_ref, expected_target_kinds=["community"], read_access="public", purpose="editorial_analysis", authorization_basis="operator_authorized", page_size=25, total_limit=100)
    pages = []
    cumulative = 0
    for _ in range(4):
        page = await adapter.read(request)
        state = EditorialSampleState(
            sample_ref=page["sample_ref"], target_ref=target_ref,
            target_kinds=frozenset({SocialTargetKind.COMMUNITY}), purpose=SocialReadPurpose.EDITORIAL_ANALYSIS,
            date_from=None, date_to=None, total_limit=100, cumulative_count=cumulative,
            server_minted=True, continuation_cursor=request.cursor,
            cursor_server_minted=request.cursor is not None, ephemeral=True, durable_index=False,
        )
        cumulative = validate_editorial_sample_response(request, state, page)
        pages.append(page)
        if page.get("next_cursor"):
            request = replace(request, sample_ref=page["sample_ref"], cursor=page["next_cursor"])
    assert cumulative == 100
    assert pages[0]["target"]["title"] == "Named Community"
    assert pages[0]["target"]["about"] == "Culture"
    assert pages[0]["target"]["basic_metrics"] == {"members": 4321}
    wall_calls = [call for call in transport.calls if call["method"] == "wall.get"]
    assert [call["params"]["count"] for call in wall_calls] == [25] * 4
    assert [call["params"]["offset"] for call in wall_calls] == [0, 25, 50, 75]
    assert all(call["params"]["filter"] == "owner" for call in wall_calls)
    assert all("provider_secret" not in json.dumps(page) for page in pages)


@pytest.mark.asyncio
async def test_runtime_and_real_vk_adapter_share_server_sample_across_four_pages(
    workspace, tmp_path,
) -> None:
    adapter, transport, refs, _, _ = workspace
    runtime = SocialWorkspaceRuntime(
        store=OAuthStateStore(str(tmp_path / "auth.sqlite")),
        adapters={"vk": adapter},
        encryption_key="runtime-vk-integration-key-123456",
    )
    identity = AccessIdentity(
        "operator",
        "chatgpt",
        frozenset({"vk:read:public"}),
        "https://mcp.example",
        "jti",
        2_000_000_000,
    )
    context = ToolCallContext(identity, identity.audience)
    native_target = mint_target(refs)
    public_target = runtime._mint_ref(
        "target",
        native_target,
        "vk",
        RuntimePrincipal.from_context(context),
    )
    payload: dict[str, Any] = {
        "platform": "vk",
        "operation": "editorial_sample",
        "target_ref": public_target,
        "expected_target_kinds": ["community"],
        "read_access": "public",
        "purpose": "editorial_analysis",
        "authorization_basis": "operator_authorized",
        "page_size": 25,
        "total_limit": 100,
    }
    pages = []
    for _ in range(4):
        page = await runtime.read(validate_read_request(payload), context)
        pages.append(page)
        if page.get("next_cursor"):
            payload.update(
                sample_ref=page["sample_ref"],
                cursor=page["next_cursor"],
            )
    assert [page["cumulative_count"] for page in pages] == [25, 50, 75, 100]
    assert pages[-1].get("next_cursor") is None
    assert len({page["sample_ref"] for page in pages}) == 1
    wall_calls = [call for call in transport.calls if call["method"] == "wall.get"]
    assert [call["params"]["offset"] for call in wall_calls] == [0, 25, 50, 75]


@pytest.mark.asyncio
async def test_editorial_sample_keeps_ads_reposts_and_pinned_out_of_owner_post_items(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    transport.flagged_wall = True
    target_ref = mint_target(refs)
    page = await adapter.read(read_request("editorial_sample", target_ref=target_ref, expected_target_kinds=["community"], read_access="public", purpose="editorial_analysis", authorization_basis="operator_authorized", page_size=5, total_limit=100))
    assert page["sampled_count"] == 2
    assert [item["text"].split()[1] for item in page["items"]] == ["4", "5"]
    assert all(set(item).isdisjoint({"marked_as_ads", "copy_history", "is_pinned"}) for item in page["items"])


@pytest.mark.asyncio
async def test_exact_person_ticket_winner_dm_uses_random_id_and_read_after_write(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    user_ref = mint_target(refs, "user")
    intent = validate_prepare_request({"platform": "vk", "action": "send_message", "idempotency_key": "ticket-winner-2026", "target_ref": user_ref, "content": {"text": "You won tickets"}})
    receipt = await adapter.execute(intent)
    assert receipt["status"] == "succeeded"
    assert receipt["read_after_write"]["verified"] is True
    calls = [call for call in transport.calls if call["method"] in {"messages.send", "messages.getById"}]
    assert [call["method"] for call in calls] == ["messages.send", "messages.getById"]
    assert set(calls[0]["params"]) == {"peer_id", "message", "random_id"}
    assert type(calls[0]["params"]["random_id"]) is int and calls[0]["params"]["random_id"] > 0
    assert "message_id" not in json.dumps(receipt) and "peer_id" not in json.dumps(receipt)
    assert await adapter.execute(intent) == receipt
    assert len([call for call in transport.calls if call["method"] == "messages.send"]) == 1


@pytest.mark.asyncio
async def test_unread_dialog_listing_returns_sender_metadata_without_message_bodies(
    workspace,
) -> None:
    adapter, transport, refs, _, _ = workspace
    request = read_request(
        "list_dialogs",
        read_access="dialogs",
        unread_only=True,
        limit=20,
    )
    result = await adapter.read(request)
    assert [(row["title"], row["kind"], row["unread_count"]) for row in result["results"]] == [
        ("Ticket Winner", "user", 1),
        ("Ticket winners", "chat", 2),
        ("VK Tickets", "community", 3),
    ]
    encoded = json.dumps(result, ensure_ascii=False)
    assert "last_message" not in encoded
    assert "private body" not in encoded
    assert "provider_secret" not in encoded
    assert "peer_id" not in encoded
    assert "user_id" not in encoded
    assert "group_id" not in encoded
    call = next(call for call in transport.calls if call["method"] == "messages.getConversations")
    assert call["actor"] is VKActor.DIALOG_READER
    assert call["params"] == {
        "count": 20,
        "offset": 0,
        "filter": "unread",
        "extended": 1,
        "fields": "screen_name",
    }
    for row in result["results"]:
        native = refs.resolve("target", row["target_ref"])
        assert adapter._dialog_peer_id(native) is not None

    first_page = await adapter.read(
        read_request(
            "list_dialogs",
            read_access="dialogs",
            unread_only=True,
            limit=3,
        )
    )
    with pytest.raises(VKWorkspaceError, match="cursor_context_mismatch"):
        await adapter.read(
            read_request(
                "list_dialogs",
                read_access="dialogs",
                unread_only=False,
                limit=3,
                cursor=first_page["next_cursor"],
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "native",
    [
        {"kind": "chat", "peer_id": 2_000_000_007},
        {"kind": "community", "group_id": 333, "owner_id": -333, "peer_id": -333},
    ],
)
async def test_returned_dialog_targets_support_explicit_message_send(
    workspace, native,
) -> None:
    adapter, transport, refs, _, _ = workspace
    target_ref = refs.mint("target", native)
    caps = await adapter.capabilities(target_ref)
    assert "send_message" in caps["actions"]
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "send_message",
            "idempotency_key": "dialog-reminder-" + str(abs(native["peer_id"])),
            "target_ref": target_ref,
            "content": {"text": "Reminder"},
        }
    )
    receipt = await adapter.execute(intent)
    assert receipt["status"] == "succeeded"
    send = next(call for call in transport.calls if call["method"] == "messages.send")
    assert send["params"]["peer_id"] == native["peer_id"]


@pytest.mark.asyncio
async def test_wall_dialog_search_discovery_comments_reactions_story_and_stats(workspace) -> None:
    adapter, _, refs, _, _ = workspace
    community = mint_target(refs)
    user = mint_target(refs, "user")
    feed = await adapter.read(read_request("list_items", target_ref=community, read_access="public", limit=2))
    search = await adapter.read(read_request("search_items", query="concert", read_access="public", limit=2))
    discovery = await adapter.read(read_request("search_targets", query="culture arts", limit=5))
    dialog = await adapter.read(read_request("list_items", target_ref=user, read_access="dialogs", limit=2))
    own = refs.mint("target", {"kind": "self", "user_id": 777, "peer_id": 777})
    conversations = await adapter.read(read_request("list_items", target_ref=own, read_access="dialogs", limit=2))
    item_ref = feed["results"][0]["item_ref"]
    exact = await adapter.read(read_request("get_item", item_ref=item_ref, read_access="public"))
    comments = await adapter.read(read_request("list_comments", item_ref=item_ref, read_access="public"))
    reactions = await adapter.read(read_request("list_reactions", item_ref=item_ref, read_access="public"))
    story_page = await adapter.read(read_request("list_stories", target_ref=community, limit=5))
    post_stats = await adapter.read(read_request("get_statistics", item_ref=item_ref))
    community_stats = await adapter.read(read_request("get_statistics", target_ref=community))
    audience = await adapter.read(read_request("get_audience", target_ref=community))
    assert len(feed["results"]) == 2 and len(search["results"]) == 2
    assert len(discovery["results"]) >= 2 and dialog["results"][0]["kind"] == "message"
    assert conversations["results"][0]["text"] == "Conversation history"
    assert exact["item"]["kind"] == "post" and comments["items"][0]["kind"] == "comment"
    assert len(exact["item"]["media"]) == 1
    assert exact["item"]["attachments"][0]["kind"] == "photo"
    assert exact["item"]["attachments"][0]["asset_ref"] == exact["item"]["media"][0]
    assert reactions["reactions"] == [{"reaction": "like", "count": 9}]
    assert story_page["results"][0]["kind"] == "story"
    assert post_stats["basic_metrics"]["reactions"] == 7
    assert community_stats["basic_metrics"]["views"] == 50
    assert audience["audience"]["total"] == 4321
    assert "provider_secret" not in json.dumps([feed, search, discovery])


@pytest.mark.asyncio
async def test_closed_action_families_media_schedule_repost_and_story(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    community = mint_target(refs)
    user = mint_target(refs, "user")
    post = refs.mint("item", {"kind": "post", "group_id": 101, "owner_id": -101, "post_id": 501})
    refs.mint("item", {"kind": "message", "peer_id": 101, "message_id": 901})
    image = refs.mint("asset", {"role": "image", "attachment": "photo-101_55"})
    album = refs.mint("asset", {"role": "image", "attachment": "album-101_57"})
    video = refs.mint("asset", {"role": "video", "attachment": "video-101_58"})
    story_asset = refs.mint("asset", {"role": "video", "attachment": "video-101_56", "story_upload_result": "safe-upload-result-123"})
    intents = [
        {"action": "publish", "target_ref": community, "content": {"text": "Media", "media": [{"asset_ref": image, "role": "image"}, {"asset_ref": album, "role": "image"}, {"asset_ref": video, "role": "video"}]}},
        {"action": "schedule", "target_ref": community, "schedule_at": "2030-01-01T12:00:00Z", "content": {"text": "Later"}},
        {"action": "comment", "item_ref": post, "content": {"text": "Nice"}},
        {"action": "reaction", "item_ref": post, "reaction": "like"},
        {"action": "edit", "item_ref": post, "content": {"text": "Edited"}},
        {"action": "delete", "item_ref": post},
        {"action": "forward", "item_ref": post, "destination_target_ref": community},
        {"action": "story", "target_ref": community, "content": {"media": [{"asset_ref": story_asset, "role": "video"}]}},
    ]
    for index, fields in enumerate(intents):
        intent = validate_prepare_request({"platform": "vk", "idempotency_key": f"closed-action-{index}", **fields})
        result = await adapter.execute(intent)
        assert result["status"] == "succeeded"
    methods = [call["method"] for call in transport.calls]
    assert {"wall.post", "wall.createComment", "likes.add", "wall.edit", "wall.delete", "wall.repost", "stories.save"}.issubset(methods)
    publish = next(call for call in transport.calls if call["method"] == "wall.post" and "attachments" in call["params"])
    assert publish["params"]["attachments"] == "photo-101_55,album-101_57,video-101_58"
    assert all(call["method"] in VK_FIXED_METHOD_ALLOWLIST for call in transport.calls)
    assert user  # exact-user destination is separately tested by DM


@pytest.mark.asyncio
async def test_actor_denial_timeout_captcha_and_no_provider_error_leakage(workspace) -> None:
    adapter, transport, refs, governor, cooldown = workspace
    community = mint_target(refs)
    transport.denied.add((VKActor.PUBLIC_READER, "read_public"))
    with pytest.raises(VKWorkspaceError, match="actor_capability_denied"):
        await adapter.read(read_request("list_items", target_ref=community, read_access="public", limit=1))
    transport.denied.clear()
    transport.captcha_method = "wall.get"
    with pytest.raises(VKWorkspaceError) as captcha:
        await adapter.read(read_request("list_items", target_ref=community, read_access="public", limit=1))
    assert captcha.value.code == "captcha_cooldown"
    assert str(captcha.value) == "captcha_cooldown" and "provider_secret" not in str(captcha.value)
    assert cooldown.captchas == [VKActor.PUBLIC_READER]
    transport.captcha_method = None
    transport.timeout_method = "wall.post"
    intent = validate_prepare_request({"platform": "vk", "action": "publish", "idempotency_key": "timeout-action-01", "target_ref": community, "content": {"text": "May have published"}})
    result = await adapter.execute(intent)
    assert result["status"] == "outcome_unknown"
    assert result["retry_safe"] is False
    assert result["error_code"] == "provider_timeout"
    assert len([call for call in transport.calls if call["method"] == "wall.post"]) == 1
    assert any(event == ("after", "community_editor", "outcome_unknown") for event in governor.events)


@pytest.mark.asyncio
async def test_definite_vk_api_error_is_failed_with_concrete_code(workspace) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    transport.api_error_method = "wall.post"
    result = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "publish",
                "idempotency_key": "definite-vk-error-27",
                "target_ref": community,
                "content": {"text": "Must fail definitively"},
            }
        )
    )
    assert result["status"] == "failed"
    assert result["retry_safe"] is True
    assert result["error_code"] == "vk_api_error_27"
    assert "provider_secret" not in json.dumps(result)


@pytest.mark.asyncio
async def test_wall_post_without_readback_remains_non_retryable_unknown(workspace) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    transport.api_error_method = "wall.getById"
    result = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "publish",
                "idempotency_key": "wall-readback-missing-001",
                "target_ref": community,
                "content": {"text": "Provider accepted but cannot attest"},
            }
        )
    )
    assert result["status"] == "outcome_unknown"
    assert result["retry_safe"] is False
    assert result["error_code"] == "read_after_write_failed"
    assert [call["method"] for call in transport.calls] == [
        "wall.post",
        "wall.getById",
    ]


@pytest.mark.asyncio
async def test_wall_post_invalid_response_is_non_retryable_unknown(workspace) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    original_invoke = transport.invoke

    async def invalid_wall_response(**call: Any) -> Any:
        if call["method"] == "wall.post":
            transport.calls.append(call)
            return {}
        return await original_invoke(**call)

    transport.invoke = invalid_wall_response  # type: ignore[method-assign]
    result = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "publish",
                "idempotency_key": "wall-invalid-response-001",
                "target_ref": community,
                "content": {"text": "Possibly accepted"},
            }
        )
    )
    assert result["status"] == "outcome_unknown"
    assert result["retry_safe"] is False
    assert result["error_code"] == "wall_post_response_invalid"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "targeted"),
    [("list_items", True), ("search_items", True), ("search_items", False)],
)
async def test_content_reads_apply_inclusive_utc_date_bounds(
    workspace, operation: str, targeted: bool
) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    original_invoke = transport.invoke

    async def bounded_invoke(**call: Any) -> Any:
        if call["method"] in {"wall.get", "wall.search", "newsfeed.search"}:
            return {
                "items": [
                    {**transport._post(19), "date": _timestamp("2026-08-19")},
                    {**transport._post(27), "date": _timestamp("2026-08-27", 0)},
                    {**transport._post(28), "date": _timestamp("2026-08-27", 23)},
                    {**transport._post(29), "date": _timestamp("2026-08-28")},
                ]
            }
        return await original_invoke(**call)

    transport.invoke = bounded_invoke  # type: ignore[method-assign]
    payload: dict[str, Any] = {
        "operation": operation,
        "read_access": "public",
        "limit": 10,
        "date_from": "2026-08-27",
        "date_to": "2026-08-27",
    }
    if operation == "search_items":
        payload["query"] = "Кантиана"
    if targeted:
        payload["target_ref"] = mint_target(refs)
    page = await adapter.read(read_request(**payload))
    assert [item["published_at"][:10] for item in page["results"]] == [
        "2026-08-27",
        "2026-08-27",
    ]


@pytest.mark.asyncio
async def test_cursor_is_bound_to_date_window(workspace) -> None:
    adapter, _transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    first = await adapter.read(
        read_request(
            "list_items",
            target_ref=community,
            read_access="public",
            limit=2,
            date_from="2026-08-19",
            date_to="2026-08-20",
        )
    )
    with pytest.raises(VKWorkspaceError, match="cursor_context_mismatch"):
        await adapter.read(
            read_request(
                "list_items",
                target_ref=community,
                read_access="public",
                limit=2,
                cursor=first["next_cursor"],
                date_from="2026-08-27",
                date_to="2026-08-27",
            )
        )


@pytest.mark.asyncio
async def test_uncertain_publish_reconciles_by_exact_wall_readback(workspace) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    claimed = _timestamp("2026-08-27", 15)
    original_invoke = transport.invoke

    async def readback_invoke(**call: Any) -> Any:
        if call["method"] == "wall.get":
            transport.calls.append(call)
            return {
                "items": [
                    {
                        "id": 1777,
                        "owner_id": -101,
                        "date": claimed + 30,
                        "text": "Exact   editorial\npost",
                        "attachments": [{"type": "photo", "photo": {"id": 55}}],
                    }
                ]
            }
        return await original_invoke(**call)

    transport.invoke = readback_invoke  # type: ignore[method-assign]
    asset = refs.mint(
        "asset", {"role": "image", "attachment": "photo-101_55"}
    )
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "publish",
            "idempotency_key": "reconcile-wall-readback-001",
            "target_ref": community,
            "content": {
                "text": "Exact editorial post",
                "media": [{"asset_ref": asset, "role": "image"}],
            },
        }
    )
    result = await adapter.reconcile_intent(
        "op_" + "r" * 24, intent, claimed_at_ms=claimed * 1000
    )
    assert result["status"] == "succeeded"
    assert result["read_after_write"]["verified"] is True
    assert [call["method"] for call in transport.calls] == ["wall.get"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("match_count", "error_code"),
    [(0, "reconciliation_not_observed"), (2, "reconciliation_ambiguous")],
)
async def test_reconciliation_never_invents_success(
    workspace, match_count: int, error_code: str
) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    claimed = _timestamp("2026-08-27", 15)

    async def readback_invoke(**call: Any) -> Any:
        assert call["method"] == "wall.get"
        return {
            "items": [
                {
                    "id": 1800 + index,
                    "owner_id": -101,
                    "date": claimed + index,
                    "text": "Exact editorial post",
                    "attachments": [],
                }
                for index in range(match_count)
            ]
        }

    transport.invoke = readback_invoke  # type: ignore[method-assign]
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "publish",
            "idempotency_key": f"reconcile-negative-{match_count}",
            "target_ref": community,
            "content": {"text": "Exact editorial post"},
        }
    )
    result = await adapter.reconcile_intent(
        "op_" + "n" * 24, intent, claimed_at_ms=claimed * 1000
    )
    assert result["status"] == "outcome_unknown"
    assert result["retry_safe"] is False
    assert result["error_code"] == error_code


@pytest.mark.asyncio
async def test_scheduled_reconciliation_binds_publish_date_and_editor_actor(
    workspace,
) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    original_invoke = transport.invoke

    async def postponed_invoke(**call: Any) -> Any:
        if call["method"] == "wall.get" and call["params"]["filter"] == "postponed":
            transport.calls.append(call)
            return {
                "items": [
                    {
                        "id": 1901,
                        "owner_id": -101,
                        "date": _timestamp("2026-08-28", 12),
                        "text": "Scheduled exact text",
                        "attachments": [],
                    }
                ]
            }
        return await original_invoke(**call)

    transport.invoke = postponed_invoke  # type: ignore[method-assign]
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "schedule",
            "idempotency_key": "scheduled-reconcile-date-001",
            "target_ref": community,
            "schedule_at": "2026-08-29T12:00:00Z",
            "content": {"text": "Scheduled exact text"},
        }
    )
    result = await adapter.reconcile_intent(
        "op_" + "s" * 24,
        intent,
        claimed_at_ms=_timestamp("2026-08-27", 15) * 1000,
    )
    assert result["status"] == "outcome_unknown"
    assert result["error_code"] == "reconciliation_not_observed"
    assert transport.calls[0]["actor"] is VKActor.COMMUNITY_EDITOR


@pytest.mark.asyncio
async def test_scheduled_reconciliation_accepts_exact_post_with_reowned_photo(
    workspace,
) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    scheduled = _timestamp("2026-08-29", 12)

    async def postponed_invoke(**call: Any) -> Any:
        transport.calls.append(call)
        assert call["method"] == "wall.get"
        if call["params"]["filter"] == "owner":
            return {"items": []}
        assert call["params"]["filter"] == "postponed"
        return {
            "items": [
                {
                    "id": 1902,
                    "owner_id": -101,
                    "date": scheduled,
                    "text": "Scheduled image text",
                    "attachments": [
                        {
                            "type": "photo",
                            "photo": {"owner_id": -101, "id": 9999},
                        }
                    ],
                }
            ]
        }

    transport.invoke = postponed_invoke  # type: ignore[method-assign]
    image = refs.mint(
        "asset", {"role": "image", "attachment": "photo868977531_457259767"}
    )
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "schedule",
            "idempotency_key": "scheduled-reconcile-reowned-photo-001",
            "target_ref": community,
            "schedule_at": "2026-08-29T12:00:00Z",
            "content": {
                "text": "Scheduled image text",
                "media": [{"asset_ref": image, "role": "image"}],
            },
        }
    )
    result = await adapter.reconcile_intent(
        "op_" + "p" * 24,
        intent,
        claimed_at_ms=_timestamp("2026-08-27", 15) * 1000,
        provider_post_id=1902,
        provider_photo_refs=((868977531, 457259767),),
    )

    assert result["status"] == "succeeded"
    assert result["read_after_write"]["verified"] is True


@pytest.mark.asyncio
async def test_scheduled_reconciliation_finds_item_after_live_publication(
    workspace,
) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    scheduled = _timestamp("2026-08-29", 12)

    async def wall_surfaces(**call: Any) -> Any:
        transport.calls.append(call)
        if call["params"]["filter"] == "postponed":
            return {"items": []}
        return {
            "items": [
                {
                    "id": 1903,
                    "owner_id": -101,
                    "date": scheduled,
                    "text": "Already live scheduled text",
                    "attachments": [],
                }
            ]
        }

    transport.invoke = wall_surfaces  # type: ignore[method-assign]
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "schedule",
            "idempotency_key": "scheduled-reconcile-live-001",
            "target_ref": community,
            "schedule_at": "2026-08-29T12:00:00Z",
            "content": {"text": "Already live scheduled text"},
        }
    )
    result = await adapter.reconcile_intent(
        "op_" + "l" * 24,
        intent,
        claimed_at_ms=_timestamp("2026-08-27", 15) * 1000,
        provider_post_id=1903,
    )

    assert result["status"] == "succeeded"
    assert [call["params"]["filter"] for call in transport.calls] == [
        "postponed",
        "owner",
    ]


@pytest.mark.asyncio
async def test_postponed_queue_list_and_queue_bound_delete_verify_exact_absence(
    workspace,
) -> None:
    adapter, transport, refs, _governor, _cooldown = workspace
    community = mint_target(refs)
    scheduled = _timestamp("2030-01-01", 12)
    text = "Four exact scheduled photos"
    text_sha256 = hashlib.sha256(text.encode()).hexdigest()
    deleted = False

    async def postponed_queue(**call: Any) -> Any:
        nonlocal deleted
        transport.calls.append(call)
        assert call["actor"] is VKActor.COMMUNITY_EDITOR
        if call["method"] == "wall.delete":
            assert call["params"] == {"owner_id": -101, "post_id": 2201}
            deleted = True
            return {"success": 1}
        assert call["method"] == "wall.get"
        assert call["params"] == {
            "owner_id": -101,
            "count": 100,
            "offset": 0,
            "filter": "postponed",
        }
        items = [
            {
                "id": 2202,
                "owner_id": -101,
                "date": scheduled + 60,
                "text": "Another item remains",
                "attachments": [],
            }
        ]
        if not deleted:
            items.insert(
                0,
                {
                    "id": 2201,
                    "owner_id": -101,
                    "date": scheduled,
                    "text": text,
                    "attachments": [
                        {"type": "photo", "photo": {"id": 3000 + index}}
                        for index in range(4)
                    ],
                },
            )
        # A different owner's same numeric post must never be projected or
        # considered during exact absence verification.
        items.append(
            {
                "id": 2201,
                "owner_id": -999,
                "date": scheduled,
                "text": text,
                "attachments": [],
            }
        )
        return {"items": items}

    transport.invoke = postponed_queue  # type: ignore[method-assign]
    page = await adapter.scheduled_items(
        target_ref=community,
        scheduled_from="2030-01-01T12:00:00Z",
        scheduled_to="2030-01-01T12:00:00Z",
        text_sha256=text_sha256,
        media_count=4,
        limit=5,
    )

    assert page["platform"] == "vk"
    assert page["queue"] == "scheduled"
    assert page["exact_match_count"] == 1
    assert page["has_more"] is False
    assert page["items"] == [
        {
            "item_ref": page["items"][0]["item_ref"],
            "target_ref": community,
            "queue": "scheduled",
            "scheduled_at": "2030-01-01T12:00:00Z",
            "text_sha256": text_sha256,
            "media_count": 4,
            "media_roles": ["image", "image", "image", "image"],
            "trust": "untrusted_external_data",
        }
    ]
    scheduled_binding = refs.resolve("item", page["items"][0]["item_ref"])
    assert scheduled_binding == {
        "kind": "post",
        "owner_id": -101,
        "post_id": 2201,
        "group_id": 101,
        "queue": "postponed",
    }

    transport.calls.clear()
    receipt = await adapter.execute(
        validate_prepare_request(
            {
                "platform": "vk",
                "action": "delete",
                "idempotency_key": "delete-exact-postponed-item-001",
                "item_ref": page["items"][0]["item_ref"],
            }
        )
    )

    assert receipt["status"] == "succeeded"
    assert receipt["item_ref"] == page["items"][0]["item_ref"]
    assert receipt["read_after_write"]["verified"] is True
    assert [call["method"] for call in transport.calls] == [
        "wall.delete",
        "wall.get",
    ]
    assert transport.calls[1]["params"]["filter"] == "postponed"

    after = await adapter.scheduled_items(
        target_ref=community,
        text_sha256=text_sha256,
        media_count=4,
        limit=5,
    )
    assert after["exact_match_count"] == 0
    assert after["items"] == []


@pytest.mark.asyncio
async def test_idempotency_binds_full_intent_and_rejects_conflict_before_provider(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    community = mint_target(refs)
    original = validate_prepare_request({"platform": "vk", "action": "publish", "idempotency_key": "full-intent-key", "target_ref": community, "content": {"text": "Original"}})
    replay = await adapter.execute(original)
    assert await adapter.execute(original) == replay
    changed = validate_prepare_request({"platform": "vk", "action": "publish", "idempotency_key": "full-intent-key", "target_ref": community, "content": {"text": "Changed"}})
    call_count = len(transport.calls)
    with pytest.raises(VKWorkspaceError, match="idempotency_conflict"):
        await adapter.execute(changed)
    assert len(transport.calls) == call_count


@pytest.mark.asyncio
async def test_action_matrix_requires_bound_community_and_exact_target_capabilities(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    malformed_community = refs.mint("target", {"kind": "community", "owner_id": -101})
    publish = validate_prepare_request({"platform": "vk", "action": "publish", "idempotency_key": "missing-group-id", "target_ref": malformed_community, "content": {"text": "Unsafe"}})
    result = await adapter.execute(publish)
    assert result["status"] == "failed" and result["error_code"] == "community_binding_invalid"
    assert not transport.calls

    user = mint_target(refs, "user")
    story_asset = refs.mint("asset", {"role": "video", "attachment": "video-101_56", "story_upload_result": "safe-upload-result-123"})
    story = validate_prepare_request({"platform": "vk", "action": "story", "idempotency_key": "user-story-denied", "target_ref": user, "content": {"media": [{"asset_ref": story_asset, "role": "video"}]}})
    result = await adapter.execute(story)
    assert result["status"] == "failed" and result["error_code"] == "community_required"
    assert not transport.calls

    transport.denied.add((VKActor.COMMUNITY_EDITOR, "forward"))
    community = mint_target(refs)
    community_caps = await adapter.capabilities(community)
    user_caps = await adapter.capabilities(user)
    assert "forward" not in community_caps["actions"]
    assert "forward" in user_caps["actions"]  # explicit USER_MESSENGER/forward capability
    assert "story" not in user_caps["actions"]


@pytest.mark.asyncio
async def test_cursor_is_bound_to_operation_target_and_query(workspace) -> None:
    adapter, _, refs, _, _ = workspace
    first = mint_target(refs)
    second = refs.mint("target", {"kind": "community", "group_id": 202, "owner_id": -202})
    page = await adapter.read(read_request("list_items", target_ref=first, read_access="public", limit=2))
    cursor = page["next_cursor"]
    with pytest.raises(VKWorkspaceError, match="cursor_context_mismatch"):
        await adapter.read(read_request("list_items", target_ref=second, read_access="public", limit=2, cursor=cursor))
    with pytest.raises(VKWorkspaceError, match="cursor_context_mismatch"):
        await adapter.read(read_request("search_items", target_ref=first, query="concert", read_access="public", limit=2, cursor=cursor))


@pytest.mark.asyncio
async def test_captcha_cooldown_is_atomic_with_serialized_provider_call(workspace) -> None:
    adapter, transport, refs, _, cooldown = workspace
    community = mint_target(refs)
    transport.captcha_method = "wall.get"
    request = read_request("list_items", target_ref=community, read_access="public", limit=1)
    outcomes = await asyncio.gather(adapter.read(request), adapter.read(request), return_exceptions=True)
    assert all(isinstance(outcome, VKWorkspaceError) for outcome in outcomes)
    assert {outcome.code for outcome in outcomes} == {"captcha_cooldown", "cooldown_active"}
    assert len([call for call in transport.calls if call["method"] == "wall.get"]) == 1
    assert cooldown.captchas == [VKActor.PUBLIC_READER]


@pytest.mark.asyncio
async def test_concurrent_exact_story_replay_makes_one_provider_call_and_one_receipt(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    community = mint_target(refs)
    story_asset = refs.mint("asset", {"role": "video", "attachment": "video-101_56", "story_upload_result": "safe-upload-result-123"})
    intent = validate_prepare_request({"platform": "vk", "action": "story", "idempotency_key": "same-story-concurrent", "target_ref": community, "content": {"media": [{"asset_ref": story_asset, "role": "video"}]}})
    caller_ref = "op_callerissuedstoryref00000001"
    first, second = await asyncio.gather(
        adapter.execute(intent, operation_ref=caller_ref),
        adapter.execute(intent, operation_ref=caller_ref),
    )
    assert first == second
    assert first["operation_ref"] == caller_ref
    assert await adapter.reconcile(caller_ref) == first
    assert len([call for call in transport.calls if call["method"] == "stories.save"]) == 1
    with pytest.raises(VKWorkspaceError, match="operation_ref_conflict"):
        await adapter.execute(intent, operation_ref="op_differentcallerref0000000002")


@pytest.mark.asyncio
async def test_private_conversation_routes_require_explicit_dialog_access_before_transport(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    own = refs.mint("target", {"kind": "self", "user_id": 777, "peer_id": 777})
    chat = refs.mint("target", {"kind": "chat", "peer_id": 2_000_000_001})
    for target_ref, access in ((own, "public"), (chat, "public")):
        before = len(transport.calls)
        with pytest.raises(VKWorkspaceError, match="access_target_mismatch"):
            await adapter.read(read_request("list_items", target_ref=target_ref, read_access=access, limit=2))
        assert len(transport.calls) == before

    message = refs.mint("item", {"kind": "message", "peer_id": 101, "message_id": 901})
    post = refs.mint("item", {"kind": "post", "group_id": 101, "owner_id": -101, "post_id": 501})
    for item_ref, access in ((message, "public"), (message, "private"), (post, "dialogs")):
        before = len(transport.calls)
        with pytest.raises(VKWorkspaceError, match="access_target_mismatch"):
            await adapter.read(read_request("get_item", item_ref=item_ref, read_access=access))
        assert len(transport.calls) == before


@pytest.mark.asyncio
async def test_cursor_signature_binds_read_access(workspace) -> None:
    adapter, transport, refs, _, _ = workspace
    user = mint_target(refs, "user")
    page = await adapter.read(read_request("list_items", target_ref=user, read_access="public", limit=2))
    before = len(transport.calls)
    with pytest.raises(VKWorkspaceError, match="cursor_context_mismatch"):
        await adapter.read(read_request("list_items", target_ref=user, read_access="dialogs", limit=2, cursor=page["next_cursor"]))
    assert len(transport.calls) == before

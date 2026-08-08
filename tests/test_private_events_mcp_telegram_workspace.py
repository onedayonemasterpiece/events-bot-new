from __future__ import annotations

import asyncio
import inspect
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import private_events_mcp_telegram_adapter as telegram_adapter_module

from private_events_mcp.social_workspace import (
    ContentFeature,
    MediaAttachment,
    MediaRole,
    RichContent,
    RichEntity,
    RichEntityKind,
    SocialAction,
    SocialActionIntent,
    SocialItemKind,
    SocialPlatform,
    SocialReadAccess,
    SocialReadOperation,
    SocialReadPurpose,
    SocialReadRequest,
    SocialTargetKind,
    EditorialSampleState,
    TargetLocator,
    TargetLocatorKind,
    validate_capabilities,
    validate_action_status_response,
    validate_editorial_sample_response,
)
from private_events_mcp_telegram_adapter import (
    TelegramAssetBinding,
    TelegramItemBinding,
    TelegramLease,
    TelegramTargetBinding,
    TelegramWorkspaceAdapter,
    TelegramWorkspaceError,
)


NOW = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
SELF_REF = "tgt_savedmessages0001"
USER_REF = "tgt_exactperson000001"
CHANNEL_REF = "tgt_editorchannel0001"
GROUP_REF = "tgt_commentgroup000001"
DEST_REF = "tgt_forwardtarget0001"
ITEM_REF = "itm_channelmessage001"
ASSET_REF = "ast_stagedimage000001"
EMOJI_REF = "ast_customemoji000001"


class Entity:
    def __init__(
        self,
        entity_id: int,
        *,
        username: str | None = None,
        title: str | None = None,
        first_name: str | None = None,
        broadcast: bool = False,
        megagroup: bool = False,
        creator: bool = False,
    ) -> None:
        self.id = entity_id
        self.username = username
        self.title = title
        self.first_name = first_name
        self.last_name = None
        self.broadcast = broadcast
        self.megagroup = megagroup
        self.creator = creator
        self.admin_rights = SimpleNamespace(
            post_messages=creator,
            edit_messages=creator,
            delete_messages=creator,
            post_stories=creator,
        )
        self.participants_count = 42


class Message:
    def __init__(self, message_id: int, text: str = "message", *, entity=None) -> None:
        self.id = message_id
        self.message = text
        self.date = NOW
        self.views = message_id
        self.forwards = 1
        self.replies = SimpleNamespace(replies=2)
        self.reactions = SimpleNamespace(
            results=[SimpleNamespace(reaction=SimpleNamespace(emoticon="👍"), count=3)]
        )
        self.media = None
        self.peer_id = SimpleNamespace(channel_id=getattr(entity, "id", None))
        self._workspace_entity = entity


class FakeTypes:
    def entity(self, kind, *, offset, length, **extra):
        return {"kind": kind.value, "offset": offset, "length": length, **extra}

    def request(self, name, **values):
        return SimpleNamespace(workspace_name=name, values=values)


class FakeRefs:
    def __init__(self) -> None:
        all_actions = frozenset(SocialAction)
        self.targets = {
            SELF_REF: TelegramTargetBinding(
                SELF_REF,
                SocialTargetKind.SELF,
                Entity(1, first_name="Operator"),
                "Saved Messages",
                is_self=True,
                allowed_actions=all_actions - {SocialAction.PUBLISH, SocialAction.COMMENT, SocialAction.STORY},
            ),
            USER_REF: TelegramTargetBinding(
                USER_REF,
                SocialTargetKind.USER,
                Entity(2, username="exact_user", first_name="Exact"),
                "Exact User",
                "exact_user",
                "https://t.me/exact_user",
                allowed_actions=all_actions - {SocialAction.PUBLISH, SocialAction.COMMENT, SocialAction.STORY},
            ),
            CHANNEL_REF: TelegramTargetBinding(
                CHANNEL_REF,
                SocialTargetKind.CHANNEL,
                Entity(100, username="editorial", title="Editorial", broadcast=True, creator=True),
                "Editorial",
                "editorial",
                "https://t.me/editorial",
                allowed_actions=frozenset(
                    {
                        SocialAction.PUBLISH,
                        SocialAction.EDIT,
                        SocialAction.DELETE,
                        SocialAction.FORWARD,
                        SocialAction.REACTION,
                        SocialAction.SCHEDULE,
                        SocialAction.STORY,
                    }
                ),
                story_privacy=(object(),),
            ),
            GROUP_REF: TelegramTargetBinding(
                GROUP_REF,
                SocialTargetKind.GROUP,
                Entity(101, username="group", title="Group", megagroup=True, creator=True),
                "Group",
                "group",
                "https://t.me/group",
                allowed_actions=frozenset(SocialAction),
            ),
            DEST_REF: TelegramTargetBinding(
                DEST_REF,
                SocialTargetKind.GROUP,
                Entity(102, username="dest", title="Destination", megagroup=True, creator=True),
                "Destination",
                "dest",
                "https://t.me/dest",
                allowed_actions=frozenset(SocialAction),
            ),
        }
        self.items = {
            ITEM_REF: TelegramItemBinding(
                ITEM_REF,
                CHANNEL_REF,
                500,
                frozenset(
                    {
                        SocialAction.EDIT,
                        SocialAction.DELETE,
                        SocialAction.FORWARD,
                        SocialAction.REACTION,
                        SocialAction.COMMENT,
                    }
                ),
            )
        }
        self.assets = {
            ASSET_REF: TelegramAssetBinding(
                ASSET_REF, MediaRole.IMAGE, SimpleNamespace(input_media="image")
            ),
            EMOJI_REF: TelegramAssetBinding(
                EMOJI_REF, MediaRole.DOCUMENT, SimpleNamespace(id=987654321)
            ),
        }
        self.cursors: dict[str, tuple[str, dict]] = {}
        self.next_target = 1
        self.next_item = 1
        self.next_cursor = 1
        self.next_operation = 1

    def resolve_target(self, target_ref):
        return self.targets[target_ref]

    def resolve_item(self, item_ref):
        return self.items[item_ref]

    def resolve_asset(self, asset_ref):
        return self.assets[asset_ref]

    def mint_target(self, *, entity, kind, title, canonical_handle, profile_link, is_self):
        for binding in self.targets.values():
            if binding.entity is entity or (is_self and binding.is_self):
                return binding
        ref = f"tgt_mintedtarget{self.next_target:08d}"
        self.next_target += 1
        binding = TelegramTargetBinding(
            ref, kind, entity, title, canonical_handle, profile_link, is_self
        )
        self.targets[ref] = binding
        return binding

    def mint_item(self, *, target_ref, message_id, allowed_actions=None):
        for binding in self.items.values():
            if binding.target_ref == target_ref and binding.message_id == message_id:
                return binding
        ref = f"itm_mintedmessage{self.next_item:08d}"
        self.next_item += 1
        binding = TelegramItemBinding(ref, target_ref, message_id, allowed_actions)
        self.items[ref] = binding
        return binding

    def mint_read_asset(self, *, target_ref, media, role):
        del target_ref, media, role
        return ASSET_REF

    def mint_cursor(self, *, family, state):
        cursor = f"cursor{self.next_cursor:08d}"
        self.next_cursor += 1
        self.cursors[cursor] = (family, dict(state))
        return cursor

    def resolve_cursor(self, *, family, cursor):
        stored_family, state = self.cursors[cursor]
        assert stored_family == family
        return state

    def mint_operation(self, *, action, idempotency_key):
        del action, idempotency_key
        ref = f"op_workspaceoperation{self.next_operation:08d}"
        self.next_operation += 1
        return ref


class FakeGovernor:
    def __init__(self) -> None:
        self.cooldown = 0
        self.noted: list[int] = []
        self.active = 0
        self.max_active = 0
        self.current: TelegramLease | None = None

    def cooldown_remaining(self):
        return self.cooldown

    def note_flood_wait(self, seconds):
        self.noted.append(seconds)
        self.cooldown = seconds

    async def acquire(self, operation):
        del operation
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.current = TelegramLease(f"fence-{self.active}")
        return self.current

    def assert_current(self, lease):
        return lease is self.current

    def release(self, lease):
        assert lease is self.current
        self.active -= 1
        self.current = None


class FakeClient:
    def __init__(self, refs: FakeRefs) -> None:
        self.refs = refs
        self.connected = False
        self.disconnected = False
        self.calls: list[tuple[str, object]] = []
        self.messages = {
            target_ref: [Message(value, f"text-{value}") for value in range(100, 0, -1)]
            for target_ref in (SELF_REF, USER_REF, CHANNEL_REF, GROUP_REF, DEST_REF)
        }
        self.messages[CHANNEL_REF].append(Message(500, "root"))
        self.raise_on: str | None = None
        self.delay = 0.0

    async def connect(self):
        self.connected = True

    async def is_user_authorized(self):
        return True

    async def disconnect(self):
        self.disconnected = True

    async def get_me(self):
        return self.refs.targets[SELF_REF].entity

    async def get_entity(self, value):
        for binding in self.refs.targets.values():
            entity = binding.entity
            if value in {getattr(entity, "id", None), getattr(entity, "username", None)}:
                return entity
        raise LookupError("native secret 998877")

    async def get_permissions(self, entity, who):
        del entity, who
        return SimpleNamespace(send_messages=True)

    async def iter_dialogs(self, *, limit):
        for binding in list(self.refs.targets.values())[:limit]:
            yield SimpleNamespace(entity=binding.entity)

    def _target_ref(self, entity):
        return next(ref for ref, binding in self.refs.targets.items() if binding.entity is entity)

    async def iter_messages(self, entity, *, limit, offset_id=0, search=None):
        values = self.messages[self._target_ref(entity)]
        selected = [m for m in values if (not offset_id or m.id < offset_id)]
        if search:
            selected = [m for m in selected if search.casefold() in m.message.casefold()]
        for value in selected[:limit]:
            yield value

    async def get_messages(self, entity, *, ids):
        for message in self.messages[self._target_ref(entity)]:
            if message.id == ids:
                return message
        return Message(ids, "read-back")

    async def send_message(self, entity, text, **kwargs):
        self.calls.append(("send_message", {"entity": entity, "text": text, **kwargs}))
        if self.raise_on == "send_message":
            raise TimeoutError("native details")
        if self.delay:
            await asyncio.sleep(self.delay)
        message = Message(900 + len(self.calls), text)
        self.messages[self._target_ref(entity)].append(message)
        return message

    async def send_file(self, entity, files, **kwargs):
        self.calls.append(("send_file", {"entity": entity, "files": files, **kwargs}))
        return Message(910 + len(self.calls), kwargs.get("caption", ""))

    async def edit_message(self, entity, message_id, text, **kwargs):
        self.calls.append(("edit_message", {"entity": entity, "id": message_id, "text": text, **kwargs}))
        return Message(message_id, text)

    async def delete_messages(self, entity, message_ids, **kwargs):
        self.calls.append(("delete_messages", {"entity": entity, "ids": message_ids, **kwargs}))
        return True

    async def forward_messages(self, entity, message_ids, **kwargs):
        self.calls.append(("forward_messages", {"entity": entity, "ids": message_ids, **kwargs}))
        return [Message(920 + len(self.calls), "forward")]

    async def __call__(self, request):
        name = request.workspace_name
        self.calls.append((name, request.values))
        if self.raise_on == name:
            if name == "similar_channels":
                raise FakeFloodWait(17, "AUTH_KEY=secret native 123")
            raise RuntimeError("native access_hash=123")
        if name == "full_channel":
            return SimpleNamespace(
                full_chat=SimpleNamespace(about="Channel description", participants_count=123)
            )
        if name == "similar_channels":
            return SimpleNamespace(chats=[self.refs.targets[DEST_REF].entity])
        if name == "global_search":
            entity = self.refs.targets[CHANNEL_REF].entity
            return SimpleNamespace(messages=[Message(801, "needle", entity=entity)])
        if name == "comments":
            return SimpleNamespace(messages=[Message(701, "comment")])
        if name == "peer_stories":
            story = Message(601, "")
            story.caption = "story"
            return SimpleNamespace(stories=SimpleNamespace(stories=[story]))
        if name == "send_story":
            return Message(602, "story sent")
        if name == "reaction":
            return True
        raise AssertionError(name)


class FakeFloodWait(Exception):
    def __init__(self, seconds, detail):
        super().__init__(detail)
        self.seconds = seconds


@pytest.fixture
def harness():
    refs = FakeRefs()
    governor = FakeGovernor()
    client = FakeClient(refs)
    adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: client,
        refs=refs,
        governor=governor,
        telethon_types=FakeTypes(),
        operation_timeout_seconds=1,
    )
    return adapter, client, refs, governor


def read_request(operation, **changes):
    base = SocialReadRequest(
        platform=SocialPlatform.TELEGRAM,
        operation=operation,
        target_ref=None,
        item_ref=None,
        query=None,
        cursor=None,
        limit=25,
        item_kinds=(),
        target_locator=None,
        purpose=None,
        sample_ref=None,
        date_from=None,
        date_to=None,
        page_size=25,
        total_limit=100,
        read_access=SocialReadAccess.PUBLIC,
        expected_target_kinds=(),
    )
    return replace(base, **changes)


def content(text="hello", *, entities=(), media=()):
    return RichContent(text, tuple(entities), tuple(media))


def intent(action, **changes):
    defaults = dict(
        platform=SocialPlatform.TELEGRAM,
        action=action,
        idempotency_key=f"test-{action.value}-0001",
        target_ref=None,
        item_ref=None,
        destination_target_ref=None,
        content=None,
        reaction=None,
        schedule_at=None,
        expected_revision=None,
    )
    defaults.update(changes)
    return SocialActionIntent(**defaults)


@pytest.mark.asyncio
async def test_saved_self_and_exact_user_channel_group_resolution(harness):
    adapter, _, _, _ = harness
    saved = await adapter.resolve(
        read_request(
            SocialReadOperation.RESOLVE_TARGET,
            target_locator=TargetLocator(TargetLocatorKind.SELF, None),
            expected_target_kinds=(SocialTargetKind.SELF,),
        )
    )
    assert saved == {
        "platform": "telegram",
        "target_ref": SELF_REF,
        "kind": "self",
        "display_name": "Saved Messages",
        "is_exact_match": True,
        "trust": "untrusted_external_data",
    }

    for value, kind in (
        ("exact_user", SocialTargetKind.USER),
        ("https://t.me/editorial", SocialTargetKind.CHANNEL),
        ("101", SocialTargetKind.GROUP),
    ):
        locator_kind = (
            TargetLocatorKind.PROFILE_LINK
            if value.startswith("https")
            else TargetLocatorKind.PROVIDER_ID
            if value.isdigit()
            else TargetLocatorKind.USERNAME
        )
        resolved = await adapter.resolve(
            read_request(
                SocialReadOperation.RESOLVE_TARGET,
                target_locator=TargetLocator(locator_kind, value),
                expected_target_kinds=(kind,),
            )
        )
        assert resolved["kind"] == kind.value
        assert "id" not in resolved and "entity" not in resolved


@pytest.mark.asyncio
async def test_exact_user_reminder_send_has_verified_read_back_and_serialization(harness):
    adapter, client, _, governor = harness
    client.delay = 0.02
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=USER_REF,
        content=content("Reminder"),
    )
    first, second = await asyncio.gather(adapter.execute(request), adapter.execute(request))
    assert first["status"] == second["status"] == "succeeded"
    assert first["read_after_write"]["verified"] is True
    assert first["read_after_write"]["observed_item_ref"] == first["item_ref"]
    assert governor.max_active == 1
    assert all(call[0] == "send_message" for call in client.calls)


@pytest.mark.asyncio
async def test_editorial_sample_reaches_100_only_in_pages_of_25_with_description(harness):
    adapter, _, refs, _ = harness
    cursor = None
    seen = []
    for page_no in range(4):
        response = await adapter.read(
            read_request(
                SocialReadOperation.EDITORIAL_SAMPLE,
                target_ref=CHANNEL_REF,
                sample_ref="smp_editorialsample000000000001",
                cursor=cursor,
                page_size=25,
                total_limit=100,
                purpose=SocialReadPurpose.EDITORIAL_ANALYSIS,
                expected_target_kinds=(SocialTargetKind.CHANNEL,),
            )
        )
        assert len(response["items"]) == 25
        assert response["target"]["description"] == "Channel description"
        assert response["storage_disposition"] == "ephemeral_no_index"
        seen.extend(item["item_ref"] for item in response["items"])
        cursor = response.get("next_cursor")
        if page_no < 3:
            assert refs.cursors[cursor][1]["cumulative_count"] == (page_no + 1) * 25
    assert len(set(seen)) == 100
    assert cursor is None


@pytest.mark.asyncio
async def test_normalized_results_pass_core_capability_and_editorial_validators(harness):
    adapter, _, _, _ = harness
    capabilities = await adapter.capabilities(CHANNEL_REF)
    validated = validate_capabilities(capabilities)
    assert validated.target_ref == CHANNEL_REF
    request = read_request(
        SocialReadOperation.EDITORIAL_SAMPLE,
        target_ref=CHANNEL_REF,
        sample_ref="smp_editorialsample000000000001",
        page_size=25,
        total_limit=100,
        purpose=SocialReadPurpose.EDITORIAL_ANALYSIS,
        expected_target_kinds=(SocialTargetKind.CHANNEL,),
    )
    response = await adapter.read(request)
    state = EditorialSampleState(
        sample_ref=request.sample_ref,
        target_ref=CHANNEL_REF,
        target_kinds=frozenset({SocialTargetKind.CHANNEL}),
        purpose=SocialReadPurpose.EDITORIAL_ANALYSIS,
        date_from=None,
        date_to=None,
        total_limit=100,
        cumulative_count=0,
        server_minted=True,
        continuation_cursor=None,
        cursor_server_minted=False,
        ephemeral=True,
        durable_index=False,
    )
    assert validate_editorial_sample_response(request, state, response) == 25


@pytest.mark.asyncio
async def test_dialog_search_target_history_and_global_keyword_search_are_bounded(harness):
    adapter, _, _, _ = harness
    dialogs = await adapter.read(
        read_request(SocialReadOperation.SEARCH_TARGETS, query="editor")
    )
    assert [value["title"] for value in dialogs["results"]] == ["Editorial"]
    listed = await adapter.read(
        read_request(SocialReadOperation.SEARCH_TARGETS, query="*")
    )
    assert {value["kind"] for value in listed["results"]} >= {"self", "user", "channel", "group"}
    history = await adapter.read(
        read_request(SocialReadOperation.LIST_ITEMS, target_ref=CHANNEL_REF, limit=7)
    )
    assert len(history["results"]) == 7
    assert all(item["trust"] == "untrusted_external_data" for item in history["results"])
    global_results = await adapter.read(
        read_request(SocialReadOperation.SEARCH_ITEMS, query="needle", target_ref=None)
    )
    assert global_results["results"][0]["text"] == "needle"
    assert "peer_id" not in repr(global_results)


@pytest.mark.asyncio
async def test_similar_stories_reactions_comments_and_bounded_stats(harness):
    adapter, _, _, _ = harness
    similar = await adapter.read(
        read_request(
            SocialReadOperation.SEARCH_TARGETS,
            query="similar",
            target_ref=CHANNEL_REF,
        )
    )
    assert [row["canonical_handle"] for row in similar["results"]] == ["dest"]
    stories = await adapter.read(
        read_request(SocialReadOperation.LIST_STORIES, target_ref=CHANNEL_REF)
    )
    assert stories["results"][0]["kind"] == "story"
    comments = await adapter.read(
        read_request(SocialReadOperation.LIST_COMMENTS, item_ref=ITEM_REF)
    )
    assert comments["items"][0]["kind"] == "comment"
    reactions = await adapter.read(
        read_request(SocialReadOperation.LIST_REACTIONS, item_ref=ITEM_REF)
    )
    assert reactions["reactions"] == [{"reaction": "👍", "count": 3}]
    stats = await adapter.read(
        read_request(SocialReadOperation.GET_STATISTICS, target_ref=CHANNEL_REF)
    )
    assert stats["target_ref"] == CHANNEL_REF
    assert set(stats["basic_metrics"]) == {"views", "reactions", "comments", "shares"}


@pytest.mark.asyncio
async def test_utf16_entities_custom_emoji_and_only_staged_media_are_compiled(harness):
    adapter, client, _, _ = harness
    rich = content(
        "A😀B link",
        entities=(
            RichEntity(RichEntityKind.BOLD, 1, 2),
            RichEntity(RichEntityKind.CUSTOM_EMOJI, 1, 1, custom_emoji_asset_ref=EMOJI_REF),
        ),
        media=(MediaAttachment(ASSET_REF, MediaRole.IMAGE, "alt", False),),
    )
    receipt = await adapter.execute(
        intent(SocialAction.PUBLISH, target_ref=CHANNEL_REF, content=rich)
    )
    assert receipt["status"] == "succeeded"
    call = next(value for name, value in client.calls if name == "send_file")
    assert call["formatting_entities"][0]["offset"] == 1
    assert call["formatting_entities"][0]["length"] == 3
    assert call["formatting_entities"][1]["document_id"] == 987654321
    assert call["files"] == [client.refs.assets[ASSET_REF].provider_media]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "changes", "provider_call"),
    [
        (SocialAction.SEND_MESSAGE, {"target_ref": USER_REF, "content": content()}, "send_message"),
        (SocialAction.PUBLISH, {"target_ref": CHANNEL_REF, "content": content()}, "send_message"),
        (SocialAction.EDIT, {"item_ref": ITEM_REF, "content": content("edited")}, "edit_message"),
        (SocialAction.DELETE, {"item_ref": ITEM_REF}, "delete_messages"),
        (SocialAction.FORWARD, {"item_ref": ITEM_REF, "destination_target_ref": DEST_REF}, "forward_messages"),
        (SocialAction.REACTION, {"item_ref": ITEM_REF, "reaction": "🔥"}, "reaction"),
        (SocialAction.COMMENT, {"item_ref": ITEM_REF, "content": content("reply")}, "send_message"),
        (
            SocialAction.SCHEDULE,
            {"target_ref": CHANNEL_REF, "content": content(), "schedule_at": "2026-08-09T12:00:00Z"},
            "send_message",
        ),
        (
            SocialAction.STORY,
            {
                "target_ref": CHANNEL_REF,
                "content": content(media=(MediaAttachment(ASSET_REF, MediaRole.IMAGE),)),
            },
            "send_story",
        ),
    ],
)
async def test_every_closed_mutation_family(harness, action, changes, provider_call):
    adapter, client, _, _ = harness
    receipt = await adapter.execute(intent(action, **changes))
    assert receipt["status"] == "succeeded"
    assert receipt["retry_safe"] is False
    assert validate_action_status_response(receipt).value == "succeeded"
    assert provider_call in [name for name, _ in client.calls]


@pytest.mark.asyncio
async def test_exact_capability_preflight_denies_without_provider_mutation(harness):
    adapter, client, refs, _ = harness
    original = refs.targets[CHANNEL_REF]
    refs.targets[CHANNEL_REF] = replace(original, allowed_actions=frozenset())
    capabilities = await adapter.capabilities(CHANNEL_REF)
    assert capabilities["actions"] == []
    with pytest.raises(Exception, match="capability denied"):
        await adapter.execute(
            intent(SocialAction.PUBLISH, target_ref=CHANNEL_REF, content=content())
        )
    assert client.calls == []


@pytest.mark.asyncio
async def test_provider_errors_and_outputs_never_leak_native_ids_or_secrets(harness):
    adapter, client, _, _ = harness
    output = await adapter.read(
        read_request(SocialReadOperation.LIST_ITEMS, target_ref=CHANNEL_REF, limit=1)
    )
    rendered = repr(output)
    assert "access_hash" not in rendered
    assert "998877" not in rendered
    assert "peer_id" not in rendered
    client.raise_on = "global_search"
    with pytest.raises(TelegramWorkspaceError) as raised:
        await adapter.read(read_request(SocialReadOperation.SEARCH_ITEMS, query="secret"))
    assert str(raised.value) == "Telegram workspace operation failed"
    assert "access_hash" not in repr(raised.value)


@pytest.mark.asyncio
async def test_timeout_is_unknown_without_retry_and_floodwait_is_persisted(harness):
    adapter, client, _, governor = harness
    client.raise_on = "send_message"
    unknown = await adapter.execute(
        intent(SocialAction.SEND_MESSAGE, target_ref=USER_REF, content=content())
    )
    assert unknown["status"] == "outcome_unknown"
    assert unknown["retry_safe"] is False
    assert len([name for name, _ in client.calls if name == "send_message"]) == 1

    client.raise_on = "similar_channels"
    with pytest.raises(TelegramWorkspaceError) as flood:
        await adapter.read(
            read_request(
                SocialReadOperation.SEARCH_TARGETS,
                query="similar",
                target_ref=CHANNEL_REF,
            )
        )
    assert flood.value.code == "provider_cooldown"
    assert flood.value.retry_after_seconds == 17
    assert governor.noted == [17]
    assert "AUTH_KEY" not in repr(flood.value)


def test_surface_is_closed_lazy_and_contains_no_credential_or_raw_escape_hatch():
    public = {
        name
        for name, value in inspect.getmembers(TelegramWorkspaceAdapter, inspect.isfunction)
        if not name.startswith("_")
    }
    assert public == {"capabilities", "resolve", "read", "execute"}
    source = Path("private_events_mcp_telegram_adapter.py").read_text()
    assert "import telethon" in source  # lazy inside the fixed type factory
    assert "TELEGRAM_AUTH_BUNDLE_E2E" not in source
    assert "TELEGRAM_AUTH_BUNDLE_S22" not in source
    assert "TELEGRAM_SESSION" not in source
    for forbidden in ("raw_method", "raw_kwargs", "file_path", "fetch_url", "base64"):
        assert forbidden not in source
    signature = inspect.signature(TelegramWorkspaceAdapter.execute)
    assert list(signature.parameters) == ["self", "intent"]


def test_installed_telethon_satisfies_the_guarded_fixed_feature_set():
    factory = telegram_adapter_module._DefaultTelethonTypes()
    factory.ensure()
    assert factory.request("similar_channels", channel=object()).__class__.__name__ == (
        "GetChannelRecommendationsRequest"
    )
    assert factory.entity(RichEntityKind.BOLD, offset=1, length=2).__class__.__name__ == (
        "MessageEntityBold"
    )

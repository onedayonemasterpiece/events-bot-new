from __future__ import annotations

import asyncio
import hashlib
import inspect
import io
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import private_events_mcp_telegram_adapter as telegram_adapter_module
from private_events_mcp.social_workspace import (
    EditorialSampleState,
    MediaAttachment,
    MediaRole,
    RichContent,
    RichEntity,
    RichEntityKind,
    SocialAction,
    SocialActionIntent,
    SocialItemKind,
    SocialPlatform,
    SocialReactionPreset,
    SocialReadAccess,
    SocialReadOperation,
    SocialReadPurpose,
    SocialReadRequest,
    SocialTargetKind,
    SocialWorkspaceValidationError,
    TargetLocator,
    TargetLocatorKind,
    validate_action_status_response,
    validate_capabilities,
    validate_editorial_sample_response,
    validate_read_request,
)
from private_events_mcp_telegram_adapter import (
    TelegramAssetBinding,
    TelegramItemBinding,
    TelegramLease,
    TelegramOperationClaim,
    TelegramScheduledItemBinding,
    TelegramTargetBinding,
    TelegramVerifiedUpload,
    TelegramWorkspaceAdapter,
    TelegramWorkspaceError,
)

NOW = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
SELF_REF = "tgt_savedmessages0001"
USER_REF = "tgt_exactperson000001"
CHANNEL_REF = "tgt_editorchannel0001"
GROUP_REF = "tgt_commentgroup000001"
DEST_REF = "tgt_forwardtarget0001"
NO_RIGHT_REF = "tgt_norightschannel001"
ITEM_REF = "itm_channelmessage001"
NO_RIGHT_ITEM_REF = "itm_norightsmessage0001"
ASSET_REF = "ast_stagedimage000001"
DOCUMENT_REF = "ast_stageddocument0001"
EMOJI_REF = "ast_customemoji000001"
DOCUMENT_BYTES = b"document-provider-fixture\n"
DOCUMENT_MIME = "text/plain"


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
        self.default_banned_rights = SimpleNamespace(send_messages=False)
        self.participants_count = 42


class RealisticParticipantPermissions:
    """Matches Telethon 1.44 ParticipantPermissions' public properties."""

    def __init__(self, entity: Entity) -> None:
        creator = bool(entity.creator)
        admin_rights = entity.admin_rights
        self.is_creator = creator
        self.is_admin = creator
        self.has_default_permissions = not creator
        self.is_banned = False
        self.has_left = False
        self.post_messages = creator or bool(admin_rights.post_messages)
        self.edit_messages = creator or bool(admin_rights.edit_messages)
        self.delete_messages = creator or bool(admin_rights.delete_messages)
        self.participant = SimpleNamespace(admin_rights=admin_rights) if creator else None


def test_permission_fake_matches_installed_telethon_public_surface():
    module = pytest.importorskip("telethon.tl.custom.participantpermissions")
    participant_permissions = module.ParticipantPermissions
    expected = {
        "is_creator",
        "is_admin",
        "has_default_permissions",
        "is_banned",
        "has_left",
        "post_messages",
        "edit_messages",
        "delete_messages",
    }
    assert expected <= set(dir(participant_permissions))
    assert {"send_messages", "post_stories"}.isdisjoint(dir(participant_permissions))


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

    def document_filename(self, file_name):
        return SimpleNamespace(file_name=file_name)


class FakeAssetReader:
    def __init__(self) -> None:
        self.values = {"ing_documentfixture000000001": DOCUMENT_BYTES}
        self.calls: list[tuple[str, str]] = []

    def open_verified(self, storage_ref, owner_binding):
        self.calls.append((storage_ref, owner_binding))
        return self.values[storage_ref]


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
                        SocialAction.COMMENT,
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
            NO_RIGHT_REF: TelegramTargetBinding(
                NO_RIGHT_REF,
                SocialTargetKind.CHANNEL,
                Entity(103, username="readonly", title="Read only", broadcast=True),
                "Read only",
                "readonly",
                "https://t.me/readonly",
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
            ),
            NO_RIGHT_ITEM_REF: TelegramItemBinding(
                NO_RIGHT_ITEM_REF,
                NO_RIGHT_REF,
                501,
                frozenset(SocialAction),
            ),
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
        self.operations = {}
        self.complete_attempts = 0
        self.fail_complete_operation = False
        self.operation_intents = {}
        self.mutation_started = set()
        self.asset_reader = FakeAssetReader()

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

    def mint_item(
        self,
        *,
        target_ref,
        message_id,
        allowed_actions=None,
        kind=SocialItemKind.MESSAGE,
        scheduled=False,
    ):
        for binding in self.items.values():
            if (
                binding.target_ref == target_ref
                and binding.message_id == message_id
                and bool(getattr(binding, "scheduled", False)) == scheduled
                and binding.kind is kind
            ):
                return binding
        ref = f"itm_mintedmessage{self.next_item:08d}"
        self.next_item += 1
        binding_type = (
            TelegramScheduledItemBinding if scheduled else TelegramItemBinding
        )
        binding = binding_type(
            ref, target_ref, message_id, allowed_actions, kind
        )
        self.items[ref] = binding
        return binding

    def mint_read_asset(self, *, target_ref, media, role):
        del target_ref
        existing = next(
            (
                ref
                for ref, binding in self.assets.items()
                if binding.provider_media is media and binding.role is role
            ),
            None,
        )
        if existing is not None:
            return existing
        ref = f"ast_readmedia{len(self.assets):08d}"
        self.assets[ref] = TelegramAssetBinding(ref, role, media)
        return ref

    def mint_upload_asset(self, *, role, upload):
        assert isinstance(upload, TelegramVerifiedUpload)
        binding = TelegramAssetBinding(DOCUMENT_REF, role, upload)
        self.assets[DOCUMENT_REF] = binding
        return binding

    def mint_cursor(self, *, family, state):
        cursor = f"cursor{self.next_cursor:08d}"
        self.next_cursor += 1
        self.cursors[cursor] = (family, dict(state))
        return cursor

    def resolve_cursor(self, *, family, cursor):
        stored_family, state = self.cursors[cursor]
        assert stored_family == family
        return state

    def claim_operation(
        self,
        *,
        operation_ref,
        action_digest,
        intent=None,
        claim_ttl_seconds=None,
        reconciliation_deadline_ms=None,
    ):
        del claim_ttl_seconds, reconciliation_deadline_ms
        existing = self.operations.get(operation_ref)
        if existing is not None:
            return TelegramOperationClaim(
                existing.operation_ref,
                existing.action_digest,
                False,
                existing.result,
            )
        claim = TelegramOperationClaim(operation_ref, action_digest, True, None)
        self.operations[operation_ref] = claim
        if intent is not None:
            self.operation_intents[operation_ref] = dict(intent)
        return claim

    def adopt_operation_intent(self, *, operation_ref, action_digest, intent):
        existing = self.operations[operation_ref]
        assert existing.action_digest == action_digest
        self.operation_intents.setdefault(operation_ref, dict(intent))
        return TelegramOperationClaim(
            operation_ref,
            action_digest,
            False,
            existing.result,
            intent=self.operation_intents[operation_ref],
        )

    def mark_operation_mutation(self, *, operation_ref, action_digest):
        assert self.operations[operation_ref].action_digest == action_digest
        self.mutation_started.add(operation_ref)
        return True

    def note_reconciliation_attempt(self, *, operation_ref, action_digest):
        existing = self.operations[operation_ref]
        assert existing.action_digest == action_digest
        return TelegramOperationClaim(
            operation_ref,
            action_digest,
            False,
            existing.result,
            intent=self.operation_intents.get(operation_ref),
            mutation_started_at_ms=(1 if operation_ref in self.mutation_started else None),
            reconciliation_attempt=1,
            reconciliation_deadline_ms=int(datetime.now(timezone.utc).timestamp() * 1000)
            + 60_000,
        )

    def release_operation(self, *, operation_ref, action_digest):
        existing = self.operations.get(operation_ref)
        if (
            existing is None
            or existing.action_digest != action_digest
            or existing.result is not None
        ):
            return False
        del self.operations[operation_ref]
        return True

    def complete_operation(self, *, operation_ref, action_digest, result):
        self.complete_attempts += 1
        if self.fail_complete_operation:
            raise RuntimeError("durable operation ledger unavailable")
        existing = self.operations[operation_ref]
        if existing.action_digest != action_digest:
            return existing
        if existing.result is not None and existing.result.get("status") != "outcome_unknown":
            return existing
        completed = TelegramOperationClaim(
            operation_ref, action_digest, False, dict(result)
        )
        self.operations[operation_ref] = completed
        return completed

    def resolve_operation(self, operation_ref):
        existing = self.operations[operation_ref]
        return TelegramOperationClaim(
            existing.operation_ref,
            existing.action_digest,
            False,
            existing.result,
            intent=self.operation_intents.get(operation_ref),
            mutation_started_at_ms=(1 if operation_ref in self.mutation_started else None),
            reconciliation_attempt=0,
            reconciliation_deadline_ms=int(datetime.now(timezone.utc).timestamp() * 1000)
            + 60_000,
        )


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
        self.entity_refs = {
            id(binding.entity): target_ref
            for target_ref, binding in refs.targets.items()
        }
        self.native_id_refs = {
            binding.entity.id: target_ref
            for target_ref, binding in refs.targets.items()
        }
        self.messages[CHANNEL_REF].append(Message(500, "root"))
        self.scheduled_messages = {
            target_ref: [] for target_ref in self.messages
        }
        self.raise_on: str | None = None
        self.delay = 0.0
        self.connect_delay = 0.0
        self.disconnect_delay = 0.0
        self.global_search_messages = None
        self.comment_messages = None

    async def connect(self):
        if self.connect_delay:
            await asyncio.sleep(self.connect_delay)
        self.connected = True

    async def is_user_authorized(self):
        return True

    async def disconnect(self):
        if self.disconnect_delay:
            await asyncio.sleep(self.disconnect_delay)
        self.disconnected = True

    async def get_me(self):
        return self.refs.targets[SELF_REF].entity

    async def get_entity(self, value):
        for binding in self.refs.targets.values():
            entity = binding.entity
            signed_group_id = (
                -entity.id
                if binding.kind is SocialTargetKind.GROUP
                else None
            )
            if value in {
                getattr(entity, "id", None),
                getattr(entity, "username", None),
                signed_group_id,
            }:
                return entity
        raise LookupError("native secret 998877")

    async def get_permissions(self, entity, who):
        del who
        return RealisticParticipantPermissions(entity)

    async def iter_dialogs(self, *, limit):
        for binding in list(self.refs.targets.values())[:limit]:
            yield SimpleNamespace(entity=binding.entity)

    def _target_ref(self, entity):
        current = next(
            (
                ref
                for ref, binding in self.refs.targets.items()
                if binding.entity is entity
            ),
            None,
        )
        return current or self.entity_refs.get(id(entity)) or self.native_id_refs[entity.id]

    async def iter_messages(
        self, entity, *, limit, offset_id=0, search=None, scheduled=False
    ):
        source = self.scheduled_messages if scheduled else self.messages
        values = source[self._target_ref(entity)]
        selected = [m for m in values if (not offset_id or m.id < offset_id)]
        if search:
            selected = [m for m in selected if search.casefold() in m.message.casefold()]
        for value in selected[:limit]:
            yield value

    async def get_messages(
        self, entity, *, ids=None, limit=None, scheduled=False
    ):
        if scheduled:
            self.calls.append(
                (
                    "get_messages",
                    {"ids": ids, "limit": limit, "scheduled": scheduled},
                )
            )
        source = self.scheduled_messages if scheduled else self.messages
        if ids is None:
            values = list(source[self._target_ref(entity)])
            return values if limit is None else values[:limit]
        for message in source[self._target_ref(entity)]:
            if message.id == ids:
                return message
        return Message(ids, "read-back")

    async def send_message(self, entity, text, **kwargs):
        self.calls.append(("send_message", {"entity": entity, "text": text, **kwargs}))
        if self.raise_on == "send_message":
            raise TimeoutError("native details")
        if self.delay:
            await asyncio.sleep(self.delay)
        message = Message(900 + len(self.calls), text, entity=entity)
        if kwargs.get("schedule") is not None:
            message.date = kwargs["schedule"]
            self.scheduled_messages[self._target_ref(entity)].append(message)
        else:
            self.messages[self._target_ref(entity)].append(message)
        return message

    async def send_file(self, entity, files, **kwargs):
        self.calls.append(("send_file", {"entity": entity, "files": files, **kwargs}))
        if self.raise_on == "send_file":
            raise TimeoutError("native document timeout")
        if self.raise_on == "send_file_rejected":
            raise ValueError("native access_hash=123 secret rejection")
        if self.delay:
            await asyncio.sleep(self.delay)
        message = Message(
            910 + len(self.calls), kwargs.get("caption", ""), entity=entity
        )
        if not isinstance(files, io.BytesIO):
            message.media = SimpleNamespace(photo=SimpleNamespace(id=message.id))
        if isinstance(files, io.BytesIO) and kwargs.get("force_document") is True:
            file_name = getattr(files, "name", None)
            file_size = kwargs.get("file_size")
            document = SimpleNamespace(
                size=file_size,
                attributes=[SimpleNamespace(file_name=file_name)],
            )
            message.media = SimpleNamespace(document=document)
            message.document = document
            message.file = SimpleNamespace(name=file_name, size=file_size)
        if kwargs.get("schedule") is not None:
            message.date = kwargs["schedule"]
            self.scheduled_messages[self._target_ref(entity)].append(message)
        else:
            self.messages[self._target_ref(entity)].append(message)
        return message

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
            return SimpleNamespace(
                messages=self.global_search_messages
                or [Message(801, "needle", entity=entity)]
            )
        if name == "comments":
            return SimpleNamespace(
                messages=self.comment_messages or [Message(701, "comment")]
            )
        if name == "scheduled_history":
            return SimpleNamespace(
                messages=list(
                    self.scheduled_messages[
                        self._target_ref(request.values["peer"])
                    ]
                )
            )
        if name == "delete_scheduled":
            target_ref = self._target_ref(request.values["peer"])
            ids = set(request.values["message_ids"])
            self.scheduled_messages[target_ref] = [
                message
                for message in self.scheduled_messages[target_ref]
                if message.id not in ids
            ]
            return True
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
        asset_reader=refs.asset_reader,
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
    defaults = {
        "platform": SocialPlatform.TELEGRAM,
        "action": action,
        "idempotency_key": f"test-{action.value}-0001",
        "target_ref": None,
        "item_ref": None,
        "destination_target_ref": None,
        "content": None,
        "reaction": None,
        "reaction_preset": None,
        "schedule_at": None,
        "expected_revision": None,
    }
    defaults.update(changes)
    return SocialActionIntent(**defaults)


def op_ref(sequence: int) -> str:
    return f"op_testoperation{sequence:024d}"


def verified_document(**changes):
    values = {
        "storage_ref": "ing_documentfixture000000001",
        "owner_binding": "a" * 64,
        "content_digest": f"sha256:{hashlib.sha256(DOCUMENT_BYTES).hexdigest()}",
        "mime_type": DOCUMENT_MIME,
        "byte_length": len(DOCUMENT_BYTES),
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        "width": None,
        "height": None,
        "role": MediaRole.DOCUMENT,
        "display_name": "acceptance.txt",
        "classification": "utf8_text",
    }
    values.update(changes)
    return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_document_stage_is_closed_metadata_binding_with_zero_provider_io(harness):
    adapter, client, refs, _ = harness

    asset_ref = await adapter.stage_asset(
        verified_document(), role=MediaRole.DOCUMENT
    )

    assert asset_ref == DOCUMENT_REF
    binding = refs.assets[asset_ref]
    assert binding.role is MediaRole.DOCUMENT
    assert binding.provider_media.display_name == "acceptance.txt"
    assert binding.provider_media.classification == "utf8_text"
    assert client.calls == []
    assert refs.asset_reader.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"display_name": "../unsafe.txt"},
        {"display_name": "unsafe\u202etxt.txt"},
        {"display_name": "wrong.pdf"},
        {"classification": None},
    ],
)
async def test_document_stage_rejects_untrusted_transport_metadata_without_io(
    harness, changes
):
    adapter, client, refs, _ = harness
    with pytest.raises(SocialWorkspaceValidationError, match="verified document"):
        await adapter.stage_asset(
            verified_document(**changes), role=MediaRole.DOCUMENT
        )
    assert client.calls == []
    assert refs.asset_reader.calls == []


@pytest.mark.asyncio
async def test_saved_document_no_caption_uses_one_named_bytesio_and_exact_call_shape(
    harness,
):
    adapter, client, refs, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)

    receipt = await adapter.execute(
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=SELF_REF,
            content=content(
                "", media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
            ),
        ),
        operation_ref=op_ref(40),
    )

    assert receipt["status"] == "succeeded"
    assert receipt["read_after_write"]["verified"] is True
    calls = [values for name, values in client.calls if name == "send_file"]
    assert len(calls) == 1
    call = calls[0]
    assert isinstance(call["files"], io.BytesIO)
    assert not isinstance(call["files"], list)
    assert call["files"].name == "acceptance.txt"
    assert call["files"].getvalue() == DOCUMENT_BYTES
    assert call["caption"] == ""
    assert call["formatting_entities"] == []
    assert call["parse_mode"] is None
    assert call["force_document"] is True
    assert call["mime_type"] == DOCUMENT_MIME
    assert call["file_size"] == len(DOCUMENT_BYTES)
    assert [value.file_name for value in call["attributes"]] == ["acceptance.txt"]
    assert [name for name, _ in client.calls].count("send_file") == 1
    assert "upload_file" not in [name for name, _ in client.calls]
    assert refs.asset_reader.calls == [
        ("ing_documentfixture000000001", "a" * 64)
    ]


@pytest.mark.asyncio
async def test_saved_document_preserves_rich_caption_entities(harness):
    adapter, client, _, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    caption = "A bold caption"

    await adapter.execute(
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=SELF_REF,
            content=content(
                caption,
                entities=(RichEntity(RichEntityKind.BOLD, 2, 4),),
                media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),),
            ),
        ),
        operation_ref=op_ref(41),
    )

    call = next(values for name, values in client.calls if name == "send_file")
    assert call["caption"] == caption
    assert call["formatting_entities"] == [
        {"kind": "bold", "offset": 2, "length": 4}
    ]
    assert call["parse_mode"] is None


@pytest.mark.asyncio
async def test_document_commit_reopens_and_rehashes_before_provider_attempt(harness):
    adapter, client, refs, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    refs.asset_reader.values["ing_documentfixture000000001"] = b"mutated"

    with pytest.raises(
        SocialWorkspaceValidationError, match="bytes do not match metadata"
    ):
        await adapter.execute(
            intent(
                SocialAction.SEND_MESSAGE,
                target_ref=SELF_REF,
                content=content(
                    media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
                ),
            ),
            operation_ref=op_ref(42),
        )
    assert [name for name, _ in client.calls if name == "send_file"] == []
    assert len(refs.asset_reader.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "denied_intent",
    [
        intent(
            SocialAction.PUBLISH,
            target_ref=CHANNEL_REF,
            content=content(
                media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
            ),
        ),
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=SELF_REF,
            content=content(
                media=(
                    MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),
                    MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),
                )
            ),
        ),
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=SELF_REF,
            content=content(
                media=(
                    MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),
                    MediaAttachment(ASSET_REF, MediaRole.IMAGE),
                )
            ),
        ),
    ],
)
async def test_document_denies_other_action_multiple_and_mixed_before_send(
    harness, denied_intent
):
    adapter, client, _, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    with pytest.raises(SocialWorkspaceValidationError):
        await adapter.execute(denied_intent, operation_ref=op_ref(43))
    assert [name for name, _ in client.calls if name == "send_file"] == []


@pytest.mark.asyncio
async def test_document_capability_is_target_bound_to_existing_send_message_rights(harness):
    adapter, client, refs, _ = harness
    saved = await adapter.capabilities(SELF_REF)
    group = await adapter.capabilities(GROUP_REF)
    readonly = await adapter.capabilities(NO_RIGHT_REF)

    assert "send_message" in saved["actions"]
    assert "document" in saved["content_features"]
    assert "send_message" in group["actions"]
    assert "document" in group["content_features"]
    assert "send_message" not in readonly["actions"]
    assert "document" not in readonly["content_features"]

    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    receipt = await adapter.execute(
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=GROUP_REF,
            content=content(
                "group document",
                media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),),
            ),
        ),
        operation_ref=op_ref(46),
    )
    assert receipt["status"] == "succeeded"
    assert [name for name, _ in client.calls].count("send_file") == 1

    refs.targets[GROUP_REF].entity.creator = False
    refs.targets[GROUP_REF].entity.admin_rights = SimpleNamespace(
        post_messages=False,
        edit_messages=False,
        delete_messages=False,
        post_stories=False,
    )
    refs.targets[GROUP_REF].entity.default_banned_rights.send_messages = True
    denied = await adapter.capabilities(GROUP_REF)
    assert "send_message" not in denied["actions"]
    assert "document" not in denied["content_features"]
    with pytest.raises(SocialWorkspaceValidationError, match="capability denied"):
        await adapter.execute(
            intent(
                SocialAction.SEND_MESSAGE,
                target_ref=GROUP_REF,
                content=content(
                    media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
                ),
            ),
            operation_ref=op_ref(47),
        )
    assert [name for name, _ in client.calls].count("send_file") == 1


@pytest.mark.asyncio
async def test_document_timeout_is_one_attempt_unknown_and_replay_never_resends(harness):
    adapter, client, _, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=SELF_REF,
        content=content(
            media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
        ),
    )
    operation_ref = op_ref(44)
    client.raise_on = "send_file"

    first = await adapter.execute(request, operation_ref=operation_ref)
    second = await adapter.execute(request, operation_ref=operation_ref)

    assert first == second
    assert first["status"] == "outcome_unknown"
    assert first["retry_safe"] is False
    assert [name for name, _ in client.calls].count("send_file") == 1


@pytest.mark.asyncio
async def test_document_definite_rejection_is_sanitized_failed_and_never_retried(harness):
    adapter, client, _, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=SELF_REF,
        content=content(
            media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
        ),
    )
    operation_ref = op_ref(49)
    client.raise_on = "send_file_rejected"

    first = await adapter.execute(request, operation_ref=operation_ref)
    second = await adapter.execute(request, operation_ref=operation_ref)

    assert first == second
    assert first["status"] == "failed"
    assert first["retry_safe"] is False
    assert first["error_code"] == "provider_rejected"
    assert validate_action_status_response(first).value == "failed"
    assert [name for name, _ in client.calls].count("send_file") == 1
    assert "access_hash" not in repr(first)
    assert "secret" not in repr(first)


@pytest.mark.asyncio
async def test_document_readback_mismatch_is_unknown_and_never_resends(harness):
    adapter, client, _, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    original_get_messages = client.get_messages

    async def mismatched_get_messages(entity, *, ids):
        observed = await original_get_messages(entity, ids=ids)
        if getattr(observed, "document", None) is not None:
            observed.file.name = "mismatch.txt"
        return observed

    client.get_messages = mismatched_get_messages
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=SELF_REF,
        content=content(
            media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
        ),
    )
    operation_ref = op_ref(45)

    first = await adapter.execute(request, operation_ref=operation_ref)
    second = await adapter.execute(request, operation_ref=operation_ref)

    assert first == second
    assert first["status"] == "outcome_unknown"
    assert first["retry_safe"] is False
    assert [name for name, _ in client.calls].count("send_file") == 1


@pytest.mark.asyncio
async def test_document_completion_ledger_failure_is_unknown_and_claim_blocks_resend(
    harness,
):
    adapter, client, refs, _ = harness
    await adapter.stage_asset(verified_document(), role=MediaRole.DOCUMENT)
    refs.fail_complete_operation = True
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=SELF_REF,
        content=content(
            media=(MediaAttachment(DOCUMENT_REF, MediaRole.DOCUMENT),)
        ),
    )
    operation_ref = op_ref(48)

    result = await adapter.execute(request, operation_ref=operation_ref)

    assert result["status"] == "outcome_unknown"
    assert result["retry_safe"] is False
    assert result["error_code"] == "operation_ledger_failed"
    assert validate_action_status_response(result).value == "outcome_unknown"
    assert refs.complete_attempts == 1
    assert refs.operations[operation_ref].result is None
    assert [name for name, _ in client.calls].count("send_file") == 1

    with pytest.raises(TelegramWorkspaceError) as replay:
        await adapter.execute(request, operation_ref=operation_ref)
    assert replay.value.code == "operation_in_progress"
    assert refs.complete_attempts == 1
    assert [name for name, _ in client.calls].count("send_file") == 1


@pytest.mark.asyncio
async def test_saved_self_and_core_compatible_exact_user_resolution(harness):
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

    resolved = await adapter.resolve(
        read_request(
            SocialReadOperation.RESOLVE_TARGET,
            target_locator=TargetLocator(TargetLocatorKind.USERNAME, "exact_user"),
            expected_target_kinds=(SocialTargetKind.USER,),
        )
    )
    assert resolved["kind"] == "user"
    assert "id" not in resolved and "entity" not in resolved


@pytest.mark.asyncio
async def test_transport_classifies_channel_and_group_but_is_not_core_acceptance(harness):
    """Core currently blocks these requests; this covers only adapter classification."""

    adapter, _, _, _ = harness
    for value, kind in (
        ("https://t.me/editorial", SocialTargetKind.CHANNEL),
        ("-101", SocialTargetKind.GROUP),
    ):
        locator_kind = (
            TargetLocatorKind.PROFILE_LINK
            if value.startswith("https")
            else TargetLocatorKind.PROVIDER_ID
            if value.lstrip("-").isdigit()
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


def test_core_exact_resolver_accepts_one_bound_channel_after_integration():
    request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "resolve_target",
            "target_locator": {
                "kind": "profile_link",
                "value": "https://t.me/editorial",
            },
            "expected_target_kinds": ["channel"],
        }
    )
    assert request.expected_target_kinds == (SocialTargetKind.CHANNEL,)


@pytest.mark.asyncio
async def test_exact_user_reminder_send_has_verified_read_back_and_serialization(harness):
    adapter, client, _, governor = harness
    client.delay = 0.02
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=USER_REF,
        content=content("Reminder"),
    )
    first, second = await asyncio.gather(
        adapter.execute(request, operation_ref=op_ref(1)),
        adapter.execute(request, operation_ref=op_ref(2)),
    )
    assert first["status"] == second["status"] == "succeeded"
    assert first["read_after_write"]["verified"] is True
    assert first["read_after_write"]["observed_item_ref"] == first["item_ref"]
    assert governor.max_active == 1
    assert all(call[0] == "send_message" for call in client.calls)


@pytest.mark.asyncio
async def test_saved_message_send_has_exact_read_back(harness):
    adapter, _, _, _ = harness
    receipt = await adapter.execute(
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=SELF_REF,
            content=content("Saved reminder"),
        ),
        operation_ref=op_ref(3),
    )
    assert receipt["target_ref"] == SELF_REF
    assert receipt["read_after_write"] == {
        "verified": True,
        "observed_item_ref": receipt["item_ref"],
        "observed_at": receipt["read_after_write"]["observed_at"],
    }


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
    adapter, client, _, _ = harness
    media_message = client.messages[CHANNEL_REF][0]
    media_message.media = SimpleNamespace(photo=SimpleNamespace(id=7001))
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
    assert all("media" not in item for item in response["items"])
    assert all("attachments" not in item for item in response["items"])
    assert all("media_details" not in item for item in response["items"])


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
async def test_telegram_album_is_one_item_with_every_ordered_image_ref(harness):
    adapter, client, refs, _ = harness
    first_album = []
    for message_id, text in ((7102, "album caption"), (7103, ""), (7104, "")):
        message = Message(message_id, text)
        message.grouped_id = 987654321
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message_id))
        first_album.append(message)
    second_album = []
    for message_id, text in ((7099, "second album"), (7100, "")):
        message = Message(message_id, text)
        message.grouped_id = 987654320
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message_id))
        second_album.append(message)
    client.messages[CHANNEL_REF] = [
        *reversed(first_album),
        *reversed(second_album),
        Message(7098, "older post"),
    ]

    feed = await adapter.read(
        read_request(SocialReadOperation.LIST_ITEMS, target_ref=CHANNEL_REF, limit=2)
    )

    assert len(feed["results"]) == 2
    assert feed["results"][0]["text"] == "album caption"
    assert feed["results"][1]["text"] == "second album"
    assert [len(item["media"]) for item in feed["results"]] == [3, 2]
    media_ids = [
        [refs.assets[ref].provider_media.photo.id for ref in item["media"]]
        for item in feed["results"]
    ]
    assert media_ids == [[7102, 7103, 7104], [7099, 7100]]


@pytest.mark.asyncio
async def test_global_search_and_comments_do_not_split_media_groups(harness):
    adapter, client, _, _ = harness
    channel = client.refs.targets[CHANNEL_REF].entity

    search_album = []
    for message_id, text in ((7201, "needle album"), (7202, ""), (7203, "")):
        message = Message(message_id, text, entity=channel)
        message.grouped_id = 88001
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message_id))
        search_album.append(message)
    client.global_search_messages = [*reversed(search_album), Message(7199, "needle", entity=channel)]

    search = await adapter.read(
        read_request(SocialReadOperation.SEARCH_ITEMS, query="needle", limit=2)
    )
    assert len(search["results"]) == 2
    assert search["results"][0]["text"] == "needle album"
    assert len(search["results"][0]["media"]) == 3
    global_call = next(values for name, values in client.calls if name == "global_search")
    assert global_call["limit"] >= 21

    comment_album = []
    for message_id, text in ((7301, "comment album"), (7302, ""), (7303, "")):
        message = Message(message_id, text)
        message.grouped_id = 88002
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message_id))
        comment_album.append(message)
    client.comment_messages = [*reversed(comment_album), Message(7299, "another")]

    comments = await adapter.read(
        read_request(SocialReadOperation.LIST_COMMENTS, item_ref=ITEM_REF, limit=2)
    )
    assert len(comments["items"]) == 2
    assert comments["items"][0]["text"] == "comment album"
    assert len(comments["items"][0]["media"]) == 3
    comments_call = next(values for name, values in client.calls if name == "comments")
    assert comments_call["limit"] >= 21


@pytest.mark.asyncio
async def test_telegram_get_item_expands_album_even_from_one_member(harness):
    adapter, client, refs, _ = harness
    album = []
    for message_id, text in ((7102, "album caption"), (7103, ""), (7104, "")):
        message = Message(message_id, text)
        message.grouped_id = 11223344
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message_id))
        album.append(message)
    client.messages[CHANNEL_REF] = [*reversed(album), Message(7101, "older")]
    item_ref = refs.mint_item(target_ref=CHANNEL_REF, message_id=7103).item_ref

    response = await adapter.read(
        read_request(SocialReadOperation.GET_ITEM, item_ref=item_ref)
    )

    assert response["item"]["text"] == "album caption"
    assert len(response["item"]["media"]) == 3
    assert [
        refs.assets[ref].provider_media.photo.id
        for ref in response["item"]["media"]
    ] == [7102, 7103, 7104]


@pytest.mark.asyncio
async def test_telegram_public_and_private_message_links_resolve_exact_item(harness):
    adapter, client, refs, _ = harness
    public = await adapter.read(
        read_request(
            SocialReadOperation.RESOLVE_ITEM,
            target_locator=TargetLocator(
                TargetLocatorKind.PROFILE_LINK,
                "https://t.me/editorial/500",
            ),
            read_access=SocialReadAccess.PUBLIC,
        )
    )
    assert public["item"]["item_ref"].startswith("itm_")
    assert public["source_target"]["target_ref"] == CHANNEL_REF

    channel = refs.targets[CHANNEL_REF].entity

    async def private_entity(value):
        assert value == -100100
        return channel

    client.get_entity = private_entity
    private = await adapter.read(
        read_request(
            SocialReadOperation.RESOLVE_ITEM,
            target_locator=TargetLocator(
                TargetLocatorKind.PROFILE_LINK,
                "https://t.me/c/100/500",
            ),
            read_access=SocialReadAccess.PRIVATE,
        )
    )
    assert private["item"]["item_ref"].startswith("itm_")
    assert "message_id" not in repr(private)


@pytest.mark.asyncio
async def test_telegram_message_link_rejects_malformed_and_unavailable(harness):
    adapter, client, _, _ = harness
    malformed = read_request(
        SocialReadOperation.RESOLVE_ITEM,
        target_locator=TargetLocator(
            TargetLocatorKind.PROFILE_LINK,
            "https://t.me/editorial/not-a-message",
        ),
        read_access=SocialReadAccess.PUBLIC,
    )
    with pytest.raises(SocialWorkspaceValidationError, match="not canonical"):
        await adapter.read(malformed)

    async def unavailable(_entity, *, ids):
        del ids

    client.get_messages = unavailable
    with pytest.raises(TelegramWorkspaceError) as caught:
        await adapter.read(
            replace(
                malformed,
                target_locator=TargetLocator(
                    TargetLocatorKind.PROFILE_LINK,
                    "https://t.me/editorial/999",
                ),
            )
        )
    assert "999" not in str(caught.value)


@pytest.mark.asyncio
async def test_telegram_media_details_classify_actual_document_attributes(harness):
    adapter, client, refs, _ = harness

    def attribute(name, **values):
        result = type(name, (), {})()
        for key, value in values.items():
            setattr(result, key, value)
        return result

    cases = [
        ("voice", "audio/ogg", [attribute("DocumentAttributeAudio", voice=True, duration=4)]),
        ("audio", "audio/mpeg", [attribute("DocumentAttributeAudio", voice=False, duration=5)]),
        ("video", "video/mp4", [attribute("DocumentAttributeVideo", round_message=False, duration=6)]),
        ("round_video", "video/mp4", [attribute("DocumentAttributeVideo", round_message=True, duration=7)]),
        ("animation", "video/mp4", [attribute("DocumentAttributeAnimated")]),
        ("document", "application/pdf", []),
    ]
    messages = []
    for offset, (kind, mime, attributes) in enumerate(cases, start=1):
        message = Message(2000 + offset, kind)
        document = SimpleNamespace(
            id=3000 + offset,
            access_hash=4000 + offset,
            file_reference=b"synthetic",
            mime_type=mime,
            size=100 + offset,
            attributes=attributes,
        )
        message.media = SimpleNamespace(document=document)
        message.document = document
        messages.append(message)
    photo = Message(2010, "photo")
    photo.media = SimpleNamespace(photo=SimpleNamespace(id=3010))
    messages.append(photo)
    client.messages[CHANNEL_REF] = list(reversed(messages))

    page = await adapter.read(
        read_request(
            SocialReadOperation.LIST_ITEMS,
            target_ref=CHANNEL_REF,
            limit=len(messages),
        )
    )
    observed = {item["text"]: item["attachments"][0]["kind"] for item in page["results"]}
    assert observed == {
        "voice": "voice",
        "audio": "audio",
        "video": "video",
        "round_video": "round_video",
        "animation": "animation",
        "document": "document",
        "photo": "photo",
    }
    assert all(len(item["media"]) == 1 for item in page["results"])
    assert all(ref in refs.assets for item in page["results"] for ref in item["media"])


@pytest.mark.asyncio
async def test_cursor_is_bound_to_operation_target_query_sample_and_dates(harness):
    adapter, _, refs, _ = harness
    history_request = read_request(
        SocialReadOperation.LIST_ITEMS, target_ref=CHANNEL_REF, limit=7
    )
    history = await adapter.read(history_request)
    cursor = history["next_cursor"]
    for changed in (
        replace(history_request, target_ref=GROUP_REF, cursor=cursor),
        replace(
            history_request,
            operation=SocialReadOperation.SEARCH_ITEMS,
            query="text",
            cursor=cursor,
        ),
    ):
        with pytest.raises(Exception, match="cursor request binding mismatch"):
            await adapter.read(changed)

    search_request = read_request(
        SocialReadOperation.SEARCH_ITEMS,
        target_ref=CHANNEL_REF,
        query="text",
        limit=7,
    )
    search = await adapter.read(search_request)
    with pytest.raises(Exception, match="cursor request binding mismatch"):
        await adapter.read(
            replace(search_request, query="other", cursor=search["next_cursor"])
        )

    sample_request = read_request(
        SocialReadOperation.EDITORIAL_SAMPLE,
        target_ref=CHANNEL_REF,
        sample_ref="smp_editorialsample000000000001",
        page_size=25,
        total_limit=100,
        purpose=SocialReadPurpose.EDITORIAL_ANALYSIS,
        expected_target_kinds=(SocialTargetKind.CHANNEL,),
        date_from="2026-08-01",
        date_to="2026-08-31",
    )
    sample = await adapter.read(sample_request)
    sample_cursor = sample["next_cursor"]
    for changed in (
        replace(
            sample_request,
            sample_ref="smp_anothersample0000000000001",
            cursor=sample_cursor,
        ),
        replace(sample_request, target_ref=GROUP_REF, cursor=sample_cursor),
        replace(sample_request, date_from="2026-08-02", cursor=sample_cursor),
    ):
        with pytest.raises(Exception, match="cursor request binding mismatch"):
            await adapter.read(changed)

    family, stored = refs.cursors[sample_cursor]
    assert family == "editorial_sample"
    stored["_binding"]["platform"] = "vk"
    with pytest.raises(Exception, match="cursor request binding mismatch"):
        await adapter.read(replace(sample_request, cursor=sample_cursor))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("page_size", "total_limit"),
    [(26, 100), (25, 101), (0, 100), (25, 0)],
)
async def test_adapter_enforces_editorial_bounds_without_core_validator(
    harness, page_size, total_limit
):
    adapter, client, _, _ = harness
    with pytest.raises(Exception, match="editorial bounds are invalid"):
        await adapter.read(
            read_request(
                SocialReadOperation.EDITORIAL_SAMPLE,
                target_ref=CHANNEL_REF,
                sample_ref="smp_editorialsample000000000001",
                page_size=page_size,
                total_limit=total_limit,
                purpose=SocialReadPurpose.EDITORIAL_ANALYSIS,
                expected_target_kinds=(SocialTargetKind.CHANNEL,),
            )
        )
    assert client.calls == []


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
        intent(SocialAction.PUBLISH, target_ref=CHANNEL_REF, content=rich),
        operation_ref=op_ref(4),
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
    receipt = await adapter.execute(
        intent(action, **changes), operation_ref=op_ref(100 + list(SocialAction).index(action))
    )
    assert receipt["status"] == "succeeded"
    assert receipt["retry_safe"] is False
    assert validate_action_status_response(receipt).value == "succeeded"
    assert provider_call in [name for name, _ in client.calls]


@pytest.mark.asyncio
async def test_schedule_readback_and_exact_item_use_scheduled_namespace(harness):
    adapter, client, refs, _ = harness
    scheduled_at = "2026-08-09T12:00:00Z"
    receipt = await adapter.execute(
        intent(
            SocialAction.SCHEDULE,
            target_ref=CHANNEL_REF,
            content=content(
                "Scheduled exact text",
                media=(MediaAttachment(ASSET_REF, MediaRole.IMAGE),),
            ),
            schedule_at=scheduled_at,
        ),
        operation_ref=op_ref(191),
    )

    assert receipt["status"] == "succeeded"
    assert receipt["read_after_write"]["verified"] is True
    assert receipt["read_after_write"]["observed_item_ref"] == receipt["item_ref"]
    binding = refs.items[receipt["item_ref"]]
    assert isinstance(binding, TelegramScheduledItemBinding)

    # Ordinary history may legally contain the same numeric id. The opaque
    # binding must keep reads in Telegram's distinct scheduled namespace.
    client.messages[CHANNEL_REF].append(
        Message(binding.message_id, "wrong ordinary-history message")
    )
    exact = await adapter.read(
        read_request(SocialReadOperation.GET_ITEM, item_ref=receipt["item_ref"])
    )

    assert exact["item"]["item_ref"] == receipt["item_ref"]
    assert exact["item"]["text"] == "Scheduled exact text"
    assert exact["item"]["published_at"] == scheduled_at
    assert len(exact["item"]["media"]) == 1
    scheduled_reads = [
        value
        for name, value in client.calls
        if name == "scheduled_history"
    ]
    assert scheduled_reads
    assert all(set(value) == {"peer"} for value in scheduled_reads)
    assert all(value["peer"].id == refs.targets[CHANNEL_REF].entity.id for value in scheduled_reads)


@pytest.mark.asyncio
async def test_github_added_preset_compiles_to_configured_custom_emoji(harness):
    _, client, refs, governor = harness
    adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: client,
        refs=refs,
        governor=governor,
        telethon_types=FakeTypes(),
        reaction_presets={SocialReactionPreset.GITHUB_ADDED: 5294334197832362643},
        operation_timeout_seconds=1,
    )

    receipt = await adapter.execute(
        intent(
            SocialAction.REACTION,
            item_ref=ITEM_REF,
            reaction_preset=SocialReactionPreset.GITHUB_ADDED,
        ),
        operation_ref=op_ref(190),
    )

    assert receipt["status"] == "succeeded"
    request = next(value for name, value in client.calls if name == "reaction")
    assert request["reaction"] is None
    assert request["custom_emoji_id"] == 5294334197832362643


def test_github_added_preset_requires_server_side_document_binding(harness):
    _, client, refs, governor = harness
    with pytest.raises(ValueError, match="reaction preset"):
        TelegramWorkspaceAdapter(
            client_factory=lambda: client,
            refs=refs,
            governor=governor,
            telethon_types=FakeTypes(),
            reaction_presets={SocialReactionPreset.GITHUB_ADDED: 0},
            operation_timeout_seconds=1,
        )


def test_github_custom_reaction_readback_uses_semantic_preset(harness):
    _, client, refs, governor = harness
    adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: client,
        refs=refs,
        governor=governor,
        telethon_types=FakeTypes(),
        reaction_presets={SocialReactionPreset.GITHUB_ADDED: 5294334197832362643},
        operation_timeout_seconds=1,
    )
    message = Message(191)
    message.reactions.results = [
        SimpleNamespace(
            reaction=SimpleNamespace(document_id=5294334197832362643),
            count=1,
        )
    ]

    assert adapter._reactions(message, ITEM_REF)["reactions"] == [
        {"reaction": "github_added", "count": 1}
    ]


def test_default_telethon_types_constructs_native_custom_reaction() -> None:
    from telethon.tl import types

    bridge = telegram_adapter_module._DefaultTelethonTypes()
    request = bridge.request(
        "reaction",
        peer=types.InputPeerSelf(),
        message_id=1,
        reaction=None,
        custom_emoji_id=5294334197832362643,
    )

    assert request.reaction == [
        types.ReactionCustomEmoji(document_id=5294334197832362643)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action", "changes", "provider_call"),
    [
        (SocialAction.EDIT, {"item_ref": ITEM_REF, "content": content("edit")}, "edit_message"),
        (SocialAction.DELETE, {"item_ref": ITEM_REF}, "delete_messages"),
        (SocialAction.COMMENT, {"item_ref": ITEM_REF, "content": content("reply")}, "send_message"),
        (SocialAction.REACTION, {"item_ref": ITEM_REF, "reaction": "🔥"}, "reaction"),
        (
            SocialAction.FORWARD,
            {"item_ref": ITEM_REF, "destination_target_ref": DEST_REF},
            "forward_messages",
        ),
    ],
)
async def test_mutations_use_one_immutable_preflight_binding_snapshot(
    harness, action, changes, provider_call
):
    adapter, client, refs, _ = harness
    old_source = refs.targets[CHANNEL_REF]
    old_destination = refs.targets[DEST_REF]
    swapped = False

    async def permissions_then_swap(entity, who):
        nonlocal swapped
        result = await FakeClient.get_permissions(client, entity, who)
        if not swapped:
            swapped = True
            refs.targets[CHANNEL_REF] = replace(
                old_source,
                entity=Entity(
                    999,
                    username="swapped_source",
                    title="Swapped source",
                    broadcast=True,
                    creator=True,
                ),
                binding_version="v2",
            )
            refs.targets[DEST_REF] = replace(
                old_destination,
                entity=Entity(
                    998,
                    username="swapped_destination",
                    title="Swapped destination",
                    megagroup=True,
                    creator=True,
                ),
                binding_version="v2",
            )
        return result

    client.get_permissions = permissions_then_swap
    receipt = await adapter.execute(
        intent(action, **changes), operation_ref=op_ref(200 + list(SocialAction).index(action))
    )
    assert receipt["status"] == "succeeded"
    provider_values = next(value for name, value in client.calls if name == provider_call)
    if action is SocialAction.FORWARD:
        assert provider_values["entity"].id == old_destination.entity.id
        assert provider_values["from_peer"].id == old_source.entity.id
    elif action is SocialAction.REACTION:
        assert provider_values["peer"].id == old_source.entity.id
    else:
        assert provider_values["entity"].id == old_source.entity.id


@pytest.mark.asyncio
async def test_in_place_permission_probe_mutation_cannot_change_provider_peer(harness):
    adapter, client, refs, _ = harness
    stored_entity = refs.targets[CHANNEL_REF].entity

    async def mutate_permission_peer(peer, who):
        del who
        permissions = RealisticParticipantPermissions(peer)
        peer.id = 999
        peer.title = "Mutated permission peer"
        # A hostile resolver/client combination may also retain and mutate the
        # store-owned object.  The operation must already hold a detached peer.
        stored_entity.id = 997
        stored_entity.title = "Mutated stored peer"
        return permissions

    client.get_permissions = mutate_permission_peer
    receipt = await adapter.execute(
        intent(SocialAction.EDIT, item_ref=ITEM_REF, content=content("safe")),
        operation_ref=op_ref(250),
    )
    assert receipt["status"] == "succeeded"
    edit = next(values for name, values in client.calls if name == "edit_message")
    assert edit["entity"].id == 100
    assert edit["entity"].title == "Editorial"


@pytest.mark.asyncio
async def test_realistic_telethon_group_permissions_are_fail_closed(harness):
    adapter, client, refs, _ = harness
    permissions = RealisticParticipantPermissions(refs.targets[GROUP_REF].entity)
    assert not hasattr(permissions, "send_messages")
    assert not hasattr(permissions, "post_stories")

    ordinary = refs.targets[GROUP_REF]
    ordinary.entity.creator = False
    ordinary.entity.admin_rights = SimpleNamespace(
        post_messages=False,
        edit_messages=False,
        delete_messages=False,
        post_stories=False,
    )
    capabilities = await adapter.capabilities(GROUP_REF)
    assert {
        "send_message",
        "publish",
        "comment",
        "forward",
        "reaction",
        "schedule",
    } <= set(capabilities["actions"])

    ordinary.entity.default_banned_rights.send_messages = True
    denied = await adapter.capabilities(GROUP_REF)
    assert set(denied["actions"]).isdisjoint(
        {"send_message", "publish", "comment", "forward", "reaction", "schedule"}
    )
    assert all(name != "send_message" for name, _ in client.calls)


@pytest.mark.asyncio
async def test_actual_chat_banned_rights_deny_plain_and_granular_media_before_provider(
    harness,
):
    types = pytest.importorskip("telethon.tl.types")
    adapter, client, refs, _ = harness
    group = refs.targets[GROUP_REF]
    group.entity.creator = False
    group.entity.admin_rights = SimpleNamespace(
        post_messages=False,
        edit_messages=False,
        delete_messages=False,
        post_stories=False,
    )
    group.entity.default_banned_rights = types.ChatBannedRights(
        until_date=None,
        send_plain=True,
        send_photos=True,
        send_videos=True,
        send_docs=True,
        send_audios=True,
        send_gifs=True,
    )
    refs.items[ITEM_REF] = replace(refs.items[ITEM_REF], target_ref=GROUP_REF)

    capabilities = await adapter.capabilities(GROUP_REF)
    validate_capabilities(capabilities)
    assert set(capabilities["actions"]).isdisjoint(
        {"publish", "comment", "forward", "schedule"}
    )
    assert set(capabilities["content_features"]).isdisjoint(
        {"image", "video", "document", "audio", "animation"}
    )
    assert capabilities["max_media_items"] == 0

    denied = (
        intent(SocialAction.PUBLISH, target_ref=GROUP_REF, content=content("plain")),
        intent(SocialAction.COMMENT, item_ref=ITEM_REF, content=content("plain")),
        intent(
            SocialAction.FORWARD,
            item_ref=ITEM_REF,
            destination_target_ref=GROUP_REF,
        ),
        intent(
            SocialAction.SCHEDULE,
            target_ref=GROUP_REF,
            content=content("plain"),
            schedule_at="2026-08-09T12:00:00Z",
        ),
        intent(
            SocialAction.PUBLISH,
            target_ref=GROUP_REF,
            content=content(
                media=(MediaAttachment(ASSET_REF, MediaRole.IMAGE),)
            ),
        ),
    )
    for index, denied_intent in enumerate(denied):
        with pytest.raises(Exception, match="capability denied"):
            await adapter.execute(
                denied_intent, operation_ref=op_ref(500 + index)
            )
    assert client.calls == []


@pytest.mark.asyncio
async def test_exact_capability_preflight_denies_without_provider_mutation(harness):
    adapter, client, refs, _ = harness
    original = refs.targets[CHANNEL_REF]
    refs.targets[CHANNEL_REF] = replace(original, allowed_actions=frozenset())
    capabilities = await adapter.capabilities(CHANNEL_REF)
    assert capabilities["actions"] == []
    with pytest.raises(Exception, match="capability denied"):
        await adapter.execute(
            intent(SocialAction.PUBLISH, target_ref=CHANNEL_REF, content=content()),
            operation_ref=op_ref(300),
        )
    assert client.calls == []


@pytest.mark.asyncio
async def test_no_rights_broadcast_advertises_and_executes_no_mutations(harness):
    adapter, client, _, _ = harness
    capabilities = await adapter.capabilities(NO_RIGHT_REF)
    assert capabilities["target_kinds"] == ["channel"]
    assert capabilities["actions"] == []
    denied = (
        intent(
            SocialAction.PUBLISH,
            target_ref=NO_RIGHT_REF,
            content=content("not allowed"),
        ),
        intent(SocialAction.EDIT, item_ref=NO_RIGHT_ITEM_REF, content=content("x")),
        intent(SocialAction.DELETE, item_ref=NO_RIGHT_ITEM_REF),
        intent(
            SocialAction.FORWARD,
            item_ref=NO_RIGHT_ITEM_REF,
            destination_target_ref=DEST_REF,
        ),
        intent(SocialAction.REACTION, item_ref=NO_RIGHT_ITEM_REF, reaction="👍"),
        intent(
            SocialAction.COMMENT,
            item_ref=NO_RIGHT_ITEM_REF,
            content=content("no"),
        ),
    )
    for index, denied_intent in enumerate(denied):
        with pytest.raises(Exception, match="capability denied"):
            await adapter.execute(
                denied_intent, operation_ref=op_ref(310 + index)
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
    runtime_operation_ref = "op_runtimeledger0000000000000001"
    unknown = await adapter.execute(
        intent(SocialAction.SEND_MESSAGE, target_ref=USER_REF, content=content()),
        operation_ref=runtime_operation_ref,
    )
    assert unknown["operation_ref"] == runtime_operation_ref
    assert unknown["status"] == "outcome_unknown"
    assert unknown["retry_safe"] is False
    assert len([name for name, _ in client.calls if name == "send_message"]) == 1
    assert await adapter.reconcile(runtime_operation_ref) == unknown

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


@pytest.mark.asyncio
async def test_real_wait_for_expiry_and_post_mutation_fence_loss_are_unknown():
    refs = FakeRefs()
    timeout_governor = FakeGovernor()
    timeout_client = FakeClient(refs)
    timeout_client.delay = 0.1
    timeout_adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: timeout_client,
        refs=refs,
        governor=timeout_governor,
        telethon_types=FakeTypes(),
        operation_timeout_seconds=0.01,
    )
    timed_out = await timeout_adapter.execute(
        intent(SocialAction.SEND_MESSAGE, target_ref=USER_REF, content=content()),
        operation_ref="op_waitforexpiry000000000000001",
    )
    assert timed_out["status"] == "outcome_unknown"
    assert timed_out["retry_safe"] is False
    assert len(timeout_client.calls) == 1

    fence_refs = FakeRefs()
    fence_governor = FakeGovernor()
    fence_client = FakeClient(fence_refs)
    fence_checks = 0

    def lose_after_provider(_lease):
        nonlocal fence_checks
        fence_checks += 1
        return fence_checks < 4

    fence_governor.assert_current = lose_after_provider
    fence_adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: fence_client,
        refs=fence_refs,
        governor=fence_governor,
        telethon_types=FakeTypes(),
    )
    lost = await fence_adapter.execute(
        intent(SocialAction.SEND_MESSAGE, target_ref=USER_REF, content=content()),
        operation_ref="op_postmutationfencelost000001",
    )
    assert lost["status"] == "outcome_unknown"
    assert lost["error_code"] == "lease_lost"
    assert len(fence_client.calls) == 1


@pytest.mark.asyncio
async def test_total_session_deadline_bounds_connect_and_disconnect_cleanup():
    connect_refs = FakeRefs()
    connect_client = FakeClient(connect_refs)
    connect_client.connect_delay = 60
    connect_adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: connect_client,
        refs=connect_refs,
        governor=FakeGovernor(),
        telethon_types=FakeTypes(),
        operation_timeout_seconds=0.01,
    )
    connect_operation = "op_connectdeadline00000000000001"
    with pytest.raises(TelegramWorkspaceError) as timed_out:
        await connect_adapter.execute(
            intent(
                SocialAction.SEND_MESSAGE,
                target_ref=USER_REF,
                content=content(),
            ),
            operation_ref=connect_operation,
        )
    assert timed_out.value.code == "provider_timeout"
    assert timed_out.value.retry_safe is True
    assert connect_operation not in connect_refs.operations

    disconnect_refs = FakeRefs()
    disconnect_client = FakeClient(disconnect_refs)
    disconnect_client.disconnect_delay = 60
    disconnect_adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: disconnect_client,
        refs=disconnect_refs,
        governor=FakeGovernor(),
        telethon_types=FakeTypes(),
        operation_timeout_seconds=0.01,
    )
    disconnect_operation = "op_disconnectdeadline00000000001"
    receipt = await disconnect_adapter.execute(
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=USER_REF,
            content=content(),
        ),
        operation_ref=disconnect_operation,
    )
    assert receipt["status"] == "succeeded"
    assert disconnect_refs.resolve_operation(disconnect_operation).result == receipt


@pytest.mark.asyncio
async def test_session_deadline_includes_serialized_queue_wait():
    refs = FakeRefs()
    client = FakeClient(refs)
    client.disconnect_delay = 60

    async def mutates_then_blocks(entity, text, **kwargs):
        client.calls.append(("send_message", {"entity": entity, "text": text, **kwargs}))
        await asyncio.sleep(60)

    client.send_message = mutates_then_blocks
    adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: client,
        refs=refs,
        governor=FakeGovernor(),
        telethon_types=FakeTypes(),
        operation_timeout_seconds=0.03,
    )
    first_operation = "op_serialqueuefirst000000000001"
    second_operation = "op_serialqueuesecond00000000001"
    first = asyncio.create_task(
        adapter.execute(
            intent(
                SocialAction.SEND_MESSAGE,
                target_ref=USER_REF,
                content=content(),
            ),
            operation_ref=first_operation,
        )
    )
    while first_operation not in refs.mutation_started:
        await asyncio.sleep(0)

    with pytest.raises(TelegramWorkspaceError) as queued_timeout:
        await adapter.execute(
            intent(
                SocialAction.SEND_MESSAGE,
                target_ref=USER_REF,
                content=content("second must not send"),
            ),
            operation_ref=second_operation,
        )
    first_receipt = await first

    assert queued_timeout.value.code == "provider_timeout"
    assert queued_timeout.value.retry_safe is True
    assert second_operation not in refs.operations
    assert first_receipt["status"] == "outcome_unknown"
    assert len([name for name, _ in client.calls if name == "send_message"]) == 1


@pytest.mark.asyncio
async def test_cleanup_cancellation_always_releases_local_session_lock():
    refs = FakeRefs()
    client = FakeClient(refs)
    disconnect_started = asyncio.Event()

    async def blocked_disconnect():
        disconnect_started.set()
        await asyncio.sleep(60)

    client.disconnect = blocked_disconnect
    adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: client,
        refs=refs,
        governor=FakeGovernor(),
        telethon_types=FakeTypes(),
        operation_timeout_seconds=1,
    )
    first_operation = "op_cleanupcancelled000000000001"
    first = asyncio.create_task(
        adapter.execute(
            intent(
                SocialAction.SEND_MESSAGE,
                target_ref=USER_REF,
                content=content(),
            ),
            operation_ref=first_operation,
        )
    )
    await disconnect_started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    async def immediate_disconnect():
        return None

    client.disconnect = immediate_disconnect
    second = await adapter.execute(
        intent(
            SocialAction.SEND_MESSAGE,
            target_ref=USER_REF,
            content=content("lock is reusable"),
        ),
        operation_ref="op_cleanupfollowup0000000000001",
    )
    assert second["status"] == "succeeded"


@pytest.mark.asyncio
async def test_caller_operation_ref_is_the_only_receipt_and_reconciliation_key(harness):
    adapter, _, refs, _ = harness
    operation_ref = "op_callerissued000000000000001"
    receipt = await adapter.execute(
        intent(SocialAction.PUBLISH, target_ref=CHANNEL_REF, content=content()),
        operation_ref=operation_ref,
    )
    assert receipt["operation_ref"] == operation_ref
    assert list(refs.operations) == [operation_ref]
    assert await adapter.reconcile(operation_ref) == receipt
    with pytest.raises(TelegramWorkspaceError) as missing:
        await adapter.reconcile("op_missingoperation000000000001")
    assert missing.value.code == "operation_not_found"


@pytest.mark.asyncio
async def test_scheduled_items_returns_one_logical_four_image_album(harness):
    adapter, client, _, _ = harness
    messages = []
    for index in range(4):
        message = Message(1200 + index, "Exact album" if index == 0 else "")
        message.grouped_id = 77
        message.date = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message.id))
        messages.append(message)
    client.scheduled_messages[CHANNEL_REF] = messages

    result = await adapter.scheduled_items(
        target_ref=CHANNEL_REF,
        scheduled_from="2026-08-31T11:59:00Z",
        scheduled_to="2026-08-31T12:01:00Z",
        text_sha256=hashlib.sha256(b"Exact album").hexdigest(),
        media_count=4,
        limit=10,
    )

    assert result["exact_match_count"] == 1
    assert len(result["items"]) == 1
    assert result["items"][0]["media_count"] == 4
    assert result["items"][0]["media_roles"] == ["image"] * 4
    assert result["items"][0]["queue"] == "scheduled"


@pytest.mark.asyncio
async def test_schedule_reconciliation_converges_from_unknown_to_exact_raw_match(harness):
    adapter, client, refs, _ = harness
    operation_ref = "op_restartreconcile000000000001"
    request = intent(
        SocialAction.SCHEDULE,
        target_ref=CHANNEL_REF,
        content=content(
            "Restart exact text",
            media=(MediaAttachment(ASSET_REF, MediaRole.IMAGE),) * 4,
        ),
        schedule_at="2026-08-31T12:00:00Z",
    )
    action_digest = telegram_adapter_module.compute_action_digest(request)
    evidence = adapter._operation_intent(request)
    refs.claim_operation(
        operation_ref=operation_ref,
        action_digest=action_digest,
        intent=evidence,
    )
    refs.mark_operation_mutation(
        operation_ref=operation_ref, action_digest=action_digest
    )
    refs.complete_operation(
        operation_ref=operation_ref,
        action_digest=action_digest,
        result={
            "platform": "telegram",
            "operation_ref": operation_ref,
            "action": "schedule",
            "status": "outcome_unknown",
            "retry_safe": False,
            "error_code": "provider_timeout",
        },
    )
    for index in range(4):
        message = Message(1300 + index, "Restart exact text" if index == 0 else "")
        message.grouped_id = 88
        message.date = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message.id))
        client.scheduled_messages[CHANNEL_REF].append(message)

    result = await adapter.reconcile(operation_ref, intent=request)

    assert result["status"] == "succeeded"
    assert result["media_count"] == 4
    assert result["read_after_write"]["verified"] is True
    assert [name for name, _ in client.calls].count("scheduled_history") >= 1


@pytest.mark.asyncio
async def test_outer_cancellation_after_mutation_durably_finalizes_claim(harness):
    adapter, client, refs, _ = harness
    operation_ref = "op_cancelledmutation00000000001"

    async def mutates_then_blocks(entity, text, **kwargs):
        client.calls.append(("send_message", {"entity": entity, "text": text, **kwargs}))
        client.messages[USER_REF].append(Message(1400, text, entity=entity))
        await asyncio.sleep(60)

    client.send_message = mutates_then_blocks
    task = asyncio.create_task(
        adapter.execute(
            intent(SocialAction.SEND_MESSAGE, target_ref=USER_REF, content=content()),
            operation_ref=operation_ref,
        )
    )
    while operation_ref not in refs.mutation_started:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    claim = refs.resolve_operation(operation_ref)
    assert claim.result is not None
    assert claim.result["status"] == "outcome_unknown"
    assert claim.result["error_code"] == "provider_cancelled"


@pytest.mark.asyncio
async def test_scheduled_delete_uses_raw_namespace_and_verifies_album_absence(harness):
    adapter, client, refs, _ = harness
    scheduled_ref = "itm_scheduledalbum000001"
    refs.items[scheduled_ref] = TelegramScheduledItemBinding(
        scheduled_ref,
        CHANNEL_REF,
        1500,
        frozenset({SocialAction.DELETE}),
        SocialItemKind.POST,
    )
    for index in range(4):
        message = Message(1500 + index, "Delete exact" if index == 0 else "")
        message.grouped_id = 99
        message.media = SimpleNamespace(photo=SimpleNamespace(id=message.id))
        client.scheduled_messages[CHANNEL_REF].append(message)

    result = await adapter.execute(
        intent(SocialAction.DELETE, item_ref=scheduled_ref),
        operation_ref="op_deletescheduledalbum000000001",
    )

    assert result["status"] == "succeeded"
    assert result["read_after_write"]["verified"] is True
    assert result["read_after_write"]["absence_verified"] is True
    assert [name for name, _ in client.calls].count("delete_scheduled") == 1
    assert [name for name, _ in client.calls].count("delete_messages") == 0
    assert client.scheduled_messages[CHANNEL_REF] == []


@pytest.mark.asyncio
async def test_schedule_reconciliation_zero_window_then_ambiguous_is_bounded(harness):
    adapter, client, refs, _ = harness
    operation_ref = "op_boundedreconcile000000000001"
    request = intent(
        SocialAction.SCHEDULE,
        target_ref=CHANNEL_REF,
        content=content("Bounded exact"),
        schedule_at="2026-08-31T12:00:00Z",
    )
    digest = telegram_adapter_module.compute_action_digest(request)
    refs.claim_operation(
        operation_ref=operation_ref,
        action_digest=digest,
        intent=adapter._operation_intent(request),
    )
    refs.mark_operation_mutation(operation_ref=operation_ref, action_digest=digest)

    pending = await adapter.reconcile(operation_ref)
    assert pending["status"] == "outcome_unknown"
    assert pending["error_code"] == "reconciliation_pending"
    assert pending["reconciliation_attempt"] == 1
    assert 1 <= pending["next_poll_after_seconds"] <= 15
    assert pending["reconciliation_deadline"].endswith("Z")

    for index in range(2):
        duplicate = Message(1600 + index, "Bounded exact")
        duplicate.date = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
        client.scheduled_messages[CHANNEL_REF].append(duplicate)
    ambiguous = await adapter.reconcile(operation_ref)
    assert ambiguous["status"] == "outcome_unknown"
    assert ambiguous["error_code"] == "reconciliation_ambiguous"
    assert ambiguous["exact_match_count"] == 2
    assert len(ambiguous["item_refs"]) == 2


@pytest.mark.asyncio
async def test_schedule_reconciliation_terminal_zero_does_not_poll_forever(harness):
    adapter, client, refs, _ = harness
    operation_ref = "op_terminalreconcile00000000001"
    request = intent(
        SocialAction.SCHEDULE,
        target_ref=CHANNEL_REF,
        content=content("Missing exact"),
        schedule_at="2026-08-31T12:00:00Z",
    )
    digest = telegram_adapter_module.compute_action_digest(request)
    refs.claim_operation(
        operation_ref=operation_ref,
        action_digest=digest,
        intent=adapter._operation_intent(request),
    )
    refs.mark_operation_mutation(operation_ref=operation_ref, action_digest=digest)
    original_note = refs.note_reconciliation_attempt

    def expired(**kwargs):
        claim = original_note(**kwargs)
        return replace(claim, reconciliation_deadline_ms=0)

    refs.note_reconciliation_attempt = expired
    terminal = await adapter.reconcile(operation_ref)
    reads = [name for name, _ in client.calls].count("scheduled_history")
    replay = await adapter.reconcile(operation_ref)

    assert terminal == replay
    assert terminal["error_code"] == "reconciliation_no_match"
    assert terminal["retry_safe"] is False
    assert [name for name, _ in client.calls].count("scheduled_history") == reads


@pytest.mark.asyncio
async def test_operation_ref_is_mandatory_and_exactly_validated_before_provider(harness):
    adapter, client, refs, _ = harness
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=USER_REF,
        content=content("required ref"),
    )
    with pytest.raises(TypeError, match="operation_ref"):
        await adapter.execute(request)
    for invalid in ("", "op_short", " op_testoperation000000000000000000000001"):
        with pytest.raises(Exception, match="operation_ref is invalid"):
            await adapter.execute(request, operation_ref=invalid)
    assert refs.operations == {}
    assert client.calls == []


@pytest.mark.asyncio
async def test_exact_operation_replay_returns_original_without_second_mutation(harness):
    adapter, client, refs, _ = harness
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=USER_REF,
        content=content("exact replay"),
    )
    operation_ref = op_ref(401)
    first = await adapter.execute(request, operation_ref=operation_ref)
    first_call_count = len(client.calls)
    second = await adapter.execute(request, operation_ref=operation_ref)
    assert second == first
    assert len(client.calls) == first_call_count
    assert refs.operations[operation_ref].result == first


@pytest.mark.asyncio
async def test_changed_intent_conflicts_on_claim_before_provider(harness):
    adapter, client, _, _ = harness
    operation_ref = op_ref(402)
    original = intent(
        SocialAction.PUBLISH,
        target_ref=CHANNEL_REF,
        content=content("original"),
    )
    changed = replace(original, content=content("changed"))
    await adapter.execute(original, operation_ref=operation_ref)
    first_call_count = len(client.calls)
    with pytest.raises(Exception, match="operation_ref intent conflict"):
        await adapter.execute(changed, operation_ref=operation_ref)
    assert len(client.calls) == first_call_count


@pytest.mark.asyncio
async def test_concurrent_same_operation_is_atomically_claimed_once():
    refs = FakeRefs()
    first_client = FakeClient(refs)
    second_client = FakeClient(refs)
    first_client.delay = 0.05
    adapters = (
        TelegramWorkspaceAdapter(
            client_factory=lambda: first_client,
            refs=refs,
            governor=FakeGovernor(),
            telethon_types=FakeTypes(),
        ),
        TelegramWorkspaceAdapter(
            client_factory=lambda: second_client,
            refs=refs,
            governor=FakeGovernor(),
            telethon_types=FakeTypes(),
        ),
    )
    request = intent(
        SocialAction.SEND_MESSAGE,
        target_ref=USER_REF,
        content=content("only once"),
    )
    operation_ref = op_ref(403)
    outcomes = await asyncio.gather(
        adapters[0].execute(request, operation_ref=operation_ref),
        adapters[1].execute(request, operation_ref=operation_ref),
        return_exceptions=True,
    )
    successes = [value for value in outcomes if isinstance(value, dict)]
    failures = [value for value in outcomes if isinstance(value, TelegramWorkspaceError)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert failures[0].code == "operation_in_progress"
    assert sum(
        name == "send_message"
        for client in (first_client, second_client)
        for name, _ in client.calls
    ) == 1
    assert await adapters[0].reconcile(operation_ref) == successes[0]


def test_surface_is_closed_lazy_and_contains_no_credential_or_raw_escape_hatch():
    assert TelegramWorkspaceAdapter.document_send_supported is True
    public = {
        name
        for name, value in inspect.getmembers(TelegramWorkspaceAdapter, inspect.isfunction)
        if not name.startswith("_")
    }
    assert public == {
        "capabilities",
        "resolve",
        "read",
        "execute",
            "reconcile",
            "scheduled_items",
            "stage_asset",
        "read_asset",
    }
    source = Path("private_events_mcp_telegram_adapter.py").read_text()
    assert "import telethon" in source  # lazy inside the fixed type factory
    assert "TELEGRAM_AUTH_BUNDLE_E2E" not in source
    assert "TELEGRAM_AUTH_BUNDLE_S22" not in source
    assert "TELEGRAM_SESSION" not in source
    for forbidden in ("raw_method", "raw_kwargs", "file_path", "fetch_url", "base64"):
        assert forbidden not in source
    signature = inspect.signature(TelegramWorkspaceAdapter.execute)
    assert list(signature.parameters) == ["self", "intent", "operation_ref"]
    assert signature.parameters["operation_ref"].kind is inspect.Parameter.KEYWORD_ONLY
    assert signature.parameters["operation_ref"].default is inspect.Parameter.empty
    assert "mint_operation" not in source


def test_installed_telethon_satisfies_the_guarded_fixed_feature_set():
    factory = telegram_adapter_module._DefaultTelethonTypes()
    factory.ensure()
    assert factory.request("similar_channels", channel=object()).__class__.__name__ == (
        "GetChannelRecommendationsRequest"
    )
    assert factory.entity(RichEntityKind.BOLD, offset=1, length=2).__class__.__name__ == (
        "MessageEntityBold"
    )
    filename = factory.document_filename("acceptance.txt")
    assert filename.__class__.__name__ == "DocumentAttributeFilename"
    assert filename.file_name == "acceptance.txt"

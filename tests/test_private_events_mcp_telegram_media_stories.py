from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from private_events_mcp.social_workspace import (
    MediaAttachment,
    MediaRole,
    RichContent,
    SocialAction,
    SocialActionIntent,
    SocialItemKind,
    SocialPlatform,
    SocialReadAccess,
    SocialReadOperation,
    SocialReadRequest,
    SocialTargetKind,
    SocialWorkspaceValidationError,
)
from private_events_mcp_telegram_adapter import (
    TelegramAssetBinding,
    TelegramItemBinding,
    TelegramLease,
    TelegramOperationClaim,
    TelegramTargetBinding,
    TelegramWorkspaceAdapter,
)

NOW = datetime.now(timezone.utc)
TARGET_REF = "tgt_storychannel000000000001"
STORY_REF = "itm_storyitem00000000000001"
READ_ASSET_REF = "ast_storymedia0000000000001"
UPLOAD_REF = "ast_storyupload000000000001"
OWNER = "a" * 64
STORAGE_REF = "ing_verifiedtelegramasset000001"


class Entity:
    broadcast = True
    creator = True
    username = "story_channel"
    title = "Story Channel"
    admin_rights = SimpleNamespace(
        post_messages=True,
        edit_messages=True,
        delete_messages=True,
        post_stories=True,
    )
    default_banned_rights = SimpleNamespace(send_messages=False)


class MessageMediaPhoto:
    def __init__(self, payload: bytes = b"visual") -> None:
        self.photo = SimpleNamespace(id=91)
        self.payload = payload


class StoryViews:
    def __init__(self, views: int, reactions: int, forwards: int) -> None:
        self.views_count = views
        self.reactions_count = reactions
        self.forwards_count = forwards
        self.recent_viewers = [111, 222]


class Story:
    def __init__(self, story_id: int, *, media=None, views=None) -> None:
        self.id = story_id
        self.date = NOW - timedelta(minutes=story_id)
        self.expire_date = NOW + timedelta(hours=12)
        self.caption = f"story {story_id}"
        self.media = media or MessageMediaPhoto()
        self.views = views or StoryViews(5, 2, 1)


class UpdateStoryID:
    def __init__(self, story_id: int, random_id: int) -> None:
        self.id = story_id
        self.random_id = random_id


@dataclass
class Request:
    name: str
    values: dict
    random_id: int | None = None


class FakeTypes:
    def request(self, name, **values):
        return Request(name, values, 778899 if name == "send_story" else None)

    def upload_media(self, uploaded, **values):
        return SimpleNamespace(uploaded=uploaded, **values)

    def public_story_privacy(self):
        return (SimpleNamespace(rule="allow_all"),)

    @staticmethod
    def story_id(response, *, random_id):
        matches = {
            update.id
            for update in response.updates
            if update.__class__.__name__ == "UpdateStoryID"
            and update.random_id == random_id
        }
        if len(matches) != 1:
            raise TimeoutError("story id mismatch")
        return matches.pop()

    def entity(self, kind, *, offset, length, **extra):
        return SimpleNamespace(kind=kind, offset=offset, length=length, **extra)


class FakeRefs:
    def __init__(self) -> None:
        self.target = TelegramTargetBinding(
            TARGET_REF,
            SocialTargetKind.CHANNEL,
            Entity(),
            title="Story Channel",
            allowed_actions=frozenset({SocialAction.STORY}),
            story_privacy=None,
        )
        self.items = {
            STORY_REF: TelegramItemBinding(
                STORY_REF,
                TARGET_REF,
                41,
                kind=SocialItemKind.STORY,
            )
        }
        self.assets = {
            READ_ASSET_REF: TelegramAssetBinding(
                READ_ASSET_REF, MediaRole.IMAGE, MessageMediaPhoto(b"inspectable")
            )
        }
        self.operations: dict[str, TelegramOperationClaim] = {}
        self.read_asset_bindings: list[dict] = []

    def resolve_target(self, target_ref):
        assert target_ref == TARGET_REF
        return self.target

    def resolve_item(self, item_ref):
        return self.items[item_ref]

    def resolve_asset(self, asset_ref):
        return self.assets[asset_ref]

    def mint_target(self, **_values):
        return self.target

    def mint_item(self, *, target_ref, message_id, allowed_actions=None, kind=SocialItemKind.MESSAGE):
        for item in self.items.values():
            if item.target_ref == target_ref and item.message_id == message_id:
                if item.kind is not kind:
                    raise AssertionError("kind changed")
                return item
        ref = f"itm_mintedstory{message_id:012d}"
        item = TelegramItemBinding(ref, target_ref, message_id, allowed_actions, kind)
        self.items[ref] = item
        return item

    def mint_read_asset(self, **values):
        self.read_asset_bindings.append(values)
        self.assets[READ_ASSET_REF] = TelegramAssetBinding(
            READ_ASSET_REF, values["role"], values["media"]
        )
        return READ_ASSET_REF

    def mint_upload_asset(self, *, role, upload):
        binding = TelegramAssetBinding(UPLOAD_REF, role, upload)
        self.assets[UPLOAD_REF] = binding
        return binding

    def mint_cursor(self, *, family, state):
        raise AssertionError((family, state))

    def resolve_cursor(self, *, family, cursor):
        raise AssertionError((family, cursor))

    def claim_operation(
        self,
        *,
        operation_ref,
        action_digest,
        intent=None,
        claim_ttl_seconds=None,
        reconciliation_deadline_ms=None,
    ):
        del intent, claim_ttl_seconds, reconciliation_deadline_ms
        existing = self.operations.get(operation_ref)
        if existing:
            return replace(existing, claimed_now=False)
        claim = TelegramOperationClaim(operation_ref, action_digest, True)
        self.operations[operation_ref] = claim
        return claim

    def mark_operation_mutation(self, *, operation_ref, action_digest):
        existing = self.operations.get(operation_ref)
        if existing is None or existing.action_digest != action_digest:
            return False
        self.operations[operation_ref] = replace(
            existing, mutation_started_at_ms=1
        )
        return True

    def release_operation(self, *, operation_ref, action_digest):
        existing = self.operations.get(operation_ref)
        if existing is None or existing.action_digest != action_digest:
            return False
        del self.operations[operation_ref]
        return True

    def complete_operation(self, *, operation_ref, action_digest, result):
        current = self.operations[operation_ref]
        completed = TelegramOperationClaim(operation_ref, action_digest, False, dict(result))
        assert current.action_digest == action_digest
        self.operations[operation_ref] = completed
        return completed

    def resolve_operation(self, operation_ref):
        return self.operations[operation_ref]


class FakeGovernor:
    def __init__(self) -> None:
        self.lease = TelegramLease("test-fence")

    def cooldown_remaining(self):
        return 0

    def note_flood_wait(self, _seconds):
        return None

    def acquire(self, _operation):
        return self.lease

    def assert_current(self, lease):
        return lease is self.lease

    def release(self, _lease):
        return None


class FakeReader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.opened = []

    def open_verified(self, storage_ref, owner_binding):
        self.opened.append((storage_ref, owner_binding))
        return self.data


class FakeClient:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.can_send = 1
        self.update_random_id_matches = True
        self.upload_delay = 0.0
        self.expected_upload = b"image-bytes"
        self.requests: list[Request] = []
        self.stories = [Story(41), Story(42, views=StoryViews(7, 3, 2))]

    async def connect(self):
        return None

    async def disconnect(self):
        return None

    def is_user_authorized(self):
        return True

    async def upload_file(self, stream, **kwargs):
        self.events.append("upload_file")
        if self.upload_delay:
            await asyncio.sleep(self.upload_delay)
        assert stream.read() == self.expected_upload
        return SimpleNamespace(handle="uploaded", kwargs=kwargs)

    async def __call__(self, request):
        self.events.append(request.name)
        self.requests.append(request)
        if request.name == "can_send_story":
            return SimpleNamespace(count_remains=self.can_send)
        if request.name == "send_story":
            random_id = request.random_id + (0 if self.update_random_id_matches else 1)
            return SimpleNamespace(updates=[UpdateStoryID(77, random_id)])
        if request.name == "stories_by_id":
            story_id = request.values["story_ids"][0]
            story = next((value for value in self.stories if value.id == story_id), Story(story_id))
            return SimpleNamespace(stories=[story])
        if request.name == "peer_stories":
            return SimpleNamespace(stories=SimpleNamespace(stories=list(self.stories)))
        if request.name == "stories_views":
            by_id = {story.id: story.views for story in self.stories}
            return SimpleNamespace(
                views=[by_id.get(story_id, StoryViews(11, 4, 2)) for story_id in request.values["story_ids"]],
                users=[SimpleNamespace(id=999, username="must-not-leak")],
            )
        raise AssertionError(request.name)

    async def iter_download(self, media, *, request_size):
        assert request_size == 512 * 1024
        yield media.payload[:4]
        yield media.payload[4:]


def verified(data=b"image-bytes", **changes):
    values = {
        "storage_ref": STORAGE_REF,
        "owner_binding": OWNER,
        "content_digest": "sha256:" + hashlib.sha256(data).hexdigest(),
        "mime_type": "image/jpeg",
        "byte_length": len(data),
        "width": 1080,
        "height": 1920,
        "duration": None,
        "expires_at": NOW + timedelta(hours=1),
    }
    values.update(changes)
    return SimpleNamespace(**values)


def story_intent(asset_ref=UPLOAD_REF, *, role=MediaRole.IMAGE):
    return SocialActionIntent(
        platform=SocialPlatform.TELEGRAM,
        action=SocialAction.STORY,
        idempotency_key="telegram-story-test-0001",
        target_ref=TARGET_REF,
        item_ref=None,
        destination_target_ref=None,
        content=RichContent(
            "caption",
            (),
            (MediaAttachment(asset_ref, role),),
        ),
        reaction=None,
        reaction_preset=None,
        schedule_at=None,
        expected_revision=None,
    )


def read_request(operation, **changes):
    request = SocialReadRequest(
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
    return replace(request, **changes)


@pytest.fixture
def harness():
    refs = FakeRefs()
    client = FakeClient()
    reader = FakeReader(b"image-bytes")
    adapter = TelegramWorkspaceAdapter(
        client_factory=lambda: client,
        refs=refs,
        governor=FakeGovernor(),
        telethon_types=FakeTypes(),
        asset_reader=reader,
        operation_timeout_seconds=1,
    )
    return adapter, client, refs, reader


@pytest.mark.asyncio
async def test_stage_and_story_commit_reopen_rehash_and_use_exact_story_sequence(harness):
    adapter, client, refs, reader = harness
    assert await adapter.stage_asset(verified(), role=MediaRole.IMAGE) == UPLOAD_REF

    result = await adapter.execute(
        story_intent(), operation_ref="op_telegramstorysuccess000001"
    )

    assert result["status"] == "succeeded"
    assert refs.items[result["item_ref"]].kind is SocialItemKind.STORY
    assert client.events == [
        "can_send_story",
        "upload_file",
        "send_story",
        "stories_by_id",
    ]
    assert reader.opened == [(STORAGE_REF, OWNER)]
    sent = next(request for request in client.requests if request.name == "send_story")
    assert sent.values["privacy_rules"][0].rule == "allow_all"


@pytest.mark.asyncio
async def test_video_staging_fails_closed_without_future_verified_duration_contract(harness):
    adapter, client, refs, reader = harness
    data = b"video-bytes"
    reader.data = data
    client.expected_upload = data
    video = verified(
        data,
        mime_type="video/mp4",
        duration=None,
        width=720,
        height=1280,
    )
    with pytest.raises(SocialWorkspaceValidationError, match="video ingress"):
        await adapter.stage_asset(video, role=MediaRole.VIDEO)
    assert UPLOAD_REF not in refs.assets
    assert client.events == []

    capabilities = await adapter.capabilities(TARGET_REF)
    assert "story" in capabilities["actions"]
    assert "image" in capabilities["content_features"]
    assert "video" not in capabilities["content_features"]


@pytest.mark.asyncio
async def test_story_rights_fail_before_upload_or_mutation(harness):
    adapter, client, _, _ = harness
    await adapter.stage_asset(verified(), role=MediaRole.IMAGE)
    client.can_send = 0

    with pytest.raises(SocialWorkspaceValidationError, match="story_quota_or_rights"):
        await adapter.execute(
            story_intent(), operation_ref="op_telegramstorynorights00001"
        )
    assert client.events == ["can_send_story"]


@pytest.mark.asyncio
async def test_stage_and_commit_reject_untrusted_metadata_and_byte_substitution(harness):
    adapter, client, _, reader = harness
    invalid = (
        verified(storage_ref="/tmp/caller-path.jpg"),
        verified(mime_type="text/html"),
        verified(byte_length=30 * 1024 * 1024 + 1),
        verified(owner_binding="not-an-owner-binding"),
    )
    for value in invalid:
        with pytest.raises(SocialWorkspaceValidationError):
            await adapter.stage_asset(value, role=MediaRole.IMAGE)

    await adapter.stage_asset(verified(), role=MediaRole.IMAGE)
    reader.data = b"substituted"
    with pytest.raises(SocialWorkspaceValidationError, match="bytes do not match"):
        await adapter.execute(
            story_intent(), operation_ref="op_telegramstorybadbytes00001"
        )
    assert "upload_file" not in client.events
    assert "send_story" not in client.events


@pytest.mark.asyncio
async def test_story_upload_timeout_and_update_id_mismatch_are_outcome_unknown(harness):
    adapter, client, _, _ = harness
    await adapter.stage_asset(verified(), role=MediaRole.IMAGE)
    client.upload_delay = 0.05
    adapter._timeout = 0.01
    timeout = await adapter.execute(
        story_intent(), operation_ref="op_telegramstorytimeout000001"
    )
    assert timeout["status"] == "outcome_unknown"
    assert timeout["retry_safe"] is False

    refs = FakeRefs()
    mismatch_client = FakeClient()
    mismatch_client.update_random_id_matches = False
    mismatch = TelegramWorkspaceAdapter(
        client_factory=lambda: mismatch_client,
        refs=refs,
        governor=FakeGovernor(),
        telethon_types=FakeTypes(),
        asset_reader=FakeReader(b"image-bytes"),
    )
    await mismatch.stage_asset(verified(), role=MediaRole.IMAGE)
    result = await mismatch.execute(
        story_intent(), operation_ref="op_telegramstoryidmismatch0001"
    )
    assert result["status"] == "outcome_unknown"
    assert "stories_by_id" not in mismatch_client.events


@pytest.mark.asyncio
async def test_story_reads_hydrate_media_and_only_return_aggregate_metrics(harness):
    adapter, client, refs, _ = harness
    page = await adapter.read(
        read_request(SocialReadOperation.LIST_STORIES, target_ref=TARGET_REF)
    )
    first = page["results"][0]
    assert first["kind"] == "story"
    assert first["media"] == [READ_ASSET_REF]
    assert first["basic_metrics"] == {
        "views": 5,
        "reactions": 2,
        "comments": 0,
        "shares": 1,
    }
    minted = refs.read_asset_bindings[0]
    assert minted["story_id"] == 41
    assert minted["item_kind"] is SocialItemKind.STORY
    assert minted["expires_at"] > NOW

    exact = await adapter.read(
        read_request(SocialReadOperation.GET_ITEM, item_ref=STORY_REF)
    )
    assert exact["item"]["kind"] == "story"
    stats = await adapter.read(
        read_request(SocialReadOperation.GET_STATISTICS, item_ref=STORY_REF)
    )
    assert stats["basic_metrics"] == {
        "views": 5,
        "reactions": 2,
        "comments": 0,
        "shares": 1,
    }
    aggregate = await adapter.read(
        read_request(
            SocialReadOperation.GET_STATISTICS,
            target_ref=TARGET_REF,
            item_kinds=(SocialItemKind.STORY,),
        )
    )
    assert aggregate["basic_metrics"] == {
        "views": 12,
        "reactions": 5,
        "comments": 0,
        "shares": 3,
    }
    rendered = repr({"page": page, "stats": stats, "aggregate": aggregate})
    assert "must-not-leak" not in rendered
    assert "recent_viewers" not in rendered
    assert all(
        name not in client.events
        for name in ("read_stories", "get_story_views_list")
    )


@pytest.mark.asyncio
async def test_story_media_can_be_materialized_with_a_hard_bound_without_mark_read(harness):
    adapter, client, _, _ = harness
    data = await adapter.read_asset(
        READ_ASSET_REF, owner_binding=OWNER, max_bytes=64
    )
    assert data == b"inspectable"
    assert client.events == []

    with pytest.raises(SocialWorkspaceValidationError, match="read bound"):
        await adapter.read_asset(
            READ_ASSET_REF, owner_binding=OWNER, max_bytes=4
        )
    assert all(name != "read_stories" for name in client.events)

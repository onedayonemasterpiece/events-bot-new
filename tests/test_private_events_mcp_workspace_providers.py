from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

import private_events_mcp_provider_adapters as legacy_adapters
import private_events_mcp_workspace_providers as workspace_providers
from private_events_mcp.social_workspace import (
    MediaRole,
    SocialTargetKind,
    validate_prepare_request,
)
from private_events_mcp_vk_adapter import VKActor
from private_events_mcp_workspace_providers import (
    DedicatedVKActorTransport,
    DurableTelegramGovernor,
    DurableVKAttemptRecorder,
    DurableVKCooldown,
    DurableVKGovernor,
    DurableVKOpaqueRefStore,
    DurableVKWorkspaceAdapter,
    InMemoryTelegramOpaqueRefStore,
    InMemoryVKOpaqueRefStore,
    ProviderBindingError,
    SQLiteProviderCoordinator,
    build_private_events_mcp_workspace_adapters,
)


def test_role_scoped_telegram_client_has_no_session_fallback(monkeypatch) -> None:
    observed = {}

    def fake_factory(session, api_id, api_hash, device):
        observed.update(
            session=session, api_id=api_id, api_hash=api_hash, device=device
        )
        return object()

    monkeypatch.setattr(legacy_adapters, "_telegram_client_factory", fake_factory)
    with pytest.raises(legacy_adapters.SocialAdapterError):
        legacy_adapters.build_role_scoped_telegram_client(
            {
                "TELEGRAM_AUTH_BUNDLE_E2E": "forbidden-fallback",
                "TELEGRAM_API_ID": "1",
                "TELEGRAM_API_HASH": "hash",
            }
        )
    bundle = "eyJzZXNzaW9uIjoicm9sZS1zZXNzaW9uIn0="
    legacy_adapters.build_role_scoped_telegram_client(
        {
            "TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP": bundle,
            "TELEGRAM_API_ID": "123",
            "TELEGRAM_API_HASH": "hash",
        }
    )
    assert observed == {
        "session": "role-session",
        "api_id": 123,
        "api_hash": "hash",
        "device": {},
    }


def test_vk_actor_transport_has_exact_credentials_and_no_fallback() -> None:
    transport = DedicatedVKActorTransport(
        allowed={VKActor.PUBLIC_READER: frozenset({"discover"})},
        environ={
            "PRIVATE_EVENTS_MCP_VK_PUBLIC_READER_TOKEN": "reader-secret",
            "VK_TOKEN": "must-not-be-used",
        },
    )
    assert transport.permits(VKActor.PUBLIC_READER, "discover") is True
    assert transport.permits(VKActor.PUBLIC_READER, "read_public") is False
    assert transport.permits(VKActor.USER_MESSENGER, "dm_send") is False
    no_role_token = DedicatedVKActorTransport(
        allowed={VKActor.PUBLIC_READER: frozenset({"discover"})},
        environ={"VK_TOKEN": "must-not-be-used"},
    )
    assert no_role_token.permits(VKActor.PUBLIC_READER, "discover") is False


def test_vk_media_upload_uses_dedicated_user_actor_token() -> None:
    transport = DedicatedVKActorTransport(
        allowed={VKActor.MEDIA_EDITOR: frozenset({"media_upload"})},
        environ={
            "PRIVATE_EVENTS_MCP_VK_MEDIA_EDITOR_TOKEN": "user-editor-secret",
            "PRIVATE_EVENTS_MCP_VK_COMMUNITY_EDITOR_TOKEN": "group-editor-secret",
        },
    )
    assert transport.permits(VKActor.MEDIA_EDITOR, "media_upload") is True
    assert transport.permits(VKActor.COMMUNITY_EDITOR, "media_upload") is False


def test_vk_attempt_stage_is_durable_and_native_result_is_encrypted(tmp_path) -> None:
    state = SQLiteProviderCoordinator(str(tmp_path / "attempt.sqlite"))
    recorder = DurableVKAttemptRecorder(state, "vk-test-signing-key")
    operation_ref = "op_" + "a" * 24
    recorder.record(
        operation_ref,
        {
            "stage": "wall_post",
            "method": "wall.post",
            "phase": "started",
        },
    )
    recorder.record(
        operation_ref,
        {
            "stage": "wall_post",
            "method": "wall.post",
            "phase": "finished",
            "http_status": 200,
            "provider_result": {"post_id": 901},
            "outcome": "succeeded",
        },
    )
    with state._connect() as conn:
        row = conn.execute(
            "SELECT * FROM social_provider_vk_attempt WHERE operation_ref=?",
            (operation_ref,),
        ).fetchone()
    assert row["attempt_no"] == 1
    assert row["stage"] == "wall_post"
    assert row["provider_method"] == "wall.post"
    assert row["request_finished_at_ms"] >= row["request_started_at_ms"]
    assert row["http_status"] == 200
    assert row["outcome_classification"] == "succeeded"
    assert row["provider_result_ciphertext"]
    assert "901" not in row["provider_result_ciphertext"]


@pytest.mark.asyncio
async def test_vk_actor_transport_reads_all_fragmented_response_chunks() -> None:
    class FragmentedContent:
        async def iter_chunked(self, _size):
            yield b'{"response":{"items":['
            yield b'{"conversation":{"unread_count":1}}'
            yield b']}}'

    body = await DedicatedVKActorTransport._read_bounded_body(
        FragmentedContent(), 1024
    )
    assert body == (
        b'{"response":{"items":['
        b'{"conversation":{"unread_count":1}}'
        b']}}'
    )


@pytest.mark.asyncio
async def test_vk_actor_transport_rejects_fragmented_response_over_cap() -> None:
    class OversizedContent:
        async def iter_chunked(self, _size):
            yield b"1234"
            yield b"5678"

    with pytest.raises(ProviderBindingError, match="response too large"):
        await DedicatedVKActorTransport._read_bounded_body(
            OversizedContent(), 7
        )


def test_vk_inner_refs_are_opaque_and_detached() -> None:
    refs = InMemoryVKOpaqueRefStore()
    native = {"kind": "community", "group_id": 123, "nested": {"value": 1}}
    ref = refs.mint("target", native)
    native["nested"]["value"] = 999
    first = refs.resolve("target", ref)
    first["nested"]["value"] = 777
    assert refs.resolve("target", ref)["nested"]["value"] == 1
    assert "123" not in ref


@pytest.mark.asyncio
async def test_durable_provider_cooldowns_and_telegram_lease(tmp_path) -> None:
    state = SQLiteProviderCoordinator(str(tmp_path / "auth.sqlite"))
    assert (tmp_path / "auth.sqlite").stat().st_mode & 0o777 == 0o600
    cooldown = DurableVKCooldown(state, captcha_seconds=60)
    await cooldown.record_captcha(VKActor.PUBLIC_READER)
    with pytest.raises(ProviderBindingError, match="cooldown"):
        await cooldown.ensure_available(VKActor.PUBLIC_READER)

    governor = DurableVKGovernor(state, interval_ms=100)
    await governor.before_call(VKActor.DIALOG_READER, "dialogs")
    await governor.before_call(VKActor.DIALOG_READER, "dialogs")

    telegram_a = DurableTelegramGovernor(state)
    telegram_b = DurableTelegramGovernor(state)
    lease = telegram_a.acquire("read")
    assert telegram_a.assert_current(lease) is True
    with pytest.raises(ProviderBindingError, match="busy"):
        telegram_b.acquire("read")
    telegram_a.release(lease)
    replacement = telegram_b.acquire("read")
    assert telegram_a.assert_current(lease) is False
    telegram_b.note_flood_wait(30)
    telegram_b.release(replacement)
    assert telegram_b.cooldown_remaining() > 0


def test_provider_state_rejects_symlink_and_non_file_paths(tmp_path) -> None:
    target = tmp_path / "target.sqlite"
    target.write_bytes(b"not-a-database")
    link = tmp_path / "linked.sqlite"
    link.symlink_to(target)
    with pytest.raises(ProviderBindingError, match="path is unsafe"):
        SQLiteProviderCoordinator(str(link))

    directory = tmp_path / "directory.sqlite"
    directory.mkdir(mode=0o755)
    with pytest.raises(ProviderBindingError, match="path is unsafe"):
        SQLiteProviderCoordinator(str(directory))
    assert directory.stat().st_mode & 0o777 == 0o755


def test_vk_bindings_are_encrypted_and_survive_restart(tmp_path) -> None:
    path = tmp_path / "auth.sqlite"
    state = SQLiteProviderCoordinator(str(path))
    first = DurableVKOpaqueRefStore(state, "signing-key-for-tests-123456789")
    ref = first.mint(
        "target", {"kind": "community", "group_id": 123, "owner_id": -123}
    )
    second = DurableVKOpaqueRefStore(
        SQLiteProviderCoordinator(str(path)), "signing-key-for-tests-123456789"
    )
    assert second.resolve("target", ref)["group_id"] == 123
    assert b'"group_id"' not in path.read_bytes()


def test_durable_provider_binding_rejects_oversized_state_before_persisting(
    tmp_path,
) -> None:
    path = tmp_path / "auth.sqlite"
    refs = DurableVKOpaqueRefStore(
        SQLiteProviderCoordinator(str(path)), "signing-key-for-tests-123456789"
    )
    with pytest.raises(ProviderBindingError, match="exceeds size limit"):
        refs.mint("cursor", {"start_from": "X" * 100_000})
    with sqlite3.connect(path) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM social_provider_binding"
        ).fetchone()[0]
    assert count == 0


@pytest.mark.asyncio
async def test_vk_operation_receipt_survives_adapter_restart(tmp_path) -> None:
    class Delegate:
        def __init__(self) -> None:
            self.calls = 0

        async def execute(self, intent, *, operation_ref):
            self.calls += 1
            return {
                "platform": "vk",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": "succeeded",
                "retry_safe": False,
            }

    path = tmp_path / "auth.sqlite"
    first_delegate = Delegate()
    first = DurableVKWorkspaceAdapter(
        first_delegate, SQLiteProviderCoordinator(str(path)), "vk-test-signing-key"
    )
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "send_message",
            "idempotency_key": "restart-safe-vk-operation",
            "target_ref": "tgt_" + "a" * 24,
            "content": {"text": "Exact reminder", "entities": [], "media": []},
        }
    )
    operation_ref = "op_" + "b" * 24
    receipt = await first.execute(intent, operation_ref=operation_ref)
    assert first_delegate.calls == 1

    second_delegate = Delegate()
    second = DurableVKWorkspaceAdapter(
        second_delegate, SQLiteProviderCoordinator(str(path)), "vk-test-signing-key"
    )
    assert await second.execute(intent, operation_ref=operation_ref) == receipt
    assert await second.reconcile(operation_ref) == receipt
    assert second_delegate.calls == 0


@pytest.mark.asyncio
async def test_vk_unknown_receipt_reconciles_from_encrypted_intent_after_restart(
    tmp_path,
) -> None:
    class Delegate:
        def __init__(self, unknown: bool) -> None:
            self.unknown = unknown
            self.reconciliations = 0

        async def execute(self, intent, *, operation_ref):
            return {
                "platform": "vk",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": "outcome_unknown" if self.unknown else "succeeded",
                "retry_safe": False,
                **({"error_code": "provider_timeout"} if self.unknown else {}),
            }

        async def reconcile_intent(
            self, operation_ref, intent, *, claimed_at_ms, **_provider_evidence
        ):
            self.reconciliations += 1
            assert intent.content.text == "Durable exact post"
            assert claimed_at_ms > 0
            return {
                "platform": "vk",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": "succeeded",
                "retry_safe": False,
                "item_ref": "itm_" + "c" * 24,
            }

    path = tmp_path / "vk-reconcile.sqlite"
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "publish",
            "idempotency_key": "durable-reconcile-vk-001",
            "target_ref": "tgt_" + "a" * 24,
            "content": {"text": "Durable exact post"},
        }
    )
    operation_ref = "op_" + "b" * 24
    first = DurableVKWorkspaceAdapter(
        Delegate(True), SQLiteProviderCoordinator(str(path)), "vk-test-signing-key"
    )
    unknown = await first.execute(intent, operation_ref=operation_ref)
    assert unknown["status"] == "outcome_unknown"
    second_delegate = Delegate(False)
    second = DurableVKWorkspaceAdapter(
        second_delegate,
        SQLiteProviderCoordinator(str(path)),
        "vk-test-signing-key",
    )
    reconciled = await second.reconcile(operation_ref)
    assert reconciled["status"] == "succeeded"
    assert second_delegate.reconciliations == 1


@pytest.mark.asyncio
async def test_durable_vk_wrapper_proxies_scheduled_read_and_safe_retry(
    tmp_path,
) -> None:
    class Delegate:
        def __init__(self) -> None:
            self.retry_calls = 0

        async def execute(self, intent, *, operation_ref):
            return {
                "platform": "vk",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": "failed",
                "retry_safe": True,
                "error_code": "media_upload_response_invalid",
            }

        async def scheduled_items(self, **kwargs):
            return {"platform": "vk", "target_ref": kwargs["target_ref"], "items": []}

        async def retry(self, intent, *, operation_ref, attempt_number):
            self.retry_calls += 1
            return {
                "platform": "vk",
                "operation_ref": operation_ref,
                "action": intent.action.value,
                "status": "succeeded",
                "retry_safe": False,
                "attempt_number": attempt_number,
            }

    delegate = Delegate()
    adapter = DurableVKWorkspaceAdapter(
        delegate,
        SQLiteProviderCoordinator(str(tmp_path / "vk-wrapper.sqlite")),
        "vk-test-signing-key",
    )
    intent = validate_prepare_request(
        {
            "platform": "vk",
            "action": "schedule",
            "idempotency_key": "durable-safe-retry-001",
            "target_ref": "tgt_" + "a" * 24,
            "content": {"text": "Exact scheduled post"},
            "schedule_at": "2026-08-31T12:00:00Z",
        }
    )
    operation_ref = "op_" + "b" * 24
    failed = await adapter.execute(intent, operation_ref=operation_ref)
    assert failed["retry_safe"] is True
    assert await adapter.scheduled_items(target_ref=intent.target_ref) == {
        "platform": "vk",
        "target_ref": intent.target_ref,
        "items": [],
    }

    retried = await adapter.retry(
        intent, operation_ref=operation_ref, attempt_number=2
    )
    assert retried["status"] == "succeeded"
    assert delegate.retry_calls == 1
    assert (await adapter.reconcile(operation_ref))["status"] == "succeeded"
    with pytest.raises(ProviderBindingError, match="not retry safe"):
        await adapter.retry(intent, operation_ref=operation_ref, attempt_number=3)


def test_telegram_bindings_survive_store_restart(config, tmp_path) -> None:
    isolated = replace(
        config,
        auth_database_path=str(tmp_path / "telegram-auth.sqlite"),
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_dm_enabled=True,
    )
    first = InMemoryTelegramOpaqueRefStore(isolated)
    binding = first.mint_target(
        entity={"id": 42},
        kind=SocialTargetKind.USER,
        title="Exact person",
        canonical_handle="exact_person",
        profile_link="https://t.me/exact_person",
        is_self=False,
    )
    second = InMemoryTelegramOpaqueRefStore(isolated)
    assert second.resolve_target(binding.target_ref).title == "Exact person"


def test_telegram_scheduled_item_namespace_survives_store_restart(
    config, tmp_path
) -> None:
    isolated = replace(
        config,
        auth_database_path=str(tmp_path / "telegram-scheduled-auth.sqlite"),
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_post_enabled=True,
    )
    first = InMemoryTelegramOpaqueRefStore(isolated)
    target = first.mint_target(
        entity={"id": 42},
        kind=SocialTargetKind.CHANNEL,
        title="Scheduled channel",
        canonical_handle="scheduled_channel",
        profile_link="https://t.me/scheduled_channel",
        is_self=False,
    )
    item = first.mint_item(
        target_ref=target.target_ref,
        message_id=777,
        scheduled=True,
    )

    second = InMemoryTelegramOpaqueRefStore(isolated)
    restored = second.resolve_item(item.item_ref)
    assert restored.message_id == 777
    assert restored.target_ref == target.target_ref
    assert restored.scheduled is True


def test_telegram_media_fingerprint_ignores_rotating_file_reference(
    config, tmp_path
) -> None:
    isolated = replace(
        config,
        auth_database_path=str(tmp_path / "telegram-media-auth.sqlite"),
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
    )
    refs = InMemoryTelegramOpaqueRefStore(isolated)
    target = refs.mint_target(
        entity=SimpleNamespace(id=42),
        kind=SocialTargetKind.USER,
        title="Synthetic target",
        canonical_handle=None,
        profile_link=None,
        is_self=False,
    )

    def mint(file_reference: bytes, *, access_hash: int = 7001) -> str:
        media = SimpleNamespace(
            document=SimpleNamespace(
                id=9001,
                access_hash=access_hash,
                file_reference=file_reference,
            )
        )
        return refs.mint_read_asset(
            target_ref=target.target_ref,
            media=media,
            role=MediaRole.DOCUMENT,
            media_kind="voice",
            item_message_id=101,
        )

    first = refs.resolve_asset(mint(b"first-expiring-capability"))
    rotated = refs.resolve_asset(mint(b"rotated-expiring-capability"))
    different_media = refs.resolve_asset(
        mint(b"rotated-expiring-capability", access_hash=7002)
    )

    assert first.identity_fingerprint == rotated.identity_fingerprint
    assert first.identity_fingerprint != different_media.identity_fingerprint
    assert (
        first.provider_media.document.file_reference
        != rotated.provider_media.document.file_reference
    )


def test_workspace_builder_is_lazy_and_matches_enabled_providers(config) -> None:
    vk_only = replace(
        config,
        universal_social_enabled=True,
        universal_social_vk_enabled=True,
    )
    adapters = build_private_events_mcp_workspace_adapters(vk_only)
    assert set(adapters) == {"vk"}
    assert not asyncio.iscoroutine(adapters["vk"])


def test_file_only_store_is_injected_into_telegram_never_vk(
    config, monkeypatch
) -> None:
    seen = {}

    def telegram(_config, *, asset_store=None):
        seen["telegram"] = asset_store
        return object()

    def vk(_config, *, asset_store=None):
        seen["vk"] = asset_store
        return object()

    monkeypatch.setattr(workspace_providers, "build_telegram_workspace_adapter", telegram)
    monkeypatch.setattr(workspace_providers, "build_vk_workspace_adapter", vk)
    file_only = replace(
        config,
        universal_social_enabled=True,
        universal_social_telegram_enabled=True,
        universal_social_vk_enabled=True,
        universal_social_dm_enabled=True,
        universal_social_file_send_enabled=True,
    )
    store = object()
    assert set(
        build_private_events_mcp_workspace_adapters(file_only, asset_store=store)
    ) == {"telegram", "vk"}
    assert seen == {"telegram": store, "vk": None}


def test_telegram_operation_claim_is_atomic_across_store_instances(config) -> None:
    first = InMemoryTelegramOpaqueRefStore(config)
    second = InMemoryTelegramOpaqueRefStore(config)
    operation_ref = "op_" + "x" * 24
    digest = "a" * 64
    claimed = first.claim_operation(
        operation_ref=operation_ref, action_digest=digest
    )
    replay = second.claim_operation(
        operation_ref=operation_ref, action_digest=digest
    )
    assert claimed.claimed_now is True
    assert replay.claimed_now is False
    with pytest.raises(ProviderBindingError, match="conflict"):
        second.claim_operation(
            operation_ref=operation_ref, action_digest="b" * 64
        )
    receipt = {
        "platform": "telegram",
        "operation_ref": operation_ref,
        "action": "send_message",
        "status": "succeeded",
        "retry_safe": False,
    }
    first.complete_operation(
        operation_ref=operation_ref,
        action_digest=digest,
        result=receipt,
    )
    assert second.resolve_operation(operation_ref).result == receipt


def test_telegram_operation_persists_encrypted_intent_and_attempt_state(config) -> None:
    store = InMemoryTelegramOpaqueRefStore(config)
    operation_ref = "op_" + "e" * 24
    digest = "c" * 64
    intent = {
        "action": "schedule",
        "target_ref": "tgt_encryptedtarget000001",
        "schedule_at": "2026-08-31T12:00:00Z",
        "text_sha256": "d" * 64,
        "media_count": 4,
        "media_digests": ["e" * 64] * 4,
    }

    claim = store.claim_operation(
        operation_ref=operation_ref,
        action_digest=digest,
        intent=intent,
        claim_ttl_seconds=30,
        reconciliation_deadline_ms=store._state.now_ms() + 60_000,
    )
    assert claim.intent == intent
    assert claim.claim_expires_at_ms > claim.claimed_at_ms
    assert claim.mutation_started_at_ms is None
    assert store.mark_operation_mutation(
        operation_ref=operation_ref, action_digest=digest
    ) is True
    attempted = store.note_reconciliation_attempt(
        operation_ref=operation_ref, action_digest=digest
    )
    assert attempted.mutation_started_at_ms is not None
    assert attempted.reconciliation_attempt == 1

    with store._state._connect() as conn:
        row = conn.execute(
            "SELECT * FROM social_provider_tg_operation WHERE operation_ref=?",
            (operation_ref,),
        ).fetchone()
    assert row["intent_ciphertext"]
    rendered = repr(dict(row))
    assert intent["target_ref"] not in rendered
    assert intent["text_sha256"] not in rendered


def test_telegram_legacy_null_claim_can_adopt_exact_intent_once(config) -> None:
    store = InMemoryTelegramOpaqueRefStore(config)
    operation_ref = "op_" + "l" * 24
    digest = "f" * 64
    now = store._state.now_ms()
    with store._state._connect() as conn:
        conn.execute(
            """INSERT INTO social_provider_tg_operation(
               operation_ref,action_digest,result_json,claimed_at_ms,updated_at_ms)
               VALUES(?,?,?,?,?)""",
            (operation_ref, digest, None, now - 120_000, now - 120_000),
        )
    intent = {
        "action": "schedule",
        "target_ref": "tgt_legacytarget0000001",
        "schedule_at": "2026-08-31T08:30:00Z",
        "text_sha256": "a" * 64,
        "media_count": 4,
        "media_digests": ["b" * 64] * 4,
    }

    adopted = store.adopt_operation_intent(
        operation_ref=operation_ref,
        action_digest=digest,
        intent=intent,
    )
    assert adopted.intent == intent
    assert store.resolve_operation(operation_ref).intent == intent
    with pytest.raises(ProviderBindingError, match="intent conflict"):
        store.adopt_operation_intent(
            operation_ref=operation_ref,
            action_digest=digest,
            intent={**intent, "media_count": 3, "media_digests": ["b" * 64] * 3},
        )


def test_telegram_unknown_result_can_converge_once_to_terminal(config) -> None:
    store = InMemoryTelegramOpaqueRefStore(config)
    operation_ref = "op_" + "r" * 24
    digest = "1" * 64
    store.claim_operation(operation_ref=operation_ref, action_digest=digest)
    unknown = {
        "platform": "telegram",
        "operation_ref": operation_ref,
        "action": "schedule",
        "status": "outcome_unknown",
        "retry_safe": False,
        "error_code": "provider_timeout",
    }
    store.complete_operation(
        operation_ref=operation_ref, action_digest=digest, result=unknown
    )
    terminal = {
        **unknown,
        "status": "failed",
        "retry_safe": True,
        "error_code": "provider_mutation_not_started",
    }
    store.complete_operation(
        operation_ref=operation_ref, action_digest=digest, result=terminal
    )
    assert store.resolve_operation(operation_ref).result == terminal
    with pytest.raises(ProviderBindingError, match="result conflict"):
        store.complete_operation(
            operation_ref=operation_ref,
            action_digest=digest,
            result={**terminal, "error_code": "different_terminal"},
        )

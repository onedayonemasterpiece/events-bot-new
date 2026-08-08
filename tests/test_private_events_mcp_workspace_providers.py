from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

import private_events_mcp_provider_adapters as legacy_adapters
from private_events_mcp.social_workspace import (
    SocialTargetKind,
    validate_prepare_request,
)
from private_events_mcp_vk_adapter import VKActor
from private_events_mcp_workspace_providers import (
    DedicatedVKActorTransport,
    DurableTelegramGovernor,
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
        first_delegate, SQLiteProviderCoordinator(str(path))
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
        second_delegate, SQLiteProviderCoordinator(str(path))
    )
    assert await second.execute(intent, operation_ref=operation_ref) == receipt
    assert await second.reconcile(operation_ref) == receipt
    assert second_delegate.calls == 0


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


def test_workspace_builder_is_lazy_and_matches_enabled_providers(config) -> None:
    vk_only = replace(
        config,
        universal_social_enabled=True,
        universal_social_vk_enabled=True,
    )
    adapters = build_private_events_mcp_workspace_adapters(vk_only)
    assert set(adapters) == {"vk"}
    assert not asyncio.iscoroutine(adapters["vk"])


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

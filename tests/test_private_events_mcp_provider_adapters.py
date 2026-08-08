from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from private_events_mcp.social import ResolvedTarget, SocialAdapterError
from private_events_mcp_provider_adapters import (
    TelegramSocialAdapter,
    VKSocialAdapter,
    build_private_events_mcp_social_adapters,
)


TELEGRAM_TARGET = ResolvedTarget("telegram", "channel", "-1001234567890")
VK_TARGET = ResolvedTarget("vk", "community", "231920894")
SESSION_SECRET = "dedicated-session-secret-value"
API_HASH_SECRET = "dedicated-api-hash-secret-value"


def _bundle(**overrides: str) -> str:
    payload = {
        "session": SESSION_SECRET,
        "device_model": "MCP device",
        "system_version": "Linux",
        "app_version": "1.0",
        "lang_code": "ru",
        "system_lang_code": "ru",
        **overrides,
    }
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")


def _telegram_env() -> dict[str, str]:
    return {
        "TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP": _bundle(),
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_API_HASH": API_HASH_SECRET,
    }


class FakeTelegramClient:
    def __init__(self, *, messages=(), error: Exception | None = None) -> None:
        self.messages = tuple(messages)
        self.error = error
        self.connected = 0
        self.authorized_checks = 0
        self.disconnected = 0
        self.iter_calls: list[tuple[int, int]] = []
        self.send_calls: list[tuple[int, str, dict[str, object]]] = []

    async def connect(self) -> None:
        self.connected += 1

    async def is_user_authorized(self) -> bool:
        self.authorized_checks += 1
        return True

    async def disconnect(self) -> None:
        self.disconnected += 1

    async def iter_messages(self, target: int, *, limit: int):
        self.iter_calls.append((target, limit))
        if self.error:
            raise self.error
        for message in self.messages:
            yield message

    async def send_message(self, target: int, text: str, **kwargs):
        self.send_calls.append((target, text, kwargs))
        if self.error:
            raise self.error
        return SimpleNamespace(id=717)


def _factory_for(client: FakeTelegramClient, captures: list[object] | None = None):
    def factory(session: str, api_id: int, api_hash: str, device):
        if captures is not None:
            captures.extend((session, api_id, api_hash, dict(device)))
        return client

    return factory


@pytest.mark.asyncio
async def test_telegram_uses_dedicated_bundle_for_read_and_plain_publish() -> None:
    client = FakeTelegramClient(
        messages=(
            SimpleNamespace(id=11, message="", date=datetime.now(timezone.utc)),
            SimpleNamespace(
                id=12,
                message="Recent generic channel text",
                date=datetime(2026, 8, 8, 12, 30, tzinfo=timezone.utc),
            ),
        )
    )
    captures: list[object] = []
    adapter = TelegramSocialAdapter(
        environ=_telegram_env(),
        client_factory=_factory_for(client, captures),
    )

    read = await adapter.read_text(target=TELEGRAM_TARGET, limit=200)
    published = await adapter.publish_text(
        target=TELEGRAM_TARGET,
        text="Arbitrary plain text requested by the operator",
        idempotency_key="telegram-request-001",
    )

    assert [(post.post_id, post.text, post.published_at) for post in read.posts] == [
        ("12", "Recent generic channel text", "2026-08-08T12:30:00Z")
    ]
    assert client.iter_calls == [(-1001234567890, 100)]
    assert client.send_calls == [
        (
            -1001234567890,
            "Arbitrary plain text requested by the operator",
            {"parse_mode": None, "link_preview": False},
        )
    ]
    assert published.reference == "telegram-message:717"
    assert client.connected == 2
    assert client.authorized_checks == 2
    assert client.disconnected == 2
    assert captures == [
        SESSION_SECRET,
        12345,
        API_HASH_SECRET,
        {
            "device_model": "MCP device",
            "system_version": "Linux",
            "app_version": "1.0",
            "lang_code": "ru",
            "system_lang_code": "ru",
        },
        SESSION_SECRET,
        12345,
        API_HASH_SECRET,
        {
            "device_model": "MCP device",
            "system_version": "Linux",
            "app_version": "1.0",
            "lang_code": "ru",
            "system_lang_code": "ru",
        },
    ]


@pytest.mark.asyncio
async def test_telegram_has_no_fallback_to_other_role_sessions() -> None:
    forbidden_secret = "forbidden-e2e-session-secret"
    environ = {
        "TELEGRAM_AUTH_BUNDLE_E2E": _bundle(session=forbidden_secret),
        "TELEGRAM_SESSION": forbidden_secret,
        "TELEGRAM_AUTH_BUNDLE_S22": _bundle(session=forbidden_secret),
        "TELEGRAM_API_ID": "12345",
        "TELEGRAM_API_HASH": API_HASH_SECRET,
    }
    factory_called = False

    def factory(*_args):
        nonlocal factory_called
        factory_called = True
        return FakeTelegramClient()

    adapter = TelegramSocialAdapter(environ=environ, client_factory=factory)
    with pytest.raises(SocialAdapterError) as caught:
        await adapter.read_text(target=TELEGRAM_TARGET, limit=5)

    assert factory_called is False
    assert forbidden_secret not in str(caught.value)
    assert forbidden_secret not in repr(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "publish"])
async def test_telegram_disconnects_and_redacts_provider_errors(operation: str) -> None:
    raw_error = RuntimeError(
        f"provider leaked {SESSION_SECRET} {API_HASH_SECRET} {TELEGRAM_TARGET.provider_target}"
    )
    client = FakeTelegramClient(error=raw_error)
    adapter = TelegramSocialAdapter(
        environ=_telegram_env(),
        client_factory=_factory_for(client),
    )

    with pytest.raises(SocialAdapterError) as caught:
        if operation == "read":
            await adapter.read_text(target=TELEGRAM_TARGET, limit=5)
        else:
            await adapter.publish_text(
                target=TELEGRAM_TARGET,
                text="plain",
                idempotency_key="telegram-request-002",
            )

    rendered = f"{caught.value!s} {caught.value!r}"
    assert SESSION_SECRET not in rendered
    assert API_HASH_SECRET not in rendered
    assert TELEGRAM_TARGET.provider_target not in rendered
    assert caught.value.__cause__ is None
    assert client.disconnected == 1
    assert repr(adapter) == "<TelegramSocialAdapter platform='telegram'>"
    assert SESSION_SECRET not in repr(adapter)


@pytest.mark.asyncio
async def test_telegram_serializes_dedicated_session_use() -> None:
    state = {"active": 0, "max_active": 0, "next_id": 0}

    class SerializedClient(FakeTelegramClient):
        async def connect(self) -> None:
            await super().connect()
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])

        async def disconnect(self) -> None:
            state["active"] -= 1
            await super().disconnect()

        async def send_message(self, target: int, text: str, **kwargs):
            await asyncio.sleep(0.01)
            state["next_id"] += 1
            return SimpleNamespace(id=state["next_id"])

    def factory(*_args):
        return SerializedClient()

    adapter = TelegramSocialAdapter(
        environ=_telegram_env(),
        client_factory=factory,
    )
    await asyncio.gather(
        adapter.publish_text(
            target=TELEGRAM_TARGET,
            text="one",
            idempotency_key="telegram-serial-001",
        ),
        adapter.publish_text(
            target=TELEGRAM_TARGET,
            text="two",
            idempotency_key="telegram-serial-002",
        ),
    )

    assert state == {"active": 0, "max_active": 1, "next_id": 2}


@pytest.mark.asyncio
async def test_vk_uses_only_fixed_wall_methods_and_parameters() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_vk_api(method: str, **params):
        calls.append((method, params))
        if method == "wall.get":
            return {
                "count": 2,
                "items": [
                    {"id": 41, "date": 1786190400, "text": "Recent VK text"},
                    {"id": 40, "date": 1786180000, "text": "   "},
                ],
            }
        if method == "wall.post":
            return {"post_id": 42}
        raise AssertionError("unexpected VK method")

    adapter = VKSocialAdapter(fake_vk_api)
    read = await adapter.read_text(target=VK_TARGET, limit=200)
    published = await adapter.publish_text(
        target=VK_TARGET,
        text="Generic VK publication",
        idempotency_key="vk-request-stable-001",
    )

    expected_guid = hashlib.sha256(b"vk-request-stable-001").hexdigest()
    assert calls == [
        (
            "wall.get",
            {
                "owner_id": -231920894,
                "count": 100,
                "filter": "owner",
                "_private_events_mcp_log_boundary": True,
            },
        ),
        (
            "wall.post",
            {
                "owner_id": -231920894,
                "from_group": 1,
                "signed": 0,
                "message": "Generic VK publication",
                "guid": expected_guid,
                "_private_events_mcp_log_boundary": True,
            },
        ),
    ]
    assert [(post.post_id, post.text) for post in read.posts] == [
        ("41", "Recent VK text")
    ]
    assert read.posts[0].published_at == "2026-08-08T12:00:00Z"
    assert published.reference == "vk-post:42"
    assert re.fullmatch(r"[0-9a-f]{64}", expected_guid)


@pytest.mark.asyncio
async def test_reads_scan_past_blanks_but_return_at_most_requested_texts() -> None:
    messages = tuple(
        SimpleNamespace(id=index, message=" ", date=None) for index in range(1, 5)
    ) + (
        SimpleNamespace(id=5, message="first", date=None),
        SimpleNamespace(id=6, message="second", date=None),
    )
    telegram_client = FakeTelegramClient(messages=messages)
    telegram = TelegramSocialAdapter(
        environ=_telegram_env(),
        client_factory=_factory_for(telegram_client),
    )
    telegram_result = await telegram.read_text(target=TELEGRAM_TARGET, limit=1)
    assert [post.text for post in telegram_result.posts] == ["first"]
    assert telegram_client.iter_calls == [(-1001234567890, 5)]

    vk_calls: list[tuple[str, dict[str, object]]] = []

    async def fake_vk_api(method: str, **params):
        vk_calls.append((method, params))
        return {
            "items": [
                *({"id": index, "text": " ", "date": 0} for index in range(1, 5)),
                {"id": 5, "text": "first", "date": 0},
                {"id": 6, "text": "second", "date": 0},
            ]
        }

    vk_result = await VKSocialAdapter(fake_vk_api).read_text(target=VK_TARGET, limit=1)
    assert [post.text for post in vk_result.posts] == ["first"]
    assert vk_calls == [
        (
            "wall.get",
            {
                "owner_id": -231920894,
                "count": 5,
                "filter": "owner",
                "_private_events_mcp_log_boundary": True,
            },
        )
    ]


@pytest.mark.asyncio
async def test_vk_guid_is_stable_and_provider_errors_are_redacted() -> None:
    guids: list[str] = []

    async def successful_vk_api(method: str, **params):
        assert method == "wall.post"
        guids.append(params["guid"])
        return {"post_id": len(guids)}

    adapter = VKSocialAdapter(successful_vk_api)
    for _ in range(2):
        await adapter.publish_text(
            target=VK_TARGET,
            text="same",
            idempotency_key="vk-idempotency-key-001",
        )
    assert guids[0] == guids[1]

    raw_secret = "vk-provider-token-secret"

    async def failing_vk_api(method: str, **params):
        raise RuntimeError(f"{raw_secret} target={params['owner_id']} method={method}")

    failing = VKSocialAdapter(failing_vk_api)
    with pytest.raises(SocialAdapterError) as caught:
        await failing.read_text(target=VK_TARGET, limit=2)
    rendered = f"{caught.value!s} {caught.value!r}"
    assert raw_secret not in rendered
    assert VK_TARGET.provider_target not in rendered
    assert caught.value.__cause__ is None
    assert repr(failing) == "<VKSocialAdapter platform='vk'>"


@pytest.mark.asyncio
async def test_adapters_reject_non_policy_shaped_targets_before_provider_calls() -> None:
    calls = 0

    async def fake_vk_api(_method: str, **_params):
        nonlocal calls
        calls += 1
        return {}

    with pytest.raises(SocialAdapterError):
        await VKSocialAdapter(fake_vk_api).read_text(
            target=ResolvedTarget("vk", "bad", "https://vk.com/club1"),
            limit=1,
        )
    assert calls == 0


def test_adapter_builder_is_lazy_and_repr_contains_no_credentials(monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP", "not-base64")

    async def should_not_run(_method: str, **_params):
        raise AssertionError("provider called during adapter construction")

    adapters = build_private_events_mcp_social_adapters(should_not_run)
    assert set(adapters) == {"telegram", "vk"}
    assert "not-base64" not in repr(adapters)


def test_disabled_create_app_does_not_build_or_validate_provider_adapters(
    monkeypatch,
) -> None:
    import main
    import private_events_mcp_provider_adapters as provider_adapters

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:abc")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_ENABLED", "0")
    monkeypatch.setenv("PRIVATE_EVENTS_MCP_PUBLIC_BASE_URL", "not-an-absolute-url")
    monkeypatch.setenv("TELEGRAM_AUTH_BUNDLE_EVENTS_BOT_MCP", "not-base64")

    def fail_if_built(_vk_api):
        raise AssertionError("disabled MCP must not build provider adapters")

    monkeypatch.setattr(
        provider_adapters,
        "build_private_events_mcp_social_adapters",
        fail_if_built,
    )
    app = main.create_app()
    assert app is not None


@pytest.mark.asyncio
async def test_real_vk_runtime_error_log_redacts_mcp_payload_and_credentials(
    monkeypatch, caplog
) -> None:
    import main

    secret_token = "vk1.a.synthetic-secret-token-material-abcdef"
    secret_text = "private draft 123456789:abcdefghijklmnopqrstuvwxyzABCDE"
    provider_error = "provider echoed private draft and owner -231920894"

    class ErrorResponse:
        @staticmethod
        def json():
            return {
                "error": {
                    "error_code": 15,
                    "error_msg": provider_error,
                    "captcha_sid": "private-captcha-id",
                    "captcha_img": "https://captcha.example/private",
                }
            }

    async def fake_http_call(*_args, **_kwargs):
        return ErrorResponse()

    async def no_throttle() -> None:
        return None

    monkeypatch.setattr(main, "VK_READ_VIA_SERVICE", False)
    monkeypatch.setattr(main, "VK_USER_TOKEN", secret_token)
    monkeypatch.setattr(main, "VK_TOKEN", None)
    monkeypatch.setattr(main, "VK_TOKEN_AFISHA", None)
    monkeypatch.setattr(main, "http_call", fake_http_call)
    monkeypatch.setattr(main, "_vk_throttle", no_throttle)

    with caplog.at_level("ERROR"):
        with pytest.raises(SocialAdapterError):
            await VKSocialAdapter(main.vk_api).publish_text(
                target=VK_TARGET,
                text=secret_text,
                idempotency_key="private-log-boundary-001",
            )

    rendered = caplog.text
    for forbidden in (
        secret_token,
        secret_token[:6],
        secret_token[-4:],
        secret_text,
        "-231920894",
        provider_error,
        "private-captcha-id",
        "captcha.example/private",
    ):
        assert forbidden not in rendered
    assert "msg=<redacted-provider-error>" in rendered
    assert "token=<redacted>" in rendered

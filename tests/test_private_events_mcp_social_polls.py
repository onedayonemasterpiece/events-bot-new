from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from private_events_mcp.social_poll_contract import (
    PollErrorCode,
    PollValidationError,
    validate_poll_prepare_request,
)
from private_events_mcp.social_poll_runtime import PollWorkspaceRuntime
from private_events_mcp.social_workspace_runtime import SocialWorkspaceRuntime
from private_events_mcp.tool_catalog import ToolCallContext


class _Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection


class _BaseRuntime(SocialWorkspaceRuntime):
    def __init__(self, path: str, *, timeout: float = 0.1) -> None:
        self.store = _Store(path)
        self.adapters = {"telegram": object(), "vk": object()}
        self.preparation_ttl_seconds = 600
        self.provider_timeout_seconds = timeout

    def _binding(self, _principal):
        return ("client", "subject", "resource")

    @staticmethod
    def _hash(value: str) -> str:
        import hashlib

        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    @staticmethod
    def _encrypt(value: str) -> str:
        return "encrypted:" + value

    @staticmethod
    def _decrypt(value: str) -> str:
        return value.removeprefix("encrypted:")

    def _principal_hash(self, principal) -> str:
        return self._hash(f"{principal.client_id}:{principal.subject}")

    @staticmethod
    def _resolve_ref(ref: str, _kind: str, _platform: str, _principal) -> str:
        return "provider:" + ref

    @staticmethod
    def _mint_ref(_kind: str, _native, _platform: str, _principal) -> str:
        return "itm_" + "z" * 24


class _Provider:
    platform = "telegram"
    transport = "test_transport"
    principal_type = "test_principal"

    def __init__(self) -> None:
        self.execute_count = 0

    async def capabilities(self, **_kwargs):
        return {
            "support": "supported",
            "create": {"supported": True, "kinds": ["regular", "quiz"]},
            "reads": {
                "state": True,
                "results": True,
                "voters": {"support": "conditional", "complete_history": True},
            },
        }

    async def validate_and_preview(self, intent, **_kwargs):
        return {
            "schedule_mode": "provider_native",
            "compatibility_transformations": [],
            "summary": f"test {intent.action.value}",
        }

    async def execute(self, intent, context, existing, step):
        del intent, existing
        self.execute_count += 1
        step(
            "poll_create",
            ordinal=1,
            state="succeeded",
            attempted=True,
            binding={"opaque": "encrypted"},
        )
        return {
            "lifecycle_state": "open",
            "provider_item_ref": "provider-item",
            "provider_binding": {"provider_poll": "private"},
            "provider_option_bindings": {
                key: {"provider_answer": index}
                for index, key in enumerate(context.option_refs)
            },
            "published_at": "2026-08-26T00:00:00Z",
        }

    async def reconcile(self, **_kwargs):
        return {"lifecycle_state": "open"}

    async def get(self, **_kwargs):
        return {
            "lifecycle_state": "open",
            "observed_at": "2026-08-26T00:00:00Z",
        }

    async def results(self, context, existing):
        del context, existing
        return {
            "state": "open",
            "total_voters": 3,
            "options": [
                {"client_key": "a", "votes": 2, "rate": 66.67},
                {"client_key": "b", "votes": 1, "rate": 33.33},
            ],
            "complete": True,
            "source": "provider",
        }

    async def voters(self, **_kwargs):
        return {"voters": [], "complete": True, "source": "provider"}


class _TimeoutProvider(_Provider):
    async def execute(self, intent, context, existing, step):
        del intent, context, existing
        self.execute_count += 1
        step("send", ordinal=1, state="attempted", attempted=True)
        await asyncio.sleep(1)
        return {}


def _context(
    scopes: frozenset[str] = frozenset(
        {
            "telegram:publish",
            "telegram:read",
            "telegram:analytics",
            "telegram:audience",
        }
    ),
) -> ToolCallContext:
    identity = SimpleNamespace(client_id="client-id", subject="operator", scopes=scopes)
    return ToolCallContext(identity, "https://mcp.example.test")


def _create_payload(*, action: str = "publish") -> dict:
    payload = {
        "platform": "telegram",
        "action": action,
        "idempotency_key": f"poll-{action}-0001",
        "target_ref": "tgt_" + "a" * 24,
        "content": {
            "poll": {
                "question": {"text": "Какой формат?"},
                "options": [
                    {"client_key": "a", "text": {"text": "Короткий"}},
                    {"client_key": "b", "text": {"text": "Подробный"}},
                ],
            }
        },
    }
    return payload


def test_poll_contract_accepts_regular_poll() -> None:
    intent = validate_poll_prepare_request(
        _create_payload(), now=datetime(2026, 1, 1, tzinfo=timezone.utc)
    )
    assert intent.poll is not None
    assert intent.poll.kind.value == "regular"
    assert [option.client_key for option in intent.poll.options] == ["a", "b"]


def test_quiz_requires_correct_answer() -> None:
    payload = _create_payload()
    payload["content"]["poll"]["kind"] = "quiz"
    with pytest.raises(PollValidationError) as caught:
        validate_poll_prepare_request(payload)
    assert caught.value.error_code == PollErrorCode.POLL_FIELD_CONFLICT.value


def test_duplicate_client_key_is_rejected() -> None:
    payload = _create_payload()
    payload["content"]["poll"]["options"][1]["client_key"] = "a"
    with pytest.raises(PollValidationError):
        validate_poll_prepare_request(payload)


def test_cross_provider_extension_is_rejected() -> None:
    payload = _create_payload()
    payload["platform"] = "vk"
    payload["content"]["poll"]["provider_options"] = {
        "telegram": {"shuffle_answers": True}
    }
    with pytest.raises(PollValidationError) as caught:
        validate_poll_prepare_request(payload)
    assert caught.value.error_code == PollErrorCode.POLL_FIELD_UNSUPPORTED.value


def test_schedule_requires_matching_absolute_timezone() -> None:
    payload = _create_payload(action="schedule")
    payload.update(
        schedule_at="2026-08-27T19:00:00+02:00",
        timezone="Europe/Kaliningrad",
    )
    intent = validate_poll_prepare_request(
        payload, now=datetime(2026, 8, 26, tzinfo=timezone.utc)
    )
    assert intent.schedule_at_utc == "2026-08-27T17:00:00Z"
    assert intent.original_offset == "+02:00"

    payload["schedule_at"] = "2026-08-27T19:00:00+03:00"
    with pytest.raises(PollValidationError) as caught:
        validate_poll_prepare_request(
            payload, now=datetime(2026, 8, 26, tzinfo=timezone.utc)
        )
    assert caught.value.error_code == PollErrorCode.SCHEDULE_WINDOW_INVALID.value


def test_raw_provider_payload_is_rejected() -> None:
    payload = _create_payload()
    payload["content"]["poll"]["raw_provider_payload"] = {}
    with pytest.raises(PollValidationError) as caught:
        validate_poll_prepare_request(payload)
    assert caught.value.error_code == PollErrorCode.POLL_FIELD_UNSUPPORTED.value


@pytest.mark.asyncio
async def test_prepare_commit_read_and_migrations(tmp_path) -> None:
    base = _BaseRuntime(str(tmp_path / "polls.sqlite"))
    runtime = PollWorkspaceRuntime(base)
    provider = _Provider()
    runtime._providers["telegram"] = provider

    prepared = await runtime.prepare(_create_payload(), _context())
    committed = await runtime.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        _context(),
    )
    assert committed["status"] == "open"
    assert committed["poll_ref"] == prepared["poll_ref"]
    assert committed["item_ref"].startswith("itm_")
    assert "provider_poll" not in str(committed)

    poll = await runtime.get(
        {"poll_ref": prepared["poll_ref"], "refresh": True}, _context()
    )
    assert poll["lifecycle_state"] == "open"

    results = await runtime.results(
        {"poll_ref": prepared["poll_ref"]}, _context()
    )
    assert results["total_voters"] == 3
    assert results["options"][0]["poll_option_ref"].startswith("popt_")

    with base.store._connect() as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {
            "social_poll",
            "social_poll_option",
            "social_poll_preparation",
            "social_poll_operation",
            "social_poll_provider_step",
            "social_poll_result_snapshot",
            "social_poll_voter_observation",
            "social_poll_audit",
        }.issubset(table_names)
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM social_poll_provider_step"
            ).fetchone()[0]
            == 1
        )


@pytest.mark.asyncio
async def test_idempotent_replay_does_not_repeat_provider_call(tmp_path) -> None:
    base = _BaseRuntime(str(tmp_path / "polls.sqlite"))
    runtime = PollWorkspaceRuntime(base)
    provider = _Provider()
    runtime._providers["telegram"] = provider

    first = await runtime.prepare(_create_payload(), _context())
    second = await runtime.prepare(_create_payload(), _context())
    assert first == second

    commit_args = {
        "preparation_ref": first["preparation_ref"],
        "action_digest": first["action_digest"],
    }
    committed_first = await runtime.commit(commit_args, _context())
    committed_second = await runtime.commit(commit_args, _context())
    assert committed_first == committed_second
    assert provider.execute_count == 1


@pytest.mark.asyncio
async def test_edit_adds_and_removes_options_without_ref_drift(tmp_path) -> None:
    base = _BaseRuntime(str(tmp_path / "polls.sqlite"))
    runtime = PollWorkspaceRuntime(base)
    runtime._providers["telegram"] = _Provider()

    prepared = await runtime.prepare(_create_payload(), _context())
    await runtime.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        _context(),
    )
    original = await runtime.get({"poll_ref": prepared["poll_ref"]}, _context())
    original_ref = original["options"][0]["poll_option_ref"]
    edit = {
        "platform": "telegram",
        "action": "poll_edit",
        "idempotency_key": "poll-edit-0001",
        "poll_ref": prepared["poll_ref"],
        "expected_revision": original["revision"],
        "content": {
            "poll": {
                "question": {"text": "Изменённый вопрос?"},
                "options": [
                    {"client_key": "a", "text": {"text": "Короткий 2"}},
                    {"client_key": "c", "text": {"text": "Оба"}},
                ],
            }
        },
    }
    edit_prepared = await runtime.prepare(edit, _context())
    await runtime.commit(
        {
            "preparation_ref": edit_prepared["preparation_ref"],
            "action_digest": edit_prepared["action_digest"],
        },
        _context(),
    )
    changed = await runtime.get({"poll_ref": prepared["poll_ref"]}, _context())
    assert changed["question"]["text"] == "Изменённый вопрос?"
    assert [option["client_key"] for option in changed["options"]] == ["a", "c"]
    assert changed["options"][0]["poll_option_ref"] == original_ref
    assert changed["options"][1]["poll_option_ref"].startswith("popt_")


@pytest.mark.asyncio
async def test_timeout_is_unknown_and_not_blindly_retried(tmp_path) -> None:
    base = _BaseRuntime(str(tmp_path / "polls.sqlite"), timeout=0.01)
    runtime = PollWorkspaceRuntime(base)
    provider = _TimeoutProvider()
    runtime._providers["telegram"] = provider

    prepared = await runtime.prepare(_create_payload(), _context())
    commit_args = {
        "preparation_ref": prepared["preparation_ref"],
        "action_digest": prepared["action_digest"],
    }
    first = await runtime.commit(commit_args, _context())
    second = await runtime.commit(commit_args, _context())
    assert first == second
    assert first["status"] == "unknown"
    assert first["retry_safe"] is False
    assert first["reconciliation_required"] is True
    assert provider.execute_count == 1


@pytest.mark.asyncio
async def test_anonymous_poll_voters_are_never_exposed(tmp_path) -> None:
    base = _BaseRuntime(str(tmp_path / "polls.sqlite"))
    runtime = PollWorkspaceRuntime(base)
    runtime._providers["telegram"] = _Provider()
    prepared = await runtime.prepare(_create_payload(), _context())
    await runtime.commit(
        {
            "preparation_ref": prepared["preparation_ref"],
            "action_digest": prepared["action_digest"],
        },
        _context(),
    )
    with pytest.raises(PollValidationError) as caught:
        await runtime.voters(
            {"poll_ref": prepared["poll_ref"], "limit": 25}, _context()
        )
    assert caught.value.error_code == PollErrorCode.POLL_RESULTS_PRIVATE.value

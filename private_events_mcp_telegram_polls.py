"""Telegram MTProto poll provider for the private Social Workspace.

It reuses the existing Telethon workspace adapter, including its authenticated
session factory, cross-process lease/fence, opaque references and timeout policy.
Provider IDs remain inside encrypted poll bindings.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import inspect
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from private_events_mcp.social_poll_contract import (
    PollAction,
    PollActionIntent,
    PollErrorCode,
    PollKind,
    PollLifecycle,
    PollValidationError,
)
from private_events_mcp.social_workspace import (
    RichContent,
    SocialAction,
    SocialItemKind,
    SocialTargetKind,
)


def _err(code: PollErrorCode, message: str, **details: Any) -> PollValidationError:
    return PollValidationError(
        code,
        message,
        platform="telegram",
        transport="telegram_mtproto",
        **details,
    )


def _iso(value: datetime | None = None) -> str:
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _epoch(value: str | None) -> int | None:
    return (
        int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        if value
        else None
    )


def _long(seed: str) -> int:
    return max(
        1,
        int.from_bytes(hashlib.sha256(seed.encode()).digest()[:8], "big")
        & ((1 << 63) - 1),
    )


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _unb64(value: Any) -> bytes:
    if not isinstance(value, str) or not value:
        raise _err(PollErrorCode.POLL_OPTION_NOT_FOUND, "provider option binding is invalid")
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except Exception:
        raise _err(PollErrorCode.POLL_OPTION_NOT_FOUND, "provider option binding is invalid") from None


def _construct(factory: Any, **values: Any) -> Any:
    if not callable(factory):
        raise _err(
            PollErrorCode.POLL_UNSUPPORTED,
            "required Telegram poll constructor is unavailable",
            safe_to_retry=True,
            capability_requirement="current Telethon poll layer",
        )
    values = {key: value for key, value in values.items() if value is not None}
    try:
        signature = inspect.signature(factory)
    except (TypeError, ValueError):
        return factory(**values)
    if not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values()):
        values = {key: value for key, value in values.items() if key in signature.parameters}
    try:
        return factory(**values)
    except TypeError as exc:
        raise _err(
            PollErrorCode.POLL_UNSUPPORTED,
            "installed Telethon poll layer is incompatible",
            safe_to_retry=True,
            capability_requirement="telethon>=1.44",
        ) from exc


def _list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray, memoryview)):
        return list(value)
    return [value]


def _messages(value: Any) -> list[Any]:
    found: list[Any] = []
    seen: set[int] = set()

    def visit(current: Any, depth: int = 0) -> None:
        if current is None or depth > 5 or id(current) in seen:
            return
        seen.add(id(current))
        if isinstance(current, Mapping):
            for key in ("message", "messages", "update", "updates"):
                visit(current.get(key), depth + 1)
            return
        if isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray, memoryview)):
            for item in list(current)[:100]:
                visit(item, depth + 1)
            return
        message = getattr(current, "message", None)
        if message is not None and not isinstance(message, str):
            visit(message, depth + 1)
        visit(getattr(current, "messages", None), depth + 1)
        visit(getattr(current, "updates", None), depth + 1)
        if type(getattr(current, "id", None)) is int and (
            getattr(current, "media", None) is not None or hasattr(current, "date")
        ):
            found.append(current)

    visit(value)
    unique: dict[int, Any] = {}
    for item in found:
        if type(item.id) is int and item.id > 0:
            unique.setdefault(item.id, item)
    return list(unique.values())


def _media(message: Any) -> Any | None:
    media = getattr(message, "media", None)
    return media if getattr(media, "poll", None) is not None else None


def _peer_key(peer: Any) -> str | None:
    for field in ("user_id", "chat_id", "channel_id", "id"):
        value = getattr(peer, field, None)
        if type(value) is int and value:
            return f"{field}:{value}"
    return None


def _entity_key(entity: Any) -> str | None:
    value = getattr(entity, "id", None)
    return f"{entity.__class__.__name__}:{value}" if type(value) is int and value else None


def _name(entity: Any) -> str:
    title = getattr(entity, "title", None)
    if isinstance(title, str) and title.strip():
        return title.strip()[:256]
    parts = [
        value.strip()
        for value in (getattr(entity, "first_name", None), getattr(entity, "last_name", None))
        if isinstance(value, str) and value.strip()
    ]
    return (" ".join(parts) or getattr(entity, "username", None) or "Telegram voter")[:256]


class TelegramPollProvider:
    platform = "telegram"
    transport = "telegram_mtproto"
    principal_type = "user_session"

    def __init__(self, adapter: Any, *, tl: Any | None = None) -> None:
        self.adapter = getattr(adapter, "delegate", adapter)
        self.injected_tl = tl

    def _tl(self) -> Any:
        if self.injected_tl is not None:
            return self.injected_tl
        try:
            from telethon.tl import functions, types  # type: ignore
        except Exception:
            raise _err(
                PollErrorCode.POLL_UNSUPPORTED,
                "Telethon poll dependency is unavailable",
                safe_to_retry=True,
                capability_requirement="telethon>=1.44",
            ) from None
        messages = getattr(functions, "messages", None)
        required_types = ("TextWithEntities", "Poll", "PollAnswer", "InputMediaPoll")
        required_calls = (
            "SendMediaRequest",
            "EditMessageRequest",
            "GetPollResultsRequest",
            "GetPollVotesRequest",
            "GetScheduledMessagesRequest",
            "GetScheduledHistoryRequest",
            "DeleteScheduledMessagesRequest",
        )
        if any(not hasattr(types, name) for name in required_types) or any(
            messages is None or not hasattr(messages, name) for name in required_calls
        ):
            raise _err(
                PollErrorCode.POLL_UNSUPPORTED,
                "installed Telethon layer lacks poll methods",
                safe_to_retry=True,
                capability_requirement="current Telegram MTProto poll layer",
            )
        return SimpleNamespace(types=types, functions=functions)

    def _request(self, name: str, **values: Any) -> Any:
        return _construct(getattr(self._tl().functions.messages, name, None), **values)

    @staticmethod
    def _binding(existing: Mapping[str, Any] | None) -> dict[str, Any]:
        value = (existing or {}).get("provider_binding")
        return dict(value) if isinstance(value, Mapping) else {}

    @staticmethod
    def _tokens(poll_ref: str, keys: Sequence[str]) -> dict[str, bytes]:
        result = {
            key: hashlib.sha256(f"tg-poll-option\0{poll_ref}\0{key}".encode()).digest()[:16]
            for key in keys
        }
        if len(set(result.values())) != len(result):
            raise _err(PollErrorCode.POLL_FIELD_CONFLICT, "poll option token collision")
        return result

    def _rich(self, rich: Any) -> Any:
        content = RichContent(text=rich.text, entities=rich.entities, media=())
        return _construct(
            self._tl().types.TextWithEntities,
            text=rich.text,
            entities=self.adapter._compile_entities(content),  # noqa: SLF001
        )

    async def _asset(self, client: Any, ref: str | None, context: Any, attempt: Any) -> Any:
        if ref is None:
            return None
        binding = self.adapter._asset(context.resolve_asset(ref))  # noqa: SLF001
        return await self.adapter._provider_media(  # noqa: SLF001
            client, binding, spoiler=False, attempt=attempt
        )

    async def _input_media(
        self,
        intent: Any,
        context: Any,
        client: Any,
        attempt: Any,
        existing: Mapping[str, Any] | None,
        *,
        close_poll: Any | None = None,
    ) -> tuple[Any, int, dict[str, bytes]]:
        if close_poll is not None:
            closed = copy.deepcopy(close_poll)
            try:
                closed.closed = True
            except Exception as exc:
                raise _err(PollErrorCode.POLL_UNSUPPORTED, "poll object cannot be closed") from exc
            return _construct(self._tl().types.InputMediaPoll, poll=closed), int(closed.id), {}
        poll = intent.poll
        if poll is None:
            raise _err(PollErrorCode.POLL_FIELD_CONFLICT, "poll specification is required")
        binding = self._binding(existing)
        poll_id = binding.get("poll_id")
        if type(poll_id) is not int or poll_id <= 0:
            poll_id = _long(context.poll_ref)
        keys = [option.client_key for option in poll.options]
        tokens = self._tokens(context.poll_ref, keys)
        answers = []
        for option in poll.options:
            text = self._rich(option.text)
            if option.media_asset_ref:
                constructor = getattr(self._tl().types, "InputPollAnswer", None)
                if constructor is None:
                    raise _err(
                        PollErrorCode.POLL_FIELD_UNSUPPORTED,
                        "media answer options require InputPollAnswer",
                        field_path="content.poll.options[].media_asset_ref",
                    )
                answers.append(
                    _construct(
                        constructor,
                        text=text,
                        media=await self._asset(client, option.media_asset_ref, context, attempt),
                    )
                )
            else:
                answers.append(
                    _construct(self._tl().types.PollAnswer, text=text, option=tokens[option.client_key])
                )
        poll_object = _construct(
            self._tl().types.Poll,
            id=poll_id,
            closed=poll.close.initially_closed,
            public_voters=not poll.anonymous,
            multiple_choice=poll.multiple_answers,
            quiz=poll.kind is PollKind.QUIZ,
            open_answers=poll.telegram.open_answers,
            revoting_disabled=poll.telegram.revoting_disabled,
            shuffle_answers=poll.telegram.shuffle_answers,
            hide_results_until_close=poll.telegram.hide_results_until_close,
            subscribers_only=poll.telegram.subscribers_only,
            question=self._rich(poll.question),
            answers=answers,
            close_period=poll.close.open_period_seconds,
            close_date=_epoch(poll.close.at_utc),
            countries_iso2=list(poll.telegram.countries_iso2) or None,
        )
        explanation = poll.explanation
        entities = (
            self.adapter._compile_entities(  # noqa: SLF001
                RichContent(text=explanation.text, entities=explanation.entities, media=())
            )
            if explanation
            else None
        )
        return (
            _construct(
                self._tl().types.InputMediaPoll,
                poll=poll_object,
                correct_answers=[tokens[key] for key in poll.correct_option_client_keys] or None,
                solution=explanation.text if explanation else None,
                solution_entities=entities,
                attached_media=await self._asset(
                    client, poll.telegram.attached_media_asset_ref, context, attempt
                ),
                solution_media=await self._asset(
                    client, poll.telegram.solution_media_asset_ref, context, attempt
                ),
            ),
            poll_id,
            tokens,
        )

    @staticmethod
    def _parts(message: Any) -> tuple[Any, Any]:
        media = _media(message)
        if media is None:
            raise _err(
                PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED,
                "Telegram message no longer contains a poll",
                safe_to_retry=False,
            )
        return media.poll, getattr(media, "results", None)

    @staticmethod
    def _state(message: Any) -> dict[str, Any]:
        poll, results = TelegramPollProvider._parts(message)
        total = getattr(results, "total_voters", None)
        return {
            "closed": bool(getattr(poll, "closed", False)),
            "anonymous": not bool(getattr(poll, "public_voters", False)),
            "multiple_answers": bool(getattr(poll, "multiple_choice", False)),
            "quiz": bool(getattr(poll, "quiz", False)),
            "open_answers": bool(getattr(poll, "open_answers", False)),
            "total_voters": int(total) if type(total) is int else None,
        }

    @staticmethod
    def _option_bindings(message: Any, keys: Sequence[str], fallback: Mapping[str, bytes]) -> dict:
        poll, _ = TelegramPollProvider._parts(message)
        answers = _list(getattr(poll, "answers", None))
        result = {}
        for index, key in enumerate(keys):
            token = getattr(answers[index], "option", None) if index < len(answers) else None
            token = bytes(token) if isinstance(token, (bytes, bytearray, memoryview)) else fallback.get(key)
            if not token:
                raise _err(
                    PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED,
                    "Telegram did not return an option binding",
                    safe_to_retry=False,
                )
            result[key] = {"option": _b64(token)}
        return result

    async def _scheduled(self, client: Any, lease: Any, target: Any, message_id: int) -> Any:
        response = await self.adapter._call(  # noqa: SLF001
            client,
            lease,
            self._request("GetScheduledMessagesRequest", peer=target.entity, id=[message_id]),
        )
        return next((item for item in _messages(response) if item.id == message_id), None)

    async def _locate(self, client: Any, lease: Any, target: Any, binding: Mapping[str, Any]) -> tuple[Any, bool]:
        message_id = binding.get("message_id")
        scheduled = bool(binding.get("scheduled"))
        if type(message_id) is int and message_id > 0:
            if scheduled:
                try:
                    message = await self._scheduled(client, lease, target, message_id)
                except Exception:
                    message = None
                if message is not None:
                    return message, True
            else:
                message = await client.get_messages(target.entity, ids=message_id)
                if message is not None:
                    return message, False
        poll_id = binding.get("poll_id")
        if type(poll_id) is not int or poll_id <= 0:
            return None, scheduled
        try:
            scheduled_messages = await self.adapter._call(  # noqa: SLF001
                client,
                lease,
                self._request("GetScheduledHistoryRequest", peer=target.entity, hash=0),
            )
            for message in _messages(scheduled_messages)[:100]:
                media = _media(message)
                if media is not None and getattr(media.poll, "id", None) == poll_id:
                    return message, True
        except Exception:
            pass
        iterator = client.iter_messages(target.entity, limit=100)
        if hasattr(iterator, "__aiter__"):
            history = []
            async for message in iterator:
                history.append(message)
        else:
            history = await iterator if inspect.isawaitable(iterator) else iterator
        for message in list(history or [])[:100]:
            media = _media(message)
            if media is not None and getattr(media.poll, "id", None) == poll_id:
                return message, False
        return None, scheduled

    def _item_ref(self, target_ref: str, message: Any, target: Any) -> str:
        return self.adapter._mint_item_binding(  # noqa: SLF001
            target_ref=target_ref,
            message_id=int(message.id),
            kind=(
                SocialItemKind.POST
                if target.kind is SocialTargetKind.CHANNEL
                else SocialItemKind.MESSAGE
            ),
            allowed_actions=None,
        ).item_ref

    @staticmethod
    def _new_binding(old: Mapping[str, Any], message: Any, poll_id: int, scheduled: bool) -> dict:
        return {
            **dict(old),
            "message_id": int(message.id),
            "poll_id": int(poll_id),
            "scheduled": scheduled,
        }

    async def _permissions(self, client: Any, target: Any) -> dict:
        actions = set(await self.adapter._live_actions(client, target))  # noqa: SLF001
        send = bool(actions & {SocialAction.SEND_MESSAGE, SocialAction.PUBLISH, SocialAction.SCHEDULE})
        if send and target.kind is SocialTargetKind.GROUP and callable(getattr(client, "get_permissions", None)):
            permissions = await client.get_permissions(target.entity, "me")
            if not (getattr(permissions, "is_creator", False) or getattr(permissions, "is_admin", False)):
                participant = getattr(getattr(permissions, "participant", None), "banned_rights", None)
                default = getattr(target.entity, "default_banned_rights", None)
                send = not any(
                    bool(getattr(rights, "send_polls", False))
                    for rights in (participant, default)
                    if rights is not None
                )
        return {
            "send": send,
            "edit": SocialAction.EDIT in actions,
            "delete": SocialAction.DELETE in actions,
            "kind": target.kind.value,
        }

    async def capabilities(self, *, target_provider_ref: str | None) -> Mapping[str, Any]:
        permission = {"send": False, "edit": False, "delete": False, "kind": None}
        if target_provider_ref:
            target = self.adapter._target(target_provider_ref)  # noqa: SLF001

            async def run(client: Any, _lease: Any, _attempt: Any) -> Mapping[str, Any]:
                return await self._permissions(client, target)

            permission = dict(await self.adapter._session("poll_capabilities", run))  # noqa: SLF001
        ready = bool(target_provider_ref and permission["send"])
        constraints = ["non_anonymous", "provider_permission", "caller_may_need_to_vote"]
        if permission["kind"] == SocialTargetKind.CHANNEL.value:
            constraints.append("broadcast_forbidden")
        return {
            "support": "supported" if ready else "conditional",
            "provider_api_version": "mtproto-current-layer",
            "authorization": {
                "status": "ready" if ready else ("target_required" if not target_provider_ref else "permission_missing"),
                "missing_scopes": [],
                "missing_permissions": [] if ready else ["send_polls"],
            },
            "create": {"supported": ready, "kinds": ["regular", "quiz"] if ready else []},
            "publish": {"immediate": ready},
            "schedule": {
                "supported": ready,
                "mode": "provider_native",
                "editable": permission["edit"],
                "cancelable": permission["delete"],
            },
            "lifecycle": {
                "close": permission["edit"],
                "delete_container": permission["delete"],
                "edit_scheduled": permission["edit"],
                "edit_published": False,
            },
            "reads": {
                "state": bool(target_provider_ref),
                "results": bool(target_provider_ref),
                "voters": {"support": "conditional", "complete_history": True, "constraints": constraints},
            },
            "fields": {
                name: {"support": support}
                for name, support in {
                    "question_entities": "supported",
                    "option_entities": "supported",
                    "explanation_entities": "supported",
                    "open_answers": "supported",
                    "revoting_disabled": "supported",
                    "shuffle_answers": "supported",
                    "hide_results_until_close": "supported",
                    "subscribers_only": "conditional",
                    "countries_iso2": "supported",
                    "attached_media": "conditional",
                    "solution_media": "conditional",
                    "option_media": "conditional",
                }.items()
            },
            "limits": {
                "options": {"minimum": 2, "maximum_source": "provider_config.poll_answers_max"},
                "close_period_seconds": {"minimum": 5, "maximum_source": "provider_config.poll_close_period_max"},
                "schedule_minimum_seconds": 10,
                "voter_page": {"maximum": 100},
            },
            "implementation": {
                "adapter": "telethon_mtproto_poll_provider",
                "tested": "unit_contract",
                "live_verified": False,
            },
        }

    async def validate_and_preview(
        self,
        intent: PollActionIntent,
        *,
        target_provider_ref: str,
        existing: Mapping[str, Any] | None,
    ) -> Mapping[str, Any]:
        target = self.adapter._target(target_provider_ref)  # noqa: SLF001

        async def run(client: Any, _lease: Any, _attempt: Any) -> Mapping[str, Any]:
            return await self._permissions(client, target)

        permission = dict(await self.adapter._session("poll_preflight", run))  # noqa: SLF001
        if intent.action in {PollAction.PUBLISH, PollAction.SCHEDULE} and not permission["send"]:
            raise _err(
                PollErrorCode.POLL_AUTHORIZATION_MISSING,
                "Telegram target does not permit polls",
                capability_requirement="send_polls permission",
            )
        if intent.action in {PollAction.EDIT, PollAction.RESCHEDULE, PollAction.CLOSE} and not permission["edit"]:
            raise _err(PollErrorCode.POLL_AUTHORIZATION_MISSING, "edit permission is missing")
        if intent.action in {PollAction.CANCEL, PollAction.DELETE_CONTAINER} and not permission["delete"]:
            raise _err(PollErrorCode.POLL_AUTHORIZATION_MISSING, "delete permission is missing")
        poll = intent.poll
        if poll:
            if len(poll.options) < 2:
                raise _err(
                    PollErrorCode.POLL_LIMIT_EXCEEDED,
                    "Telegram polls require at least two options",
                    field_path="content.poll.options",
                )
            if target.kind is SocialTargetKind.CHANNEL and not poll.anonymous:
                raise _err(
                    PollErrorCode.POLL_FIELD_CONFLICT,
                    "public polls cannot be sent to broadcast channels",
                    field_path="content.poll.anonymous",
                )
            if poll.telegram.subscribers_only and target.kind not in {SocialTargetKind.CHANNEL, SocialTargetKind.GROUP}:
                raise _err(
                    PollErrorCode.POLL_FIELD_CONFLICT,
                    "subscribers_only requires a channel or group",
                    field_path="content.poll.provider_options.telegram.subscribers_only",
                )
            if poll.kind is PollKind.QUIZ and any(option.media_asset_ref for option in poll.options):
                raise _err(
                    PollErrorCode.POLL_FIELD_UNSUPPORTED,
                    "quiz media options cannot be mapped to correct answer tokens safely",
                    field_path="content.poll.options[].media_asset_ref",
                )
        if intent.content and intent.content.media:
            raise _err(
                PollErrorCode.POLL_FIELD_CONFLICT,
                "use typed Telegram poll media fields instead of content.media",
                field_path="content.media",
            )
        if intent.action in {PollAction.SCHEDULE, PollAction.RESCHEDULE} and (
            (_epoch(intent.schedule_at_utc) or 0) < int(datetime.now(timezone.utc).timestamp()) + 10
        ):
            raise _err(
                PollErrorCode.SCHEDULE_WINDOW_INVALID,
                "Telegram schedules under ten seconds are sent immediately",
                field_path="schedule_at",
            )
        state = str((existing or {}).get("lifecycle_state") or "")
        if intent.action in {PollAction.EDIT, PollAction.RESCHEDULE, PollAction.CANCEL} and state not in {
            PollLifecycle.QUEUED.value,
            PollLifecycle.PROVIDER_SCHEDULED.value,
        }:
            raise _err(PollErrorCode.POLL_NOT_EDITABLE, "only scheduled polls can be changed")
        if intent.action is PollAction.CLOSE and state not in {
            PollLifecycle.PUBLISHED.value,
            PollLifecycle.OPEN.value,
        }:
            raise _err(PollErrorCode.POLL_NOT_EDITABLE, "poll is not open")
        schedule_mode = "provider_native" if intent.action in {PollAction.SCHEDULE, PollAction.RESCHEDULE} or state == PollLifecycle.PROVIDER_SCHEDULED.value else None
        return {
            "summary": f"{intent.action.value} Telegram native poll",
            "schedule_mode": schedule_mode,
            "provider_schedule_at": intent.schedule_at_utc,
            "compatibility_transformations": [],
            "available_after_commit": ["state", "results", "close", "delete_container", "conditional_voters"],
            "safe_preview": {
                "provider_method_family": "messages.sendMedia/messages.editMessage",
                "native_poll": True,
                "provider_ids_exposed": False,
                "schedule_mode": schedule_mode,
            },
        }

    async def _create(self, intent: PollActionIntent, context: Any, step: Any) -> Mapping[str, Any]:
        target = self.adapter._target(context.target_provider_ref)  # noqa: SLF001

        async def run(client: Any, lease: Any, attempt: Any) -> Mapping[str, Any]:
            media, poll_id, tokens = await self._input_media(intent, context, client, attempt, None)
            scheduled = intent.action is PollAction.SCHEDULE
            random_id = _long(context.operation_ref)
            request = self._request(
                "SendMediaRequest",
                peer=target.entity,
                media=media,
                message=intent.content.text if intent.content else "",
                entities=(self.adapter._compile_entities(intent.content) if intent.content else []),  # noqa: SLF001
                random_id=random_id,
                schedule_date=_epoch(intent.schedule_at_utc) if scheduled else None,
            )
            step("telegram_send_poll", ordinal=1, state="attempted", attempted=True)
            attempt.provider_mutation_attempted = True
            response = await self.adapter._call(client, lease, request)  # noqa: SLF001
            message = next((item for item in _messages(response) if _media(item)), None)
            if message is None:
                raise _err(
                    PollErrorCode.PROVIDER_OUTCOME_UNKNOWN,
                    "Telegram accepted the request without a confirmable message",
                    provider_method="messages.sendMedia",
                    safe_to_retry=False,
                )
            readback = (
                await self._scheduled(client, lease, target, message.id)
                if scheduled
                else await client.get_messages(target.entity, ids=message.id)
            )
            if readback is None or _media(readback) is None:
                raise _err(
                    PollErrorCode.PROVIDER_OUTCOME_UNKNOWN,
                    "Telegram poll read-after-write failed",
                    safe_to_retry=False,
                )
            actual_poll, _ = self._parts(readback)
            actual_id = getattr(actual_poll, "id", None)
            actual_id = actual_id if type(actual_id) is int and actual_id > 0 else poll_id
            binding = self._new_binding(
                {"target_ref": context.target_provider_ref, "random_id": random_id},
                readback,
                actual_id,
                scheduled,
            )
            step("telegram_send_poll", ordinal=1, state="succeeded", attempted=True, binding=binding)
            lifecycle = PollLifecycle.PROVIDER_SCHEDULED.value if scheduled else (
                PollLifecycle.CLOSED.value if getattr(actual_poll, "closed", False) else PollLifecycle.OPEN.value
            )
            result = {
                "lifecycle_state": lifecycle,
                "provider_binding": binding,
                "provider_state": self._state(readback),
                "provider_option_bindings": self._option_bindings(
                    readback,
                    [option.client_key for option in intent.poll.options],
                    tokens,
                ),
                "schedule_mode": "provider_native" if scheduled else None,
                "provider_schedule_at": intent.schedule_at_utc if scheduled else None,
                "published_at": None if scheduled else _iso(getattr(readback, "date", None)),
                "actual_publish_at": None if scheduled else _iso(getattr(readback, "date", None)),
                "last_synced_at": _iso(),
            }
            if not scheduled:
                result["provider_item_ref"] = self._item_ref(context.target_provider_ref, readback, target)
            return result

        return await self.adapter._session("poll_send", run)  # noqa: SLF001

    async def _mutate(
        self,
        intent: PollActionIntent,
        context: Any,
        existing: Mapping[str, Any],
        step: Any,
    ) -> Mapping[str, Any]:
        target = self.adapter._target(context.target_provider_ref)  # noqa: SLF001
        old = self._binding(existing)
        message_id = old.get("message_id")
        if type(message_id) is not int:
            raise _err(PollErrorCode.POLL_REFERENCE_INVALID, "Telegram poll binding is missing")

        async def run(client: Any, lease: Any, attempt: Any) -> Mapping[str, Any]:
            action = intent.action
            method = f"telegram_{action.value}"
            step(method, ordinal=1, state="attempted", attempted=True)
            if action is PollAction.EDIT:
                if not old.get("scheduled"):
                    raise _err(PollErrorCode.POLL_NOT_EDITABLE, "published Telegram polls cannot be edited")
                media, poll_id, tokens = await self._input_media(intent, context, client, attempt, existing)
                request = self._request(
                    "EditMessageRequest",
                    peer=target.entity,
                    id=message_id,
                    message=intent.content.text if intent.content else "",
                    entities=(self.adapter._compile_entities(intent.content) if intent.content else []),  # noqa: SLF001
                    media=media,
                    schedule_date=_epoch(existing.get("provider_schedule_at") or existing.get("scheduled_at")),
                )
                attempt.provider_mutation_attempted = True
                await self.adapter._call(client, lease, request)  # noqa: SLF001
                message = await self._scheduled(client, lease, target, message_id)
                if message is None:
                    raise _err(PollErrorCode.PROVIDER_OUTCOME_UNKNOWN, "scheduled edit is not observable", safe_to_retry=False)
                binding = self._new_binding(old, message, poll_id, True)
                result = {
                    "lifecycle_state": PollLifecycle.PROVIDER_SCHEDULED.value,
                    "provider_binding": binding,
                    "provider_state": self._state(message),
                    "provider_option_bindings": self._option_bindings(
                        message, [option.client_key for option in intent.poll.options], tokens
                    ),
                    "schedule_mode": "provider_native",
                    "provider_schedule_at": existing.get("provider_schedule_at") or existing.get("scheduled_at"),
                    "last_synced_at": _iso(),
                }
            elif action is PollAction.RESCHEDULE:
                if not old.get("scheduled"):
                    raise _err(PollErrorCode.SCHEDULE_ALREADY_DISPATCHED, "poll already left the schedule queue")
                attempt.provider_mutation_attempted = True
                await self.adapter._call(  # noqa: SLF001
                    client,
                    lease,
                    self._request(
                        "EditMessageRequest",
                        peer=target.entity,
                        id=message_id,
                        schedule_date=_epoch(intent.schedule_at_utc),
                    ),
                )
                message = await self._scheduled(client, lease, target, message_id)
                if message is None:
                    raise _err(PollErrorCode.PROVIDER_OUTCOME_UNKNOWN, "rescheduled poll is not observable", safe_to_retry=False)
                result = {
                    "lifecycle_state": PollLifecycle.PROVIDER_SCHEDULED.value,
                    "provider_binding": self._new_binding(old, message, int(old.get("poll_id") or 0), True),
                    "provider_state": self._state(message),
                    "schedule_mode": "provider_native",
                    "provider_schedule_at": intent.schedule_at_utc,
                    "last_synced_at": _iso(),
                }
            elif action is PollAction.CANCEL:
                if not old.get("scheduled"):
                    raise _err(PollErrorCode.SCHEDULE_ALREADY_DISPATCHED, "poll already left the schedule queue")
                attempt.provider_mutation_attempted = True
                await self.adapter._call(  # noqa: SLF001
                    client,
                    lease,
                    self._request("DeleteScheduledMessagesRequest", peer=target.entity, id=[message_id]),
                )
                if await self._scheduled(client, lease, target, message_id) is not None:
                    raise _err(PollErrorCode.PROVIDER_OUTCOME_UNKNOWN, "scheduled poll still exists", safe_to_retry=False)
                result = {
                    "lifecycle_state": PollLifecycle.CANCELED.value,
                    "provider_binding": old,
                    "provider_state": {"canceled": True},
                    "schedule_mode": "provider_native",
                    "canceled_at": _iso(),
                    "last_synced_at": _iso(),
                }
            elif action is PollAction.CLOSE:
                message, scheduled = await self._locate(client, lease, target, old)
                if message is None or scheduled:
                    raise _err(PollErrorCode.POLL_NOT_EDITABLE, "published poll cannot be located")
                media, poll_id, _ = await self._input_media(
                    SimpleNamespace(poll=None),
                    context,
                    client,
                    attempt,
                    existing,
                    close_poll=self._parts(message)[0],
                )
                attempt.provider_mutation_attempted = True
                await self.adapter._call(  # noqa: SLF001
                    client,
                    lease,
                    self._request("EditMessageRequest", peer=target.entity, id=message.id, media=media),
                )
                readback = await client.get_messages(target.entity, ids=message.id)
                if readback is None or not getattr(self._parts(readback)[0], "closed", False):
                    raise _err(PollErrorCode.PROVIDER_OUTCOME_UNKNOWN, "poll close is unconfirmed", safe_to_retry=False)
                binding = self._new_binding(old, readback, poll_id, False)
                result = {
                    "lifecycle_state": PollLifecycle.CLOSED.value,
                    "provider_item_ref": self._item_ref(context.target_provider_ref, readback, target),
                    "provider_binding": binding,
                    "provider_state": self._state(readback),
                    "closed_at": _iso(),
                    "last_synced_at": _iso(),
                }
            elif action is PollAction.DELETE_CONTAINER:
                attempt.provider_mutation_attempted = True
                if old.get("scheduled"):
                    await self.adapter._call(  # noqa: SLF001
                        client,
                        lease,
                        self._request("DeleteScheduledMessagesRequest", peer=target.entity, id=[message_id]),
                    )
                else:
                    await client.delete_messages(target.entity, [message_id], revoke=True)
                result = {
                    "lifecycle_state": PollLifecycle.CONTAINER_DELETED.value,
                    "provider_binding": old,
                    "provider_state": {"container_deleted": True},
                    "last_synced_at": _iso(),
                }
            else:
                raise _err(PollErrorCode.POLL_UNSUPPORTED, "unsupported Telegram poll action")
            step(method, ordinal=1, state="succeeded", attempted=True, binding=result.get("provider_binding"))
            return result

        return await self.adapter._session("poll_mutation", run)  # noqa: SLF001

    async def execute(
        self,
        intent: PollActionIntent,
        *,
        context: Any,
        existing: Mapping[str, Any] | None,
        step: Any,
    ) -> Mapping[str, Any]:
        if intent.action in {PollAction.PUBLISH, PollAction.SCHEDULE}:
            return await self._create(intent, context, step)
        if existing is None:
            raise _err(PollErrorCode.POLL_REFERENCE_INVALID, "existing poll is required")
        return await self._mutate(intent, context, existing, step)

    async def _read_message(self, context: Any, existing: Mapping[str, Any], operation: str) -> tuple[Any, bool, Any]:
        target = self.adapter._target(context.target_provider_ref)  # noqa: SLF001
        binding = self._binding(existing)

        async def run(client: Any, lease: Any, _attempt: Any) -> tuple[Any, bool, Any]:
            message, scheduled = await self._locate(client, lease, target, binding)
            return message, scheduled, client

        message, scheduled, _client = await self.adapter._session(operation, run)  # noqa: SLF001
        return message, scheduled, target

    async def get(self, *, context: Any, existing: Mapping[str, Any]) -> Mapping[str, Any]:
        message, scheduled, _target = await self._read_message(context, existing, "poll_get")
        if message is None:
            return {
                "lifecycle_state": PollLifecycle.UNKNOWN.value,
                "refresh_failed": True,
                "unavailable_reason": "provider_message_not_found",
                "observed_at": _iso(),
            }
        poll, _ = self._parts(message)
        lifecycle = PollLifecycle.PROVIDER_SCHEDULED.value if scheduled else (
            PollLifecycle.CLOSED.value if getattr(poll, "closed", False) else PollLifecycle.OPEN.value
        )
        return {
            **self._state(message),
            "lifecycle_state": lifecycle,
            "provider_schedule_at": _iso(getattr(message, "date", None)) if scheduled else None,
            "published_at": None if scheduled else _iso(getattr(message, "date", None)),
            "observed_at": _iso(),
        }

    async def reconcile(self, *, context: Any, existing: Mapping[str, Any], step: Any) -> Mapping[str, Any]:
        target = self.adapter._target(context.target_provider_ref)  # noqa: SLF001
        old = self._binding(existing)

        async def run(client: Any, lease: Any, _attempt: Any) -> Mapping[str, Any]:
            message, scheduled = await self._locate(client, lease, target, old)
            if message is None:
                return {
                    "lifecycle_state": PollLifecycle.UNKNOWN.value,
                    "provider_binding": old,
                    "provider_state": {"observable": False},
                    "reconciliation_required": True,
                    "error_code": PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED.value,
                    "last_synced_at": _iso(),
                }
            poll, _ = self._parts(message)
            lifecycle = PollLifecycle.PROVIDER_SCHEDULED.value if scheduled else (
                PollLifecycle.CLOSED.value if getattr(poll, "closed", False) else PollLifecycle.OPEN.value
            )
            binding = self._new_binding(
                old,
                message,
                int(getattr(poll, "id", None) or old.get("poll_id") or 0),
                scheduled,
            )
            step("telegram_poll_reconcile", ordinal=90, state="succeeded", binding=binding)
            result = {
                "lifecycle_state": lifecycle,
                "provider_binding": binding,
                "provider_state": self._state(message),
                "schedule_mode": "provider_native" if scheduled else None,
                "provider_schedule_at": _iso(getattr(message, "date", None)) if scheduled else None,
                "published_at": None if scheduled else _iso(getattr(message, "date", None)),
                "reconciliation_required": False,
                "last_synced_at": _iso(),
            }
            if not scheduled:
                result["provider_item_ref"] = self._item_ref(context.target_provider_ref, message, target)
            return result

        return await self.adapter._session("poll_reconcile", run)  # noqa: SLF001

    @staticmethod
    def _token_map(existing: Mapping[str, Any]) -> dict[bytes, tuple[str, str]]:
        result = {}
        for option in _list(existing.get("options")):
            if not isinstance(option, Mapping) or not isinstance(option.get("provider_binding"), Mapping):
                continue
            encoded = option["provider_binding"].get("option")
            if isinstance(encoded, str):
                result[_unb64(encoded)] = (str(option.get("client_key")), str(option.get("poll_option_ref")))
        return result

    def _results(self, message: Any, existing: Mapping[str, Any]) -> list[dict[str, Any]]:
        poll, results = self._parts(message)
        total = getattr(results, "total_voters", None)
        total = int(total) if type(total) is int else None
        counts = {}
        for item in _list(getattr(results, "results", None)):
            token = getattr(item, "option", None)
            votes = getattr(item, "voters", None)
            if isinstance(token, (bytes, bytearray, memoryview)):
                votes = int(votes) if type(votes) is int else None
                counts[bytes(token)] = (votes, votes / total * 100 if votes is not None and total else None)
        token_map = self._token_map(existing)
        normalized = []
        for answer in _list(getattr(poll, "answers", None)):
            token = getattr(answer, "option", None)
            token = bytes(token) if isinstance(token, (bytes, bytearray, memoryview)) else None
            if token not in token_map:
                continue
            votes, rate = counts.get(token, (None, None))
            normalized.append({"client_key": token_map[token][0], "votes": votes, "rate": rate})
        return normalized

    async def results(self, *, context: Any, existing: Mapping[str, Any]) -> Mapping[str, Any]:
        target = self.adapter._target(context.target_provider_ref)  # noqa: SLF001
        binding = self._binding(existing)

        async def run(client: Any, lease: Any, _attempt: Any) -> Mapping[str, Any]:
            message, scheduled = await self._locate(client, lease, target, binding)
            if message is None:
                raise _err(PollErrorCode.POLL_UNSUPPORTED, "poll message cannot be located", retryable=True, safe_to_retry=True)
            if scheduled:
                return {
                    "state": PollLifecycle.PROVIDER_SCHEDULED.value,
                    "total_voters": None,
                    "options": [
                        {"client_key": item.get("client_key"), "votes": None, "rate": None}
                        for item in _list(existing.get("options"))
                        if isinstance(item, Mapping)
                    ],
                    "complete": False,
                    "source": "provider_schedule",
                    "unavailable_reason": "not_published",
                    "observed_at": _iso(),
                }
            poll, _ = self._parts(message)
            response = await self.adapter._call(  # noqa: SLF001
                client,
                lease,
                self._request(
                    "GetPollResultsRequest",
                    peer=target.entity,
                    msg_id=message.id,
                    poll_hash=getattr(poll, "hash", None),
                ),
            )
            updated = next((item for item in _messages(response) if _media(item)), None)
            updated = updated or await client.get_messages(target.entity, ids=message.id) or message
            current, values = self._parts(updated)
            total = getattr(values, "total_voters", None)
            return {
                "state": PollLifecycle.CLOSED.value if getattr(current, "closed", False) else PollLifecycle.OPEN.value,
                "total_voters": int(total) if type(total) is int else None,
                "options": self._results(updated, existing),
                "complete": True,
                "source": "provider",
                "observed_at": _iso(),
            }

        return await self.adapter._session("poll_results", run)  # noqa: SLF001

    async def voters(
        self,
        *,
        context: Any,
        existing: Mapping[str, Any],
        option_binding: Any | None,
        cursor: str | None,
        limit: int,
    ) -> Mapping[str, Any]:
        target = self.adapter._target(context.target_provider_ref)  # noqa: SLF001
        if target.kind is SocialTargetKind.CHANNEL:
            raise _err(
                PollErrorCode.POLL_VOTERS_UNAVAILABLE,
                "Telegram forbids fetching channel poll voters",
                provider_method="messages.getPollVotes",
                capability_requirement="non-broadcast target",
            )
        option = _unb64(option_binding.get("option")) if isinstance(option_binding, Mapping) else None
        if cursor is None:
            offset = ""
        elif cursor.startswith("tg_"):
            try:
                offset = _unb64(cursor[3:]).decode()
            except UnicodeDecodeError:
                raise _err(PollErrorCode.POLL_FIELD_CONFLICT, "voter cursor is invalid") from None
        else:
            raise _err(PollErrorCode.POLL_FIELD_CONFLICT, "voter cursor is invalid")
        binding = self._binding(existing)

        async def run(client: Any, lease: Any, _attempt: Any) -> Mapping[str, Any]:
            message, scheduled = await self._locate(client, lease, target, binding)
            if message is None or scheduled:
                raise _err(PollErrorCode.POLL_VOTERS_UNAVAILABLE, "voters are unavailable before publication")
            response = await self.adapter._call(  # noqa: SLF001
                client,
                lease,
                self._request(
                    "GetPollVotesRequest",
                    peer=target.entity,
                    id=message.id,
                    option=option,
                    offset=offset,
                    limit=limit,
                ),
            )
            entities = {
                key: entity
                for entity in [*_list(getattr(response, "users", None)), *_list(getattr(response, "chats", None))]
                if (key := _entity_key(entity))
            }
            token_map = self._token_map(existing)
            try:
                principal = context.base_runtime._principal_hash(context.principal)  # noqa: SLF001
            except Exception:
                principal = context.poll_ref
            voters = []
            for vote in _list(getattr(response, "votes", None))[:limit]:
                native = _peer_key(getattr(vote, "peer", None))
                if not native:
                    continue
                peer_id = native.split(":", 1)[1]
                entity = next((value for key, value in entities.items() if key.endswith(":" + peer_id)), None)
                options = []
                single = getattr(vote, "option", None)
                if isinstance(single, (bytes, bytearray, memoryview)):
                    options.append(bytes(single))
                options.extend(
                    bytes(value)
                    for value in _list(getattr(vote, "options", None))
                    if isinstance(value, (bytes, bytearray, memoryview))
                )
                item = {
                    "voter_ref": "vtr_" + _b64(
                        hashlib.sha256(f"tg-voter\0{principal}\0{context.poll_ref}\0{native}".encode()).digest()[:18]
                    ),
                    "display_name": _name(entity),
                    "option_refs": [token_map[value][1] for value in options if value in token_map],
                }
                username = getattr(entity, "username", None)
                if isinstance(username, str) and username.replace("_", "").isalnum():
                    item["profile_link"] = f"https://t.me/{username}"
                date = getattr(vote, "date", None)
                if type(date) is int and date > 0:
                    item["voted_at"] = _iso(datetime.fromtimestamp(date, timezone.utc))
                voters.append(item)
            next_offset = getattr(response, "next_offset", None)
            next_cursor = "tg_" + _b64(next_offset.encode()) if isinstance(next_offset, str) and next_offset else None
            return {
                "voters": voters,
                "complete": next_cursor is None,
                "source": "provider",
                "observed_at": _iso(),
                **({"next_cursor": next_cursor} if next_cursor else {}),
            }

        return await self.adapter._session("poll_voters", run)  # noqa: SLF001


__all__ = ["TelegramPollProvider"]

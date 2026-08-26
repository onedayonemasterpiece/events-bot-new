"""Telegram MTProto native poll provider for the Social Workspace.

The first checkpoint exposes accurate transport capabilities and validates the
closed contract. Provider mutation/read methods are completed in the following
adapter checkpoint; until then every attempted mutation fails closed before a
provider call.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from private_events_mcp.social_poll_contract import (
    PollActionIntent,
    PollErrorCode,
    PollKind,
    PollValidationError,
)


class TelegramPollProvider:
    platform = "telegram"
    transport = "telegram_mtproto"
    principal_type = "user_session"

    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter

    async def capabilities(self, *, target_provider_ref: str | None) -> Mapping[str, Any]:
        return {
            "support": "conditional",
            "provider_api_version": "mtproto-current-layer",
            "authorization": {
                "status": "ready" if target_provider_ref else "target_required",
                "missing_scopes": [],
                "missing_permissions": ([] if target_provider_ref else ["exact target resolution"]),
            },
            "create": {"supported": False, "kinds": []},
            "publish": {"immediate": False},
            "schedule": {
                "supported": False,
                "mode": "provider_native",
                "editable": True,
                "cancelable": True,
            },
            "lifecycle": {
                "close": False,
                "delete_container": False,
                "edit_published": False,
            },
            "reads": {
                "state": False,
                "results": False,
                "voters": {
                    "support": "conditional",
                    "complete_history": True,
                    "constraints": ["non_anonymous", "non_channel", "provider_permission"],
                },
            },
            "fields": {
                "question_entities": {"support": "supported"},
                "option_entities": {"support": "supported"},
                "explanation_entities": {"support": "supported", "kinds": ["quiz"]},
                "open_answers": {"support": "supported", "kinds": ["regular"]},
                "revoting_disabled": {"support": "supported"},
                "shuffle_answers": {"support": "supported"},
                "hide_results_until_close": {"support": "supported"},
                "subscribers_only": {"support": "conditional", "target_kinds": ["channel", "group"]},
                "countries_iso2": {"support": "conditional", "target_kinds": ["channel"]},
                "attached_media": {"support": "conditional", "requires": "staged_media"},
                "solution_media": {"support": "conditional", "kinds": ["quiz"], "requires": "staged_media"},
                "option_media": {"support": "conditional", "requires": "staged_media"},
            },
            "limits": {
                "options": {"minimum": 2, "maximum_source": "provider_config.poll_answers_max"},
                "close_period_seconds": {"minimum": 5, "maximum_source": "provider_config.poll_close_period_max"},
            },
            "implementation": {
                "adapter": "contract_only_provider_pending",
                "tested": "unit",
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
        poll = intent.poll
        if poll is not None and len(poll.options) < 2:
            raise PollValidationError(
                PollErrorCode.POLL_LIMIT_EXCEEDED,
                "Telegram MTProto polls require at least two options",
                field_path="content.poll.options",
                platform="telegram",
                transport=self.transport,
            )
        return {
            "summary": f"{intent.action.value} Telegram native poll",
            "schedule_mode": "provider_native" if intent.action.value in {"schedule", "poll_reschedule"} else None,
            "provider_schedule_at": intent.schedule_at_utc,
            "compatibility_transformations": [],
            "available_after_commit": ["state", "results", "close", "delete_container", "conditional_voters"],
            "safe_preview": {
                "provider_method_family": "messages.sendMedia/messages.editMessage",
                "native_poll": True,
                "provider_ids_exposed": False,
            },
        }

    async def execute(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(
            PollErrorCode.POLL_UNSUPPORTED,
            "Telegram poll provider mutation adapter is not installed",
            platform="telegram",
            transport=self.transport,
            retryable=False,
            safe_to_retry=True,
            capability_requirement="telegram poll adapter checkpoint",
        )

    async def reconcile(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(
            PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED,
            "Telegram poll reconciliation adapter is not installed",
            platform="telegram",
            transport=self.transport,
            retryable=False,
            safe_to_retry=False,
        )

    async def get(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(PollErrorCode.POLL_UNSUPPORTED, "Telegram poll reads are not installed")

    async def results(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(PollErrorCode.POLL_UNSUPPORTED, "Telegram poll results are not installed")

    async def voters(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(PollErrorCode.POLL_VOTERS_UNAVAILABLE, "Telegram poll voters are not installed")


__all__ = ["TelegramPollProvider"]

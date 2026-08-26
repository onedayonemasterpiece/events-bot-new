"""VK 5.199 native poll provider for the Social Workspace.

The first checkpoint exposes the official object/wall saga capability model and
strict validation. Provider mutation/read methods are completed in the VK
adapter checkpoint; until then calls fail before invoking VK.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from private_events_mcp.social_poll_contract import (
    CompatibilityPolicy,
    PollActionIntent,
    PollErrorCode,
    PollKind,
    PollValidationError,
)


class VKPollProvider:
    platform = "vk"
    transport = "vk_api_5_199"
    principal_type = "community_editor_user_token"

    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter

    async def capabilities(self, *, target_provider_ref: str | None) -> Mapping[str, Any]:
        return {
            "support": "conditional",
            "provider_api_version": "5.199",
            "authorization": {
                "status": "ready" if target_provider_ref else "target_required",
                "missing_scopes": [],
                "missing_permissions": ([] if target_provider_ref else ["exact community target resolution"]),
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
                "delete_poll_object": False,
            },
            "reads": {
                "state": False,
                "results": False,
                "voters": {
                    "support": "conditional",
                    "complete_history": True,
                    "constraints": ["public_poll", "token_permission"],
                },
            },
            "fields": {
                "question_entities": {"support": "explicit_best_effort", "transformation": "strip_formatting"},
                "option_entities": {"support": "explicit_best_effort", "transformation": "strip_formatting"},
                "disable_unvote": {"support": "supported"},
                "background_ref": {"support": "conditional", "allowed_values_source": "polls.getBackgrounds"},
                "photo_asset_ref": {"support": "conditional", "requires": "poll photo upload permission"},
                "question_edit": {"support": "supported", "editable_after_publish": True},
                "answer_add": {"support": "supported", "editable_after_publish": True},
                "answer_edit": {"support": "supported", "editable_after_publish": True},
                "answer_delete": {"support": "supported", "editable_after_publish": True},
                "end_date_edit": {"support": "supported", "editable_after_publish": True},
            },
            "limits": {
                "options": {"minimum": 1, "maximum": 10},
                "voter_page": {"maximum": 1000, "mcp_server_maximum": 100},
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
        if poll is not None and poll.kind is PollKind.QUIZ:
            raise PollValidationError(
                PollErrorCode.POLL_FIELD_UNSUPPORTED,
                "VK does not provide quiz semantics",
                field_path="content.poll.kind",
                platform="vk",
                transport=self.transport,
                capability_requirement="create.kinds=regular",
            )
        if poll is not None and len(poll.options) > 10:
            raise PollValidationError(
                PollErrorCode.POLL_LIMIT_EXCEEDED,
                "VK supports at most ten poll answers",
                field_path="content.poll.options",
                platform="vk",
                transport=self.transport,
            )
        transformations: list[dict[str, Any]] = []
        if poll is not None:
            formatted = bool(poll.question.entities) or any(option.text.entities for option in poll.options)
            if formatted:
                if poll.compatibility_policy is not CompatibilityPolicy.EXPLICIT_BEST_EFFORT:
                    raise PollValidationError(
                        PollErrorCode.POLL_FIELD_UNSUPPORTED,
                        "VK poll text does not preserve rich entities",
                        field_path="content.poll.question.entities",
                        platform="vk",
                        transport=self.transport,
                        capability_requirement="compatibility_policy=explicit_best_effort",
                    )
                transformations.append(
                    {
                        "field_paths": ["content.poll.question.entities", "content.poll.options[].text.entities"],
                        "transformation": "strip_formatting_keep_text",
                        "semantic_change": False,
                    }
                )
        return {
            "summary": f"{intent.action.value} VK native poll attachment",
            "schedule_mode": "provider_native" if intent.action.value in {"schedule", "poll_reschedule"} else None,
            "provider_schedule_at": intent.schedule_at_utc,
            "compatibility_transformations": transformations,
            "available_after_commit": ["state", "results", "conditional_voters", "edit", "cancel", "close", "delete_container"],
            "safe_preview": {
                "provider_saga": ["polls.create", "wall.post"],
                "native_poll": True,
                "provider_ids_exposed": False,
                "orphan_compensation": "record_only_provider_has_no_poll_delete",
            },
        }

    async def execute(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(
            PollErrorCode.POLL_UNSUPPORTED,
            "VK poll provider mutation adapter is not installed",
            platform="vk",
            transport=self.transport,
            retryable=False,
            safe_to_retry=True,
            capability_requirement="VK poll adapter checkpoint",
        )

    async def reconcile(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(
            PollErrorCode.PROVIDER_RECONCILIATION_REQUIRED,
            "VK poll reconciliation adapter is not installed",
            platform="vk",
            transport=self.transport,
            retryable=False,
            safe_to_retry=False,
        )

    async def get(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(PollErrorCode.POLL_UNSUPPORTED, "VK poll reads are not installed")

    async def results(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(PollErrorCode.POLL_UNSUPPORTED, "VK poll results are not installed")

    async def voters(self, *args: Any, **kwargs: Any) -> Mapping[str, Any]:
        raise PollValidationError(PollErrorCode.POLL_VOTERS_UNAVAILABLE, "VK poll voters are not installed")


__all__ = ["VKPollProvider"]

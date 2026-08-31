from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

import pytest
from jsonschema import Draft202012Validator, ValidationError, validate

from private_events_mcp.access_policy import CODEX_MAX_SCOPES
from private_events_mcp.social_workspace import (
    DIRECT_USER_AUTHORIZED_ACTIONS,
    SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_SCHEMA,
    SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_SEARCH_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_SCHEMA,
    SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_READ_SCHEMA,
    SOCIAL_WORKSPACE_RETRY_SCHEMA,
    SOCIAL_WORKSPACE_SCHEDULED_ITEMS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_SCHEDULED_ITEMS_SCHEMA,
    SOCIAL_WORKSPACE_SCOPES,
    SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA,
    SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_GET_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
    ApprovalContext,
    ApprovalGrant,
    AuditAppendResult,
    ContentFeature,
    DurableIdempotencyReservation,
    EditorialSampleState,
    ExecutionSafetyHooks,
    GateDecision,
    MediaRole,
    RecursiveRedactionResult,
    SafetyAuditEvent,
    SocialAction,
    SocialActionStatus,
    SocialReactionPreset,
    SocialReadPurpose,
    SocialTargetKind,
    SocialWorkspaceValidationError,
    compute_action_digest,
    enforce_action_gates,
    enforce_editorial_sample_gates,
    enforce_execution_safety,
    required_scope_for_action,
    required_scope_for_read,
    validate_action_status_response,
    validate_asset_stage_request,
    validate_asset_status_request,
    validate_capabilities,
    validate_commit_request,
    validate_editorial_sample_response,
    validate_opaque_ref,
    validate_prepare_request,
    validate_read_request,
    validate_retry_request,
    validate_scheduled_items_request,
    validate_resolved_target_preview,
    validate_send_message_receipt,
    validate_status_request,
)

TARGET_REF = "tgt_abcdefghijklmnop"
OTHER_TARGET_REF = "tgt_ponmlkjihgfedcba"
ITEM_REF = "itm_abcdefghijklmnop"
ASSET_REF = "ast_abcdefghijklmnop"
PREPARATION_REF = "prep_" + "a" * 24
OPERATION_REF = "op_" + "b" * 24
SAMPLE_REF = "smp_" + "c" * 24
APPROVAL_REF = "apr_" + "d" * 24
APPROVAL_RECEIPT = "arc_" + "e" * 24
UPLOAD_REF = "upl_" + "f" * 24


ALL_SCHEMAS = (
    SOCIAL_WORKSPACE_READ_SCHEMA,
    SOCIAL_WORKSPACE_SCHEDULED_ITEMS_SCHEMA,
    SOCIAL_WORKSPACE_SCHEDULED_ITEMS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_RETRY_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA,
    SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA,
    SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_GET_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_SEARCH_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ITEM_RESOLVE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA,
)


def _property_names(value: Any) -> set[str]:
    names: set[str] = set()
    if isinstance(value, Mapping):
        properties = value.get("properties")
        if isinstance(properties, Mapping):
            names.update(str(name) for name in properties)
        for child in value.values():
            names.update(_property_names(child))
    elif isinstance(value, list):
        for child in value:
            names.update(_property_names(child))
    return names


def _send_message() -> dict[str, Any]:
    return {
        "platform": "telegram",
        "action": "send_message",
        "idempotency_key": "reminder-1234",
        "target_ref": TARGET_REF,
        "content": {
            "text": "Привет, встреча в 19:00 ✨",
            "entities": [
                {"kind": "bold", "offset": 0, "length": 6},
                {
                    "kind": "custom_emoji",
                    "offset": 24,
                    "length": 1,
                    "custom_emoji_asset_ref": ASSET_REF,
                },
            ],
            "media": [
                {"asset_ref": ASSET_REF, "role": "image", "alt_text": "Афиша"}
            ],
        },
    }


def _capabilities() -> dict[str, Any]:
    return {
        "platform": "telegram",
        "target_ref": TARGET_REF,
        "target_kinds": ["user"],
        "read_operations": ["resolve_target", "get_item"],
        "actions": ["send_message"],
        "content_features": ["rich_text", "custom_emoji", "image"],
        "max_text_length": 4096,
        "max_media_items": 10,
    }


def _editorial(**updates: Any) -> dict[str, Any]:
    payload = {
        "platform": "vk",
        "operation": "editorial_sample",
        "target_ref": TARGET_REF,
        "expected_target_kinds": ["community"],
        "read_access": "public",
        "purpose": "editorial_analysis",
        "authorization_basis": "operator_authorized",
        "date_from": "2026-07-01",
        "date_to": "2026-08-08",
        "page_size": 25,
        "total_limit": 100,
    }
    payload.update(updates)
    return payload


def _editorial_state(request, **updates: Any) -> EditorialSampleState:
    state = EditorialSampleState(
        sample_ref=SAMPLE_REF,
        target_ref=TARGET_REF,
        target_kinds=frozenset({SocialTargetKind.COMMUNITY}),
        purpose=SocialReadPurpose.EDITORIAL_ANALYSIS,
        date_from="2026-07-01",
        date_to="2026-08-08",
        total_limit=100,
        cumulative_count=0,
        server_minted=True,
        continuation_cursor=request.cursor,
        cursor_server_minted=request.cursor is not None,
        ephemeral=True,
        durable_index=False,
    )
    return replace(state, **updates)


def test_telegram_item_link_and_audio_flag_are_optional_compatible_additions() -> None:
    base = {
        "platform": "telegram",
        "operation": "resolve_item",
        "target_locator": {
            "kind": "profile_link",
            "value": "https://t.me/example_channel/42",
        },
        "read_access": "public",
    }
    validate(base, SOCIAL_WORKSPACE_READ_SCHEMA)
    assert validate_read_request(base).transcribe_audio is True
    disabled = {**base, "transcribe_audio": False}
    validate(disabled, SOCIAL_WORKSPACE_READ_SCHEMA)
    assert validate_read_request(disabled).transcribe_audio is False
    assert "transcribe_audio" not in SOCIAL_WORKSPACE_READ_SCHEMA["required"]
    assert validate_read_request(base).transcription_wait_seconds == 0
    for seconds in (0, 25, 30):
        request = {
            **base,
            "transcribe_audio": True,
            "transcription_wait_seconds": seconds,
        }
        validate(request, SOCIAL_WORKSPACE_READ_SCHEMA)
        assert validate_read_request(request).transcription_wait_seconds == seconds
    for invalid in (True, "25", 1.5, -1, 31):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_read_request(
                {
                    **base,
                    "transcribe_audio": True,
                    "transcription_wait_seconds": invalid,
                }
            )
    with pytest.raises(SocialWorkspaceValidationError):
        validate_read_request({**base, "transcription_wait_seconds": 1})
    with pytest.raises(ValidationError):
        validate({**base, "transcription_wait_seconds": 1}, SOCIAL_WORKSPACE_READ_SCHEMA)
    with pytest.raises(SocialWorkspaceValidationError):
        validate_read_request(
            {
                **base,
                "transcribe_audio": False,
                "transcription_wait_seconds": 0,
            }
        )
    with pytest.raises(SocialWorkspaceValidationError):
        validate_read_request(
            {
                **base,
                "platform": "vk",
                "target_locator": {
                    "kind": "profile_link",
                    "value": "https://vk.com/wall-1_2",
                },
                "transcription_wait_seconds": 1,
            }
        )
    with pytest.raises(
        SocialWorkspaceValidationError,
        match="unsupported for this read operation",
    ):
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "search_items",
                "query": "voice",
                "read_access": "private",
                "transcribe_audio": True,
                "transcription_wait_seconds": 1,
            }
        )


def test_exact_item_resolution_infers_access_for_legacy_chatgpt_calls() -> None:
    private_request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "resolve_item",
            "target_locator": {
                "kind": "profile_link",
                "value": "https://t.me/c/100/500",
            },
            # The old generic ChatGPT schema advertised this resolve_target-only
            # hint and did not require read_access.
            "expected_target_kinds": ["group"],
        }
    )
    assert private_request.read_access.value == "private"
    assert private_request.expected_target_kinds == (SocialTargetKind.GROUP,)

    public_request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "resolve_item",
            "target_locator": {
                "kind": "profile_link",
                "value": "https://t.me/example_channel/42",
            },
        }
    )
    assert public_request.read_access.value == "public"


@pytest.mark.parametrize(
    "expected_target_kinds",
    [["self"], ["group", "channel"]],
)
def test_exact_item_resolution_rejects_unsafe_legacy_target_kind_hints(
    expected_target_kinds: list[str],
) -> None:
    with pytest.raises(
        SocialWorkspaceValidationError,
        match="at most one non-self target kind",
    ):
        validate_read_request(
            {
                "platform": "telegram",
                "operation": "resolve_item",
                "target_locator": {
                    "kind": "profile_link",
                    "value": "https://t.me/c/100/500",
                },
                "expected_target_kinds": expected_target_kinds,
            }
        )


def test_item_output_schema_accepts_additive_safe_audio_details() -> None:
    payload = {
        "item": {
            "item_ref": ITEM_REF,
            "target_ref": TARGET_REF,
            "kind": "message",
            "published_at": "2026-08-25T12:00:00Z",
            "text": "",
            "caption": "",
            "basic_metrics": {"views": 0},
            "media": [ASSET_REF],
            "attachments": [
                {
                    "asset_ref": ASSET_REF,
                    "kind": "voice",
                    "mime_type": "audio/ogg",
                    "byte_length": 12,
                    "duration_seconds": 1.5,
                    "transcription": {
                        "status": "ready",
                        "transcription_ref": "atr_" + "x" * 24,
                        "text": "external transcript",
                        "cache_hit": True,
                        "created": False,
                        "text_included": True,
                        "truncated": False,
                        "next_offset": None,
                        "next_poll_after_seconds": 0,
                        "trust": "untrusted_external_data",
                    },
                    "trust": "untrusted_external_data",
                }
            ],
            "trust": "untrusted_external_data",
        },
        "transcription_summary": {
            "total": 1,
            "ready": 1,
            "queued": 0,
            "running": 0,
            "failed": 0,
            "cache_hits": 1,
            "created": 0,
            "wait_expired": False,
            "next_poll_after_seconds": 0,
        },
        "trust": "untrusted_external_data",
    }
    validate(payload, SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA)


def _approval_grant(context: ApprovalContext, **updates: Any) -> ApprovalGrant:
    grant = ApprovalGrant(
        approval_ref=APPROVAL_REF,
        approval_receipt=APPROVAL_RECEIPT,
        client_id=context.client_id,
        subject=context.subject,
        resource=context.resource,
        action_digest=context.action_digest,
        preparation_ref=context.preparation_ref,
        preparation_expires_at=context.preparation_expires_at,
        durable_state=True,
        expires_at="2030-01-01T00:00:00Z",
        one_time=True,
        prior_uses=0,
        consumed_now=True,
    )
    return replace(grant, **updates)


def test_schemas_are_valid_closed_and_have_no_native_escape_hatches() -> None:
    for schema in ALL_SCHEMAS:
        Draft202012Validator.check_schema(schema)
    property_names = set().union(*(_property_names(schema) for schema in ALL_SCHEMAS))
    assert property_names.isdisjoint(
        {
            "method",
            "kwargs",
            "html",
            "path",
            "file_path",
            "fetch_url",
            "raw_method",
            "raw_kwargs",
            "raw_html",
        }
    )
    assert all(schema.get("additionalProperties") is False for schema in ALL_SCHEMAS)


def test_action_enum_and_granular_scope_architecture_are_exact() -> None:
    assert {action.value for action in SocialAction} == {
        "send_message", "publish", "edit", "delete", "forward", "reaction",
        "comment", "schedule", "story",
    }
    suffixes = {
        "discover", "read:public", "read:private", "read:dialogs", "dm:send",
        "post:publish", "edit", "delete", "forward", "reaction", "comment",
        "schedule", "story:read", "story:write", "analytics", "audience",
    }
    assert SOCIAL_WORKSPACE_SCOPES == {
        f"{platform}:{suffix}" for platform in ("telegram", "vk") for suffix in suffixes
    } | {"vk:notifications:read"}
    assert SOCIAL_WORKSPACE_SCOPES.isdisjoint(CODEX_MAX_SCOPES)
    assert required_scope_for_action("telegram", "send_message") == {"telegram:dm:send"}
    assert required_scope_for_action("vk", "publish") == {"vk:post:publish"}
    assert required_scope_for_action("vk", "delete") == {"vk:delete"}
    assert required_scope_for_read("telegram", "resolve_target") == {"telegram:discover"}
    assert required_scope_for_read("vk", "get_item", "private") == {"vk:read:private"}
    assert required_scope_for_read("vk", "list_dialogs", "dialogs") == {
        "vk:read:dialogs"
    }
    assert required_scope_for_read("vk", "list_stories") == {"vk:story:read"}
    assert required_scope_for_read("vk", "get_statistics") == {"vk:analytics"}
    assert required_scope_for_read("vk", "get_audience") == {"vk:audience"}
    assert required_scope_for_read("vk", "list_notifications") == {
        "vk:notifications:read"
    }
    with pytest.raises(SocialWorkspaceValidationError):
        required_scope_for_read("telegram", "list_notifications")
    with pytest.raises(SocialWorkspaceValidationError):
        required_scope_for_read("vk", "get_item")


def test_vk_dialog_listing_is_metadata_only_and_strictly_bound() -> None:
    payload = {
        "platform": "vk",
        "operation": "list_dialogs",
        "read_access": "dialogs",
        "unread_only": True,
        "limit": 20,
    }
    validate(payload, SOCIAL_WORKSPACE_READ_SCHEMA)
    request = validate_read_request(payload)
    assert request.unread_only is True
    assert request.required_scopes == {"vk:read:dialogs"}
    validate(
        {
            "results": [
                {
                    "target_ref": TARGET_REF,
                    "kind": "user",
                    "title": "Ticket winner",
                    "unread_count": 2,
                    "trust": "untrusted_external_data",
                }
            ],
            "trust": "untrusted_external_data",
        },
        SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA,
    )
    with pytest.raises(ValidationError):
        validate(
            {
                "results": [
                    {
                        "target_ref": TARGET_REF,
                        "kind": "user",
                        "title": "Ticket winner",
                        "unread_count": 2,
                        "text": "must never cross metadata-only boundary",
                        "trust": "untrusted_external_data",
                    }
                ],
                "trust": "untrusted_external_data",
            },
            SOCIAL_WORKSPACE_DIALOG_LIST_OUTPUT_SCHEMA,
        )
    for mutation in (
        {**payload, "platform": "telegram"},
        {**payload, "read_access": "private"},
        {**payload, "limit": 26},
        {**payload, "target_ref": TARGET_REF},
        {**payload, "unread_only": "yes"},
    ):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_read_request(mutation)
    with pytest.raises(SocialWorkspaceValidationError):
        validate_read_request(
            {
                "platform": "vk",
                "operation": "list_items",
                "target_ref": TARGET_REF,
                "read_access": "dialogs",
                "unread_only": True,
            }
        )


def test_saved_messages_self_locator_has_no_value_and_resolves_only_self() -> None:
    payload = {
        "platform": "telegram",
        "operation": "resolve_target",
        "target_locator": {"kind": "self"},
        "expected_target_kinds": ["self"],
    }
    validate(payload, SOCIAL_WORKSPACE_READ_SCHEMA)
    request = validate_read_request(payload)
    assert request.target_locator is not None and request.target_locator.value is None
    preview = {
        "platform": "telegram",
        "target_ref": TARGET_REF,
        "kind": "self",
        "display_name": "Избранное",
        "is_exact_match": True,
        "trust": "untrusted_external_data",
    }
    assert validate_resolved_target_preview(request, preview) == TARGET_REF
    for locator in ({"kind": "self", "value": "me"}, {"kind": "self", "value": "123"}):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_read_request(
                {
                    "platform": "telegram",
                    "operation": "resolve_target",
                    "target_locator": locator,
                    "expected_target_kinds": ["self"],
                }
            )


@pytest.mark.parametrize(
    ("platform", "locator"),
    [
        ("telegram", {"kind": "username", "value": "@exact_person"}),
        ("telegram", {"kind": "profile_link", "value": "https://t.me/exact_person"}),
        ("vk", {"kind": "provider_id", "value": "123456"}),
    ],
)
def test_exact_person_locators_bind_requested_user_kind_and_canonical_preview(
    platform: str, locator: dict[str, str]
) -> None:
    request_payload = {
        "platform": platform,
        "operation": "resolve_target",
        "target_locator": locator,
        "expected_target_kinds": ["user"],
    }
    validate(request_payload, SOCIAL_WORKSPACE_READ_SCHEMA)
    request = validate_read_request(request_payload)
    preview = {
        "platform": platform,
        "target_ref": TARGET_REF,
        "kind": "user",
        "display_name": "Точный пользователь",
        "canonical_handle": "exact_person",
        "is_exact_match": True,
        "trust": "untrusted_external_data",
    }
    validate(preview, SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA)
    assert validate_resolved_target_preview(request, preview) == TARGET_REF
    with pytest.raises(SocialWorkspaceValidationError, match="kind mismatch"):
        validate_resolved_target_preview(request, {**preview, "kind": "channel"})


@pytest.mark.parametrize(
    ("platform", "kind", "locator"),
    [
        ("telegram", "channel", {"kind": "username", "value": "public_channel"}),
        ("telegram", "group", {"kind": "provider_id", "value": "-123456"}),
        ("vk", "community", {"kind": "profile_link", "value": "https://vk.com/community"}),
    ],
)
def test_exact_nonself_resolution_supports_one_declared_target_kind(
    platform: str, kind: str, locator: dict[str, str]
) -> None:
    request = validate_read_request({
        "platform": platform,
        "operation": "resolve_target",
        "target_locator": locator,
        "expected_target_kinds": [kind],
    })
    preview = {
        "platform": platform,
        "target_ref": TARGET_REF,
        "kind": kind,
        "display_name": "Exact target",
        "is_exact_match": True,
        "trust": "untrusted_external_data",
    }
    assert validate_resolved_target_preview(request, preview) == TARGET_REF
    with pytest.raises(SocialWorkspaceValidationError, match="one non-self"):
        validate_read_request({
            "platform": platform,
            "operation": "resolve_target",
            "target_locator": locator,
            "expected_target_kinds": [kind, "user"],
        })


def test_profile_locator_rejects_arbitrary_or_credentialed_links() -> None:
    for link in (
        "https://evil.example/person",
        "https://user:pass@t.me/person",
        "http://t.me/person",
        "https://vk.com/person?redirect=evil",
    ):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_read_request(
                {
                    "platform": "telegram" if "t.me" in link else "vk",
                    "operation": "resolve_target",
                    "target_locator": {"kind": "profile_link", "value": link},
                    "expected_target_kinds": ["user"],
                }
            )


def test_only_typed_opaque_references_cross_action_boundary() -> None:
    assert validate_opaque_ref(TARGET_REF, "target") == TARGET_REF
    for value in ("@person", "123456", "https://t.me/person", "tgt_short"):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_opaque_ref(value, "target")
    with pytest.raises(SocialWorkspaceValidationError):
        validate_opaque_ref(TARGET_REF, "item")
    with pytest.raises(SocialWorkspaceValidationError, match="unsupported request field"):
        validate_prepare_request({**_send_message(), "method": "messages.send"})


def test_editorial_sample_is_single_target_bounded_and_continuation_is_paired() -> None:
    validate(_editorial(), SOCIAL_WORKSPACE_READ_SCHEMA)
    initial = validate_read_request(_editorial())
    assert initial.page_size == 25 and initial.total_limit == 100
    assert initial.required_scopes == {"vk:read:public"}
    continued = validate_read_request(
        _editorial(sample_ref=SAMPLE_REF, cursor="next_page_2")
    )
    assert continued.sample_ref == SAMPLE_REF
    for mutation in (
        {"page_size": 26},
        {"total_limit": 101},
        {"query": "cross-target expansion"},
        {"target_locator": {"kind": "username", "value": "another"}},
        {"expected_target_kinds": ["user"]},
        {"expected_target_kinds": ["self"]},
        {"read_access": "dialogs"},
        {"sample_ref": SAMPLE_REF},
        {"cursor": "orphan_cursor"},
    ):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_read_request(_editorial(**mutation))


def test_editorial_state_is_server_minted_immutable_ephemeral_and_cumulative() -> None:
    initial = validate_read_request(_editorial())
    allowed = lambda _request: GateDecision(True, "approved")
    state = enforce_editorial_sample_gates(
        initial,
        consent_hook=allowed,
        purpose_hook=allowed,
        ephemeral_policy_hook=allowed,
        state_hook=lambda request: _editorial_state(request),
    )
    assert state.sample_ref == SAMPLE_REF
    continued = validate_read_request(
        _editorial(sample_ref=SAMPLE_REF, cursor="next_page_2", page_size=10)
    )
    enforce_editorial_sample_gates(
        continued,
        consent_hook=allowed,
        purpose_hook=allowed,
        ephemeral_policy_hook=allowed,
        state_hook=lambda request: _editorial_state(request, cumulative_count=75),
    )
    bad_states = (
        {"server_minted": False},
        {"ephemeral": False},
        {"durable_index": True},
        {"target_ref": OTHER_TARGET_REF},
        {"date_from": "2026-06-01"},
        {"total_limit": 99},
        {"cumulative_count": 95},
        {"cursor_server_minted": False},
        {"continuation_cursor": "other_cursor"},
    )
    for mutation in bad_states:
        with pytest.raises(SocialWorkspaceValidationError):
            enforce_editorial_sample_gates(
                continued,
                consent_hook=allowed,
                purpose_hook=allowed,
                ephemeral_policy_hook=allowed,
                state_hook=lambda request, mutation=mutation: _editorial_state(request, **mutation),
            )
    with pytest.raises(SocialWorkspaceValidationError, match="ephemeral_policy denied"):
        enforce_editorial_sample_gates(
            initial,
            consent_hook=allowed,
            purpose_hook=allowed,
            ephemeral_policy_hook=lambda request: GateDecision(False, "index_requested"),
            state_hook=lambda request: _editorial_state(request),
        )


def test_editorial_output_requires_profile_fields_metrics_trust_and_stays_bounded() -> None:
    page = {
        "sample_ref": SAMPLE_REF,
        "target": {
            "target_ref": TARGET_REF,
            "kind": "community",
            "title": "Канал",
            "about": "О канале",
            "description": "Редакционная политика",
            "basic_metrics": {"members": 1000},
            "trust": "untrusted_external_data",
        },
        "items": [
            {
                "item_ref": f"itm_{index:016d}",
                "kind": "post",
                "published_at": "2026-08-08T12:00:00Z",
                "text": "т" * 768,
                "caption": "п" * 256,
                "basic_metrics": {"views": 100, "reactions": 4},
                "trust": "untrusted_external_data",
            }
            for index in range(25)
        ],
        "sampled_count": 25,
        "cumulative_count": 25,
        "total_limit": 100,
        "next_cursor": "next_page_2",
        "storage_disposition": "ephemeral_no_index",
        "trust": "untrusted_external_data",
    }
    validate(page, SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA)
    request = validate_read_request(_editorial())
    state = _editorial_state(request)
    assert validate_editorial_sample_response(request, state, page) == 25
    assert len(json.dumps(page, ensure_ascii=False).encode("utf-8")) < 128 * 1024
    for target_mutation in (
        {"about": None},
        {"description": None},
        {"basic_metrics": {}},
        {"trust": "trusted_instructions"},
    ):
        target = dict(page["target"])
        if next(iter(target_mutation.values())) is None:
            target.pop(next(iter(target_mutation)))
        else:
            target.update(target_mutation)
        with pytest.raises(ValidationError):
            validate({**page, "target": target}, SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA)

    adversarial_pages = (
        {"sample_ref": "smp_" + "z" * 24},
        {"sampled_count": 24},
        {"cumulative_count": 24},
        {"target": {**page["target"], "target_ref": OTHER_TARGET_REF}},
        {"target": {**page["target"], "kind": "user"}},
        {"items": [{**page["items"][0], "published_at": "2026-06-30T23:59:59Z"}]},
    )
    for mutation in adversarial_pages:
        with pytest.raises(SocialWorkspaceValidationError):
            validate_editorial_sample_response(request, state, {**page, **mutation})
    with pytest.raises(SocialWorkspaceValidationError, match="state binding"):
        validate_editorial_sample_response(request, replace(state, date_from="2026-06-01"), page)
    final_state = _editorial_state(request, cumulative_count=75)
    final_page = {**page, "cumulative_count": 100, "next_cursor": "should_not_exist"}
    with pytest.raises(SocialWorkspaceValidationError, match="without remaining"):
        validate_editorial_sample_response(request, final_state, final_page)


def test_rich_send_prepare_and_capability_gates_are_fail_closed() -> None:
    validate(_send_message(), SOCIAL_WORKSPACE_PREPARE_SCHEMA)
    intent = validate_prepare_request(_send_message())
    assert intent.action is SocialAction.SEND_MESSAGE
    assert intent.required_scopes == {"telegram:dm:send"}
    assert intent.content is not None
    assert intent.content.features == {
        ContentFeature.RICH_TEXT, ContentFeature.CUSTOM_EMOJI, ContentFeature.IMAGE
    }
    capabilities = validate_capabilities(_capabilities())
    enforce_action_gates(
        intent,
        consent_hook=lambda value: GateDecision(True, "approved"),
        policy_hook=lambda value: GateDecision(True, "approved"),
        capability_hook=lambda value: capabilities,
    )
    with pytest.raises(SocialWorkspaceValidationError, match="consent denied"):
        enforce_action_gates(
            intent,
            consent_hook=lambda value: GateDecision(False, "no_consent"),
            policy_hook=lambda value: GateDecision(True, "approved"),
            capability_hook=lambda value: capabilities,
        )


def test_prepare_output_allows_direct_user_authorization_and_stable_digest() -> None:
    intent = validate_prepare_request(_send_message())
    digest = compute_action_digest(intent)
    assert digest == compute_action_digest(intent) and len(digest) == 64
    prepared = {
        "preparation_ref": PREPARATION_REF,
        "action": "send_message",
        "status": "awaiting_human_approval",
        "action_digest": digest,
        "target_ref": TARGET_REF,
        "summary": "Send a direct reminder to the resolved person",
        "expires_at": "2026-08-08T13:00:00Z",
        "required_scopes": ["telegram:dm:send"],
    }
    validate(prepared, SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA)
    validate({**prepared, "status": "approved"}, SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA)


def test_direct_user_authorization_is_outbound_only_not_edit_or_delete() -> None:
    assert DIRECT_USER_AUTHORIZED_ACTIONS == {
        SocialAction.SEND_MESSAGE,
        SocialAction.PUBLISH,
        SocialAction.FORWARD,
        SocialAction.REACTION,
        SocialAction.COMMENT,
        SocialAction.SCHEDULE,
        SocialAction.STORY,
    }
    assert SocialAction.EDIT not in DIRECT_USER_AUTHORIZED_ACTIONS
    assert SocialAction.DELETE not in DIRECT_USER_AUTHORIZED_ACTIONS


def test_github_added_reaction_preset_is_closed_telegram_only_contract() -> None:
    payload = {
        "platform": "telegram",
        "action": "reaction",
        "idempotency_key": "github-added-0001",
        "item_ref": ITEM_REF,
        "reaction_preset": "github_added",
    }
    validate(payload, SOCIAL_WORKSPACE_PREPARE_SCHEMA)
    intent = validate_prepare_request(payload)
    assert intent.reaction is None
    assert intent.reaction_preset is SocialReactionPreset.GITHUB_ADDED

    with pytest.raises(ValidationError):
        validate({**payload, "reaction": "✅"}, SOCIAL_WORKSPACE_PREPARE_SCHEMA)
    with pytest.raises(SocialWorkspaceValidationError, match="exactly one"):
        validate_prepare_request({**payload, "reaction": "✅"})
    with pytest.raises(SocialWorkspaceValidationError, match="Telegram-only"):
        validate_prepare_request({**payload, "platform": "vk"})


def test_commit_requires_external_atomic_one_use_approval_bound_to_identity_and_digest() -> None:
    intent = validate_prepare_request(_send_message())
    digest = compute_action_digest(intent)
    context = ApprovalContext(
        "chatgpt-client",
        "owner",
        "https://resource",
        digest,
        PREPARATION_REF,
        "2027-01-01T00:00:00Z",
        True,
    )
    payload = {
        "preparation_ref": PREPARATION_REF,
        "approval_ref": APPROVAL_REF,
        "approval_receipt": APPROVAL_RECEIPT,
        "action_digest": digest,
    }
    validate(payload, SOCIAL_WORKSPACE_COMMIT_SCHEMA)
    with pytest.raises(SocialWorkspaceValidationError, match="hook is required"):
        validate_commit_request(payload, context=context)
    committed = validate_commit_request(
        payload,
        context=context,
        approval_hook=lambda approval_ref, receipt, binding: _approval_grant(binding),
        now=datetime(2026, 8, 8, tzinfo=timezone.utc),
    )
    assert committed.approval_ref == APPROVAL_REF
    adversarial = (
        {"client_id": "other-client"},
        {"subject": "other-subject"},
        {"resource": "https://other-resource"},
        {"action_digest": "0" * 64},
        {"preparation_ref": "prep_" + "z" * 24},
        {"preparation_expires_at": "2027-02-01T00:00:00Z"},
        {"durable_state": False},
        {"expires_at": "2020-01-01T00:00:00Z"},
        {"one_time": False},
        {"prior_uses": 1},
        {"consumed_now": False},
    )
    for mutation in adversarial:
        with pytest.raises(SocialWorkspaceValidationError):
            validate_commit_request(
                payload,
                context=context,
                approval_hook=lambda a, r, binding, mutation=mutation: _approval_grant(
                    binding, **mutation
                ),
                now=datetime(2026, 8, 8, tzinfo=timezone.utc),
            )
    with pytest.raises(SocialWorkspaceValidationError):
        validate_commit_request({"preparation_ref": PREPARATION_REF, "confirm": True})
    arbitrary_preparation = {**payload, "preparation_ref": "prep_" + "z" * 24}
    with pytest.raises(SocialWorkspaceValidationError, match="preparation"):
        validate_commit_request(
            arbitrary_preparation,
            context=context,
            approval_hook=lambda a, r, binding: _approval_grant(binding),
            now=datetime(2026, 8, 8, tzinfo=timezone.utc),
        )


def test_status_contract_models_provider_uncertainty_and_disallows_blind_retry() -> None:
    assert {
        "awaiting_human_approval", "approved", "provider_attempted", "outcome_unknown"
    }.issubset({status.value for status in SocialActionStatus})
    unknown = {
        "platform": "vk",
        "operation_ref": OPERATION_REF,
        "action": "publish",
        "status": "outcome_unknown",
        "retry_safe": False,
    }
    validate(unknown, SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA)
    assert validate_action_status_response(unknown) is SocialActionStatus.OUTCOME_UNKNOWN
    with pytest.raises(ValidationError):
        validate({**unknown, "retry_safe": True}, SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA)


def test_scheduled_items_contract_is_exact_bounded_and_provider_neutral() -> None:
    request = validate_scheduled_items_request(
        {
            "platform": "telegram",
            "target_ref": TARGET_REF,
            "scheduled_from": "2026-08-31T08:00:00Z",
            "scheduled_to": "2026-08-31T13:00:00+02:00",
            "text_sha256": "a" * 64,
            "media_count": 4,
            "limit": 10,
        }
    )
    assert request.required_scopes == {"telegram:schedule"}
    assert request.limit == 10
    response = {
        "platform": "telegram",
        "target_ref": TARGET_REF,
        "queue": "scheduled",
        "items": [
            {
                "item_ref": ITEM_REF,
                "target_ref": TARGET_REF,
                "queue": "scheduled",
                "scheduled_at": "2026-08-31T12:00:00Z",
                "text_sha256": "a" * 64,
                "media_count": 4,
                "media_roles": ["image", "image", "image", "image"],
                "trust": "untrusted_external_data",
            }
        ],
        "exact_match_count": 1,
        "has_more": False,
        "trust": "untrusted_external_data",
    }
    validate(response, SOCIAL_WORKSPACE_SCHEDULED_ITEMS_OUTPUT_SCHEMA)
    for mutation in (
        {"limit": 26},
        {"media_count": 11},
        {"text_sha256": "not-a-digest"},
        {"scheduled_from": "2026-08-31T12:00:00Z", "scheduled_to": "2026-08-31T11:00:00Z"},
        {"provider_id": 42},
    ):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_scheduled_items_request(
                {"platform": "telegram", "target_ref": TARGET_REF, **mutation}
            )


def test_retry_and_extended_status_contracts_are_closed() -> None:
    assert validate_retry_request({"operation_ref": OPERATION_REF}) == OPERATION_REF
    with pytest.raises(SocialWorkspaceValidationError):
        validate_retry_request({"operation_ref": OPERATION_REF, "force": True})
    validate(
        {
            "platform": "telegram",
            "operation_ref": OPERATION_REF,
            "preparation_ref": PREPARATION_REF,
            "logical_action_ref": "act_" + "c" * 24,
            "action": "schedule",
            "status": "outcome_unknown",
            "retry_safe": False,
            "attempt_number": 2,
            "stage": "scheduled_history_readback",
            "mutation_boundary_reached": True,
            "scheduled_at": "2026-08-31T12:00:00Z",
            "media_count": 4,
            "reconciliation_attempt": 3,
            "next_poll_after_seconds": 10,
            "reconciliation_deadline": "2026-08-31T12:05:00Z",
            "exact_match_count": 0,
            "item_refs": [],
            "read_after_write": {
                "verified": False,
                "absence_verified": False,
                "observed_at": "2026-08-31T12:00:10Z",
            },
        },
        SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
    )


def test_exact_person_send_success_requires_read_after_write_receipt() -> None:
    receipt = {
        "platform": "telegram",
        "action": "send_message",
        "status": "succeeded",
        "retry_safe": False,
        "operation_ref": OPERATION_REF,
        "target_ref": TARGET_REF,
        "item_ref": ITEM_REF,
        "read_after_write": {
            "verified": True,
            "observed_item_ref": ITEM_REF,
            "observed_at": "2026-08-08T12:00:00Z",
        },
    }
    validate(receipt, SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA)
    assert validate_send_message_receipt(receipt) == (OPERATION_REF, ITEM_REF)
    validate(receipt, SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA)
    assert validate_action_status_response(receipt) is SocialActionStatus.SUCCEEDED
    for mutation in (
        {"read_after_write": None},
        {
            "read_after_write": {
                "verified": True,
                "observed_item_ref": "itm_ponmlkjihgfedcba",
                "observed_at": "2026-08-08T12:00:00Z",
            }
        },
    ):
        candidate = {**receipt, **mutation}
        if mutation["read_after_write"] is None:
            candidate.pop("read_after_write")
            with pytest.raises(ValidationError):
                validate(candidate, SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA)
        else:
            with pytest.raises(SocialWorkspaceValidationError, match="item mismatch"):
                validate_send_message_receipt(candidate)
            with pytest.raises(SocialWorkspaceValidationError, match="item mismatch"):
                validate_action_status_response(candidate)
    weak_proof = {
        **receipt,
        "read_after_write": {
            **receipt["read_after_write"],
            "verified": False,
        },
    }
    with pytest.raises(SocialWorkspaceValidationError, match="not verified"):
        validate_action_status_response(weak_proof)


def test_external_output_families_are_closed_bounded_and_untrusted() -> None:
    target = {
        "target_ref": TARGET_REF,
        "kind": "channel",
        "title": "Channel",
        "about": "About",
        "description": "Description",
        "basic_metrics": {"members": 2},
        "trust": "untrusted_external_data",
    }
    item = {
        "item_ref": ITEM_REF,
        "kind": "post",
        "published_at": "2026-08-08T12:00:00Z",
        "text": "External text",
        "caption": "Caption",
        "basic_metrics": {"views": 2},
        "entities": [
            {"kind": "link", "offset": 0, "length": 8, "link_target": "https://example.com"},
            {
                "kind": "custom_emoji",
                "offset": 9,
                "length": 1,
                "custom_emoji_asset_ref": ASSET_REF,
            },
        ],
        "trust": "untrusted_external_data",
    }
    samples = (
        (SOCIAL_WORKSPACE_TARGET_SEARCH_OUTPUT_SCHEMA, {"results": [target], "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_TARGET_LIST_OUTPUT_SCHEMA, {"results": [target], "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_TARGET_GET_OUTPUT_SCHEMA, {"target": target, "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_ITEM_LIST_OUTPUT_SCHEMA, {"results": [item], "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_ITEM_SEARCH_OUTPUT_SCHEMA, {"results": [item], "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_ITEM_GET_OUTPUT_SCHEMA, {"item": item, "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA, {"root_item_ref": ITEM_REF, "items": [{**item, "kind": "comment"}], "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA, {"results": [{**item, "kind": "story"}], "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_REACTIONS_OUTPUT_SCHEMA, {"item_ref": ITEM_REF, "reactions": [{"reaction": "👍", "count": 1}], "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_STATISTICS_OUTPUT_SCHEMA, {"target_ref": TARGET_REF, "period_from": "2026-08-01T00:00:00Z", "period_to": "2026-08-08T00:00:00Z", "basic_metrics": {"views": 4}, "trust": "untrusted_external_data"}),
        (SOCIAL_WORKSPACE_AUDIENCE_OUTPUT_SCHEMA, {"target_ref": TARGET_REF, "audience": {"total": 2}, "trust": "untrusted_external_data"}),
    )
    for schema, sample in samples:
        validate(sample, schema)
        with pytest.raises(ValidationError):
            validate({**sample, "trust": "trusted_instructions"}, schema)
    with pytest.raises(ValidationError):
        validate(
            {"results": [item], "trust": "untrusted_external_data"},
            SOCIAL_WORKSPACE_STORIES_OUTPUT_SCHEMA,
        )
    with pytest.raises(ValidationError):
        validate(
            {"root_item_ref": ITEM_REF, "items": [item], "trust": "untrusted_external_data"},
            SOCIAL_WORKSPACE_THREAD_OUTPUT_SCHEMA,
        )


def test_asset_lifecycle_uses_fileparams_and_opaque_refs_not_paths_or_legacy_handles() -> None:
    request = {
        "platform": "telegram",
        "file": {
            "download_url": "https://files.example.test/download?signed=1",
            "file_id": "file_chatgpt_example",
            "mime_type": "image/png",
            "file_name": "poster.png",
        },
        "role": "image",
    }
    validate(request, SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA)
    parsed = validate_asset_stage_request(request)
    assert parsed.file.file_id == "file_chatgpt_example"
    assert parsed.file.download_url.startswith("https://files.example.test/")
    assert validate_asset_status_request({"asset_ref": ASSET_REF}) == ASSET_REF
    validate({"asset_ref": ASSET_REF, "status": "ready"}, SOCIAL_WORKSPACE_ASSET_STAGE_OUTPUT_SCHEMA)
    validate(
        {
            "asset_ref": ASSET_REF,
            "status": "ready",
            "mime_type": "image/png",
            "byte_length": 1024,
            "content_digest": "sha256:" + "a" * 64,
            "expires_at": "2030-01-01T00:00:00Z",
            "trust": "untrusted_external_data",
        },
        SOCIAL_WORKSPACE_ASSET_STATUS_OUTPUT_SCHEMA,
    )
    for escape in (
        {"path": "/tmp/a.png"},
        {"url": "https://evil/a.png"},
        {"upload_ref": UPLOAD_REF},
        {"content_digest": "sha256:" + "a" * 64},
    ):
        with pytest.raises((SocialWorkspaceValidationError, ValidationError)):
            validate_asset_stage_request({**request, **escape})


def test_document_stage_is_telegram_only_and_accepts_text_mime_hint() -> None:
    request = {
        "platform": "telegram",
        "file": {
            "download_url": "https://files.example.test/download?signed=1",
            "file_id": "file_chatgpt_document",
            "mime_type": "text/markdown",
            "file_name": "notes.md",
        },
        "role": "document",
    }
    validate(request, SOCIAL_WORKSPACE_ASSET_STAGE_SCHEMA)
    assert validate_asset_stage_request(request).role is MediaRole.DOCUMENT
    with pytest.raises(SocialWorkspaceValidationError, match="only for Telegram"):
        validate_asset_stage_request({**request, "platform": "vk"})
    hostile_name = "../unsafe\u202e.apk"
    document = validate_asset_stage_request(
        {
            **request,
            "file": {**request["file"], "file_name": hostile_name},
        }
    )
    assert document.file.file_name == hostile_name
    with pytest.raises(SocialWorkspaceValidationError, match="file_name"):
        validate_asset_stage_request(
            {
                **request,
                "role": "image",
                "file": {**request["file"], "file_name": hostile_name},
            }
        )

    intent = validate_prepare_request(
        {
            "platform": "telegram",
            "action": "send_message",
            "idempotency_key": "document-valid-123",
            "target_ref": TARGET_REF,
            "content": {
                "text": "caption",
                "entities": [],
                "media": [{"asset_ref": ASSET_REF, "role": "document"}],
            },
        }
    )
    assert intent.content is not None
    assert intent.content.media[0].role is MediaRole.DOCUMENT


@pytest.mark.parametrize("platform,action,roles", [
    ("vk", "send_message", ["document"]),
    ("telegram", "publish", ["document"]),
    ("telegram", "send_message", ["document", "document"]),
    ("telegram", "send_message", ["document", "image"]),
])
def test_document_prepare_rejects_wrong_provider_action_or_cardinality(
    platform, action, roles
) -> None:
    payload = {
        "platform": platform,
        "action": action,
        "idempotency_key": "document-policy-123",
        "target_ref": TARGET_REF,
        "content": {
            "text": "caption",
            "entities": [],
            "media": [
                {"asset_ref": ASSET_REF, "role": role} for role in roles
            ],
        },
    }
    with pytest.raises(SocialWorkspaceValidationError, match="one Telegram"):
        validate_prepare_request(payload)
    with pytest.raises(ValidationError):
        validate(payload, SOCIAL_WORKSPACE_PREPARE_SCHEMA)


def _safety_hooks(
    *,
    denied: str | None = None,
    recursive: bool = True,
    durable_audit: bool = True,
    audit_events: list[SafetyAuditEvent] | None = None,
):
    def redact(value):
        def walk(node):
            if isinstance(node, dict):
                return {key: ("<redacted>" if key == "credential" else walk(child)) for key, child in node.items()}
            if isinstance(node, list):
                return [walk(child) for child in node]
            return node

        return RecursiveRedactionResult(walk(value), recursive)

    def budget(name):
        return lambda context: GateDecision(name != denied, f"{name}_decision")

    def append(event: SafetyAuditEvent) -> AuditAppendResult:
        if audit_events is not None:
            audit_events.append(event)
        return AuditAppendResult(True, durable_audit)

    return ExecutionSafetyHooks(
        recursive_redaction=redact,
        response_cap=budget("response_cap"),
        append_audit=append,
        durable_idempotency=lambda intent, digest: DurableIdempotencyReservation(
            intent.idempotency_key, digest, True, denied != "idempotency"
        ),
        rate_budget=budget("rate_budget"),
        egress_budget=budget("egress_budget"),
        media_budget=budget("media_budget"),
    )


def test_execution_safety_requires_every_hook_and_recursive_redaction() -> None:
    intent = validate_prepare_request(_send_message())
    response = {"nested": [{"credential": "secret", "text": "external"}]}
    with pytest.raises(SocialWorkspaceValidationError, match="hooks are required"):
        enforce_execution_safety(
            intent, response, client_id="client", subject="owner", resource="resource", hooks=None
        )
    result = enforce_execution_safety(
        intent,
        response,
        client_id="client",
        subject="owner",
        resource="resource",
        hooks=_safety_hooks(),
    )
    assert result["nested"][0]["credential"] == "<redacted>"
    denied_events: list[SafetyAuditEvent] = []
    with pytest.raises(SocialWorkspaceValidationError, match="recursive redaction"):
        enforce_execution_safety(
            intent,
            response,
            client_id="client",
            subject="owner",
            resource="resource",
            hooks=_safety_hooks(recursive=False, audit_events=denied_events),
        )
    assert len(denied_events) == 1 and denied_events[0].reason_code == "redaction_incomplete"


@pytest.mark.parametrize(
    "denied",
    ["idempotency", "response_cap", "rate_budget", "egress_budget", "media_budget"],
)
def test_execution_safety_fails_closed_for_idempotency_and_all_budgets(denied: str) -> None:
    intent = validate_prepare_request(_send_message())
    events: list[SafetyAuditEvent] = []
    with pytest.raises(SocialWorkspaceValidationError):
        enforce_execution_safety(
            intent,
            {"trust": "untrusted_external_data"},
            client_id="client",
            subject="owner",
            resource="resource",
            hooks=_safety_hooks(denied=denied, audit_events=events),
        )
    assert len(events) == 1 and events[0].outcome == "denied"
    with pytest.raises(SocialWorkspaceValidationError, match="encoded response cap"):
        enforce_execution_safety(
            intent,
            {"text": "x" * 2000},
            client_id="client",
            subject="owner",
            resource="resource",
            hooks=_safety_hooks(),
            encoded_response_cap=100,
        )
    with pytest.raises(SocialWorkspaceValidationError, match="audit"):
        enforce_execution_safety(
            intent,
            {},
            client_id="client",
            subject="owner",
            resource="resource",
            hooks=_safety_hooks(durable_audit=False),
        )


def test_rate_denial_appends_durable_sanitized_audit_before_raising() -> None:
    intent = validate_prepare_request(_send_message())
    events: list[SafetyAuditEvent] = []
    response = {"credential": "must-never-enter-audit", "text": "external"}
    with pytest.raises(SocialWorkspaceValidationError, match="rate_budget denied"):
        enforce_execution_safety(
            intent,
            response,
            client_id="sensitive-client",
            subject="personal-operator-id",
            resource="private-resource",
            hooks=_safety_hooks(denied="rate_budget", audit_events=events),
        )
    assert len(events) == 1
    event = events[0]
    assert event.outcome == "denied"
    assert event.reason_code == "rate_budget_denied"
    serialized = repr(event)
    assert "must-never-enter-audit" not in serialized
    assert "personal-operator-id" not in serialized
    assert len(event.principal_hash) == 64


@pytest.mark.parametrize(
    "payload",
    [
        {"platform": "telegram", "action": "delete", "idempotency_key": "delete-1234"},
        {"platform": "vk", "action": "forward", "idempotency_key": "forward-1234", "item_ref": ITEM_REF},
        {"platform": "telegram", "action": "story", "idempotency_key": "story-1234", "target_ref": TARGET_REF, "content": {"text": "no media"}},
        {"platform": "telegram", "action": "reaction", "idempotency_key": "reaction-1234", "item_ref": ITEM_REF, "reaction": " "},
    ],
)
def test_action_specific_shapes_fail_closed(payload: dict[str, Any]) -> None:
    with pytest.raises(SocialWorkspaceValidationError):
        validate_prepare_request(payload)


def test_status_request_reference_is_exclusive() -> None:
    assert validate_status_request({"operation_ref": OPERATION_REF}) == ("operation", OPERATION_REF)
    with pytest.raises(SocialWorkspaceValidationError):
        validate_status_request(
            {"operation_ref": OPERATION_REF, "preparation_ref": PREPARATION_REF}
        )

from __future__ import annotations

import json
from typing import Any, Mapping

import pytest
from jsonschema import Draft202012Validator, validate

from private_events_mcp.access_policy import CODEX_MAX_SCOPES
from private_events_mcp.social_workspace import (
    ContentFeature,
    GateDecision,
    SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_SCHEMA,
    SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_SCHEMA,
    SOCIAL_WORKSPACE_READ_SCHEMA,
    SOCIAL_WORKSPACE_SCOPES,
    SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA,
    SocialAction,
    SocialCapabilities,
    SocialPlatform,
    SocialReadOperation,
    SocialTargetKind,
    SocialWorkspaceValidationError,
    enforce_action_gates,
    enforce_editorial_sample_gates,
    required_scope_for_action,
    required_scope_for_read,
    validate_capabilities,
    validate_commit_request,
    validate_opaque_ref,
    validate_prepare_request,
    validate_read_request,
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


ALL_SCHEMAS = (
    SOCIAL_WORKSPACE_READ_SCHEMA,
    SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA,
    SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_SCHEMA,
    SOCIAL_WORKSPACE_PREPARE_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_SCHEMA,
    SOCIAL_WORKSPACE_COMMIT_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_SCHEMA,
    SOCIAL_WORKSPACE_STATUS_OUTPUT_SCHEMA,
    SOCIAL_WORKSPACE_SEND_MESSAGE_RECEIPT_SCHEMA,
    SOCIAL_WORKSPACE_CAPABILITIES_SCHEMA,
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


def test_action_enum_is_closed_and_scope_derivation_is_exact() -> None:
    assert {action.value for action in SocialAction} == {
        "send_message",
        "publish",
        "edit",
        "delete",
        "forward",
        "reaction",
        "comment",
        "schedule",
        "story",
    }
    assert required_scope_for_action("telegram", "send_message") == {"telegram:publish"}
    assert required_scope_for_action("vk", "delete") == {"vk:manage"}
    assert required_scope_for_action("telegram", "story") == {"telegram:stories"}
    assert required_scope_for_read("vk", "search_items") == {"vk:read"}
    assert required_scope_for_read("vk", "get_statistics") == {"vk:analytics"}
    assert len(SOCIAL_WORKSPACE_SCOPES) == 10
    assert SOCIAL_WORKSPACE_SCOPES.isdisjoint(CODEX_MAX_SCOPES)
    with pytest.raises(SocialWorkspaceValidationError):
        required_scope_for_action("telegram", "messages.send")


def test_only_typed_opaque_references_cross_the_action_boundary() -> None:
    assert validate_opaque_ref(TARGET_REF, "target") == TARGET_REF
    for value in ("@person", "123456", "https://t.me/person", "tgt_short"):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_opaque_ref(value, "target")
    with pytest.raises(SocialWorkspaceValidationError):
        validate_opaque_ref(TARGET_REF, "item")
    with pytest.raises(SocialWorkspaceValidationError, match="unsupported request field"):
        validate_prepare_request({**_send_message(), "method": "messages.send"})


def test_exact_person_resolution_produces_a_canonical_preview_contract() -> None:
    request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "resolve_target",
            "target_locator": {"kind": "username", "value": "@exact_person"},
        }
    )
    assert request.target_locator is not None
    assert request.target_locator.value == "exact_person"
    assert request.required_scopes == {"telegram:read"}
    validate(
        {
            "platform": "telegram",
            "target_ref": TARGET_REF,
            "kind": "user",
            "display_name": "Точный пользователь",
            "canonical_handle": "exact_person",
            "profile_link": "https://t.me/exact_person",
            "is_exact_match": True,
            "trust": "untrusted_external_data",
        },
        SOCIAL_WORKSPACE_TARGET_PREVIEW_SCHEMA,
    )
    for locator in (
        {"kind": "profile_link", "value": "https://evil.example/person"},
        {"kind": "profile_link", "value": "https://user:pass@t.me/person"},
    ):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_read_request(
                {"platform": "telegram", "operation": "resolve_target", "target_locator": locator}
            )


def test_editorial_sample_is_single_target_bounded_paginated_and_ephemeral() -> None:
    request = validate_read_request(
        {
            "platform": "vk",
            "operation": "editorial_sample",
            "target_ref": TARGET_REF,
            "purpose": "editorial_analysis",
            "date_from": "2026-07-01",
            "date_to": "2026-08-08",
            "page_size": 25,
            "total_limit": 100,
            "sample_ref": SAMPLE_REF,
            "cursor": "next_page_2",
        }
    )
    assert request.page_size == 25
    assert request.total_limit == 100
    assert request.required_scopes == {"vk:read"}
    for mutation in (
        {"page_size": 26},
        {"total_limit": 101},
        {"query": "global expansion"},
        {"target_locator": {"kind": "username", "value": "another"}},
    ):
        with pytest.raises(SocialWorkspaceValidationError):
            validate_read_request(
                {
                    "platform": "vk",
                    "operation": "editorial_sample",
                    "target_ref": TARGET_REF,
                    "purpose": "editorial_analysis",
                    **mutation,
                }
            )

    page = {
        "sample_ref": SAMPLE_REF,
        "target": {
            "target_ref": TARGET_REF,
            "kind": "community",
            "title": "Канал",
            "about": "О канале",
            "description": "Редакционная политика",
            "basic_metrics": {"members": 1000},
        },
        "items": [
            {
                "item_ref": f"itm_{index:016d}",
                "published_at": "2026-08-08T12:00:00Z",
                "text": "т" * 768,
                "caption": "п" * 256,
                "basic_metrics": {"views": 100, "reactions": 4},
            }
            for index in range(25)
        ],
        "sampled_count": 25,
        "total_limit": 100,
        "next_cursor": "next_page_2",
        "storage_disposition": "ephemeral_no_index",
        "trust": "untrusted_external_data",
    }
    validate(page, SOCIAL_WORKSPACE_EDITORIAL_SAMPLE_SCHEMA)
    assert len(json.dumps(page, ensure_ascii=False).encode("utf-8")) < 128 * 1024


def test_editorial_sampling_requires_consent_and_purpose_hooks() -> None:
    request = validate_read_request(
        {
            "platform": "telegram",
            "operation": "editorial_sample",
            "target_ref": TARGET_REF,
            "purpose": "editorial_analysis",
        }
    )
    calls: list[str] = []

    def allowed(name: str):
        def hook(_request):
            calls.append(name)
            return GateDecision(True, "approved")

        return hook

    enforce_editorial_sample_gates(
        request, consent_hook=allowed("consent"), purpose_hook=allowed("purpose")
    )
    assert calls == ["consent", "purpose"]
    with pytest.raises(SocialWorkspaceValidationError, match="purpose denied"):
        enforce_editorial_sample_gates(
            request,
            consent_hook=allowed("consent"),
            purpose_hook=lambda _request: GateDecision(False, "wrong_purpose"),
        )


def test_rich_send_message_prepare_is_semantically_validated_and_capability_gated() -> None:
    validate(_send_message(), SOCIAL_WORKSPACE_PREPARE_SCHEMA)
    intent = validate_prepare_request(_send_message())
    assert intent.action is SocialAction.SEND_MESSAGE
    assert intent.required_scopes == {"telegram:publish"}
    assert intent.content is not None
    assert intent.content.features == {
        ContentFeature.RICH_TEXT,
        ContentFeature.CUSTOM_EMOJI,
        ContentFeature.IMAGE,
    }
    capabilities = validate_capabilities(_capabilities())
    decisions: list[str] = []

    def gate(name: str):
        def check(_intent):
            decisions.append(name)
            return GateDecision(True, "approved")

        return check

    enforce_action_gates(
        intent,
        consent_hook=gate("consent"),
        policy_hook=gate("policy"),
        capability_hook=lambda _intent: capabilities,
    )
    assert decisions == ["consent", "policy"]
    denied = SocialCapabilities(
        platform=SocialPlatform.TELEGRAM,
        target_ref=OTHER_TARGET_REF,
        target_kinds=frozenset({SocialTargetKind.USER}),
        read_operations=frozenset(),
        actions=frozenset({SocialAction.SEND_MESSAGE}),
        content_features=capabilities.content_features,
        max_text_length=4096,
        max_media_items=10,
    )
    with pytest.raises(SocialWorkspaceValidationError, match="target_mismatch"):
        enforce_action_gates(
            intent,
            consent_hook=gate("consent"),
            policy_hook=gate("policy"),
            capability_hook=lambda _intent: denied,
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"platform": "telegram", "action": "delete", "idempotency_key": "delete-1234"},
        {
            "platform": "vk",
            "action": "forward",
            "idempotency_key": "forward-1234",
            "item_ref": ITEM_REF,
        },
        {
            "platform": "telegram",
            "action": "story",
            "idempotency_key": "story-1234",
            "target_ref": TARGET_REF,
            "content": {"text": "story without media"},
        },
        {
            "platform": "telegram",
            "action": "reaction",
            "idempotency_key": "reaction-1234",
            "item_ref": ITEM_REF,
            "reaction": " ",
        },
    ],
)
def test_action_specific_shapes_fail_closed(payload: dict[str, Any]) -> None:
    with pytest.raises(SocialWorkspaceValidationError):
        validate_prepare_request(payload)


def test_prepare_commit_status_and_read_after_write_receipt_are_typed() -> None:
    assert validate_commit_request(
        {"preparation_ref": PREPARATION_REF, "confirm": True}
    ) == (PREPARATION_REF, True)
    assert validate_status_request({"operation_ref": OPERATION_REF}) == (
        "operation",
        OPERATION_REF,
    )
    receipt = {
        "platform": "telegram",
        "action": "send_message",
        "status": "succeeded",
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
    with pytest.raises(SocialWorkspaceValidationError, match="item mismatch"):
        validate_send_message_receipt(
            {
                **receipt,
                "read_after_write": {
                    **receipt["read_after_write"],
                    "observed_item_ref": "itm_ponmlkjihgfedcba",
                },
            }
        )
    with pytest.raises(SocialWorkspaceValidationError):
        validate_commit_request({"preparation_ref": PREPARATION_REF, "confirm": False})

"""Install the fixed VK API 5.199 calls used by native poll support.

The Social Workspace VK adapter deliberately exposes a closed operation map.
Poll support extends that map only when the provider adapter is loaded. No
public input controls provider method names, actors, or parameter vocabularies.
"""

from __future__ import annotations

import sys
from typing import Any


def _poll_calls(vk: Any) -> dict[str, Any]:
    return {
        "poll_create": vk._policy(  # noqa: SLF001 - internal extension boundary
            vk.VKActor.COMMUNITY_EDITOR,
            "post_publish",
            "polls.create",
            ["owner_id", "question", "add_answers"],
            [
                "is_anonymous",
                "is_multiple",
                "end_date",
                "photo_id",
                "background_id",
                "disable_unvote",
            ],
        ),
        "poll_get": vk._policy(  # noqa: SLF001
            vk.VKActor.COMMUNITY_EDITOR,
            "post_publish",
            "polls.getById",
            ["owner_id", "poll_id"],
            ["is_board", "extended", "friends_count", "fields", "name_case"],
        ),
        "poll_edit": vk._policy(  # noqa: SLF001
            vk.VKActor.COMMUNITY_EDITOR,
            "edit",
            "polls.edit",
            ["owner_id", "poll_id"],
            [
                "question",
                "add_answers",
                "edit_answers",
                "delete_answers",
                "end_date",
                "photo_id",
                "background_id",
            ],
        ),
        "poll_voters": vk._policy(  # noqa: SLF001
            vk.VKActor.COMMUNITY_EDITOR,
            "post_publish",
            "polls.getVoters",
            ["owner_id", "poll_id", "answer_ids"],
            [
                "is_board",
                "friends_only",
                "offset",
                "count",
                "fields",
                "name_case",
            ],
        ),
        # Scheduled and not-yet-public posts must be read through the same
        # community editor actor, not through the public/service-token reader.
        "poll_wall_item": vk._policy(  # noqa: SLF001
            vk.VKActor.COMMUNITY_EDITOR,
            "post_publish",
            "wall.getById",
            ["posts"],
            ["extended"],
        ),
        "poll_wall_feed": vk._policy(  # noqa: SLF001
            vk.VKActor.COMMUNITY_EDITOR,
            "post_publish",
            "wall.get",
            ["owner_id", "count", "filter"],
            ["offset"],
        ),
        # The general wall.edit contract intentionally omits publish_date.
        # Poll rescheduling gets a separate closed operation instead.
        "poll_wall_edit": vk._policy(  # noqa: SLF001
            vk.VKActor.COMMUNITY_EDITOR,
            "edit",
            "wall.edit",
            ["owner_id", "post_id", "message"],
            ["attachments", "publish_date", "signed"],
        ),
    }


def install_vk_poll_calls() -> None:
    """Idempotently extend the VK adapter and live transport allowlist."""

    import private_events_mcp_vk_adapter as vk

    calls = getattr(vk, "_CALLS", None)
    if not isinstance(calls, dict):
        raise RuntimeError("VK fixed-call registry is not extensible")
    for operation, policy in _poll_calls(vk).items():
        current = calls.get(operation)
        if current is not None and current != policy:
            raise RuntimeError(f"VK operation collision: {operation}")
        calls[operation] = policy

    allowlist = frozenset(policy.method for policy in calls.values())
    operation_actors = {
        name: (policy.actor.value, policy.capability)
        for name, policy in calls.items()
    }
    vk.VK_FIXED_METHOD_ALLOWLIST = allowlist
    vk.VK_OPERATION_ACTORS = operation_actors

    # Production bindings import the allowlist by value. If they are already
    # loaded, update that module-global value before the next transport call.
    bindings = sys.modules.get("private_events_mcp_workspace_providers")
    if bindings is not None:
        setattr(bindings, "VK_FIXED_METHOD_ALLOWLIST", allowlist)


__all__ = ["install_vk_poll_calls"]

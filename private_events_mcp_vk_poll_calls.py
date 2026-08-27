"""Install the fixed VK API 5.199 calls used by native poll support.

The Social Workspace VK adapter deliberately exposes a closed operation map.
Poll support extends that map before production provider bindings import the
method allowlist.  No public input controls provider method names, actors, or
parameter vocabularies.
"""

from __future__ import annotations

import private_events_mcp_vk_adapter as _vk


_POLL_CALLS = {
    "poll_create": _vk._policy(  # noqa: SLF001 - internal extension boundary
        _vk.VKActor.COMMUNITY_EDITOR,
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
    "poll_get": _vk._policy(  # noqa: SLF001
        _vk.VKActor.COMMUNITY_EDITOR,
        "post_publish",
        "polls.getById",
        ["owner_id", "poll_id"],
        ["is_board", "extended", "friends_count", "fields", "name_case"],
    ),
    "poll_edit": _vk._policy(  # noqa: SLF001
        _vk.VKActor.COMMUNITY_EDITOR,
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
    "poll_voters": _vk._policy(  # noqa: SLF001
        _vk.VKActor.COMMUNITY_EDITOR,
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
    # Scheduled and not-yet-public posts must be read through the same dedicated
    # community editor actor, not through the public/service-token reader.
    "poll_wall_item": _vk._policy(  # noqa: SLF001
        _vk.VKActor.COMMUNITY_EDITOR,
        "post_publish",
        "wall.getById",
        ["posts"],
        ["extended"],
    ),
    "poll_wall_feed": _vk._policy(  # noqa: SLF001
        _vk.VKActor.COMMUNITY_EDITOR,
        "post_publish",
        "wall.get",
        ["owner_id", "count", "filter"],
        ["offset"],
    ),
    # The general wall.edit contract intentionally omits publish_date. Poll
    # rescheduling gets a separate closed operation rather than broadening it.
    "poll_wall_edit": _vk._policy(  # noqa: SLF001
        _vk.VKActor.COMMUNITY_EDITOR,
        "edit",
        "wall.edit",
        ["owner_id", "post_id", "message"],
        ["attachments", "publish_date", "signed"],
    ),
}


def install_vk_poll_calls() -> None:
    """Idempotently extend the VK adapter's closed operation vocabulary."""

    calls = getattr(_vk, "_CALLS", None)
    if not isinstance(calls, dict):
        raise RuntimeError("VK fixed-call registry is not extensible")
    for operation, policy in _POLL_CALLS.items():
        current = calls.get(operation)
        if current is not None and current != policy:
            raise RuntimeError(f"VK operation collision: {operation}")
        calls[operation] = policy

    # These values are imported by the production transport after package
    # initialization, so recompute them from the now-complete fixed registry.
    _vk.VK_FIXED_METHOD_ALLOWLIST = frozenset(  # type: ignore[attr-defined]
        policy.method for policy in calls.values()
    )
    _vk.VK_OPERATION_ACTORS = {  # type: ignore[attr-defined]
        name: (policy.actor.value, policy.capability)
        for name, policy in calls.items()
    }


__all__ = ["install_vk_poll_calls"]

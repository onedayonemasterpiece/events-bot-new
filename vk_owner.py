"""Helpers for VK owner-id semantics.

VK distinguishes between communities (groups/pages/events) and personal
profiles (users). In the VK API, ``owner_id`` is **negative** for
communities and **positive** for users. Historically this codebase only
monitored communities, so ``vk_source.group_id``/``vk_inbox.group_id``
stored a positive community id and downstream code always prefixed
``-`` when calling ``wall.get`` or building post URLs.

To monitor personal pages we now also accept ``owner_type='user'``
rows where ``group_id`` actually stores the positive user_id and
``owner_id`` is positive (no leading minus in the post URL).

These small pure functions keep the convention in one place so the
sprawling URL builders and API call sites can opt-in incrementally
without losing backward compatibility for existing community rows.
"""

from __future__ import annotations

GROUP_OWNER_TYPES: frozenset[str] = frozenset({"group", "page", "public", "event"})
USER_OWNER_TYPES: frozenset[str] = frozenset({"user", "profile"})


def normalize_owner_type(value: str | None) -> str:
    """Normalize an owner_type string to ``"user"`` or ``"group"``.

    Unknown / empty values fall back to ``"group"`` (the legacy default)
    so the helper stays safe for pre-migration rows.
    """
    raw = str(value or "").strip().lower()
    if raw in USER_OWNER_TYPES:
        return "user"
    return "group"


def signed_owner_id(group_id: int | str, owner_type: str | None = "group") -> int:
    """Return the signed VK ``owner_id`` for ``group_id`` of ``owner_type``.

    For communities (``group``/``page``/``public``/``event``) the value is
    negated to ``-abs(id)``; for users it stays positive.
    """
    try:
        numeric = int(str(group_id).lstrip("-"))
    except (TypeError, ValueError):
        return 0
    if numeric <= 0:
        return 0
    if normalize_owner_type(owner_type) == "user":
        return numeric
    return -numeric


def vk_wall_url(group_id: int | str, post_id: int | str, owner_type: str | None = "group") -> str:
    """Build the canonical ``https://vk.com/wall<owner_id>_<post_id>`` URL.

    For communities the owner part is prefixed with ``-``; for users it
    is the plain positive id, matching what VK itself renders.
    """
    try:
        numeric = int(str(group_id).lstrip("-"))
    except (TypeError, ValueError):
        numeric = 0
    try:
        pid = int(str(post_id))
    except (TypeError, ValueError):
        pid = 0
    if normalize_owner_type(owner_type) == "user":
        owner_part = str(numeric)
    else:
        owner_part = f"-{numeric}"
    return f"https://vk.com/wall{owner_part}_{pid}"

from __future__ import annotations

from .social_workspace import SOCIAL_WORKSPACE_SCOPES

READ_SCOPES = frozenset({"events:read", "incidents:read", "operations:read"})
LEGACY_SOCIAL_SCOPES = frozenset(
    {"telegram:read", "telegram:publish", "vk:read", "vk:publish"}
)
GRANULAR_SOCIAL_SCOPES = SOCIAL_WORKSPACE_SCOPES
# Legacy scopes remain valid only for the pre-existing four legacy social tools.
# They are deliberately not aliases for any granular workspace capability: the
# workspace requires its exact ``platform:suffix`` scope for every operation.
SOCIAL_SCOPES = LEGACY_SOCIAL_SCOPES | GRANULAR_SOCIAL_SCOPES

SOCIAL_MUTATION_SCOPES = frozenset(
    scope
    for scope in GRANULAR_SOCIAL_SCOPES
    if scope.split(":", 1)[1]
    in {
        "dm:send",
        "post:publish",
        "edit",
        "delete",
        "forward",
        "reaction",
        "comment",
        "schedule",
        "story:write",
    }
)
# Only the new granular workspace mutations are protected by the external
# operator-approval flow.  The two coarse legacy publish scopes intentionally
# retain their older one-use prepare/commit semantics until the legacy tools are
# retired; they must never be described as externally approved.
APPROVAL_REQUIRED_SOCIAL_SCOPES = SOCIAL_MUTATION_SCOPES
LEGACY_PUBLISH_SCOPES = frozenset({"telegram:publish", "vk:publish"})

_LEGACY_READ_SUFFIXES = frozenset(
    {
        "discover",
        "read:public",
        "read:private",
        "read:dialogs",
        "story:read",
        "analytics",
        "audience",
        "notifications:read",
    }
)


def legacy_social_scope_for(required_scope: str) -> str | None:
    """Return the stable coarse ChatGPT scope for one granular social scope.

    The four original scopes are durable provider capability families.  This
    compatibility mapping never crosses provider or read/write boundaries and
    therefore lets an existing ChatGPT connector discover later typed tools
    without changing its URL/client identity.  Codex cannot use the mapping
    because its client policy never permits any legacy social scope.
    """

    if not isinstance(required_scope, str) or ":" not in required_scope:
        return None
    platform, suffix = required_scope.split(":", 1)
    if platform not in {"telegram", "vk"}:
        return None
    if suffix in _LEGACY_READ_SUFFIXES:
        return f"{platform}:read"
    if required_scope in SOCIAL_MUTATION_SCOPES:
        return f"{platform}:publish"
    return None

CHATGPT_DEFAULT_SCOPES = READ_SCOPES
CODEX_DEFAULT_SCOPES = READ_SCOPES
CHATGPT_MAX_SCOPES = READ_SCOPES | SOCIAL_SCOPES | {"offline_access"}
CODEX_MAX_SCOPES = READ_SCOPES | {"offline_access"}

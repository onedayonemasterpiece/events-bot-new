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
APPROVAL_REQUIRED_SOCIAL_SCOPES = (
    SOCIAL_MUTATION_SCOPES | {"telegram:publish", "vk:publish"}
)

CHATGPT_DEFAULT_SCOPES = READ_SCOPES
CODEX_DEFAULT_SCOPES = READ_SCOPES
CHATGPT_MAX_SCOPES = READ_SCOPES | SOCIAL_SCOPES | {"offline_access"}
CODEX_MAX_SCOPES = READ_SCOPES | {"offline_access"}

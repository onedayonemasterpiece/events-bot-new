from __future__ import annotations

from .social_workspace import SOCIAL_WORKSPACE_SCOPES

READ_SCOPES = frozenset({"events:read", "incidents:read", "operations:read"})
EVENT_WRITE_SCOPES = frozenset({"events:write"})
AUDIO_TRANSCRIPTION_SCOPES = frozenset({"audio:transcribe"})
LEGACY_SOCIAL_SCOPES = frozenset(
    {"telegram:read", "telegram:publish", "vk:read", "vk:publish"}
)
GRANULAR_SOCIAL_SCOPES = SOCIAL_WORKSPACE_SCOPES
# Legacy scopes remain durable same-provider read/write capability families for
# existing ChatGPT connectors. They never cross provider or read/write
# boundaries; unknown future granular capabilities still fail closed.
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
# Granular scopes describe mutation authority.  Outbound actions explicitly
# requested through the typed ChatGPT tool use one-use prepare/commit without a
# second prompt; edit/delete retain external operator approval.  Coarse legacy
# scopes stay inside the same provider/read-write family.
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

    # Audio transcription sends short-lived voice notes through Telegram and
    # deletes them after recognition.  Treat it as a later typed capability of
    # the stable Telegram publish family so existing ChatGPT connectors do not
    # need a new OAuth consent solely because the tool was added later.
    if required_scope == "audio:transcribe":
        return "telegram:publish"
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


def social_scopes_authorized(
    required_scopes: frozenset[str], granted_scopes: frozenset[str]
) -> bool:
    """Check exact scopes, then one fail-closed stable social family fallback."""

    if required_scopes.issubset(granted_scopes):
        return True
    if len(required_scopes) != 1:
        return False
    legacy_scope = legacy_social_scope_for(next(iter(required_scopes)))
    return legacy_scope is not None and legacy_scope in granted_scopes


CHATGPT_DEFAULT_SCOPES = READ_SCOPES
CODEX_DEFAULT_SCOPES = READ_SCOPES
OPENCODE_DEFAULT_SCOPES = READ_SCOPES
CHATGPT_MAX_SCOPES = (
    READ_SCOPES
    | EVENT_WRITE_SCOPES
    | {"partners:manage"}
    | SOCIAL_SCOPES
    | AUDIO_TRANSCRIPTION_SCOPES
    | {"offline_access"}
)
CODEX_MAX_SCOPES = READ_SCOPES | {"offline_access"}
OPENCODE_MAX_SCOPES = CHATGPT_MAX_SCOPES

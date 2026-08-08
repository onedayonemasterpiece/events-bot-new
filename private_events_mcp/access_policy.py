from __future__ import annotations


READ_SCOPES = frozenset({"events:read", "incidents:read", "operations:read"})
SOCIAL_SCOPES = frozenset(
    {"telegram:read", "telegram:publish", "vk:read", "vk:publish"}
)
CHATGPT_DEFAULT_SCOPES = READ_SCOPES
CODEX_DEFAULT_SCOPES = READ_SCOPES
CHATGPT_MAX_SCOPES = READ_SCOPES | SOCIAL_SCOPES | {"offline_access"}
CODEX_MAX_SCOPES = READ_SCOPES | {"offline_access"}

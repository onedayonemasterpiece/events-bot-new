"""Private evidence MCP with optional provider-neutral social adapters."""

from .config import PrivateEventsMCPConfig
from .social_poll_tools import install_social_poll_extension
from private_events_mcp_vk_poll_calls import install_vk_poll_calls

# Install before ``integration`` imports the social tool builder and before the
# production VK provider binding imports its fixed method allowlist. Both
# extensions are idempotent and leave Social Workspace disabled by default.
install_vk_poll_calls()
install_social_poll_extension()

from .integration import attach_private_events_mcp
from .social import (
    ResolvedTarget,
    SocialAdapter,
    SocialPost,
    SocialPublishReceipt,
    SocialReadResult,
)

__all__ = [
    "PrivateEventsMCPConfig",
    "ResolvedTarget",
    "SocialAdapter",
    "SocialPost",
    "SocialPublishReceipt",
    "SocialReadResult",
    "attach_private_events_mcp",
]

"""Private evidence MCP with optional provider-neutral social adapters."""

from .config import PrivateEventsMCPConfig
from .social_poll_tools import install_social_poll_extension

# Install before ``integration`` imports the social tool builder. The patch is
# idempotent and leaves the catalog unchanged when Social Workspace is disabled.
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

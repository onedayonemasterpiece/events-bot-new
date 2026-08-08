"""Private evidence MCP with optional provider-neutral social adapters."""

from .config import PrivateEventsMCPConfig
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

"""Private, read-only MCP surface for events-bot production evidence."""

from .config import PrivateEventsMCPConfig
from .integration import attach_private_events_mcp

__all__ = ["PrivateEventsMCPConfig", "attach_private_events_mcp"]

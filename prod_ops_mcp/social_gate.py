from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

_TARGET_ALIAS = re.compile(r"^(telegram|vk|max):[a-z0-9_.-]{2,80}$")


class SocialAction(str, Enum):
    READ_CACHED = "read_cached"
    READ_LIVE = "read_live"
    PLAN_PUBLISH = "plan_publish"
    EXECUTE_PUBLISH = "execute_publish"


@dataclass(frozen=True, slots=True)
class SocialDecision:
    allowed: bool
    reason: str


class SocialCapabilityGate:
    """Policy boundary for future Telegram/VK/MAX adapters.

    The phase-one MCP server does not register any live network or write tool.
    This gate makes the future boundary explicit and testable before adapters
    are connected to provider credentials.
    """

    def __init__(self, policy: Mapping[str, Any] | None = None, *, write_enabled: bool = False) -> None:
        self._policy = dict(policy or {})
        self._write_enabled = bool(write_enabled)

    @classmethod
    def from_env(cls) -> "SocialCapabilityGate":
        raw = os.getenv("PROD_OPS_MCP_SOCIAL_POLICY_JSON", "").strip()
        if not raw:
            policy: Mapping[str, Any] = {}
        else:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise ValueError("PROD_OPS_MCP_SOCIAL_POLICY_JSON must be an object")
            policy = parsed
        return cls(policy, write_enabled=False)

    def decide(self, platform: str, action: SocialAction, *, target: str | None = None) -> SocialDecision:
        platform = platform.strip().lower()
        if platform not in {"telegram", "vk", "max"}:
            return SocialDecision(False, "unsupported_platform")
        platform_policy = self._policy.get(platform)
        if not isinstance(platform_policy, Mapping):
            return SocialDecision(False, "adapter_not_enabled")
        if not bool(platform_policy.get(action.value)):
            return SocialDecision(False, "capability_not_allowlisted")
        if action in {SocialAction.PLAN_PUBLISH, SocialAction.EXECUTE_PUBLISH}:
            if not self._write_enabled:
                return SocialDecision(False, "global_write_gate_disabled")
            if not target or not _TARGET_ALIAS.fullmatch(target):
                return SocialDecision(False, "target_alias_required")
            allowed_targets = platform_policy.get("targets") or []
            if target not in allowed_targets:
                return SocialDecision(False, "target_not_allowlisted")
        if action is SocialAction.EXECUTE_PUBLISH:
            return SocialDecision(False, "two_phase_execution_not_implemented")
        return SocialDecision(True, "allowed")

    def describe(self) -> dict[str, Any]:
        platforms: dict[str, Any] = {}
        for platform in ("telegram", "vk", "max"):
            platforms[platform] = {
                "adapter_status": "not_registered",
                "read_cached": self.decide(platform, SocialAction.READ_CACHED).allowed,
                "read_live": False,
                "plan_publish": False,
                "execute_publish": False,
                "provider_network_calls_from_mcp": False,
            }
        return {
            "phase": "read_only_mvp",
            "write_master_gate": False,
            "provider_network_calls_from_mcp": False,
            "platforms": platforms,
            "future_contract": {
                "read_live": "separate capability + provider egress budget",
                "publish": "allowlisted target alias -> plan -> explicit confirmation -> execute -> verify receipt",
                "raw_target_ids": "forbidden",
                "provider_tokens_in_mcp_responses": "forbidden",
            },
        }

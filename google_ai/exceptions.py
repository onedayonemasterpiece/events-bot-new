"""Custom exceptions for Google AI SDK."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class RateLimitError(Exception):
    """Raised when rate limits are exceeded.
    
    NO_WAIT policy: this error is raised immediately without waiting.
    """
    blocked_reason: str  # 'rpm' | 'tpm' | 'rpd'
    retry_after_ms: Optional[int] = None
    model: Optional[str] = None
    api_key_id: Optional[str] = None
    minute_bucket: Optional[str] = None
    day_bucket: Optional[str] = None
    # Provider quotas are shared by Google Cloud project + model.  The key id is
    # useful diagnostics, but is deliberately not the quota identity.
    quota_scope: Optional[str] = None
    quota_reason: Optional[str] = None

    @property
    def quota_bucket(self) -> Optional[str]:
        """Project/model quota identity; never derived from an API key."""

        if not self.quota_scope or not self.model:
            return None
        return f"{self.quota_scope}:{self.model}"
    
    def __str__(self) -> str:
        msg = f"Rate limit exceeded: {self.blocked_reason}"
        if self.retry_after_ms:
            msg += f" (retry after {self.retry_after_ms}ms)"
        return msg


@dataclass
class ProviderError(Exception):
    """Raised when Google AI provider returns an error.
    
    Retryable errors will be retried up to 3 times.
    """
    error_type: str
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    retryable: bool = False
    status_code: Optional[int] = None
    retry_after_ms: Optional[int] = None
    finish_reason: Optional[str] = None
    provider_response_id: Optional[str] = None
    provider_request_id: Optional[str] = None
    provider_model_version: Optional[str] = None
    quota_scope: Optional[str] = None
    quota_reason: Optional[str] = None
    model: Optional[str] = None
    # Kept as Any to avoid an exceptions -> client import cycle.  The gateway
    # stores UsageInfo here when the provider returned accounting metadata but
    # the response itself was non-success (for example MAX_TOKENS).
    usage: Optional[Any] = None

    @property
    def quota_bucket(self) -> Optional[str]:
        if not self.quota_scope or not self.model:
            return None
        return f"{self.quota_scope}:{self.model}"
    
    def __str__(self) -> str:
        msg = f"Provider error: {self.error_type}"
        if self.error_code:
            msg += f" ({self.error_code})"
        if self.error_message:
            msg += f": {self.error_message}"
        if self.retry_after_ms:
            msg += f" (retry after {self.retry_after_ms}ms)"
        return msg


class SecretsError(Exception):
    """Raised when secrets cannot be retrieved or decrypted."""
    pass


class ReservationError(Exception):
    """Raised when rate limit reservation fails unexpectedly."""
    pass

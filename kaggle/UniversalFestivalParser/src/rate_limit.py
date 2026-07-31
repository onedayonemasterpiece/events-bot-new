"""Rate limiter for Gemma 3-27B API calls.

Implements token bucket algorithm for:
- 30 RPM (requests per minute)
- 15K TPM (tokens per minute)
- 14.4K RPD (requests per day) - tracked externally
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class FestivalParserRateLimitError(RuntimeError):
    """Raised when the parser would exceed its configured LLM request budget."""


class _FestivalSecretsProvider:
    """Keep the legacy explicit key parameter behind ``GoogleAIClient``.

    The shared reservation selects an env-var lane.  Only the parser's historic
    ``GOOGLE_API_KEY`` lane may use the explicitly supplied value; a reservation
    for any other lane must be resolved by the standard secrets provider.
    """

    def __init__(self, api_key: str | None, fallback: Any):
        self._api_key = (api_key or "").strip()
        self._fallback = fallback

    def get_secret(self, name: str) -> Optional[str]:
        if name == "GOOGLE_API_KEY" and self._api_key:
            return self._api_key
        return self._fallback.get_secret(name)


def build_festival_google_ai_client(
    *,
    api_key: str | None,
    consumer: str,
) -> Any:
    """Build a fail-closed shared Google AI gateway for parser LLM calls.

    The dedicated Supabase helper deliberately requires its own limiter
    credentials.  It must not fall back to a product-data client or to a raw
    provider key.  ``max_retries=1`` and an empty model-fallback chain preserve
    the parser's existing physical provider-attempt cap; feature-level retry
    loops remain the sole owners of any additional attempts.
    """

    from google_ai import GoogleAIClient, SecretsProvider
    from google_ai.limiter_supabase import (
        build_google_ai_limiter_supabase_client,
    )

    supabase = build_google_ai_limiter_supabase_client(require_configured=True)
    secrets = _FestivalSecretsProvider(api_key, SecretsProvider())
    client = GoogleAIClient(
        supabase_client=supabase,
        secrets_provider=secrets,
        consumer=consumer,
        default_env_var_name="GOOGLE_API_KEY",
    )

    # These consumers must never inherit a process-local/direct-key escape
    # hatch from ambient runtime settings.
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    client.max_retries = 1
    client.fallback_models = []
    return client


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting.
    
    Uses conservative safety margins to prevent hitting limits
    when parallel processes also use Gemma API:
    - 45% margin for RPM/TPM (minute limits)  
    - 85% margin for RPD (daily limit)
    """
    rpm: int = 30  # Requests per minute (actual limit)
    tpm: int = 15000  # Tokens per minute (actual limit)
    rpd: int = 14400  # Requests per day (actual limit)
    minute_margin: float = 0.45  # 45% margin → use 55% of limit
    daily_margin: float = 0.85  # 85% margin → use 15% of limit
    
    @property
    def effective_rpm(self) -> int:
        """RPM with 45% safety margin applied."""
        return int(self.rpm * (1 - self.minute_margin))  # 30 * 0.55 = 16
    
    @property
    def effective_tpm(self) -> int:
        """TPM with 45% safety margin applied."""
        return int(self.tpm * (1 - self.minute_margin))  # 15000 * 0.55 = 8250
    
    @property
    def effective_rpd(self) -> int:
        """RPD with 85% safety margin applied."""
        return int(self.rpd * (1 - self.daily_margin))  # 14400 * 0.15 = 2160


class TokenBucket:
    """Token bucket rate limiter."""
    
    def __init__(self, capacity: int, refill_rate: float):
        """
        Args:
            capacity: Maximum tokens in bucket
            refill_rate: Tokens added per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.monotonic()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now
    
    def consume(self, tokens: int = 1) -> bool:
        """Try to consume tokens. Returns True if successful."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def wait_time(self, tokens: int = 1) -> float:
        """Calculate wait time to have enough tokens."""
        self._refill()
        if self.tokens >= tokens:
            return 0.0
        needed = tokens - self.tokens
        return needed / self.refill_rate


class GemmaRateLimiter:
    """Rate limiter for Gemma 3-27B API.
    
    Usage:
        limiter = GemmaRateLimiter()
        
        async with limiter.acquire(estimated_tokens=1000):
            response = await call_gemma(prompt)
            limiter.record_usage(actual_prompt_tokens, actual_response_tokens)
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        
        # Token buckets for RPM and TPM (with safety margin applied)
        self._rpm_bucket = TokenBucket(
            capacity=self.config.effective_rpm,  # 25 (with 15% margin)
            refill_rate=self.config.effective_rpm / 60.0,
        )
        self._tpm_bucket = TokenBucket(
            capacity=self.config.effective_tpm,  # 12750 (with 15% margin)
            refill_rate=self.config.effective_tpm / 60.0,
        )
        
        # Tracking
        self._total_requests = 0
        self._total_tokens = 0
        self._daily_requests = 0
        self._last_reset_day: Optional[str] = None
    
    def acquire(self, estimated_tokens: int = 500) -> "RateLimitContext":
        """Acquire rate limit slot before making API call."""
        return RateLimitContext(self, estimated_tokens)
    
    async def wait_if_needed(self, estimated_tokens: int) -> float:
        """Wait if rate limited. Returns seconds waited."""
        total_waited = 0.0
        estimated = max(1, int(estimated_tokens or 1))
        
        # Check daily limit
        self._check_daily_reset()
        if self._daily_requests >= self.config.effective_rpd:
            raise FestivalParserRateLimitError(
                f"Daily request limit reached ({self._daily_requests}/{self.config.effective_rpd})"
            )
        if estimated > self.config.effective_tpm:
            raise FestivalParserRateLimitError(
                f"Estimated tokens exceed per-call budget ({estimated}>{self.config.effective_tpm})"
            )
        
        # Wait for RPM bucket
        while True:
            rpm_wait = self._rpm_bucket.wait_time(1)
            if rpm_wait <= 0:
                break
            logger.debug("Rate limited (RPM), waiting %.2fs", rpm_wait)
            await asyncio.sleep(min(rpm_wait, 5.0))
            total_waited += min(rpm_wait, 5.0)
        
        # Wait for TPM bucket
        while True:
            tpm_wait = self._tpm_bucket.wait_time(estimated)
            if tpm_wait <= 0:
                break
            logger.debug("Rate limited (TPM), waiting %.2fs", tpm_wait)
            await asyncio.sleep(min(tpm_wait, 5.0))
            total_waited += min(tpm_wait, 5.0)
        
        # Consume tokens
        self._rpm_bucket.consume(1)
        self._tpm_bucket.consume(estimated)
        
        return total_waited
    
    def record_usage(self, prompt_tokens: int, response_tokens: int) -> None:
        """Record actual token usage after API call."""
        total = prompt_tokens + response_tokens
        self._total_requests += 1
        self._total_tokens += total
        self._daily_requests += 1
        
        logger.debug(
            "Rate limiter: request=%d tokens=%d (total: %d requests, %d tokens)",
            self._total_requests,
            total,
            self._total_requests,
            self._total_tokens,
        )
    
    def _check_daily_reset(self) -> None:
        """Reset daily counter if new day."""
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._last_reset_day != today:
            self._daily_requests = 0
            self._last_reset_day = today
    
    def get_stats(self) -> dict:
        """Get current rate limiter statistics."""
        return {
            "total_requests": self._total_requests,
            "total_tokens": self._total_tokens,
            "daily_requests": self._daily_requests,
            "rpm_bucket_tokens": self._rpm_bucket.tokens,
            "tpm_bucket_tokens": self._tpm_bucket.tokens,
        }
    
    def save_usage(self, path: str | Path) -> None:
        """Save usage statistics to JSON (for cross-run tracking)."""
        path = Path(path)
        data = {
            "daily_requests": self._daily_requests,
            "last_reset_day": self._last_reset_day,
            "total_requests": self._total_requests,
            "total_tokens": self._total_tokens,
        }
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")


class RateLimitContext:
    """Context manager for rate-limited API calls."""
    
    def __init__(self, limiter: GemmaRateLimiter, estimated_tokens: int):
        self._limiter = limiter
        self._estimated_tokens = estimated_tokens
        self._wait_time = 0.0
    
    async def __aenter__(self) -> "RateLimitContext":
        self._wait_time = await self._limiter.wait_if_needed(self._estimated_tokens)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        pass
    
    @property
    def wait_time(self) -> float:
        """Seconds waited for rate limit."""
        return self._wait_time

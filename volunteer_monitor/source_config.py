from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(slots=True, frozen=True)
class DobroSourceConfig:
    """Bounded, permission-aware configuration for the Dobro.ru adapter.

    The adapter is intentionally single-source and fail-closed. Adding another
    host requires a separate reviewed configuration rather than broadening the
    allowlist at runtime.
    """

    search_url: str = "https://dobro.ru/search?d_c=1&d_s=1&t=e"
    # Dobro.ru's current city chooser renders the region-level option in this
    # abbreviated form. Downstream source validation still accepts and
    # canonicalizes the full `Калининградская область` value from detail pages.
    region_name: str = "Калининградская обл"
    max_more_clicks: int = 40
    max_items: int = 120
    playwright_timeout_ms: int = 30_000
    detail_timeout_seconds: float = 30.0
    max_response_bytes: int = 4 * 1024 * 1024
    headless: bool = True
    permission_reference: str = "pending-volunteer-centre-approval"
    evidence_dir: Path | None = None

    def validate(self) -> None:
        split = urlsplit(self.search_url)
        if split.scheme != "https" or (split.hostname or "").casefold() not in {
            "dobro.ru",
            "www.dobro.ru",
        }:
            raise ValueError("search_url must be an HTTPS Dobro.ru URL")
        if not self.region_name.strip():
            raise ValueError("region_name must not be empty")
        if not 0 <= self.max_more_clicks <= 100:
            raise ValueError("max_more_clicks must be in 0..100")
        if not 1 <= self.max_items <= 250:
            raise ValueError("max_items must be in 1..250")
        if not 5_000 <= self.playwright_timeout_ms <= 120_000:
            raise ValueError("playwright_timeout_ms must be in 5000..120000")
        if not 5.0 <= float(self.detail_timeout_seconds) <= 120.0:
            raise ValueError("detail_timeout_seconds must be in 5..120")
        if not 256 * 1024 <= self.max_response_bytes <= 16 * 1024 * 1024:
            raise ValueError("max_response_bytes must be in 256KiB..16MiB")
        if not self.permission_reference.strip():
            raise ValueError("permission_reference must not be empty")

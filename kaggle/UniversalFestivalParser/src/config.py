"""Configuration module for Universal Festival Parser.

Reads configuration from environment variables and Kaggle input.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ParserConfig:
    """Configuration for the festival parser."""
    
    # Input
    festival_url: str
    run_id: str
    
    # Parser settings
    parser_version: str = "1.0.0"
    debug: bool = False
    
    # Playwright settings
    headless: bool = True
    timeout_ms: int = 30000
    wait_until: str = "networkidle"
    
    # LLM settings
    llm_model: str = "gemma-3-27b"
    max_retries: int = 3
    max_llm_calls: int = 2
    max_estimated_tokens_per_call: int = 8000
    dry_run: bool = False
    no_llm: bool = False
    
    # Output paths
    output_dir: Path = Path("/kaggle/working")
    
    @classmethod
    def from_environment(cls) -> "ParserConfig":
        """Create config from environment variables."""
        festival_url = os.getenv("FESTIVAL_URL", "")
        run_id = os.getenv("RUN_ID", "")
        config_data: dict = {}
        
        if not festival_url:
            # Try to read from config file
            config_path = Path("/kaggle/input/run-config/config.json")
            if config_path.exists():
                try:
                    config_data = json.loads(config_path.read_text())
                    festival_url = config_data.get("festival_url", "")
                    run_id = config_data.get("run_id", "")
                except Exception as e:
                    logger.error("Failed to read config file: %s", e)
        
        if not festival_url:
            raise ValueError("FESTIVAL_URL environment variable is required")
        
        if not run_id:
            from datetime import datetime, timezone
            import hashlib
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            url_hash = hashlib.sha256(festival_url.encode()).hexdigest()[:8]
            run_id = f"{timestamp}_{url_hash}"

        def _value(name: str, default: object = None) -> object:
            env_value = os.getenv(name)
            if env_value is not None:
                return env_value
            snake = name.lower()
            if snake in config_data:
                return config_data.get(snake)
            return default

        def _bool(name: str, default: bool = False) -> bool:
            raw = _value(name, default)
            if isinstance(raw, bool):
                return raw
            return str(raw or "").strip().lower() in ("1", "true", "yes", "on")

        def _int(name: str, default: int, *, minimum: int, maximum: int) -> int:
            raw = _value(name, default)
            try:
                value = int(raw)
            except Exception:
                value = default
            return max(minimum, min(value, maximum))
        
        return cls(
            festival_url=festival_url,
            run_id=run_id,
            parser_version=str(_value("PARSER_VERSION", "1.0.0") or "1.0.0"),
            debug=_bool("DEBUG", False),
            headless=str(_value("HEADLESS", "true")).strip().lower() != "false",
            timeout_ms=_int("TIMEOUT_MS", 30000, minimum=5000, maximum=120000),
            llm_model=str(_value("LLM_MODEL", "gemma-3-27b") or "gemma-3-27b"),
            max_retries=_int("MAX_RETRIES", 3, minimum=0, maximum=5),
            max_llm_calls=_int("MAX_LLM_CALLS", 2, minimum=0, maximum=2),
            max_estimated_tokens_per_call=_int(
                "MAX_ESTIMATED_TOKENS_PER_CALL",
                8000,
                minimum=1000,
                maximum=8250,
            ),
            dry_run=_bool("DRY_RUN", False),
            no_llm=_bool("NO_LLM", False),
        )
    
    def get_output_path(self, filename: str) -> Path:
        """Get full output path for a file."""
        return self.output_dir / filename

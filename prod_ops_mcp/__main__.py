from __future__ import annotations

import hashlib
import logging

from aiohttp import web

from .config import OpsMCPConfig
from .server import create_app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    config = OpsMCPConfig.from_env(require_enabled=True)
    endpoint_hash = hashlib.sha256(config.path_secret.encode()).hexdigest()[:12]
    logging.info(
        "prod_ops_mcp starting host=%s port=%s endpoint_hash=%s path_only=%s",
        config.bind_host,
        config.port,
        endpoint_hash,
        int(config.allow_path_only_auth),
    )
    app = create_app(config)
    web.run_app(
        app,
        host=config.bind_host,
        port=config.port,
        access_log=None,
        print=lambda _: None,
    )


if __name__ == "__main__":
    main()

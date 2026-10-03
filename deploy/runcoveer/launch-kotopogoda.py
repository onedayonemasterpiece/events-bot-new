"""Preflight starts HTTP and read-only integration probes without jobs/cutover."""
import os
import sys
sys.path.insert(0, "/app")
import main
# Reuse the existing bounded stdlib handler, preserving Kotopogoda JSON/redaction.
from runtime_logging import install_runtime_file_logging

root_logger = main.logging.getLogger()
console_formatter = root_logger.handlers[0].formatter
file_handler = install_runtime_file_logging(root_logger)
if file_handler is not None:
    file_handler.setFormatter(console_formatter)
    for context_filter in root_logger.filters:
        file_handler.addFilter(context_filter)
app = main.create_app()
if os.getenv("DEPLOY_PREFLIGHT", "1") == "1":
    app.on_startup.clear()
    app.on_cleanup.clear()
    async def initialize(app):
        import aiohttp
        app["bot"].session = aiohttp.ClientSession()
        main.logging.info("deployment_preflight component=kotopogoda webhook=unchanged scheduler=disabled")
    async def cleanup(app):
        await app["bot"].session.close()
    app.on_startup.append(initialize)
    app.on_cleanup.append(cleanup)
main.web.run_app(app, host="0.0.0.0", port=8080)

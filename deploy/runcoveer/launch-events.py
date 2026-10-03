"""Preflight runs the real HTTP routes without webhook registration or jobs."""
import os
import sys
sys.path.insert(0, "/app")
import main
if os.getenv("DEPLOY_PREFLIGHT", "1") == "1":
    async def initialize(app, db, bot, webhook):
        await db.init()
        main.logging.info("deployment_preflight component=events webhook=unchanged scheduler=disabled")
    main.init_db_and_scheduler = initialize
main.web.run_app(main.create_app(), host="0.0.0.0", port=8080)

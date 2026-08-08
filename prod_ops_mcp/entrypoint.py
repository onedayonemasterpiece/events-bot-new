from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time

from .config import OpsMCPConfig


def _exec_bot() -> None:
    os.execv(sys.executable, [sys.executable, "main.py"])


def main() -> None:
    if os.getenv("ENABLE_PROD_OPS_MCP", "0").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        _exec_bot()
        return

    logging.basicConfig(level=logging.INFO)
    try:
        OpsMCPConfig.from_env(require_enabled=True)
    except Exception as exc:
        # The optional gateway fails closed without taking the production bot
        # down. No listener is started with invalid security configuration.
        logging.error("prod_ops_mcp disabled due to invalid configuration: %s", exc)
        _exec_bot()
        return

    bot = subprocess.Popen([sys.executable, "main.py"])
    gateway = subprocess.Popen([sys.executable, "-m", "prod_ops_mcp"])
    stopping = False
    gateway_failure_reported = False

    def stop(signum: int, _frame) -> None:
        nonlocal stopping
        if stopping:
            return
        stopping = True
        for process in (gateway, bot):
            if process.poll() is None:
                process.send_signal(signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        while True:
            bot_code = bot.poll()
            gateway_code = gateway.poll()
            if bot_code is not None:
                if gateway.poll() is None:
                    gateway.terminate()
                try:
                    gateway.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    gateway.kill()
                raise SystemExit(bot_code)
            if gateway_code is not None and not gateway_failure_reported:
                gateway_failure_reported = True
                logging.error(
                    "prod_ops_mcp sidecar exited code=%s; bot remains primary; "
                    "automatic restart is intentionally disabled",
                    gateway_code,
                )
            time.sleep(1)
    finally:
        stop(signal.SIGTERM, None)
        for process in (gateway, bot):
            if process.poll() is not None:
                continue
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


if __name__ == "__main__":
    main()

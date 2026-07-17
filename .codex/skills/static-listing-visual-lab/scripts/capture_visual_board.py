#!/usr/bin/env python3
"""Capture full-page desktop/mobile screenshots of a visual board with Playwright."""
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="http(s) URL or local HTML path")
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise SystemExit("Install Python Playwright and Chromium before capture") from exc
    value = args.url
    path = Path(value)
    url = path.resolve().as_uri() if path.exists() else value
    args.output_dir.mkdir(parents=True, exist_ok=True)
    viewports = {"desktop": (1440, 900), "mobile": (390, 844)}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        for name, (width, height) in viewports.items():
            page = browser.new_page(viewport={"width": width, "height": height}, device_scale_factor=1)
            page.goto(url, wait_until="networkidle")
            page.screenshot(path=str(args.output_dir / f"visual-board-{name}.png"), full_page=True)
        browser.close()


if __name__ == "__main__":
    main()

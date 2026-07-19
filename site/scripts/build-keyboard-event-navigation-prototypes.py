#!/usr/bin/env python3
"""Build the two immutable desktop keyboard-navigation prototype pages only."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

EVENT_SLUGS = (
    "spektakl-sobaka-na-sene-kaliningrad-6408",
    "spektakl-elementarno-hadson-delo-o-sobake-b-kaliningrad-6593",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-prefix", default="preview-20260719-keyboard-event-navigation-v1")
    parser.add_argument("--target-prefix", default="preview-20260719-keyboard-event-navigation-v5")
    args = parser.parse_args()

    site = Path(__file__).resolve().parents[1]
    dist = site / "dist"
    target_root = dist / args.target_prefix
    if target_root.exists():
        shutil.rmtree(target_root)

    component = (site / "src/components/KeyboardEventNavigationPrototype.astro").read_text()
    component = component.replace("<script is:inline>", "<script>").replace("<style is:global is:inline>", "<style>")
    if "is:inline" in component or "is:global" in component:
        raise RuntimeError("Prototype component still contains Astro-only inline directives")

    footer = '<footer class="site-footer'
    old_event_prefix = f"/{args.source_prefix}/sobytiya/"
    outputs: list[Path] = []
    for slug in EVENT_SLUGS:
        source = dist / args.source_prefix / "sobytiya" / slug / "index.html"
        html = source.read_text()
        if html.count(footer) != 1:
            raise RuntimeError(f"Expected one footer in {source}")
        if "data-keyboard-quickstart" in html:
            raise RuntimeError(f"Source already contains the keyboard prototype: {source}")
        html = html.replace(old_event_prefix, "/sobytiya/")
        html = html.replace(footer, component + "\n" + footer, 1)
        target = target_root / "sobytiya" / slug / "index.html"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(html)
        outputs.append(target)

    files = sorted(path for path in target_root.rglob("*") if path.is_file())
    if files != sorted(outputs):
        raise RuntimeError(f"Unexpected generated files: {files}")
    for target in files:
        html = target.read_text()
        required = (
            'name="robots" content="noindex,nofollow,noarchive"',
            "data-keyboard-quickstart",
            "data-event-content-copy-actions",
            "ke_keyboard_shortcut_daily_v2",
        )
        missing = [marker for marker in required if marker not in html]
        if missing:
            raise RuntimeError(f"Missing markers in {target}: {missing}")
        if old_event_prefix in html:
            raise RuntimeError(f"Preview event link leaked into {target}")

    for target in files:
        print(f"{target.relative_to(site)}\t{target.stat().st_size} bytes")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

MARKER = "    app = web.Application()\n"
APP_MODULE_CANDIDATES = ("main.py", "main_part2.py")
ROOT_FILES = (
    "private_events_mcp_provider_adapters.py",
    "private_events_mcp_telegram_adapter.py",
    "private_events_mcp_vk_adapter.py",
    "private_events_mcp_workspace_providers.py",
)
PRIVATE_FIXTURE_MARKER = "@pytest.fixture\ndef repo_root"
INSERT = (
    "    app = web.Application()\n"
    "    # Private Events MCP: strict no-op unless PRIVATE_EVENTS_MCP_ENABLED=1.\n"
    "    from private_events_mcp import PrivateEventsMCPConfig, attach_private_events_mcp\n"
    "    private_mcp_config = PrivateEventsMCPConfig.from_env()\n"
    "    private_mcp_social_adapters = None\n"
    "    private_mcp_workspace_adapters = None\n"
    "    if private_mcp_config.enabled:\n"
    "        if private_mcp_config.universal_social_enabled:\n"
    "            from private_events_mcp_workspace_providers import (\n"
    "                build_private_events_mcp_workspace_adapters,\n"
    "            )\n"
    "            private_mcp_workspace_adapters = build_private_events_mcp_workspace_adapters(private_mcp_config)\n"
    "        else:\n"
    "            from main import vk_api\n"
    "            from private_events_mcp_provider_adapters import (\n"
    "                build_private_events_mcp_social_adapters,\n"
    "            )\n"
    "            private_mcp_social_adapters = build_private_events_mcp_social_adapters(vk_api)\n"
    "    attach_private_events_mcp(\n"
    "        app, private_mcp_config, social_adapters=private_mcp_social_adapters,\n"
    "        social_workspace_adapters=private_mcp_workspace_adapters,\n"
    "    )\n"
)


def copy_tree(source: Path, target: Path, *, skip: frozenset[Path] = frozenset()) -> None:
    for path in source.rglob("*"):
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        relative = path.relative_to(source)
        if relative in skip:
            continue
        destination = target / relative
        if path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)


def find_app_module(repo: Path) -> Path:
    matches = []
    for relative in APP_MODULE_CANDIDATES:
        candidate = repo / relative
        if candidate.is_file() and MARKER in candidate.read_text(encoding="utf-8"):
            matches.append(candidate)
    if len(matches) != 1:
        names = ", ".join(str(path) for path in matches) or "none"
        raise RuntimeError(
            f"Expected exactly one app module containing {MARKER.strip()!r}; found {names}. "
            "Integrate manually before route registration."
        )
    return matches[0]


def merge_test_fixtures(source: Path, target: Path) -> bool:
    if not source.is_file():
        return False
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        return True

    incoming = source.read_text(encoding="utf-8")
    current = target.read_text(encoding="utf-8")
    if "def event_db_digest(" in current:
        return False
    marker_at = incoming.find(PRIVATE_FIXTURE_MARKER)
    if marker_at < 0:
        raise RuntimeError(f"Private MCP fixtures not found in {source}")
    for fixture_name in ("repo_root", "event_db", "config", "event_db_digest"):
        if f"def {fixture_name}(" in current:
            raise RuntimeError(
                f"Refusing to overwrite existing pytest fixture {fixture_name!r} in {target}"
            )
    if "import sqlite3\n" not in current:
        current = current.replace("import hashlib\n", "import hashlib\nimport sqlite3\n", 1)
    if "from pathlib import Path\n" not in current:
        current = current.replace("import sqlite3\n", "import sqlite3\nfrom pathlib import Path\n", 1)
    if "from private_events_mcp.config import PrivateEventsMCPConfig\n" not in current:
        current = current.rstrip() + "\n\nfrom private_events_mcp.config import PrivateEventsMCPConfig\n"
    fixture_block = incoming[marker_at:]
    target.write_text(current.rstrip() + "\n\n\n" + fixture_block, encoding="utf-8")
    return True


def patch_main(path: Path) -> bool:
    content = path.read_text(encoding="utf-8")
    if "attach_private_events_mcp(" in content:
        return False
    occurrences = content.count(MARKER)
    if occurrences != 1:
        raise RuntimeError(
            f"Expected exactly one {MARKER.strip()!r} marker in {path}; found {occurrences}. "
            "Integrate manually before route registration."
        )
    path.write_text(content.replace(MARKER, INSERT, 1), encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply the private events MCP repository overlay.")
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument(
        "--overlay",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Overlay root containing private_events_mcp, tests, docs and scripts.",
    )
    args = parser.parse_args()
    repo = args.repo.resolve()
    overlay = args.overlay.resolve()
    app_module = find_app_module(repo)
    source_conftest = overlay / "tests" / "conftest.py"
    target_conftest = repo / "tests" / "conftest.py"

    for relative in ("private_events_mcp", "tests", "docs", "scripts"):
        source = overlay / relative
        if source.exists():
            skipped = frozenset({Path("conftest.py")}) if relative == "tests" else frozenset()
            copy_tree(source, repo / relative, skip=skipped)
    for relative in ROOT_FILES:
        source = overlay / relative
        if source.is_file():
            shutil.copy2(source, repo / relative)
    fixtures_changed = merge_test_fixtures(source_conftest, target_conftest)
    changed = patch_main(app_module)
    print(
        f"overlay_applied=1 main_patched={int(changed)} "
        f"fixtures_merged={int(fixtures_changed)} app_module={app_module.name} repo={repo}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

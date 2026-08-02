#!/usr/bin/env python3
"""Build bounded Region Talk social-source capture rows.

The production acquisition adapter lives in CandidateReport because Kaggle
ships that module as one self-contained source file.  This entrypoint exposes
the same deterministic contract to local callers and offline fixtures without
opening another Telegram session or making any LLM/provider request.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
IMPLEMENTATION_PATH = (
    ROOT / "kaggle" / "RegionTalkCandidateReport" / "region_talk_candidate_report.py"
)


def _load_implementation() -> Any:
    module_name = "_region_talk_candidate_report_capture_contract"
    existing = sys.modules.get(module_name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(module_name, IMPLEMENTATION_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load CandidateReport capture contract: {IMPLEMENTATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_IMPLEMENTATION = _load_implementation()

SOURCE_PROFILE_CAPTURE_VERSION = _IMPLEMENTATION.SOURCE_PROFILE_CAPTURE_VERSION
SOURCE_PROFILE_CAPTURE_FINGERPRINT_VERSION = _IMPLEMENTATION.SOURCE_PROFILE_CAPTURE_FINGERPRINT_VERSION
capture_settings = _IMPLEMENTATION.capture_settings
normalize_source_profile_capture_text = _IMPLEMENTATION.normalize_source_profile_capture_text
source_profile_capture_canonical_source_key = _IMPLEMENTATION.source_profile_capture_canonical_source_key
classify_source_profile_capture_post = _IMPLEMENTATION.classify_source_profile_capture_post
build_source_profile_capture = _IMPLEMENTATION.build_source_profile_capture
source_profile_capture_storage_pk = _IMPLEMENTATION.source_profile_capture_storage_pk
capture_change_decision = _IMPLEMENTATION.capture_change_decision


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a deterministic Region Talk social-source capture from JSON; no network/provider calls."
    )
    parser.add_argument("input_json", type=Path, help="JSON object with source, posts, optional description/pinned_post")
    parser.add_argument("--output", type=Path, help="Write capture JSON here (stdout by default)")
    args = parser.parse_args(argv)
    payload = json.loads(args.input_json.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("source"), dict) or not isinstance(payload.get("posts"), list):
        parser.error("input must be an object containing source object and posts array")
    capture = build_source_profile_capture(
        payload["source"],
        payload["posts"],
        description=str(payload.get("description") or ""),
        pinned_post=payload.get("pinned_post") if isinstance(payload.get("pinned_post"), dict) else None,
        scan_posts=payload.get("scan_posts"),
        min_authored_posts=payload.get("min_authored_posts"),
        selected_excerpts=payload.get("selected_excerpts"),
        archive_exhausted=bool(payload.get("archive_exhausted")),
    )
    rendered = json.dumps(capture, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

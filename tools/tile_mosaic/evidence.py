"""Portable, deterministic sidecar helpers for committed review evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .core import reproducible_timestamp


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def normalize_baseline_manifest(
    manifest_path: str | Path,
    *,
    input_reference: str,
    output_name: str,
    plan_name: str,
) -> dict[str, Any]:
    """Strip runner-local paths from a generated baseline manifest.

    The downloaded source candidate may have a transport-specific temporary
    filename.  The accepted evidence contract refers only to the selected
    source bytes and stable artifact names, so a successful fallback transport
    cannot create an otherwise meaningless Git diff.
    """

    path = Path(manifest_path).expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["generated_at"] = reproducible_timestamp()
    payload.setdefault("input", {})["reference"] = str(input_reference)
    payload.setdefault("output", {})["path"] = str(output_name)
    backend_result = payload.get("backend_result")
    if isinstance(backend_result, dict) and "output" in backend_result:
        backend_result["output"] = str(output_name)
    plan = payload.setdefault("plan", {})
    plan["input_path"] = str(plan_name)
    plan["path"] = str(plan_name)
    preset = payload.get("preset")
    if isinstance(preset, dict) and preset.get("path"):
        preset["path"] = f"presets/{Path(str(preset['path'])).name}"
    _write_json(path, payload)
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize tile-mosaic review sidecars")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--input-reference", required=True)
    parser.add_argument("--output-name", required=True)
    parser.add_argument("--plan-name", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = normalize_baseline_manifest(
        args.manifest,
        input_reference=args.input_reference,
        output_name=args.output_name,
        plan_name=args.plan_name,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

"""End-to-end LLM-driven pipeline for one event id, from the live DB.

This is the production-shaped path used for today's honest 6-event selection:

  1. Pull event row from `db_prod_snapshot.sqlite`.
  2. Download the primary + up to 3 extras (with phash dedup).
  3. Run the deterministic preliminary pass (poster_analysis + region
     detection + sticker rendering) — we still need the sticker PNGs.
  4. Replace the deterministic placement with LLM-A.
  5. Plan camera with LLM-B.
  6. Write a manifest to `slot_<id>_llm/`.

After this script runs for an event, the operator can drive `blender`
+ `slot_trace.py` to render the visual artefacts.

Usage:
    .venv/bin/python scripts/afishathumb/llm_full_pipeline.py 4834
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "afishathumb"))

from prepare_slot import prepare_slot  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("event_id", type=int)
    args = ap.parse_args()
    # First populate slot_<id>/ with poster + stickers via the
    # deterministic pass (we reuse its sticker rendering work).
    prepare_slot(args.event_id, placement="algo")
    # Then run LLM-A + LLM-B using the cached artefacts and write
    # slot_<id>_llm/manifest.json.
    from llm_ab_test import run as ab_run  # noqa: WPS433
    ab_run(args.event_id)


if __name__ == "__main__":
    main()

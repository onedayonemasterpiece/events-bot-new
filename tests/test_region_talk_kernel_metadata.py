from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_region_talk_script_kernels_disable_sibling_file_instrumentation() -> None:
    """Kaggle script pushes upload only ``code_file`` as the executable body.

    The shared status wrapper depends on a renamed sibling source file, so
    Region Talk workers (which already write their own durable heartbeats) must
    explicitly opt out of that wrapper.
    """

    kernel_dirs = (
        "RegionTalkCandidateReport",
        "RegionTalkBgeM3Enrichment",
        "RegionTalkImageDiagnostic",
        "RegionTalkQwen3Embedding06BEnrichment",
    )
    for directory in kernel_dirs:
        metadata_path = ROOT / "kaggle" / directory / "kernel-metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        assert metadata["kernel_type"] == "script"
        assert metadata["events_bot_disable_status_instrumentation"] is True

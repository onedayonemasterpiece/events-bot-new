from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR_PATH = ROOT / "scripts" / "validate_product_atlas_v1.py"


def _load_validator():
    spec = importlib.util.spec_from_file_location("validate_product_atlas_v1", VALIDATOR_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_product_atlas_v1_is_semantically_closed() -> None:
    summary = _load_validator().validate()
    assert summary["entities"] > 0
    assert summary["entity_kinds"] >= 10
    assert summary["sources"] > 0
    assert summary["archetypes"] == 17
    assert summary["user_stories"] > 0

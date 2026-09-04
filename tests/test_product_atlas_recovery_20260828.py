from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_product_atlas_recovery_20260828() -> None:
    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, str(root / "scripts/validate_product_atlas_recovery_20260828.py")],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "PRODUCT_ATLAS_RECOVERY_20260828_PASS" in completed.stdout

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def fixture_dir() -> Path:
    return Path(__file__).parent / "fixtures" / "volunteer_monitor"

"""Neural-first contour SVG generator.

The package is intentionally import-light: heavyweight CV/ML dependencies are
loaded inside the stages that need them so local tests can validate config and
SVG contracts without a Kaggle GPU environment.
"""

from .config import RunConfig, load_config
from .pipeline import ContourGenerator, RunResult

__all__ = ["ContourGenerator", "RunConfig", "RunResult", "load_config"]

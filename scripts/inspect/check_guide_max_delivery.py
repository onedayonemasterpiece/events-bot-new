"""Bounded offline release checks for native guide visual delivery."""
from pathlib import Path
import py_compile
import subprocess

ROOT = Path(__file__).resolve().parents[2]
for relative in ("guide_excursions/max_delivery.py", "guide_excursions/max_digest.py",
                 "guide_excursions/vibepublish_client.py", "guide_excursions/visual_digest.py",
                 "scheduling.py", "markup.py"):
    py_compile.compile(str(ROOT / relative), doraise=True)
subprocess.run(["git", "diff", "--check"], cwd=ROOT, check=True, timeout=30)
subprocess.run(["git", "diff", "--cached", "--check"], cwd=ROOT, check=True, timeout=30)
print("compile_and_diff_check=passed")

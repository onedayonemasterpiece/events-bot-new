import hashlib
import json
import sys
from pathlib import Path

EXPECTED_FILES = {
    "transport_refresh/__init__.py", "transport_refresh/ics.py", "transport_refresh/kernel.py",
    "transport_refresh/provider_job.py", "transport_refresh/schema.py",
    "transport_refresh/selection.py", "transport_refresh/store.py",
}


def _mount_runtime() -> None:
    matches = sorted(Path("/kaggle/input").rglob("transport_refresh_package_manifest.json"))
    if len(matches) != 1:
        raise RuntimeError(f"expected one transport runtime manifest, got {len(matches)}")
    manifest_path = matches[0]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = manifest.get("files") if manifest.get("schema_version") == "kenigevents.transport_runtime_package.v1" else None
    if not isinstance(files, dict) or set(files) != EXPECTED_FILES:
        raise RuntimeError("transport runtime manifest has unexpected files")
    root = manifest_path.parent.resolve()
    for relative, expected_hash in files.items():
        path = (root / relative).resolve()
        path.relative_to(root)
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
            raise RuntimeError(f"transport runtime hash mismatch: {relative}")
    sys.path.insert(0, str(root))


_mount_runtime()

from transport_refresh.kernel import kernel_main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(kernel_main("kppk"))

#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
import time
import zipfile
from io import BytesIO
from pathlib import Path

from video_announce.kaggle_client import KaggleClient
from video_announce.story_publish import (
    STORY_PUBLISH_CONFIG_FILENAME,
    ensure_story_secret_datasets,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("crumple_story_smoke")

PROJECT_ROOT = Path(__file__).parent.parent
KERNEL_DIR = PROJECT_ROOT / "kaggle" / "CrumpleVideo"
TEST_AFISHA_DIR = PROJECT_ROOT / "video_announce" / "test_afisha"
ASSETS_DIR = PROJECT_ROOT / "video_announce" / "assets"
CYGRE_FONT_DIR = PROJECT_ROOT / "kaggle" / "CherryFlash" / "assets" / "ro_znanie_fonts"
CYGRE_FONT_ZIP = CYGRE_FONT_DIR.parent / "ro_znanie.zip"
OUTPUT_DIR = PROJECT_ROOT / "artifacts" / "codex" / "crumple_story_smoke"


def _require_env(key: str) -> str:
    value = (os.getenv(key) or "").strip()
    if not value:
        raise RuntimeError(f"{key} is required")
    return value


def _story_target() -> str:
    raw = (
        os.getenv("CRUMPLE_STORY_SMOKE_TARGET")
        or os.getenv("VIDEO_ANNOUNCE_STORY_SMOKE_TARGET")
        or ""
    ).strip()
    if not raw:
        raise RuntimeError(
            "Set CRUMPLE_STORY_SMOKE_TARGET or VIDEO_ANNOUNCE_STORY_SMOKE_TARGET"
        )
    if raw.startswith("https://t.me/"):
        return "@" + raw.split("https://t.me/", 1)[1].split("/", 1)[0]
    return raw


def _create_payload(image_name: str) -> dict:
    return {
        "intro": {
            "count": 1,
            "text": "STORY SMOKE",
            "cities": ["Калининград"],
        },
        "selection_params": {
            "mode": "test",
            "is_test": True,
            "story_publish_mode": "image",
            "story_publish_smoke_only": True,
        },
        "scenes": [
            {
                "about": "Smoke story publish",
                "date": "сегодня",
                "location": "Калининград",
                "image": image_name,
                "images": [image_name],
            }
        ],
    }


async def run_story_smoke() -> bool:
    client = KaggleClient()
    assets_dataset = os.getenv(
        "CRUMPLE_ASSETS_DATASET", "richss777/video-announce-assets"
    ).strip()
    target_peer = _story_target()
    _require_env("KAGGLE_USERNAME")
    _require_env("KAGGLE_KEY")

    images = sorted(
        [
            path
            for path in TEST_AFISHA_DIR.iterdir()
            if path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
        ]
    )
    if not images:
        raise RuntimeError(f"No smoke images found in {TEST_AFISHA_DIR}")
    image_path = images[0]

    with tempfile.TemporaryDirectory() as tmp_dir:
        dataset_path = Path(tmp_dir) / "crumple-story-smoke"
        dataset_path.mkdir(parents=True, exist_ok=True)
        shutil.copy2(image_path, dataset_path / image_path.name)
        for font in ASSETS_DIR.glob("*.ttf"):
            shutil.copy2(font, dataset_path / font.name)
        for font in ASSETS_DIR.glob("*.otf"):
            shutil.copy2(font, dataset_path / font.name)
        for font_name in ("Cygre-Medium.ttf", "Cygre-Regular.ttf"):
            font_path = CYGRE_FONT_DIR / font_name
            if font_path.exists():
                shutil.copy2(font_path, dataset_path / font_name)
                continue
            if CYGRE_FONT_ZIP.exists():
                with zipfile.ZipFile(CYGRE_FONT_ZIP) as zf:
                    member = next(
                        (
                            name
                            for name in zf.namelist()
                            if not name.endswith("/") and Path(name).name == font_name
                        ),
                        None,
                    )
                    if member is not None:
                        (dataset_path / font_name).write_bytes(zf.read(member))
                        continue
                    nested_zip_name = next(
                        (
                            name
                            for name in zf.namelist()
                            if not name.endswith("/") and Path(name).name == "cygre_default.zip"
                        ),
                        None,
                    )
                    if nested_zip_name is not None:
                        with zipfile.ZipFile(BytesIO(zf.read(nested_zip_name))) as nested:
                            nested_member = next(
                                (
                                    name
                                    for name in nested.namelist()
                                    if not name.endswith("/") and Path(name).name == font_name
                                ),
                                None,
                            )
                            if nested_member is not None:
                                (dataset_path / font_name).write_bytes(nested.read(nested_member))
        payload = _create_payload(image_path.name)
        (dataset_path / "payload.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (dataset_path / STORY_PUBLISH_CONFIG_FILENAME).write_text(
            json.dumps(
                {
                    "version": 1,
                    "mode": "image",
                    "smoke_only": True,
                    "period_seconds": 24 * 60 * 60,
                    "targets": [
                        {
                            "peer": target_peer,
                            "label": target_peer,
                            "delay_seconds": 0,
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        dataset_slug = f"{os.getenv('KAGGLE_USERNAME').strip()}/crumple-story-smoke"
        (dataset_path / "dataset-metadata.json").write_text(
            json.dumps(
                {
                    "id": dataset_slug,
                    "title": "Crumple Story Smoke",
                    "licenses": [{"name": "CC0-1.0"}],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        try:
            client.create_dataset(dataset_path)
        except Exception:
            logger.exception("dataset create failed, trying version update")
            client.create_dataset_version(
                dataset_path,
                version_notes="story smoke refresh",
                quiet=True,
                convert_to_csv=False,
                dir_mode="zip",
            )

        secret_datasets = await ensure_story_secret_datasets(client)
        logger.info("story secret datasets: %s", secret_datasets)

        await asyncio.sleep(15)
        client.push_kernel(
            kernel_path=KERNEL_DIR,
            dataset_sources=[dataset_slug, assets_dataset, *secret_datasets],
        )
        meta = json.loads((KERNEL_DIR / "kernel-metadata.json").read_text(encoding="utf-8"))
        kernel_ref = meta.get("id", "richss777/crumple-video")

    logger.info("kernel deployed: %s", kernel_ref)
    start_time = time.time()
    max_wait = 30 * 60
    while time.time() - start_time < max_wait:
        status = client.get_kernel_status(kernel_ref)
        state = str(status.get("status") or "").upper()
        logger.info("status=%s", state or "UNKNOWN")
        if state == "COMPLETE":
            break
        if state in {"ERROR", "FAILED", "CANCELLED"}:
            raise RuntimeError(f"Kernel failed: {status}")
        await asyncio.sleep(20)
    else:
        raise TimeoutError("Story smoke kernel timed out")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    files = client.download_kernel_output(kernel_ref, path=OUTPUT_DIR, force=True)
    logger.info("downloaded files: %s", files)
    report_path = OUTPUT_DIR / "story_publish_report.json"
    if not report_path.exists():
        raise RuntimeError("story_publish_report.json not found in kernel output")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    logger.info("story publish report: %s", report)
    return bool(report.get("ok"))


if __name__ == "__main__":
    ok = asyncio.run(run_story_smoke())
    raise SystemExit(0 if ok else 1)

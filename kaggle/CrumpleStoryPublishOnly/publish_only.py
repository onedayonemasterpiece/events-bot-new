from __future__ import annotations

import asyncio
import importlib.util
import json
import shutil
from pathlib import Path

WORKING_DIR = Path('/kaggle/working')
INPUT_DIR = Path('/kaggle/input')


def log(message: str) -> None:
    print(message, flush=True)


def _first_existing(patterns: list[str]) -> Path | None:
    for pattern in patterns:
        for path in sorted(INPUT_DIR.glob(pattern)):
            if path.exists() and path.is_file():
                return path
    return None


def _copy_helper() -> Path:
    candidates = sorted(INPUT_DIR.glob('*/kaggle_common/story_publish.py'))
    for helper_path in candidates:
        if helper_path.exists():
            target = WORKING_DIR / 'story_publish.py'
            shutil.copy2(helper_path, target)
            log(f'✅ Copied story_publish.py from bundled helper {helper_path}')
            return target
    raise RuntimeError('kaggle_common/story_publish.py not found in publish-only dataset')


def _load_story_publish_module(helper_path: Path):
    spec = importlib.util.spec_from_file_location('story_publish', helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Could not load story_publish module from {helper_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


helper_path = _copy_helper()
story_publish = _load_story_publish_module(helper_path)

final_video = _first_existing([
    '*/crumple_video_final.mp4',
    '*/crumple_video_story_720x1280.mp4',
    '**/crumple_video_final.mp4',
    '**/crumple_video_story_720x1280.mp4',
])
if final_video is None:
    raise RuntimeError('publish-only final video not found in Kaggle input')

config_path = _first_existing(['*/story_publish.json', '**/story_publish.json'])
if config_path is None:
    raise RuntimeError('publish-only story_publish.json not found in Kaggle input')

log(f'▶️ Publish-only source video: {final_video}')
log(f'▶️ Publish-only config: {config_path}')

report = asyncio.run(
    story_publish.publish_story_from_kaggle(
        final_video_path=final_video,
        intro_path=None,
        posters=[],
        search_roots=[config_path.parent, INPUT_DIR],
        output_dir=WORKING_DIR,
        log=log,
    )
)

(WORKING_DIR / 'publish_only_manifest.json').write_text(
    json.dumps(
        {
            'ok': bool(report and report.get('ok')),
            'fanout_ok': bool(report and report.get('fanout_ok')),
            'required_ok': bool(report and report.get('required_ok')),
            'source_video': str(final_video),
            'config_path': str(config_path),
        },
        ensure_ascii=False,
        indent=2,
    ),
    encoding='utf-8',
)

if not report or not report.get('ok'):
    raise RuntimeError('publish-only story publish failed; see story_publish_report.json')
log('✅ Publish-only story publish completed')

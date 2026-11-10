"""Execute the notebook's real asset loader with legacy and retained Kaggle mounts."""
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FONTS = ['Benzin-Bold.ttf', 'BebasNeue-Bold.ttf', 'Oswald-VariableFont_wght.ttf', 'DrukCyr-Bold.ttf']


@pytest.mark.parametrize('mount', ['video-afisha-session-1601', 'datasets/richss777/video-afisha-session-1601', None])
def test_actual_notebook_asset_loader(tmp_path, mount):
    notebook = json.loads((ROOT / 'kaggle/CrumpleVideo/crumple_video.ipynb').read_text())
    source = next(''.join(c['source']) for c in notebook['cells'] if 'required_font_names' in ''.join(c.get('source', [])))
    source = source[:source.index('_MONTHS_RU =')]
    input_root = tmp_path / 'input'
    input_root.mkdir()
    working = tmp_path / 'working'
    if mount:
        dataset = input_root / mount
        dataset.mkdir(parents=True)
        for name in FONTS:
            (dataset / name).write_bytes(b'path-control-only')
        (dataset / 'pattern_preview.py').write_text('# path control\n')
        (dataset / 'assets').mkdir()
        (dataset / 'assets' / 'music.mp3').write_bytes(b'path-control-only')
    source = source.replace('/kaggle/input', str(input_root)).replace('/kaggle/working', str(working))
    scope = {}
    if mount is None:
        with pytest.raises(FileNotFoundError, match='Required CrumpleVideo intro font not found'):
            exec(compile(source, 'actual-crumple-asset-loader', 'exec'), scope)
    else:
        exec(compile(source, 'actual-crumple-asset-loader', 'exec'), scope)
        assert (working / 'pattern_preview.py').read_text() == '# path control\n'
        assert (working / 'assets' / 'music.mp3').read_bytes() == b'path-control-only'
        for name in FONTS:
            assert (working / 'assets' / name).read_bytes() == b'path-control-only'

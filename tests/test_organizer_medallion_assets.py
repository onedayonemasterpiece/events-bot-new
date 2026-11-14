import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from PIL import Image, ImageChops
import pytest

from tg_graphic_medallions import resolve_event_graphic_medallions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('slug,name', [('bar-bastion', 'Бар Бастион'),
                                      ('dom-kitoboya', 'Дом китобоя'),
                                      ('dom-semyi', 'Дом семьи')])
def test_site_and_vk_share_normalized_circular_assets(slug, name):
    manifest = json.loads((ROOT / 'site/src/data/organizerMedallions.json').read_text())
    item = next(item for item in manifest['items'] if item['slug'] == slug)
    script = """
import {resolveEventMedallions} from './site/src/lib/eventMedallions.ts';
import {readFileSync} from 'node:fs';
const items = JSON.parse(readFileSync('site/src/data/organizerMedallions.json','utf8')).items;
const event = {venue_name: process.argv[1], organizer_names: [], source_urls: []};
console.log(JSON.stringify(resolveEventMedallions(event, items).identities.map(x => x.item.slug)));
"""
    result = subprocess.check_output(['node', '--experimental-strip-types', '--input-type=module',
                                      '-e', script, name], cwd=ROOT, text=True)
    assert json.loads(result) == [slug]
    site_path = ROOT / 'site/public' / item['avatarUrl'].lstrip('/')
    fallback = ROOT / 'site/public' / item['fallbackPngUrl'].lstrip('/')
    resolved = resolve_event_graphic_medallions(SimpleNamespace(location_name=name))
    assert Path(next(m for m in resolved if m['slug'] == slug)['asset_path']) == fallback
    with Image.open(fallback) as png, Image.open(site_path) as webp:
        png, webp = png.convert('RGBA'), webp.convert('RGBA')
        assert png.size == webp.size == (512, 512)
        assert png.getpixel((0, 0))[3] == 0
        assert png.getpixel((256, 256))[3] == 255
        assert png.getchannel('A').getbbox() == (0, 0, 512, 512)
        background = Image.new('RGBA', png.size, '#E9EDE1')
        difference = ImageChops.difference(Image.alpha_composite(background, png).convert('RGB'),
                                         Image.alpha_composite(background, webp).convert('RGB'))
        assert difference.getbbox() is None
        if slug == 'bar-bastion':
            assert max(png.getpixel((12, 256))[:3]) < 240  # No white field inside the circle.

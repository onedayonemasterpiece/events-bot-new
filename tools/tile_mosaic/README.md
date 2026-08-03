# Tile Mosaic Material Lab

Deterministic build-time generator for a continuous image projected over a
physical-looking tile grid.

## Main commands

```bash
python -m tools.tile_mosaic.generate --input source.jpg --output render.png
python -m tools.tile_mosaic.refine_render --base-render baseline.png --base-plan baseline.png.plan.json --profile tools/tile_mosaic/refinements/reference_balanced_v1.json --output variant.png
python -m tools.tile_mosaic.lab --frozen-base-render baseline.png --frozen-base-plan baseline.png.plan.json --output-dir artifacts/lab
python -m unittest discover -s tests -p 'test_tile_mosaic_*.py' -v
```

`presets/` are full source-to-render profiles. `refinements/` are bounded,
second-stage studies that preserve the accepted frozen baseline.

# Blender compatibility contract

The physical renderer is exercised against checksum-pinned Blender `4.0.2` in GitHub Actions.

Blender 4.0 identifies EEVEE as `BLENDER_EEVEE`; newer Blender releases identify the successor as `BLENDER_EEVEE_NEXT`. `blender_renderer.py` probes both enum identifiers and fails loudly with the supported RNA enum list when neither is available.

This file is intentionally inside `tools/tile_mosaic/` so a connector-authored compatibility change starts the complete material-laboratory workflow after a source fix committed by GitHub Actions.
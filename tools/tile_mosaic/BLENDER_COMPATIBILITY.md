# Blender compatibility contract

The physical renderer is exercised against checksum-pinned Blender `4.0.2` in GitHub Actions.

Blender 4.0 identifies EEVEE as `BLENDER_EEVEE`; newer Blender releases identify the successor as `BLENDER_EEVEE_NEXT`. `blender_renderer.py` probes both enum identifiers and fails loudly with the supported RNA enum list when neither is available.

Blender 4.0 also requires Auto Smooth before hardened bevel normals and the Weighted Normal modifier are valid. The tile mesh enables that contract where the RNA property exists; on later Blender lines it keeps the ordinary bevel-normal path instead of emitting invalid-modifier warnings.

EEVEE needs an EGL/OpenGL runtime even in background mode. Minimal CI containers may omit `libEGL.so.1`; in that environment an EEVEE request deterministically falls back to Cycles CPU. The renderer records both `requested_engine` and the effective `engine` in its terminal receipt, while keeping the same immutable scene plan, UV projection, geometry and material parameters.

This compatibility document is inside `tools/tile_mosaic/` so a connector-authored source-adjacent commit starts the complete material-laboratory workflow after a source fix committed by GitHub Actions.
# Contour SVG Generator Spec v0.2

This file is intentionally kept as a historical pointer only.

The active engineering contract is:

- `requirements.md`
- `contour_svg_generator_engineering_spec_v0_3.md`
- `gemini_prompts_and_schemas_v0_1.md`
- `models_tools_catalog_v0_1.md`
- `architecture_elements_library_v0_1.md`
- `implementation_backlog_v0_1.md`

Do not implement from v0.2. It predated the current neural-first/no-surrogate
contract and contains outdated assumptions. The current final path is:

```text
multi-state masks -> candidate line graph -> semantic pruning -> primitive rendering -> final.svg
```

Raster traces, ControlNet rasters, Canny/edge maps and vector-traced line-art
may be saved as proposal/debug artifacts, but they are not allowed to become
`final.svg` unless they have been converted into semantic line groups and then
rendered through the primitive renderer.

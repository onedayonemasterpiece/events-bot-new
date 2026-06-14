# Evidence Fusion Pipeline Design — 2026-06-14

This is a working design note based on `requirements/user_audit_1.md` and the
Kaggle debug artifacts from the sample building runs. It does not replace the
canonical requirements.

## Core Correction

The useful intermediate artifacts are not independent final candidates. They
are evidence layers that should vote into one shared architectural scene graph.

The pipeline should move from:

```text
many debug maps → many independent candidates → ranking hopes one is coherent
```

to:

```text
many debug maps → fused architectural scene graph → controlled candidate families
```

The central object is not an edge map, a ControlNet raster, or a traced SVG. The
central object is a coarse-to-fine graph of the building:

- global shell / silhouette;
- roof mass and cornice bands;
- facade planes and the main corner edge;
- occluder/unknown zones where completion is allowed;
- semantic openings and architectural features;
- style/render instructions.

## What The User Audit Reveals

The previous Kaggle run already produced several high-value layers:

| Layer from audit | Interpretation | Role in to-be pipeline |
| --- | --- | --- |
| `facade wallplane` | Broad facade-plane evidence is useful. | Defines major planes and keeps the result from becoming loose fragments. |
| `deeplsd_lines_overlay` | Strong structural line evidence. | Votes for roof/cornice/vertical structure, especially long straight lines. |
| `edge_map` | Very accurate but too dense. | Evidence map for confirming lines/details, not direct final trace. |
| `elements_overlay` | Windows/openings are detected well enough as anchors. | Semantic anchors for window rhythm and feature preservation. |
| `masked_background` / masks | Gives strong object/background boundary cues. | Helps establish outer shell and reject non-building leakage. |
| `mlsd_guide` | Good large geometry. | Main structural scaffold for perspective and facade planes. |
| `occluder_mask` | Separates trees/foreground blockers. | Drives conservative interpolation through hidden regions. |

The design implication: the weak part is not lack of tools. The weak part is
the missing fusion/controller layer that decides how these pieces combine.

## Proposed To-Be Pipeline

### 1. Evidence Inventory

Every run first produces and records a typed evidence inventory:

```text
source photo
semantic plan
object / occluder / background / object_unknown masks
facade wall_plane and element masks
edge_map
M-LSD / DeepLSD / HAWP line sets
ControlNet / IP-Adapter neural line-art proposals
style reference
```

Each layer must have a declared role: shell, plane, structure, detail,
occlusion, style, or proposal. A layer without a role should not affect final
selection.

### 2. Coarse Shell First

Build a global building shell before selecting details.

Inputs:

- SAM2 object mask and allowed-line region;
- background/object boundary;
- facade `wall_plane` mask;
- longest M-LSD / DeepLSD / HAWP roof and facade lines;
- Gemini primary-object description and focus hints.

Output:

```text
BuildingShell {
  visible_hull,
  inferred_hull_under_occluders,
  roof_ridge_or_top_profile,
  base_profile,
  main_corner_edge,
  left/right/front plane polygons,
  confidence per segment
}
```

Acceptance at this stage:

- The result has a complete readable mass, not just local details.
- Roof/top profile, facade corner and base are present.
- Occluder regions may contain inferred shell segments only when supported by
  adjacent visible geometry.

### 3. Perspective And Plane Graph

Group straight lines by dominant perspective directions and assign them to
facade planes.

Inputs:

- M-LSD / DeepLSD / HAWP lines;
- facade wall_plane box/mask;
- shell polygons;
- edge-map support score.

Output:

```text
PlaneGraph {
  facade_planes[],
  vanishing_direction_groups[],
  cornice_bands[],
  vertical_edges[],
  balcony/base/step bands[]
}
```

This stage prevents the final candidate from mixing roof lines, side-wall lines
and texture lines as equal strokes.

### 4. Semantic Feature Anchors

Use facade parsing and edge-map confirmation to place details inside the plane
graph.

Inputs:

- `elements_overlay` / facade elements;
- edge-map local support;
- Gemini feature list;
- plane graph.

Output:

```text
FeatureGraph {
  windows[],
  doors[],
  arches[],
  balconies[],
  pilasters[],
  cornices[],
  stairs_or_base_details[]
}
```

Feature anchors are not allowed to define the building shell. They can only add
detail after the shell and planes are coherent.

### 5. Occluder-Aware Completion

Completion happens against the scene graph, not against raw line fragments.

Allowed:

- continue a roof/cornice/base line through `object_unknown`;
- repeat a window/opening only inside the same plane and rhythm;
- complete a facade edge if both shell and plane graph agree.

Forbidden:

- drawing foliage or fence edges as architecture;
- adding a new facade/wings/object not present in source;
- filling occluders with decorative texture.

Output primitives must carry `completion_status`:

- `visible_only`;
- `interpolated_shell`;
- `interpolated_band`;
- `repeated_pattern`;
- `neural_proposed_pending_validation`.

### 6. Neural Glue Branch

The neural branch should be treated as a compositor/repair advisor, not as a
standalone truth source.

Recommended experiments:

1. **Gemini fusion editor**
   - Input: source photo, masks overlay, edge map, M-LSD/DeepLSD overlay,
     elements overlay, current primitive preview, style reference.
   - Output: structured merge/keep/drop/extend instructions over scene-graph
     components.

2. **ControlNet/IP-Adapter line-art compositor**
   - Input: source-preserving img2img init, occluder-neutralized guide maps,
     depth, style reference.
   - Output: PNG proposal that shows how a coherent two-color graphic could
     look.
   - Usage: extract suggestions for missing shell/detail; never accept directly
     as `final.svg`.

3. **Pairwise repair loop**
   - Input: top primitive preview vs neural/style proposal vs reference style.
   - Output: repair plan: missing shell segment, over-detailed area, wrong
     occluder leakage, style mismatch.

The important design choice: neural output may help glue and simplify the
layers, but final SVG still comes from editable primitives.

### 7. Primitive Rendering Families

Candidates should become controlled views of the same fused scene graph:

| Family | Purpose |
| --- | --- |
| `shell_only` | Proves global silhouette/roof/base coherence. |
| `plane_scaffold` | Shows planes, corner, roof, cornice bands. |
| `balanced_architectural` | Shell + planes + key openings. |
| `postcard_minimal` | Fewest lines that preserve building identity. |
| `feature_emphasis_openings` | More windows/arches/balconies. |
| `neural_repair_applied` | Primitive graph after Gemini/neural repair suggestions. |

This avoids comparing unrelated candidates where one has good roof lines and
another has good windows but neither owns the full building.

### 8. Ranking Gates

Hard gates should run before aesthetic/Gemini ranking:

- shell completeness gate: roof/top profile, main corner, base and facade mass
  must exist;
- same-building gate: source identity preserved, no generic replacement;
- occluder leakage gate: tree/fence/sky/road lines cannot dominate;
- coarse-to-fine gate: shell and planes must be present before details;
- editability gate: final SVG uses primitives, no raster/image embed;
- style gate: two-color line graphic, sparse enough to read as a designed
  poster/postcard.

## Current Implementation Gap

What is already present:

- most evidence layers from the audit are produced and saved;
- multi-state masks exist;
- typed `EvidenceInventory` exists and writes JSON/contact-sheet artifacts;
- `BuildingShell` exists and writes shell JSON/overlay/score plus a
  `shell_only` diagnostic SVG/PNG candidate;
- `PlaneGraph` exists and writes plane JSON/overlay/score plus a
  `plane_scaffold` diagnostic SVG/PNG candidate;
- `FeatureGraph` exists and writes feature JSON/overlay/score plus a
  `feature_scaffold` diagnostic SVG/PNG candidate;
- line graph exists;
- facade parser exists;
- completion proposals exist;
- ControlNet/IP-Adapter proposal branch is being added;
- primitive renderer and SVG hard gates exist;
- Kaggle status events expose stages and debug artifacts.

What requires serious redesign before expecting sample-quality output:

- completion still works mostly from raw line groups and facade elements instead
  of a graph-level `OccluderAwareCompletionGraph`;
- neural branch does not yet operate as a structured fusion/repair loop;
- ranking has shell/primitive hard gates but still lacks recognizability and
  repair-plan gates;
- final polished candidates are not yet all renderings of one repaired
  `PrimitiveScene`.

## Implementation Order

1. Done: add `EvidenceInventory` and record all user-audited layers with roles.
2. Done: add `BuildingShell` from masks + wall_plane + dominant structural lines.
3. Done: add `PlaneGraph` from M-LSD / DeepLSD / HAWP with edge-map support.
4. Done: attach facade elements to planes/rows and reject orphan details outside the shell.
5. Move completion to shell/plane/feature graph, not raw line fragments.
6. Add Gemini fusion editor over graph components, chunked through the shared
   limiter.
7. Add neural compositor proposals as repair/style evidence.
8. Render candidate families from the same fused graph.
9. Add shell/coarse-to-fine hard gates before Gemini postcard ranking.
10. Re-run the sample in Kaggle and compare all artifacts against
    `user_audit_1.md`.

## Required Debug Artifacts

Future runs should include:

- `debug/evidence_inventory.json`;
- `debug/evidence_contact_sheet.png`;
- `debug/building_shell.json`;
- `debug/building_shell_overlay.png`;
- `debug/plane_graph.json`;
- `debug/plane_graph_overlay.png`;
- `debug/feature_graph.json`;
- `debug/fusion_editor_request.json`;
- `debug/fusion_editor_actions.json`;
- `debug/neural_repair_contact_sheet.png`;
- `debug/fusion_acceptance_report.json`.

These artifacts should let a human answer quickly: did the pipeline understand
the whole building before it drew windows and details?

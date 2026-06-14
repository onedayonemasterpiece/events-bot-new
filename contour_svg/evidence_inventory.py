from __future__ import annotations

import json
from pathlib import Path

from .contracts import EvidenceInventory, EvidenceItem, FacadeElement, GuideSet, LinePrimitive, MaskBundle, SemanticPlan
from .dependencies import require_module


def build_evidence_inventory(
    *,
    image,
    masks: MaskBundle,
    guides: GuideSet,
    facade_elements: list[FacadeElement],
    semantic_plan: SemanticPlan,
    out_dir: str | Path,
    neural_rasters: list[tuple[str, Path]] | None = None,
) -> EvidenceInventory:
    np = require_module("numpy", "numpy")
    Image = require_module("PIL.Image", "Pillow")

    debug = Path(out_dir) / "debug"
    debug.mkdir(parents=True, exist_ok=True)

    object_visible = np.array(Image.open(masks.object_visible).convert("L")) > 32
    occluder = np.array(Image.open(masks.occluder).convert("L")) > 32 if masks.occluder else np.zeros_like(object_visible)
    background = np.array(Image.open(masks.background).convert("L")) > 32

    items: list[EvidenceItem] = []
    items.extend(_mask_items(masks, object_visible, occluder, background))
    items.extend(_facade_items(facade_elements))
    items.extend(_line_items(guides.lines, object_visible, occluder, background))
    items.extend(_guide_image_items(guides))
    items.extend(_semantic_items(semantic_plan))
    items.extend(_neural_items(neural_rasters or []))

    contact_sheet = _write_contact_sheet(
        image=image,
        masks=masks,
        guides=guides,
        facade_elements=facade_elements,
        neural_rasters=neural_rasters or [],
        out_path=debug / "evidence_contact_sheet.png",
    )
    inventory = EvidenceInventory(items=items, contact_sheet=contact_sheet)
    (debug / "evidence_inventory.json").write_text(
        json.dumps(inventory.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return inventory


def _mask_items(masks: MaskBundle, object_visible, occluder, background) -> list[EvidenceItem]:
    Image = require_module("PIL.Image", "Pillow")
    return [
        EvidenceItem(
            id="E_mask_object_visible",
            kind="mask_region",
            source="sam2_primary_minus_occluders",
            role_hint="shell",
            semantic_hint="object_visible",
            bbox_xyxy=_mask_bbox(Image.open(masks.object_visible).convert("L")),
            confidence=0.82,
            object_visible_overlap=1.0,
            debug_image=masks.object_visible,
            metadata={"area_ratio": _area_ratio(object_visible)},
        ),
        EvidenceItem(
            id="E_mask_background",
            kind="mask_region",
            source="multi_state_masks",
            role_hint="negative_space",
            semantic_hint="background",
            bbox_xyxy=_mask_bbox(Image.open(masks.background).convert("L")),
            confidence=0.72,
            background_overlap=1.0,
            debug_image=masks.background,
            metadata={"area_ratio": _area_ratio(background)},
        ),
        EvidenceItem(
            id="E_mask_object_unknown",
            kind="mask_region",
            source="sam2_primary_dilated_occluder_intersection",
            role_hint="completion_zone",
            semantic_hint="object_unknown",
            bbox_xyxy=_mask_bbox(Image.open(masks.object_unknown).convert("L")),
            confidence=0.60,
            debug_image=masks.object_unknown,
        ),
        EvidenceItem(
            id="E_mask_occluder",
            kind="occluder_region",
            source="open_vocab_detector_sam2",
            role_hint="occlusion",
            semantic_hint="tree_foliage_fence_or_foreground",
            bbox_xyxy=_mask_bbox(Image.open(masks.occluder).convert("L")) if masks.occluder else None,
            confidence=0.72,
            occluder_overlap=1.0,
            debug_image=masks.occluder,
            metadata={"area_ratio": _area_ratio(occluder)},
        ),
    ]


def _facade_items(elements: list[FacadeElement]) -> list[EvidenceItem]:
    items: list[EvidenceItem] = []
    for idx, element in enumerate(elements):
        role = "plane" if element.element_type == "wall_plane" else "feature_anchor"
        kind = "wall_plane" if element.element_type == "wall_plane" else "facade_element"
        items.append(
            EvidenceItem(
                id=f"E_facade_{idx:04d}",
                kind=kind,
                source=element.source,
                role_hint=role,
                semantic_hint=element.element_type,
                bbox_xyxy=element.bbox_xyxy,
                confidence=element.confidence,
                debug_image=element.mask_path,
                metadata={"element_id": element.id, "evidence": element.evidence},
            )
        )
    return items


def _line_items(lines: list[LinePrimitive], object_visible, occluder, background) -> list[EvidenceItem]:
    items: list[EvidenceItem] = []
    for idx, line in enumerate(lines):
        points = _sample_points(line)
        role = _role_hint(line)
        items.append(
            EvidenceItem(
                id=f"E_line_{idx:05d}",
                kind="line_segment" if len(line.points) <= 2 else "polyline",
                source=line.source,
                role_hint=role,
                semantic_hint=line.role,
                bbox_xyxy=_line_bbox(line),
                geometry=line,
                confidence=line.confidence,
                object_visible_overlap=_mask_ratio(points, object_visible),
                occluder_overlap=_mask_ratio(points, occluder),
                background_overlap=_mask_ratio(points, background),
                metadata={"length": line.length, "priority": line.priority},
            )
        )
    return items


def _guide_image_items(guides: GuideSet) -> list[EvidenceItem]:
    items: list[EvidenceItem] = []
    for item_id, role, path in [
        ("E_guide_edge_map", "detail_confirmation", guides.edge_map),
        ("E_guide_mlsd", "structure", guides.mlsd_guide),
        ("E_guide_line_overlay", "structure_overlay", guides.line_overlay),
    ]:
        if path:
            items.append(
                EvidenceItem(
                    id=item_id,
                    kind="guide_image",
                    source=Path(path).name,
                    role_hint=role,
                    semantic_hint=item_id.removeprefix("E_guide_"),
                    confidence=0.70,
                    debug_image=path,
                )
            )
    return items


def _semantic_items(semantic_plan: SemanticPlan) -> list[EvidenceItem]:
    return [
        EvidenceItem(
            id="E_semantic_plan",
            kind="semantic_plan",
            source=semantic_plan.source,
            role_hint="semantic_prior",
            semantic_hint=str(semantic_plan.primary_object.get("label") or semantic_plan.primary_object.get("name") or "primary_object"),
            confidence=0.75,
            metadata=semantic_plan.to_dict(),
        )
    ]


def _neural_items(neural_rasters: list[tuple[str, Path]]) -> list[EvidenceItem]:
    return [
        EvidenceItem(
            id=f"E_neural_{idx:03d}",
            kind="neural_line",
            source=label,
            role_hint="proposal",
            semantic_hint="line_art_repair_or_style",
            confidence=0.45,
            debug_image=path,
            metadata={"proposal_only": True},
        )
        for idx, (label, path) in enumerate(neural_rasters)
    ]


def _write_contact_sheet(
    *,
    image,
    masks: MaskBundle,
    guides: GuideSet,
    facade_elements: list[FacadeElement],
    neural_rasters: list[tuple[str, Path]],
    out_path: Path,
) -> Path:
    Image = require_module("PIL.Image", "Pillow")
    ImageDraw = require_module("PIL.ImageDraw", "Pillow")

    entries: list[tuple[str, object]] = [
        ("source", image),
        ("multi_state_masks", masks.overlay),
        ("object_visible", masks.object_visible),
        ("occluder", masks.occluder),
        ("edge_map", guides.edge_map),
        ("mlsd_guide", guides.mlsd_guide),
        ("line_overlay", guides.line_overlay),
    ]
    wall_plane = next((element.mask_path for element in facade_elements if element.element_type == "wall_plane" and element.mask_path), None)
    if wall_plane:
        entries.append(("wall_plane", wall_plane))
    elements_overlay = out_path.parent / "elements_overlay.png"
    if elements_overlay.exists():
        entries.append(("elements_overlay", elements_overlay))
    for label, path in neural_rasters[:4]:
        entries.append((label, path))

    thumbs: list[tuple[str, object]] = []
    for label, ref in entries:
        if ref is None:
            continue
        try:
            img = ref.copy() if hasattr(ref, "copy") else Image.open(ref).convert("RGB")
        except Exception:
            continue
        img.thumbnail((260, 170))
        canvas = Image.new("RGB", (280, 205), (20, 20, 20))
        canvas.paste(img.convert("RGB"), ((280 - img.width) // 2, 8))
        draw = ImageDraw.Draw(canvas)
        draw.text((10, 184), label[:38], fill=(235, 235, 235))
        thumbs.append((label, canvas))
    if not thumbs:
        raise RuntimeError("EvidenceInventory requires at least one visual evidence thumbnail")
    cols = 3
    rows = (len(thumbs) + cols - 1) // cols
    sheet = Image.new("RGB", (cols * 280, rows * 205), (10, 10, 10))
    for idx, (_label, thumb) in enumerate(thumbs):
        sheet.paste(thumb, ((idx % cols) * 280, (idx // cols) * 205))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(out_path)
    return out_path


def _mask_bbox(mask) -> tuple[float, float, float, float] | None:
    bbox = mask.getbbox()
    if bbox is None:
        return None
    x1, y1, x2, y2 = bbox
    return float(x1), float(y1), float(x2), float(y2)


def _area_ratio(mask) -> float:
    return float(mask.sum()) / float(max(1, mask.size))


def _line_bbox(line: LinePrimitive) -> tuple[float, float, float, float]:
    xs = [x for x, _ in line.points]
    ys = [y for _, y in line.points]
    return min(xs), min(ys), max(xs), max(ys)


def _sample_points(line: LinePrimitive) -> list[tuple[int, int]]:
    points: list[tuple[int, int]] = []
    for a, b in zip(line.points, line.points[1:]):
        ax, ay = a
        bx, by = b
        steps = max(2, int((((bx - ax) ** 2 + (by - ay) ** 2) ** 0.5) / 8.0))
        for idx in range(steps + 1):
            t = idx / steps
            points.append((int(round(ax + (bx - ax) * t)), int(round(ay + (by - ay) * t))))
    return points


def _mask_ratio(points: list[tuple[int, int]], mask) -> float:
    if not points:
        return 0.0
    h, w = mask.shape[:2]
    inside = 0
    for x, y in points:
        if 0 <= x < w and 0 <= y < h and bool(mask[y, x]):
            inside += 1
    return round(inside / len(points), 4)


def _role_hint(line: LinePrimitive) -> str:
    if line.role == "silhouette":
        return "shell"
    if line.role == "roofline":
        return "roof"
    if line.role == "structure":
        return "structure"
    if line.role == "arc":
        return "feature_anchor"
    return "detail_confirmation"

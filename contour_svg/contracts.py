from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class BoundingBox:
    xyxy: tuple[float, float, float, float]
    label: str = "object"
    score: float = 1.0
    source: str = "groundingdino"

    def clipped(self, width: int, height: int) -> "BoundingBox":
        x1, y1, x2, y2 = self.xyxy
        return BoundingBox(
            (
                max(0.0, min(float(width), x1)),
                max(0.0, min(float(height), y1)),
                max(0.0, min(float(width), x2)),
                max(0.0, min(float(height), y2)),
            ),
            label=self.label,
            score=self.score,
            source=self.source,
        )


@dataclass
class SemanticPlan:
    primary_object: dict[str, Any]
    style_relevant_features: list[dict[str, Any]] = field(default_factory=list)
    occluders: list[dict[str, Any]] = field(default_factory=list)
    should_ignore: list[str] = field(default_factory=list)
    completion_policy: dict[str, Any] = field(default_factory=dict)
    notes_for_generation: str = ""
    source: str = "gemini"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MaskArtifact:
    path: Path
    bbox: BoundingBox | None
    source: str
    warnings: list[str] = field(default_factory=list)


@dataclass
class MaskBundle:
    object_visible: Path
    occluder: Path | None
    background: Path
    object_unknown: Path
    allowed_line_region: Path
    overlay: Path
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_visible": str(self.object_visible),
            "occluder": str(self.occluder) if self.occluder else None,
            "background": str(self.background),
            "object_unknown": str(self.object_unknown),
            "allowed_line_region": str(self.allowed_line_region),
            "overlay": str(self.overlay),
            "warnings": self.warnings,
        }


@dataclass
class LinePrimitive:
    points: list[tuple[float, float]]
    role: str = "line"
    priority: int = 5
    source: str = "guide"
    confidence: float = 1.0

    @property
    def length(self) -> float:
        total = 0.0
        for a, b in zip(self.points, self.points[1:]):
            total += ((b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2) ** 0.5
        return total


@dataclass
class LineCandidate:
    id: str
    geometry: LinePrimitive
    angle_deg: float
    source: list[str]
    vp_group: str
    object_visible_overlap: float
    occluder_overlap: float
    background_overlap: float
    object_unknown_overlap: float
    junction_support: float
    semantic_guess: str
    raw_score: float
    keep_probability: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "geometry": {
                "type": "polyline",
                "points": self.geometry.points,
            },
            "length": self.geometry.length,
            "angle_deg": self.angle_deg,
            "source": self.source,
            "vp_group": self.vp_group,
            "object_visible_overlap": self.object_visible_overlap,
            "occluder_overlap": self.occluder_overlap,
            "background_overlap": self.background_overlap,
            "object_unknown_overlap": self.object_unknown_overlap,
            "junction_support": self.junction_support,
            "semantic_guess": self.semantic_guess,
            "raw_score": self.raw_score,
            "keep_probability": self.keep_probability,
        }


@dataclass
class LineGroup:
    id: str
    members: list[str]
    merged_geometry: LinePrimitive
    semantic_label: str
    importance: str
    decision: str = "candidate"
    reason: str = ""
    confidence: float = 0.0
    object_visible_overlap: float = 0.0
    occluder_overlap: float = 0.0
    background_overlap: float = 0.0
    duplicate_group: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "members": self.members,
            "merged_geometry": {
                "type": "polyline",
                "points": self.merged_geometry.points,
            },
            "semantic_label": self.semantic_label,
            "importance": self.importance,
            "decision": self.decision,
            "reason": self.reason,
            "confidence": self.confidence,
            "object_visible_overlap": self.object_visible_overlap,
            "occluder_overlap": self.occluder_overlap,
            "background_overlap": self.background_overlap,
            "duplicate_group": self.duplicate_group,
        }


@dataclass
class FacadeElement:
    id: str
    element_type: str
    bbox_xyxy: tuple[float, float, float, float]
    confidence: float
    source: str
    evidence: list[str] = field(default_factory=list)
    mask_path: Path | None = None
    completion_status: str = "visible"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "element_type": self.element_type,
            "bbox_xyxy": list(self.bbox_xyxy),
            "confidence": self.confidence,
            "source": self.source,
            "evidence": self.evidence,
            "mask_path": str(self.mask_path) if self.mask_path else None,
            "completion_status": self.completion_status,
        }


@dataclass
class CompletionProposal:
    id: str
    completion_type: str
    geometry: LinePrimitive
    confidence: float
    source_group_ids: list[str] = field(default_factory=list)
    source_element_ids: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    accepted: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "completion_type": self.completion_type,
            "geometry": {
                "type": "polyline",
                "points": self.geometry.points,
            },
            "confidence": self.confidence,
            "source_group_ids": self.source_group_ids,
            "source_element_ids": self.source_element_ids,
            "evidence": self.evidence,
            "accepted": self.accepted,
        }


@dataclass
class ArchPrimitive:
    id: str
    primitive_type: str
    geometry: LinePrimitive
    importance: str
    source_group_ids: list[str]
    confidence: float
    render_role: str
    evidence: list[str] = field(default_factory=list)
    completion_status: str = "visible_only"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "primitive_type": self.primitive_type,
            "geometry": {
                "type": "polyline",
                "points": self.geometry.points,
            },
            "importance": self.importance,
            "source_group_ids": self.source_group_ids,
            "confidence": self.confidence,
            "render_role": self.render_role,
            "evidence": self.evidence,
            "completion_status": self.completion_status,
        }


@dataclass
class CandidateSVG:
    candidate_id: str
    family: str
    primitive_ids: list[str]
    final_eligible: bool
    svg_path: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "family": self.family,
            "primitive_ids": self.primitive_ids,
            "final_eligible": self.final_eligible,
            "svg_path": str(self.svg_path) if self.svg_path else None,
        }


@dataclass
class GuideSet:
    primary_mask: Path
    occluder_mask: Path | None
    edge_map: Path | None
    mlsd_guide: Path | None
    line_overlay: Path | None
    lines: list[LinePrimitive] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class Candidate:
    candidate_id: str
    variant: str
    family: str
    lines: list[LinePrimitive] = field(default_factory=list)
    raster_path: Path | None = None
    final_eligible: bool = False
    primitive_rendered: bool = False
    proposal_only: bool = False
    candidate_svg: CandidateSVG | None = None
    svg_path: Path | None = None
    preview_path: Path | None = None
    meta_path: Path | None = None
    cv_score: float = 0.0
    gemini_score: float | None = None
    accepted: bool = True
    failure_flags: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    parameters: dict[str, Any] = field(default_factory=dict)

    def to_meta(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "variant": self.variant,
            "family": self.family,
            "path_count": len(self.lines),
            "raster_path": str(self.raster_path) if self.raster_path else None,
            "final_eligible": self.final_eligible,
            "primitive_rendered": self.primitive_rendered,
            "proposal_only": self.proposal_only,
            "candidate_svg": self.candidate_svg.to_dict() if self.candidate_svg else None,
            "svg_path": str(self.svg_path) if self.svg_path else None,
            "preview_path": str(self.preview_path) if self.preview_path else None,
            "cv_score": self.cv_score,
            "gemini_score": self.gemini_score,
            "accepted": self.accepted,
            "failure_flags": self.failure_flags,
            "warnings": self.warnings,
            "parameters": self.parameters,
        }

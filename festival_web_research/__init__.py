"""Provider-free core for approval-gated festival web research."""
from .contracts import (
    CheckpointKind,
    CheckpointRecord,
    Claim,
    Decision,
    FestivalClassification,
    EntityRole,
    ItemDisposition,
    PrimaryTopology,
    ProgrammeItem,
    ProgrammeStructure,
    SemanticEventGate,
    ResearchSubject,
    SourceRole,
    SourceSnapshot,
)

__all__ = [
    "CheckpointKind", "CheckpointRecord", "Claim", "Decision", "EntityRole",
    "FestivalClassification", "ItemDisposition", "PrimaryTopology", "ProgrammeItem",
    "ProgrammeStructure", "ResearchSubject", "SemanticEventGate", "SourceRole", "SourceSnapshot",
]

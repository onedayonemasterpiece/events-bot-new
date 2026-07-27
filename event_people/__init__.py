"""Event participant registry, semantic decision validation and persistence."""

from .service import (
    EVENT_PEOPLE_DECISION_SCHEMA,
    ensure_kgd80_registry,
    grounded_people_decisions,
    sync_event_people,
)

__all__ = [
    "EVENT_PEOPLE_DECISION_SCHEMA",
    "ensure_kgd80_registry",
    "grounded_people_decisions",
    "sync_event_people",
]

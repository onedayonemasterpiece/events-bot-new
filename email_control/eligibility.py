from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Iterable


@dataclass(frozen=True)
class RecommendationIssue:
    event_ids: tuple[int, ...]
    hero_event_id: int | None
    page_published: bool
    page_validated: bool

    def validate_sendable(self) -> None:
        if len(self.event_ids) != 3 or len(set(self.event_ids)) != 3:
            raise ValueError("recommendation email requires exactly three distinct events")
        if any(event_id <= 0 for event_id in self.event_ids):
            raise ValueError("event ids must be positive")
        if self.hero_event_id is not None and self.hero_event_id not in self.event_ids:
            raise ValueError("hero must be one of the three email events")
        if not self.page_published or not self.page_validated:
            raise ValueError("personal page must be published and validated before enqueue")


@dataclass(frozen=True)
class SendEligibility:
    verified_identity: bool
    purpose_consent: bool
    active_admission: bool
    suppressed: bool
    active_recommendation_count: int
    capacity: int = 200

    def allows_recommendation(self, issue: RecommendationIssue) -> bool:
        issue.validate_sendable()
        return (
            self.verified_identity
            and self.purpose_consent
            and self.active_admission
            and not self.suppressed
            and 0 <= self.active_recommendation_count <= self.capacity == 200
        )


class RecommendationAdmissionGate:
    """Thread-safe executable mirror of the SQL admission invariant.

    Supabase remains the production authority. This small model exists so the cap and
    idempotency behavior can be tested without touching the drifted live database.
    """

    def __init__(self, capacity: int = 200):
        if capacity != 200:
            raise ValueError("launch recommendation capacity is fixed at 200")
        self.capacity = capacity
        self._active: set[str] = set()
        self._lock = Lock()

    def activate(self, user_id: str) -> bool:
        if not user_id:
            raise ValueError("user_id is required")
        with self._lock:
            if user_id in self._active:
                return True
            if len(self._active) >= self.capacity:
                return False
            self._active.add(user_id)
            return True

    def revoke(self, user_id: str) -> None:
        with self._lock:
            self._active.discard(user_id)

    def active_users(self) -> frozenset[str]:
        with self._lock:
            return frozenset(self._active)

    def activate_many(self, user_ids: Iterable[str]) -> list[bool]:
        return [self.activate(user_id) for user_id in user_ids]

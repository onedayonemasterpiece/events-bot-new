from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ContourStatus:
    client: Any | None = None
    progress: dict[str, Any] = field(default_factory=dict)

    def event(
        self,
        event: str,
        *,
        phase: str,
        status: str = "running",
        progress: dict[str, Any] | None = None,
        message: str | None = None,
    ) -> None:
        payload = dict(progress or {})
        payload.setdefault("phase", phase)
        self.progress.update(payload)
        self.progress["phase"] = phase
        self.progress.setdefault("progress_label", phase)
        if self.client is not None:
            self.client.event(event, phase=phase, status=status, progress=payload, message=message)

    def stage(
        self,
        phase: str,
        *,
        step_index: int,
        step_total: int,
        label: str,
        status: str = "running",
        event: str | None = None,
        **extra: Any,
    ) -> None:
        progress_percent = int(round((step_index / max(1, step_total)) * 100))
        progress = {
            "step_index": step_index,
            "step_total": step_total,
            "progress_percent": progress_percent,
            "progress_label": label,
            **extra,
        }
        self.event(event or f"{phase}_{status}", phase=phase, status=status, progress=progress)

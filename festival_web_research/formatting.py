"""Secret-free operator summaries."""
from __future__ import annotations

from .coordinator import ResearchResult


def format_research_result(result: ResearchResult) -> str:
    festival = result.candidate.get("festival") or {}
    classification = result.candidate.get("classification") or {}
    lines = [
        f"Festival research run #{result.run_id}: {result.state}",
        f"Candidate: {festival.get('name') or 'unknown'}",
        f"Topology: {classification.get('primary_topology') or 'unknown'} / {classification.get('programme_structure') or 'unknown'}",
        f"Independent agreement: {bool(result.quality.get('independent_agreement'))}",
        f"Conflicts: {int(result.quality.get('conflict_count') or 0)}",
        "Public apply: disabled (operator review only)",
    ]
    for lane in result.lanes:
        lines.append(
            f"Lane {lane.lane}: provider={lane.provider_status}, semantic={lane.semantic_status}, key_env={lane.key_env or '-'}"
        )
    return "\n".join(lines)

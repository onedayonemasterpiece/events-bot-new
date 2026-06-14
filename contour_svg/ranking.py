from __future__ import annotations

from .contracts import Candidate


def rank_candidates(candidates: list[Candidate]) -> list[Candidate]:
    def key(candidate: Candidate) -> tuple[int, int, float, float, str]:
        accepted = 1 if candidate.accepted else 0
        final_eligible = 1 if candidate.final_eligible else 0
        gemini = candidate.gemini_score if candidate.gemini_score is not None else candidate.cv_score
        structure = float(candidate.parameters.get("global_structure_score") or 0.0)
        diagonal_noise = float(candidate.parameters.get("diagonal_noise_ratio") or 0.0)
        path_count = len([line for line in candidate.lines if line.length > 0])
        simplicity = max(0.0, 10.0 - abs(path_count - 92) / 16.0)
        family_bonus = {
            "POSTCARD_MINIMAL": 0.35,
            "CONSERVATIVE_COMPLETION": 0.25,
            "FEATURE_EMPHASIS_OPENINGS": 0.18,
            "BALANCED_ARCHITECTURAL": 0.12,
        }.get(candidate.family, 0.0)
        blended = (
            0.44 * float(gemini)
            + 0.22 * float(candidate.cv_score)
            + 0.14 * structure
            + 0.15 * simplicity
            - 2.0 * diagonal_noise
            + family_bonus
        )
        return (accepted, final_eligible, blended, structure, candidate.candidate_id)

    return sorted(candidates, key=key, reverse=True)

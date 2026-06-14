from contour_svg.contracts import Candidate
from contour_svg.ranking import rank_candidates


def test_rank_candidates_prefers_accepted_and_scores():
    bad_high = Candidate("bad", "B1", "P4", cv_score=9.5, accepted=False)
    ok_low = Candidate("ok_low", "B1", "P1", cv_score=4.0, accepted=True)
    ok_high = Candidate("ok_high", "B1", "CONTROLNET", cv_score=7.0, gemini_score=8.0, accepted=True)

    ranked = rank_candidates([bad_high, ok_low, ok_high])

    assert [c.candidate_id for c in ranked] == ["ok_high", "ok_low", "bad"]


def test_rank_candidates_prefers_final_eligible_over_proposals():
    proposal = Candidate("proposal", "B1", "CONTROLNET_LINEART", cv_score=9.8, gemini_score=9.8, accepted=True)
    proposal.proposal_only = True
    final = Candidate(
        "final",
        "B2",
        "PRIMITIVE_ARCHITECTURAL_BALANCED",
        cv_score=5.0,
        gemini_score=5.0,
        accepted=True,
        final_eligible=True,
        primitive_rendered=True,
    )

    ranked = rank_candidates([proposal, final])

    assert [c.candidate_id for c in ranked] == ["final", "proposal"]

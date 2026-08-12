from types import SimpleNamespace

from smart_update_identity import stable_candidate_identity


def candidate(**kwargs):
    values = dict(
        source_url="https://vk.com/wall-1_2?from=feed",
        occurrence_key=None,
        candidate_key=None,
        producer_ordinal=None,
        ticket_link=None,
        date="2026-09-10",
        end_date=None,
        time="19:00",
        source_message_id=2,
    )
    values.update(kwargs)
    return SimpleNamespace(**values)


def test_explicit_occurrence_key_is_stable_across_edited_payload():
    left = candidate(occurrence_key="occurrence:abc")
    right = candidate(occurrence_key="occurrence:abc", date="2026-09-11", time="20:00")
    assert stable_candidate_identity(left) == stable_candidate_identity(right)


def test_producer_ordinal_distinguishes_multi_event_carrier():
    first = stable_candidate_identity(candidate(producer_ordinal=0))
    second = stable_candidate_identity(candidate(producer_ordinal=1))
    assert first[0] != second[0]
    assert first[1] != second[1]


def test_structured_occurrence_is_not_derived_from_free_text():
    base = stable_candidate_identity(candidate())
    edited = candidate()
    edited.source_text = "completely edited prose"
    assert stable_candidate_identity(edited) == base

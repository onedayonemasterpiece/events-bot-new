import main
import smart_event_update as su


def test_event_parse_defender_flags_generic_digest_shell_without_logistics():
    reasons = main._event_parse_defender_check(
        [
            {
                "title": "Дайджест, мы давно его ждали",
                "date": "2026-06-04",
                "time": "",
                "location_name": "",
            }
        ]
    )

    assert reasons == ["events[0].generic_digest_shell:Дайджест, мы давно его ждали"]


def test_event_parse_defender_allows_named_digest_event_with_logistics():
    reasons = main._event_parse_defender_check(
        [
            {
                "title": "Дайджест летних концертов",
                "date": "2026-06-04",
                "time": "19:00",
                "location_name": "Дом китобоя",
            }
        ]
    )

    assert reasons == []


def test_smart_update_detects_generic_digest_shell_candidate():
    candidate = su.EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-1_1",
        source_text="Мы давно его ждали",
        title="Дайджест, мы давно его ждали",
        date="2026-06-04",
        time="",
        location_name="Не указано",
        city="Калининград",
    )

    assert su._looks_like_generic_digest_shell_candidate(candidate) is True


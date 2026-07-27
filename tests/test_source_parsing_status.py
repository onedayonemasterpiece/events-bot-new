from source_parsing.handlers import (
    SourceParsingResult,
    SourceParsingStats,
    _source_parsing_terminal_status,
)


def test_source_loss_cannot_finish_green():
    no_survivors = SourceParsingResult(errors=["Philharmonia kernel failed"])
    assert _source_parsing_terminal_status(no_survivors) == "error"

    partial = SourceParsingResult(
        stats_by_source={
            "dramteatr": SourceParsingStats(
                source="dramteatr",
                total_received=4,
                already_exists=4,
            )
        },
        errors=["Qtickets kernel failed"],
    )
    assert _source_parsing_terminal_status(partial) == "partial"


def test_failed_items_are_partial_but_clean_sources_are_success():
    failed_item = SourceParsingResult(
        stats_by_source={
            "philharmonia": SourceParsingStats(
                source="philharmonia",
                total_received=2,
                new_added=1,
                failed=1,
            )
        }
    )
    assert _source_parsing_terminal_status(failed_item) == "partial"

    clean = SourceParsingResult(
        stats_by_source={
            "philharmonia": SourceParsingStats(
                source="philharmonia",
                total_received=2,
                new_added=2,
            )
        }
    )
    assert _source_parsing_terminal_status(clean) == "success"

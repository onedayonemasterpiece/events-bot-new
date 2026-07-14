from llm_source_grounding import claim_is_grounded, source_contains_quote


ECODVOR_TEASER = (
    "8 августа Летний Экодвор вернётся в Железнодорожные ворота. "
    "Мы уже намечаем программу с новыми лекциями, мастер-классами и другими активностями."
)


def test_rejects_synthetic_ecodvor_purpose_from_thin_teaser() -> None:
    verdict = claim_is_grounded(
        "Цель: продвижение экологических инициатив, обмен опытом и активный досуг.",
        ECODVOR_TEASER,
        min_ratio=0.38,
    )

    assert verdict.ok is False


def test_accepts_fact_supported_by_ecodvor_teaser() -> None:
    verdict = claim_is_grounded(
        "В программе намечены новые лекции, мастер-классы и другие активности.",
        ECODVOR_TEASER,
        min_ratio=0.45,
    )

    assert verdict.ok is True


def test_quote_must_be_contiguous_source_text() -> None:
    assert source_contains_quote(ECODVOR_TEASER, "новыми лекциями, мастер-классами")
    verdict = claim_is_grounded(
        "На Экодворе будут лекции и мастер-классы.",
        ECODVOR_TEASER,
        evidence_quote="организаторы обещают большую экологическую программу",
    )
    assert verdict.ok is False
    assert verdict.reason == "quote_not_in_source"


def test_rejects_unsupported_tire_detail_and_number() -> None:
    source = "На Экодворе собирают чистые соусники и материалы для повторного использования."
    verdict = claim_is_grounded(
        "Можно сдать до 4 шин на переработку.",
        source,
        evidence_quote="материалы для повторного использования",
    )

    assert verdict.ok is False

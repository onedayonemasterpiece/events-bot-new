@manual
Feature: Guide excursions monitoring
  Manual live scenario index for guide-only monitoring and digest publication.

  Scenario: Full scan keeps unpublished light findings eligible for digest
    Given the operator runs `/guide_excursions` in live mode
    When a light scan materializes a future digest-ready occurrence
    And that occurrence is not yet published in `new_occurrences`
    And the next full scan completes successfully
    Then the occurrence remains eligible for the scheduled or manual `new_occurrences` digest

  Scenario: Compact guide digest can be published as a single album caption
    Given the selected digest has materialized media assets
    And the whole rendered digest fits into one safe Telegram caption
    When the digest is published
    Then the target channel receives one media album without a separate text follow-up post

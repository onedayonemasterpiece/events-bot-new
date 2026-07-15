# Data/product lane results

## Lane identity

- Requirements: R01, R02, R03, R06
- Branch: `agent/typed-briefing-artist-unusual-20260715/data-product`
- Base: `a9b829d6a865bcf08bc267aa5360103298e461fc`
- Implementation commit: `ad768d56`
- Original XLSX was read from `/home/dev/projects/events-bot-new/artifacts/kaliningrad_artist_registry_batch_001.xlsx` and was not committed.

## Requirement closure

| ID | Status | Result |
|---|---|---|
| R01 | Done | Added a stdlib-only reproducible XLSX converter and committed `docs/reference/data/artist_registry_batch_001.canonical.json`, with source SHA/profile/safety contract and all 1,235 entities. |
| R02 | Done (design foundation) | Documented identity candidate matching, row-level locality enrichment, five-state locality model and fail-closed rules. Snapshot explicitly assigns every seed entity `locality.status=unknown`; membership and absence never prove non-locality. |
| R03 | Done (backlog contract) | Specified the automatic 14-day rolling visiting-artist digest: eligibility, role/locality gates, grouping/dedupe, manifest, copy, cadence, observability, retractions and acceptance/rollout. No production pipeline is falsely claimed. |
| R06 | Done (design contract) | Added a two-stage LLM-first unusualness framework with semantic signatures, season/category baselines, evidence-linked adjudication, hard blockers, distinct-fact fallback, public copy validation and evaluation gates. |

## Workbook profile and material findings

- SHA-256: `c40b238910d677c935d4c19bafb2d2f3fc14294d83193d27a6047f15075843ac`
- Workbook date: 2026-07-15; registry rows: 1,235.
- Unique IDs: 1,235; unique match keys: 1,231; duplicate groups/rows: 4/8.
- Alias-enriched rows: 47.
- Actual row-level QIDs/activity confirmations/verification dates: 0/0/0.
- All rows point to one of six category/list bucket URLs, not a person-specific identity or locality source.
- `Audit_Checks` caches `COUNTA(Wikidata_QID)=1235` although the expected value is 0 and all cells are empty; the converter recomputes and records 0.
- Therefore the sheet is useful for candidate recall but cannot directly classify an artist as local/non-local or active.

## Files

- `scripts/convert_artist_registry_xlsx.py`
- `tests/test_artist_registry_converter.py`
- `docs/reference/artist-visit-registry.md`
- `docs/reference/data/artist_registry_batch_001.canonical.json`
- `docs/llm/unusual-event-detection.md`
- `docs/backlog/features/static-typed-briefing/artist-arrivals-and-unusual-events.md`
- `docs/backlog/features/static-typed-briefing/README.md`

## Validation

```text
python3 -m unittest tests.test_artist_registry_converter -v
4 tests, OK

python3 scripts/convert_artist_registry_xlsx.py <xlsx> <canonical-json> \
  --expected-sha256 c40b...75843ac --check
OK: canonical output is reproducible

python3 -m json.tool docs/reference/data/artist_registry_batch_001.canonical.json
OK
```

## Residual risks / next gates

1. No seed row has row-level QID, activity or locality evidence. On-demand enrichment must precede a public arrival claim.
2. Only 47/1,235 rows have aliases; identity recall for stage names/transliteration needs enrichment and labeled eval.
3. The digest and unusualness detector remain design/backlog. They require shadow runs, human labels and stated precision/grounding gates before automatic publication.
4. Historic examples such as a strawberry day, poppy bloom or lantern-lighter walk are explicitly treated as type illustrations, never as current event facts.

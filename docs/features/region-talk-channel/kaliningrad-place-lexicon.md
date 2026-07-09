# Kaliningrad place lexicon v1 — Region Talk Channel

Status: MVP-1.x recall/guardrail lexicon. Machine-readable file: [`kaliningrad-place-lexicon-v1.csv`](kaliningrad-place-lexicon-v1.csv).

## Purpose

The lexicon helps the Region Talk monitor recognize posts that are substantively about **Kaliningrad Oblast**, including small settlements and tourist/nature places such as Краснолесье, Виштынецкое озеро, Роминтенская пуща and Балтийская коса.

It is **not** a keyword-only final classifier. It is used for:

- recall of regional places that may appear without the word “Калининград”;
- lexicon-derived Telegram discovery/preflight query banks for cities,
  settlements, landmarks and POIs;
- scope evidence for the LLM-owned `kaliningrad_oblast_only_scope_gate`;
- explainability columns in XLSX;
- features for later semantic/vector and verifier stages.

Final candidate quality still depends on semantic enrichment, source/profile context and manual/verifier review.

## Strict scope rule

A post is accepted only if the main content is about Kaliningrad Oblast.

Allowed:

- one Kaliningrad Oblast place;
- several cities/settlements/nature places inside the oblast;
- regional routes fully inside the oblast.

Rejected:

- Kaliningrad + Карелия/Алтай/Сочи/Санкт-Петербург/etc. in the same destination list;
- Kaliningrad Oblast as one item in “10 places in Russia”;
- country/region comparison where Kaliningrad is only one positive mention.

External places in footer, hashtags, boilerplate, menus, ads or cross-links are not automatic rejection; external places in the main route/list/comparison are rejection.

## CSV schema

Columns:

`place_id`, `canonical_name`, `place_type`, `municipality`, `district_or_okrug`, `is_city`, `is_settlement`, `is_tourist_place`, `is_nature_place`, `is_historical_name`, `aliases`, `old_names`, `latin_aliases`, `common_misspellings`, `geo_scope`, `priority_tier`, `ambiguity_level`, `allowed_for_kaliningrad_scope`, `requires_context`, `reject_if_external_context`, `source_url`, `source_note`.

Aliases use semicolon separators.

## Provenance

Seed v1 combines:

- official municipal center anchors from the Government portal: `https://gov39.ru/vlast/muni/`;
- official/cultural tourism anchors from `https://visit-kaliningrad.ru/`;
- Curonian Spit official source: `https://www.park-kosa.ru/`;
- curated user-provided regional/tourist/historical aliases for MVP-1.x.

## Ambiguity policy

Ambiguous names like `39 регион`, `Балтийское море`, `Лесное`, `Морское`, `Северный`, `Высокое` are marked with `requires_context=true` or higher `ambiguity_level`. They may support recall but should not alone decide the final candidate.

Old German/Prussian names are accepted as Kaliningrad-scope aliases only when attached to the corresponding oblast place. They must not create external-region false positives by themselves.

## Discovery query generation

The CSV is the source of truth for Region Talk discovery keywords:

- global Telegram keyword search uses a bounded rotating subset of
  travel-intent phrases and safe `core`/`tourist`/`important` terms;
- global Telegram hashtag search uses a separate bounded rotating subset of
  safe hashtag variants derived from the same rows;
- source-local preflight search can use the broader lexicon bank, because it is
  scoped to a known channel and is only evidence for prioritization.

Rows with `requires_context=true` or high ambiguity are not standalone global
search anchors. They may support scoped/source-local search or downstream
semantic evidence when enough Kaliningrad context is present.

## Builder

Run:

```bash
python3 scripts/build_kaliningrad_place_lexicon.py
```

Outputs:

- `docs/features/region-talk-channel/kaliningrad-place-lexicon-v1.csv`;
- `artifacts/region-talk/place-lexicon-latest.csv`;
- `data/region-talk/place-lexicon-latest.json`.

Validation checks include duplicate ids, empty names, required core places, semicolon alias separators and ambiguity guardrails.

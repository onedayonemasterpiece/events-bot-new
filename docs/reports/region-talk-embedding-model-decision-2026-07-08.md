> Superseded: this report used compact `short_summary`/summary-like YDB rows and is not valid as the final model-quality decision. The full-text redo is recorded in `docs/reports/region-talk-fulltext-embedding-model-decision-2026-07-08.md`. Runtime-only evidence remains useful, but quality conclusions here are superseded.

# Region Talk embedding model decision — 2026-07-08

Status: CPU-only live-YDB research result. No Telegram discovery/session was used.

## Scope

Compared a 300-row text-bearing Region Talk YDB sample from existing rows only:

- `publication_candidate_item`
- `candidate_memory_item`
- `image_queue_item`
- `processed_post_item`
- `post_live_item`

Generated local artifacts:

```text
artifacts/codex/region-talk-embedding-model-decision/20260708T131750Z/comparison.json
artifacts/codex/region-talk-embedding-model-decision/20260708T131750Z/comparison.csv
artifacts/codex/region-talk-embedding-model-decision/20260708T131750Z/model_only_candidates.xlsx
artifacts/codex/region-talk-embedding-model-decision/20260708T131750Z/README.md
```

## CPU runtime evidence

| Model | Rows scored | CPU elapsed | Practicality |
|---|---:|---:|---|
| `intfloat/multilingual-e5-base` | 300 | 193.828s | practical |
| `BAAI/bge-m3` | 300 | 482.956s | practical |
| `google/embeddinggemma-300m` | 300 | 235.262s | practical |
| `Qwen/Qwen3-Embedding-0.6B` | projected from 12-row probe | ~13,776s/300 | CPU-not-practical |

## Decision

Keep `intfloat/multilingual-e5-base` + `BAAI/bge-m3` as the production baseline.

Keep `google/embeddinggemma-300m` as a shadow/larger-validation lane, not a production third lane yet. It has a promising raw recall signal but did not clear the strong/manual-quality gate on this sample.

Do not add `Qwen/Qwen3-Embedding-0.6B` to the CPU pipeline: projected runtime is far above the 60-minute/300-post maximum.

## Key metrics

Baseline E5+BGE accepted rows: 94/300.

EmbeddingGemma:

- present rows: 300;
- accepted rows: 79;
- agreement with baseline: 0.823333;
- model-only accepted: 19 (+20.213% over baseline accepted count);
- model-only high confidence: 5;
- model-only after safety filters: 5;
- model-only with existing image-ready/evidence: 0;
- accepted rows with anti-region signal: 9 (0.113924);
- accepted rows with high ad/promo signal: 0.

Qwen 0.6B:

- present rows: 16 from prior probe;
- model-only accepted: 1;
- projected CPU runtime for 300 rows: ~3.8 hours;
- decision: CPU-not-practical.

## Manual spot-check

The YDB proxy labels for `candidate_memory_item` are weak and overstate quality: many rows are merely remembered rows, not final KO travel/review candidates.

Potentially useful Gemma-only examples:

- `https://t.me/travel_yutturizm/35761` — Старый Тильзит / Советск; plausible KO place/story candidate missed by baseline.
- `https://t.me/kulturnaya_chaika/7937` — Kaliningrad author excursion; relevant, but event/excursion-like and needs product-policy review.

Weak/error examples:

- `https://t.me/phuket_seichas/956` — Phuket beach/weather, non-KO.
- `https://t.me/dv_traveler/9750` — external/Baikal-like geo signal, non-KO.
- `https://t.me/kenig01/27069` — road accident/news, not travel/review/useful-story.
- `https://t.me/northtourizm/5976` — Karelia tourism news, external-region false positive.

## Follow-up

1. Keep EmbeddingGemma writes research-only (`embeddinggemma_300m_enrichment_item`) and do not feed production fusion yet.
2. Run a larger balanced validation with explicit manual/LLM labels for Gemma-only and baseline-only disagreements.
3. Promote Gemma only if model-only valid KO travel/review posts clear the 30% gate and image-ready/final-candidate yield improves without increasing anti-region/ad false positives.

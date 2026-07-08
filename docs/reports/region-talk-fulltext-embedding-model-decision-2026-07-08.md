# Region Talk full-text embedding model decision — 2026-07-08

Status: completed full-text CPU-only validation on 500 existing YDB posts.

This report supersedes `region-talk-embedding-model-decision-2026-07-08.md`, which used compact `short_summary`/summary-like rows and is not valid for final model-quality choice.

## Decision

Keep **E5+BGE** as the production text baseline.

Do **not** add `google/embeddinggemma-300m` as a production third lane yet. Keep it only as a **shadow / larger-validation lane**: it produced a raw model-only accept gain, but manual review shows too few strong KO travel/review wins and too many external/irrelevant model-only accepts.

Do **not** add `Qwen/Qwen3-Embedding-0.6B` to the CPU pipeline: it started and scored a full-text CPU probe, but projected runtime is far above the practical budget.

## Full-text dataset

Source: existing Region Talk YDB `post_url` rows only, no new source/channel discovery.

Telegram fetch:

- local server Telethon session from `TELEGRAM_AUTH_BUNDLE_E2E`;
- direct `get_messages` for already-known `post_url` message ids;
- **no public `t.me/s` fallback** because public pages can be truncated;
- no new discovery, no joins, no publisher paths.

YDB kind: `fulltext_validation_item`.

Evidence:

- dataset `20260708T135940Z`: 415 valid full texts;
- dataset `20260708T141100Z`: 124 valid full texts;
- total valid full texts in YDB: 539;
- median text length: 664 chars;
- average text length: 842 chars;
- max text length: 4071 chars;
- fetch method counts in both batches: only `telethon_get_messages`.

## CPU model runs

| Model | Run | Rows | CPU elapsed | Status |
|---|---|---:|---:|---|
| E5 `intfloat/multilingual-e5-base` | `region-talk-e5-fulltext500-cpu-20260708T141746Z` | 500 | 547.167s | done |
| EmbeddingGemma `google/embeddinggemma-300m` | `region-talk-embeddinggemma-300m-fulltext500-reprocess-cpu-20260708T143300Z` | 500 | 681.151s | done |
| BGE `BAAI/bge-m3` | `region-talk-bge-m3-fulltext500-reprocess-cpu-20260708T150422Z` | 500 | 1897.131s | done |
| Qwen `Qwen/Qwen3-Embedding-0.6B` | `region-talk-qwen3-06b-fulltext64-cpu-20260708T141832Z` | 64 | 2459.223s | CPU-not-practical projection |

Qwen 0.6B projection from the 64-row full-text probe:

- ~11,528 seconds for 300 rows (~3.2 hours);
- ~19,213 seconds for 500 rows (~5.3 hours).

## Comparison artifacts

```text
artifacts/codex/region-talk-fulltext-embedding-model-decision/20260708T154134Z/comparison.json
artifacts/codex/region-talk-fulltext-embedding-model-decision/20260708T154134Z/comparison.csv
artifacts/codex/region-talk-fulltext-embedding-model-decision/20260708T154134Z/model_only_candidates.xlsx
artifacts/codex/region-talk-fulltext-embedding-model-decision/20260708T154134Z/manual_review_gemma_model_only.json
artifacts/codex/region-talk-fulltext-embedding-model-decision/20260708T154134Z/README.md
```

Comparison script was run with:

- `--base-kind fulltext_validation_item`;
- `--require-model-source-kind fulltext_validation_item`;
- E5 prefix `e5_fulltext_multilingual_base`.

This prevents mixing the invalid old summary-based rows into the final comparison.

## Metrics

Baseline E5+BGE accepted rows: **137/500**.

| Model | Rows present | Accepted | Agreement with baseline | Model-only accepted | Raw recall gain vs baseline | High-confidence model-only | Safety-filtered model-only | Image-ready model-only | Anti/external accepted rate | High ad/promo accepted |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| EmbeddingGemma 300M | 500 | 116 | 0.834 | 31 | +22.6% | 9 | 9 | 0 | 14/116 = 12.1% | 0 |
| Qwen3 0.6B | 64 | 17 | 0.732 | 7 | +5.1% partial-sample | 2 | 2 | 0 | 3/17 = 17.6% | 0 |

EmbeddingGemma model-only averages:

- travel/emotion/review score: 0.293358;
- useful-story score: 0.264626.

## Manual review of EmbeddingGemma-only accepted rows

Manual review scope: 31 rows accepted by EmbeddingGemma and not accepted by baseline E5+BGE.

Result:

| Class | Count | Rate |
|---|---:|---:|
| Strong KO travel/review | 2 | 6.5% |
| KO-relevant but not strong travel/review | 9 | 29.0% |
| Weak/ad/promo | 3 | 9.7% |
| External or irrelevant false positive | 17 | 54.8% |

Strong useful examples:

- `https://t.me/travel_yutturizm/33996` — Pravdinsk / Kaliningrad Oblast historical travel description; good Region Talk candidate.
- `https://t.me/devochka_izmarcipana/3949` — personal local padel review in Holmogorovka / Family Court; useful emotion/review signal, but has soft-promo risk.

KO-relevant but not strong travel/review examples:

- `https://t.me/northtourizm/5678` — complaint/news about visiting Planeta Okean in Kaliningrad; service/news, not a strong travel/review candidate.
- `https://t.me/kaliningradartmuseum/8080` — Kaliningrad museum weekly exhibitions/classes; event listing, not incremental travel/review recall.
- `https://t.me/rf_history/20641` — Baltic cultural forum / Koenigsberg naming discussion; local/cultural relevance, not travel/review.

Error / filter examples:

- `https://t.me/dv_traveler/9750` — Far East bay/place card; accepted as KO-like visual place card but external region.
- `https://t.me/good_expedition/3059` — Baikal/Olhon trip diary; good travel text but wrong region.
- `https://t.me/travelguiderussia/11059` — Lipetsk guide; wrong region.
- `https://t.me/uletet_1/13775` — Kaliningrad flight promo code; should be ad/promo filtered.

## Decision against original gates

Promotion gate required all of:

1. at least +10% new text-accepted candidates beyond union(E5+BGE);
2. at least 30% valid KO travel/review among model-only candidates;
3. anti-region/ad false positives not increased by more than 15%;
4. CPU runtime within practical budget;
5. a clear production role.

EmbeddingGemma:

- passes raw recall: 31 model-only accepts = +22.6% vs baseline accepted count;
- passes CPU runtime: 681s/500, within the 30-minute desired budget;
- fails manual-quality gate: only 2/31 = 6.5% strong KO travel/review model-only candidates;
- weakens safety profile: 17/31 model-only accepted rows were manual external/irrelevant false positives;
- role is not yet clean enough for production fusion because the model-only wins require extra filtering and are mostly not strong Region Talk posts.

Qwen 0.6B:

- starts successfully on CPU, so the previous “does not run” result is not repeated;
- is still CPU-not-practical: 64 full-text rows took 2459s, projecting to ~3.2h/300 rows;
- partial sample does not justify promotion: raw model-only signal is small (+5.1% partial-sample) and anti/external rate is higher than EmbeddingGemma.

## Final recommendation

Production: keep **E5+BGE**.

Research:

- keep EmbeddingGemma as a shadow lane only if we want a larger labeled validation set;
- do not wire EmbeddingGemma into production accept/reject fusion until model-only strong KO travel/review quality clears the 30% gate;
- drop Qwen 0.6B from CPU production consideration unless GPU/batching/runtime constraints change.

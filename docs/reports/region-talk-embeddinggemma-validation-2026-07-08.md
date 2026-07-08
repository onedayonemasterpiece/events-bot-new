# Region Talk EmbeddingGemma CPU validation — 2026-07-08

Status: research evidence only. No production fusion change was made.

## Scope

Validate the corrected Kaggle model link
`google/embeddinggemma/Transformers/embeddinggemma-300m/1` as a CPU-only
Region Talk text-vector research worker over live YDB rows, and compare the
result against existing BGE-M3 evidence.

Canonical code paths:

- worker: `kaggle/RegionTalkQwen3Embedding06BEnrichment/region_talk_qwen3_embedding_06b_enrichment.py`;
- launcher: `kaggle/execute_region_talk_qwen3_embedding_06b_enrichment.py`
  with `--model-size embeddinggemma`;
- comparison helper: `scripts/region_talk_embedding_quality_compare.py`.

The worker writes research-only rows under `embeddinggemma_300m_*` kinds and
does not feed production fusion.

## CPU runability evidence

| Run id | Batch | Result | Device | Elapsed | YDB writes | Artifact |
|---|---:|---|---|---:|---:|---|
| `region-talk-embeddinggemma-300m-cpu-probe-20260708T121443Z` | 12 | `error` | CPU | 140s before import failure | 0 | `artifacts/codex/kaggle/region-talk-qwen3-embedding-06b-enrichment/region-talk-embeddinggemma-300m-cpu-probe-20260708T121443Z-failed/` |
| `region-talk-embeddinggemma-300m-cpu-probe-20260708T121936Z` | 12 | `done` | CPU | 151.689s | 12 | `artifacts/codex/kaggle/region-talk-qwen3-embedding-06b-enrichment/region-talk-embeddinggemma-300m-cpu-probe-20260708T121936Z/` |
| `region-talk-embeddinggemma-300m-cpu-probe-20260708T122347Z` | 36 | `done` | CPU | 148.109s | 36 | `artifacts/codex/kaggle/region-talk-qwen3-embedding-06b-enrichment/region-talk-embeddinggemma-300m-cpu-probe-20260708T122347Z/` |

The first EmbeddingGemma attempt used the Google/Kaggle preview install
`git+https://github.com/huggingface/transformers@v4.56.0-Embedding-Gemma-preview`
and failed because `sentence_transformers` could not import
`PreTrainedModel` from that preview `transformers` package in the Kaggle
container. The successful CPU runs use stable packages:
`sentence-transformers>=5.1.0` and `transformers>=4.56.0`.

Observed successful CPU summary:

- model: `google/embeddinggemma-300m`;
- Kaggle model source: `google/embeddinggemma/Transformers/embeddinggemma-300m/1`;
- encoder contract: `embeddinggemma_300m_sentence_transformers_dense_768_v1`;
- max length: 1024;
- second successful run wrote 36 rows in 148.109 seconds on CPU, including
  semantic-bank and geo-bank scoring.

## BGE-vs-EmbeddingGemma comparison

Comparison command:

```bash
python scripts/region_talk_embedding_quality_compare.py \
  --limit 5000 \
  --qwen-model-short embeddinggemma_300m
```

Output artifact:

- `artifacts/codex/region-talk-embedding-quality/20260708T122909Z/comparison.json`
- `artifacts/codex/region-talk-embedding-quality/20260708T122909Z/comparison.csv`

Summary:

| Metric | Value |
|---|---:|
| Paired BGE+EmbeddingGemma rows | 45 |
| BGE rows scanned | 45 |
| EmbeddingGemma rows scanned | 45 |
| Labeled pairs | 45 |
| Positive pairs | 42 |
| Negative pairs | 3 |
| Gate agreement rate | 0.888889 |
| Top-class agreement rate | 0.311111 |
| Avg `EmbeddingGemma - BGE` quality-axis delta, all | -0.011034 |
| Avg `EmbeddingGemma - BGE` quality-axis delta, positives | -0.010820 |
| Avg `EmbeddingGemma - BGE` quality-axis delta, negatives | -0.014027 |

Interpretation:

- EmbeddingGemma is CPU-practical for the Region Talk offline vector worker:
  36 rows plus full prototype scoring completed in about 2.5 minutes on Kaggle
  CPU after dependency setup.
- On the current shared prototype axes it is close to, but slightly below, BGE
  by average score delta. It is not a clear BGE replacement yet.
- High gate agreement (`0.888889`) makes it a credible research candidate for a
  low-cost CPU recall lane or fallback lane, pending manual review of
  disagreement rows and a larger Gemini/image-confirmed validation set.

## Qwen 4B/8B CPU note

GPU smoke probes proved that Qwen3-Embedding 4B/8B can load when Kaggle GPU is
available, but GPU is not a useful production assumption because of the small
weekly Kaggle GPU quota.

CPU-only Qwen3-Embedding-4B run
`region-talk-qwen3-embedding-4b-cpu-probe-20260708T115611Z` loaded the model
on CPU (`model_load_done` at 74.3s) but was still stuck in the first full
prototype-bank encode after more than 12 minutes and was cancelled. The Kaggle
kernel and temporary input datasets were deleted. Since 8B is larger than 4B,
8B CPU is treated as not viable without a separate reduced-prototype experiment.

## Decision

Do not add Qwen 4B/8B to the CPU pipeline.

Keep BGE-M3 as the required production enrichment lane for now. Keep
EmbeddingGemma as a research-only CPU candidate: it is fast enough to test on
larger real-YDB batches and may become a third/fallback vector lane only if
manual disagreement review and Gemini/image-confirmed labels show better recall
or useful complementarity.

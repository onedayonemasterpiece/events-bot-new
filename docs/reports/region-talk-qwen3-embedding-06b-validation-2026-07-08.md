# Region Talk Qwen3-Embedding-0.6B validation — 2026-07-08

Status: research evidence only. No production fusion change was made.

## Scope

Validate whether `Qwen/Qwen3-Embedding-0.6B` can run as a separate no-Telegram
Kaggle worker on real live-YDB Region Talk text rows, and collect first-pass
BGE-vs-Qwen search-quality evidence.

Canonical code paths:

- worker: `kaggle/RegionTalkQwen3Embedding06BEnrichment/region_talk_qwen3_embedding_06b_enrichment.py`;
- launcher: `kaggle/execute_region_talk_qwen3_embedding_06b_enrichment.py`;
- comparison helper: `scripts/region_talk_embedding_quality_compare.py`.

## Runability evidence

| Run id | Batch | Result | Device | Elapsed | YDB writes | Artifact |
|---|---:|---|---|---:|---:|---|
| `region-talk-qwen3-embedding-06b-probe-20260708T094141Z` | 4 | `done` | CPU | 428.716s | 4 | `artifacts/codex/kaggle/region-talk-qwen3-embedding-06b-enrichment/region-talk-qwen3-embedding-06b-probe-20260708T094141Z/` |
| `region-talk-qwen3-embedding-06b-probe-20260708T095632Z` | 12 | `done` | CPU | 551.055s | 12 | `artifacts/codex/kaggle/region-talk-qwen3-embedding-06b-enrichment/region-talk-qwen3-embedding-06b-probe-20260708T095632Z/` |

The second run used the corrected Kaggle title/slug (`06B`) and completed
through the normal launcher, including output download and temporary input
dataset cleanup.

Post-run live YDB counts: `qwen3_embedding_0_6b_enrichment_item=16`,
`qwen3_embedding_0_6b_enrichment_result=3` (two run rows + `latest`),
`business_heartbeat_qwen3_embedding_0_6b_enrichment=3`.

Observed timing shape on Kaggle CPU:

- model load: about 68s;
- prototype bank encoding: about 5 minutes for 22 semantic + 323 geo prototypes;
- row encoding/scoring: variable, roughly 5-30s per row in the small batches;
- no Telegram auth bundle was packaged or used.

Logs showed pip dependency warnings after upgrading `sentence-transformers` /
`transformers`; they did not block this isolated worker, but this is another
reason not to run Qwen inside the main notebook.

## Initial BGE-vs-Qwen comparison

Comparison command:

```bash
python scripts/region_talk_embedding_quality_compare.py --limit 3000
```

Output artifact:

- `artifacts/codex/region-talk-embedding-quality/20260708T100734Z/comparison.json`
- `artifacts/codex/region-talk-embedding-quality/20260708T100734Z/comparison.csv`

Summary:

| Metric | Value |
|---|---:|
| Paired BGE+Qwen rows | 16 |
| BGE rows scanned | 45 |
| Qwen rows scanned | 16 |
| Labeled pairs | 16 |
| Positive pairs | 13 |
| Negative pairs | 3 |
| Gate agreement rate | 0.6875 |
| Top-class agreement rate | 0.125 |
| Avg `Qwen - BGE` quality-axis delta, all | -0.009602 |
| Avg `Qwen - BGE` quality-axis delta, positives | -0.015894 |
| Avg `Qwen - BGE` quality-axis delta, negatives | +0.017660 |

Interpretation:

- Qwen can run and write real YDB research rows.
- On the first 16 paired rows it does **not** beat BGE. BGE is stronger on the
  shared prototype quality axis for positive rows, while Qwen tends to score some
  rejected rows too positively.
- The labels are mixed-confidence live-YDB labels (`publication_candidate_item`,
  `candidate_memory_item`, `image_queue_item`). This is enough to avoid
  immediate production promotion, but not enough for a final model decision.

## Decision

Do not add Qwen to production fusion yet and do not replace BGE. Keep Qwen as a
research-only worker. If further investigation is desired, run a larger balanced
comparison set with more final Gemini/image-confirmed positives and rejected
controls, then re-evaluate precision/recall and disagreement rows manually.

## Anti-vector note

Semantic diversity / anti-vector ranking should be dual-model E5+BGE, not E5-only.
Qwen vectors should participate only if this research gate later promotes Qwen as
a production model slot.

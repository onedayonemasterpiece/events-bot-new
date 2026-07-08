# Region Talk BGE-M3 isolated validation — 2026-07-08

Purpose: validate the hypothesis that `BAAI/bge-m3` can run stably when isolated in a clean Kaggle notebook that does only text vectorization over live YDB rows.

## Implementation under test

- Notebook script: `kaggle/RegionTalkBgeM3Enrichment/region_talk_bge_m3_enrichment.py`
- Launcher: `kaggle/execute_region_talk_bge_m3_enrichment.py`
- Model: `BAAI/bge-m3`
- Encoder contract: `bge_m3_flagembedding_dense_v1`
- Backend: `FlagEmbedding` / `BGEM3FlagModel`
- Device: Kaggle CPU
- Telegram session: none (`REGION_TALK_AUTH_BUNDLE_ENV=REGION_TALK_NO_TELEGRAM_BUNDLE`)
- YDB writes:
  - `text_vector_enrichment_item`
  - `bge_m3_enrichment_result:<run_id>`
  - `bge_m3_enrichment_result:latest`
  - `business_heartbeat_bge_m3_enrichment`

## Infra notes before successful runs

- System Python in the worktree did not have the Kaggle SDK; the run was launched through the existing repository venv: `/home/dev/projects/events-bot-new/artifacts/region-talk-kaggle-venv2`.
- A first Kaggle attempt reached the notebook but failed before model load because Kaggle did not receive a YDB service-account secret and the YDB SDK fell back to metadata credentials. Successful runs passed a short-lived `YC_IAM_TOKEN` through the encrypted Kaggle input dataset.

## Successful live-YDB batches

| Run id | Batch limit | Batch size | Rows loaded | Rows scored | Rows written | Dense vectors stored | Elapsed seconds | Status |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `region-talk-bge-m3-probe-20260708T084246Z` | 8 | 2 | 8 | 8 | 8 | 8 | 187.092 | `done` |
| `region-talk-bge-m3-probe-20260708T084748Z` | 12 | 4 | 12 | 12 | 12 | 12 | 434.036 | `done` |
| `region-talk-bge-m3-probe-20260708T085641Z` | 24 | 4 | 24 | 24 | 24 | 24 | 141.925 | `done` |
| `region-talk-bge-m3-probe-20260708T090106Z` | 4 | 2 | 4 | 4 | 4 | 4 | 145.047 | `done` |

YDB verification after the final run:

```text
text_vector_enrichment_item: 48
bge_m3_enrichment_result: 5
business_heartbeat_bge_m3_enrichment: 5
latest result: run_id=region-talk-bge-m3-probe-20260708T090106Z, rows_loaded=4, rows_scored=4, rows_written=4, ydb_write_status=ok
```

Downloaded artifacts are under ignored local paths:

```text
artifacts/codex/kaggle/region-talk-bge-m3-enrichment/<run_id>/
```

## Conclusion

BGE-M3 is stable enough to keep as a separate vectorization worker for the Region Talk pipeline. The main CandidateReport should not load BGE-M3 together with E5; it should consume `text_vector_enrichment_item` rows and perform model-free fusion/scoring when both model lanes are present.

## Follow-up

1. Wire CandidateReport to consume BGE `text_vector_enrichment_item` rows for production fusion.
2. Add equivalent durable E5 `text_vector_enrichment_item` rows if true semantic anti-vector diversity must use both E5 and BGE vectors.
3. Add `publication_semantic_history_item` and MMR/cosine diversity over confirmed/sent/published history.
4. Replace temporary access-token launch with a stable Kaggle YDB service-account secret lane once provisioned.

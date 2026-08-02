# Region Talk stats snapshots

Status: implemented helper contract for operational reporting.

Every operator request for Region Talk funnel statistics must create a durable YDB snapshot first, then report the current values and deltas from the previous snapshot. Do not use screenshots, chat history or `latest_state` aggregate rows as the comparison baseline.

## Command

```bash
python3 scripts/region_talk_stats_snapshot.py
```

The command reads row-level YDB state from `region_talk_state_kv`, stores a new snapshot row back to the same YDB table and prints a Markdown table.

Snapshot rows:

- `kind = region_talk_stats_snapshot`
- `pk = region_talk_stats_snapshot:<snapshot_id>`
- `payload_json.schema_version = region-talk-stats-snapshot-v1`

The payload includes:

- `metrics` — stable metric keys and current values;
- `delta_from_previous` — current minus previous `region_talk_stats_snapshot` row;
- `metric_definitions` — business meaning, funnel stage, monotonic flag and how to read negative deltas;
- `source.excluded_sources` — explicitly excludes screenshots, chat history and stale `latest_state` aggregates.

## Source rows used

Only row-level YDB rows are used:

- `source_queue_item`
- `processed_post_item`
- `post_live_item`
- `candidate_memory_item`
- `image_queue_item`
- `publication_candidate_item`

`latest_state` may lag row-level writes and must not be used for funnel deltas.

## Reading negative deltas

Not every funnel line is a cumulative progress counter.

- Current-status metrics (`pending_scan`, `processed_terminal_total`, `needs_rescan_or_retry`, `processed_no_ko`, source status buckets) are the **current distribution of states**. They can go down when rows are reclassified, retried, deduplicated or moved to another status.
- Progress-like metrics (`processed_post_rows`, `source_posts_scanned_sum`, `candidate_memory_rows`, `image_queue_total`, `image_actual_scored`, `publication_queue_total`, `publication_gemini_accept`) should normally grow. If they go down, treat it as a cleanup/dedup/rebuild event and mention that explicitly.

The canonical explanation for each metric is embedded in every snapshot under `metric_definitions`.

## Operator response rule

In operator-facing answers, use business labels first and raw YDB statuses only in parentheses when needed. Always make the funnel object explicit:

- **source-level** = channel/public/source row;
- **post-level** = concrete post/publication candidate;
- **media-level** = image fetch/scoring state.

Do not answer with raw statuses such as `candidate`, `skipped_or_rejected`, `processed_no_ko` without explaining the object and funnel stage.

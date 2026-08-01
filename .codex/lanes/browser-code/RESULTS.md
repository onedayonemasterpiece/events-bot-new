# BROWSER-CODE lane results

- Lane ID: `BROWSER-CODE`
- Requirement IDs: `R05`, `R06` (JS-only article media materialization)
- Base SHA: `029b6d44`
- Branch: `agent/region-talk-editorial/browser-materializer`
- Production mutation/deploy: none

## Delivered

- Added `scripts/region_talk_article_browser_materialize.py`, a dry-run-first YDB worker which renders at most three due JS-only external article pages per invocation.
- Each page has a serializable row lease and a lease-checked final merge. The worker preserves concurrent CandidateReport fields and writes only its browser/media lifecycle fields.
- Chromium retains rendered JSON-LD/DOM image evidence: durable source URL, final page/referrer URL, article role, selector/path, alt, figure caption and rendered dimensions. Accepted refs re-enter the existing ImageDiagnostic download + selective-VLM association/ranking path.
- A rendered page with zero associated images is terminal for media acquisition. Transient failures retry after 6h and then 24h; the third attempt is terminal. Both terminal cases retain native link preview as the usable fallback.
- Canonical page, redirects and every browser subresource are limited to public HTTP(S); credential-bearing URLs and any DNS answer containing private/reserved/link-local addresses fail closed. ImageDiagnostic applies the same public-destination guard to article/image HTTP acquisition.
- The worker uses no Telegram session and has no Telegram dependency. Source media is carried with prominent source attribution/original link; legacy rights metadata is provenance, not an admission blocker.
- CandidateReport's concurrent merge now preserves browser-owned leases, attempt/retry state, rendered refs and evidence.
- The Fly image installs Playwright Chromium and only Chromium.

## Validation

```text
python3 -m py_compile \
  scripts/region_talk_article_browser_materialize.py \
  kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py \
  kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py

/home/dev/.codex/venvs/events-bot-new/bin/python -m pytest -q \
  tests/test_region_talk_article_browser_materialize.py \
  tests/test_region_talk_image_diagnostic.py \
  tests/test_region_talk_candidate_report.py \
  --disable-warnings --maxfail=5
# 373 passed in 36.36s

git diff --check
```

No live browser/network, Telegram, YDB write, Fly deploy or production mutation was performed.

## Exact orchestrator integration hook (intentionally not edited in this lane)

`writer` owns `scripts/region_talk_orchestrator.py`, so integration must add this hook serially:

1. In `collect_product_metrics`, count image rows into:
   - `image_browser_materialization_due_total`: `image_queue_status=needs_browser_materialization`, attempts `<3`, `next_attempt_after<=now`, and no unexpired browser lease;
   - `image_browser_materialization_retry_wait_total`;
   - `image_browser_materialization_terminal_total` (`terminal_no_associated_images|terminal_fetch_failed`);
   - `image_browser_materialized_total`.
2. In `build_actions`, immediately after `prefetch_vk_media` and before `launch_image_diagnostic`, append only when `image_browser_materialization_due_total>0`:

```python
_action(
    "materialize_article_browser",
    ["python3", "scripts/region_talk_article_browser_materialize.py", "--execute", "--limit", "3"],
    f"{metrics['image_browser_materialization_due_total']} JS-only article pages are due for bounded media materialization",
    resource="local:region-talk-chromium",
    parallel_safe=True,
    timeout_seconds=180,
)
```

3. Add the script to `_supports_arg` for both `--env-file` and `--run-id`, so it receives the same run identity and explicit environment file as other local workers.
4. Surface the four counters in the cycle/result JSON and operator stats. The existing action result already supplies `claimed`, `materialized`, `zero_associated_terminal`, `retry_wait`, `fetch_failed_terminal`, and `lease_lost`; retain those as per-run product metrics.
5. Do **not** count browser-wait rows in ImageDiagnostic `image_actionable_work_total`: the notebook deliberately skips them until direct refs exist. After browser success the same row becomes `needs_actual_image_fetch` and the ordinary ImageDiagnostic action handles it in a following/current cycle.

## Changed files

- `.env.example`
- `CHANGELOG.md`
- `Dockerfile`
- `docs/features/region-talk-channel/image-postcardness.md`
- `docs/features/region-talk-channel/ydb-schema.md`
- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`
- `kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py`
- `scripts/region_talk_article_browser_materialize.py`
- `tests/test_region_talk_article_browser_materialize.py`
- `tests/test_region_talk_candidate_report.py`
- `tests/test_region_talk_image_diagnostic.py`

## Integration notes

- The browser action can run in parallel with D1/D2 work because it holds neither Telegram session. It still shares Fly CPU/memory, so the hard three-page cap and the dedicated resource key must remain.
- This lane does not download/reupload assets to Telegram itself. It materializes durable source refs/evidence and delegates byte acquisition plus VLM ranking to the existing ImageDiagnostic contract.

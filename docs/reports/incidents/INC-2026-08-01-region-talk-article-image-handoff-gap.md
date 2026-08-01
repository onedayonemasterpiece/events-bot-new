# INC-2026-08-01 Region Talk article image handoff gap

Status: investigating
Severity: sev2
Service: Region Talk external-article discovery and operator drafts
Opened: 2026-08-01
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-region-talk-article-link-precedence`, `INC-2026-08-01-region-talk-draft-backfill-nameerror`
Related docs: `docs/features/region-talk-channel/README.md`, `docs/features/region-talk-channel/editorial-visual-product.md`

## Summary

Eligible imported editorial and academic articles entered CandidateReport with
an empty `media_and_rights.candidate_urls`. The projection correctly said
`has_media=false`, but the image-queue builder interpreted that as having no
possible image input and never created an `image_queue_item`. ImageDiagnostic,
which can discover associated images from the canonical article page itself,
therefore never saw those articles.

## User / Business Impact

- current Peasant Studies and RG article drafts remained
  `media_materialization_pending` despite having canonical article URLs;
- article discovery could grow text candidates without growing visually ready
  operator drafts;
- the existing terminal link-preview fallback could not be reached honestly,
  because no acquisition attempt existed to produce terminal evidence.

## Detection

Production YDB showed fused `good_text_weak_media` candidate-memory rows for the
affected articles but no same-URL `image_queue_item`. Code inspection confirmed
that `new_posts` required `has_media`, candidate-memory admission rejected
`no_media_for_image_analysis`, while ImageDiagnostic already supported bounded
HTTP/Playwright discovery from `post_url`.

## Timeline

- 2026-08-01 15:59 UTC — scheduled CandidateReport completed and retained the
  imported article candidates.
- 2026-08-01 16:00 UTC — article YDB audit found candidate memory but no image
  queue rows for Peasant Studies, RG and RUDN.
- 2026-08-01 16:10 UTC — the producer/consumer handoff mismatch was reproduced
  locally and a bounded routing fix entered regression testing.

## Root Cause

CandidateReport conflated two different facts: “a direct image URL is already
known” and “there is an actionable source from which images can be acquired.”
For social posts those usually coincide; for web articles the canonical page is
the acquisition source even when a researcher supplied no image URL.

## Contributing Factors

- `has_media` was intentionally truthful and therefore false for these imports;
- ImageDiagnostic page discovery was implemented downstream, but its producer
  contract did not describe an article page as a distinct acquisition target;
- coverage tested article galleries and direct URLs independently, not the full
  empty-`candidate_urls` queue handoff.

## Automation Contract

### Treat as regression guard when

- changing external-publication intake, CandidateReport image admission,
  ImageDiagnostic web acquisition or terminal article preview fallback.

### Affected surfaces

- `kaggle/RegionTalkCandidateReport/region_talk_candidate_report.py`;
- `kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py`;
- YDB `candidate_memory_item`, `image_queue_item` and
  `publication_candidate_item` rows;
- orchestrator CandidateReport → ImageDiagnostic → v8 draft backfill chain.

### Mandatory checks before closure or deploy

- prove an eligible external article with no direct image URL creates one
  selected `needs_actual_image_fetch` row;
- prove that row retains `has_media=false` and a canonical
  `external_article_page` acquisition target;
- prove the downstream web worker extracts an article-associated gallery with
  no direct-image input;
- prove the ordinary publication gate still rejects an untouched no-media
  article and social no-media controls;
- run focused and full Region Talk regression suites;
- deploy a clean exact `origin/main` SHA, execute a compensating discovery/image
  pass and inspect affected production YDB rows and operator drafts.

### Required evidence

- focused/full test output;
- deployed SHA reachable from `origin/main`, Fly version/image and health;
- production before/after rows for at least Peasant Studies and RG;
- resulting actual-image/browser-terminal state and regenerated operator draft
  or a precise external acquisition blocker.

## Immediate Mitigation

The drafts remain fail-closed and are not sent without media evidence. Existing
ready article/social candidates continue through the independent queue.

## Corrective Actions

- represent the canonical web page as `media_acquisition_target_type=external_article_page`;
- allow this target into the image queue without mutating truthful `has_media`;
- sign only pre-image acquisition eligibility for the worker; final publication
  eligibility still requires actual or explicit terminal fallback evidence.

## Follow-up Actions

- [ ] deploy and run the compensating article image acquisition pass;
- [ ] regenerate and deliver the affected v8 article drafts when their media
  evidence becomes ready.

## Release And Closure Evidence

- deployed SHA: pending
- deploy path: pending
- regression checks: focused checks in progress
- post-deploy verification: pending

## Prevention

The regression boundary now distinguishes known media from an actionable media
source and includes an end-to-end no-direct-image web-article control.

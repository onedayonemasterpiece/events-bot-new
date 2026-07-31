# Image postcardness scoring

Status: **album/editorial-gallery-safe guardrails implemented; calibrated scorer remains under evaluation**. Goal: make MVP visibly useful by showing selected photos + model image report + why they work as a Region Talk illustration, without silently discarding a good album because its first frame, its OG preview or one unavailable model scored poorly.

> The 2026-07-14 live audit found at least four operator-confirmed false
> rejects among 18 exact image-only rejects. The old runtime scored one anchor
> frame and treated an uncalibrated arithmetic score as a terminal quality
> verdict. Do not lower the threshold as an isolated fix. Canonical evidence:
> [image-scoring false-negative review](image-scoring-false-negative-review.md).
> The accepted external methodology and its stop/go criteria are recorded in
> [image-scoring audit methodology v2](../../reference/image-scoring-audit-methodology-v2.md).

## Deployed album-safe transition contract

The current production-safe transition contract is
`region_talk_image_editorial_gallery_guard_v3`; its publication attestation is
`region_talk_publication_eligibility_v5`.

### Editorial gallery and visual-genre correction (v3)

The Archi.ru article “Вся мудрость океана” exposed two distinct false-negative
causes rather than weak media: the first pass scored only the supplied OG image,
while the CLIP positive bank was dominated by scenic travel/postcard concepts.
Professional museum interiors and architectural frames can therefore be
technically and aesthetically strong while receiving a low scenic score.

The v3 transition fixes the methodology without adding a domain-specific
Archi.ru exception:

- an external editorial/academic page may expose a bounded intentional image
  gallery through `data-fancybox`/`data-lightbox` anchors; the worker scores up
  to the ordinary 20-frame cap and ignores unrelated navigation/recommendation
  thumbnails;
- the research-provided OG/direct URL remains a fallback and may fill a free
  gallery slot, but it is no longer assumed to represent the whole article;
- structured content metadata selects a diagnostic visual track:
  `scenic_travel`, `editorial_publication` or
  `architecture_interior_editorial`;
- the architecture/interior prompt bank measures professional composition,
  light, space, materials, detail and editorial usefulness. A compatibility
  `clip_postcardness_score` is still written for existing consumers, but v3
  explicitly tags it as genre visual fit rather than a calibrated probability;
- the selective VLM prompt judges a strong standalone **editorial teaser
  illustration**, not only an outdoor tourist postcard, and sees the complete
  bounded gallery;
- low raw CV/CLIP/LAION/NIMA output still cannot become a terminal reject or a
  new uncalibrated auto-accept. It routes to the selective VLM/human review.

An operator may persist `approve_visual` only against a complete, decoded,
safe gallery manifest. The attestation is bound to
`input_media_manifest_hash`; any changed gallery invalidates it. This resolves
visual suitability only: source, geography, text/vector, rights and final
Gemini gates remain mandatory. Media rights stay `score_only_no_reuse` unless
separately cleared.

### Live editorial-gallery canary, 2026-07-20

The real Archi.ru candidate `https://archi.ru/russia/101203/vsya-mudrost-okeana`
was rerun under v3. The generic lightbox extractor acquired and decoded the
full bounded sample **20/20**, not only the old OG preview. The structured
`architecture_criticism` metadata selected
`architecture_interior_editorial`; the anchor/best frame produced overall
`0.720`, genre visual fit `0.987`, aesthetic `0.680` and technical `0.705`.
The unchanged legacy positive path therefore accepted the complete gallery
even before applying the owner's manifest-bound visual approval. The approval
was then persisted, the normal final Gemini verifier accepted the candidate,
and the anti-vector queue ranked it first. Telegram operator evidence:
candidate message `32424`, queue snapshot `32425`.

- Telegram media with the same exact `grouped_id` and all VK photo
  attachments are acquired as one bounded post/album manifest. The default
  cap is 20 images, which covers a complete Telegram album and the practical
  VK attachment bound used by this worker. A canary may explicitly lower
  `REGION_TALK_IMAGE_MAX_IMAGES_PER_POST`; the image launcher propagates that
  value into its private runtime config instead of silently reverting to the
  default.
- Every fetched image receives its own compact `image_frame_score_item` with
  content hash and model/version evidence. Kaggle-local file paths are not
  persisted.
- `expected_image_count`, `fetched_image_count`,
  `image_acquisition_status`, `input_media_manifest_hash` and
  `selected_media_ids` make partial acquisition explicit.
- A partial album, exhausted acquisition, missing required model component or
  low legacy score is **non-terminal**: it becomes `needs_visual_review` or
  `scoring_retry`, never `AUTO_REJECT_ALL_WEAK` and never a permanent
  tombstone.
- The previous positive path is preserved only when the complete album's
  anchor also passes the unchanged legacy contract. Later-frame best score is
  recorded as shadow evidence; it is not used as an uncalibrated raw-max
  shortcut.
- Old low-score single-anchor rows are eligible for an idempotent versioned
  rescore. The four operator-locked positive cases are regression fixtures and
  cannot become terminal quality rejects.
- The v4→v5 migration authorization survives the temporary image lease status.
  A row that was accepted by v4 and then blocked only by that circular status/
  gate-version transition is recoverable; a current source, compliance or text
  reject is never reopened by this exception.
- A missing or stale publication attestation is refresh work for
  CandidateReport, not evidence that the post or media is bad. ImageDiagnostic
  writes `image_eligibility_status=deferred_refresh` and preserves an existing
  `actual_scored` result, frame count and score. Only a current semantic,
  source or compliance rejection becomes terminal. The orchestrator continues
  to count accepted v2/v3/v4 low-score rows as versioned rescore work even if
  an earlier migration attempt temporarily wrote the terminal image status.
- Publication eligibility is tri-state at the image consumer as well as at the
  producer. `needs_source_review`, `needs_text_review` and
  `needs_visual_review` are soft deferrals
  (`image_eligibility_status=deferred_soft_gate`), not aliases for `reject`.
  Rows with an existing album score remain `actual_scored`; unsupported media
  retains its own terminal media evidence; rows with no image evidence become
  `deferred_text_gate`. A hard current local/spam/compliance/text rejection may
  close `image_queue_status`, but cannot zero frame counts, scores, acquisition
  state or model output.
- Source-level exclusion based on average raw image score is disabled. Raw
  source image statistics remain diagnostics only; exact local/spam/legal
  exclusions are unaffected.

This transition intentionally does **not** claim that the legacy CV/CLIP/
LAION/NIMA score is calibrated. Full scoring of acquired frames is currently
used as a bounded canary/diagnostic baseline so that frame coverage and cost
can be measured. A long-term automatic `AUTO_ACCEPT`/
`AUTO_REJECT_ALL_WEAK` contract still requires the labelled, source-disjoint
calibration and shadow acceptance gates in the external methodology. Until
then, uncertain cases abstain into review instead of being rejected.

### Live album canary, 2026-07-14

The fixed CPU canary acquired two complete Telegram albums: 10/10 frames for
`routecommunity/1342` and 7/7 for `hotostay/14641`. Thus the album acquisition
and lease-recheck defects are closed: **2 post rows and 17 distinct frames**
were fetched and scored. Reports and heartbeats now state post and frame counts
separately; the old “actual images” label had in fact counted post rows.

The canary also exposed the next real blocker: the runtime could not obtain the
pyiqa NIMA weights, so both rows correctly became non-terminal
`scoring_retry` instead of false accepts/rejects. CLIP came from the pinned
Kaggle input and LAION loaded successfully. Root cause was not a three-hour
NIMA inference: the CLIP loader set process-wide `HF_HUB_OFFLINE=1`; the
Hugging Face client captured that flag and then refused the later pyiqa/NIMA
weight lookup. CLIP now relies only on its own `local_files_only=True` guard
and does not mutate the network policy of independent components. This is not
proof of a calibrated production image decision; deterministic packaging of
every required model or a versioned VLM review lane remains necessary before
automatic acceptance.

A follow-up one-post CPU canary after that isolation fix acquired and scored
all **10/10** frames of `routecommunity/1342`.  NIMA loaded successfully in
about 41 seconds, proving that the model outage was fixed rather than hidden.
The post was deliberately written as non-terminal `needs_visual_review`: its
legacy anchor score was `0.518`, while the strongest shadow frame was `0.693`,
which is exactly the album/scorer disagreement that must not be converted into
either an uncalibrated auto-accept or a terminal reject.  The Kaggle API and
downloaded report show this run completed in roughly five minutes.  If the web
UI continues to display it as running for hours without logs, treat that as a
stale UI surface and verify the API status, YDB business heartbeat and output
report before cancelling or diagnosing a frozen model.

CandidateReport and ImageDiagnostic may run concurrently on their separate
session roles.  Before CandidateReport writes an existing image row it must
re-read the current YDB image ledger and merge only its source/text/vector
eligibility fields.  Album acquisition, frame counts, model outputs and the
versioned image decision belong to ImageDiagnostic and cannot be replaced by
CandidateReport's older start-of-run snapshot.  A hard source/text/compliance
rejection may still close the queue status while retaining that audit evidence.

The 2026-07-15 live cycle exposed a stricter ownership boundary. An early
CandidateReport handoff converted `needs_source_review`, missing BGE fusion and
an ambiguous vector status into `rejected_text_gate` for 21 rows. The scalar
image evidence survived, but three real `actual_scored` rows disappeared from
the active funnel. These states are now tri-state: only authoritative
local/spam/compliance/official evidence or a current hard post/vector reject is
terminal; missing positive evidence is deferred and cannot enter image/Gemini
spend. A durable actual-image result remains `actual_scored` while the
source/text verdict is refreshed, and a previously hidden result is restored
from its diagnostic evidence. Source joins use the case-insensitive canonical
source identity, so Telegram handle casing cannot create a false unknown
source. The final handoff skips an early row only when CandidateReport's own
source/text/vector projection is unchanged; a richer final hard verdict for
the same row must still be written, while ImageDiagnostic-owned frame/model
fields remain protected by the latest-row merge.

A subsequent one-item ImageDiagnostic poll exposed the same missing tri-state
boundary in the image consumer itself: it rewrote 92 ledger rows as
`rejected_publication_eligibility`, including soft `needs_*` decisions, and
zeroed diagnostic frame counts. The consumer now separates hard rejection,
producer-contract refresh and soft review. Queue-poll heartbeats report hard
blocked, gate-version refresh and soft-deferred counts separately, so a large
review cohort can no longer be hidden inside a misleading terminal-reject
counter. This repair changes state ownership only; it does not lower image
thresholds or turn `needs_visual_review` into automatic acceptance.

## Principle

Do not run expensive VLM on every image. Use a cascade:

```text
cheap CV gates
  → lightweight aesthetic/technical model
  → CLIP/SigLIP zero-shot prompts
  → VLM verifier for top media only
```

## Stage 1 — cheap CV gates

Reject or downrank early:

- resolution too low;
- unsuitable aspect ratio unless crop-safe;
- blur/sharpness failure;
- poor brightness/contrast;
- tiny file size or compression artifacts;
- perceptual hash duplicate / near duplicate;
- screenshot/document scan/meme/banner/ad detection;
- excessive text overlay;
- large faces/person-risk when reuse is unsafe;
- incident/news visual signals.

## Stage 2 — lightweight aesthetic model

Kaggle-runnable options:

- LAION aesthetic predictor;
- NIMA-like aesthetic/technical score;
- other documented lightweight image scorer.

Store `technical_quality_score`, `aesthetic_score`, `low_noise_score` and model/version in `model_report_json`.

## Stage 3 — CLIP/SigLIP zero-shot

Positive prompts:

- “beautiful postcard travel photo”
- “scenic Baltic sea travel photo”
- “beautiful old European city architecture”
- “Curonian Spit dunes and forest”
- “Kaliningrad travel postcard photo”
- “atmospheric seaside resort town”

Negative prompts:

- “screenshot”
- “meme”
- “advertising banner”
- “news incident photo”
- “low quality blurry photo”
- “document scan”
- “crowded political event”
- “accident scene”

## Stage 4 — VLM verifier for top media only

Preferred verifier: Gemini Flash-Lite / current configured Gemini lite/flash-lite VLM lane. Alternatives if Kaggle GPU/runtime permits: Gemma multimodal, Qwen2.5-VL-7B-Instruct, or another explicitly documented local VLM.

Output JSON contract:

```json
{
  "is_postcard_like": true,
  "postcardness_score": 0.0,
  "technical_quality_score": 0.0,
  "aesthetic_score": 0.0,
  "region_visual_relevance_score": 0.0,
  "publication_safety_score": 0.0,
  "contains_large_people_faces": false,
  "contains_text_overlay": false,
  "contains_watermark": false,
  "contains_news_or_incident_visuals": false,
  "recognizable_region_elements": ["sea", "dunes", "old architecture"],
  "short_explanation": "...",
  "rejection_reason": null
}
```

## MVP thresholds

Main candidate thresholds:

- `technical_quality_score >= 0.65`
- `aesthetic_score >= 0.70`
- `postcardness_score >= 0.72`
- `publication_safety_score >= 0.95`
- `low_noise_score >= 0.80`
- `overall_media_score >= 0.75`

For the main queue/report section:

- at least 1 strong image is required;
- 2–5 strong images are preferred for VK carousel;
- if only 1 image passes, Telegram single-photo is possible later, VK may use one photo + branded quote/summary cards;
- weak media may appear only in debug section.


## MVP-1 visible proof requirements

MVP-1 must visibly prove image scoring usefulness in `04_review_queue`, `05_favorites` and `09_image_quality`. For each candidate/favorite show:

- selected image or thumbnail/local artifact reference;
- `postcardness_score`;
- `aesthetic_score`;
- `region_visual_relevance_score`;
- `publication_safety_score`;
- model explanation;
- why the image passed or failed.

Main favorites require strong images. Good text + weak image goes only to `10_good_text_weak_media`.

## Explicit scoring modes

The first test runner may implement only one mode, but docs/config must name all modes:

- `cv_only` — cheap resolution/blur/brightness/duplicate/safety gates only.
- `cv_aesthetic` — cheap CV plus lightweight aesthetic/technical scoring.
- `cv_aesthetic_clip` — cheap CV + aesthetic + CLIP/SigLIP zero-shot prompts.
- `cv_aesthetic_clip_vlm` — full cascade with VLM verifier for top media only.
Image acquisition failures remain retryable across separate notebook runs, but
an item with `last_image_diag_run_id` equal to the current run is not leased
again in that run. This prevents a persistent Telegram/VK authorization error
from becoming a same-run hot retry loop while preserving later recovery.

## Runtime observability

Long image stages write the component and business-row context into the YDB
heartbeat, not only a generic event name. Model load events retain
`model`/`model_id`/`device`/timing or error; inference events retain the current
post URL, image queue id, component scores and per-row timings. This makes a
slow CLIP, LAION or NIMA load distinguishable from media acquisition and from
an individual corrupt image while the Kaggle kernel is still running.

## Reproducible CLIP loading on Kaggle

The image worker must not download the 600 MB CLIP encoder from Hugging Face
during every CPU run. The kernel attaches the pinned Kaggle Model
`yujkaggle/openaiclip-vit-base-patch32/PyTorch/default/1`, resolves the complete
local Transformers directory under `/kaggle/input`, and calls both
`CLIPProcessor.from_pretrained(...)` and `CLIPModel.from_pretrained(...)` with
`local_files_only=True`. Kaggle product runs fail the CLIP component quickly if
that model input is absent instead of waiting indefinitely on a Hub request.
`REGION_TALK_CLIP_MODEL_LOCAL_PATH` remains available for an explicit local
test path; `REGION_TALK_CLIP_REQUIRE_LOCAL_MODEL=0` is a local-development
escape hatch and is not the production default.

Internet remains enabled because the worker still has to acquire public post
media. It is not the model-distribution channel. Model heartbeats include
`model_origin` and `model_reference`, so live acceptance can prove that the
pinned input, not a network fallback, was used.

# Media locator lifecycle

`https://t.me/<handle>/<message_id>#media` is a **post-level marker**, not a
direct image URL. ImageDiagnostic must ignore it as an HTTP image locator and
retrieve the message media through its role-scoped `DISCOVERY2` Telethon
session. Direct HTTP media is accepted only when the response advertises an
image (or binary octet-stream); an HTML response falls through to Telegram
media retrieval instead of becoming a terminal decode failure.

The image queue is downstream of the complete text decision, so it stores
hashes, eligibility evidence and media state but not another durable copy of
the full post text. Exact text remains only in active candidate/vector/Gemini
work and is removed after the final verdict.

The legacy score path still computes `overall_media_score >= 0.66` by default
and retains its narrow near-threshold lane (`overall >= 0.63`, `postcardness >=
0.85`, `aesthetic >= 0.52`, `technical >= 0.68`). These numbers are now
explicitly labelled **legacy diagnostics**, not calibrated probabilities.
They may preserve an already-established positive path for a completely
acquired album, but they may not create a terminal quality reject.

The current contract is `region_talk_publication_eligibility_v5`. It maps
partial acquisition, missing component evidence and low/uncertain legacy
scores to non-terminal visual review/retry. It does not use the highest raw
frame score as a new accept rule, does not lower `0.66`, and does not invent
new weights. Automatic all-weak rejection remains disabled until a labelled
source-disjoint holdout validates the post-level selective decision described
in the external methodology.

Final Gemini verifier prompt v5 treats a short recurring author footer with
links to excursions, useful services or the author's other profiles as neutral
unless the commercial offer/price/discount/promo/booking CTA dominates the
post body. This preserves the ban on advertising without rejecting an
otherwise independent travel story solely because of its standard footer.

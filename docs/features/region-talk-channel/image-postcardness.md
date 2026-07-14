# Image postcardness scoring

Status: **implemented single-anchor scorer under calibration review**. Goal: make MVP visibly useful by showing selected photos + model image report + why they are “открыточные”.

> The 2026-07-14 live audit found at least four operator-confirmed false
> rejects among 18 exact image-only rejects. The runtime currently scores one
> anchor frame, not the complete Telegram/VK album, and its arithmetic
> multi-model score is not calibrated. Do not lower the threshold as an
> isolated fix. Canonical evidence and the external-consultant brief:
> [image-scoring false-negative review](image-scoring-false-negative-review.md).

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

The current (not yet golden-set-calibrated) publication media gate uses
`overall_media_score >= 0.66` by default. A
narrow near-threshold lane prevents a weighted-score edge case from discarding
an exceptional postcard before Gemini: `overall >= 0.63`, `postcardness >=
0.85`, `aesthetic >= 0.52` and `technical >= 0.68` must all hold. This does not
auto-accept the post; it only permits the final Gemini review. The calibrated
contract is `region_talk_publication_eligibility_v4`. The narrow high-postcard
lane applies a `0.001` tolerance only to its three-decimal aesthetic boundary,
so a score reported as `0.519` is not discarded against `0.520`; `0.518` still
fails. The overall, postcardness and technical floors are unchanged.

These thresholds describe the deployed v4 behavior; the word `calibrated`
does not mean that a labelled source-disjoint holdout has validated them.
Until the review protocol is complete, album/high-disagreement low scores
should be considered candidates for a non-terminal review lane, not evidence
that a lower global number is safe.

Final Gemini verifier prompt v5 treats a short recurring author footer with
links to excursions, useful services or the author's other profiles as neutral
unless the commercial offer/price/discount/promo/booking CTA dominates the
post body. This preserves the ban on advertising without rejecting an
otherwise independent travel story solely because of its standard footer.

# Image postcardness scoring

Status: design. Goal: make MVP visibly useful by showing selected photos + model image report + why they are “открыточные”.

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

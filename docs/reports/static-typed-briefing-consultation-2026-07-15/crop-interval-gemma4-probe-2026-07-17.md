# Gemma 4 crop-interval probe — 2026-07-17

> Status: **supplementary production-model probe**, not an external consultant
> review and not a production metadata migration.

## Question

Can the inexpensive Gemma image lane choose a meaningful crop with a very small
prompt, without asking it to narrate reasoning or directly author CSS?

Models were called without fallbacks through the existing Google AI key lane:

- `models/gemma-4-31b-it`;
- `models/gemma-4-26b-a4b-it`.

There is no “Gemini 31B” model id in this repository. The relevant model is
**Gemma 4 31B**. Gemini 3.1 Pro remains a separate agy consultant surface.

## Failed direct-focus prompt

A tiny request for a `3.9:1` CSS focal point was not reliable. On event `6611`
31B rejected the cover while 26B returned `focusY=35`, which still risked a head
cut. On event `6565` both returned `focusY=75`, prioritizing the dancers but
losing the elevated halo. A model must not directly own `object-position`.

An exact-geometry variant for event `6611` did better: given source
`2560×1541` and crop `2560×656`, 31B returned `top=65px` in `4.397s`; 26B
returned `67px` in `1.228s`. The same approach still overclaimed feasibility on
the more complex `6565` scene, so geometry validation remains mandatory.

## Minimal accepted prompt

```text
Image is {W}x{H}. Classify it as portrait or scene. Return its crop-critical
vertical interval: for portrait, all faces and complete heads; for scene, all
principal subjects. Original-image pixels. JSON only.
```

Schema:

```json
{
  "kind": "portrait|scene",
  "top_px": 0,
  "bottom_px": 1
}
```

Runtime config: image part first, temperature `0`, native JSON schema,
`thinking_level=MINIMAL`, `include_thoughts=false`, maximum output `72`.

| Source | Gemma 4 31B | Latency | Gemma 4 26B A4B | Latency |
|---|---:|---:|---:|---:|
| `6611`, 2560×1541, two people | `portrait [64,548]` | 3.125s | `portrait [68,475]` | 9.174s |
| `6565`, 1920×1080, stage | `scene [197,875]` | 1.717s | `scene [578,870]` | 19.013s |

31B retained the complex scene span. 26B omitted the halo and showed erratic
latency; it may only be a secondary proposal behind deterministic rejection or
consensus, not the sole crop author.

## Accepted boundary

The model authors a versioned **semantic interval**, not a crop. A deterministic
solver then:

1. derives actual source crop dimensions for each target aspect;
2. expands head intervals with a top/bottom safety margin;
3. fits the interval and derives `focusY`, or rejects cover if it cannot fit;
4. selects a different source, source-faithful portrait cluster, multi-image
   composition, contain, or text-only fallback;
5. persists model id, prompt version, media hash, interval and manual override.

The reproducible CLI is
[`scripts/inspect/probe_briefing_crop_interval.py`](../../../scripts/inspect/probe_briefing_crop_interval.py).
Its pure solver tests cover accepted event `6611` geometry and rejection of the
non-fitting `6565` interval.

Official transport/model references used after the local SDK blocker:
[Google Gen AI Python SDK](https://googleapis.github.io/python-genai/),
[Gemma 4 model card](https://ai.google.dev/gemma/docs/core/model_card_4),
[Gemma on the Gemini API](https://ai.google.dev/gemma/docs/core/gemma_on_gemini_api).

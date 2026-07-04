# Event hero date block design research — 2026-07-04

Canonical note for the static-site event hero date/time/weekday component after the Pinterest + external-model design pass.

## Source funnel

The research used the local Pinterest idea-library workflow and visually reviewed two saved boards:

- `/home/dev/projects/pinterest-idea-library/collections/20260704-kenigevents-strong-date-block-ui-batch1-20260704` — 60 pins, board + thumbnails.
- `/home/dev/projects/pinterest-idea-library/collections/20260704-kenigevents-strong-date-block-ui-batch2-20260704` — 70 pins, board + thumbnails.

Total funnel: **130 collected candidates across 10 successful query lanes**. The first one-shot collector run was intentionally discarded after a browser close; the durable source of truth is the two batch `pins.json` files above. Pinterest references are inspiration metadata only: copy layout mechanics, not the concrete artwork.

## Shortlist and votes

The self-review produced 14 reusable patterns. Gemini Pro and a-opus were asked to vote only after the in-repo visual review existed.

| ID | Pattern | Self | Gemini Pro | a-opus | Decision |
|---|---|---:|---:|---:|---|
| P13 | Date square + companion facts rail | 1 | 1 | 1 | **Winner / next production candidate** |
| P14 | Vertical month tab + horizontal time pill | 2 | 2 | 3 | Lab candidate |
| P01 | Square calendar tile + right context | 5 | 3 | 2 | Safe fallback |
| P03 | Editorial oversized numerals | 3 | 4 | 6 | Design boundary test |
| P04 | Vertical date rail | 4 | 8 | 4 | Conservative rail test |
| P09 | Current split-badge baseline | 9 | 5 | 5 | Control |
| P06/P07/P05 | calendar grid / dark glass / floating sticker | low | veto | veto | Do not implement globally |

The lab page `/lab/date-block/` renders the five model-selected alternatives plus the current control and includes the full 14-pattern comparison table with Pinterest source links.

## Recommended production direction: P13

Use a **date square + companion facts rail**:

- left square: short weekday, large day number, month;
- right rail, ordered by scan priority: time, venue, admission/price, format/duration;
- rows with missing data disappear rather than showing decorative placeholders;
- on small mobile the rail may stack below the square; at tablet/desktop it locks beside the square;
- use the existing warm KenigEvents palette, tabular numbers and SVG icons if icons are needed; do not use emoji as structural icons.

This solves both the current readability problem and the next surrounding-composition problem: a rectangular/square date anchor can reserve the right side for context without forcing another facts block above the CTA.

## Vetoes

- Mini calendar grid: too slow for “when?” scanning.
- Dark glass/nightlife card: brand mismatch for the warm cultural site.
- Floating sticker: decorative z-index/shadow trick, not reliable information architecture.
- Ticket stub: useful grid mechanics, but the ticket metaphor can falsely imply paid admission.
- Brutalist black/white: keep only as rare art-direction inspiration, not a global component.

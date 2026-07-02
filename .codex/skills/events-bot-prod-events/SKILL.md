---
name: events-bot-prod-events
description: Use when inspecting future or target-date events in events-bot production, especially questions like "what events are in prod for tomorrow", "why did poll-to-repost have few categories", "which events are repostable/popular", or when comparing production DB inventory with Telegram/VK poll outcomes. Provides a read-only Fly SQLite workflow and a reusable inventory script.
---

# Events Bot Prod Events

Use this skill for production event inventory, not for local fixtures. It is
project-specific to events-bot and Fly app `events-bot-new-wngqia`.

## Workflow

1. Clarify the absolute target date. If the user says "tomorrow", convert it to
   a concrete date and weekday before drawing conclusions.
2. Load Fly auth without printing secrets:
   `set -a; source /home/dev/.config/fly/release.env; set +a`.
3. Prefer the bundled script for repeatable read-only evidence:
   `python .codex/skills/events-bot-prod-events/scripts/future_events_inventory.py --date YYYY-MM-DD`.
4. Summarize the funnel:
   active DB events -> events with `tg_event_post_id` -> poll run eligible count
   -> popularity-qualified count -> option count and candidate ids.
5. For poll-to-repost, inspect `poll_repost_run.error_json` and
   `result_json.selection_trace.popularity`; those fields often contain the
   exact eligibility/popularity diagnostics used when the poll was created or
   resolved.
6. Keep the distinction clear:
   `source_vk_post_url` may be a legacy `kldevents` pointer; factual source
   evidence lives in `event_source`, while repost/repeater mappings live in
   `event_publication`.

## Script

`scripts/future_events_inventory.py` runs a read-only SQL probe through
`fly ssh console` and prints JSON. Useful flags:

- `--date YYYY-MM-DD`: required target date.
- `--app events-bot-new-wngqia`: override Fly app if needed.
- `--runs N`: number of recent poll runs for the target date to include.

The script intentionally does not download the production DB and does not write
to `/data/db.sqlite`.

## Interpretation Notes

- A busy calendar date can still produce a small poll if many events lack a
  repostable `@kldevents` Telegram post, fail target-date post matching, are not
  popularity-qualified, or collapse into duplicate live popularity groups.
- For weekend claims, state the exact weekday from the target date. Do not rely
  on the user's relative wording if it conflicts with production run data.
- If code changed after a suspicious poll, separate "what happened in that run"
  from "what the new code will do next".

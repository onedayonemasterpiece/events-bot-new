# Vector selection contract for Region Talk

## Positive classes

- `ko_visit_impression`: author/subscriber visited Kaliningrad Oblast and shares impressions, emotions, memorable details.
- `ko_route_useful`: useful route/advice about one or several places inside Kaliningrad Oblast.
- `ko_visual_place_card`: substantive card about a Kaliningrad Oblast location, nature/architecture/history, with visual potential.

## Negative classes

- `other_region_travel`: main topic is Moscow/another Russian region/country; Kaliningrad token is absent, incidental, hashtag, or homonym.
- `multi_region_roundup`: Kaliningrad is one item in a Russia-wide/multi-country list.
- `news_report`: factual news, official statement, court/police/investigation, record/weather, transport/policy, scientific-news blurb.
- `event_announcement`: afisha, exhibit/concert/program/schedule/registration/tickets.
- `ad_or_promo`: ad label, promo code, commercial service, tour/excursion sale, booking, contest/CTA.
- `local_institution_pr_event_report`: local institution self-report or PR about participating/hosting an event.
- `low_substance`: chat fragment, one-liner, test/internal bot output, photo-only caption with no useful regional story.

## Fusion policy

For each model, embed the post text as query and semantic-bank examples as passages. Record top positive, top negative, margin, and per-model top classes. Fuse by mean score and mark `both_models`, `model_disagreement`, or `single_model_fallback`.

Reject before image scoring when a negative class dominates and positive Kaliningrad fit is below the class-specific threshold. Keep ambiguous rows only for ranking/debug; they should not become publication-ready without final verifier.

## XLSX acceptance signal

`04a_final_shortlist` must not be filled with news/afisha/PR rows. Old candidate memory may retain rows for diagnostics, but product-facing shortlist must re-check memory through vector status and exclude vector-negative rows.

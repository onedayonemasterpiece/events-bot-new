# Communication UI results

Status: implemented R01-R08 in the isolated briefing lab.

- Mixed 8-scene deck plus fallback: counts, share/like/not-interested education, curiosity/social/comment DEMO signals.
- Lab-only bounded local memory scheduler uses `ke-briefing-memory-v1`; automatic selection is unseen/oldest with 1d, 14d, 30d and 90d action-success suppression. Exposure is recorded only after >=50% visibility for 250ms. Manual selection, Replay and Play All bypass and do not record exposure.
- Action success is synchronized from `ke_event_feedback_log_v1` and `ke:event-action-success`, never from a lab click.
- Pace preference uses `ke-briefing-lab-prefs-v1` with URL > saved > normal and reset control.
- Decorative mark is the cropped real `announcements-wordmark-ui.svg` path; bar/underscore cursor options blink at 780ms with configured linger and reduced-motion suppression.
- Touch controls are at least 44px; hero/categories/feed constraints remain.

Verification: targeted build/test attempted by worker; see parent integration verification for authoritative result.

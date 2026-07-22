# Integration report — Popular desktop V28

| Lane | Requirement IDs | Branch | Status | Head SHA | Merge/cherry-pick | Evidence |
|---|---|---|---|---|---|---|
| popular-desktop-v28 | R01–R09 | integration/popular-desktop-v28-20260720 | integrated | 6c191ad1 | serial lane already on integration branch | preview gate, 3 unit tests, 12 desktop geometry checks, 360/390/430 mobile checks |

The two earlier agents were read-only mappers. They made no filesystem changes
and therefore required no merge or rejection step. Mobile-owned components and
mobile media-query rules have no diff in V28.

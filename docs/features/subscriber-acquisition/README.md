# Subscriber Acquisition

Canonical requirements: [requirements.md](requirements.md).

Discovery MVP design: [mvp-discovery.md](mvp-discovery.md).

Source materials: [source/](source/).

## Implementation entrypoints

Shadow-mode MVP scaffolding is implemented in `subscriber_acquisition/` and is
wired into the bot through `main_part2.py` as `/acq*` superadmin commands,
including `/acq_surface_add` and `/acq_surfaces` candidate review cards. Core
state is stored in Fly SQLite tables with SQLModel models in `models.py` and raw
bootstrap DDL in `db.py`. The import payload contract is documented in
[`schemas/acq_discovery_result.schema.json`](schemas/acq_discovery_result.schema.json).

The MVP is intentionally review-only: it may send Telegram messages only to
`ACQ_REVIEW_CHAT_ID` review chat and has guardrails against external Telegram/VK
posting. Kaggle/session wiring uses the existing heavy-job and S22 remote-session
controls; the scanner runtime is under `kaggle/SubscriberAcquisitionDiscovery/`.
`/acq_run` uses the existing Kaggle encrypted split-dataset + status framework by
default (`ACQ_DISCOVERY_RUNNER=kaggle`) and can import an explicit result JSON.
`ACQ_DISCOVERY_RUNNER=local` is only a dev fallback that writes/imports
`acq_discovery_result.json` without any external sends. Local launchers require
the same Kaggle API dependency lane as Telegram Monitoring; `kagglesdk==0.1.30`
is pinned because newer 0.1.31+ wheels currently break Kaggle API imports before
the kernel can be pushed.

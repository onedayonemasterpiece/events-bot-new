# Static event preproduction continuation — integration report

Date: 2026-07-18
Branch: `integration/static-event-preprod-continuation-20260718`
Base: `38401787645584c2508579bd61dc3c345d0b207d`

## Requirement ledger

| ID | Outcome | Integrated change | Evidence |
|---|---|---|---|
| R01 | completed | Accepted compact graphite `SiteFooter` is global; lab route remains a regression specimen only. | root commit `d56e5310`; Gemini 3.1 Pro (High) compact-share validation `PASS`; component/runtime tests and preview/public checks below |
| R02 | completed | Medallion wrapper exposes decorative ring/shadow overflow without changing fail-closed identity resolution. | lane implementation `5f161b8c`, integrated as `06a21881`; `.codex/lanes/media_polish/RESULTS.md` |
| R03 | completed | Desktop static related cards emit skeleton/busy state before load and clear it on load/error with fixed geometry. | lane implementation `5f161b8c`, integrated as `06a21881`; `.codex/lanes/media_polish/RESULTS.md` |
| R04 | completed | Split portrait/OCR pages use measured one-row CTA; Editorial wide-photo pages keep the stacked panel with bottom utility row. | lane implementation `edaba366`, integrated as `7664831b`; 4/4 Playwright geometry in `.codex/lanes/portrait_cta/RESULTS.md` |
| R05 | completed | Desktop event continuation is bounded at six cards, honest personal/popular-fallback modes, no load-more, with terminal `Все анонсы`; mobile fallback is not duplicated. | root commit `d56e5310`; Gemini 3.1 Pro synthesis verdict `PASS` in ignored artifact `artifacts/codex/static-event-preprod-continuation/agy/desktop-continuation-synthesis-validation-gemini-3.1-pro-high.md` |
| R06 | completed | Accepted v11/v12 templates and latest verified immutable secret candidate are canonical for bot/operator review links. | lane implementation `cec13af1`, integrated as `c7d7459a`; `.codex/lanes/preprod_release/RESULTS.md` |
| R07 | completed | Smart Update/preproduction release keeps a durable fail-closed pointer and does not advance on failed/no-op/incomplete/artifact-only builds. | lane implementation `cec13af1`, integrated as `c7d7459a`; focused 39 Python + 5 Node tests |

## Integration order

1. `98b53251` / `38401787` — isolated then compact accepted footer specimen.
2. `7664831b` / `ee590292` — portrait CTA implementation/evidence.
3. `d56e5310` — footer rollout and bounded continuation.
4. `06a21881` / `341a984e` — medallion and related-media polish/evidence.
5. `c7d7459a` / `076348c6` — durable candidate review routing/evidence.
6. root documentation, regression tests, candidate build and live verification.

No worker patch was dropped. Lane worktrees were read-only after their clean committed handoff; integration conflicts were limited to a changelog reconciliation.

## Safety boundary

- Immutable noindex `_review/<token>/` candidate only.
- Production root, `current` and stable ICS are not publication targets.
- Candidate publication must pin the exact pushed `origin/main` SHA and pass accepted-template, noindex, no-referrer, prefix-isolation and root-isolation gates.
- Telegram handoff occurs only after public Playwright acceptance of the fresh candidate.

## Final validation

The implementation is main-reachable and deployed. Smart Update created a
fresh immutable secret candidate through the production Kaggle rail, and the
public HTTP/Playwright gates plus Telegram readback passed. Product visual
acceptance remains the only open gate in the parent media incident.

### Local validation (integration source)

- Preview build: `380` pages / `303` events, `PREVIEW_BUILD_ID=preview-20260718t-static-event-preprod-continuation-local` — passed.
- Preview checker: `303 events`, `strict_related=false` — passed.
- Event runtime + personal-feed Node tests: `14/14` — passed.
- Static-release Node tests: `5/5` — passed.
- Static release/handoff/debounce/public-gate pytest: `39/39` — passed.
- Content/media tests: `6/6`; event-media quality tests: `6/6` — passed.
- Python compilation, shell syntax and `git diff --check` — passed.
- CTA Playwright at `1536×864`: `4/4` (Split `6876/4783`; Editorial `6551/5374`) — passed.
- Local browser acceptance at `1536×864`: one global footer partnership link, two legal placeholders, coloured MAX, zero overflow; immature continuation mode `popular_fallback`, `Ещё события`, exactly six unique cards, no load-more, `Все анонсы` — passed.
- Local browser acceptance at `390×844`: fallback continuation hidden, accepted mobile renderer visible, zero overflow — passed.

### Production preproduction evidence

- exact `origin/main` / deployed source:
  `191eb0c7239aacdddad72398f8a45a6e08c9f3ad`;
- Fly image `deployment-01KXVDHRCAN2C6DGA5JSQ885HD`, machine version
  `1711`, one passing critical check;
- automatic startup catch-up / Smart Update build
  `production-secret-20260718T221203-9132a760`, run
  `static-site:production-secret-20260718T221203-9132a760:76d8fb98cf48`;
- immutable input `snapshot-20260718T201203-9828d9a7c1`; published result
  SHA-256 `f634d18f812913653bf04bc75a5ceee51e0c156c652fb77748a07f30d49f5326`,
  manifest SHA-256
  `4d6a81ce6031f20bf1c510910a30cb407d25d95c2bce27dbe3d6ab3d71ecddbe`,
  `984` create-only objects;
- durable current-candidate receipt points to that exact source and token
  SHA-256
  `25f8da859b2d03c2d21e9fc1bad58ba947fc756aebfad70d99d3916be1989def`;
  the bearer token itself is deliberately not committed;
- public HTTP `200` with `noindex` and `no-referrer` on the candidate index,
  frozen footer/CTA/transport specimens, and current events `6851`, `6551` and
  `5658`;
- public Playwright at `1536x864` verified the frozen Split CTA in one row
  (`100.78 px`) and Editorial CTA stacked (`227.125 px`), with all controls
  contained; actual `6851` used the compact Split phone CTA, while `6551` and
  `5658` retained Editorial hierarchy;
- the footer rendered once with one partnership link, both legal placeholders,
  coloured MAX asset and exact accessible prompt
  `Понравились Анонсы? Поделитесь`; Gemini 3.1 Pro (High) validated the compact
  84 px inline-bar with `PASS` and no blockers; venue medallion overflow was
  visible; all three forced transport arms retained the accepted structure and
  `на Кауп` wording; desktop continuation was finite at six cards with
  `popular_fallback`, no load-more and terminal `Все анонсы`;
- mobile `390 px` kept the accepted mobile renderer without horizontal
  overflow; intercepted related images exposed ten `aria-busy=true` cards and
  ten visible skeleton fallbacks before load;
- candidate publication did not mutate the production root or sitemap: body
  SHA-256
  `e2ddecb6c2856a94d4579a3091604b7c0804f3545220f43e94eac73e0aab450d`,
  sitemap SHA-256
  `643f22960e703b91c173d4d52425ca28b6513da9612904047d9930508e329fa7`;
- corrected current-candidate links were sent with the role-approved E2E human
  session and read back as Telegram message `361`, reply to message `261`, in
  topic `2` of chat `-1004337049383`.

The production ledger recorded real `kernel_started`, `resource_acquire`,
multi-phase `alive`, `preflight_ok`, terminal `report_written` and
`resource_release` events. The run is `done/cleanup`, has a real heartbeat and
terminal timestamp, and `static_site_build_state` has no active claim.

Post-run production checks: `PRAGMA quick_check=ok`, public `/healthz` HTTP
`200` with `ready=true` and disk `status=ok`, about `1.6 GiB` free on `/data`,
bounded runtime-log rotation, and no fresh `Errno 28`, disk-full or static-build
failure entries. Terminal cleanup removed `268,010,146` bytes of the successful
input and left only the configured diagnostic snapshot, its exact SQLite
sidecars, and three pre-existing 1 KiB orphan temporary-journal files.

### Checklist correction

The final read-only checklist found stale footer and event `5756` acceptance
assertions plus an inert static-score label. Hotfix
`hotfix/static-event-preprod-gates-20260718` aligns the secret-candidate marker,
the full desktop contract and the upcoming-date fallback signal before the
compensating Smart Update/Kaggle build. This supersedes the earlier local claim
that all candidate gates had already been exercised.

# Region Talk publisher profile enrichment results

These reusable publisher-profile packages are separate from
`region-talk-external-research-result-*.json` candidate intake. They are
**not** consumed by the candidate auto-import workflow and grant no publication
permission. Their only execute path is the dedicated exact-main
publisher-profile workflow/importer.

| Date (UTC) | File | Source | Linked candidates | SHA-256 | Status |
|---|---|---|---:|---|---|
| 2026-08-02 | `region-talk-publisher-profile-enrichment-archi-ru-2026-08-02.json` | Архи.ру | 1 | `f8440fd7d6430386624936c3181bac11936e64da0d26f7641b7c763f3c906666` | Schema-valid; ready reusable profile |
| 2026-08-02 | `region-talk-publisher-profile-enrichment-peasantstudies-ru-2026-08-02.json` | «Крестьяноведение» | 1 | `0d61c1eac7799e70e677a23eb61537bf8c725aebbd1e8fd035548fde28e37433` | Schema-valid; ready reusable profile |
| 2026-08-02 | `region-talk-publisher-profile-enrichment-rg-ru-2026-08-02.json` | «Российская газета» | 1 | `2bae5d314ec2388b6a5033ef233e04dce4cf29e471e61237585157ff05918f1e` | Schema-valid; mixed brand, candidate externality re-adjudication required |

## Important correction

The linked `rg.ru/.../reg-szfo/...` article is proposed for externality
re-adjudication because the exact page identifies a Kaliningrad-region
correspondent and a regional section. The package/importer does not mutate the
candidate. The implemented explicit review command performs a serializable
correction/identity/intake reread, requires the exact current intake snapshot
hash and writes only a reviewed correction plus immutable audit. The expected
live RG decision is `block_regional`; regeneration stays disabled.

## Import boundary

The dedicated importer and guarded workflow consume only files matching:

`region-talk-publisher-profile-enrichment-*.json`

The existing `region-talk-external-research-result-*.json` candidate importer
and workflow glob remain unchanged. Import status and production readback are
recorded in release artifacts rather than edited into these sidecars.

# Region Talk publisher profile enrichment results

These reusable publisher-profile packages are separate from `region-talk-external-research-result-*.json` candidate intake. They are **not** consumed by the current candidate auto-import workflow and grant no publication permission.

| Date (UTC) | File | Source | Linked candidates | SHA-256 | Status |
|---|---|---|---:|---|---|
| 2026-08-02 | `region-talk-publisher-profile-enrichment-archi-ru-2026-08-02.json` | Архи.ру | 1 | `baba927d2269c76322959e1d60e6249fef947d7a1742d284bbe0dd9b0fa266c1` | Schema-valid; ready reusable profile |
| 2026-08-02 | `region-talk-publisher-profile-enrichment-peasantstudies-ru-2026-08-02.json` | «Крестьяноведение» | 1 | `92fae3aa615303c3e1a5e54994fd532700d6622c5586a87cf0d6cee076616562` | Schema-valid; ready reusable profile |
| 2026-08-02 | `region-talk-publisher-profile-enrichment-rg-ru-2026-08-02.json` | «Российская газета» | 1 | `79d6cd3019959e26ee703e9e3831cab08ae9a86f2d50e445bef7f0555970a127` | Schema-valid; mixed brand, candidate externality re-adjudication required |

## Important correction

The linked `rg.ru/.../reg-szfo/...` article is proposed for externality re-adjudication because the exact page identifies a Kaliningrad-region correspondent and a regional section. The package does not mutate the candidate; implementation must perform a strong live YDB re-read and enter the explicit review path.

## Future import boundary

A dedicated importer may consume files matching:

`region-talk-publisher-profile-enrichment-*.json`

The existing `region-talk-external-research-result-*.json` candidate importer and workflow glob must remain unchanged.

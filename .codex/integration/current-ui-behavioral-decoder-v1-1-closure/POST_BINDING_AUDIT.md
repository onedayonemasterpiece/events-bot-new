# Post-binding audit addendum — Current UI Behavioral Decoder v1.1

**Reviewer:** `/root/behavioral_final_audit (Archimedes, read-only acceptance reviewer)`

**Result:** `PASS`. The final binding is valid and
`READY_FOR_PROJECT_NORMALIZATION_SYNTHESIS` is justified.

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| PB-01 | Final manifest/status | Done | Manifest SHA-256 `c676be4f2ad956b8a58c7707c8f71b7bb33afd771e506457309597e76d67d9a1`; READY; blockers empty; 293 terminal / 236 PASS / 39 MISMATCH / 18 UNREACHABLE; 134 reviews; 87 findings / 0 blocking. |
| PB-02 | Receipt binding | Done | Receipt SHA-256 `d981ad23280dd177d1fef8a59674fe754c5887c76a0981cd722a59c604780d9f`; complete/READY; exact manifest hash and counts. |
| PB-03 | Review ledger | Done | Manual ledger SHA-256 `8dafd73a26c14aa6229fdd9d25eb82f14e8639cd47245666ee0cbe792a5e6864`; declared provenance matches. |
| PB-04 | Audit binding | Done | Audit commit `26697c4164ec67b804b66ee89f7b459dfbc34e76`; report SHA-256 `30ecc47815c7815cb91ff290b84b8aaa49a567d7d4d1ce52114a1b1ccf268fff`; stable reviewer and capture/materializer identities match. |
| PB-05 | Actions/Release/security | Done | Run `31327863197`, artifact `9042097413`, raw asset `507763470`, digest `8bb8712e…`, secret scan PASS, immutable/source/STOP facts bound. |
| PB-06 | Reviewed compact Release | Done | Asset `507776086`, 1,511,707 bytes, SHA-256 `cd89640e1d808a293fda8704c7294160a1d918009b3b21fa3562fde66900f4ab`; ZIP and embedded hashes PASS. |
| PB-07 | Durable receipt Release | Done | Asset `507776555`, 2,899 bytes, SHA-256 `4f292848430683aaff537567a1307cdd4a4a8dfe6d62931794d1c9ba4405e3e7`; binds capture, compact package, counts, design publication, audit, STOP, and validator PASS. |
| PB-08 | Strict validation/CI | Done | Strict closure validator PASS; PR #456 Python/static-browser/contracts/facts/gate checks PASS. |

Critical: 0. High: 0. Medium: 0. The 39 mismatches, 18 unreachable
observations, and two rail implementation gaps remain truthful nonblocking
evidence; none is converted into implementation approval. No further capture
or review is required.

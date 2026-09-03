import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const acceptance = JSON.parse(await readFile(
  new URL('../scripts/n0-successor-acceptance.v1.json', import.meta.url),
  'utf8',
));
const overlay = JSON.parse(await readFile(
  new URL('../scripts/n0-current-ref-overlay.v1.json', import.meta.url),
  'utf8',
));

test('current-ref overlay supersedes refs only and cannot rewrite acceptance semantics', () => {
  assert.equal(overlay.schema_version, 'kenigevents.n0-current-ref-overlay.v1');
  assert.equal(overlay.contract_version, acceptance.contract_version);
  assert.equal(overlay.supersedes.file, 'site/scripts/n0-successor-acceptance.v1.json');
  assert.equal(overlay.supersedes.scope, 'current_refs_and_evidence_cutoff_only');
  assert.equal(overlay.supersedes.acceptance_semantics_changed, false);
  assert.equal(overlay.latest_meaningful_comment.id, 5529571393);
});

test('live refs preserve the accepted build-critical source boundaries', () => {
  assert.equal(overlay.refs.source_successor, acceptance.current_refs.source_successor.head);
  assert.equal(overlay.refs.F0_source_boundary, acceptance.current_refs.F0.source_head);
  assert.equal(overlay.refs.M0_source_boundary, acceptance.current_refs.M0.source_head);
  assert.equal(overlay.refs.M0_current_downstream_head, acceptance.current_refs.M0.downstream_head);
  assert.equal(overlay.refs.A0, acceptance.current_refs.A0.current_head);
  assert.equal(overlay.refs.F0_current_reporting_head, '0246d5bc1cd606bf5e71d4d3419892d63c54216e');
  assert.equal(overlay.refs.thin_sot, 'de6dbfc3b7b920a9a829923cc7eca3150f526079');
});

test('reporting and SoT movement cannot become hidden runtime gates', () => {
  assert.equal(overlay.classification.F0_reporting, 'REVIEW_EVIDENCE_NOT_RUNTIME_INPUT');
  assert.ok(overlay.non_build_critical_refs.includes('F0_current_reporting_head'));
  assert.equal(overlay.truth_boundary.reporting_head_is_not_runtime_source, true);
  assert.equal(overlay.truth_boundary.source_acceptance_is_not_executable_pass, true);
  assert.equal(overlay.truth_boundary.published_baseline_is_not_current_successor_pass, true);
  assert.equal(overlay.truth_boundary.penpot_spec_is_not_penpot_materialization, true);
});

test('current remaining source clusters exactly match the N0 candidate boundary', () => {
  assert.deepEqual(overlay.remaining_source_clusters, [
    'F0_ROUTE_THEME_CONSUMPTION',
    'F0_INTEREST_CLUB_CARD_RESIDUAL',
    'A0_FOCUS_ROUTE_IDENTITIES',
  ]);
  assert.deepEqual(overlay.remaining_source_clusters, acceptance.nearest_full_real_candidate.blocking_source_fixes);
  assert.equal(overlay.v0.platform_blocker_comment, 5529063082);
  assert.equal(overlay.v0.browser_verdict, 'NOT_EXECUTED');
});

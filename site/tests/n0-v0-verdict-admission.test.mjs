import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const admission = JSON.parse(await readFile(
  new URL('../scripts/n0-v0-verdict-admission.v1.json', import.meta.url),
  'utf8',
));

const CEBEAFEE = 'cebeafeee08251a327145ee973ee035cced65204';
const SUCCESSOR = '3ca6a143e4286c165282c2d8ceef1759a41185b7';
const SUCCESSOR_BUILD = 'preview-real-3ca6a143e-normalized-20260903-v1';

const defect = (code) => admission.current_V0_harness.blocking_defects.find((item) => item.code === code);

test('admission preserves exact accepted precursor and public successor identity', () => {
  assert.equal(admission.schema, 'kenigevents.n0-v0-verdict-admission.v1');
  assert.equal(admission.version, '1.1.0');
  assert.equal(admission.contract_version, '1.10.0');
  assert.deepEqual(admission.FR0_requirement_comments, [5531944339, 5531980502, 5532544488]);
  assert.deepEqual(admission.candidate_targets.map(({ source_sha }) => source_sha), [CEBEAFEE, SUCCESSOR]);
  assert.equal(admission.candidate_targets[0].build_id, 'preview-real-cebeafeee-normalized-20260903-v1');
  assert.equal(admission.candidate_targets[1].build_id, SUCCESSOR_BUILD);
  assert.equal(admission.candidate_targets[1].public_url, `https://kenigevents.ru/${SUCCESSOR_BUILD}/__preview/`);
  assert.equal(admission.candidate_targets[1].manifest_url, `https://kenigevents.ru/${SUCCESSOR_BUILD}/preview-build.json`);
  assert.equal(admission.candidate_targets[1].snapshot_id, 'issue621-real-3ca6a143e-20260903T211550Z');
  assert.match(admission.candidate_targets[1].snapshot_sha256, /^[0-9a-f]{64}$/u);
});

test('the current automated V0 classifier is rejected without blocking personal browser observation', () => {
  const harness = admission.current_V0_harness;
  assert.equal(harness.branch, 'work/ui-normalization-v0-harness-20260903');
  assert.equal(harness.head, '2e71e5521a1ebbd5f98c794abebd70aced030639');
  assert.equal(harness.script_blob, 'f3618120b5e2980f31dcd468328de4b99985e6fe');
  assert.equal(harness.test_blob, 'fc1ec9afaf50f4f61fb50eef858363c0b6936221');
  assert.equal(harness.classification, 'AUTOMATED_CLASSIFIER_NOT_ADMISSIBLE_UNCHANGED_MANUAL_BROWSER_EVIDENCE_ALLOWED');
  assert.equal(harness.blocking_defects.length, 5);
  assert.equal(harness.does_not_block_personal_browser_observation, true);
  assert.equal(harness.does_not_block_R0_kaggle_publication, true);
  assert.equal(harness.does_not_create_FR0_source_batch, true);
});

test('runtime typo and four FR0 semantic/scope gaps are all explicit', () => {
  assert.match(defect('V0_HARNESS_RUNTIME_IDENTIFIER_TYPO').evidence, /AUTHORED_AGAINCT_SOURCE/u);
  assert.match(defect('V0_HARNESS_RAW_BOX_ESCAPE_FALSE_POSITIVE').effect, /5531980502/u);
  assert.match(defect('V0_HARNESS_INTERACTION_OWNER_NONE_FALSE_POSITIVE').effect, /5531944339/u);
  assert.match(defect('V0_HARNESS_RESOURCE_STATE_EVIDENCE_MISSING').required_resolution, /resource-state/u);
  assert.match(defect('V0_HARNESS_UNSCOPED_BARE_MEDIA_FRAME_MARKERS').effect, /malformed canonical v1/u);
});

test('canonical v1 roots and historical bare markers have separate verdict semantics', () => {
  const scope = admission.media_frame_root_scope;
  assert.equal(scope.canonical_selector, '[data-media-frame][data-media-frame-contract="v1"]');
  assert.equal(scope.legacy_selector, '[data-media-frame]:not([data-media-frame-contract])');
  assert.equal(scope.legacy_classification, 'LEGACY_RESOURCE_MARKER_OUTSIDE_CURRENT_V1_DELIVERY');
  assert.equal(scope.legacy_resource_identity, 'event-format.media.primary-large-frame / primary_media_frame');
  assert.deepEqual(scope.rules, [
    'legacy markers are not malformed MediaFrame v1 roots',
    'legacy markers receive no current v1 normalization credit',
    'legacy markers remain visible in the later normalization census',
    'this classification authorizes no product-source change or FR0 batch',
  ]);
});

test('manual V0 verdict remains admissible only with direct and complete browser evidence', () => {
  const personal = admission.admission_paths.personal_browser_verdict;
  assert.equal(personal.admissible_without_classifier_update, true);
  for (const required of [
    'V0 personally uses my-browser-bridge',
    'the issue result names the exact public URL, source SHA, build ID and snapshot',
    'each finding cites route, viewport, selector and observed browser evidence',
    'the result applies accepted clipping and interaction-owner semantics rather than the stale classifier rules',
    'canonical MediaFrame roots use the exact v1 selector and bare legacy markers are reported separately',
    'the result separately reports exhibitions route, row, MediaFrame, resource-state, interaction and accessibility sections',
    'omitted required sections are INCOMPLETE rather than PASS',
    'R0 smoke, source assertions and inferred CSS are not substituted for browser evidence',
  ]) assert.ok(personal.required.includes(required), `missing personal-verdict gate: ${required}`);
});

test('exhibitions slice admission uses the accepted FR0 contract', () => {
  const slice = admission.exhibitions_vertical_slice_admission;
  assert.equal(slice.source_acceptance, 'COMPLETE');
  assert.equal(slice.browser_acceptance, 'PENDING');
  assert.deepEqual(slice.required_viewports, [375, 620, 1024, 1440]);
  assert.deepEqual(slice.required_frame_surfaces, [
    'exhibitions-deck',
    'exhibitions-gallery',
    'exhibitions-medallion',
  ]);
  assert.deepEqual(slice.required_resource_states, ['pending', 'loaded', 'fallback', 'broken']);
  assert.equal(slice.accepted_interaction_owners['exhibitions-deck'], 'caller');
  assert.equal(slice.accepted_interaction_owners['exhibitions-gallery'], 'caller');
  assert.match(slice.accepted_interaction_owners['exhibitions-medallion'], /none/u);
  assert.match(slice.clipping_rule, /raw image-box extension is not drift by itself/u);
  assert.equal(slice.canonical_root_selector, admission.media_frame_root_scope.canonical_selector);
  assert.equal(slice.acceptance_is_independent_of_unrelated_route_drift, true);
});

test('known runtime EventCard drift cannot be hidden by whole-preview or slice PASS', () => {
  const drift = admission.known_source_bound_drift;
  assert.equal(drift.comment, 5532284724);
  assert.equal(drift.N0_acceptance_comment, 5532335696);
  assert.equal(drift.domain, 'client-created EventCard runtime MediaFrame rebinding');
  assert.equal(drift.expected_on_3ca6, 'DRIFT_OR_INCOMPLETE_NOT_PASS');
  assert.equal(drift.does_not_reject_exhibitions_slice, true);
});

test('the correction affects neither accepted publication nor integrated successor', () => {
  assert.equal(admission.decision.current_automated_harness, 'REJECTED_UNCHANGED');
  assert.equal(admission.decision.personal_V0_browser_verdict, 'READY_FOR_ADMISSION');
  assert.equal(admission.decision.cebeafee_publication, 'UNAFFECTED');
  assert.equal(admission.decision.three_batch_successor, 'PUBLICATION_ACCEPTED_V0_PENDING');
  assert.equal(admission.decision.next_action_owner, 'V0_THEN_N0');
});

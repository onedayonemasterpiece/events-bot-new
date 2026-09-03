import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const admission = JSON.parse(await readFile(
  new URL('../scripts/n0-v0-verdict-admission.v1.json', import.meta.url),
  'utf8',
));

const CEBEAFEE = 'cebeafeee08251a327145ee973ee035cced65204';
const SUCCESSOR = '3ca6a143e4286c165282c2d8ceef1759a41185b7';

const defect = (code) => admission.current_V0_harness.blocking_defects.find((item) => item.code === code);

test('admission preserves exact current and successor target identity', () => {
  assert.equal(admission.schema, 'kenigevents.n0-v0-verdict-admission.v1');
  assert.equal(admission.version, '1.0.0');
  assert.equal(admission.contract_version, '1.10.0');
  assert.deepEqual(admission.candidate_targets.map(({ source_sha }) => source_sha), [CEBEAFEE, SUCCESSOR]);
  assert.equal(admission.candidate_targets[0].build_id, 'preview-real-cebeafeee-normalized-20260903-v1');
  assert.equal(admission.candidate_targets[1].build_id, null);
});

test('the current automated V0 classifier is rejected without blocking personal browser observation', () => {
  const harness = admission.current_V0_harness;
  assert.equal(harness.branch, 'work/ui-normalization-v0-harness-20260903');
  assert.equal(harness.head, '2e71e5521a1ebbd5f98c794abebd70aced030639');
  assert.equal(harness.script_blob, 'f3618120b5e2980f31dcd468328de4b99985e6fe');
  assert.equal(harness.test_blob, 'fc1ec9afaf50f4f61fb50eef858363c0b6936221');
  assert.equal(harness.classification, 'AUTOMATED_CLASSIFIER_NOT_ADMISSIBLE_UNCHANGED_MANUAL_BROWSER_EVIDENCE_ALLOWED');
  assert.equal(harness.blocking_defects.length, 4);
  assert.equal(harness.does_not_block_personal_browser_observation, true);
  assert.equal(harness.does_not_block_R0_kaggle_publication, true);
  assert.equal(harness.does_not_create_FR0_source_batch, true);
});

test('runtime typo and three FR0 semantic gaps are all explicit', () => {
  assert.match(defect('V0_HARNESS_RUNTIME_IDENTIFIER_TYPO').evidence, /AUTHORED_AGAINCT_SOURCE/u);
  assert.match(defect('V0_HARNESS_RAW_BOX_ESCAPE_FALSE_POSITIVE').effect, /5531980502/u);
  assert.match(defect('V0_HARNESS_INTERACTION_OWNER_NONE_FALSE_POSITIVE').effect, /5531944339/u);
  assert.match(defect('V0_HARNESS_RESOURCE_STATE_EVIDENCE_MISSING').required_resolution, /resource-state/u);
});

test('manual V0 verdict remains admissible only with direct and complete browser evidence', () => {
  const personal = admission.admission_paths.personal_browser_verdict;
  assert.equal(personal.admissible_without_classifier_update, true);
  for (const required of [
    'V0 personally uses my-browser-bridge',
    'the issue result names the exact public URL, source SHA, build ID and snapshot',
    'each finding cites route, viewport, selector and observed browser evidence',
    'the result applies accepted clipping and interaction-owner semantics rather than the stale classifier rules',
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
  assert.equal(slice.acceptance_is_independent_of_unrelated_route_drift, true);
});

test('the correction affects neither accepted publication nor integrated successor', () => {
  assert.equal(admission.decision.current_automated_harness, 'REJECTED_UNCHANGED');
  assert.equal(admission.decision.personal_V0_browser_verdict, 'READY_FOR_ADMISSION_WHEN_PUBLISHED');
  assert.equal(admission.decision.cebeafee_publication, 'UNAFFECTED');
  assert.equal(admission.decision.three_batch_successor, 'UNAFFECTED');
  assert.equal(admission.decision.next_action_owner, 'V0');
});

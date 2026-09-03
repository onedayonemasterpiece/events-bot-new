import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const receipt = JSON.parse(await readFile(
  new URL('../scripts/n0-runtime-eventcard-mediaframe-drift.v1.json', import.meta.url),
  'utf8',
));

const SUCCESSOR = '3ca6a143e4286c165282c2d8ceef1759a41185b7';

const confirmed = (code) => receipt.source_evidence.confirmed_defects.find((item) => item.code === code);

test('FR0 runtime finding is accepted as an isolated A0 consumer drift', () => {
  assert.equal(receipt.schema, 'kenigevents.n0-runtime-eventcard-mediaframe-drift.v1');
  assert.equal(receipt.version, '1.0.0');
  assert.equal(receipt.contract_version, '1.10.0');
  assert.equal(receipt.finding.source_comment, 5532284724);
  assert.equal(receipt.finding.source_role, 'FR0');
  assert.equal(receipt.finding.candidate_sha, SUCCESSOR);
  assert.equal(receipt.finding.classification, 'ACCEPTED_A0_RUNTIME_CONSUMER_DRIFT');
  assert.equal(receipt.finding.source_writer, 'A0');
  assert.equal(receipt.finding.protocol_acceptance_owner, 'FR0');
  assert.equal(receipt.finding.candidate_acceptance_owner, 'N0');
  assert.equal(receipt.finding.mechanical_integration_and_execution_owner, 'R0');
});

test('exact EventLayout source evidence covers every proven runtime defect', () => {
  assert.equal(receipt.source_evidence.path, 'site/src/layouts/EventLayout.astro');
  assert.equal(receipt.source_evidence.blob, '85ffdf1ecfb8abe0fe8487d0570957c98199ff9e');
  assert.deepEqual(receipt.source_evidence.runtime_entrypoints, [
    'applyRuntimeRelatedLayout',
    'createEventCardElement',
    'KenigEventsCreateEventCard',
    'KenigEventsRenderEventCard',
  ]);
  for (const code of [
    'RUNTIME_FRAME_VARIABLES_CLEARED',
    'RUNTIME_PROTOCOL_NOT_REBOUND',
    'DUPLICATE_RUNTIME_FIT_OWNER',
    'LEGACY_LOAD_TRANSITION',
    'MISSING_SOURCE_INCOMPLETE_FALLBACK',
    'BROKEN_RESOURCE_CAN_RETAIN_TEMPLATE_STATE',
  ]) assert.ok(confirmed(code), `missing confirmed source defect ${code}`);
  assert.match(confirmed('RUNTIME_FRAME_VARIABLES_CLEARED').evidence, /style\.cssText/u);
  assert.match(confirmed('DUPLICATE_RUNTIME_FIT_OWNER').evidence, /style\.objectFit/u);
});

test('static roots and unaffected delivery slices remain accepted', () => {
  const preserved = receipt.preserved_acceptance;
  assert.equal(preserved.static_EventCard_source.blob, '7bc689ac9b6f823955828706ea4f5ecb23acb58a');
  assert.equal(preserved.static_EventCard_source.status, 'ACCEPTED_UNCHANGED');
  assert.equal(preserved.static_ListingEventCard_source.blob, 'b8a218b9a0accb9f64b51be332a4e797b4aac2b7');
  assert.equal(preserved.static_ListingEventCard_source.status, 'ACCEPTED_UNCHANGED');
  assert.equal(preserved.M0_batch, 'ACCEPTED_UNCHANGED');
  assert.equal(preserved.F0_batch, 'ACCEPTED_UNCHANGED');
  assert.equal(preserved.exhibitions_vertical_slice_source, 'ACCEPTED_UNCHANGED');
  assert.equal(preserved.cebeafee_publication, 'ACCEPTED_UNCHANGED');
});

test('known drift does not delay the exact 3ca6 Kaggle publication', () => {
  const delivery = receipt.delivery_semantics;
  assert.equal(delivery.publish_3ca6_exact_full_real_preview, true);
  assert.equal(delivery.may_label_3ca6_FR0_MediaFrame_PASS, false);
  assert.equal(delivery.known_drift_must_be_disclosed_in_V0_trigger, true);
  assert.equal(delivery.defect_isolated_from_unrelated_domains, true);
  assert.equal(delivery.does_not_delay_current_3ca6_Kaggle_transaction, true);
  assert.equal(delivery.next_correction_candidate_required_after_publication, true);
});

test('correction remains one A0 helper and may not fork canonical roots', () => {
  const contract = receipt.correction_contract;
  assert.deepEqual(contract.allowed_source, [
    'site/src/layouts/EventLayout.astro',
    'one bounded A0 runtime-clone regression file',
  ]);
  assert.match(contract.implementation_shape, /one local EventLayout runtime binding helper/u);
  for (const forbidden of [
    'site/src/components/EventCard.astro',
    'site/src/components/listings/ListingEventCard.astro',
    'site/src/components/media-frame.css',
    'a second runtime card renderer',
    'a second MediaFrame implementation',
  ]) assert.ok(contract.forbidden_source.includes(forbidden), `missing forbidden source ${forbidden}`);
  assert.deepEqual(contract.required_structural_protocol, {
    contract: 'v1',
    style_owner: 'media-frame.css',
    surface: 'event-card',
    interaction_owner: 'caller',
    clip_radius_fill: 'preserve current canonical structural values',
  });
  assert.ok(contract.remove_duplicate_ownership.includes('no runtime image.style.objectFit assignment'));
  assert.ok(contract.remove_duplicate_ownership.includes('no runtime image.style.objectPosition assignment'));
});

test('resource-state transition contract is fail closed and removes stale template evidence', () => {
  const transitions = receipt.correction_contract.required_resource_transitions;
  assert.equal(transitions.url_present_initial, 'pending');
  assert.match(transitions.successful_load, /^loaded;/u);
  assert.match(transitions.network_error_or_unusable_decode, /broken \+ kind=fallback \+ fit=contain/u);
  assert.match(transitions.network_error_or_unusable_decode, /resource_load_error/u);
  assert.match(transitions.network_error_or_unusable_decode, /failed src\/srcset removed/u);
  assert.match(transitions.no_usable_source, /fallback \+ kind=fallback \+ fit=contain/u);
  assert.match(transitions.no_usable_source, /runtime_event_card_fallback/u);
  assert.ok(receipt.correction_contract.required_authoritative_rebinding.some((item) => item.includes('clear all stale template')));
});

test('regression and V0 gates require actual client-created cards', () => {
  assert.equal(receipt.regression_acceptance.suggested_path, 'site/tests/a0-runtime-event-card-media-frame-binding.test.mjs');
  assert.ok(receipt.regression_acceptance.required.includes('no runtime style.objectFit or style.objectPosition assignment remains'));
  assert.deepEqual(receipt.v0_acceptance.required_runtime_consumers, [
    'at least one client-created personal-feed or authorized-search EventCard',
    'at least one client-created related-continuation EventCard',
  ]);
  assert.deepEqual(receipt.v0_acceptance.required_viewport_classes, ['mobile', 'desktop']);
  assert.equal(receipt.v0_acceptance['3ca6_expected_result'], 'DRIFT_OR_INCOMPLETE_NOT_PASS');
  assert.equal(receipt.v0_acceptance.corrected_successor_expected_result, 'PERSONAL_V0_VERDICT_REQUIRED');
});

test('queue authorizes exactly one A0 correction and no FR0 source batch', () => {
  const queue = receipt.queue_and_candidate_rule;
  assert.equal(queue.FR0_source_batch, null);
  assert.equal(queue.FR0_parallel_product_write, false);
  assert.equal(queue.new_A0_batch_now_authorized_by_factual_FR0_drift, true);
  assert.equal(queue.maximum_A0_batches_outside_current_successor, 1);
  assert.equal(queue.broken_batch_excluded_separately, true);
  assert.equal(queue.N0_auto_pull_on_A0_result, true);
  assert.equal(queue.candidate_lag_minutes_max, 30);
  assert.equal(queue.next_full_real_preview_max_minutes_after_correction_intake, 60);
});

test('rollback and final N0 decision preserve unrelated accepted work', () => {
  assert.equal(receipt.rollback.unit, 'A0-RUNTIME-EVENTCARD-MEDIAFRAME-REBIND');
  assert.deepEqual(receipt.rollback.revert_only, [
    'EventLayout runtime helper and call-site rebinding',
    'its bounded regression file',
  ]);
  assert.ok(receipt.rollback.do_not_revert.includes('static EventCard or ListingEventCard source'));
  assert.ok(receipt.rollback.do_not_revert.includes('FR0 canonical media-frame.css'));
  assert.ok(receipt.rollback.do_not_revert.includes('cebeafee or 3ca6 immutable preview prefixes'));
  assert.equal(receipt.N0_decision.finding, 'ACCEPTED');
  assert.equal(receipt.N0_decision['3ca6_publication'], 'MUST_CONTINUE');
  assert.equal(receipt.N0_decision.FR0_MediaFrame_delivery, 'NOT_ACCEPTED_ON_3CA6');
  assert.equal(receipt.N0_decision.exhibitions_vertical_slice, 'STILL_INDEPENDENTLY_ACCEPTABLE_IF_V0_SECTIONS_PASS');
  assert.equal(receipt.N0_decision.next_source_action_owner, 'A0');
});

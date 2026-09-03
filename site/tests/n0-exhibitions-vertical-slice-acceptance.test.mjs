import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');
const acceptance = JSON.parse(await read('scripts/n0-exhibitions-vertical-slice-acceptance.v1.json'));
const strictSource = process.env.N0_REQUIRE_EXHIBITIONS_SLICE_SOURCE === '1';

const FROZEN = 'cebeafeee08251a327145ee973ee035cced65204';
const EXPECTED_BLOBS = Object.freeze({
  'site/src/components/ExhibitionsPersonalSurface.astro': '61d065efbc9b05254601ae807b7fcffec701bd04',
  'site/src/components/ExhibitionPrototypeRow.astro': 'ff94b32a288b079f27ca9e8c33d6975f52012478',
  'site/src/components/exhibitionsMediaFrameBridge.mjs': '1898f0ce973676241d54530d51c17b577e7c6509',
  'site/src/components/media-frame.css': '1231b0665054da3cd9bf936585a7d2e02838b82a',
  'site/tests/fr0-exhibitions-media-frame-contract.test.mjs': 'ff26615718b83134db9bb65e1a10831818381743',
});

test('vertical slice is source-accepted on one frozen transaction without browser inflation', () => {
  assert.equal(acceptance.schema, 'kenigevents.n0-exhibitions-vertical-slice-acceptance.v1');
  assert.equal(acceptance.version, '1.1.0');
  assert.equal(acceptance.contract_version, '1.10.0');
  assert.deepEqual(acceptance.v0_requirement_comments, [5531944339, 5531980502]);
  assert.equal(acceptance.frozen_transaction.source_sha, FROZEN);
  assert.equal(acceptance.frozen_transaction.build_id, 'preview-real-cebeafeee-normalized-20260903-v1');
  assert.equal(acceptance.slice.id, 'EXHIBITIONS_PERSONAL_ROW_FR0_MEDIAFRAME');
  assert.deepEqual(acceptance.slice.chain, [
    'ExhibitionsPersonalSurface',
    'ExhibitionPrototypeRow',
    'FR0 MediaFrame',
  ]);
  assert.equal(acceptance.slice.route, '/vystavki/');
  assert.equal(acceptance.slice.source_verdict, 'ACCEPTED_SOURCE');
  assert.equal(acceptance.slice.browser_verdict, 'PENDING_V0');
  assert.equal(acceptance.slice.independent_acceptance, true);
  assert.equal(acceptance.slice.unrelated_route_drift_rejects_slice, false);
  assert.equal(acceptance.source_acceptance.runtime_or_browser_credit, false);
});

test('exact source blobs and ownership are pinned to cebeafee', () => {
  assert.equal(acceptance.exact_source.length, 5);
  assert.deepEqual(
    Object.fromEntries(acceptance.exact_source.map(({ path:sourcePath, blob }) => [sourcePath, blob])),
    EXPECTED_BLOBS,
  );
  assert.equal(
    acceptance.exact_source.find(({ path:sourcePath }) => sourcePath.endsWith('media-frame.css'))?.owner,
    'FR0_CANONICAL_STYLE',
  );
  assert.equal(
    acceptance.exact_source.find(({ path:sourcePath }) => sourcePath.endsWith('ExhibitionsPersonalSurface.astro'))?.owner,
    'A0_CONSUMER',
  );
});

test('source acceptance preserves one framing owner and accepted medallion semantics', () => {
  const source = acceptance.source_acceptance;
  assert.equal(source.root_identity, 'PASS_BY_SOURCE_REVIEW');
  assert.equal(source.canonical_row_reuse, 'PASS_BY_SOURCE_REVIEW');
  assert.equal(source.framing_owner, 'PASS_BY_SOURCE_REVIEW');
  assert.equal(source.fit_and_focal_protocol, 'PASS_BY_SOURCE_REVIEW');
  assert.equal(source.resource_state_protocol, 'PASS_BY_SOURCE_REVIEW');
  assert.equal(source.non_interactive_medallion_owner_none, 'ACCEPTED_PROTOCOL');
  assert.equal(source.competing_route_local_attribute_scoped_MediaFrame_owner, 'NOT_FOUND_BY_SOURCE_REVIEW');
});

test('V0 matrix cannot omit route, resource-state, clipping, interaction or accessibility evidence', () => {
  const required = acceptance.v0_required_sections;
  assert.deepEqual(required.viewports, [375, 620, 1024, 1440]);
  for (const section of [
    'route_and_document',
    'frame_protocol',
    'clipping_observation',
    'resource_state_observation',
    'interaction',
    'row_and_accessibility',
  ]) assert.ok(required[section].length > 0, `missing V0 section ${section}`);
  assert.ok(required.resource_state_observation.includes('resource_state pending|loaded|fallback|broken'));
  assert.ok(required.interaction.includes('medallion is a non-interactive span with interaction-owner=none'));
  assert.ok(required.interaction.includes('missing/unknown interaction owner or MediaFrame-owned activation is DRIFT'));
  assert.deepEqual(required.clipping_observation, [
    'image_box_extends_beyond_frame boolean',
    'computed_frame_overflow_x hidden|clip|visible|other',
    'computed_frame_overflow_y hidden|clip|visible|other',
    'paint_or_hit_test_escapes_frame boolean',
    'clip_owner media-frame.css|other|absent',
  ]);
  assert.equal(
    required.clipping_classification.extended_box_with_canonical_clip_and_no_paint_or_hit_test_escape,
    'PASS_INTENTIONAL_CROP_OR_PARALLAX',
  );
  assert.equal(
    required.clipping_classification.extended_box_with_visible_or_absent_clip_or_observable_escape,
    'FR0_DRIFT_MEDIA_FRAME_IMAGE_ESCAPES_FRAME',
  );
  assert.equal(
    required.clipping_classification.contain_media_visible_content_clipped_or_document_area_suppressed,
    'FR0_DRIFT',
  );
  assert.equal(required.clipping_classification.raw_image_box_containment_is_sufficient, false);
  assert.equal(acceptance.acceptance_rule.omitted_required_case, 'INCOMPLETE_NOT_PASS');
  assert.equal(acceptance.acceptance_rule.unrelated_route_drift, 'DOES_NOT_REJECT_THIS_SLICE');
  assert.equal(acceptance.acceptance_rule.N0_may_not_self_issue_browser_verdict, true);
});

test('rollback preserves the immutable prefix and isolates the smallest slice units', () => {
  assert.equal(acceptance.rollback_boundary.preserve_frozen_immutable_prefix, true);
  assert.deepEqual(acceptance.rollback_boundary.smallest_units, [
    'exhibitions consumer materialization',
    'ExhibitionPrototypeRow protocol binding',
    'FR0 exhibitions MediaFrame batch',
  ]);
  assert.ok(acceptance.rollback_boundary.do_not_revert.includes('accepted festivals source'));
  assert.ok(acceptance.rollback_boundary.do_not_revert.includes('one canonical Kaggle generation path'));
});

test('strict integrated-source mode verifies the accepted chain without redefining FR0', async () => {
  if (!strictSource) return;
  const [surface, row, bridge, frameCss] = await Promise.all([
    read('src/components/ExhibitionsPersonalSurface.astro'),
    read('src/components/ExhibitionPrototypeRow.astro'),
    read('src/components/exhibitionsMediaFrameBridge.mjs'),
    read('src/components/media-frame.css'),
  ]);

  for (const marker of [
    "import './design-system/product-contour-foundations.css';",
    "import ExhibitionPrototypeRow from './ExhibitionPrototypeRow.astro';",
    'data-ds-family="ExhibitionsPersonalSurface"',
    'data-ds-variant="ranked-personal-catalog"',
    'data-ke-foundation-consumer="exhibitions-personal-surface"',
  ]) assert.ok(surface.includes(marker), `surface misses ${marker}`);
  assert.match(surface, /newItems\.map\(\(item, index\) => <ExhibitionPrototypeRow/u);
  assert.match(surface, /priorityItems\.map\(\(item, index\) => <ExhibitionPrototypeRow/u);
  assert.match(surface, /tailItems\.map\(\(item, index\) => <ExhibitionPrototypeRow/u);

  for (const marker of [
    "import './media-frame.css';",
    'data-media-frame-surface="exhibitions-deck"',
    'data-media-frame-surface="exhibitions-medallion"',
    'data-media-frame-interaction-owner="caller"',
    'data-media-frame-interaction-owner="none"',
    'data-media-frame-placeholder',
    'data-media-frame-fallback',
  ]) assert.ok(row.includes(marker), `row misses ${marker}`);

  for (const marker of [
    "const FRAME_STYLE_OWNER = 'media-frame.css';",
    "state === 'loading' ? 'pending'",
    "state === 'error' ? 'broken'",
    "state === 'depth' ? 'fallback'",
    "frame.dataset.mediaFrameKind = 'fallback'",
    "frame.dataset.mediaFrameFit = 'contain'",
    "frame.dataset.mediaFrameCropPermission = 'forbidden'",
    "publishFrame(media, 'exhibitions-gallery'",
    "interactionOwner: 'none'",
  ]) assert.ok(bridge.includes(marker), `bridge misses ${marker}`);

  for (const marker of [
    '[data-media-frame][data-media-frame-contract="v1"]',
    'overflow: hidden',
    'object-position: var(--media-frame-object-position, 50% 50%)',
    '[data-media-frame-fit="cover"] > [data-media-frame-image]',
    '[data-media-frame-fit="contain"] > [data-media-frame-image]',
    'data-media-frame-surface="exhibitions-deck"',
    'data-media-frame-surface="exhibitions-gallery"',
    'data-media-frame-surface="exhibitions-medallion"',
    'data-media-frame-resource-state="broken"',
  ]) assert.ok(frameCss.includes(marker), `canonical frame CSS misses ${marker}`);

  const localStyle = [...surface.matchAll(/<style\b[^>]*>([\s\S]*?)<\/style>/giu)]
    .map((match) => match[1])
    .join('\n');
  assert.doesNotMatch(
    localStyle,
    /\[data-media-frame[^{}]*\{[^}]*(?:object-fit|object-position|overflow|clip-path|border-radius)\s*:/iu,
    'surface recreates a competing attribute-scoped MediaFrame owner',
  );
});

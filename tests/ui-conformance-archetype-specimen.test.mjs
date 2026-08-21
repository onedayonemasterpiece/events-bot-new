import assert from 'node:assert/strict';
import test from 'node:test';
import { renderSpecimenPage } from '../scripts/current_ui_resource_graph/v1/specimens/materialize.mjs';
import {
  assertEventAssetsLocalized, resolveEventCardArchetypeContext, rewriteEventAssets, validateCapturedArchetypeEvidence,
} from '../scripts/ui_conformance/archetype-specimen.mjs';

const fixtureIds = ['event.real.7906', 'event.real.8156', 'event.real.6628', 'event.real.4327'];
function resolvedCase(overrides = {}) {
  return {
    component_id:'event.card', viewport_id:'desktop-1280', event_fixture_id:'event.real.8156',
    container_geometry:{ viewport_width:1280, viewport_height:900, container_width:1180, used_card_width:380, device_scale_factor:1 },
    consumer_layout_resolution:{
      schema_version:'consumer-layout-resolution.v1',
      surface:{ surface_id:'event-detail-related', archetype_id:'optimized-event-card-grid', parent_component:'OptimizedEventCardGrid', placement_claim:'controlled-layout-only' },
      viewport:{ viewport_id:'desktop-1280', width_px:1280, height_px:900, device_scale_factor:1 },
      container:{ content_width_px:1180 },
      grid:{ columns:3, column_gap_px:20, row_gap_px:20, align_items:'stretch' },
      packing:{ algorithm:'packRelatedCardRows', limit:4, row_size:3, ordered_input_fixture_ids:fixtureIds, ordered_output_fixture_ids:fixtureIds, placement:{ fixture_id:'event.real.8156', row_index:0, column_index:1, row_ratio:5/6, row_mode:'document-led-bounded-cover' } },
      row_policy:{ peer_fixture_ids:fixtureIds.slice(0,3), equal_height_policy:'equal-height-within-row', shared_media_ratio_within_row:true },
      used_card_geometry:{ width_px:380 },
    },
    ...overrides,
  };
}

test('v2 consumer layout resolves a controlled, non-ranking OptimizedEventCardGrid specimen', () => {
  const context=resolveEventCardArchetypeContext(resolvedCase());
  assert.equal(context.archetype_id,'optimized-event-card-grid');
  assert.equal(context.container_width,1180);
  assert.equal(context.expected_card_width,380);
  assert.deepEqual(context.input_fixture_ids,fixtureIds);
  assert.equal(context.ranking_supplied,false);
  assert.equal(context.production_route_placement_claimed,false);
  assert.equal(context.selected_card_selector,'[data-event-card][data-event-id="8156"]');
});

test('archetype resolver rejects a fabricated route/ranking placement claim and incomplete fixture binding', () => {
  const routeClaim=resolvedCase(); routeClaim.consumer_layout_resolution.surface.placement_claim='production-observed';
  assert.throws(()=>resolveEventCardArchetypeContext(routeClaim),/controlled-layout-only/u);
  const missing=resolvedCase(); missing.consumer_layout_resolution.packing.ordered_input_fixture_ids=['event.real.7906'];
  assert.throws(()=>resolveEventCardArchetypeContext(missing),/not bound/u);
  const invented=resolvedCase(); invented.consumer_layout_resolution.packing.ordered_output_fixture_ids=[...fixtureIds.slice(0,3),'event.real.9999'];
  assert.throws(()=>resolveEventCardArchetypeContext(invented),/exact permutation/u);
  const secondRenderer=resolvedCase(); secondRenderer.consumer_layout_resolution.packing.algorithm='localGridClone';
  assert.throws(()=>resolveEventCardArchetypeContext(secondRenderer),/packRelatedCardRows/u);
});

test('existing specimen renderer mounts the real OptimizedEventCardGrid without a local grid fork', () => {
  const context=resolveEventCardArchetypeContext(resolvedCase());
  const row={ id:'archetype', renderer:'optimized-event-card-grid', source_paths:['src/components/OptimizedEventCardGrid.astro'], props:{ limit:4,rowSize:3,mediaTreatment:'hybrid',surface:'event_detail_related' }, container:{width:1180} };
  const page=renderSpecimenPage(row,{ events:fixtureIds.map((id,index)=>({id:7906+index,title:id})), selectedEventId:7907, trace:{archetype:context} });
  assert.match(page,/OptimizedEventCardGrid\.astro/u);
  assert.match(page,/<Component events=\{events\} limit=\{4\} rowSize=\{3\}/u);
  assert.doesNotMatch(page,/grid-template-columns/u);
  assert.match(page,/"ranking_supplied":false/u);
});

test('asset localization rewrites primary and derivative src fields used by EventCard', () => {
  const remote='https://cdn.invalid/primary.webp'; const thumb='https://cdn.invalid/thumb.webp';
  const event={ image_url:remote, image_source_url:remote, image_assets:[{src:remote,source_url:remote,thumbnail_sources:[{src:thumb}]}] };
  rewriteEventAssets(event,new Map([[remote,'/__ui-assets/primary.webp'],[thumb,'/__ui-assets/thumb.webp']]));
  assert.equal(event.image_url,'/__ui-assets/primary.webp');
  assert.equal(event.image_assets[0].src,'/__ui-assets/primary.webp');
  assert.equal(event.image_assets[0].thumbnail_sources[0].src,'/__ui-assets/thumb.webp');
  assert.equal(assertEventAssetsLocalized(event),event);
  assert.throws(()=>assertEventAssetsLocalized({image_url:'https://cdn.invalid/unbound.webp'}),/escaped local asset binding/u);
});

test('capture validation proves 380px cards, explicit placement, shared ratio and CSS-grid equal height', () => {
  const context=resolveEventCardArchetypeContext(resolvedCase());
  const cards=fixtureIds.map((fixtureId,index)=>({
    event_id:Number(fixtureId.split('.').at(-1)), row_index:index<3?0:1, column_index:index%3,
    row_ratio:index<3?5/6:1.5, row_mode:index<3?'document-led-bounded-cover':'document-led-uncropped',
    rect:{x:(index%3)*400,y:index<3?0:700,width:380,height:index<3?680:500},
  }));
  const evidence={root:{width:1180,height:1200},grid:{display:'grid',column_count:3,column_gap:20,row_gap:20,align_items:'stretch'},ordered_event_ids:fixtureIds.map((value)=>Number(value.split('.').at(-1))),cards};
  assert.equal(validateCapturedArchetypeEvidence(evidence,context).status,'PASS');
  cards[1].rect.height=679;
  const failed=validateCapturedArchetypeEvidence(evidence,context);
  assert.equal(failed.status,'FAIL');
  assert.equal(failed.checks.peer_equal_height,false);
});

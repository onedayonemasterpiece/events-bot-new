const REQUIRED_ARCHETYPE = 'optimized-event-card-grid';

function finite(value, label) {
  const number = Number(value);
  if (!Number.isFinite(number)) throw new Error(`Invalid ${label}: ${value}`);
  return number;
}

function unique(values) {
  return [...new Set(values)];
}

/**
 * Resolve the parent context already bound into a v2 Golden render case.
 * This adapter deliberately does not invent a route, ranking result, or sibling
 * fixture set: all of those facts must arrive in consumer_layout_resolution.
 */
export function resolveEventCardArchetypeContext(resolvedCase) {
  const layout = resolvedCase?.consumer_layout_resolution;
  if (!layout) return null;
  if (resolvedCase.component_id !== 'event.card') throw new Error('Consumer archetype is only supported for event.card');
  if (layout.schema_version !== 'consumer-layout-resolution.v1') throw new Error(`Unsupported consumer layout schema: ${layout.schema_version}`);
  if (layout.surface?.archetype_id !== REQUIRED_ARCHETYPE || layout.surface?.parent_component !== 'OptimizedEventCardGrid') {
    throw new Error('Consumer layout is not the real OptimizedEventCardGrid archetype');
  }
  if (layout.surface?.placement_claim !== 'controlled-layout-only') {
    throw new Error('Archetype specimen must remain controlled-layout-only; route/ranking placement is not supplied');
  }
  if (layout.viewport?.viewport_id !== resolvedCase.viewport_id
    || finite(layout.viewport?.width_px, 'viewport width') !== finite(resolvedCase.container_geometry?.viewport_width, 'case viewport width')
    || finite(layout.viewport?.height_px, 'viewport height') !== finite(resolvedCase.container_geometry?.viewport_height, 'case viewport height')
    || finite(layout.viewport?.device_scale_factor, 'device scale factor') !== finite(resolvedCase.container_geometry?.device_scale_factor, 'case device scale factor')) {
    throw new Error('Consumer viewport does not match the resolved render case');
  }
  const containerWidth = finite(layout.container?.content_width_px, 'container width');
  if (containerWidth !== finite(resolvedCase.container_geometry?.container_width, 'case container width')) {
    throw new Error('Consumer container does not match the resolved render case');
  }
  const packing = layout.packing || {};
  if (packing.algorithm !== 'packRelatedCardRows') throw new Error('Consumer layout must use the production packRelatedCardRows algorithm');
  const inputFixtureIds = Array.isArray(packing.ordered_input_fixture_ids) ? packing.ordered_input_fixture_ids : [];
  const outputFixtureIds = Array.isArray(packing.ordered_output_fixture_ids) ? packing.ordered_output_fixture_ids : [];
  if (!inputFixtureIds.length || unique(inputFixtureIds).length !== inputFixtureIds.length) throw new Error('Exact ordered archetype input fixtures are required and must be unique');
  if (!outputFixtureIds.length || unique(outputFixtureIds).length !== outputFixtureIds.length) throw new Error('Exact ordered archetype output fixtures are required and must be unique');
  if (packing.placement?.fixture_id !== resolvedCase.event_fixture_id || !inputFixtureIds.includes(resolvedCase.event_fixture_id)) {
    throw new Error('Selected EventCard fixture is not bound to the archetype packing input/placement');
  }
  if (JSON.stringify([...inputFixtureIds].sort()) !== JSON.stringify([...outputFixtureIds].sort())) {
    throw new Error('Archetype output fixtures must be an exact permutation of the declared input fixtures');
  }
  if (finite(packing.limit, 'packing limit') !== outputFixtureIds.length) throw new Error('Packing limit must equal the declared output fixture count');
  if (finite(packing.row_size, 'packing row size') !== finite(layout.grid?.columns, 'grid columns')) throw new Error('Packing row size and CSS grid columns disagree');

  const expectedWidth = finite(layout.used_card_geometry?.width_px, 'used card width');
  if (Math.abs(expectedWidth - finite(resolvedCase.container_geometry?.used_card_width, 'case used card width')) > 1e-6) {
    throw new Error('Used card width does not match the resolved render case');
  }
  const columns = finite(layout.grid.columns, 'grid columns');
  const columnGap = finite(layout.grid.column_gap_px, 'column gap');
  if (Math.abs((columns * expectedWidth) + ((columns - 1) * columnGap) - containerWidth) > 1e-6) {
    throw new Error('Declared card width, column count and gaps do not fill the consumer container');
  }
  const peerFixtureIds = layout.row_policy?.peer_fixture_ids || [];
  const selectedOutputIndex = outputFixtureIds.indexOf(resolvedCase.event_fixture_id);
  const declaredRowIndex = finite(packing.placement.row_index, 'selected row index');
  const declaredColumnIndex = finite(packing.placement.column_index, 'selected column index');
  const selectedRowFixtures = outputFixtureIds.slice(declaredRowIndex * columns, (declaredRowIndex + 1) * columns);
  if (layout.grid.align_items !== 'stretch'
    || layout.row_policy?.equal_height_policy !== 'equal-height-within-row'
    || layout.row_policy?.shared_media_ratio_within_row !== true
    || peerFixtureIds.length !== columns
    || unique(peerFixtureIds).length !== columns
    || !peerFixtureIds.includes(resolvedCase.event_fixture_id)
    || peerFixtureIds.some((fixtureId) => !outputFixtureIds.includes(fixtureId))
    || JSON.stringify([...peerFixtureIds].sort()) !== JSON.stringify([...selectedRowFixtures].sort())
    || Math.floor(selectedOutputIndex / columns) !== declaredRowIndex
    || selectedOutputIndex % columns !== declaredColumnIndex) {
    throw new Error('Consumer row policy does not prove the selected CSS-grid equal-height row');
  }
  const selectedEventId = Number(String(resolvedCase.event_fixture_id).replace(/^event\.real\./u, ''));
  if (!Number.isSafeInteger(selectedEventId)) throw new Error('Selected fixture must use an event.real.<integer> identifier');
  return {
    schema_version: 'ui_conformance_archetype_specimen.v1',
    archetype_id: REQUIRED_ARCHETYPE,
    parent_component: 'OptimizedEventCardGrid',
    surface_id: layout.surface.surface_id,
    placement_claim: 'controlled-layout-only',
    production_route_placement_claimed: false,
    ranking_supplied: false,
    viewport: {
      width: finite(layout.viewport.width_px, 'viewport width'),
      height: finite(layout.viewport.height_px, 'viewport height'),
      device_scale_factor: finite(layout.viewport.device_scale_factor, 'device scale factor'),
    },
    container_width: containerWidth,
    columns,
    column_gap: columnGap,
    row_gap: finite(layout.grid.row_gap_px, 'row gap'),
    align_items: layout.grid.align_items,
    input_fixture_ids: [...inputFixtureIds],
    output_fixture_ids: [...outputFixtureIds],
    selected_fixture_id: resolvedCase.event_fixture_id,
    selected_event_id: selectedEventId,
    limit: finite(packing.limit, 'packing limit'),
    row_size: finite(packing.row_size, 'packing row size'),
    media_treatment: packing.media_treatment || 'hybrid',
    expected_card_width: expectedWidth,
    selected_placement: {
      row_index: declaredRowIndex,
      column_index: declaredColumnIndex,
      row_ratio: finite(packing.placement.row_ratio, 'selected row ratio'),
      row_mode: packing.placement.row_mode,
    },
    peer_fixture_ids: [...peerFixtureIds],
    equal_height_policy: layout.row_policy?.equal_height_policy,
    shared_media_ratio_within_row: layout.row_policy?.shared_media_ratio_within_row === true,
    parent_selector: '[data-optimized-event-card-grid]',
    selected_card_selector: `[data-event-card][data-event-id="${selectedEventId}"]`,
  };
}

export function rewriteEventAssets(event, localAssets) {
  const rewrite = (value) => (value && localAssets.has(value) ? localAssets.get(value) : value);
  event.image_url = rewrite(event.image_url);
  event.image_source_url = rewrite(event.image_source_url);
  for (const asset of event.image_assets || []) {
    asset.src = rewrite(asset.src);
    asset.url = rewrite(asset.url);
    asset.source_url = rewrite(asset.source_url);
    for (const derivative of asset.thumbnail_sources || []) {
      derivative.src = rewrite(derivative.src);
      derivative.url = rewrite(derivative.url);
    }
  }
  return event;
}

export function assertEventAssetsLocalized(event) {
  const urls = [event.image_url, event.image_source_url];
  for (const asset of event.image_assets || []) {
    urls.push(asset.src, asset.url, asset.source_url);
    for (const derivative of asset.thumbnail_sources || []) urls.push(derivative.src, derivative.url);
  }
  const remote = urls.filter((value) => typeof value === 'string' && /^https?:\/\//u.test(value));
  if (remote.length) throw new Error(`Golden fixture media escaped local asset binding: ${remote.join(', ')}`);
  return event;
}

export function validateCapturedArchetypeEvidence(evidence, archetype, tolerance = 0.15) {
  if (!evidence || !archetype) throw new Error('Captured archetype evidence and contract are required');
  const selected = evidence.cards.find((card) => card.event_id === archetype.selected_event_id);
  const peerIds = archetype.peer_fixture_ids.map((value) => Number(String(value).replace(/^event\.real\./u, '')));
  const peers = evidence.cards.filter((card) => peerIds.includes(card.event_id));
  const checks = {
    parent_width: Math.abs(evidence.root.width - archetype.container_width) <= tolerance,
    grid_display: evidence.grid.display === 'grid',
    grid_columns: evidence.grid.column_count === archetype.columns,
    grid_column_gap: Math.abs(evidence.grid.column_gap - archetype.column_gap) <= tolerance,
    grid_row_gap: Math.abs(evidence.grid.row_gap - archetype.row_gap) <= tolerance,
    align_items: evidence.grid.align_items === archetype.align_items,
    output_order: JSON.stringify(evidence.ordered_event_ids) === JSON.stringify(archetype.output_fixture_ids.map((value) => Number(String(value).replace(/^event\.real\./u, '')))),
    selected_present: Boolean(selected),
    selected_width: Boolean(selected) && Math.abs(selected.rect.width - archetype.expected_card_width) <= tolerance,
    selected_placement: Boolean(selected)
      && selected.row_index === archetype.selected_placement.row_index
      && selected.column_index === archetype.selected_placement.column_index,
    selected_row_ratio: Boolean(selected) && Math.abs(selected.row_ratio - archetype.selected_placement.row_ratio) <= 1e-5,
    selected_row_mode: Boolean(selected) && selected.row_mode === archetype.selected_placement.row_mode,
    peer_set_complete: peers.length === peerIds.length,
    peer_equal_height: peers.length > 0 && Math.max(...peers.map((card) => card.rect.height)) - Math.min(...peers.map((card) => card.rect.height)) <= tolerance,
    peer_shared_row_ratio: peers.length > 0 && Math.max(...peers.map((card) => card.row_ratio)) - Math.min(...peers.map((card) => card.row_ratio)) <= 1e-5,
  };
  return { status: Object.values(checks).every(Boolean) ? 'PASS' : 'FAIL', checks };
}

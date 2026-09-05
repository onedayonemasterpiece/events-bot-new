import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { REQUIRED_CAPSULES, SPECIMEN_SCHEMA } from './registry.mjs';
import { assertFixtureDelta } from './fixtures.mjs';

const SAFE_ID = /^[a-z0-9][a-z0-9-]{2,80}$/u;
const SAFE_SOURCE = /^src\/(?:components|lib|pages)\/[A-Za-z0-9_./\[\]-]+\.(?:astro|ts|mjs)$/u;
const SENSITIVE = /(?:authorization|bearer\s|password|client[_-]?secret|access[_-]?token|api[_-]?key|sb_(?:secret|publishable)|https?:\/\/|\/_review\/)/iu;

export function stableHash(value) {
  const stable = (item) => Array.isArray(item) ? item.map(stable)
    : item && typeof item === 'object' ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, stable(item[key])])) : item;
  return createHash('sha256').update(JSON.stringify(stable(value))).digest('hex');
}

function assertSafeTree(value, path = '$') {
  if (typeof value === 'string' && SENSITIVE.test(value)) throw new Error(`Sensitive value forbidden at ${path}`);
  if (!value || typeof value !== 'object') return;
  for (const [key, child] of Object.entries(value)) {
    if (/^(?:html|innerhtml|outerhtml|href|src|url)$/iu.test(key)) throw new Error(`Unsafe evidence field forbidden: ${path}.${key}`);
    assertSafeTree(child, `${path}.${key}`);
  }
}

export function assertSpecimenRegistry(registry) {
  if (registry.schema_version !== SPECIMEN_SCHEMA) throw new Error('Unexpected specimen schema');
  const all = [...registry.controlled_specimens, ...registry.real_route_verifications, ...registry.source_model_only_cases];
  const ids = new Set();
  for (const row of all) {
    if (!SAFE_ID.test(row.id) || ids.has(row.id)) throw new Error(`Invalid or duplicate specimen id: ${row.id}`);
    ids.add(row.id);
    if (row.production_state_claimed !== false) throw new Error(`Unreviewed production claim forbidden: ${row.id}`);
    if (!Array.isArray(row.capsule_ids) && row.route_kind !== undefined) throw new Error(`Missing capsule refs: ${row.id}`);
  }
  for (const row of registry.controlled_specimens) {
    if (row.route_kind !== 'controlled-specimen' || row.claim_scope !== 'controlled-candidate-source-render-only') throw new Error(`Fake route claim: ${row.id}`);
    if (!row.root_selector || !row.renderer || !Array.isArray(row.expected_markers)) throw new Error(`Incomplete controlled specimen: ${row.id}`);
    if (row.fixture_ref) assertFixtureDelta(row.fixture_delta);
    for (const path of [...row.source_paths, ...row.consumer_paths]) if (!SAFE_SOURCE.test(path)) throw new Error(`Unsafe source binding: ${path}`);
  }
  for (const row of registry.real_route_verifications) {
    if (row.route_kind !== 'exact-real-route-verification' || !row.route_template.startsWith('/')) throw new Error(`Invalid real-route binding: ${row.id}`);
    if (!row.contexts?.length || !row.selectors?.length || !row.source_paths?.length) throw new Error(`Incomplete real-route evidence plan: ${row.id}`);
    for (const context of row.contexts) if (!['mobile', 'desktop'].includes(context.name) || !Number.isInteger(context.viewport?.width) || !Number.isInteger(context.viewport?.height)) throw new Error(`Invalid real-route context: ${row.id}`);
    for (const path of row.source_paths) if (!SAFE_SOURCE.test(path)) throw new Error(`Unsafe real-route source binding: ${path}`);
  }
  for (const row of registry.source_model_only_cases) {
    if (row.reachability !== 'source-model-only' || row.production_state_claimed !== false || !row.reason || !row.source_paths?.length) throw new Error(`Incomplete source-model-only case: ${row.id}`);
  }
  const covered = new Set(all.flatMap((row) => row.capsule_ids || []));
  const missing = REQUIRED_CAPSULES.filter((id) => !covered.has(id));
  if (missing.length) throw new Error(`Incomplete six-capsule coverage: ${missing.join(', ')}`);
  if (registry.controlled_specimens.length > 40) throw new Error('First-wave registry exceeds bounded pairwise budget');
  return true;
}

export function assertEvidencePacket(packet) {
  if (packet.schema_version !== SPECIMEN_SCHEMA || packet.evidence_status !== 'captured-not-reviewed') throw new Error('Invalid evidence packet');
  if (packet.production_state_claimed !== false || packet.proof_label !== 'controlled-specimen-browser-element') throw new Error('Fake production evidence claim');
  if (!/^[a-f0-9]{64}$/u.test(packet.screenshot?.sha256 || '') || !/^[a-f0-9]{16}$/u.test(packet.screenshot?.dhash || '')) throw new Error('Screenshot hashes missing');
  if (packet.dom?.full_html_retained !== false || packet.network?.raw_urls_retained !== false) throw new Error('Unbounded DOM/network evidence');
  assertSafeTree(packet);
  return true;
}

export function validateTraceIntegrity(registry, observations = [], pageVerifications = [], { requireComplete = false } = {}) {
  assertSpecimenRegistry(registry);
  const plans = new Set(registry.controlled_specimens.map((row) => row.id));
  const routes = new Set(registry.real_route_verifications.map((row) => row.id));
  for (const row of observations) if (!plans.has(row.specimen_id)) throw new Error(`Dangling specimen observation: ${row.specimen_id}`);
  for (const row of pageVerifications) {
    const routeRef = row.route_binding_id || row.route_binding_ref;
    if (!routes.has(routeRef)) throw new Error(`Dangling real-page verification: ${routeRef}`);
  }
  for (const row of observations) {
    if (!row.source_paths?.length || !row.plan_id || row.production_state_claimed !== false) throw new Error(`Incomplete specimen trace: ${row.specimen_id}`);
  }
  if (requireComplete) {
    const observedPlans = new Set(observations.map((row) => row.specimen_id));
    const verifiedRoutes = new Map();
    const explicitlyUnreachableRoutes = new Set();
    for (const row of pageVerifications) {
      if (row.status === 'explicit-unreachable') explicitlyUnreachableRoutes.add(row.route_binding_ref);
      if (!verifiedRoutes.has(row.route_binding_ref)) verifiedRoutes.set(row.route_binding_ref, new Set());
      verifiedRoutes.get(row.route_binding_ref).add(row.context_name);
    }
    for (const row of registry.controlled_specimens) if (!observedPlans.has(row.id)) throw new Error(`Missing required controlled observation: ${row.id}`);
    for (const row of registry.real_route_verifications) {
      if (explicitlyUnreachableRoutes.has(row.id)) continue;
      const contexts = verifiedRoutes.get(row.id) || new Set();
      for (const context of row.contexts) if (!contexts.has(context.name)) throw new Error(`Missing required real-route context: ${row.id}/${context.name}`);
    }
    for (const row of registry.source_model_only_cases) if (row.reachability !== 'source-model-only' || row.production_state_claimed !== false) throw new Error(`Unclassified unreachable case: ${row.id}`);
  }
  return true;
}

/** Validate export structure, never certify an unmaterialized native Penpot file. */
export function assertFreeCollectionStructuralProjection(record, { expectedSha, expectedEventIds, repoRoot, structuralOnly = false }) {
  const fail = (message) => { throw new Error(`Free collection projection: ${message}`); };
  const hash = (value) => /^[a-f0-9]{64}$/u.test(value || '');
  const p = record?.provenance;
  if (record?.schema !== 'current_ui_free_collection_structural_projection_v1') fail('schema');
  if (!/^[a-f0-9]{40}$/u.test(expectedSha || '') || p?.repo_sha !== expectedSha
      || p.manifest?.repo_sha !== expectedSha || !hash(p.manifest_sha256) || !hash(p.registry_sha256)
      || !p.snapshot?.id || !hash(p.snapshot?.sha256) || !Number.isFinite(Date.parse(p.reference_clock))) fail('source/data/clock provenance');
  if (record.viewport?.width !== 1440 && record.viewport?.width !== 390) fail('viewport');
  if (record.viewport?.height !== (record.viewport.width === 1440 ? 900 : 844)) fail('viewport height');
  if (expectedEventIds?.length !== 5 || JSON.stringify(record.event_ids) !== JSON.stringify(expectedEventIds)
      || record.sample_size !== 5 || record.catalog_total < record.sample_size
      || record.eligibility_filter !== 'confirmed-free') fail('sample event order/eligible catalog');
  const bindings = new Map((record.source_bindings || []).map((row) => [row.id, row]));
  if (!bindings.size) fail('source bindings');
  for (const binding of bindings.values()) if (!binding.path || !hash(binding.sha256)
      || !Number.isInteger(binding.version) || !Array.isArray(binding.styles)
      || binding.styles.some((style) => !style.path || !hash(style.sha256))) fail('source owner hash');
  if (!structuralOnly) {
    if (!repoRoot || p.registry_path !== 'site/src/design-system/astro-family-registry.v1.json') fail('exact source repository required');
    const source = (file) => execFileSync('git', ['show', `${expectedSha}:${file}`], { cwd: repoRoot });
    const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
    const registryBytes = source(p.registry_path);
    if (digest(registryBytes) !== p.registry_sha256) fail('registry content mismatch');
    const registry = JSON.parse(registryBytes.toString('utf8'));
    for (const binding of bindings.values()) {
      const family = registry.families.find((row) => row.id === binding.id);
      if (!family || family.astro_root !== binding.path || family.version !== binding.version
          || digest(source(binding.path)) !== binding.sha256
          || JSON.stringify(family.style_owners) !== JSON.stringify(binding.styles.map((row) => row.path))
          || binding.styles.some((row) => digest(source(row.path)) !== row.sha256)) fail('source binding content mismatch');
    }
  }
  const nodes = [];
  const seen = new Set();
  const visit = (node, parentId = null) => {
    if (!node || seen.has(node.stable_id) || node.stable_id !== `free-collection.${stableHash(node.anatomy_path).slice(0, 24)}`) fail('stable identity');
    if (node.parent_id !== parentId) fail('parent link');
    seen.add(node.stable_id); nodes.push(node);
    if (node.kind === 'text') { if (typeof node.text !== 'string') fail('text'); return; }
    if (node.kind !== 'element' || !node.computed || !node.attributes) fail('element anatomy');
    for (const key of ['x', 'y', 'width', 'height']) if (!Number.isFinite(node.bounds?.[key])) fail('geometry');
    if (node.bounds.width < 0 || node.bounds.height < 0) fail('negative geometry');
    if (node.containing_family && !bindings.has(node.containing_family)) fail('unresolved owner');
    if (node.identity) {
      const owner = bindings.get(node.identity.family);
      if (!owner || String(owner.version) !== node.identity.version || node.containing_family !== owner.id) fail('component version/owner');
    }
    if (node.svg && (!node.svg.markup || createHash('sha256').update(node.svg.markup).digest('hex') !== node.svg.sha256)) fail('SVG source');
    if (node.image?.natural_width > 0 && (!hash(node.image.asset_sha256)
        || record.assets?.[node.image.current_src || node.image.src]?.sha256 !== node.image.asset_sha256)) fail('image content binding');
    for (const child of node.children || []) visit(child, node.stable_id);
  };
  visit(record.tree);
  if (record.tree.identity?.family !== 'FreeCollectionSurface'
      || record.tree.identity?.variant !== 'standard-free-listing'
      || nodes.filter((node) => node.identity?.family === 'AdaptiveEventCardGrid').length !== 1) fail('ordinary listing composition identities');
  const cards = nodes.filter((node) => Object.hasOwn(node.attributes || {}, 'data-event-card'));
  if (cards.length !== record.sample_size || JSON.stringify(cards.map((node) => node.attributes['data-event-id'])) !== JSON.stringify(expectedEventIds)) fail('sample card corpus');
  const flatten = (node) => [node, ...(node.children || []).flatMap(flatten)];
  for (const card of cards) {
    if (card.identity?.family !== 'EventCard') fail('card identity');
    const parts = flatten(card);
    const frames = parts.filter((node) => Object.hasOwn(node.attributes || {}, 'data-media-frame'));
    if (!frames.length) fail('MediaFrame');
    for (const frame of frames) {
      const attrs = frame.attributes;
      if (attrs['data-media-frame-contract'] !== 'v1' || !['loaded', 'fallback', 'broken'].includes(attrs['data-media-frame-resource-state'])) fail('MediaFrame state');
      const loadedImages = flatten(frame).filter((node) => node.image?.natural_width > 0 && node.image?.natural_height > 0);
      if (attrs['data-media-frame-resource-state'] === 'loaded' && !loadedImages.length) fail('loaded media asset');
      if (attrs['data-media-frame-resource-state'] !== 'loaded' && !Object.hasOwn(attrs, 'data-media-frame-fallback')) fail('fallback anatomy');
    }
    for (const semantic of ['like', 'not_interested']) if (!parts.some((node) => node.attributes?.['data-feedback-action'] === semantic)) fail(`action ${semantic}`);
    if (!parts.some((node) => Object.hasOwn(node.attributes || {}, 'data-native-share'))) fail('share action');
    const calendars = parts.filter((node) => Object.hasOwn(node.attributes || {}, 'data-calendar-action'));
    if ((card.attributes['data-calendar-eligible'] === 'true') !== Boolean(calendars.length)
        || calendars.some((node) => !node.attributes.href)) fail('calendar eligibility/href');
  }
  return { valid: true, node_count: nodes.length, card_count: cards.length, penpot_round_trip: false };
}

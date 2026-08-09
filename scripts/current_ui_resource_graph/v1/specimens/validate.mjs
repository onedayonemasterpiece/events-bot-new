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

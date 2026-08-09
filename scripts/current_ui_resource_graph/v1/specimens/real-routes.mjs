import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  captureStableLocatorPng, collectBoundedElementFacts, safeCapturedValue,
} from './capture.mjs';
import { buildSpecimenRegistry, PINNED_CANDIDATE_SHA, SPECIMEN_SCHEMA } from './registry.mjs';
import { assertSpecimenRegistry, stableHash, validateTraceIntegrity } from './validate.mjs';

const sha = (value) => createHash('sha256').update(value).digest('hex');

export function safeManifestRelativeKey(key) {
  return typeof key === 'string' && key.length > 0 && Buffer.byteLength(key) <= 4096
    && !key.startsWith('/') && !key.includes('\\') && !/[\u0000-\u001f\u007f]/u.test(key)
    && key.split('/').every((part) => part && part !== '.' && part !== '..');
}

export function routeFromManifestKey(key) {
  if (!safeManifestRelativeKey(key)) throw new Error('Unsafe exact candidate manifest key');
  if (key === 'index.html') return '/';
  if (key.endsWith('/index.html')) return `/${key.slice(0, -11)}/`;
  return `/${key.replace(/\.html$/u, '')}`;
}

function keyFromRoute(route) {
  if (!/^\/(?:[A-Za-z0-9._~-]+\/)*$/u.test(route)) throw new Error('Real-route template did not resolve to a safe static route');
  return route === '/' ? 'index.html' : `${route.slice(1)}index.html`;
}

function manifestIndex(manifest, expectedSourceSha) {
  if (manifest?.schema_version !== 'static_secret_candidate_manifest_v1' || !Array.isArray(manifest.files)) throw new Error('Exact candidate manifest is required');
  if (manifest.repo_sha !== expectedSourceSha) throw new Error('Exact candidate manifest source SHA mismatch');
  const index = new Map();
  for (const file of manifest.files) {
    if (!safeManifestRelativeKey(file.key) || index.has(file.key)) throw new Error('Unsafe or duplicate exact candidate manifest key');
    if (!/^[a-f0-9]{64}$/u.test(file.sha256 || '') || !Number.isSafeInteger(file.size) || file.size < 0) throw new Error('Invalid exact candidate manifest file metadata');
    index.set(file.key, file);
  }
  return index;
}

function resolveRoute(binding, eventById) {
  if (binding.event_id === null) {
    if (binding.route_template.includes('{')) throw new Error(`Unresolved static route template: ${binding.id}`);
    return binding.route_template;
  }
  const event = eventById.get(binding.event_id);
  if (!event || typeof event.slug !== 'string' || !event.slug) return null;
  return binding.route_template.replace('{slug}', event.slug);
}

function unresolvedBinding(binding, reason, extra = {}) {
  return {
    schema_version: SPECIMEN_SCHEMA, id: `resolved-route.${sha(binding.id).slice(0, 16)}`,
    route_binding_id: binding.id, event_id: binding.event_id, relative_key: null, relative_key_sha256: null,
    manifest_file_sha256: null, manifest_file_bytes: null, runtime_observation_id: null, route_hash: null,
    page_family: null, selectors: [...binding.selectors], expected_absent_selectors: [...binding.expected_absent_selectors],
    contexts: binding.contexts.map((row) => structuredClone(row)), source_paths: [...binding.source_paths], capsule_ids: [...binding.capsule_ids],
    candidate_sha: PINNED_CANDIDATE_SHA, binding_status: 'explicit-unreachable', unreachable_reason: reason,
    full_url_retained: false, production_state_claimed: false, human_review_status: 'pending', state_equivalence: 'unreachable-no-equivalence-claim', normalization_allowed: false,
    ...extra,
  };
}

export function resolveExactRealRouteBindings({
  registry = buildSpecimenRegistry(), manifest, runtimeObservations, catalog,
}) {
  assertSpecimenRegistry(registry); const files = manifestIndex(manifest, registry.pinned_candidate_sha);
  if (!Array.isArray(runtimeObservations)) throw new Error('Exact candidate runtime observations are required');
  if (!Array.isArray(catalog?.events)) throw new Error('Exact PreviewEvent catalog is required');
  const events = new Map(catalog.events.map((event) => [event.id, event]));
  const runtimeByKey = new Map(runtimeObservations
    .filter((row) => row.plane === 'latest_checked_kaggle_candidate' && safeManifestRelativeKey(row.route_relative_path))
    .map((row) => [row.route_relative_path, row]));
  return registry.real_route_verifications.map((binding) => {
    const route = resolveRoute(binding, events);
    if (!route) return unresolvedBinding(binding, 'exact-candidate-preview-event-row-missing');
    const relativeKey = keyFromRoute(route);
    const file = files.get(relativeKey); const runtime = runtimeByKey.get(relativeKey);
    if (!file) return unresolvedBinding(binding, 'exact-candidate-manifest-relative-key-missing', { relative_key: relativeKey, relative_key_sha256: sha(relativeKey), event_slug_sha256: binding.event_id === null ? null : sha(events.get(binding.event_id).slug) });
    if (!runtime) return unresolvedBinding(binding, 'exact-candidate-runtime-observation-missing', { relative_key: relativeKey, relative_key_sha256: sha(relativeKey), manifest_file_sha256: file.sha256, manifest_file_bytes: file.size, event_slug_sha256: binding.event_id === null ? null : sha(events.get(binding.event_id).slug) });
    const routeHash = sha(route);
    if (runtime.route_hash !== routeHash) throw new Error(`Runtime route hash mismatch for binding: ${binding.id}`);
    if (runtime.content_fixture?.content_sha256 !== file.sha256) throw new Error(`Runtime content hash mismatch for binding: ${binding.id}`);
    return {
      schema_version: SPECIMEN_SCHEMA, id: `resolved-route.${sha(binding.id).slice(0, 16)}`,
      route_binding_id: binding.id, event_id: binding.event_id,
      event_slug_sha256: binding.event_id === null ? null : sha(events.get(binding.event_id).slug),
      relative_key: relativeKey, relative_key_sha256: sha(relativeKey), manifest_file_sha256: file.sha256,
      manifest_file_bytes: file.size, runtime_observation_id: runtime.id, route_hash: routeHash,
      page_family: runtime.page_family, selectors: [...binding.selectors],
      expected_absent_selectors: [...binding.expected_absent_selectors], contexts: binding.contexts.map((row) => structuredClone(row)),
      source_paths: [...binding.source_paths], capsule_ids: [...binding.capsule_ids],
      candidate_sha: registry.pinned_candidate_sha, binding_status: 'exact-manifest-and-runtime-bound',
      full_url_retained: false, production_state_claimed: false, human_review_status: 'pending',
      state_equivalence: 'not-reviewed', normalization_allowed: false,
    };
  });
}

function telemetryFor(page) {
  const facts = { console_counts: {}, console_hashes: [], resource_counts: {}, status_counts: {}, failed_count: 0 };
  page.on('console', (message) => { facts.console_counts[message.type()] = (facts.console_counts[message.type()] || 0) + 1; facts.console_hashes.push(sha(message.text())); });
  page.on('request', (request) => { const type = request.resourceType(); facts.resource_counts[type] = (facts.resource_counts[type] || 0) + 1; });
  page.on('response', (response) => { const status = String(response.status()); facts.status_counts[status] = (facts.status_counts[status] || 0) + 1; });
  page.on('requestfailed', () => { facts.failed_count += 1; });
  return facts;
}

function safeTarget(candidateBase, relativeKey) {
  let base;
  try { base = new URL(candidateBase.endsWith('/') ? candidateBase : `${candidateBase}/`); }
  catch { throw new Error('Unsafe candidate base'); }
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password) throw new Error('Unsafe candidate base');
  const encoded = relativeKey.split('/').map(encodeURIComponent).join('/'); const target = new URL(encoded, base);
  if (target.origin !== base.origin || !target.pathname.startsWith(base.pathname)) throw new Error('Candidate manifest key escapes configured base');
  return target;
}

async function visibleLocator(page, selector) {
  const candidates = page.locator(selector); const count = Math.min(await candidates.count(), 12);
  for (let index = 0; index < count; index += 1) if (await candidates.nth(index).isVisible()) return candidates.nth(index);
  return null;
}

export function assertRealRouteEvidencePacket(packet) {
  if (packet.schema_version !== SPECIMEN_SCHEMA || packet.production_state_claimed !== false || packet.human_review_status !== 'pending') throw new Error('Invalid real-route evidence boundary');
  if (packet.full_url_retained !== false || JSON.stringify(packet).match(/https?:\/\//iu)) throw new Error('Real-route evidence leaked a URL');
  if (packet.evidence_kind === 'element-capture') {
    if (!packet.screenshot?.perceptually_stable) throw new Error('Unstable exact real-route element capture');
    if (!/^[a-f0-9]{64}$/u.test(packet.screenshot.sha256) || !/^[a-f0-9]{16}$/u.test(packet.screenshot.dhash)) throw new Error('Real-route screenshot hashes missing');
    if (packet.dom.full_html_retained !== false || packet.network.raw_urls_retained !== false) throw new Error('Unbounded real-route evidence');
  } else if (packet.evidence_kind === 'expected-absence') {
    if (packet.observed_count !== 0 || packet.screenshot !== null) throw new Error('Invalid expected-absence evidence');
  } else if (packet.evidence_kind === 'explicit-unreachable') {
    if (packet.screenshot !== null || !packet.unreachable_reason || packet.context !== null) throw new Error('Invalid explicit-unreachable evidence');
  } else throw new Error('Unknown real-route evidence kind');
  return true;
}

export async function captureExactRealRoutes({
  browser, candidateBase, resolvedBindings, outputDir, imageComparator, maxElementsPerContext = 2,
}) {
  if (!Array.isArray(resolvedBindings) || !resolvedBindings.length) throw new Error('Resolved real-route bindings required');
  if (!Number.isInteger(maxElementsPerContext) || maxElementsPerContext < 1 || maxElementsPerContext > 3) throw new Error('Unsafe real-route element capture budget');
  mkdirSync(outputDir, { recursive: true }); const observations = [];
  for (const binding of resolvedBindings) {
    if (binding.binding_status === 'explicit-unreachable') {
      const packet = {
        schema_version: SPECIMEN_SCHEMA, id: `real-route-unreachable.${sha(binding.route_binding_id).slice(0, 16)}`,
        evidence_kind: 'explicit-unreachable', route_binding_id: binding.route_binding_id, resolved_route_id: binding.id,
        relative_key: binding.relative_key, relative_key_sha256: binding.relative_key_sha256, route_hash: null,
        runtime_observation_id: null, page_family: null, context: null, screenshot: null,
        source_paths: binding.source_paths, capsule_ids: binding.capsule_ids, unreachable_reason: binding.unreachable_reason,
        state_equivalence: 'unreachable-no-equivalence-claim', proof_label: 'exact-candidate-binding-unreachable',
        full_url_retained: false, production_state_claimed: false, human_review_status: 'pending', normalization_allowed: false,
      };
      assertRealRouteEvidencePacket(packet); observations.push(packet); continue;
    }
    for (const contextPlan of binding.contexts) {
    const context = await browser.newContext({ viewport: contextPlan.viewport, reducedMotion: 'reduce' }); const page = await context.newPage(); const telemetry = telemetryFor(page);
    try {
      const target = safeTarget(candidateBase, binding.relative_key);
      let response;
      try { response = await page.goto(target.href, { waitUntil: 'domcontentloaded', timeout: 30_000 }); } catch { throw new Error(`Exact route navigation failed for binding: ${binding.route_binding_id}`); }
      if (!response) throw new Error(`Exact route document response missing for binding: ${binding.route_binding_id}`);
      const documentBytes = await response.body();
      if (documentBytes.length !== binding.manifest_file_bytes || sha(documentBytes) !== binding.manifest_file_sha256) throw new Error(`Exact route document identity mismatch for binding: ${binding.route_binding_id}`);
      await page.waitForLoadState('networkidle', { timeout: 20_000 });
      await page.evaluate(() => document.fonts?.ready);
      await page.addStyleTag({ content: '*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important;scroll-behavior:auto!important}' });
      await page.evaluate(() => new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done))));
      for (const selector of binding.expected_absent_selectors) {
        const count = await page.locator(selector).count();
        const absence = {
          schema_version: SPECIMEN_SCHEMA, id: `real-route-absence.${sha(`${binding.route_binding_id}\0${contextPlan.name}\0${selector}`).slice(0, 16)}`,
          evidence_kind: 'expected-absence', route_binding_id: binding.route_binding_id, resolved_route_id: binding.id,
          relative_key: binding.relative_key, relative_key_sha256: binding.relative_key_sha256, route_hash: binding.route_hash,
          runtime_observation_id: binding.runtime_observation_id, page_family: binding.page_family, context: contextPlan,
          selector, observed_count: count, screenshot: null, source_paths: binding.source_paths, capsule_ids: binding.capsule_ids,
          state_equivalence: 'expected-absence-source-contract-not-reviewed', proof_label: 'exact-candidate-browser-absence-unreviewed',
          full_url_retained: false, production_state_claimed: false, human_review_status: 'pending', normalization_allowed: false,
        };
        if (count !== 0) throw new Error(`Expected-absence selector rendered for binding: ${binding.route_binding_id}`);
        assertRealRouteEvidencePacket(absence); observations.push(absence);
      }
      const selected = [];
      for (const selector of binding.selectors.filter((item) => !binding.expected_absent_selectors.includes(item))) {
        const locator = await visibleLocator(page, selector); if (locator) selected.push({ selector, locator });
        if (selected.length >= maxElementsPerContext) break;
      }
      if (!selected.length) throw new Error(`No visible evidence selector for binding: ${binding.route_binding_id}`);
      for (let index = 0; index < selected.length; index += 1) {
        const { selector, locator } = selected[index]; const name = `${binding.route_binding_id}-${contextPlan.name}-${index}.png`;
        const screenshot = await captureStableLocatorPng({ locator, path: join(outputDir, 'component-screenshots', name), imageComparator, label: `Exact real-route ${binding.route_binding_id}/${contextPlan.name}/${index}` });
        const facts = await collectBoundedElementFacts(locator, []);
        let aria; try { aria = safeCapturedValue(await locator.ariaSnapshot({ timeout: 3000 }), 6000); } catch (error) { aria = { unavailable: true, error_class: error.constructor?.name || 'Error' }; }
        const packet = {
          schema_version: SPECIMEN_SCHEMA, id: `real-route-observation.${sha(`${binding.route_binding_id}\0${contextPlan.name}\0${selector}\0${index}`).slice(0, 16)}`,
          evidence_kind: 'element-capture', route_binding_id: binding.route_binding_id, resolved_route_id: binding.id,
          relative_key: binding.relative_key, relative_key_sha256: binding.relative_key_sha256, route_hash: binding.route_hash,
          runtime_observation_id: binding.runtime_observation_id, page_family: binding.page_family, context: contextPlan,
          selector, source_paths: binding.source_paths, capsule_ids: binding.capsule_ids,
          dom: { tag: facts.tag, classes: facts.classes, attributes: facts.attributes, redacted_attribute_names: facts.redacted_attribute_names, child_count: facts.child_count, text_length: facts.text_length, text_sha256: facts.text_sha256, full_html_retained: false },
          accessibility: { aria_snapshot: aria, ...facts.state }, computed: facts.computed, geometry: facts.geometry,
          css_variables: facts.css_variables, pseudo: facts.pseudo, media: facts.media, media_queries: facts.media_queries,
          cascade: facts.cascade, loaded_fonts: facts.loaded_fonts,
          screenshot: { path: `component-screenshots/${name}`, ...screenshot },
          console: { counts: telemetry.console_counts, message_text_retained: false, message_hashes: telemetry.console_hashes.slice(0, 20) },
          network: { counts_by_resource_type: telemetry.resource_counts, response_status_counts: telemetry.status_counts, failed_count: telemetry.failed_count, raw_urls_retained: false },
          state_equivalence: 'exact-route-element-captured-equivalence-not-reviewed', proof_label: 'exact-candidate-browser-element-unreviewed',
          full_url_retained: false, production_state_claimed: false, human_review_status: 'pending', normalization_allowed: false,
        };
        assertRealRouteEvidencePacket(packet); observations.push(packet);
      }
    } finally { await context.close(); }
    }
  }
  writeFileSync(join(outputDir, 'real-route-observations.jsonl'), `${observations.map((row) => JSON.stringify(row)).join('\n')}\n`);
  return observations;
}

function sourceRefs(paths) {
  return [...new Set(paths)].sort().map((path) => ({ id: `source-ref.${sha(path).slice(0, 16)}`, logical_path: path, candidate_sha: PINNED_CANDIDATE_SHA }));
}

export function adaptEvidenceForSnapshot({
  registry = buildSpecimenRegistry(), specimenObservations = [], realRouteObservations = [], requireComplete = false,
}) {
  const controlledById = new Map(registry.controlled_specimens.map((row) => [row.id, row]));
  const routesById = new Map(registry.real_route_verifications.map((row) => [row.id, row]));
  const canonicalSpecimens = specimenObservations.map((row) => {
    const plan = controlledById.get(row.specimen_id); if (!plan) throw new Error(`Dangling controlled observation: ${row.specimen_id}`);
    const routeRefs = registry.real_route_verifications.filter((route) => route.capsule_ids.some((id) => plan.capsule_ids.includes(id))).map((route) => route.id).sort();
    return {
      ...row, id: `specimen-observation.${sha(`${row.specimen_id}\0${row.step}`).slice(0, 16)}`,
      source_refs: sourceRefs(plan.source_paths), route_binding_refs: routeRefs,
      trace_status: 'source-to-specimen-with-real-route-bindings-unreviewed', production_state_claimed: false,
      human_review_status: 'pending', state_equivalence: row.state_equivalence || 'not-reviewed', normalization_allowed: false,
    };
  });
  const pageVerifications = realRouteObservations.map((row) => {
    const binding = routesById.get(row.route_binding_id); if (!binding) throw new Error(`Dangling real-route observation: ${row.route_binding_id}`);
    return {
      id: `page-verification.${sha(row.id).slice(0, 16)}`, page_family: row.page_family, route_hash: row.route_hash,
      route_relative_key: row.relative_key, viewport: row.context?.viewport || null, context_name: row.context?.name || 'not-applicable',
      screenshot_path: row.screenshot?.path || null, evidence_kind: row.evidence_kind,
      screenshot: row.screenshot ? structuredClone(row.screenshot) : null,
      element_evidence: row.evidence_kind === 'element-capture' ? {
        dom: row.dom, accessibility: row.accessibility, computed: row.computed, geometry: row.geometry,
        css_variables: row.css_variables, pseudo: row.pseudo, media: row.media,
        media_queries: row.media_queries, cascade: row.cascade, loaded_fonts: row.loaded_fonts,
        console: row.console, network: row.network,
      } : row.evidence_kind === 'expected-absence' ? { selector: row.selector, observed_count: row.observed_count } : null,
      status: row.evidence_kind === 'expected-absence' ? 'expected-absence-confirmed-unreviewed'
        : row.evidence_kind === 'explicit-unreachable' ? 'explicit-unreachable' : 'component-captured-unreviewed',
      route_binding_id: row.route_binding_id, route_binding_ref: row.route_binding_id, runtime_observation_ref: row.runtime_observation_id,
      capsule_ids: [...binding.capsule_ids],
      source_refs: sourceRefs(binding.source_paths), evidence_record_ref: row.id,
      proof_label: row.proof_label, production_state_claimed: false, production_equivalence_claimed: false,
      evidence_status: row.evidence_kind === 'element-capture' ? 'captured-not-reviewed' : row.evidence_kind,
      review_status: 'pending-human-visual-review', human_review_status: 'pending', normalization_allowed: false,
    };
  });
  validateTraceIntegrity(registry, canonicalSpecimens, pageVerifications, { requireComplete });
  return {
    schema_version: SPECIMEN_SCHEMA, specimen_observations: canonicalSpecimens, page_verifications: pageVerifications,
    detached_adapter_sha256: stableHash({ canonicalSpecimens, pageVerifications }), human_review_status: 'pending', normalization_allowed: false,
  };
}

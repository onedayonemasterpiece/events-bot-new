import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { basename, join, relative, resolve } from 'node:path';
import { DISPOSITIONS, REACHABILITY, assertClassificationInvariants, classificationCounts, knownExceptions } from './classification.mjs';
import { COMPONENT_BREAKPOINT_CONTEXTS } from './evidence.mjs';

export const V1_SCHEMA = 'current_ui_component_decoder_v1';
export const COMPACT_REQUIRED = Object.freeze([
  'manifest.json', 'receipt.json', 'summary.md', 'artifact-index.json', 'source-files.jsonl',
  'source-bindings.jsonl', 'component-families.jsonl', 'composition-edges.jsonl', 'consumers.jsonl',
  'route-families.jsonl', 'page-state-signatures.jsonl', 'specimen-plan.jsonl',
  'specimen-observations.jsonl', 'page-verification.jsonl', 'mismatches.jsonl', 'unresolved.jsonl',
  'penpot-materialization-candidates.json',
]);

function stableObject(value) {
  if (Array.isArray(value)) return value.map(stableObject);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map((key) => [key, stableObject(value[key])]));
  return value;
}
function json(value, pretty = false) { return `${JSON.stringify(stableObject(value), null, pretty ? 2 : 0)}\n`; }
function sha(value) { return createHash('sha256').update(value).digest('hex'); }
function safeSnapshotId(value) {
  if (!/^[a-z0-9][a-z0-9._-]{4,100}$/iu.test(value)) throw new Error('Unsafe v1 snapshot id');
  return value;
}
function claimWrite(path, content, budget) {
  const bytes = Buffer.from(content); budget.claim(bytes.length, basename(path)); writeFileSync(path, bytes);
}
function writeJsonl(path, rows, budget) {
  claimWrite(path, rows.map((row) => json(row)).join(''), budget);
}
function files(root) {
  const output = [];
  function visit(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) visit(path); else if (entry.isFile()) output.push(path);
    }
  }
  visit(root); return output;
}

export function v1SnapshotRoot(output, snapshotId) {
  return join(resolve(output), 'catalog', 'component-decoder', safeSnapshotId(snapshotId));
}

export function initializeV1Receipt(output, snapshotId, snapshotTime) {
  const root = v1SnapshotRoot(output, snapshotId); mkdirSync(root, { recursive: true });
  writeFileSync(join(root, 'receipt.json'), json({ schema_version: V1_SCHEMA, status: 'started', handoff_status: 'NO_GO', snapshot_id: snapshotId, snapshot_time: snapshotTime }, true));
  return root;
}

function sourceFiles(sourceRecords) {
  return sourceRecords.map((record) => ({
    id: record.id, plane: record.plane, path: record.path, type: record.type, name: record.name,
    content_sha256: record.content_sha256, parser: record.evidence?.parser, parser_status: record.evidence?.parser_status,
    state_parser_status: record.source_state?.parser_status || 'not_available', source_line_count: record.evidence?.source_line_count,
  })).sort((a, b) => a.path.localeCompare(b.path) || a.plane.localeCompare(b.plane));
}

function sourceBindings(components, styles) {
  const bySource = new Map();
  for (const style of styles) if (style.source_id) {
    if (!bySource.has(style.source_id)) bySource.set(style.source_id, []);
    bySource.get(style.source_id).push(style.id);
  }
  return components.flatMap((component) => component.plane_bindings.map((binding) => ({
    id: `binding.${sha(`${component.id}\0${binding.plane}`).slice(0, 16)}`, component_id: component.id,
    logical_path: component.logical_path, ...binding,
    style_observation_ids: (bySource.get(binding.source_id) || []).sort(),
    style_provenance: (bySource.get(binding.source_id) || []).length ? 'postcss-source-ast-plane-scoped' : 'no-component-local-style-observation',
  }))).sort((a, b) => a.logical_path.localeCompare(b.logical_path) || a.plane.localeCompare(b.plane));
}

function edges(sourceRecords) {
  return sourceRecords.flatMap((source) => (source.direct_dependencies || []).map((target) => ({
    id: `edge.${sha(`${source.id}\0${target}`).slice(0, 16)}`, plane: source.plane,
    consumer_source_id: source.id, dependency_source_id: target, edge_kind: 'static-import', proof_label: 'source-ast-import-graph',
  }))).sort((a, b) => a.id.localeCompare(b.id));
}

function consumers(sourceRecords) {
  return sourceRecords.filter((record) => record.type === 'component').map((record) => ({
    id: `consumer-set.${sha(record.id).slice(0, 16)}`, source_id: record.id, plane: record.plane,
    logical_path: record.path, direct_consumers: record.consumers || [], consumer_count: (record.consumers || []).length,
    proof_label: 'source-ast-import-graph',
  })).sort((a, b) => a.logical_path.localeCompare(b.logical_path) || a.plane.localeCompare(b.plane));
}

function stateSignatures(runtime) {
  return runtime.map((item) => ({
    id: `page-state.${sha(item.id).slice(0, 16)}`, plane: item.plane, page_family: item.page_family,
    route_hash: item.route_hash, structure_hash: item.structure_hash,
    source_mapping: item.source_mapping, source_page_ids: item.source_page_ids,
    surface_markers: item.surface_markers || [], event_resources: item.event_resources || {},
    proof_label: item.plane === 'current_root_prelaunch' ? 'public-root-runtime-observed' : 'exact-candidate-runtime-observed',
  })).sort((a, b) => a.plane.localeCompare(b.plane) || a.page_family.localeCompare(b.page_family) || a.route_hash.localeCompare(b.route_hash));
}

function routeFamilies(pageFamilies) {
  return pageFamilies.map((item) => ({
    ...item,
    surface_classification: item.id === 'page-family.labs-preview-special' ? 'lab-only' : 'current-as-is-route-family',
    production_baseline: item.id === 'page-family.labs-preview-special' ? 'excluded-intentional-desktop-only-lab' : 'requires-plane-specific-observation',
    normalization_allowed: false,
  }));
}

function specimenPlan(components) {
  return components.map((component) => ({
    id: `specimen-plan.${component.id.slice('component.'.length)}`, component_id: component.id,
    logical_path: component.logical_path,
    plan_status: ['lab-only', 'experiment-only', 'needs-verification'].includes(component.disposition) ? 'controlled-specimen-required' :
      component.reachability === 'production-observed' ? 'representative-real-page-verification-required' : 'route-or-controlled-specimen-required',
    required_contexts: component.logical_path.includes('transport/') || /TransportSchedule/u.test(component.logical_path) ? ['390', '420', '540', '700', '720', '1728'] : ['390', '1728'],
    evidence_claim_limit: component.disposition === 'experiment-only' ? 'experiment-off-source-only' : 'as-is-only',
    normalization_allowed: false,
  }));
}

function pageVerification(screenshots, componentEvidence) {
  const rows = screenshots.map((item) => ({
    id: `page-verification.${sha(item.id).slice(0, 16)}`, page_family: item.page_family,
    route_hash: item.route_hash || null, viewport: item.viewport || null, screenshot_path: item.screenshot_path,
    status: item.viewport_status === 'not_captured' ? 'not-captured' : 'page-captured',
    proof_label: item.source === 'exact_candidate_browser' ? 'exact-candidate-page-browser' : 'index-only-no-browser-proof',
  }));
  for (const item of componentEvidence) rows.push({
    id: `page-verification.${sha(item.id).slice(0, 16)}`, page_family: item.page_family, route_hash: item.route_hash,
    viewport: item.viewport, screenshot_path: item.screenshot_path, component_evidence_id: item.id,
    status: item.component_binding ? 'component-captured-and-bound' : 'component-captured-binding-unresolved', proof_label: item.proof_label,
  });
  return rows.sort((a, b) => a.id.localeCompare(b.id));
}

function unresolvedRows(components, coverage) {
  const rows = components.filter((item) => item.disposition === 'needs-verification').map((item) => ({
    id: `unresolved.${item.id.slice('component.'.length)}`, kind: 'component-classification', logical_path: item.logical_path,
    reason: item.reachability_basis, blocks_handoff: true, normalization_allowed: false,
  }));
  for (const item of knownExceptions().filter((entry) => entry.current_status === 'not-observed')) rows.push({
    id: `unresolved.${item.id.slice('exception.'.length)}`, kind: 'absent-as-is-future-requirement',
    reason: item.id, blocks_handoff: false, synthesis_allowed: false, normalization_allowed: false,
  });
  for (const item of coverage.filter((entry) => ['MISSING', 'AMBIGUOUS'].includes(entry.status))) if (!rows.some((row) => row.id.endsWith(item.id))) rows.push({
    id: `unresolved.coverage-${item.id}`, kind: 'coverage', reason: item.note, status: item.status,
    blocks_handoff: item.status === 'AMBIGUOUS', normalization_allowed: false,
  });
  return rows.sort((a, b) => a.id.localeCompare(b.id));
}

function mismatchRows(components) {
  return components.filter((item) => item.disposition === 'needs-verification').map((item) => ({
    id: `mismatch.${item.id.slice('component.'.length)}`, component_id: item.id, logical_path: item.logical_path,
    channels: ['source-consumer-graph', 'exact-runtime-mapping'], conclusion: 'requires-additional-verification',
    decision: 'NOT_MERGED', normalization_allowed: false,
  }));
}

function evaluateGates({ components, componentEvidence, specimenObservations, candidateContracts, capsules, canonical }) {
  const counts = classificationCounts(components);
  const gates = {
    logical_component_classification: canonical ? components.length === 107 : components.length > 0,
    closed_classification_enums: components.every((item) => DISPOSITIONS.includes(item.disposition) && REACHABILITY.includes(item.reachability)),
    state_aware_source_records: components.every((item) => Object.values(item.source_state_by_plane).some((state) => state && ['parsed', 'empty'].includes(state.parser_status))),
    component_scoped_browser_evidence: componentEvidence.some((item) => item.component_binding),
    controlled_specimen_evidence: specimenObservations.length > 0,
    candidate_as_is_contracts: candidateContracts.length > 0,
    reconciliation_capsules: capsules.length >= 6,
    source_to_specimen_to_real_page_trace: false,
    normalization_stop_preserved: true,
    plane_identity_preserved: components.every((item) => item.plane_bindings.every((binding) => binding.plane)),
  };
  return { status: Object.values(gates).every(Boolean) ? 'GO' : 'NO_GO', gates, blockers: Object.entries(gates).filter(([, passed]) => !passed).map(([name]) => name), counts };
}

export function writeV1Snapshot({
  output, snapshotId, snapshotTime, identity, sourceRecords, components, families, pageFamilies, runtime,
  screenshots, viewportEvidence, componentEvidence = [], coverage = [], budget,
  styles = [],
  specimenPlanRows = null, specimenObservations = [], candidateContracts = [], capsules = [],
}) {
  const root = initializeV1Receipt(output, snapshotId, snapshotTime);
  const canonical = identity.latest_checked_kaggle_candidate.source_sha === 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
  assertClassificationInvariants(components, { canonical });
  const dirs = ['components', 'candidate-contracts', 'conformance-capsules'];
  for (const dir of dirs) mkdirSync(join(root, dir), { recursive: true });
  writeJsonl(join(root, 'source-files.jsonl'), sourceFiles(sourceRecords), budget);
  writeJsonl(join(root, 'source-bindings.jsonl'), sourceBindings(components, styles), budget);
  writeJsonl(join(root, 'component-families.jsonl'), families.map((item) => ({ ...item, decision: 'NOT_MERGED', normalization_allowed: false })), budget);
  for (const component of components) claimWrite(join(root, 'components', `${component.id}.json`), json(component, true), budget);
  writeJsonl(join(root, 'composition-edges.jsonl'), edges(sourceRecords), budget);
  writeJsonl(join(root, 'consumers.jsonl'), consumers(sourceRecords), budget);
  writeJsonl(join(root, 'route-families.jsonl'), routeFamilies(pageFamilies), budget);
  writeJsonl(join(root, 'page-state-signatures.jsonl'), stateSignatures(runtime), budget);
  const plans = specimenPlanRows || specimenPlan(components);
  writeJsonl(join(root, 'specimen-plan.jsonl'), plans, budget);
  writeJsonl(join(root, 'specimen-observations.jsonl'), specimenObservations, budget);
  writeJsonl(join(root, 'page-verification.jsonl'), pageVerification(screenshots, componentEvidence), budget);
  const mismatches = mismatchRows(components); const unresolved = unresolvedRows(components, coverage);
  writeJsonl(join(root, 'mismatches.jsonl'), mismatches, budget);
  writeJsonl(join(root, 'unresolved.jsonl'), unresolved, budget);
  for (const contract of candidateContracts) claimWrite(join(root, 'candidate-contracts', `${contract.id}.contract.json`), json(contract, true), budget);
  if (!candidateContracts.length) claimWrite(join(root, 'candidate-contracts', 'README.md'), '# Candidate AS-IS contracts\n\nPending reconciliation; no normative TO-BE contract is asserted.\n', budget);
  for (const capsule of capsules) claimWrite(join(root, 'conformance-capsules', `${capsule.id}.json`), json(capsule, true), budget);
  if (!capsules.length) claimWrite(join(root, 'conformance-capsules', 'README.md'), '# Reconciliation capsules\n\nPending source/specimen/consumer reconciliation.\n', budget);
  claimWrite(join(root, 'penpot-materialization-candidates.json'), json({ schema_version: V1_SCHEMA, status: 'not-materialized', candidates: [], stop_reason: 'normalization-and-Penpot-materialization-are-out-of-scope' }, true), budget);
  const gate = evaluateGates({ components, componentEvidence, specimenObservations, candidateContracts, capsules, canonical });
  const summary = `# Current UI Decoder v1 — evidence completion\n\n` +
    `- Snapshot: \`${snapshotId}\`\n- Logical components: ${components.length}${canonical ? '/107' : ''}\n` +
    `- Dispositions: ${JSON.stringify(gate.counts.dispositions)}\n- Reachability: ${JSON.stringify(gate.counts.reachability)}\n` +
    `- Handoff gate: **${gate.status}**\n- Blockers: ${gate.blockers.join(', ') || 'none'}\n\n` +
    `This is an immutable AS-IS evidence snapshot. It does not authorize merge, split, normalization, tokenization, Penpot mutation, or Astro/CSS changes.\n`;
  claimWrite(join(root, 'summary.md'), summary, budget);
  const excluded = new Set(['artifact-index.json', 'manifest.json', 'receipt.json']);
  const indexed = files(root).filter((path) => !excluded.has(relative(root, path))).map((path) => {
    const bytes = readFileSync(path); return { path: relative(root, path), bytes: bytes.length, sha256: sha(bytes), storage: 'compact-git-snapshot' };
  });
  indexed.push({ path: 'manifest.json', storage: 'compact-git-snapshot', status: 'written-after-artifact-index', digest_source: 'receipt.json' });
  indexed.push({ path: 'receipt.json', storage: 'compact-git-snapshot', status: 'written-after-manifest', digest_source: 'not-self-indexed' });
  indexed.push({ path: 'artifact-index.json', storage: 'compact-git-snapshot', status: 'self-index-not-hashed', digest_source: 'manifest.json' });
  indexed.push({ path: '../../../screenshots/', storage: 'actions-heavy-artifact', status: existsSync(join(output, 'screenshots')) ? 'present' : 'not-captured' });
  indexed.push({ path: '../../../component-screenshots/', storage: 'actions-heavy-artifact', status: existsSync(join(output, 'component-screenshots')) ? 'present' : 'not-captured' });
  indexed.push({ path: '../../../component-evidence.jsonl', storage: 'actions-heavy-artifact', status: existsSync(join(output, 'component-evidence.jsonl')) ? 'present' : 'not-captured' });
  claimWrite(join(root, 'artifact-index.json'), json({ schema_version: V1_SCHEMA, snapshot_id: snapshotId, entries: indexed }, true), budget);
  const manifestOutputs = files(root).filter((path) => !['manifest.json', 'receipt.json'].includes(relative(root, path))).map((path) => {
    const bytes = readFileSync(path); return [relative(root, path), { bytes: bytes.length, sha256: sha(bytes) }];
  });
  const manifest = {
    schema_version: V1_SCHEMA, snapshot_id: snapshotId, snapshot_time: snapshotTime,
    identity_planes: identity, classification: gate.counts, go_no_go: gate,
    evidence: { component_observation_count: componentEvidence.length, specimen_observation_count: specimenObservations.length, viewport_observation_count: viewportEvidence.length, source_style_observation_count: styles.length, style_provenance: ['postcss-source-ast-plane-scoped', 'browser-computed-element-plane-scoped'], requested_component_breakpoints: COMPONENT_BREAKPOINT_CONTEXTS },
    known_exceptions: knownExceptions(), outputs: Object.fromEntries(manifestOutputs),
    constraints: { as_is_only: true, candidate_contracts_are_not_normative: true, merge: false, split: false, normalization: false, tokenization: false, penpot_mutation: false, astro_css_mutation: false, full_html_retained: false, bearer_url_retained: false },
  };
  claimWrite(join(root, 'manifest.json'), json(manifest, true), budget);
  assertV1SnapshotInvariants(root, { canonical });
  const receipt = { schema_version: V1_SCHEMA, status: 'complete', evidence_completion: gate.status === 'GO' ? 'complete' : 'partial', handoff_status: gate.status, snapshot_id: snapshotId, snapshot_time: snapshotTime, manifest_sha256: sha(readFileSync(join(root, 'manifest.json'))), blockers: gate.blockers };
  writeFileSync(join(root, 'receipt.json'), json(receipt, true));
  return { root, manifest, receipt };
}

export function assertV1SnapshotInvariants(root, { canonical = false } = {}) {
  for (const name of COMPACT_REQUIRED) if (!existsSync(join(root, name))) throw new Error(`Required compact v1 output missing: ${name}`);
  const componentFiles = files(join(root, 'components')).filter((path) => path.endsWith('.json'));
  if (canonical && componentFiles.length !== 107) throw new Error(`Compact v1 components must contain 107 records; observed ${componentFiles.length}`);
  for (const path of componentFiles) {
    const row = JSON.parse(readFileSync(path, 'utf8'));
    if (!DISPOSITIONS.includes(row.disposition) || !REACHABILITY.includes(row.reachability)) throw new Error(`Invalid closed classification enum: ${relative(root, path)}`);
    if (row.decision !== 'NOT_MERGED' || row.recommendation !== 'unresolved') throw new Error(`Normalization STOP violated: ${relative(root, path)}`);
  }
  const manifest = JSON.parse(readFileSync(join(root, 'manifest.json'), 'utf8'));
  if (manifest.constraints.normalization !== false || manifest.constraints.astro_css_mutation !== false) throw new Error('Compact v1 normalization STOP missing');
  if (manifest.go_no_go.status === 'GO' && manifest.go_no_go.blockers.length) throw new Error('GO receipt contains blockers');
}

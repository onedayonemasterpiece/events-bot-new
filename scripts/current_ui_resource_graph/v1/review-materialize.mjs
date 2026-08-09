#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateCompactSnapshot } from './validate-snapshot.mjs';

const SCHEMA = 'current_ui_decoder_human_review_v1';
const CAPSULES = [
  'capsule.01-event-presentation-states', 'capsule.02-button-cta', 'capsule.03-media-heavy',
  'capsule.04-transport', 'capsule.05-medallions', 'capsule.06-artifacts',
];
const TRACE_KINDS = new Set(['state-equivalent', 'consumer-exists-only', 'lab-source-only']);
const sha = (value) => createHash('sha256').update(value).digest('hex');
const stable = (value) => Array.isArray(value) ? value.map(stable) : value && typeof value === 'object'
  ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, stable(value[key])])) : value;
const json = (value, pretty = false) => `${JSON.stringify(stable(value), null, pretty ? 2 : 0)}\n`;
const readJson = (path) => JSON.parse(readFileSync(path, 'utf8'));
const readJsonl = (path) => readFileSync(path, 'utf8').split('\n').filter(Boolean).map(JSON.parse);
const writeJsonl = (path, rows) => writeFileSync(path, rows.map((row) => json(row)).join(''));

function files(root) {
  const rows = [];
  for (const entry of readdirSync(root, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) rows.push(...files(path)); else if (entry.isFile()) rows.push(path);
  }
  return rows;
}

function assertDigest(value, label) {
  if (!/^sha256:[a-f0-9]{64}$/u.test(value || '')) throw new Error(`${label} must be sha256:<64 hex>`);
}

function verifyRaster(outputRoot, screenshot) {
  if (!screenshot?.path || !/^[a-z0-9._/-]+$/iu.test(screenshot.path) || screenshot.path.includes('..')) throw new Error('Unsafe reviewed raster path');
  const path = resolve(outputRoot, screenshot.path);
  if (!path.startsWith(`${resolve(outputRoot)}/`) || !existsSync(path)) throw new Error(`Reviewed raster missing: ${screenshot.path}`);
  if (sha(readFileSync(path)) !== screenshot.sha256) throw new Error(`Reviewed raster hash mismatch: ${screenshot.path}`);
}

export function assertHumanReviewLedger(ledger, snapshotId, observations, pageVerifications, bindings, outputRoot) {
  if (ledger.schema_version !== SCHEMA || ledger.snapshot_id !== snapshotId) throw new Error('Review ledger identity mismatch');
  if (!ledger.reviewer || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/u.test(ledger.reviewed_at || '')) throw new Error('Review ledger reviewer/time missing');
  assertDigest(ledger.actions_artifact?.artifact_digest, 'Actions artifact digest');
  assertDigest(ledger.permanent_storage?.sha256, 'Permanent evidence digest');
  if (!ledger.actions_artifact?.run_id || !ledger.actions_artifact?.artifact_id || !ledger.actions_artifact?.expires_at) throw new Error('Actions artifact provenance incomplete');
  if (!ledger.permanent_storage?.uri || !ledger.permanent_storage?.object_version || !ledger.permanent_storage?.bytes) throw new Error('Permanent evidence provenance incomplete');
  const capsuleIds = ledger.capsules?.map((item) => item.id).sort() || [];
  if (JSON.stringify(capsuleIds) !== JSON.stringify([...CAPSULES].sort())) throw new Error('Review ledger must cover exactly six capsules');
  const knownEvidenceIds = new Set([...observations, ...pageVerifications].map((item) => item.id));
  for (const row of ledger.capsules) {
    if (row.review_status !== 'reviewed' || !['high', 'medium', 'low'].includes(row.confidence) || !row.conclusion || !Array.isArray(row.evidence_ids) || !row.evidence_ids.length) throw new Error(`Incomplete capsule review: ${row.id}`);
    if (!Array.isArray(row.alternatives_considered)) throw new Error(`Capsule alternatives missing: ${row.id}`);
    if (row.evidence_ids.some((id) => !knownEvidenceIds.has(id))) throw new Error(`Capsule review has a dangling evidence ref: ${row.id}`);
  }
  const bindingIds = new Set(bindings.map((item) => item.id));
  const observationMap = new Map(ledger.observations?.map((item) => [item.id, item]) || []);
  if (observationMap.size !== observations.length) throw new Error('Every controlled specimen observation must be reviewed exactly once');
  const verificationIds = new Set(pageVerifications.map((item) => item.id));
  for (const observation of observations) {
    verifyRaster(outputRoot, observation.screenshot);
    const review = observationMap.get(observation.id);
    if (!review || review.review_status !== 'reviewed' || !TRACE_KINDS.has(review.trace_kind) || !bindingIds.has(review.source_binding_id)) throw new Error(`Incomplete specimen review trace: ${observation.id}`);
    if (review.trace_kind !== 'lab-source-only' && !verificationIds.has(review.page_verification_id)) throw new Error(`Reviewed specimen lacks a real-page trace: ${observation.id}`);
    if (review.trace_kind === 'state-equivalent' && review.visual_result !== 'match') throw new Error(`State-equivalent trace is not a visual match: ${observation.id}`);
  }
  const capturedPages = pageVerifications.filter((item) => item.screenshot?.sha256 || item.screenshot_sha256);
  const pageMap = new Map(ledger.page_verifications?.map((item) => [item.id, item]) || []);
  if (pageMap.size !== capturedPages.length) throw new Error('Every raster-backed real-page verification must be reviewed exactly once');
  for (const page of capturedPages) {
    const screenshot = page.screenshot || { path: page.screenshot_path, sha256: page.screenshot_sha256 };
    verifyRaster(outputRoot, screenshot);
    const review = pageMap.get(page.id);
    if (!review || review.review_status !== 'reviewed' || !review.visual_result) throw new Error(`Incomplete page visual review: ${page.id}`);
  }
  const rasterPaths = ['screenshots', 'component-screenshots'].flatMap((directory) => {
    const root = join(outputRoot, directory); if (!existsSync(root)) return [];
    return files(root).filter((path) => /\.(?:png|jpe?g|webp)$/iu.test(path)).map((path) => relative(outputRoot, path));
  }).sort();
  const rasterMap = new Map((ledger.raster_reviews || []).map((item) => [item.path, item]));
  if (rasterMap.size !== rasterPaths.length || rasterPaths.some((path) => !rasterMap.has(path))) throw new Error('Human review ledger must cover every indexed raster exactly once');
  for (const path of rasterPaths) {
    const review = rasterMap.get(path); const bytes = readFileSync(join(outputRoot, path));
    if (review.review_status !== 'reviewed' || !review.visual_result || review.sha256 !== sha(bytes)) throw new Error(`Invalid raster review: ${path}`);
  }
  return true;
}

export function capsuleEvidenceMatches(item, capsuleId) {
  const canonical = String(capsuleId || '').replace(/^capsule\./u, '');
  return (item?.capsule_ids || []).some((id) => String(id || '').replace(/^capsule\./u, '') === canonical);
}

function rewriteCapsule(root, capsuleReview, observations, pageVerifications) {
  const directory = capsuleReview.id.replace(/^capsule\./u, '');
  const capsuleRoot = join(root, 'conformance-capsules', directory);
  const capsulePath = join(capsuleRoot, 'capsule.json');
  const capsule = readJson(capsulePath);
  Object.assign(capsule, {
    review_status: 'reviewed', evidence_status: 'source-specimen-page-reconciled',
    review: { reviewer: capsuleReview.reviewer, reviewed_at: capsuleReview.reviewed_at,
      conclusion: capsuleReview.conclusion, confidence: capsuleReview.confidence,
      alternatives_considered: capsuleReview.alternatives_considered, evidence_ids: capsuleReview.evidence_ids },
    decision: 'NOT_MERGED', recommendation: 'unresolved', normalization_allowed: false,
  });
  writeFileSync(capsulePath, json(capsule, true));
  const specimenRefs = observations.filter((item) => capsuleEvidenceMatches(item, capsuleReview.id)).map((item) => ({
    id: `capsule-ref.${item.id}`, observation_id: item.id, specimen_id: item.specimen_id,
    screenshot_sha256: item.screenshot.sha256, review_status: 'reviewed', observation_attached: true,
    production_state_claimed: false, decision: 'NOT_MERGED', normalization_allowed: false,
  }));
  writeJsonl(join(capsuleRoot, 'specimen-observation-refs.jsonl'), specimenRefs);
  const pageRefs = pageVerifications.filter((item) => capsuleEvidenceMatches(item, capsuleReview.id)).map((item) => ({
    id: `capsule-ref.${item.id}`, verification_id: item.id, route_binding_id: item.route_binding_id,
    screenshot_sha256: item.screenshot?.sha256 || item.screenshot_sha256 || null,
    review_status: item.review_status, production_observed_by_capsule: item.production_state_claimed === true,
    decision: 'NOT_MERGED', normalization_allowed: false,
  }));
  writeJsonl(join(capsuleRoot, 'real-page-verification-refs.jsonl'), pageRefs);
  const unresolvedPath = join(capsuleRoot, 'unresolved-refs.jsonl');
  const unresolved = readJsonl(unresolvedPath).map((item) => ({ ...item, blocks_handoff: false,
    review_disposition: 'enumerated-for-family-scoped-defragmentation', reviewed_at: capsuleReview.reviewed_at }));
  writeJsonl(unresolvedPath, unresolved);
  writeFileSync(join(capsuleRoot, 'REVIEW.md'), `# ${capsule.title || capsuleReview.id}\n\nStatus: **REVIEWED**\n\nConclusion: ${capsuleReview.conclusion}\n\nConfidence: ${capsuleReview.confidence}\n\nEvidence remains AS-IS and NOT_MERGED. No normalization, tokenization, Penpot or runtime mutation is authorized.\n`);
  const indexed = files(capsuleRoot).filter((path) => basename(path) !== 'evidence-index.json').map((path) => {
    const bytes = readFileSync(path); return { path: relative(capsuleRoot, path), bytes: bytes.length, sha256: sha(bytes) };
  });
  writeFileSync(join(capsuleRoot, 'evidence-index.json'), json({ schema_version: capsule.schema_version,
    capsule_id: capsule.id, status: 'reviewed-compact-capsule-index', heavy_evidence_status: 'reviewed-and-permanently-bound', entries: indexed }, true));
}

export function materializeReviewedHandoff({ snapshotRoot, reviewLedgerPath }) {
  const root = resolve(snapshotRoot); const outputRoot = resolve(root, '../../..');
  validateCompactSnapshot(root);
  const manifest = readJson(join(root, 'manifest.json'));
  const ledger = readJson(resolve(reviewLedgerPath));
  const observations = readJsonl(join(root, 'specimen-observations.jsonl'));
  const pageVerifications = readJsonl(join(root, 'page-verification.jsonl'));
  const bindings = readJsonl(join(root, 'source-bindings.jsonl'));
  assertHumanReviewLedger(ledger, manifest.snapshot_id, observations, pageVerifications, bindings, outputRoot);
  const reviews = new Map(ledger.observations.map((item) => [item.id, item]));
  const reviewedObservations = observations.map((item) => ({ ...item, ...reviews.get(item.id),
    screenshot_sha256: item.screenshot.sha256, evidence_status: 'reviewed', review_status: 'reviewed', human_review_status: 'reviewed',
    trace_status: 'source-to-specimen-with-real-route-binding-reviewed', production_state_claimed: false }));
  writeJsonl(join(root, 'specimen-observations.jsonl'), reviewedObservations);
  const pageReviews = new Map(ledger.page_verifications.map((item) => [item.id, item]));
  const reviewedPages = pageVerifications.map((item) => pageReviews.has(item.id) ? ({ ...item, ...pageReviews.get(item.id),
    evidence_status: 'reviewed', review_status: 'reviewed', human_review_status: 'reviewed' }) : item);
  writeJsonl(join(root, 'page-verification.jsonl'), reviewedPages);
  writeFileSync(join(root, 'review-ledger.json'), json(ledger, true));
  const unresolved = readJsonl(join(root, 'unresolved.jsonl')).map((item) => item.blocks_handoff === true ? ({ ...item,
    blocks_handoff: false, resolution_status: 'evidence-attached-and-reviewed-not-normalized', review_ledger_sha256: sha(readFileSync(resolve(reviewLedgerPath))) }) : item);
  writeJsonl(join(root, 'unresolved.jsonl'), unresolved);
  for (const capsuleReview of ledger.capsules) rewriteCapsule(root, { ...capsuleReview, reviewer: ledger.reviewer, reviewed_at: ledger.reviewed_at }, reviewedObservations, reviewedPages);
  writeFileSync(join(root, 'summary.md'), `# Current UI Decoder v1 — reviewed immutable AS-IS handoff\n\n` +
    `- Snapshot: \`${manifest.snapshot_id}\`\n- Logical components: ${manifest.classification.total}/107\n` +
    `- Controlled observations reviewed: ${reviewedObservations.length}\n- Raster-backed page verifications reviewed: ${ledger.page_verifications.length}\n` +
    `- Reconciliation capsules reviewed: 6/6\n- Verdict: **GO_FOR_FAMILY_SCOPED_DEFRAGMENTATION**\n\n` +
    `This verdict authorizes only family-scoped defragmentation analysis. It does not authorize merge, split, normalization, tokenization, Penpot mutation, or Astro/CSS/runtime changes.\n`);

  const artifactIndexPath = join(root, 'artifact-index.json');
  const artifactIndex = readJson(artifactIndexPath);
  artifactIndex.entries = (artifactIndex.entries || []).map((entry) => {
    if (entry.storage !== 'compact-git-snapshot' || !entry.path || ['manifest.json', 'receipt.json', 'artifact-index.json'].includes(entry.path)) return entry;
    const path = join(root, entry.path); const bytes = readFileSync(path);
    return { ...entry, bytes: bytes.length, sha256: sha(bytes) };
  });
  if (!artifactIndex.entries.some((entry) => entry.path === 'review-ledger.json')) {
    const bytes = readFileSync(join(root, 'review-ledger.json'));
    artifactIndex.entries.push({ path: 'review-ledger.json', bytes: bytes.length, sha256: sha(bytes), storage: 'compact-git-snapshot' });
  }
  artifactIndex.entries.sort((a, b) => String(a.path || a.logical_path || '').localeCompare(String(b.path || b.logical_path || '')));
  artifactIndex.actions = { ...artifactIndex.actions, ...ledger.actions_artifact, post_upload_metadata_status: 'attached-and-verified-by-human-review-materializer' };
  artifactIndex.permanent_storage = ledger.permanent_storage;
  artifactIndex.human_review = { schema_version: ledger.schema_version, reviewer: ledger.reviewer, reviewed_at: ledger.reviewed_at,
    ledger_sha256: sha(readFileSync(resolve(reviewLedgerPath))), capsule_count: ledger.capsules.length,
    observation_count: reviewedObservations.length, page_verification_count: ledger.page_verifications.length,
    raster_review_count: ledger.raster_reviews.length };
  writeFileSync(artifactIndexPath, json(artifactIndex, true));

  const gates = { ...manifest.go_no_go.gates,
    controlled_specimen_evidence: true, six_capsule_evidence_coverage: true,
    capsule_human_visual_review: true, source_to_specimen_to_real_page_trace: true,
    blocking_unresolved_clear: true,
  };
  if (!Object.values(gates).every(Boolean)) throw new Error(`Reviewed handoff still has blockers: ${Object.entries(gates).filter(([, ok]) => !ok).map(([id]) => id).join(', ')}`);
  manifest.go_no_go = { ...manifest.go_no_go, status: 'GO', gates, blockers: [],
    verdict: 'GO_FOR_FAMILY_SCOPED_DEFRAGMENTATION' };
  manifest.review = artifactIndex.human_review;
  manifest.decoder = { ...manifest.decoder, reviewed_handoff_materializer: 'scripts/current_ui_resource_graph/v1/review-materialize.mjs' };
  const excluded = new Set(['manifest.json', 'receipt.json']);
  manifest.outputs = Object.fromEntries(files(root).filter((path) => !excluded.has(relative(root, path))).map((path) => {
    const bytes = readFileSync(path); return [relative(root, path), { bytes: bytes.length, sha256: sha(bytes) }];
  }));
  writeFileSync(join(root, 'manifest.json'), json(manifest, true));
  const receipt = { schema_version: manifest.schema_version, status: 'complete', evidence_completion: 'complete',
    handoff_status: 'GO', verdict: 'GO_FOR_FAMILY_SCOPED_DEFRAGMENTATION', snapshot_id: manifest.snapshot_id,
    snapshot_time: manifest.snapshot_time, reviewed_at: ledger.reviewed_at,
    manifest_sha256: sha(readFileSync(join(root, 'manifest.json'))), blockers: [] };
  writeFileSync(join(root, 'receipt.json'), json(receipt, true));
  validateCompactSnapshot(root);
  return { root, receipt, artifact_index: artifactIndex };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [snapshotRoot, reviewLedgerPath] = process.argv.slice(2);
  if (!snapshotRoot || !reviewLedgerPath) throw new Error('Usage: review-materialize.mjs <snapshot-root> <review-ledger.json>');
  process.stdout.write(json(materializeReviewedHandoff({ snapshotRoot, reviewLedgerPath }), true));
}

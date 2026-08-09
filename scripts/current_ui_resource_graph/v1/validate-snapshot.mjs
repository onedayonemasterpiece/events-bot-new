#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { basename, join, relative, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { assertCandidateContracts } from './contracts.mjs';
import { DISPOSITIONS, REACHABILITY } from './classification.mjs';

const sha = (value) => createHash('sha256').update(value).digest('hex');
const readJson = (path) => JSON.parse(readFileSync(path, 'utf8'));
const readJsonl = (path) => readFileSync(path, 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));

function walk(root) {
  const result = [];
  const visit = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) visit(path); else if (entry.isFile()) result.push(path);
    }
  };
  visit(root); return result;
}

function verifyFile(root, entry, label) {
  const path = resolve(root, entry.path);
  if (path !== root && !path.startsWith(`${root}/`)) throw new Error(`${label} contains unsafe path: ${entry.path}`);
  if (!existsSync(path) || !statSync(path).isFile()) throw new Error(`${label} file missing: ${entry.path}`);
  const bytes = readFileSync(path);
  if (entry.bytes !== undefined && bytes.length !== entry.bytes) throw new Error(`${label} byte mismatch: ${entry.path}`);
  if (entry.sha256 && sha(bytes) !== entry.sha256) throw new Error(`${label} hash mismatch: ${entry.path}`);
  return path;
}

export function assertReviewedCapsuleEvidence({ capsule, specimenRefs, pageRefs, observationIds, pageIds }) {
  if (!specimenRefs.length || !pageRefs.length) throw new Error(`Reviewed capsule lacks local specimen/page evidence refs: ${capsule.id}`);
  const localIds = new Set();
  for (const ref of specimenRefs) {
    if (!ref.observation_id || !observationIds.has(ref.observation_id)) throw new Error(`Dangling capsule specimen observation: ${capsule.id}`);
    localIds.add(ref.observation_id);
  }
  for (const ref of pageRefs) {
    if (!ref.verification_id || !pageIds.has(ref.verification_id)) throw new Error(`Dangling capsule page verification: ${capsule.id}`);
    localIds.add(ref.verification_id);
  }
  const reviewEvidence = capsule.review?.evidence_ids || [];
  if (!reviewEvidence.length || reviewEvidence.some((id) => !localIds.has(id))) throw new Error(`Capsule review evidence is not represented by local refs: ${capsule.id}`);
  return true;
}

export function validateCompactSnapshot(snapshotRoot, { canonical = null } = {}) {
  const root = resolve(snapshotRoot);
  const manifestPath = join(root, 'manifest.json'); const receiptPath = join(root, 'receipt.json');
  if (!existsSync(manifestPath) || !existsSync(receiptPath)) throw new Error('Snapshot manifest or receipt missing');
  const manifest = readJson(manifestPath); const receipt = readJson(receiptPath);
  const canonicalSnapshot = canonical ?? manifest.identity_planes?.latest_checked_kaggle_candidate?.source_sha === 'ef7aa62e45c60f7a12da6160f490719c0721ec03';
  if (receipt.manifest_sha256 !== sha(readFileSync(manifestPath))) throw new Error('Receipt-to-manifest hash mismatch');
  for (const [path, metadata] of Object.entries(manifest.outputs || {})) verifyFile(root, { path, ...metadata }, 'manifest.outputs');

  const artifactIndex = readJson(join(root, 'artifact-index.json'));
  for (const entry of artifactIndex.entries || []) if (entry.storage === 'compact-git-snapshot' && entry.sha256) verifyFile(root, entry, 'artifact-index');

  const componentPaths = walk(join(root, 'components')).filter((path) => path.endsWith('.json'));
  if (canonicalSnapshot && componentPaths.length !== 107) throw new Error(`Canonical component count mismatch: ${componentPaths.length}`);
  for (const path of componentPaths) {
    const row = readJson(path);
    if (!DISPOSITIONS.includes(row.disposition) || !REACHABILITY.includes(row.reachability)) throw new Error(`Closed component enum mismatch: ${basename(path)}`);
    if (row.decision !== 'NOT_MERGED' || row.recommendation !== 'unresolved') throw new Error(`Component STOP mismatch: ${basename(path)}`);
  }

  const contracts = walk(join(root, 'candidate-contracts')).filter((path) => path.endsWith('.contract.json')).map(readJson);
  assertCandidateContracts(contracts);
  const contractById = new Map(contracts.map((item) => [item.id, item]));
  const observations = readJsonl(join(root, 'specimen-observations.jsonl'));
  const pageVerifications = readJsonl(join(root, 'page-verification.jsonl'));
  const observationIds = new Set(observations.map((item) => item.id));
  const pageIds = new Set(pageVerifications.map((item) => item.id));
  const capsuleRoots = readdirSync(join(root, 'conformance-capsules'), { withFileTypes: true }).filter((item) => item.isDirectory()).map((item) => join(root, 'conformance-capsules', item.name)).sort();
  if (capsuleRoots.length !== 6) throw new Error(`Capsule count mismatch: ${capsuleRoots.length}`);
  for (const capsuleRoot of capsuleRoots) {
    const index = readJson(join(capsuleRoot, 'evidence-index.json'));
    for (const entry of index.entries || []) verifyFile(capsuleRoot, entry, `capsule ${basename(capsuleRoot)}`);
    const refs = readJson(join(capsuleRoot, 'candidate-contract-ref.json'));
    for (const ref of refs.contracts || []) {
      if (!contractById.has(ref.id)) throw new Error(`Dangling capsule contract: ${ref.id}`);
      verifyFile(root, ref, `capsule contract ${ref.id}`);
    }
    if (manifest.go_no_go?.status === 'GO') assertReviewedCapsuleEvidence({
      capsule: readJson(join(capsuleRoot, 'capsule.json')),
      specimenRefs: readJsonl(join(capsuleRoot, 'specimen-observation-refs.jsonl')),
      pageRefs: readJsonl(join(capsuleRoot, 'real-page-verification-refs.jsonl')),
      observationIds, pageIds,
    });
  }

  for (const name of ['specimen-plan.jsonl', 'specimen-observations.jsonl', 'page-verification.jsonl', 'mismatches.jsonl', 'unresolved.jsonl']) {
    const rows = readJsonl(join(root, name));
    if (new Set(rows.map((item) => item.id)).size !== rows.length) throw new Error(`Duplicate IDs in ${name}`);
  }
  const blocking = readJsonl(join(root, 'unresolved.jsonl')).filter((item) => item.blocks_handoff === true);
  if (manifest.go_no_go?.status === 'GO' && blocking.length) throw new Error('GO snapshot contains blocking unresolved records');
  if (manifest.go_no_go?.status === 'GO') {
    if (!observations.length || !observations.every((item) => item.source_binding_id && item.screenshot_sha256 &&
      item.review_status === 'reviewed' && ['state-equivalent', 'consumer-exists-only', 'lab-source-only'].includes(item.trace_kind) &&
      (item.trace_kind === 'lab-source-only' || pageIds.has(item.page_verification_id)))) throw new Error('GO snapshot lacks reviewed bound specimen evidence');
    if (capsuleRoots.some((capsuleRoot) => readJson(join(capsuleRoot, 'capsule.json')).review_status !== 'reviewed')) throw new Error('GO snapshot contains unreviewed capsule');
    if (!artifactIndex.actions?.artifact_id || !artifactIndex.actions?.run_id || !/^sha256:[a-f0-9]{64}$/u.test(artifactIndex.actions?.artifact_digest || '')) throw new Error('GO snapshot lacks durable Actions provenance');
    if (!artifactIndex.permanent_storage?.uri || !artifactIndex.permanent_storage?.object_version || !/^sha256:[a-f0-9]{64}$/u.test(artifactIndex.permanent_storage?.sha256 || '')) throw new Error('GO snapshot lacks permanent evidence provenance');
    if (!manifest.review?.ledger_sha256 || !manifest.review?.reviewed_at || !(manifest.review?.raster_review_count > 0)) throw new Error('GO snapshot lacks human review provenance');
  }

  const unsafe = walk(root).filter((path) => /(?:_review\/|authorization\s*[:=]\s*["']?(?:bearer|basic|[a-z0-9_-]{16})|bearer\s+[a-z0-9._~-]{12}|sb_publishable_[a-z0-9_-]+|"data-supabase-key"\s*:\s*"[^"{])/iu.test(readFileSync(path, 'utf8')));
  if (unsafe.length) throw new Error(`Secret-shaped compact evidence: ${unsafe.map((path) => relative(root, path)).join(', ')}`);
  return { status: 'valid', files: walk(root).length, components: componentPaths.length, contracts: contracts.length, capsules: capsuleRoots.length, blocking_unresolved: blocking.length };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const root = process.argv[2];
  if (!root) throw new Error('Usage: validate-snapshot.mjs <snapshot-root>');
  process.stdout.write(`${JSON.stringify(validateCompactSnapshot(root))}\n`);
}

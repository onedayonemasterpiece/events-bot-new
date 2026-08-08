#!/usr/bin/env node
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, resolve } from 'node:path';
import { createRequire } from 'node:module';
import {
  Budget, DEFAULT_IDENTITIES, FAMILY_SEEDS, JsonlWriter, REQUIRED_FILES, SCHEMA,
  assertGraphInvariants, candidateGraph, captureBrowserEvidence, computedStyleObservations, coverageRows, desktopMobile, ensureOutput,
  eventPresentationFormats, fragmentationReport, inventorySource, loadParsers, manifestHtmlFiles,
  mapRuntimeToSource, observedFamilies, outputHashes, pageFamiliesFromSource, parseArgs, redactFactory,
  REQUIRED_CANDIDATE_CHECKS, scanLocalRuntime, scanPublicRoot, scanRemoteRuntime, screenshotIndex, sha256, stableJson,
  styleObservations, validateManifestInventory, validateSourcePin, withRetry, writeDeterministic,
} from './graph-lib.mjs';
import { classifyLogicalComponents, classificationCounts } from './v1/classification.mjs';
import { initializeV1Receipt, writeV1Snapshot } from './v1/snapshot.mjs';
import { buildDecoderReconciliationBundle } from './v1/capsules.mjs';

function bool(value, fallback = false) {
  if (value === undefined) return fallback;
  if (value === 'true' || value === '1') return true;
  if (value === 'false' || value === '0') return false;
  throw new Error(`Expected boolean value, received ${value}`);
}

function firstOutputArg(argv) {
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--output') return argv[i + 1];
    if (argv[i].startsWith('--output=')) return argv[i].slice('--output='.length);
  }
  return 'artifacts/current-ui-resource-graph/partial';
}

function outputFiles(root) {
  const found = [];
  function visit(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) visit(path); else if (entry.isFile()) found.push(path);
    }
  }
  visit(root); return found;
}

async function fetchManifest(base, redact, maxBytes) {
  const url = new URL('secret-candidate-manifest.json', base.endsWith('/') ? base : `${base}/`);
  return withRetry('Candidate manifest fetch', async () => {
    const response = await fetch(url, { redirect: 'error', headers: { accept: 'application/json' } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const declared = Number(response.headers.get('content-length') || 0);
    if (declared > maxBytes) throw new Error('Candidate manifest exceeds byte budget');
    const bytes = Buffer.from(await response.arrayBuffer());
    if (bytes.length > maxBytes) throw new Error('Candidate manifest exceeds byte budget');
    return { bytes, manifest: JSON.parse(bytes.toString('utf8')) };
  }, { redact });
}

function validateRuntimeIdentity(manifest, bytes, identities, verifyProduction) {
  const inventory = validateManifestInventory(manifest);
  const actual = {
    manifest_sha256: sha256(bytes),
    schema_version: manifest.schema_version, generated_at: manifest.generated_at,
    build_id: manifest.build_id, run_id: manifest.run_id, source_sha: manifest.repo_sha,
    snapshot: manifest.snapshot, versions: manifest.versions,
    production_manifest_sha256: manifest.production_manifest_sha256,
    production_tree_sha256: manifest.production_tree_sha256,
    ...inventory,
  };
  if (!verifyProduction) return actual;
  const expected = identities.latest_checked_kaggle_candidate;
  const mismatches = [];
  if (manifest.schema_version !== 'static_secret_candidate_manifest_v1') mismatches.push('schema_version');
  if (actual.manifest_sha256 !== expected.manifest_sha256) mismatches.push('manifest_sha256');
  if (actual.generated_at !== expected.generated_at) mismatches.push('generated_at');
  if (actual.tree_sha256 !== expected.tree_sha256) mismatches.push('tree_sha256');
  if (actual.production_manifest_sha256 !== expected.production_manifest_sha256) mismatches.push('production_manifest_sha256');
  if (actual.production_tree_sha256 !== expected.production_tree_sha256) mismatches.push('production_tree_sha256');
  if (manifest.repo_sha !== expected.source_sha) mismatches.push('source_sha');
  if (manifest.build_id !== expected.build_id) mismatches.push('build_id');
  if (manifest.run_id !== expected.run_id) mismatches.push('run_id');
  if (manifest.snapshot?.snapshot_id !== expected.snapshot_id && manifest.snapshot?.id !== expected.snapshot_id) mismatches.push('snapshot_id');
  for (const [key, value] of Object.entries(expected.manifest_facts)) if (actual[key] !== value) mismatches.push(`manifest_facts.${key}`);
  for (const check of REQUIRED_CANDIDATE_CHECKS) if (manifest.checks?.[check] !== 'ok') mismatches.push(`checks.${check}`);
  if (manifest.site_mode !== 'secret_candidate' || manifest.publication_mode !== 'secret_link') mismatches.push('candidate_publication_profile');
  if (mismatches.length) throw new Error(`Exact candidate identity mismatch: ${mismatches.sort().join(', ')}`);
  return actual;
}

function sourceCounts(records) {
  const count = (type) => records.filter((record) => record.type === type).length;
  const result = { total: records.length, components: count('component'), layouts: count('layout'), pages: count('page'), controllers_or_modules: count('controller_or_module'), stylesheets: count('stylesheet') };
  if (result.components + result.layouts + result.pages + result.controllers_or_modules + result.stylesheets !== result.total) throw new Error('Source inventory category count mismatch');
  return result;
}

function sourceCountsByPlane(records) {
  return Object.fromEntries([...new Set(records.map((record) => record.plane))].sort().map((plane) => [plane, sourceCounts(records.filter((record) => record.plane === plane))]));
}

function summaryMarkdown(identity, counts, families, coverage) {
  const highImpact = [...families].sort((a, b) => (b.runtime_observations.length + b.implementations.length) - (a.runtime_observations.length + a.implementations.length) || a.id.localeCompare(b.id)).slice(0, 20);
  return `# Current UI Resource Graph v0\n\n` +
    `## Build identity\n\n` +
    `- Snapshot: \`${identity.snapshot_id}\`\n` +
    `- Candidate source SHA: \`${identity.latest_checked_kaggle_candidate.source_sha}\`\n` +
    `- Candidate build: \`${identity.latest_checked_kaggle_candidate.build_id}\`\n` +
    `- Candidate run: \`${identity.latest_checked_kaggle_candidate.run_id}\`\n` +
    `- Candidate generated/build date: \`${identity.latest_checked_kaggle_candidate.generated_at}\`\n` +
    `- Candidate manifest SHA-256: \`${identity.latest_checked_kaggle_candidate.manifest_sha256}\`\n` +
    `- Candidate Astro / Node: \`${identity.latest_checked_kaggle_candidate.astro_version}\` / \`${identity.latest_checked_kaggle_candidate.node_version}\`\n` +
    `- Candidate snapshot versions: ${JSON.stringify(identity.latest_checked_kaggle_candidate.versions || {})}\n` +
    `- Candidate role: immutable noindex candidate; it is not the production root and root promotion is disabled.\n` +
    `- Current public root release: \`${identity.current_root_prelaunch.release_id}\` (source \`${identity.current_root_prelaunch.source_sha}\`, published ${identity.current_root_prelaunch.published_at}, observed HTML \`${identity.current_root_prelaunch.html_sha256}\`).\n` +
    `- Identity planes remain independent; family aggregation groups the same logical source path across planes and reports cross-plane drift separately.\n\n` +
    `## Pages\n\n- Candidate HTML routes: ${counts.candidate_routes}\n- Separate public-root observations: ${counts.public_root_observations}\n- Page families: ${counts.page_families}\n- Layouts by plane: ${JSON.stringify(Object.fromEntries(Object.entries(counts.source_by_plane).map(([plane, value]) => [plane, value.layouts])))}\n\n` +
    `## Components\n\n- Source records across both planes: ${counts.source.total}\n- Source records by plane: ${JSON.stringify(counts.source_by_plane)}\n- Source components by plane: ${JSON.stringify(Object.fromEntries(Object.entries(counts.source_by_plane).map(([plane, value]) => [plane, value.components])))}\n- Observed UI families: ${counts.observed_families}\n- Candidate families: ${counts.candidate_families}\n- Event presentation resource formats: ${counts.event_presentation_formats} (${counts.event_presentation_observed} runtime-observed)\n- Desktop event layout formats: editorial landscape and split portrait/poster; their side/stacked and inline CTA placements remain distinct.\n- Event media formats: primary large frame, large poster companion, and small remaining-photo thumbnail/preview rails remain distinct.\n\n` +
    `## Fragmentation\n\n- Duplicate/fragmentation candidates: ${counts.fragmentation}\n- Style inconsistencies: ${counts.style_inconsistencies}\n- Total style observations: ${counts.styles}\n- Unresolved mappings/questions: ${counts.unresolved}\n\n` +
    `## Mobile/Desktop\n\n- Shared structures: ${counts.shared}\n- Divergent structures: ${counts.divergent}\n- Unknown independent comparison: ${counts.desktop_mobile_unknown}\n\n` +
    `## Top 20 high-impact families\n\n${highImpact.map((item, index) => `${index + 1}. ${item.label} — ${item.status}`).join('\n')}\n\n` +
    `## Coverage hypotheses\n\n- FOUND: ${coverage.filter((item) => item.status === 'FOUND').length}\n- MISSING: ${coverage.filter((item) => item.status === 'MISSING').length}\n- DISCOVERED: ${coverage.filter((item) => item.status === 'DISCOVERED').length}\n- AMBIGUOUS: ${coverage.filter((item) => item.status === 'AMBIGUOUS').length}\n\n` +
    `## Required evidence-completion step\n\nComplete component specimens, source-to-page reconciliation, capsule review, and immutable handoff. STOP before normalization.\n`;
}

function coverageMarkdown(rows, sourcePin, runtimeFacts) {
  const lines = rows.map((row) => `| ${row.label.replaceAll('|', '\\|')} | ${row.status} | ${row.source_evidence.join(', ') || '—'} | ${row.runtime_evidence.join(', ') || '—'} | ${row.note.replaceAll('|', '\\|')} |`);
  return `# Current UI Resource Graph coverage\n\n` +
    `Source pin: \`${sourcePin.match}\`, tree \`${sourcePin.tree_hash}\`. Runtime manifest: ${runtimeFacts.html_count} HTML / ${runtimeFacts.page_count} pages / ${runtimeFacts.file_count} files / ${runtimeFacts.bytes} bytes.\n\n` +
    `Experimental archaeology and old branches are excluded. FOUND requires source and exact-manifest runtime evidence; one channel is AMBIGUOUS, neither is MISSING, and unseeded observed page families are DISCOVERED.\n\n` +
    `| Hypothesis | Result | Source evidence | Runtime evidence | Note |\n|---|---|---|---|---|\n${lines.join('\n')}\n`;
}

function unresolvedMarkdown(families, coverage) {
  const unknown = families.filter((item) => item.status !== 'observed');
  const missing = coverage.filter((item) => item.status === 'MISSING' || item.status === 'AMBIGUOUS');
  return `# Unresolved questions\n\n` +
    `No answer below authorizes merging, normalization, a component contract, tokens, variants, patterns, Penpot mutation, or Astro changes.\n\n` +
    `## Family mappings\n\n${unknown.map((item) => `- ${item.label}: ${item.status}; requires independent source/runtime/semantic evidence.`).join('\n') || '- None.'}\n\n` +
    `## Coverage gaps\n\n${missing.map((item) => `- ${item.label}: ${item.status}. ${item.note}`).join('\n') || '- None.'}\n`;
}

async function writeJsonl(path, records, budget) {
  const writer = new JsonlWriter(path, budget);
  const ordered = [...records].sort((a, b) => String(a.id || '').localeCompare(String(b.id || '')));
  for (const record of ordered) await writer.write(record);
  await writer.close();
  return writer.count;
}

async function run(argv) {
  const output = ensureOutput(firstOutputArg(argv));
  let redact = redactFactory();
  const receiptPath = join(output, 'receipt.json');
  let v1ReceiptPath = null;
  let receiptTime = DEFAULT_IDENTITIES.snapshot_time;
  writeFileSync(receiptPath, stableJson({ schema_version: SCHEMA, status: 'started', snapshot_time: receiptTime }, true));
  try {
    const args = parseArgs(argv);
    receiptTime = args.snapshot_time || receiptTime;
    writeFileSync(receiptPath, stableJson({ schema_version: SCHEMA, status: 'started', snapshot_time: receiptTime }, true));
    const sourceRoot = resolve(args.source_root || 'site/src');
    const siteRoot = resolve(args.site_root || dirname(sourceRoot));
    const identity = structuredClone(DEFAULT_IDENTITIES);
    if (args.source_sha) identity.latest_checked_kaggle_candidate.source_sha = args.source_sha;
    if (args.snapshot_id) identity.snapshot_id = args.snapshot_id;
    if (args.snapshot_time) identity.snapshot_time = args.snapshot_time;
    const v1SnapshotId = args.v1_snapshot_id || `decoder-v1-${identity.snapshot_id}`;
    const candidateOverrides = {
      build_id: args.candidate_build_id, run_id: args.candidate_run_id,
      snapshot_id: args.candidate_snapshot_id, manifest_sha256: args.candidate_manifest_sha256,
      generated_at: args.candidate_generated_at, tree_sha256: args.candidate_tree_sha256,
      production_manifest_sha256: args.production_manifest_sha256, production_tree_sha256: args.production_tree_sha256,
      astro_version: args.astro_version, node_version: args.node_version,
    };
    for (const [key, value] of Object.entries(candidateOverrides)) if (value) identity.latest_checked_kaggle_candidate[key] = value;
    const rootOverrides = {
      source_sha: args.root_source_sha, release_id: args.root_release_id,
      actions_run_id: args.root_actions_run_id, artifact_id: args.root_artifact_id,
      published_at: args.root_published_at, runtime_url: args.root_runtime_url, html_sha256: args.root_html_sha256,
    };
    for (const [key, value] of Object.entries(rootOverrides)) if (value) identity.current_root_prelaunch[key] = value;
    const baseFromFile = args.candidate_base_url_file ? readFileSync(resolve(args.candidate_base_url_file), 'utf8').trim() : '';
    const baseFromEnv = process.env[args.candidate_base_url_env || 'CURRENT_UI_GRAPH_CANDIDATE_BASE_URL'] || '';
    const candidateBase = baseFromFile || baseFromEnv;
    redact = redactFactory([candidateBase]);
    const budget = new Budget(Number(args.output_byte_budget || 75 * 1024 * 1024));
    v1ReceiptPath = join(initializeV1Receipt(output, v1SnapshotId, identity.snapshot_time), 'receipt.json');
    const maxHtmlBytes = Number(args.max_html_bytes || 4 * 1024 * 1024);
    const rootSourceRoot = resolve(args.root_source_root || sourceRoot);
    const sourcePin = validateSourcePin(sourceRoot, identity.latest_checked_kaggle_candidate.source_sha, args.source_tree_hash || '');
    const rootSourcePin = validateSourcePin(rootSourceRoot, identity.current_root_prelaunch.source_sha, args.root_source_tree_hash || '');
    const parsers = await loadParsers(siteRoot);

    const requireFromSite = createRequire(join(siteRoot, 'package.json'));
    const astroVersion = JSON.parse(readFileSync(requireFromSite.resolve('astro/package.json'), 'utf8')).version;
    const verifyProduction = bool(args.verify_production_identity, true);
    if (verifyProduction && astroVersion !== identity.latest_checked_kaggle_candidate.astro_version) throw new Error('Pinned Astro version mismatch');
    if (verifyProduction && process.versions.node !== identity.latest_checked_kaggle_candidate.node_version) throw new Error('Pinned Node version mismatch');

    let runtimeManifestBytes, runtimeManifest;
    if (args.runtime_manifest) {
      runtimeManifestBytes = readFileSync(resolve(args.runtime_manifest)); runtimeManifest = JSON.parse(runtimeManifestBytes.toString('utf8'));
    } else {
      if (!candidateBase) throw new Error('Runtime candidate base URL must be supplied through the configured environment variable or file');
      ({ bytes: runtimeManifestBytes, manifest: runtimeManifest } = await fetchManifest(candidateBase, redact, Number(args.max_manifest_bytes || 16 * 1024 * 1024)));
    }
    const runtimeFacts = validateRuntimeIdentity(runtimeManifest, runtimeManifestBytes, identity, verifyProduction);
    identity.latest_checked_kaggle_candidate.versions = runtimeFacts.versions || {};
    identity.latest_checked_kaggle_candidate.snapshot = runtimeFacts.snapshot || {};

    const candidateSourceRecords = await inventorySource(sourceRoot, parsers, { plane: 'latest_checked_kaggle_candidate' });
    const rootSourceRecords = await inventorySource(rootSourceRoot, parsers, { plane: 'current_root_prelaunch' });
    const sourceRecords = [...candidateSourceRecords, ...rootSourceRecords];
    const candidateRuntime = args.runtime_root
      ? await scanLocalRuntime(resolve(args.runtime_root), runtimeManifest, parsers, maxHtmlBytes)
      : await scanRemoteRuntime(candidateBase, runtimeManifest, parsers, maxHtmlBytes, redact);
    let rootRuntime;
    if (args.root_runtime_file) {
      const bytes = readFileSync(resolve(args.root_runtime_file));
      const fileUrl = `data:text/html;base64,${bytes.toString('base64')}`;
      rootRuntime = await scanPublicRoot(fileUrl, identity.current_root_prelaunch.html_sha256, parsers, maxHtmlBytes, redact);
    } else rootRuntime = await scanPublicRoot(identity.current_root_prelaunch.runtime_url, identity.current_root_prelaunch.html_sha256, parsers, maxHtmlBytes, redact);
    const runtime = mapRuntimeToSource([...candidateRuntime, rootRuntime], sourceRecords);
    let styles = await styleObservations({ latest_checked_kaggle_candidate: sourceRoot, current_root_prelaunch: rootSourceRoot }, sourceRecords, parsers);
    const families = observedFamilies(sourceRecords, runtime);
    const pageFamilies = pageFamiliesFromSource(sourceRecords, runtime);
    let viewportEvidence = [];
    let componentEvidence = [];
    let screenshots = screenshotIndex(pageFamilies);
    if (bool(args.browser_observations, false)) {
      const browserResult = await captureBrowserEvidence({
        candidateBase, manifest: runtimeManifest, runtimeObservations: candidateRuntime, families,
        siteRoot, outputDir: output, budget, maxPages: Number(args.browser_max_pages || 20),
        snapshotTime: identity.snapshot_time,
      });
      viewportEvidence = browserResult.viewportEvidence;
      componentEvidence = browserResult.componentEvidence || [];
      const capturedFamilies = new Set(browserResult.screenshots.map((item) => item.page_family));
      screenshots = [...browserResult.screenshots, ...screenshots.filter((item) => !capturedFamilies.has(item.page_family))]
        .sort((a, b) => a.page_family.localeCompare(b.page_family) || String(a.route_hash).localeCompare(String(b.route_hash)) || (a.viewport?.width || 0) - (b.viewport?.width || 0));
      styles = [...styles, ...computedStyleObservations(viewportEvidence)];
    }
    const candidates = candidateGraph(families, sourceRecords, runtime, styles);
    const fragmentation = fragmentationReport(families, styles, sourceRecords, runtime);
    const desktop = desktopMobile(pageFamilies, families, viewportEvidence);
    const coverage = coverageRows(sourceRecords, runtime, pageFamilies);
    const eventFormats = eventPresentationFormats(sourceRecords, runtime, screenshots);
    const logicalComponents = classifyLogicalComponents(sourceRecords, runtime);
    const reconciliation = buildDecoderReconciliationBundle({ eventPresentationRecords: eventFormats });

    await writeJsonl(join(output, 'source-components.jsonl'), sourceRecords, budget);
    await writeJsonl(join(output, 'observed-ui-families.jsonl'), families, budget);
    await writeJsonl(join(output, 'runtime-observations.jsonl'), runtime, budget);
    await writeJsonl(join(output, 'page-families.jsonl'), pageFamilies, budget);
    await writeJsonl(join(output, 'event-presentation-formats.jsonl'), eventFormats, budget);
    await writeJsonl(join(output, 'desktop-mobile-analysis.jsonl'), desktop, budget);
    await writeJsonl(join(output, 'style-observations.jsonl'), styles, budget);
    await writeJsonl(join(output, 'fragmentation-report.jsonl'), fragmentation, budget);
    await writeJsonl(join(output, 'candidate-component-graph.jsonl'), candidates, budget);
    await writeJsonl(join(output, 'screenshots-index.jsonl'), screenshots, budget);
    await writeJsonl(join(output, 'component-evidence.jsonl'), componentEvidence, budget);

    const counts = {
      routes: runtime.length, candidate_routes: candidateRuntime.length, public_root_observations: 1,
      page_families: pageFamilies.length, source: sourceCounts(sourceRecords),
      source_by_plane: sourceCountsByPlane(sourceRecords),
      observed_families: families.filter((item) => item.status === 'observed').length,
      candidate_families: candidates.filter((item) => item.status !== 'unknown').length,
      event_presentation_formats: eventFormats.length,
      event_presentation_observed: eventFormats.filter((item) => item.status === 'observed').length,
      fragmentation: fragmentation.filter((item) => item.status === 'fragmented').length,
      styles: styles.length,
      style_inconsistencies: styles.filter((item) => item.source_divergence === 'distinct_literals_observed' || item.computed_inconsistency === 'observed_divergence').length,
      unresolved: candidates.filter((item) => item.status !== 'observed').length + coverage.filter((item) => item.status !== 'FOUND').length,
      shared: desktop.filter((item) => item.scope === 'ui_family' && item.relation === 'shared_structure_observed').length,
      divergent: desktop.filter((item) => item.scope === 'ui_family' && item.relation === 'divergent_structure_observed').length,
      desktop_mobile_unknown: desktop.filter((item) => item.scope === 'ui_family' && item.relation === 'unknown').length,
      logical_components: logicalComponents.length,
      logical_component_classification: classificationCounts(logicalComponents),
      component_evidence: componentEvidence.length,
    };
    writeDeterministic(join(output, 'summary.md'), summaryMarkdown(identity, counts, families, coverage), budget);
    writeDeterministic(join(output, 'coverage-report.md'), coverageMarkdown(coverage, sourcePin, runtimeFacts), budget);
    writeDeterministic(join(output, 'unresolved-questions.md'), unresolvedMarkdown(families, coverage), budget);

    const hashedNames = [
      ...REQUIRED_FILES.filter((name) => name !== 'manifest.json'),
    ];
    const manifest = {
      schema_version: SCHEMA, snapshot_id: identity.snapshot_id, snapshot_time: identity.snapshot_time,
      identity_planes: {
        current_root_prelaunch: identity.current_root_prelaunch,
        latest_checked_kaggle_candidate: identity.latest_checked_kaggle_candidate,
      },
      identity_invariant: 'candidate_is_not_production_root', source_pins: { latest_checked_kaggle_candidate: sourcePin, current_root_prelaunch: rootSourcePin },
      runtime_manifest: runtimeFacts,
      runtime_planes: {
        latest_checked_kaggle_candidate: {
          observation_count: candidateRuntime.length,
          manifest_sha256: runtimeFacts.manifest_sha256,
          tree_sha256: runtimeFacts.tree_sha256,
          html_count: runtimeFacts.html_count,
        },
        current_root_prelaunch: {
          observation_count: 1,
          html_sha256: rootRuntime.content_fixture.content_sha256,
          expected_html_sha256: identity.current_root_prelaunch.html_sha256,
          verification: 'exact_sha256_match',
        },
      },
      counts, coverage_results: Object.fromEntries(['FOUND', 'MISSING', 'DISCOVERED', 'AMBIGUOUS'].map((status) => [status, coverage.filter((row) => row.status === status).length])),
      viewports: { core: [{ width: 390, height: 844 }, { width: 1728, height: 900 }], optional_evidence: [{ width: 430, height: 932 }, { width: 768, height: 1024 }, { width: 1280, height: 800 }] },
      visual_evidence_contract: { raw_raster_role: 'noncanonical_visual_evidence', canonical_fingerprint: 'dhash-64', in_session_stability: 'two_consecutive_exact_buffers', cross_run_acceptance: 'equal_perceptual_dhash_64' },
      output_byte_budget: budget.limit, output_bytes_before_manifest: budget.used,
      outputs: outputHashes(output, hashedNames),
      constraints: { automatic_defragmentation: false, automatic_merge: false, candidate_as_is_contract_generation: true, normative_component_contract_generation: false, full_html_retained: false, bearer_url_retained: false },
    };
    writeDeterministic(join(output, 'manifest.json'), stableJson(manifest, true), budget);
    assertGraphInvariants(output);
    const serialized = [...REQUIRED_FILES, 'receipt.json'].filter((name) => {
      try { return readFileSync(join(output, name), 'utf8').includes(candidateBase) && Boolean(candidateBase); } catch { return false; }
    });
    if (serialized.length) throw new Error(`Secret redaction invariant failed: ${serialized.join(', ')}`);
    writeV1Snapshot({
      output, snapshotId: v1SnapshotId, snapshotTime: identity.snapshot_time,
      identity: { current_root_prelaunch: identity.current_root_prelaunch, latest_checked_kaggle_candidate: identity.latest_checked_kaggle_candidate },
      sourceRecords, components: logicalComponents, families, pageFamilies, runtime, screenshots,
      viewportEvidence, componentEvidence, coverage, styles, budget,
      specimenPlanRows: reconciliation.specimen_plan,
      specimenObservations: reconciliation.specimen_observations,
      candidateContracts: reconciliation.candidate_contracts,
      capsules: reconciliation.capsules,
      mismatchRowsExtra: reconciliation.mismatches,
      unresolvedRowsExtra: reconciliation.unresolved,
    });
    if (candidateBase) {
      const candidateToken = new URL(candidateBase).pathname.split('/').filter(Boolean).find((part) => part.length >= 20) || '';
      const leaked = outputFiles(output).filter((path) => {
        const bytes = readFileSync(path); return bytes.includes(candidateBase) || (candidateToken && bytes.includes(candidateToken));
      });
      if (leaked.length) throw new Error(`Recursive secret redaction invariant failed: ${leaked.map((path) => basename(path)).sort().join(', ')}`);
    }
    writeFileSync(receiptPath, stableJson({ schema_version: SCHEMA, status: 'complete', snapshot_id: identity.snapshot_id, snapshot_time: identity.snapshot_time, manifest_sha256: sha256(readFileSync(join(output, 'manifest.json'))), output_bytes: budget.used }, true));
    return 0;
  } catch (error) {
    const message = redact(error?.message || error);
    writeFileSync(receiptPath, stableJson({ schema_version: SCHEMA, status: 'failed', snapshot_time: receiptTime, error: message }, true));
    if (v1ReceiptPath) writeFileSync(v1ReceiptPath, stableJson({ schema_version: 'current_ui_component_decoder_v1', status: 'failed', handoff_status: 'NO_GO', snapshot_time: receiptTime, error: message }, true));
    process.stderr.write(`Current UI Resource Graph failed: ${message}\n`);
    return 1;
  }
}

process.exitCode = await run(process.argv.slice(2));

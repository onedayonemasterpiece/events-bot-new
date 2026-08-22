#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { assertImmutableSha } from './immutable-checkout.mjs';

function parse(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i]; if (!key.startsWith('--')) throw new Error(`Unexpected argument: ${key}`);
    const value = argv[i + 1]; if (!value || value.startsWith('--')) throw new Error(`${key} requires a value`);
    out[key.slice(2)] = value; i += 1;
  }
  return out;
}
const sha = (value) => createHash('sha256').update(value).digest('hex');
const json = (path) => JSON.parse(readFileSync(resolve(path), 'utf8'));
const args = parse(process.argv.slice(2));
for (const key of ['case', 'resolved', 'capture', 'font-manifest', 'materialization-receipt', 'design-sha', 'output']) if (!args[key]) throw new Error(`--${key} is required`);
const caseRow = json(args.case); const resolvedCase = json(args.resolved); const captureDir = resolve(args.capture);
const font = json(`${captureDir}/font-preflight.json`); const astroReceipt = json(`${captureDir}/astro-capture-receipt.json`);
const materializationReceipt = json(args['materialization-receipt']);
const penpotPath = args.penpot ? resolve(args.penpot) : null;
const penpotReceipt = args['penpot-receipt'] ? json(args['penpot-receipt']) : null;
if (Boolean(penpotPath) !== Boolean(penpotReceipt)) throw new Error('--penpot and --penpot-receipt must be supplied together');
const expected = (actual, value, label) => { if (actual !== value) throw new Error(`Identity join mismatch for ${label}: ${String(actual)} != ${String(value)}`); };
assertImmutableSha(args['design-sha'], 'Design repository SHA');
for (const [actual, value, label] of [
  [resolvedCase.case_id, caseRow.case_id, 'case/resolved case_id'],
  [resolvedCase.component_id, caseRow.component_id, 'case/resolved component_id'],
  [resolvedCase.contract_version, caseRow.contract_version, 'case/resolved contract_version'],
  [resolvedCase.contract_sha256, caseRow.contract_sha256, 'case/resolved contract_sha256'],
  [resolvedCase.state_key, caseRow.state_key, 'case/resolved state_key'],
  [resolvedCase.event_fixture_id, caseRow.fixture_id, 'case/resolved fixture_id'],
  [resolvedCase.event_payload_sha256, caseRow.fixture_sha256, 'case/resolved fixture_sha256'],
  [resolvedCase.source_database_snapshot_fingerprint, caseRow.fixture_snapshot_sha256, 'case/resolved fixture snapshot'],
  [resolvedCase.asset_manifest_sha256, caseRow.asset_manifest_sha256, 'case/resolved asset manifest'],
  [resolvedCase.resolved_render_case_sha256, caseRow.resolved_render_case_sha256, 'case/resolved render hash'],
  [materializationReceipt.case_id, caseRow.case_id, 'materialization case_id'],
  [materializationReceipt.resolved_render_case_sha256, resolvedCase.resolved_render_case_sha256, 'materialization resolved hash'],
  [materializationReceipt.contract_sha256, resolvedCase.contract_sha256, 'materialization contract hash'],
  [materializationReceipt.corpus_id, resolvedCase.corpus_id, 'materialization corpus_id'],
  [materializationReceipt.corpus_sha256, resolvedCase.corpus_sha256, 'materialization corpus hash'],
  [materializationReceipt.event_fixture_id, resolvedCase.event_fixture_id, 'materialization fixture_id'],
  [materializationReceipt.event_payload_sha256, resolvedCase.event_payload_sha256, 'materialization fixture hash'],
  [materializationReceipt.asset_manifest_sha256, resolvedCase.asset_manifest_sha256, 'materialization asset manifest'],
  [astroReceipt.case_id, caseRow.case_id, 'capture case_id'],
  [astroReceipt.resolved_render_case_sha256, resolvedCase.resolved_render_case_sha256, 'capture resolved hash'],
]) expected(actual, value, label);
assertImmutableSha(materializationReceipt.astro_source_repository_sha, 'Materialized Astro source SHA');
assertImmutableSha(materializationReceipt.conformance_tooling_repository_sha, 'Materialized tooling SHA');
if (caseRow.astro_binding?.repository_sha) expected(materializationReceipt.astro_source_repository_sha, caseRow.astro_binding.repository_sha, 'case/materialization Astro SHA');
const fontManifest = json(args['font-manifest']);
expected(materializationReceipt.font_manifest_sha256, fontManifest.font_manifest_sha256, 'materialization font manifest');
const penpotBytesSha = penpotPath && existsSync(penpotPath) ? sha(readFileSync(penpotPath)) : null;
if (penpotReceipt) {
  for (const [actual, value, label] of [
    [penpotReceipt.case_id, caseRow.case_id, 'Penpot case_id'],
    [penpotReceipt.component_id, resolvedCase.component_id, 'Penpot component_id'],
    [penpotReceipt.contract_version, resolvedCase.contract_version, 'Penpot contract_version'],
    [penpotReceipt.contract_sha256, resolvedCase.contract_sha256, 'Penpot contract_sha256'],
    [penpotReceipt.state_key, resolvedCase.state_key, 'Penpot state_key'],
    [penpotReceipt.fixture_id, resolvedCase.event_fixture_id, 'Penpot fixture_id'],
    [penpotReceipt.fixture_sha256, resolvedCase.event_payload_sha256, 'Penpot fixture_sha256'],
    [penpotReceipt.fixture_snapshot_sha256, resolvedCase.source_database_snapshot_fingerprint, 'Penpot fixture snapshot'],
    [penpotReceipt.resolved_render_case_sha256, resolvedCase.resolved_render_case_sha256, 'Penpot resolved hash'],
    [penpotReceipt.asset_manifest_sha256, resolvedCase.asset_manifest_sha256, 'Penpot asset manifest'],
    [penpotReceipt.export_sha256, penpotBytesSha, 'Penpot export bytes'],
  ]) expected(actual, value, label);
  if (caseRow.penpot_binding?.file_id) expected(penpotReceipt.file_id, caseRow.penpot_binding.file_id, 'case/Penpot file_id');
  if (caseRow.penpot_binding?.page_id) expected(penpotReceipt.page_id, caseRow.penpot_binding.page_id, 'case/Penpot page_id');
  if (caseRow.penpot_binding?.board_or_component_id) expected(penpotReceipt.board_id ?? penpotReceipt.native_component_id, caseRow.penpot_binding.board_or_component_id, 'case/Penpot shape_id');
  if (caseRow.penpot_binding?.export_sha256) expected(penpotReceipt.export_sha256, caseRow.penpot_binding.export_sha256, 'case/Penpot export hash');
}
const tuple = {
  schema_version: 'ui_conformance_actual_tuple_v2', case_id: caseRow.case_id,
  design_repository_sha: args['design-sha'],
  astro_source_repository_sha: materializationReceipt.astro_source_repository_sha,
  conformance_tooling_repository_sha: materializationReceipt.conformance_tooling_repository_sha,
  component_id: resolvedCase.component_id, contract_version: resolvedCase.contract_version,
  contract_sha256: resolvedCase.contract_sha256, state_key: resolvedCase.state_key,
  fixture_id: resolvedCase.event_fixture_id, fixture_sha256: resolvedCase.event_payload_sha256,
  fixture_snapshot_sha256: resolvedCase.source_database_snapshot_fingerprint, fixture_mode: caseRow.fixture_mode,
  penpot_component_id: penpotReceipt?.component_id ?? null,
  penpot_state_key: penpotReceipt?.state_key ?? null,
  penpot_renderable_native_surface: penpotReceipt?.renderable_native_surface === true,
  penpot_fixture_id: penpotReceipt?.fixture_id ?? null,
  penpot_fixture_sha256: penpotReceipt?.fixture_sha256 ?? null,
  penpot_fixture_snapshot_sha256: penpotReceipt?.fixture_snapshot_sha256 ?? null,
  penpot_resolved_render_case_sha256: penpotReceipt?.resolved_render_case_sha256 ?? null,
  viewport_id: resolvedCase.viewport_id, viewport_width: resolvedCase.container_geometry.viewport_width,
  viewport_height: resolvedCase.container_geometry.viewport_height, container_width: resolvedCase.container_geometry.container_width,
  device_scale_factor: resolvedCase.container_geometry.device_scale_factor,
  font_loaded: font.font_loaded === true,
  font_manifest_sha256: fontManifest.font_manifest_sha256,
  font_manifest_file_sha256: sha(readFileSync(resolve(args['font-manifest']))),
  expected_asset_manifest_sha256: resolvedCase.asset_manifest_sha256,
  asset_manifest_sha256: materializationReceipt.asset_manifest_sha256,
  verified_assets: materializationReceipt.verified_assets,
  penpot_asset_manifest_sha256: penpotReceipt?.asset_manifest_sha256 ?? null,
  penpot_export_sha256: penpotBytesSha,
  astro_screenshot_sha256: astroReceipt.screenshot_sha256,
  astro_selected_card_screenshot_sha256: astroReceipt.selected_card_screenshot_sha256 ?? astroReceipt.screenshot_sha256,
  astro_parent_archetype_screenshot_sha256: astroReceipt.parent_archetype_screenshot_sha256 ?? null,
  archetype_validation: astroReceipt.archetype_validation ?? null,
  resolved_render_case_sha256: resolvedCase.resolved_render_case_sha256,
  evidence: {
    font_preflight: `${captureDir}/font-preflight.json`,
    astro_capture_receipt: `${captureDir}/astro-capture-receipt.json`,
    penpot_export: penpotPath,
    penpot_export_receipt: args['penpot-receipt'] ? resolve(args['penpot-receipt']) : null,
  },
  created_at: new Date().toISOString(),
};
mkdirSync(dirname(resolve(args.output)), { recursive: true }); writeFileSync(resolve(args.output), `${JSON.stringify(tuple, null, 2)}\n`);
process.stdout.write(`${JSON.stringify({ output: resolve(args.output), tuple_sha256: sha(JSON.stringify(tuple)), font_loaded: tuple.font_loaded, penpot_export_sha256: tuple.penpot_export_sha256 }, null, 2)}\n`);

#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { loadPreviewEventCatalog, resolvePreviewEventFixture } from '../current_ui_resource_graph/v1/specimens/fixtures.mjs';
import { stableHash } from '../current_ui_resource_graph/v1/specimens/validate.mjs';

function parse(argv) {
  const values = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i]; if (!key.startsWith('--')) throw new Error(`Unexpected argument: ${key}`);
    const value = argv[i + 1];
    if (!value || value.startsWith('--')) values[key.slice(2)] = true;
    else { values[key.slice(2)] = value; i += 1; }
  }
  return values;
}
const sha = (value) => createHash('sha256').update(value).digest('hex');
const args = parse(process.argv.slice(2));
if (!args.case || !args.site || !args.output) throw new Error('--case, --site and --output are required');
const caseRow = JSON.parse(readFileSync(resolve(args.case), 'utf8'));
const site = resolve(args.site); const sourcePath = resolve(site, 'src/data/preview-events.json');
const catalog = loadPreviewEventCatalog(site); const fresh = args['fresh-event'] === true;
let fixtureId = caseRow.fixture_id; let event = null; let fixtureTrace = null;
if (fresh) {
  const profile = String(args['fixture-profile'] || 'photo-card');
  const assets = (row) => Array.isArray(row.image_assets) ? row.image_assets : [];
  const accepts = (row) => {
    const list = assets(row);
    if (profile === 'photo-card') return list.some((a) => (a.image_text_mode || row.image_text_mode) === 'visual_only' && (a.safe_crop === true || a.crop_safe === true));
    if (profile === 'poster-card') return list.some((a) => Number(a.height) > Number(a.width) && (a.image_text_mode || row.image_text_mode) !== 'visual_only');
    if (profile === 'gallery') return list.length >= 3;
    if (profile === 'no-image') return list.length === 0 && !row.image_url;
    throw new Error(`Unsupported --fixture-profile: ${profile}`);
  };
  const selected = [...catalog.events].filter(accepts).sort((a, b) => Number(b.id) - Number(a.id))[0];
  if (!selected) throw new Error(`No deterministic fresh event for profile ${profile}`);
  fixtureId = `event.real.${selected.id}`;
}
if (fixtureId !== null) {
  const match = /^event\.real\.(\d+)$/u.exec(fixtureId);
  if (!match) throw new Error(`Unsupported event fixture id: ${caseRow.fixture_id}`);
  const resolved = resolvePreviewEventFixture(catalog, { catalog: 'preview-events', event_id: Number(match[1]) }, {});
  event = resolved.event; fixtureTrace = resolved.trace;
}
const assets = (event?.image_assets || []).map((asset) => ({
  src_sha256: sha(String(asset.src || '')), width: asset.width ?? null, height: asset.height ?? null,
  current_pixel_sha256: asset.current_pixel_sha256 || null, geometry_pixel_sha256: asset.geometry_pixel_sha256 || null,
  asset_key: asset.asset_key || null,
}));
const snapshotSha = event ? stableHash(event) : null;
const resolvedProps = caseRow.component_id === 'core.button'
  ? { variant: 'primary', size: 'default', state: 'default' }
  : caseRow.component_id === 'event.card'
    ? {
        variant: 'split-actions',
        desktopRelatedCrop: caseRow.viewport_id.startsWith('desktop'),
        mobileFlowMedia: caseRow.viewport_id.startsWith('mobile'),
      }
    : {};
const resolvedCase = {
  schema_version: 'resolved_render_case_v1', case_id: caseRow.case_id,
  component_id: caseRow.component_id, contract_version: caseRow.contract_version,
  contract_sha256: caseRow.contract_sha256, state_key: caseRow.state_key,
  fixture_id: fixtureId, fixture_sha256: event ? stableHash({ event, assets }) : null,
  fixture_source: event ? 'site/src/data/preview-events.json' : null, fixture_mode: fresh ? 'fresh-advisory' : (caseRow.fixture_mode || 'blocking-golden'),
  fixture_snapshot_sha256: snapshotSha, fixture_source_file_sha256: event ? sha(readFileSync(sourcePath)) : null,
  source_repository_sha: caseRow.astro_binding.repository_sha,
  event_id: event?.id ?? null, event, resolved_props: resolvedProps, assets,
  asset_manifest_sha256: stableHash(assets), fixture_trace: fixtureTrace,
  viewport: { id: caseRow.viewport_id, width: caseRow.viewport_width, height: caseRow.viewport_height, container_width: caseRow.container_width, device_scale_factor: caseRow.device_scale_factor },
  authority_mode: caseRow.authority_mode, conformance_profile: caseRow.conformance_profile,
};
resolvedCase.resolved_render_case_sha256 = stableHash(resolvedCase);
mkdirSync(dirname(resolve(args.output)), { recursive: true }); writeFileSync(resolve(args.output), `${JSON.stringify(resolvedCase, null, 2)}\n`);
if (fresh && args['effective-case-output']) {
  const effectiveCase = structuredClone(caseRow); effectiveCase.fixture_id = fixtureId; effectiveCase.fixture_sha256 = resolvedCase.fixture_sha256;
  effectiveCase.fixture_snapshot_sha256 = snapshotSha; effectiveCase.fixture_source = `events-bot-new/site/src/data/preview-events.json@${caseRow.astro_binding.repository_sha}`;
  effectiveCase.fixture_mode = 'fresh-advisory';
  mkdirSync(dirname(resolve(args['effective-case-output'])), { recursive: true }); writeFileSync(resolve(args['effective-case-output']), `${JSON.stringify(effectiveCase, null, 2)}\n`);
}
process.stdout.write(`${JSON.stringify({ output: resolve(args.output), resolved_render_case_sha256: resolvedCase.resolved_render_case_sha256, fixture_snapshot_sha256: snapshotSha, fixture_sha256: resolvedCase.fixture_sha256 }, null, 2)}\n`);

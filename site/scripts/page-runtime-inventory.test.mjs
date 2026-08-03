import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { buildPageRuntimeInventory } from './check-page-runtime-inventory.mjs';

const onboarding = '<span data-standard-onboarding-runtime="standard-onboarding-context-v1" data-standard-onboarding-context="information" data-standard-onboarding-slot="page_end" data-standard-onboarding-mode="inert" data-standard-onboarding-artifact-program="disabled" data-standard-onboarding-club-program="disabled" data-standard-onboarding-raffle-program="disabled"></span>';
const page = (extra='', includeOnboarding=true) => `<!doctype html><html><body><span data-p13n-runtime-marker="p13n-runtime-v1" data-p13n-static-only-reason="explicit"></span>${extra}${includeOnboarding ? onboarding : ''}<script>"data-static-site-auth-runtime"</script></body></html>`;

test('runtime inventory distinguishes shared, diagnostic, static and non-html routes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'page-runtime-'));
  for (const name of ['shared','diagnostic','static','data.json','lab/specimen']) await mkdir(join(root,name), { recursive:true });
  await writeFile(join(root,'shared/index.html'), page('<i data-static-site-auth-runtime></i>'));
  await writeFile(join(root,'diagnostic/index.html'), page('<main data-focus-connectivity></main>'));
  await writeFile(join(root,'static/index.html'), page());
  await writeFile(join(root,'data.json/index.html'), '{"ok":true}');
  await writeFile(join(root,'lab/specimen/index.html'), '<!doctype html><html></html>');
  await mkdir(join(root,'data'), { recursive:true });
  await mkdir(join(root,'event'), { recursive:true });
  await writeFile(join(root,'data/feed.json'), '{"items":[]}');
  await writeFile(join(root,'event/item.ics'), 'BEGIN:VCALENDAR\nEND:VCALENDAR\n');
  await writeFile(join(root,'pwa-sw.js'), 'self.addEventListener("fetch",()=>{});');
  await writeFile(join(root,'manifest.webmanifest'), '{}');
  await mkdir(join(root,'fokus-gruppa'), { recursive:true });
  await writeFile(join(root,'fokus-gruppa/index.html'), page('<i data-static-site-auth-runtime></i>', false));
  const inventory = buildPageRuntimeInventory(root);
  assert.equal(inventory.counts.shared_auth_transport, 2);
  assert.equal(inventory.counts.specialized_diagnostic_transport, 1);
  assert.equal(inventory.counts.explicit_static_only, 1);
  assert.equal(inventory.counts.excluded_lab_html, 1);
  assert.equal(inventory.counts.excluded_generated_non_html_html, 1);
  assert.equal(inventory.counts.excluded_non_html_json, 1);
  assert.equal(inventory.counts.excluded_non_html_ics, 1);
  assert.equal(inventory.counts.excluded_service_worker, 1);
  assert.equal(inventory.counts.excluded_webmanifest, 1);
  assert.equal(inventory.counts.excluded_lab_or_non_html, 6);
  assert.equal(inventory.counts.standard_onboarding_eligible_html, 3);
  assert.equal(inventory.counts.standard_onboarding_inert_ok, 3);
  assert.equal(inventory.counts.standard_onboarding_focus_boundary_excluded, 1);
  assert.equal(inventory.counts.standard_onboarding_failures, 0);
  assert.equal(inventory.counts.failures, 0);
});

test('runtime inventory strips the generated preview-token directory from public paths', async () => {
  const root = await mkdtemp(join(tmpdir(), 'page-runtime-preview-'));
  await mkdir(join(root, 'preview-secret_candidate.42', 'lab', 'specimen'), { recursive:true });
  await mkdir(join(root, 'preview-secret_candidate.42', 'events'), { recursive:true });
  await writeFile(join(root, 'preview-secret_candidate.42', 'lab', 'specimen', 'index.html'), '<!doctype html><html></html>');
  await writeFile(join(root, 'preview-secret_candidate.42', 'events', 'index.html'), page());
  const inventory = buildPageRuntimeInventory(root);
  assert.equal(inventory.pages.find((item) => item.relative_path.endsWith('lab/specimen/index.html'))?.public_path, '/lab/specimen/');
  assert.equal(inventory.pages.find((item) => item.relative_path.endsWith('events/index.html'))?.public_path, '/events/');
  assert.equal(inventory.counts.excluded_lab_or_non_html, 1);
  assert.equal(inventory.counts.failures, 0);
});

test('runtime inventory fails an enabled gated onboarding programme', async () => {
  const root = await mkdtemp(join(tmpdir(), 'page-runtime-onboarding-gate-'));
  await mkdir(join(root, 'events'), { recursive:true });
  await writeFile(join(root, 'events/index.html'), page().replace('data-standard-onboarding-artifact-program="disabled"', 'data-standard-onboarding-artifact-program="enabled"'));
  const inventory = buildPageRuntimeInventory(root);
  assert.equal(inventory.counts.standard_onboarding_failures, 1);
  assert.match(inventory.pages[0].failures.join(','), /standard_onboarding_gated_program_enabled/u);
});

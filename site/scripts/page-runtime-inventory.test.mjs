import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { buildPageRuntimeInventory } from './check-page-runtime-inventory.mjs';

const page = (extra='') => `<!doctype html><html><body><span data-p13n-runtime-marker="p13n-runtime-v1" data-p13n-static-only-reason="explicit"></span>${extra}<script>"data-static-site-auth-runtime"</script></body></html>`;

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
  const inventory = buildPageRuntimeInventory(root);
  assert.equal(inventory.counts.shared_auth_transport, 1);
  assert.equal(inventory.counts.specialized_diagnostic_transport, 1);
  assert.equal(inventory.counts.explicit_static_only, 1);
  assert.equal(inventory.counts.excluded_lab_html, 1);
  assert.equal(inventory.counts.excluded_generated_non_html_html, 1);
  assert.equal(inventory.counts.excluded_non_html_json, 1);
  assert.equal(inventory.counts.excluded_non_html_ics, 1);
  assert.equal(inventory.counts.excluded_service_worker, 1);
  assert.equal(inventory.counts.excluded_webmanifest, 1);
  assert.equal(inventory.counts.excluded_lab_or_non_html, 6);
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

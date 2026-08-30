import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';

const root = process.env.ARTIFACT_GENERATED_ROOT;
if (!root) throw new Error('ARTIFACT_GENERATED_ROOT is required');

test('generated immutable research preview contains one real current-weekend artifact and no adjacent rerolls', async () => {
  const currentHtml = await readFile(path.join(root, 'vyhodnye/index.html'), 'utf8');
  assert.equal((currentHtml.match(/data-amber-artifact(?:\s|>)/gu) || []).length, 1);
  const assigned = /data-amber-artifact-event-id="(\d+)"/u.exec(currentHtml)?.[1];
  assert.ok(assigned, 'generated current weekend must expose its selected real event id');
  assert.match(currentHtml, new RegExp(`data-mobile-listing-row[^>]+data-event(?:-id)?="${assigned}"`, 'u'));
  assert.match(currentHtml, /class="event-like-cta"[\s\S]*data-amber-artifact/u);

  const adjacentRoot = path.join(root, 'vyhodnye');
  const entries = await readdir(adjacentRoot, { withFileTypes: true });
  const adjacent = entries.filter((entry) => entry.isDirectory() && /^\d{4}-\d{2}-\d{2}$/u.test(entry.name));
  assert.ok(adjacent.length > 0, 'fixture build must contain adjacent weekend routes');
  for (const entry of adjacent) {
    const html = await readFile(path.join(adjacentRoot, entry.name, 'index.html'), 'utf8');
    assert.equal((html.match(/data-amber-artifact(?:\s|>)/gu) || []).length, 0, entry.name);
  }
});

test('generated collection is noindex and retains exact-seven local-only/detail contracts', async () => {
  const html = await readFile(path.join(root, 'artefakty/index.html'), 'utf8');
  const robots = /name="robots" content="([^"]+)"/u.exec(html)?.[1]?.split(',') || [];
  assert.ok(['noindex', 'nofollow', 'noarchive'].every((directive) => robots.includes(directive)));
  assert.match(html, /data-artifact-collection/u);
  assert.equal((html.match(/data-artifact-slot=/gu) || []).length, 7);
  assert.match(html, /data-artifact-visual-donor="008839b14598105d1fed5b4e386d6d6f29d93d1f"/u);
  assert.match(html, /Знаки Янтарного края/u);
  assert.match(html, /Только на этом устройстве/u);
  assert.match(html, /<dialog[^>]+data-artifact-dialog/u);
  assert.doesNotMatch(html, /Поделиться артефактом · скоро/u);
});

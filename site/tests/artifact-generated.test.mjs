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

test('generated concrete collection is noindex and retains the 1-found/7-private detail contract', async () => {
  const html = await readFile(
    path.join(root, 'artefakty/kollektsii/znaki-yantarnogo-kraya/index.html'),
    'utf8',
  );
  assert.match(html, /name="robots" content="noindex,nofollow,noarchive"/u);
  assert.match(html, /data-artifact-collection-progress/u);
  assert.equal((html.match(/data-artifact-state="locked"/gu) || []).length, 7);
  assert.equal((html.match(/data-artifact-state="found"/gu) || []).length, 1);
  assert.match(html, /ни его название, ни изображение/u);
  assert.match(html, /<dialog[^>]+data-artifact-dialog/u);
  assert.match(html, /mailto:info@kenigevents\.ru/u);
});

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const read = (relativePath) => readFile(path.join(siteRoot, relativePath), 'utf8');

test('EventMediaRail normalizes displayable media, srcset candidates and rejected input census', async () => {
  const source = await read('src/components/EventMediaRail.astro');

  assert.match(source, /const normalizedMediaUrl = \(value: string \| null \| undefined\) =>/u);
  assert.match(source, /if \(\/\^\(\?:data\|blob\):\/iu\.test\(raw\)\) return raw;/u);
  assert.match(source, /return eventImageUrl\(raw\) \|\| raw;/u);
  assert.match(source, /const normalizedSrcset = \(value: string \| null \| undefined\) =>/u);
  assert.match(source, /const candidates = raw\.split\(','\)\.map/u);
  assert.match(source, /\(\\S\+\)\(\?:\\s\+\(\\d\+\(\?:\\\.\\d\+\)\?\[wx\]\)\)\?/u);
  assert.match(source, /return candidates\.length \? candidates\.join\(', '\) : undefined;/u);
  assert.match(source, /const normalizedThumbnailSources = \(asset: EventImageAsset\)/u);
  assert.match(source, /src: normalizedMediaUrl\(item\.src\)/u);
  assert.match(source, /\.filter\(\(item\): item is \{ src: string; width: number; height: number \} => Boolean\(item\.src && item\.width && item\.height\)\)/u);
  assert.match(source, /const thumbnailSrc = normalizedMediaUrl\(item\.thumbnailSrc\) \|\| src;/u);
  assert.match(source, /thumbnailSrcset: normalizedSrcset\(item\.thumbnailSrcset\)/u);
  assert.match(source, /const normalizedItems = normalizedCandidates\.filter\(\(item\) => Boolean\(item\.src && item\.thumbnailSrc\)\);/u);
  assert.match(source, /const rejectedCount = normalizedCandidates\.length - normalizedItems\.length;/u);
  assert.match(source, /data-event-media-rail-input-total=\{normalizedCandidates\.length\}/u);
  assert.match(source, /data-event-media-rail-total=\{normalizedItems\.length\}/u);
  assert.match(source, /data-event-media-rail-rejected=\{rejectedCount \|\| undefined\}/u);
});

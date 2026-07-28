import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const read = (path) => readFile(new URL(`../${path}`, import.meta.url), 'utf8');

test('#780 reaction rerank leaves the clicked viewport prefix attached', async () => {
  const source = await read('src/layouts/EventLayout.astro');
  const start = source.indexOf('function reorderExistingCards');
  const end = source.indexOf('function appendDiscoveryEvents', start);
  const reorder = source.slice(start, end);

  assert.match(reorder, /const tail = sortCards\(cards\.slice\(anchorIndex \+ 1\)\)/u);
  assert.match(reorder, /tail\.forEach\(\(card\) => feed\.appendChild\(card\)\)/u);
  assert.doesNotMatch(reorder, /const frozen|\.{3}frozen/u);
});

test('#787 local favorites become ready before cloud reconciliation settles', async () => {
  const source = await read('src/components/FavoritesSurface.astro');
  const localRender = source.indexOf('await renderJoined({ catalog, local, snapshot })');
  const localReady = source.indexOf("setHydrationState('ready')", localRender);
  const cloudWait = source.indexOf('const remote = await remotePromise', localReady);

  assert.match(source, /data-favorites-state="loading"/u);
  assert.match(source, /skeleton\.hidden = state !== 'loading'/u);
  assert.match(source, /favoritesLocalReadyMs/u);
  assert.ok(localRender >= 0, 'local render must exist');
  assert.ok(localReady > localRender, 'ready state must follow the local render');
  assert.ok(cloudWait > localReady, 'cloud reconciliation must not block local ready state');
});

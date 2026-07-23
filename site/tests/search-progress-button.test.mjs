import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const search = read('src/components/AuthorizedEventSearch.astro');
const layout = read('src/layouts/EventLayout.astro');

test('R8 keeps the accepted v28 in-button progress geometry and semantics', () => {
  assert.match(
    layout,
    /\.authorized-search--standalone \.authorized-search__submit \{[\s\S]*?width:\s*100%;[\s\S]*?min-height:\s*50px;[\s\S]*?border-radius:\s*8px;[\s\S]*?background:\s*#221a14;/u,
  );
  assert.match(
    layout,
    /\.authorized-search--standalone \.authorized-search__submit::before \{[\s\S]*?background:\s*#98401f;/u,
  );
  assert.match(
    layout,
    /@keyframes authorized-search-submit-indeterminate \{[\s\S]*?translateX\(-70%\)[\s\S]*?translateX\(180%\)/u,
  );
  assert.match(search, /data-search-submit/u);
  assert.match(search, /data-search-progress role="progressbar"[^>]*aria-label="Ход поиска"[^>]*aria-valuemin="0"[^>]*aria-valuemax="100"/u);
  assert.match(search, /data-search-progress-label role="status" aria-live="polite" aria-atomic="true"/u);
  assert.match(search, /submit\.setAttribute\('aria-busy', isLoading \? 'true' : 'false'\)/u);
  assert.match(search, /submitLabel\.textContent = progressValue >= 100 \? 'Готово' : 'Ищу…'/u);
});

test('R8 guards duplicate submits and owns success/error/abort/reset transitions', () => {
  assert.match(search, /async function runSearch\([^)]*\) \{\s*if \(!supabase \|\| loading \|\| searchStartPending\) return;/u);
  assert.match(search, /searchStartPending = true;[\s\S]*?await supabase\.auth\.getSession\(\)/u);
  assert.match(search, /activeSearchController = controller;\s*searchStartPending = false;\s*setSearchLoading\(true/u);
  assert.match(search, /submit\.disabled = isLoading/u);
  assert.match(search, /progressValue = Math\.max\(progressValue,/u);
  assert.match(search, /if \(nextStageRank < progressStageRank\) return/u);
  assert.match(search, /if \(completed\) scheduleSearchProgressReset\(epoch\);\s*else resetSearchProgress\(epoch\);/u);
  assert.match(search, /function abortActiveSearch\(\)/u);
  assert.match(search, /const pendingSessionCheck = searchStartPending/u);
  assert.match(search, /searchEpoch \+= 1;[\s\S]*?searchStartPending = false;[\s\S]*?controller\?\.abort\(\);[\s\S]*?resetSearchProgress\(searchEpoch\)/u);
  assert.match(search, /const sessionCheckEpoch = searchEpoch;\s*searchStartPending = true/u);
  assert.match(search, /if \(sessionCheckEpoch !== searchEpoch \|\| !searchStartPending\) return/u);
  assert.match(search, /logout\?\.addEventListener\('click', async \(\) => \{\s*abortActiveSearch\(\);/u);
  assert.match(search, /window\.addEventListener\('pagehide', abortActiveSearch, \{ once: true \}\)/u);
});

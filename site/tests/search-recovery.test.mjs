import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

const search = readFileSync(
  new URL('../src/components/AuthorizedEventSearch.astro', import.meta.url),
  'utf8',
);

function extractFunction(source, name) {
  const start = source.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `${name} must exist`);
  const declarationStart = source.slice(start - 6, start) === 'async ' ? start - 6 : start;
  const openingBrace = source.indexOf(') {', start) + 2;
  assert.ok(openingBrace > 1, `${name} opening brace must exist`);
  let depth = 0;
  for (let index = openingBrace; index < source.length; index += 1) {
    if (source[index] === '{') depth += 1;
    if (source[index] === '}') depth -= 1;
    if (depth === 0) return source.slice(declarationStart, index + 1);
  }
  assert.fail(`${name} must have a balanced function body`);
}

function transportHarness(fetchImpl) {
  const sources = [
    extractFunction(search, 'timeoutError'),
    extractFunction(search, 'abortError'),
    extractFunction(search, 'fetchWithSearchHeadersTimeout'),
    extractFunction(search, 'readSearchChunkWithTimeout'),
  ].join('\n');
  return new Function(
    'fetch',
    'window',
    'searchFetchHeadersTimeoutMs',
    'searchStreamIdleTimeoutMs',
    `${sources}; return { fetchWithSearchHeadersTimeout, readSearchChunkWithTimeout };`,
  )(
    fetchImpl,
    { setTimeout, clearTimeout },
    30,
    30,
  );
}

test('fetch/header watchdog rejects even when fetch ignores AbortSignal forever', async () => {
  let capturedSignal;
  const { fetchWithSearchHeadersTimeout } = transportHarness((_endpoint, init) => {
    capturedSignal = init.signal;
    return new Promise(() => {});
  });

  await assert.rejects(
    fetchWithSearchHeadersTimeout('/event-search', { method: 'POST' }, { signal: new AbortController().signal }),
    /search_fetch_headers_timeout/u,
  );
  assert.equal(capturedSignal.aborted, true, 'the watchdog aborts its coordinated request controller');
});

test('parent epoch cancellation still reaches the response stream after headers resolve', async () => {
  let capturedSignal;
  const { fetchWithSearchHeadersTimeout } = transportHarness((_endpoint, init) => {
    capturedSignal = init.signal;
    return Promise.resolve({ ok: true, headers: new Headers() });
  });
  const controller = new AbortController();

  await fetchWithSearchHeadersTimeout('/event-search', { method: 'POST' }, { signal: controller.signal });
  assert.equal(capturedSignal.aborted, false);
  controller.abort(new DOMException('Page hidden', 'AbortError'));
  assert.equal(capturedSignal.aborted, true, 'pagehide/logout abort remains linked after the header promise settles');
});

test('every reader.read receives a fresh idle timeout after a delivered chunk', async () => {
  let reads = 0;
  const reader = {
    read() {
      reads += 1;
      if (reads === 1) return Promise.resolve({ value: new Uint8Array([1]), done: false });
      return new Promise(() => {});
    },
  };
  const { readSearchChunkWithTimeout } = transportHarness(() => Promise.reject(new Error('unused')));
  const signal = new AbortController().signal;

  assert.deepEqual(
    await readSearchChunkWithTimeout(reader, signal),
    { value: new Uint8Array([1]), done: false },
  );
  assert.deepEqual(
    await readSearchChunkWithTimeout(reader, signal),
    { streamIdleTimedOut: true },
  );
  assert.equal(reads, 2);
});

test('stream rescue and overall watchdog are bounded and cleanup remains epoch-owned', () => {
  const invoke = extractFunction(search, 'invokeEventSearch');
  const run = extractFunction(search, 'runSearch');

  assert.match(invoke, /let jsonRescueAttempted = false/u);
  assert.match(invoke, /if \(jsonRescueAttempted\)[\s\S]*?jsonRescueAttempted = true/u);
  assert.match(invoke, /cancelSearchReader\(\)[\s\S]*?invokeEventSearchJson\([^;]+?'stream_stalled'/u);
  assert.match(invoke, /readSearchChunkWithTimeout\(reader, signal\)/u);
  assert.match(invoke, /if \(!finalData && !finalError\) \{\s*return rescueStalledStream\(\);/u);
  assert.match(run, /Promise\.race\(\[searchRequest, overallWatchdog\]\)/u);
  assert.match(run, /controller\.abort\(error\)/u);
  assert.match(
    run,
    /finally \{[\s\S]*?clearTimeout\(overallWatchdogTimer\)[\s\S]*?setSkeletonLoading\(false\)[\s\S]*?setMoreLoading\(false\)[\s\S]*?setSearchLoading\(false\)/u,
  );
});

test('account avatar uses the first identity grapheme and exposes signed-in identity', () => {
  const displayUserName = new Function(
    `${extractFunction(search, 'displayUserName')}; return displayUserName;`,
  )();
  const userInitial = new Function(
    `${extractFunction(search, 'userInitial')}; return userInitial;`,
  )();

  assert.equal(displayUserName({ email: 'alex@example.invalid', user_metadata: {} }), 'alex@example.invalid');
  assert.equal(
    displayUserName({ email: 'alex@example.invalid', user_metadata: { name: 'Жанна' } }),
    'Жанна',
  );
  assert.equal(
    displayUserName({ email: 'alex@example.invalid', user_metadata: { preferred_username: 'A' } }),
    'alex@example.invalid',
    'a one-letter provider username must not mask a known email identity',
  );
  assert.equal(userInitial('alex@example.invalid'), 'A');
  assert.equal(userInitial('жанна'), 'Ж');
  assert.equal(userInitial('👩‍💻 разработчик'), '👩‍💻');
  assert.doesNotMatch(search, /data-search-avatar-(?:img|icon)/u, 'opaque provider images and stacked icon fallbacks are absent');
  assert.match(search, /accountToggle\.setAttribute\('aria-label', `Аккаунт\. \$\{signedInLabel\}`\)/u);
  assert.match(search, /accountToggle\.setAttribute\('title', signedInLabel\)/u);
  assert.match(search, /if \(accountLabel\) accountLabel\.textContent = name/u);
  assert.match(search, /<span>Вошли как<\/span>\s*<strong data-search-user-name>/u);
});

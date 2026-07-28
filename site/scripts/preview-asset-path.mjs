/**
 * Resolve a generated Astro runtime URL back to the local preview tree.
 *
 * A production-candidate preview gate may render runtime URLs against an
 * immutable CDN prefix whose build id differs from the ephemeral
 * `preview-gate-*` id. The checked bytes still live in the local `_astro`
 * directory, so use the bounded runtime suffix rather than assuming both
 * prefixes are identical.
 */
export function localPreviewRuntimePath(href) {
  let pathname = '';
  try {
    pathname = new URL(String(href || ''), 'https://preview.invalid/').pathname;
  } catch {
    return null;
  }
  const marker = '/_astro/';
  const markerIndex = pathname.lastIndexOf(marker);
  if (markerIndex < 0) return null;
  const suffix = pathname.slice(markerIndex + 1);
  if (
    !/^_astro\/[A-Za-z0-9._/-]+$/u.test(suffix)
    || suffix.includes('/../')
    || suffix.endsWith('/')
  ) {
    return null;
  }
  return suffix;
}

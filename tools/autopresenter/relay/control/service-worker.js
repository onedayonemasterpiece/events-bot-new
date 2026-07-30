const CACHE_VERSION = 'autopresenter-control-shell-v8';
const CONTROL_SHELL = [
  '/control/',
  '/control/auth-storage.js',
  '/control/manifest.webmanifest',
  '/control/icons/icon-192.png',
  '/control/icons/icon-512.png',
  '/control/icons/icon-maskable-512.png',
];
const CONTROL_SHELL_PATHS = new Set(CONTROL_SHELL);

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_VERSION).then((cache) => cache.addAll(CONTROL_SHELL)),
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((names) => Promise.all(
      names
        .filter((name) => name.startsWith('autopresenter-control-shell-') && name !== CACHE_VERSION)
        .map((name) => caches.delete(name)),
    )),
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const request = event.request;
  const url = new URL(request.url);

  // Bearer tokens only travel to /api/* in request headers. API requests,
  // non-GET requests, and cross-origin requests must never touch CacheStorage.
  if (
    request.method !== 'GET'
    || url.origin !== self.location.origin
    || url.pathname.startsWith('/api/')
  ) {
    return;
  }

  if (request.mode === 'navigate' && url.pathname.startsWith('/control/')) {
    event.respondWith(
      fetch(request).catch(() => caches.match('/control/')),
    );
    return;
  }

  // Cache only the fixed, token-free shell. Query variants are always fetched.
  if (!url.search && CONTROL_SHELL_PATHS.has(url.pathname)) {
    event.respondWith(
      caches.match(request).then((cached) => cached || fetch(request)),
    );
  }
});

import type { APIRoute } from 'astro';

const source = `
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  event.respondWith(fetch(event.request));
});
`;

export const GET: APIRoute = () => new Response(source.trimStart(), {
  headers: {
    'Content-Type': 'application/javascript; charset=utf-8',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Service-Worker-Allowed': '/',
  },
});

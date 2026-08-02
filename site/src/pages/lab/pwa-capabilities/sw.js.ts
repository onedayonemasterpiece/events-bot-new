import type { APIRoute } from 'astro';

const source = `
const LAB_PATH = '/lab/pwa-capabilities/';
const LAB_CACHE = 'kenigevents-pwa-capabilities-lab-v1';

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));

self.addEventListener('message', (event) => {
  if (event.data?.type !== 'SIMULATE_PUSH') return;
  const payload = event.data.payload || {};
  event.waitUntil(self.registration.showNotification(payload.title || 'PWA Lab: simulation', {
    body: payload.body || 'Simulated payload — не настоящий remote push.',
    tag: 'pwa-lab-simulation',
    icon: '/assets/pwa/announcements-192.png',
    data: { url: LAB_PATH, kind: 'simulation' },
  }));
});

self.addEventListener('push', (event) => {
  let payload = { title: 'PWA Lab: remote push', body: 'Получен Web Push payload.' };
  try { payload = { ...payload, ...(event.data?.json() || {}) }; } catch { payload.body = event.data?.text() || payload.body; }
  event.waitUntil(self.registration.showNotification(payload.title, {
    body: payload.body,
    tag: 'pwa-lab-remote',
    icon: '/assets/pwa/announcements-192.png',
    data: { url: LAB_PATH, kind: 'remote' },
  }));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clients) => {
    const existing = clients.find((client) => new URL(client.url).pathname === LAB_PATH);
    return existing ? existing.focus() : self.clients.openWindow(LAB_PATH);
  }));
});

self.addEventListener('periodicsync', (event) => {
  if (event.tag !== 'pwa-capabilities-lab-refresh') return;
  event.waitUntil(caches.open(LAB_CACHE));
});
`;

export const GET: APIRoute = () => new Response(source.trimStart(), {
  headers: {
    'Content-Type': 'application/javascript; charset=utf-8',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Service-Worker-Allowed': '/lab/pwa-capabilities/',
  },
});

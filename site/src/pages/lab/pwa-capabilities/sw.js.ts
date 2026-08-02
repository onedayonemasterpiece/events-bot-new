import type { APIRoute } from 'astro';
import { withBase } from '../../../lib/events';

const labPath = withBase('/lab/pwa-capabilities/');
const iconUrl = withBase('/assets/pwa/announcements-192.png');
const labCache = `kenigevents-pwa-capabilities-lab-${encodeURIComponent(labPath)}`;

const source = `
const LAB_PATH = ${JSON.stringify(labPath)};
const LAB_ICON_URL = ${JSON.stringify(iconUrl)};
const LAB_CACHE = ${JSON.stringify(labCache)};

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (event) => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  event.respondWith(fetch(event.request));
});

self.addEventListener('message', (event) => {
  if (event.data?.type !== 'SIMULATE_PUSH') return;
  const payload = event.data.payload || {};
  event.waitUntil(self.registration.showNotification(payload.title || 'PWA Lab: simulation', {
    body: payload.body || 'Simulated payload — не настоящий remote push.',
    tag: 'pwa-lab-simulation',
    icon: LAB_ICON_URL,
    data: { url: LAB_PATH, kind: 'simulation' },
  }));
});

self.addEventListener('push', (event) => {
  let payload = { title: 'PWA Lab: remote push', body: 'Получен Web Push payload.' };
  try { payload = { ...payload, ...(event.data?.json() || {}) }; } catch { payload.body = event.data?.text() || payload.body; }
  event.waitUntil(self.registration.showNotification(payload.title, {
    body: payload.body,
    tag: 'pwa-lab-remote',
    icon: LAB_ICON_URL,
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
  },
});

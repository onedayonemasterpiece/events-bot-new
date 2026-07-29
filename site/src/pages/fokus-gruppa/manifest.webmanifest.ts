import type { APIRoute } from 'astro';
import { withBase } from '../../lib/events';

export const GET: APIRoute = () => {
  const scope = withBase('/');
  const manifest = {
    id: scope,
    name: 'Анонсы',
    short_name: 'Анонсы',
    description: 'Полюбить Калининград: события города и области — с понятным маршрутом к следующему впечатлению.',
    lang: 'ru',
    dir: 'ltr',
    start_url: withBase('/?launch=pwa'),
    scope,
    display: 'standalone',
    background_color: '#fbf7ef',
    theme_color: '#98401f',
    prefer_related_applications: false,
    related_applications: [
      {
        platform: 'webapp',
        url: withBase('/fokus-gruppa/manifest.webmanifest'),
        id: scope,
      },
    ],
    launch_handler: {
      client_mode: 'navigate-existing',
    },
    icons: [
      {
        src: withBase('/assets/pwa/announcements-brand-v2-192.png'),
        sizes: '192x192',
        type: 'image/png',
        purpose: 'any',
      },
      {
        src: withBase('/assets/pwa/announcements-brand-v2-512.png'),
        sizes: '512x512',
        type: 'image/png',
        purpose: 'any',
      },
      {
        src: withBase('/assets/pwa/announcements-brand-v2-maskable-192.png'),
        sizes: '192x192',
        type: 'image/png',
        purpose: 'maskable',
      },
      {
        src: withBase('/assets/pwa/announcements-brand-v2-maskable-512.png'),
        sizes: '512x512',
        type: 'image/png',
        purpose: 'maskable',
      },
    ],
  };

  return new Response(JSON.stringify(manifest), {
    headers: {
      'Content-Type': 'application/manifest+json; charset=utf-8',
      'Cache-Control': 'public, max-age=300, must-revalidate',
    },
  });
};

import type { APIRoute } from 'astro';
import { withBase } from '../../lib/events';

export const GET: APIRoute = () => {
  const scope = withBase('/');
  // Preserve the already installed manifest identity: changing it would make
  // Chromium treat this as a different application. Branding and destination
  // are the ordinary product; the controller only resumes unfinished invites.
  const startUrl = withBase('/fokus-gruppa/priglashenie/?launch=pwa');
  const secretUrl = withBase('/zakrytaya-afisha/');
  const manifest = {
    id: withBase('/fokus-gruppa/pwa'),
    name: 'Анонсы',
    short_name: 'Анонсы',
    description: 'Полюбить Калининград: события города и области — с понятным маршрутом к следующему впечатлению.',
    lang: 'ru',
    dir: 'ltr',
    start_url: startUrl,
    scope,
    display: 'standalone',
    background_color: '#fbf7ef',
    theme_color: '#98401f',
    prefer_related_applications: false,
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
    shortcuts: [
      {
        name: 'Закрытая афиша',
        short_name: 'Афиша',
        url: secretUrl,
        icons: [
          {
            src: withBase('/assets/pwa/announcements-brand-v2-192.png'),
            sizes: '192x192',
            type: 'image/png',
          },
        ],
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

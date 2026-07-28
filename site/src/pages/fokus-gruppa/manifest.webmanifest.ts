import type { APIRoute } from 'astro';
import { withBase } from '../../lib/events';

export const GET: APIRoute = () => {
  const scope = withBase('/');
  // A static manifest cannot inspect localStorage. This tiny onboarding route
  // is the start controller: active local participation immediately replaces
  // it with /zakrytaya-afisha/, while absent/pending state stays on onboarding.
  const startUrl = withBase('/fokus-gruppa/priglashenie/?launch=pwa');
  const secretUrl = withBase('/zakrytaya-afisha/');
  const manifest = {
    id: withBase('/fokus-gruppa/pwa'),
    name: 'Анонсы · фокус-группа',
    short_name: 'Анонсы Lab',
    description: 'Мобильная афиша для участников исследовательского периода.',
    lang: 'ru',
    dir: 'ltr',
    start_url: startUrl,
    scope,
    display: 'standalone',
    background_color: '#fff7e7',
    theme_color: '#98401f',
    prefer_related_applications: false,
    icons: [
      {
        src: withBase('/assets/pwa/focus-group-icon.png'),
        sizes: '1254x1254',
        type: 'image/png',
        purpose: 'any',
      },
      // Keep exact 192/512 installability fallbacks without altering the
      // supplied focus-group artwork.
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
    ],
    shortcuts: [
      {
        name: 'Закрытая афиша',
        short_name: 'Афиша',
        url: secretUrl,
        icons: [
          {
            src: withBase('/assets/pwa/focus-group-icon.png'),
            sizes: '1254x1254',
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

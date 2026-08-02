import type { APIRoute } from 'astro';
import { withBase } from '../../../lib/events';

export const GET: APIRoute = () => {
  const labUrl = withBase('/lab/pwa-capabilities/');
  const manifest = {
    id: labUrl,
    name: 'KenigEvents PWA Capabilities Lab',
    short_name: 'PWA Lab',
    description: 'Изолированная лаборатория browser PWA-возможностей KenigEvents.',
    lang: 'ru',
    dir: 'ltr',
    start_url: labUrl,
    scope: labUrl,
    display: 'standalone',
    background_color: '#f5ede1',
    theme_color: '#9d3f1c',
    prefer_related_applications: false,
    icons: [
      {
        src: withBase('/assets/pwa/announcements-192.png'),
        sizes: '192x192',
        type: 'image/png',
        purpose: 'any',
      },
      {
        src: withBase('/assets/pwa/announcements-512.png'),
        sizes: '512x512',
        type: 'image/png',
        purpose: 'any',
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

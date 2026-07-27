import type { APIRoute } from 'astro';
import { siteHomeHref, withBase } from '../lib/events';

export const GET: APIRoute = () => {
  const scope = withBase('/');
  const configuredStartUrl = String(import.meta.env.PUBLIC_PWA_START_URL || '').trim();
  const startUrl = configuredStartUrl || siteHomeHref();
  if (!startUrl.startsWith('/') || startUrl.startsWith('//') || !startUrl.startsWith(scope)) {
    throw new Error(`PUBLIC_PWA_START_URL must stay inside manifest scope ${scope}`);
  }
  const manifest = {
    id:scope,
    name:'Полюбить Калининград — Анонсы',
    short_name:'Анонсы',
    description:'События Калининграда и области — с понятным маршрутом к следующему впечатлению.',
    lang:'ru',
    dir:'ltr',
    start_url:startUrl,
    scope,
    display:'standalone',
    background_color:'#fbf7ef',
    theme_color:'#98401f',
    prefer_related_applications:false,
    icons:[
      {
        src:withBase('/assets/pwa/announcements-192.png'),
        sizes:'192x192',
        type:'image/png',
        purpose:'any',
      },
      {
        src:withBase('/assets/pwa/announcements-512.png'),
        sizes:'512x512',
        type:'image/png',
        purpose:'any',
      },
    ],
  };

  return new Response(JSON.stringify(manifest), {
    headers:{
      'Content-Type':'application/manifest+json; charset=utf-8',
      'Cache-Control':'public, max-age=300, must-revalidate',
    },
  });
};

import type { APIRoute } from 'astro';
import { getPublicArtifactRegistry } from '../../lib/artifacts';

export const GET: APIRoute = () => new Response(
  JSON.stringify(getPublicArtifactRegistry()),
  {
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'public, max-age=300, stale-while-revalidate=3600',
    },
  },
);

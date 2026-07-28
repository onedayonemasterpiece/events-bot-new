import { getStaticEventDateManifest } from '../../lib/staticEventDates';

export function GET() {
  const manifest = getStaticEventDateManifest();
  return new Response(JSON.stringify(manifest), {
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'public, max-age=300, stale-while-revalidate=3600',
    },
  });
}

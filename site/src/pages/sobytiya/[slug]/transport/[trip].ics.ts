import type { APIRoute } from 'astro';
import { buildTransportIcs } from '../../../../lib/ics';
import {
  eventTransportTripKey,
  getEventTransportSuggestion,
  type EventTrainOption,
  type EventTransportDirection,
  type EventTransportSuggestion,
} from '../../../../lib/eventTransport';
import { getEvents } from '../../../../lib/events';
import type { PreviewEvent } from '../../../../lib/types';

interface TransportIcsProps {
  event: PreviewEvent;
  suggestion: EventTransportSuggestion;
  direction: EventTransportDirection;
  train: EventTrainOption;
}

export function getStaticPaths() {
  return getEvents().flatMap((event) => {
    const suggestion = getEventTransportSuggestion(event);
    if (!suggestion) return [];
    const entries: Array<{ direction: EventTransportDirection; train: EventTrainOption }> = [
      ...suggestion.outbound.map((train) => ({ direction: 'outbound' as const, train })),
      ...suggestion.returns.map((train) => ({ direction: 'return' as const, train })),
    ];
    return entries.map(({ direction, train }) => ({
      params: { slug: event.slug, trip: eventTransportTripKey(direction, train) },
      props: { event, suggestion, direction, train } satisfies TransportIcsProps,
    }));
  });
}

export const GET: APIRoute = ({ props }) => {
  const { event, suggestion, direction, train } = props as TransportIcsProps;
  const trip = eventTransportTripKey(direction, train);
  return new Response(buildTransportIcs(event, suggestion, direction, train), {
    headers: {
      'content-type': 'text/calendar; charset=utf-8',
      'content-disposition': `inline; filename="kenigevents-${event.id}-${trip}.ics"`,
      'cache-control': 'public, max-age=300',
    },
  });
};

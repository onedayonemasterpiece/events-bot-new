import type { APIRoute } from 'astro';
import { buildTransportIcs, transportIcsDownloadFilename } from '../../../../lib/ics';
import {
  eventTransportCalendarEntries,
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
    return eventTransportCalendarEntries(suggestion).map(({ direction, train }) => ({
      params: { slug: event.slug, trip: eventTransportTripKey(suggestion, direction, train) },
      props: { event, suggestion, direction, train } satisfies TransportIcsProps,
    }));
  });
}

export const GET: APIRoute = ({ props }) => {
  const { event, suggestion, direction, train } = props as TransportIcsProps;
  const trip = eventTransportTripKey(suggestion, direction, train);
  return new Response(buildTransportIcs(event, suggestion, direction, train), {
    headers: {
      'content-type': 'text/calendar; charset=utf-8',
      'content-disposition': `inline; filename="${transportIcsDownloadFilename(event, trip)}"`,
      'cache-control': 'public, max-age=300',
    },
  });
};

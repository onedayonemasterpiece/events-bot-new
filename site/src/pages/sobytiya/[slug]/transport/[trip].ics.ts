import type { APIRoute } from 'astro';
import { buildTransportIcs, transportIcsDownloadFilename } from '../../../../lib/ics';
import { desktopEventWithExplicitEnd } from '../../../../lib/desktopEventTransport';
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
    // Desktop can recover a strictly source-labelled duration that has not yet
    // reached the export. Its return shortlist can therefore differ from the
    // unchanged mobile shortlist. Generate the union so every rendered rail
    // calendar link resolves, while keeping one canonical file per trip key.
    const variants = [event, desktopEventWithExplicitEnd(event)];
    const paths = new Map<string, ReturnType<typeof pathForTrip>>();
    for (const variant of variants) {
      const suggestion = getEventTransportSuggestion(variant);
      if (!suggestion) continue;
      for (const { direction, train } of eventTransportCalendarEntries(suggestion)) {
        const trip = eventTransportTripKey(suggestion, direction, train);
        paths.set(trip, pathForTrip(variant, suggestion, direction, train));
      }
    }
    return [...paths.values()];
  });
}

function pathForTrip(
  event: PreviewEvent,
  suggestion: EventTransportSuggestion,
  direction: EventTransportDirection,
  train: EventTrainOption,
) {
  return {
    params: { slug: event.slug, trip: eventTransportTripKey(suggestion, direction, train) },
    props: { event, suggestion, direction, train } satisfies TransportIcsProps,
  };
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

/// <reference path="../.astro/types.d.ts" />
/// <reference types="astro/client" />

interface ImportMetaEnv {
  readonly PUBLIC_SERVICE_SHARE_DESKTOP_MODE?: 'd0' | 'd1' | 'd2';
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

interface Window {
  KenigEventsNormalizeInternalEventUrl?: (value: string, options?: { absolute?: boolean }) => string;
  KenigEventsRenderEventCard?: (item: unknown, variant?: string) => string;
  KenigEventsCreateEventCard?: (item: unknown, variant?: string) => HTMLElement | null;
  applyFeedbackState?: (options?: { skipDiscoveryHydration?: boolean; anchorEventId?: string | number | null }) => Promise<void>;
}

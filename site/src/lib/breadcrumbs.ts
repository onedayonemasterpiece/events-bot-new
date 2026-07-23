import type { PreviewEvent } from './types';
import { isExhibitionLikeEvent, siteHomeHref, withBase } from './events';

export type BreadcrumbItem = {
  label: string;
  href: string;
};

/**
 * Breadcrumbs describe the materialized site hierarchy, never the visit path.
 * Do not invent a category parent until that category has a real landing page.
 */
export function eventBreadcrumbParents(event: Pick<PreviewEvent, 'event_type' | 'title' | 'topics'>): BreadcrumbItem[] {
  const parents: BreadcrumbItem[] = [{ label:'Афиша', href:siteHomeHref() }];
  if (isExhibitionLikeEvent(event)) {
    parents.push({ label:'Выставки', href:withBase('/vystavki/') });
  }
  return parents;
}

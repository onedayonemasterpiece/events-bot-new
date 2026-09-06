export type AssistantSurfaceState={viewedSectionId:string|null;viewedTitle?:string|null;refinementBaseId:string|null;pendingDraftId:string|null;capture:string};
export type AssistantSurfaceAdapter={version:'1.0.0';element:HTMLElement;getState:()=>AssistantSurfaceState;
  showComposer:()=>void;showSection:(id:string)=>void;beforeOverlayOpen:()=>Promise<void>;diagnostic:()=>Record<string,unknown>};
/** FI-16/FI-17 integration boundary. This adapter owns NO sticky positioning,
 * occupied rectangles, nav shell, heading clone or z-index. The shared islands
 * owner may mount the same composer and MUST await beforeOverlayOpen(). */
export function announceAssistantSurface(adapter:AssistantSurfaceAdapter):()=>void{
  const owner=window as Window&{KenigEventsSearchAdapterV1?:AssistantSurfaceAdapter};
  owner.KenigEventsSearchAdapterV1=adapter;
  window.dispatchEvent(new CustomEvent('kenigevents:search-adapter-ready',{detail:adapter}));
  return()=>{if(owner.KenigEventsSearchAdapterV1===adapter)delete owner.KenigEventsSearchAdapterV1;};
}

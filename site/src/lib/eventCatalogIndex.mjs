/** Index one immutable exported snapshot; return fresh arrays to sort/filter consumers. */
export function createEventCatalogIndex(current, archived) {
 const order=(a,b)=>(a.starts_at||a.start_date).localeCompare(b.starts_at||b.start_date)||a.id-b.id;
 const currentRows=[...current].sort(order);
 // Preserve the existing detail merge precedence for overlapping retained IDs.
 const byId=new Map([...current,...archived].map(event=>[event.id,event]));
 const detailRows=[...byId.values()].sort(order);
 const bySlug=new Map();for(const event of detailRows)if(!bySlug.has(event.slug))bySlug.set(event.slug,event);
 return {current:()=>[...currentRows],details:()=>[...detailRows],byId:id=>byId.get(id),bySlug:slug=>bySlug.get(slug)};
}

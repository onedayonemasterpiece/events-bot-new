import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const siteRoot=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const root=path.join(siteRoot,'evidence/free-collection-g12');
const readJson=async(rel)=>JSON.parse(await readFile(path.join(root,rel),'utf8'));
const hash=(bytes)=>createHash('sha256').update(bytes).digest('hex');
const fixtures={
  '8006':{title:'Донорская акция «Стань донором крови»',media:'dd8834258d4a1ebde029aca1960bdd224bdf636d3fd8aee8fc7824012475de8b',calendar:true},
  '8200':{title:'Музыкальная экспедиция Бориса Андрианова',media:'56aa670778d82f16b1d286e3449c1e25776c19c67a0012c12dd39f00dce61c6e',calendar:true},
  '2182':{title:'Песчаная палитра Куршской косы',media:'99d4b75ef3291c90e1457b6fdc3fe89e519b327f9d6c8ff56cd95f763e71ab1e',calendar:false},
  '6711':{title:'Выставка «Под шум балтийского ветра»',media:'34365f31af78b1a72aa271b0bc8b5a5300a5ed3d6e3cf8399068d2a2913f9a36',calendar:false},
  '7609':{title:'Выставка «Живая нить традиций»',media:'1e0fd5f604728ad96b4ec00ecb639e27b1a10acdc6565bfe14a4f83fa4195de2',calendar:false},
};

test('evidence is rendered by the current production component chain without production edits',async()=>{
  const regions=await readJson('regions.json');
  assert.equal(regions.astro_base,'64f75d10f7aff33fa616cee212878bd9d03673b1');
  assert.deepEqual(regions.production_source_bindings.map((x)=>x.path),[
    'site/src/components/EventCard.astro','site/src/components/FreeCollectionSurface.astro','site/src/layouts/EventLayout.astro','site/src/styles/design-system.css',
  ]);
  for (const binding of regions.production_source_bindings) {
    const bytes=await readFile(path.join(siteRoot,binding.path.replace(/^site\//,'')));
    assert.equal(hash(bytes),binding.sha256);
  }
});

test('four L2 cases and four L3 groups have browser-used boxes, styles and line fragments',async()=>{
  const {regions}=await readJson('regions.json');
  assert.equal(regions.length,8);
  assert.deepEqual(regions.map((x)=>x.id).sort(),[
    'eventcard.desktop-wide-calendar.8006','eventcard.desktop-packed-calendar-absent.2182','eventcard.mobile-wide-calendar.8006','eventcard.mobile-packed-calendar-absent.2182',
    'row.desktop.events','row.desktop.exhibitions','group.mobile.events','group.mobile.exhibitions',
  ].sort());
  for (const region of regions) {
    assert.ok(region.root.box.width>0 && region.root.box.height>0,region.id);
    assert.ok(region.descendants.some((n)=>n.line_fragments.length>0),`${region.id} line fragments`);
    assert.ok(region.descendants.every((n)=>Object.hasOwn(n.computed,'fontFamily')),`${region.id} computed styles`);
    assert.equal(region.viewport.dpr,1);
  }
});

test('fixture content, media and actual calendar presence are independent exact expectations',async()=>{
  const {regions}=await readJson('regions.json');
  for (const region of regions) for (const [id,expected] of Object.entries(fixtures)) {
    const card=[region.root,...region.descendants].find((n)=>n.data['data-event-id']===id && n.data['data-event-card']!==undefined);
    if (!card) continue;
    const descendants=region.descendants;
    assert.ok(descendants.some((n)=>n.owner_event_id===id && (n.data['data-event-title']===expected.title || n.text_direct===expected.title)),`${region.id}/${id} title`);
    assert.ok(descendants.some((n)=>n.owner_event_id===id && n.data['data-card-image']!==undefined && n.attributes.src.includes(expected.media)),`${region.id}/${id} media identity`);
    const image=descendants.find((n)=>n.owner_event_id===id && n.data['data-card-image']!==undefined);
    assert.ok(image && image.data['data-card-authoritative-fit']);
    const calendar=descendants.some((n)=>n.data['data-calendar-event-id']===id);
    assert.equal(calendar,expected.calendar,`${region.id}/${id} calendar`);
  }
  const exhibitions=regions.find((r)=>r.id==='row.desktop.exhibitions');
  const cards=exhibitions.descendants.filter((n)=>n.data['data-event-card']!==undefined);
  assert.equal(cards[0].box.height,cards[1].box.height,'first desktop exhibition row is equal height');
});

test('font and bounded PNG manifests are complete and content addressed',async()=>{
  const fonts=await readJson('runtime-font-manifest.json'); const captures=await readJson('captures-manifest.json');
  assert.equal(fonts.font_ready,true); assert.ok(fonts.regions.some((r)=>r.platform_fonts.length>0));
  assert.ok(fonts.font_files.some((f)=>f.locally_hashable && f.file_sha256));
  assert.equal(captures.captures.length,8);
  for (const capture of captures.captures) {
    const bytes=await readFile(path.join(siteRoot,capture.path.replace(/^site\//,'')));
    assert.equal(bytes.subarray(1,4).toString(),'PNG'); assert.equal(hash(bytes),capture.sha256);
    assert.ok(capture.width>0 && capture.height>0);
  }
});

test('export map and receipt contract bind every region to one governed component identity',async()=>{
  const map=await readJson('export-readback-map.json'); const contract=await readJson('root-receipt-contract.json');
  assert.equal(map.mappings.length,8); assert.equal(new Set(map.mappings.map((x)=>x.astro_region_id)).size,8);
  assert.equal(contract.rules.one_master_identity,'component.event-card.free-collection');
  assert.equal(contract.rules.four_structural_variants.length,4);
  assert.equal(contract.rules.old_penpot_readback_is_authority,false);
});
